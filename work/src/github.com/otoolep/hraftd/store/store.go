// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
	"strconv"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/otoolep/hraftd/consistenthash"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
    RaftDir  string
    RaftBind string
    inmem    bool

    nodeID string

    mu         sync.Mutex
    raftMap    map[string]*raft.Raft
    fsmMap     map[string]*fsm  
    hashRing   *consistenthash.HashRing
    logger     *log.Logger
}

// New returns a new Store.
func New(nodeID string, inmem bool) *Store {
    return &Store{
        inmem:     inmem,
        nodeID:    nodeID,
        raftMap:   make(map[string]*raft.Raft),
        fsmMap:    make(map[string]*fsm),
        hashRing:  consistenthash.NewHashRing(3), //change to change number of replicas
        logger:    log.New(os.Stderr, "[store] ", log.LstdFlags),
    }
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Store) Open(enableSingle bool, localID string, shardIDs []string) error {
    s.logger.Printf("Opening store with shards: %v", shardIDs)
    for i, shardID := range shardIDs {
        raftDir := filepath.Join(s.RaftDir, shardID)
        if err := os.MkdirAll(raftDir, 0700); err != nil {
            return fmt.Errorf("failed to create Raft directory for shard %s: %v", shardID, err)
        }

        // unique RaftBind by incrementing the port
        raftBindPerShard, err := incrementPort(s.RaftBind, i)
        if err != nil {
            return fmt.Errorf("failed to generate raft bind address per shard: %v", err)
        }

        // Initialize Raft for this shard
        ra, err := s.setupRaft(raftDir, raftBindPerShard, enableSingle, localID+"-"+shardID, shardID)
        if err != nil {
            return fmt.Errorf("failed to set up Raft for shard %s: %v", shardID, err)
        }
        s.raftMap[shardID] = ra

        s.hashRing.AddNode(consistenthash.Node{
            ID:   shardID,
            Addr: raftBindPerShard,
        })
    }
    return nil
}

func (s *Store) setupRaft(raftDir, raftBind string, enableSingle bool, localID, shardID string) (*raft.Raft, error) {
    // Setup Raft configuration.
    config := raft.DefaultConfig()
    config.LocalID = raft.ServerID(localID)

    // Setup Raft communication.
    addr, err := net.ResolveTCPAddr("tcp", raftBind)
    if err != nil {
        return nil, err
    }
    transport, err := raft.NewTCPTransport(raftBind, addr, 3, 10*time.Second, os.Stderr)
    if err != nil {
        return nil, err
    }

    // Create the snapshot store. This allows the Raft to truncate the log.
    snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("file snapshot store: %s", err)
    }

    // Create the log store and stable store.
    var logStore raft.LogStore
    var stableStore raft.StableStore
    if s.inmem {
        logStore = raft.NewInmemStore()
        stableStore = raft.NewInmemStore()
    } else {
        boltDB, err := raftboltdb.New(raftboltdb.Options{
            Path: filepath.Join(raftDir, "raft.db"),
        })
        if err != nil {
            return nil, fmt.Errorf("new bolt store: %s", err)
        }
        logStore = boltDB
        stableStore = boltDB
    }

    // Instantiate the Raft systems.
    fsm := newFSM()
    ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
    if err != nil {
        return nil, fmt.Errorf("new raft: %s", err)
    }

    s.fsmMap[shardID] = fsm

    if enableSingle {
        configuration := raft.Configuration{
            Servers: []raft.Server{
                {
                    ID:      config.LocalID,
                    Address: transport.LocalAddr(),
                },
            },
        }
        ra.BootstrapCluster(configuration)
    }

    return ra, nil
}

func incrementPort(raftBind string, offset int) (string, error) {
    host, portStr, err := net.SplitHostPort(raftBind)
    if err != nil {
        return "", err
    }
    port, err := strconv.Atoi(portStr)
    if err != nil {
        return "", err
    }
    newPort := port + offset
    return net.JoinHostPort(host, strconv.Itoa(newPort)), nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
    shardID := s.hashRing.GetNode(key).ID
    s.mu.Lock()
    fsm, ok := s.fsmMap[shardID]
    s.mu.Unlock()
    if !ok {
        return "", fmt.Errorf("no FSM for shard %s", shardID)
    }

    fsm.mu.Lock()
    defer fsm.mu.Unlock()
    value := fsm.m[key]
    return value, nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
    shardID := s.hashRing.GetNode(key).ID
    ra, ok := s.raftMap[shardID]
    if !ok {
        return fmt.Errorf("no Raft instance for shard %s", shardID)
    }

    if ra.State() != raft.Leader {
        return fmt.Errorf("not leader")
    }

    c := &command{
        Op:    "set",
        Key:   key,
        Value: value,
    }
    b, err := json.Marshal(c)
    if err != nil {
        return err
    }

    f := ra.Apply(b, raftTimeout)
    return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
    shardID := s.hashRing.GetNode(key).ID
    ra, ok := s.raftMap[shardID]
    if !ok {
        return fmt.Errorf("no Raft instance for shard %s", shardID)
    }

    if ra.State() != raft.Leader {
        return fmt.Errorf("not leader")
    }

    c := &command{
        Op:  "delete",
        Key: key,
    }
    b, err := json.Marshal(c)
    if err != nil {
        return err
    }

    f := ra.Apply(b, raftTimeout)
    return f.Error()
}

// Join joins a node to the cluster for the given shards.
func (s *Store) Join(nodeID, addr string, shardIDs []string) error {
    s.logger.Printf("received join request for node %s at %s with shards %v", nodeID, addr, shardIDs)

    for i, shardID := range shardIDs {
        ra, ok := s.raftMap[shardID]
        if !ok {
            // Node doesn't have this shard; initialize Raft for it.
            raftDir := filepath.Join(s.RaftDir, shardID)
            if err := os.MkdirAll(raftDir, 0700); err != nil {
                return fmt.Errorf("failed to create Raft directory for shard %s: %v", shardID, err)
            }

            raftBindPerShard, err := incrementPort(s.RaftBind, i)
            if err != nil {
                return fmt.Errorf("failed to generate raft bind address per shard: %v", err)
            }

            ra, err := s.setupRaft(raftDir, raftBindPerShard, false, nodeID+"-"+shardID, shardID)
            if err != nil {
                return fmt.Errorf("failed to set up Raft for shard %s: %v", shardID, err)
            }
            s.raftMap[shardID] = ra
        }

        // Join the Raft cluster for this shard
        raftBindPerShard, err := incrementPort(addr, i)
        if err != nil {
            return fmt.Errorf("failed to generate raft bind address per shard: %v", err)
        }

        if err := s.joinRaftCluster(ra, nodeID+"-"+shardID, raftBindPerShard); err != nil {
            return err
        }
    }
    return nil
}

func (s *Store) joinRaftCluster(ra *raft.Raft, nodeID, addr string) error {
    configFuture := ra.GetConfiguration()
    if err := configFuture.Error(); err != nil {
        return err
    }

    for _, srv := range configFuture.Configuration().Servers {
        if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
            if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
                s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
                return nil
            }

            future := ra.RemoveServer(srv.ID, 0, 0)
            if err := future.Error(); err != nil {
                return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
            }
        }
    }

    f := ra.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
    if f.Error() != nil {
        return f.Error()
    }
    s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
    return nil
}

type fsm struct {
    mu sync.Mutex
    m  map[string]string
}

func newFSM() *fsm {
    return &fsm{
        m: make(map[string]string),
    }
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
