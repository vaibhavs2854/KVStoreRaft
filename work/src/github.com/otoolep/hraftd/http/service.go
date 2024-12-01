// Package httpd provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package httpd

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) (string, error)

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

    // Join joins the node, identified by nodeID and reachable at addr, to the cluster.
    Join(nodeID string, addr string, shardIDs []string) error
}

// Service provides HTTP service.
type Service struct {
	addr string
	ln   net.Listener

	store Store
}

func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}
// Start starts the service.
func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.ln.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
    m := map[string]interface{}{}
    if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    remoteAddr, ok := m["addr"].(string)
    if !ok {
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    nodeID, ok := m["id"].(string)
    if !ok {
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    shards, ok := m["shards"].([]interface{})
    if !ok {
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    shardIDs := make([]string, len(shards))
    for i, shard := range shards {
        shardID, ok := shard.(string)
        if !ok {
            w.WriteHeader(http.StatusBadRequest)
            return
        }
        shardIDs[i] = shardID
    }

    if err := s.store.Join(nodeID, remoteAddr, shardIDs); err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
    getKey := func() string {
        parts := strings.Split(r.URL.Path, "/")
        if len(parts) != 3 {
            return ""
        }
        return parts[2]
    }

    key := getKey()
    if key == "" {
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    switch r.Method {
    case "GET":
        v, err := s.store.Get(key)
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            return
        }

        b, err := json.Marshal(map[string]string{key: v})
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            return
        }
        io.WriteString(w, string(b))

    case "POST":
        // Read the value from the POST body.
        m := map[string]string{}
        if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
            w.WriteHeader(http.StatusBadRequest)
            return
        }
        for k, v := range m {
            if err := s.store.Set(k, v); err != nil {
                w.WriteHeader(http.StatusInternalServerError)
                return
            }
        }

    case "DELETE":
        if err := s.store.Delete(key); err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            return
        }

    default:
        w.WriteHeader(http.StatusMethodNotAllowed)
    }
    return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}