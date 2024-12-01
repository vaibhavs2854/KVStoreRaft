package consistenthash

import (
    "fmt"
    "hash/fnv"
    "sort"
    "strconv"
    "sync"
)

type Node struct {
    ID   string
    Addr string
}

type HashRing struct {
    nodes       []uint32           
    nodeMap     map[uint32]Node  
    replicas    int                 
    nodeHashes  map[string][]uint32
    hashFn      func(data []byte) uint32
    sync.RWMutex
}
func NewHashRing(replicas int) *HashRing {
    return &HashRing{
        replicas:   replicas,
        nodeMap:    make(map[uint32]Node),
        nodeHashes: make(map[string][]uint32),
        hashFn:     fnvHash,
    }
}

func fnvHash(data []byte) uint32 {
    h := fnv.New32a()
    h.Write(data)
    return h.Sum32()
}

func (h *HashRing) AddNode(node Node) {
    h.Lock()
    defer h.Unlock()
    fmt.Printf("Adding node '%s' with address '%s'\n", node.ID, node.Addr)
    for i := 0; i < h.replicas; i++ {
        virtualNodeID := node.ID + strconv.Itoa(i)
        hash := h.hashFn([]byte(virtualNodeID))
        h.nodes = append(h.nodes, hash)
        h.nodeMap[hash] = node
        h.nodeHashes[node.ID] = append(h.nodeHashes[node.ID], hash)
        fmt.Printf("Added virtual node '%s' with hash %d\n", virtualNodeID, hash)
    }
    sort.Slice(h.nodes, func(i, j int) bool { return h.nodes[i] < h.nodes[j] })
    fmt.Printf("Current hash ring nodes: %v\n", h.nodes)
}

func (h *HashRing) RemoveNode(nodeID string) {
    h.Lock()
    defer h.Unlock()
    fmt.Printf("Removing node '%s'\n", nodeID)
    hashes := h.nodeHashes[nodeID]
    for _, hash := range hashes {
        idx := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i] >= hash })
        if idx < len(h.nodes) && h.nodes[idx] == hash {
            h.nodes = append(h.nodes[:idx], h.nodes[idx+1:]...)
            delete(h.nodeMap, hash)
            fmt.Printf("Removed virtual node with hash %d\n", hash)
        }
    }
    delete(h.nodeHashes, nodeID)
    fmt.Printf("Current hash ring nodes: %v\n", h.nodes)
}

func (h *HashRing) GetNode(key string) Node {
    h.RLock()
    defer h.RUnlock()
    if len(h.nodes) == 0 {
        fmt.Println("Hash ring is empty")
        return Node{}
    }
    hash := h.hashFn([]byte(key))
    idx := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i] >= hash })
    if idx == len(h.nodes) {
        idx = 0
    }
    node := h.nodeMap[h.nodes[idx]]
    fmt.Printf(" Key '%s' (hash %d) is assigned to node '%s'\n", key, hash, node.ID)
    return node
}

func (h *HashRing) PrintRing() {
    h.RLock()
    defer h.RUnlock()
    fmt.Println("Current hash ring state:")
    for _, hash := range h.nodes {
        node := h.nodeMap[hash]
        fmt.Printf("  Hash %d: Node %s\n", hash, node.ID)
    }
}
