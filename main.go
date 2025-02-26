package main

import (
	"hash/fnv"
	"slices"
	"sync"
)

type Node struct {
	ID   string
	Keys map[string]string
}

type ConsistentHashRing struct {
	mu     sync.RWMutex
	nodes  map[uint32]*Node
	hashes []uint32
}

func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:  make(map[uint32]*Node),
		hashes: []uint32{},
	}
}

func hashFunction(key string) uint32 {
	h := fnv.New32()
	h.Write([]byte(key))
	return h.Sum32()
}

func (chr *ConsistentHashRing) AddNode(id string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	hash := hashFunction(id)
	chr.nodes[hash] = &Node{ID: id, Keys: make(map[string]string)}
	chr.hashes = append(chr.hashes, hash)

	slices.Sort(chr.hashes)
}

func (chr *ConsistentHashRing) GetNextNodeIndex(hash uint32) int {
	for i, h := range chr.hashes {
		if h >= hash {
			return i
		}
	}

	if len(chr.hashes) > 0 {
		return 0
	}

	return -1
}

func (chr *ConsistentHashRing) GetNode(hash string) *Node {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	if len(chr.hashes) == 0 {
		return nil
	}

	idx := chr.GetNextNodeIndex(hashFunction(hash))

	if idx == -1 {
		return nil
	}

	return chr.nodes[chr.hashes[idx]]
}

func (chr *ConsistentHashRing) StoreKey(key, val string) {
	node := chr.GetNode(key)

	if node != nil {
		node.Keys[key] = val
	}
}
