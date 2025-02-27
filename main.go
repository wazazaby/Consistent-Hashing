package main

import (
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

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
	return murmur3.Sum32([]byte(key))
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
	if len(chr.hashes) == 0 {
		return -1
	}

	for i, h := range chr.hashes {
		if h > hash {
			return i
		}
	}

	return 0
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

func (chr *ConsistentHashRing) RetrieveKey(key string) (string, error) {
	node := chr.GetNode(key)
	if node == nil {
		return "", errors.New("no node found")
	}

	val, ok := node.Keys[key]
	if !ok {
		return "", errors.New("no key found to be stored in node: " + node.ID)
	}

	return val, nil
}

func (chr *ConsistentHashRing) RemoveNode(id string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	hash := hashFunction(id)
	node, exists := chr.nodes[hash]
	if !exists {
		return
	}

	nextNodeIndex := chr.GetNextNodeIndex(hash)

	nextNode := chr.nodes[chr.hashes[nextNodeIndex]]

	maps.Copy(nextNode.Keys, node.Keys)

	delete(chr.nodes, hash)
	for i, h := range chr.hashes {
		if h == hash {
			chr.hashes = slices.Delete(chr.hashes, i, i+1)
		}
	}
}

func (chr *ConsistentHashRing) PrintRing() {
	for _, h := range chr.hashes {
		fmt.Printf("Node: %s \t\t Hash: %d \t\t Total Keys: %v\n", chr.nodes[h].ID, h, len(chr.nodes[h].Keys))
	}
}

func randomString(minLen, maxLen int) string {
	length := rand.Intn(maxLen-minLen+1) + minLen

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().Unix())
}

func main() {
	ring := NewConsistentHashRing()

	ring.AddNode("Server1")
	ring.AddNode("Server2")
	ring.AddNode("Server3")
	ring.AddNode("Server4")
	ring.AddNode("Server5")
	ring.AddNode("Server6")
	ring.AddNode("Server7")
	ring.AddNode("Server8")
	ring.AddNode("Server9")
	ring.AddNode("Server10")
	ring.AddNode("Server11")
	ring.AddNode("Server12")
	ring.AddNode("Server13")
	ring.AddNode("Server14")
	ring.AddNode("Server15")
	ring.AddNode("Server16")
	ring.AddNode("Server17")
	ring.AddNode("Server18")
	ring.AddNode("Server19")
	ring.AddNode("Server20")
	ring.AddNode("Server21")
	ring.AddNode("Server22")
	ring.AddNode("Server23")
	ring.AddNode("Server24")
	ring.AddNode("Server25")
	ring.AddNode("Server26")
	ring.AddNode("Server27")
	ring.AddNode("Server28")
	ring.AddNode("Server29")
	ring.AddNode("Server30")
	ring.AddNode("Server31")
	ring.AddNode("Server32")
	ring.AddNode("Server33")
	ring.AddNode("Server34")
	ring.AddNode("Server35")
	ring.AddNode("Server36")
	ring.AddNode("Server37")
	ring.AddNode("Server38")
	ring.AddNode("Server39")
	ring.AddNode("Server40")
	ring.AddNode("Server41")
	ring.AddNode("Server42")
	ring.AddNode("Server43")
	ring.AddNode("Server45")
	ring.AddNode("Server46")
	ring.AddNode("Server47")
	ring.AddNode("Server48")
	ring.AddNode("Server49")
	ring.AddNode("Server50")

	for range 5000000 {
		ring.StoreKey(randomString(5, 10), randomString(5, 10))
	}

	ring.PrintRing()
}
