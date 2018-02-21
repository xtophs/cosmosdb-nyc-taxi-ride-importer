package main

import (
	"sync"
)

// Nexter generates unique sequential ids in a threadsafe way.
type Nexter struct {
	id   uint64
	lock sync.Mutex
}

// Next generates a new id
func (n *Nexter) Next() (nextID uint64) {
	n.lock.Lock()
	nextID = n.id
	n.id++
	//fmt.Printf("Next id %d\n", n.id)
	n.lock.Unlock()
	return
}

func (n *Nexter) Last() (lastID uint64) {
	n.lock.Lock()
	lastID = n.id - 1
	n.lock.Unlock()
	return
}
