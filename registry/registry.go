package registry

import (
	"sync"

	bindings "go.temporal.io/sdk/internalbindings"
)

// Registry atomically pairs one Push with one Listen per ID. The first value
// and first listener win; once paired (or discarded), later calls for that ID
// are ignored. Callbacks run outside the lock.
type Registry[T any] struct {
	mu    sync.Mutex
	slots map[uint64]*slot[T]
}

// ListenerFunc is invoked once per ID with the delivered (value, err) pair.
type ListenerFunc[T any] func(value T, err error)

type entry[T any] struct {
	value T
	err   error
}

type slot[T any] struct {
	entry    *entry[T]
	listener ListenerFunc[T]
	terminal bool
}

func (c *Registry[T]) Listen(id uint64, cl ListenerFunc[T]) {
	if cl == nil {
		return
	}

	c.mu.Lock()
	s := c.slot(id)
	if s.terminal || s.listener != nil {
		c.mu.Unlock()
		return
	}

	if s.entry == nil {
		s.listener = cl
		c.mu.Unlock()
		return
	}

	e := *s.entry
	s.entry = nil
	s.terminal = true
	c.mu.Unlock()

	cl(e.value, e.err)
}

func (c *Registry[T]) Push(id uint64, value T, err error) {
	c.mu.Lock()
	s := c.slot(id)
	if s.terminal || s.entry != nil {
		c.mu.Unlock()
		return
	}

	if s.listener == nil {
		s.entry = &entry[T]{value: value, err: err}
		c.mu.Unlock()
		return
	}

	listener := s.listener
	s.listener = nil
	s.terminal = true
	c.mu.Unlock()

	listener(value, err)
}

// Discard terminally closes id and drops any pending entry or listener.
func (c *Registry[T]) Discard(id uint64) {
	c.mu.Lock()
	s := c.slot(id)
	s.entry = nil
	s.listener = nil
	s.terminal = true
	c.mu.Unlock()
}

func (c *Registry[T]) slot(id uint64) *slot[T] {
	if c.slots == nil {
		c.slots = make(map[uint64]*slot[T])
	}
	if s := c.slots[id]; s != nil {
		return s
	}

	s := new(slot[T])
	c.slots[id] = s
	return s
}

// IDRegistry used to gain access to child workflow ids after they become available via callback result.
type IDRegistry = Registry[bindings.WorkflowExecution]

// Listener is the listener type for IDRegistry.
type Listener = ListenerFunc[bindings.WorkflowExecution]
