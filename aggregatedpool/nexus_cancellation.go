package aggregatedpool

import (
	"sync"
	"sync/atomic"
)

// NexusMethodCancellation is the current cooperative-cancellation state of one
// in-flight Nexus handler method.
type NexusMethodCancellation struct {
	Cancelled bool //nolint:misspell // Preserve the exported field name for source compatibility.
	Reason    string
}

// IsCanceled reports whether cancellation was observed.
func (s NexusMethodCancellation) IsCanceled() bool {
	return s.Cancelled //nolint:misspell // Read the compatibility field through a correctly spelled API.
}

// NexusMethodCancellationRegistry keeps handler-method cancellation state in
// the RoadRunner process rather than a PHP worker. PHP worker processes handle
// one pool request at a time, so a second pool request is neither process-affine
// nor deliverable to a busy single-worker pool.
type NexusMethodCancellationRegistry struct {
	mu      sync.RWMutex
	entries map[uint64]NexusMethodCancellation
	nextID  atomic.Uint64
}

// RegisterNew allocates and registers an invocation ID. The sequence is not
// reset when entries are cleared, preventing a late watcher from an old worker
// pool from canceling a newly registered invocation after plugin reset.
func (r *NexusMethodCancellationRegistry) RegisterNew() uint64 {
	id := r.nextID.Add(1)
	if id == 0 {
		id = r.nextID.Add(1)
	}
	r.Register(id)
	return id
}

// Register marks an invocation active and not canceled.
func (r *NexusMethodCancellationRegistry) Register(invocationID uint64) {
	if invocationID == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.entries == nil {
		r.entries = make(map[uint64]NexusMethodCancellation)
	}
	r.entries[invocationID] = NexusMethodCancellation{}
}

// Cancel marks an active invocation canceled. It returns false when the
// invocation has already completed (or was never registered).
func (r *NexusMethodCancellationRegistry) Cancel(invocationID uint64, reason string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.entries[invocationID]
	if !ok {
		return false
	}
	if state.IsCanceled() {
		return true
	}

	state.Cancelled = true //nolint:misspell // Preserve the exported field name for source compatibility.
	state.Reason = reason
	r.entries[invocationID] = state
	return true
}

// Lookup returns an immutable snapshot for an active invocation.
func (r *NexusMethodCancellationRegistry) Lookup(invocationID uint64) (NexusMethodCancellation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	state, ok := r.entries[invocationID]
	return state, ok
}

// Discard removes an invocation after its PHP handler request has returned.
func (r *NexusMethodCancellationRegistry) Discard(invocationID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.entries, invocationID)
}

// Reset drops state owned by the old PHP worker pool during plugin reset.
func (r *NexusMethodCancellationRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	clear(r.entries)
}
