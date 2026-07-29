package aggregatedpool

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNexusMethodCancellationRegistry_Lifecycle(t *testing.T) {
	registry := new(NexusMethodCancellationRegistry)

	registry.Register(42)
	state, ok := registry.Lookup(42)
	require.True(t, ok)
	assert.False(t, state.Cancelled)
	assert.Empty(t, state.Reason)

	require.True(t, registry.Cancel(42, "context canceled"))
	state, ok = registry.Lookup(42)
	require.True(t, ok)
	assert.True(t, state.Cancelled)
	assert.Equal(t, "context canceled", state.Reason)

	// Cancellation is sticky and non-consuming.
	state, ok = registry.Lookup(42)
	require.True(t, ok)
	assert.True(t, state.Cancelled)

	registry.Discard(42)
	_, ok = registry.Lookup(42)
	assert.False(t, ok)
	assert.False(t, registry.Cancel(42, "late cancellation"))
}

func TestNexusMethodCancellationRegistry_ZeroIDIsNeverRegistered(t *testing.T) {
	registry := new(NexusMethodCancellationRegistry)
	registry.Register(0)

	_, ok := registry.Lookup(0)
	assert.False(t, ok)
	assert.False(t, registry.Cancel(0, "cancelled"))
}

func TestNexusMethodCancellationRegistry_Reset(t *testing.T) {
	registry := new(NexusMethodCancellationRegistry)
	first := registry.RegisterNew()
	second := registry.RegisterNew()
	require.True(t, registry.Cancel(second, "cancelled"))

	registry.Reset()

	_, firstFound := registry.Lookup(first)
	_, secondFound := registry.Lookup(second)
	assert.False(t, firstFound)
	assert.False(t, secondFound)

	afterReset := registry.RegisterNew()
	assert.Greater(t, afterReset, second, "reset must not reuse invocation IDs")
}

func TestNexusMethodCancellationRegistry_ConcurrentLifecycle(t *testing.T) {
	const invocations = 128

	registry := new(NexusMethodCancellationRegistry)
	var wg sync.WaitGroup
	for i := 1; i <= invocations; i++ {
		id := uint64(i)
		wg.Add(1)
		go func() {
			defer wg.Done()

			registry.Register(id)
			registry.Cancel(id, strconv.FormatUint(id, 10))
			state, ok := registry.Lookup(id)
			if ok {
				assert.True(t, state.Cancelled)
			}
			registry.Discard(id)
		}()
	}
	wg.Wait()

	for i := 1; i <= invocations; i++ {
		_, ok := registry.Lookup(uint64(i))
		assert.False(t, ok)
	}
}
