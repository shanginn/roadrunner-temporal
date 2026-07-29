package rrtemporal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/roadrunner-temporal/v5/aggregatedpool"
)

func TestNexusMethodCancellationRPC_JSONContract(t *testing.T) {
	request, err := json.Marshal(NexusMethodCancellationRequest{InvocationID: 17})
	require.NoError(t, err)
	assert.JSONEq(t, `{"invocationId":17}`, string(request))

	active, err := json.Marshal(NexusMethodCancellationResponse{})
	require.NoError(t, err)
	assert.JSONEq(t, `{"cancelled":false}`, string(active))

	cancelled, err := json.Marshal(NexusMethodCancellationResponse{
		Cancelled: true,
		Reason:    "context canceled",
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"cancelled":true,"reason":"context canceled"}`, string(cancelled))
}

func TestGetNexusMethodCancellation_ReturnsStickySnapshot(t *testing.T) {
	registry := new(aggregatedpool.NexusMethodCancellationRegistry)
	registry.Register(17)

	service := &rpc{
		plugin: &Plugin{
			temporal: &temporal{nexusMethodCancellations: registry},
		},
	}

	var active NexusMethodCancellationResponse
	require.NoError(t, service.GetNexusMethodCancellation(
		NexusMethodCancellationRequest{InvocationID: 17},
		&active,
	))
	assert.False(t, active.Cancelled)
	assert.Empty(t, active.Reason)

	require.True(t, registry.Cancel(17, "context canceled"))

	var first NexusMethodCancellationResponse
	require.NoError(t, service.GetNexusMethodCancellation(
		NexusMethodCancellationRequest{InvocationID: 17},
		&first,
	))
	assert.True(t, first.Cancelled)
	assert.Equal(t, "context canceled", first.Reason)

	var second NexusMethodCancellationResponse
	require.NoError(t, service.GetNexusMethodCancellation(
		NexusMethodCancellationRequest{InvocationID: 17},
		&second,
	))
	assert.Equal(t, first, second, "polling must not consume cancellation")
}

func TestGetNexusMethodCancellation_UnknownAndZeroAreNotCancelled(t *testing.T) {
	service := &rpc{
		plugin: &Plugin{
			temporal: &temporal{
				nexusMethodCancellations: new(aggregatedpool.NexusMethodCancellationRegistry),
			},
		},
	}

	for _, invocationID := range []uint64{0, 999} {
		var out NexusMethodCancellationResponse
		require.NoError(t, service.GetNexusMethodCancellation(
			NexusMethodCancellationRequest{InvocationID: invocationID},
			&out,
		))
		assert.False(t, out.Cancelled)
		assert.Empty(t, out.Reason)
	}
}
