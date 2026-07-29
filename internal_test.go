package rrtemporal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/roadrunner-temporal/v5/internal"
)

func TestConnectError_IncludesAddressCauseAndHint(t *testing.T) {
	cause := errors.New("context deadline exceeded")
	err := connectError("localhost:7233", cause)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "localhost:7233", "should name the target address")
	assert.Contains(t, err.Error(), "context deadline exceeded", "should include the underlying cause")
	assert.Contains(t, err.Error(), "reachable", "should hint about reachability/readiness")
	assert.ErrorIs(t, err, cause, "should wrap the cause so errors.Is keeps working")
}

func TestHasNexusServices(t *testing.T) {
	assert.False(t, HasNexusServices(nil))
	assert.False(t, HasNexusServices([]*internal.WorkerInfo{{}}))
	assert.True(t, HasNexusServices([]*internal.WorkerInfo{{
		NexusServices: []internal.NexusServiceInfo{{Name: "billing"}},
	}}))
}
