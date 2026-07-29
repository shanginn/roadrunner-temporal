package aggregatedpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	rrErrors "github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	goridgePipe "github.com/roadrunner-server/goridge/v3/pkg/pipe"
	"github.com/roadrunner-server/pool/fsm"
	poolPipe "github.com/roadrunner-server/pool/ipc/pipe"
	"github.com/roadrunner-server/pool/payload"
	"github.com/roadrunner-server/pool/pool"
	staticPool "github.com/roadrunner-server/pool/pool/static_pool"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	nexusPoolHelperEnv = "RR_TEMPORAL_NEXUS_POOL_HELPER"
	nexusPoolGateEnv   = "RR_TEMPORAL_NEXUS_POOL_GATE"
)

type nexusPoolExecResult struct {
	body []byte
	err  error
}

// TestNexusPoolMethodCancellationDispatchIsNotWorkerAffine exercises the
// pinned RoadRunner static pool rather than a pool mock. It documents why a
// second pool Exec cannot be used to cancel an in-flight PHP Nexus method:
// there is no free worker in a one-worker pool and another PHP process is
// selected when the pool has spare workers.
func TestNexusPoolMethodCancellationDispatchIsNotWorkerAffine(t *testing.T) {
	t.Run("one worker blocks the cancellation dispatch", func(t *testing.T) {
		workerPool, gate := newNexusStaticPool(t, 1, 50*time.Millisecond)
		startResult := execNexusPoolRequest(workerPool, []byte("block"))
		workingPID := waitForNexusWorkingWorker(t, workerPool)

		_, err := workerPool.Exec(
			context.Background(),
			&payload.Payload{Body: []byte("cancel"), Codec: frame.CodecRaw},
			make(chan struct{}),
		)
		require.Error(t, err)
		require.True(t, rrErrors.Is(rrErrors.NoFreeWorkers, err), "unexpected pool error: %v", err)

		releaseNexusPoolWorker(t, gate)
		result := waitForNexusPoolResult(t, startResult)
		require.NoError(t, result.err)
		require.Equal(t, strconv.FormatInt(workingPID, 10), string(result.body))
	})

	t.Run("spare worker routes cancellation to another process", func(t *testing.T) {
		workerPool, gate := newNexusStaticPool(t, 2, time.Second)
		startResult := execNexusPoolRequest(workerPool, []byte("block"))
		workingPID := waitForNexusWorkingWorker(t, workerPool)

		cancelResponse, err := workerPool.Exec(
			context.Background(),
			&payload.Payload{Body: []byte("cancel"), Codec: frame.CodecRaw},
			make(chan struct{}),
		)
		require.NoError(t, err)

		response := <-cancelResponse
		require.NotNil(t, response)
		require.NoError(t, response.Error())

		cancelPID, err := strconv.ParseInt(string(response.Body()), 10, 64)
		require.NoError(t, err)
		require.NotEqual(t, workingPID, cancelPID)

		releaseNexusPoolWorker(t, gate)
		result := waitForNexusPoolResult(t, startResult)
		require.NoError(t, result.err)
		require.Equal(t, strconv.FormatInt(workingPID, 10), string(result.body))
	})
}

func newNexusStaticPool(
	t *testing.T,
	numWorkers uint64,
	allocateTimeout time.Duration,
) (*staticPool.Pool, string) {
	t.Helper()

	gate := t.TempDir() + "/release"
	command := func([]string) *exec.Cmd {
		cmd := exec.CommandContext( //nolint:gosec // The executable is the current test binary, not user input.
			context.Background(),
			os.Args[0],
			"-test.run=^TestNexusPoolWorkerHelper$",
		)
		cmd.Env = append(
			os.Environ(),
			nexusPoolHelperEnv+"=1",
			nexusPoolGateEnv+"="+gate,
		)
		return cmd
	}

	workerPool, err := staticPool.NewPool(
		context.Background(),
		command,
		poolPipe.NewPipeFactory(zap.NewNop()),
		&pool.Config{
			NumWorkers:      numWorkers,
			AllocateTimeout: allocateTimeout,
			DestroyTimeout:  2 * time.Second,
		},
		zap.NewNop(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.WriteFile(gate, []byte("release"), 0o600)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		workerPool.Destroy(ctx)
	})

	return workerPool, gate
}

func execNexusPoolRequest(workerPool *staticPool.Pool, body []byte) <-chan nexusPoolExecResult {
	result := make(chan nexusPoolExecResult, 1)
	go func() {
		response, err := workerPool.Exec(
			context.Background(),
			&payload.Payload{Body: body, Codec: frame.CodecRaw},
			make(chan struct{}),
		)
		if err != nil {
			result <- nexusPoolExecResult{err: err}
			return
		}

		poolResponse := <-response
		if poolResponse == nil {
			result <- nexusPoolExecResult{err: errors.New("pool returned a nil response")}
			return
		}

		result <- nexusPoolExecResult{
			body: poolResponse.Body(),
			err:  poolResponse.Error(),
		}
	}()

	return result
}

func waitForNexusWorkingWorker(t *testing.T, workerPool *staticPool.Pool) int64 {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, worker := range workerPool.Workers() {
			if worker.State().Compare(fsm.StateWorking) {
				return worker.Pid()
			}
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("no RoadRunner worker entered the working state")
	return 0
}

func releaseNexusPoolWorker(t *testing.T, gate string) {
	t.Helper()
	require.NoError(t, os.WriteFile(gate, []byte("release"), 0o600))
}

func waitForNexusPoolResult(
	t *testing.T,
	result <-chan nexusPoolExecResult,
) nexusPoolExecResult {
	t.Helper()

	select {
	case response := <-result:
		return response
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the RoadRunner worker response")
		return nexusPoolExecResult{}
	}
}

// TestNexusPoolWorkerHelper is a subprocess-backed Goridge worker used by the
// static-pool integration test above.
func TestNexusPoolWorkerHelper(t *testing.T) {
	if os.Getenv(nexusPoolHelperEnv) == "" {
		t.Skip("RoadRunner pool helper subprocess")
	}

	require.NoError(t, runNexusPoolWorker())
}

func runNexusPoolWorker() error {
	relay := goridgePipe.NewPipeRelay(os.Stdin, os.Stdout)

	for {
		request := frame.NewFrame()
		if err := relay.Receive(request); err != nil {
			return err
		}

		if request.ReadFlags()&frame.CONTROL != 0 {
			stop, err := handleNexusPoolControl(relay, request.Payload())
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
			continue
		}

		if string(request.Payload()) == "block" {
			if err := waitForNexusPoolGate(os.Getenv(nexusPoolGateEnv)); err != nil {
				return err
			}
		}

		if err := sendNexusPoolFrame(
			relay,
			request.ReadFlags()&^frame.CONTROL,
			[]byte(strconv.Itoa(os.Getpid())),
			true,
		); err != nil {
			return err
		}
	}
}

func handleNexusPoolControl(relay *goridgePipe.Relay, body []byte) (bool, error) {
	command := struct {
		PID  int  `json:"pid"`
		Stop bool `json:"stop"`
	}{}
	if err := json.Unmarshal(body, &command); err != nil {
		return false, err
	}

	if command.Stop {
		return true, nil
	}
	if command.PID <= 0 {
		return false, fmt.Errorf("unexpected RoadRunner control payload: %s", body)
	}

	response, err := json.Marshal(struct {
		PID int `json:"pid"`
	}{PID: os.Getpid()})
	if err != nil {
		return false, err
	}

	return false, sendNexusPoolFrame(
		relay,
		frame.CONTROL|frame.CodecJSON,
		response,
		false,
	)
}

func sendNexusPoolFrame(
	relay *goridgePipe.Relay,
	flags byte,
	body []byte,
	withBodyOffset bool,
) error {
	response := frame.NewFrame()
	response.WriteVersion(response.Header(), frame.Version1)
	response.WriteFlags(response.Header(), flags)
	if withBodyOffset {
		response.WriteOptions(response.HeaderPtr(), 0)
	}
	response.WritePayloadLen(response.Header(), uint32(len(body))) //nolint:gosec
	response.WritePayload(body)
	response.WriteCRC(response.Header())

	return relay.Send(response)
}

func waitForNexusPoolGate(gate string) error {
	if gate == "" {
		return errors.New("missing RoadRunner pool helper gate")
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// The parent test creates this exact path under t.TempDir and passes it
		// only to its helper subprocess.
		_, err := os.Stat(gate) //nolint:gosec
		if err == nil {
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		time.Sleep(time.Millisecond)
	}

	return errors.New("timed out waiting for RoadRunner pool helper gate")
}
