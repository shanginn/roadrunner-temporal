package aggregatedpool

import (
	"context"
	"errors"
	"maps"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/roadrunner-server/pool/payload"
	staticPool "github.com/roadrunner-server/pool/pool/static_pool"
	poolWorker "github.com/roadrunner-server/pool/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/roadrunner-temporal/v5/internal"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// ── Service registration ──────────────────────────────────────

// putPld must clear the payload before returning it to the pool — otherwise
// stale Body/Context bytes leak into the next Encode call.
func TestNexusHandler_PayloadPoolResetsOnPut(t *testing.T) {
	handler := NewNexusHandler(nil, nil, zap.NewNop(), "default")

	pld := handler.getPld()
	pld.Body = []byte("test")
	pld.Context = []byte("ctx")
	pld.Codec = 7

	handler.putPld(pld)
	assert.Nil(t, pld.Body)
	assert.Nil(t, pld.Context)
	assert.Equal(t, uint8(0), pld.Codec)
}

func TestNexusHandler_CreateNexusService_RegistersOperations(t *testing.T) {
	handler := NewNexusHandler(nil, nil, zap.NewNop(), "default")
	svc := handler.CreateNexusService("tq", "GreetingService", []string{"greet", "farewell"})

	require.NotNil(t, svc.Operation("greet"))
	require.NotNil(t, svc.Operation("farewell"))
	assert.Nil(t, svc.Operation("missing"))
}

func TestNexusHandler_CreateNexusService_AcceptsEmptyAndNilOperations(t *testing.T) {
	handler := NewNexusHandler(nil, nil, zap.NewNop(), "default")
	require.NotPanics(t, func() { handler.CreateNexusService("tq", "S", nil) })
	require.NotPanics(t, func() { handler.CreateNexusService("tq", "S", []string{}) })
}

func TestNexusHandler_CreateNexusService_IsolatesOperationsBetweenServices(t *testing.T) {
	handler := NewNexusHandler(nil, nil, zap.NewNop(), "default")
	a := handler.CreateNexusService("tq", "ServiceA", []string{"opA"})
	b := handler.CreateNexusService("tq", "ServiceB", []string{"opB"})

	assert.NotNil(t, a.Operation("opA"))
	assert.Nil(t, a.Operation("opB"))
	assert.NotNil(t, b.Operation("opB"))
	assert.Nil(t, b.Operation("opA"))
}

// Compile-time guarantee that nexusOperation satisfies the nexus SDK interfaces.
var (
	_ nexus.RegisterableOperation                             = (*nexusOperation)(nil)
	_ nexus.Operation[converter.RawValue, converter.RawValue] = (*nexusOperation)(nil)
)

// TaskQueue from CreateNexusService must reach each operation — that's the
// link the dispatch path follows when a task arrives.
func TestNexusOperation_TaskQueuePropagation(t *testing.T) {
	log := zap.NewNop()
	handler := NewNexusHandler(nil, nil, log, "default")

	taskQueue := "my-special-queue"
	svc := handler.CreateNexusService(taskQueue, "Svc", []string{"op1", "op2", "op3"})
	require.NotNil(t, svc)

	for _, opName := range []string{"op1", "op2", "op3"} {
		op := svc.Operation(opName)
		require.NotNil(t, op)

		concrete, ok := op.(*nexusOperation)
		require.True(t, ok, "operation %q is not *nexusOperation", opName)
		assert.Equal(t, taskQueue, concrete.taskQueue, "operation %q has wrong task queue", opName)
		assert.Equal(t, "Svc", concrete.serviceName, "operation %q has wrong service name", opName)
		assert.Equal(t, opName, concrete.name)
	}
}

// ── Mock codec ─────────────────────────────────────────────

type mockCodec struct {
	encodeCalled int32
	decodeCalled int32
	encodeErr    error
	decodeErr    error
	encodedCtx   *internal.Context
	encodedMsg   *internal.Message
	decodeMsgs   []*internal.Message
}

func (m *mockCodec) Encode(ctx *internal.Context, p *payload.Payload, msgs ...*internal.Message) error {
	atomic.AddInt32(&m.encodeCalled, 1)
	m.encodedCtx = ctx
	if len(msgs) > 0 {
		m.encodedMsg = msgs[0]
	}
	if m.encodeErr != nil {
		return m.encodeErr
	}
	p.Body = []byte("encoded")
	return nil
}

func (m *mockCodec) Decode(p *payload.Payload, msgs *[]*internal.Message) error {
	atomic.AddInt32(&m.decodeCalled, 1)
	if m.decodeErr != nil {
		return m.decodeErr
	}
	*msgs = append(*msgs, m.decodeMsgs...)
	return nil
}

func (m *mockCodec) DecodeWorkerInfo(_ *payload.Payload, _ *[]*internal.WorkerInfo) error {
	return nil
}

// recordingCodec is a thread-safe codec stub that pushes every Encode'd
// message ID into a channel. Used by concurrency tests.
type recordingCodec struct {
	ids     chan<- uint64
	stopErr error
}

func (c *recordingCodec) Encode(_ *internal.Context, _ *payload.Payload, msgs ...*internal.Message) error {
	if len(msgs) > 0 {
		c.ids <- msgs[0].ID
	}
	return c.stopErr
}

func (c *recordingCodec) Decode(_ *payload.Payload, _ *[]*internal.Message) error { return nil }
func (c *recordingCodec) DecodeWorkerInfo(_ *payload.Payload, _ *[]*internal.WorkerInfo) error {
	return nil
}

// recordingPool is the workerless Pool fixture used by caller-side tests.
type recordingPool struct{}

func (*recordingPool) Exec(context.Context, *payload.Payload, chan struct{}) (chan *staticPool.PExec, error) {
	panic("not used")
}
func (*recordingPool) Workers() []*poolWorker.Process     { return nil }
func (*recordingPool) RemoveWorker(context.Context) error { panic("not used") }
func (*recordingPool) AddWorker() error                   { panic("not used") }
func (*recordingPool) QueueSize() uint64                  { return 0 }
func (*recordingPool) Reset(context.Context) error        { panic("not used") }

// ── Encoding behavior tests (no pool needed) ───────────────────────

func TestStartOperation_EncodesTaskQueue(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop after encode"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_, err := handler.startOperation(
		context.Background(),
		"my-task-queue",
		"GreetingService",
		"greet",
		nil,
		nexus.StartOperationOptions{},
	)

	require.Error(t, err)
	assert.NotNil(t, codec.encodedCtx, "Context should be set on encode call")
	assert.Equal(t, "my-task-queue", codec.encodedCtx.TaskQueue)
}

func TestStartOperation_EncodesAllFields(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop after encode"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")
	handler.endpoint = func(context.Context) string { return "orders-endpoint" }

	_, _ = handler.startOperation(
		context.Background(),
		"tq",
		"MyService",
		"myOp",
		nil,
		nexus.StartOperationOptions{
			RequestID:   "req-123",
			CallbackURL: "http://callback.example.com",
			Header: nexus.Header{
				"Content-Type":  "application/json",
				"Authorization": "Bearer xyz",
			},
			CallbackHeader: nexus.Header{
				"X-Token": "callback-token",
			},
		},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok, "Command should be InvokeNexusOperation, got %T", codec.encodedMsg.Command)

	assert.Equal(t, "MyService", cmd.Service)
	assert.Equal(t, "myOp", cmd.Operation)
	assert.Equal(t, "default", cmd.Namespace)
	assert.Equal(t, "tq", cmd.TaskQueue)
	assert.Equal(t, "orders-endpoint", cmd.Endpoint)
	assert.Equal(t, "req-123", cmd.RequestID)
	assert.Equal(t, "http://callback.example.com", cmd.Callback)
	assert.Equal(t, "application/json", cmd.Headers["Content-Type"])
	assert.Equal(t, "Bearer xyz", cmd.Headers["Authorization"])
	assert.Equal(t, "callback-token", cmd.CallbackHeaders["X-Token"])
}

func TestFormatNexusTimeout(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		duration time.Duration
		expected string
	}{
		"expired": {
			duration: -33_542 * time.Nanosecond,
			expected: "0ms",
		},
		"zero": {
			duration: 0,
			expected: "0ms",
		},
		"one nanosecond": {
			duration: time.Nanosecond,
			expected: "0.000001ms",
		},
		"positive sub-millisecond": {
			duration: 999_999 * time.Nanosecond,
			expected: "0.999999ms",
		},
		"whole millisecond": {
			duration: time.Millisecond,
			expected: "1ms",
		},
		"fractional millisecond": {
			duration: time.Millisecond + 500*time.Microsecond,
			expected: "1.5ms",
		},
		"seconds use milliseconds": {
			duration: 59 * time.Second,
			expected: "59000ms",
		},
		"minutes use milliseconds": {
			duration: 2 * time.Minute,
			expected: "120000ms",
		},
		"hours use milliseconds": {
			duration: time.Hour,
			expected: "3600000ms",
		},
		"trailing zeroes are removed": {
			duration: time.Millisecond + 100*time.Nanosecond,
			expected: "1.0001ms",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expected, formatNexusTimeout(test.duration))
		})
	}
}

func TestNormalizeNexusTimeoutHeaders(t *testing.T) {
	t.Parallel()

	t.Run("no timeout headers", func(t *testing.T) {
		headers := nexus.Header{"X-Trace-ID": "trace-1"}
		assert.Equal(t, headers, normalizeNexusTimeoutHeaders(context.Background(), headers))
	})

	t.Run("actual request deadline and operation budget become decimal milliseconds", func(t *testing.T) {
		now := time.Now()
		ctx, cancel := context.WithDeadline(context.Background(), now.Add(123_456_789*time.Nanosecond))
		defer cancel()
		headers := nexus.Header{
			"request-timeout":   "1h",
			"Request-Timeout":   "2h",
			"operation-timeout": "1h",
			"OPERATION-TIMEOUT": "999.999µs",
			"X-Trace-ID":        "trace-1",
		}
		original := maps.Clone(headers)

		assert.Equal(t, nexus.Header{
			"Request-Timeout":   "123.456789ms",
			"Operation-Timeout": "0.999999ms",
			"X-Trace-ID":        "trace-1",
		}, normalizeNexusTimeoutHeadersAt(ctx, headers, now))
		assert.Equal(t, original, headers, "normalization must not mutate SDK-owned headers")
	})

	t.Run("expired and malformed budgets fail closed", func(t *testing.T) {
		now := time.Now()
		ctx, cancel := context.WithDeadline(context.Background(), now.Add(-time.Nanosecond))
		defer cancel()
		headers := nexus.Header{
			"request-timeout":   "not-a-duration",
			"Request-Timeout":   "1h",
			"operation-timeout": "-33.542µs",
		}

		assert.Equal(t, nexus.Header{
			"Request-Timeout":   "0ms",
			"Operation-Timeout": "0ms",
		}, normalizeNexusTimeoutHeadersAt(ctx, headers, now))
	})
}

func TestStartOperation_NormalizesTimeoutHeaders(t *testing.T) {
	codec := &mockCodec{encodeErr: errors.New("stop after encode")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	headers := nexus.Header{
		"Request-Timeout":   "1h",
		"request-timeout":   "2h",
		"operation-timeout": "-1ns",
		"X-Trace-ID":        "trace-1",
	}
	original := maps.Clone(headers)

	_, _ = handler.startOperation(
		ctx,
		"tq",
		"MyService",
		"myOp",
		nil,
		nexus.StartOperationOptions{Header: headers},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok)
	assert.Equal(t, map[string]string{
		"Request-Timeout":   "0ms",
		"Operation-Timeout": "0ms",
		"X-Trace-ID":        "trace-1",
	}, cmd.Headers)
	assert.Equal(t, original, headers, "encoding must not mutate SDK-owned headers")
}

func TestStartOperation_EncodesPayload(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop after encode"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	input := &commonpb.Payload{
		Data:     []byte("hello"),
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
	}

	_, _ = handler.startOperation(
		context.Background(),
		"tq",
		"Svc",
		"op",
		input,
		nexus.StartOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	require.NotNil(t, codec.encodedMsg.Payloads)
	require.Len(t, codec.encodedMsg.Payloads.Payloads, 1)
	assert.Equal(t, []byte("hello"), codec.encodedMsg.Payloads.Payloads[0].Data)
}

func TestStartOperation_NilInputProducesNilPayloads(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop after encode"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_, _ = handler.startOperation(
		context.Background(),
		"tq",
		"Svc",
		"op",
		nil,
		nexus.StartOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	assert.Nil(t, codec.encodedMsg.Payloads, "Payloads should be nil when input is nil")
}

func TestStartOperation_EncodeErrorReturnsError(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("boom"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	result, err := handler.startOperation(
		context.Background(),
		"tq",
		"Svc",
		"op",
		nil,
		nexus.StartOperationOptions{},
	)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, nexusInternalHandlerErrorMessage, err.(*nexus.HandlerError).Message)
	assert.NotContains(t, err.Error(), "boom")
}

func TestStartOperation_IncrementsSeqID(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_, _ = handler.startOperation(context.Background(), "tq", "S", "o", nil, nexus.StartOperationOptions{})
	firstID := codec.encodedMsg.ID

	_, _ = handler.startOperation(context.Background(), "tq", "S", "o", nil, nexus.StartOperationOptions{})
	secondID := codec.encodedMsg.ID

	assert.Greater(t, secondID, firstID, "seqID should increment between calls")
}

func TestStartOperation_EncodesCallerLinks(t *testing.T) {
	codec := &mockCodec{encodeErr: errors.New("stop")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	u1, err := url.Parse("https://caller.example/res/1")
	require.NoError(t, err)
	u2, err := url.Parse("https://caller.example/res/2")
	require.NoError(t, err)

	_, _ = handler.startOperation(context.Background(), "tq", "S", "op", nil, nexus.StartOperationOptions{
		Links: []nexus.Link{
			{URL: u1, Type: "example.one"},
			{URL: u2, Type: "example.two"},
		},
	})

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok, "Command should be InvokeNexusOperation, got %T", codec.encodedMsg.Command)
	require.Len(t, cmd.Links, 2)
	assert.Equal(t, "https://caller.example/res/1", cmd.Links[0].URL)
	assert.Equal(t, "example.one", cmd.Links[0].Type)
	assert.Equal(t, "https://caller.example/res/2", cmd.Links[1].URL)
	assert.Equal(t, "example.two", cmd.Links[1].Type)
}

func TestStartOperation_NoLinksOmitsField(t *testing.T) {
	codec := &mockCodec{encodeErr: errors.New("stop")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_, _ = handler.startOperation(context.Background(), "tq", "S", "op", nil, nexus.StartOperationOptions{})

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok, "Command should be InvokeNexusOperation, got %T", codec.encodedMsg.Command)
	assert.Empty(t, cmd.Links, "Links should be empty when options.Links is nil")
}

// ── Cancel encoding tests ──────────────────────────────────────

func TestCancelOperation_EncodesTaskQueue(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop after encode"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	err := handler.cancelOperation(
		context.Background(),
		"my-tq",
		"Svc",
		"op",
		"token-xyz",
		nexus.CancelOperationOptions{},
	)

	require.Error(t, err)
	require.NotNil(t, codec.encodedCtx)
	assert.Equal(t, "my-tq", codec.encodedCtx.TaskQueue)
}

func TestCancelOperation_EncodesAllFields(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")
	handler.endpoint = func(context.Context) string { return "orders-endpoint" }

	_ = handler.cancelOperation(
		context.Background(),
		"tq",
		"GreetingService",
		"greet",
		"async-token-123",
		nexus.CancelOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.CancelNexusOperation)
	require.True(t, ok, "Command should be CancelNexusOperation, got %T", codec.encodedMsg.Command)

	assert.Equal(t, "GreetingService", cmd.Service)
	assert.Equal(t, "greet", cmd.Operation)
	assert.Equal(t, "default", cmd.Namespace)
	assert.Equal(t, "tq", cmd.TaskQueue)
	assert.Equal(t, "orders-endpoint", cmd.Endpoint)
	assert.Equal(t, "async-token-123", cmd.OperationToken)
	assert.NotZero(t, cmd.InvocationID)
}

// TestCancelOperation_ExtractsHeaders verifies the caller's cancel-request
// headers are pulled off nexus.CancelOperationOptions.Header onto the command,
// symmetric with startOperation. The PHP CancelNexusOperation router reads these
// under `headers` and surfaces them on the handler's OperationContext.
func TestCancelOperation_ExtractsHeaders(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_ = handler.cancelOperation(
		context.Background(),
		"tq",
		"GreetingService",
		"greet",
		"async-token-123",
		nexus.CancelOperationOptions{
			Header: nexus.Header{
				"X-Nexus-Trace-Id": "trace-1",
				"Authorization":    "Bearer xyz",
			},
		},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.CancelNexusOperation)
	require.True(t, ok, "Command should be CancelNexusOperation, got %T", codec.encodedMsg.Command)

	assert.Equal(t, "trace-1", cmd.Headers["X-Nexus-Trace-Id"])
	assert.Equal(t, "Bearer xyz", cmd.Headers["Authorization"])
}

func TestCancelOperation_NormalizesTimeoutHeaders(t *testing.T) {
	codec := &mockCodec{encodeErr: errors.New("stop")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	headers := nexus.Header{
		"request-timeout": "750µs",
		"REQUEST-TIMEOUT": "1h",
		"X-Trace-ID":      "trace-1",
	}
	original := maps.Clone(headers)

	_ = handler.cancelOperation(
		ctx,
		"tq",
		"GreetingService",
		"greet",
		"async-token-123",
		nexus.CancelOperationOptions{Header: headers},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.CancelNexusOperation)
	require.True(t, ok)
	assert.Equal(t, map[string]string{
		"Request-Timeout": "0ms",
		"X-Trace-ID":      "trace-1",
	}, cmd.Headers)
	assert.Equal(t, original, headers, "encoding must not mutate SDK-owned headers")
}

// TestStartOperation_EncodesNamespace verifies the handler's configured
// namespace lands on the InvokeNexusOperation command, where PHP reads it as
// $options['namespace']. Sourced from plugin config, not from internal.Context.
func TestStartOperation_EncodesNamespace(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop after encode"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "test-ns")

	_, _ = handler.startOperation(
		context.Background(),
		"tq",
		"MyService",
		"myOp",
		nil,
		nexus.StartOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok, "Command should be InvokeNexusOperation, got %T", codec.encodedMsg.Command)
	assert.Equal(t, "test-ns", cmd.Namespace)
}

// TestCancelOperation_EncodesNamespace is the cancel-side counterpart.
func TestCancelOperation_EncodesNamespace(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("stop"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "test-ns")

	_ = handler.cancelOperation(
		context.Background(),
		"tq",
		"GreetingService",
		"greet",
		"async-token-123",
		nexus.CancelOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.CancelNexusOperation)
	require.True(t, ok, "Command should be CancelNexusOperation, got %T", codec.encodedMsg.Command)
	assert.Equal(t, "test-ns", cmd.Namespace)
}

// ── Cancel reply decoding ───────────────────────────────────────

// A PHP-side rejection (e.g. HandlerException NOT_IMPLEMENTED for a manual-token
// operation without an #[OperationCancel] routine) must surface as a handler
// error instead of being swallowed as cancel success.
func TestDecodeCancelReply_FailurePropagatesHandlerError(t *testing.T) {
	codec := &mockCodec{
		decodeMsgs: []*internal.Message{
			{
				Failure: &failurepb.Failure{
					Message: "cancellation is not supported",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type:          "NOT_IMPLEMENTED",
							RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
						},
					},
				},
			},
		},
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	err := handler.decodeCancelReply(&payload.Payload{})

	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok, "expected *nexus.HandlerError, got %T", err)
	assert.Equal(t, nexus.HandlerErrorTypeNotImplemented, he.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, he.RetryBehavior)
	assert.Equal(t, "cancellation is not supported", he.Message)
}

func TestDecodeCancelReply_NoFailureMeansSuccess(t *testing.T) {
	codec := &mockCodec{
		decodeMsgs: []*internal.Message{{}},
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	assert.NoError(t, handler.decodeCancelReply(&payload.Payload{}))
}

// PHP always replies with exactly one message; an empty reply is a protocol
// fault, not cancel success.
func TestDecodeCancelReply_EmptyReplyIsProtocolFault(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	err := handler.decodeCancelReply(&payload.Payload{})

	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok, "expected *nexus.HandlerError, got %T", err)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, he.RetryBehavior)
}

func TestDecodeCancelReply_DecodeErrorIsInternalNonRetryable(t *testing.T) {
	codec := &mockCodec{decodeErr: errors.New("bad frame")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	err := handler.decodeCancelReply(&payload.Payload{})

	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok, "expected *nexus.HandlerError, got %T", err)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, he.RetryBehavior)
}

func TestCancelOperation_EncodeError(t *testing.T) {
	codec := &mockCodec{
		encodeErr: errors.New("encode failed"),
	}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	err := handler.cancelOperation(context.Background(), "tq", "S", "o", "t", nexus.CancelOperationOptions{})
	require.Error(t, err)
	assert.Equal(t, nexusInternalHandlerErrorMessage, err.(*nexus.HandlerError).Message)
	assert.NotContains(t, err.Error(), "encode failed")
}

// ── Concurrent seqID test ──────────────────────────────────────

func TestStartOperation_ConcurrentSeqIDIncrement(t *testing.T) {
	const goroutines = 20

	idCh := make(chan uint64, goroutines)
	codec := &recordingCodec{ids: idCh, stopErr: errors.New("stop")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = handler.startOperation(context.Background(), "tq", "S", "o", nil, nexus.StartOperationOptions{})
		}()
	}
	wg.Wait()
	close(idCh)

	unique := make(map[uint64]struct{}, goroutines)
	for id := range idCh {
		assert.Greater(t, id, uint64(0))
		unique[id] = struct{}{}
	}
	assert.Equal(t, goroutines, len(unique), "every concurrent startOperation must get a unique seqID")
}

// ── Method cancellation tests ──────────────────────────────────

// InvocationID is the correlation key PHP uses to poll RoadRunner's
// process-independent method-cancellation registry.
func TestStartOperation_SetsInvocationID(t *testing.T) {
	codec := &mockCodec{encodeErr: errors.New("stop")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_, _ = handler.startOperation(
		context.Background(), "tq", "S", "op",
		nil, nexus.StartOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok)
	assert.NotZero(t, cmd.InvocationID)
}

func TestCancelOperation_SetsInvocationID(t *testing.T) {
	codec := &mockCodec{encodeErr: errors.New("stop")}
	handler := NewNexusHandler(codec, nil, zap.NewNop(), "default")

	_ = handler.cancelOperation(
		context.Background(), "tq", "S", "op",
		"token", nexus.CancelOperationOptions{},
	)

	require.NotNil(t, codec.encodedMsg)
	cmd, ok := codec.encodedMsg.Command.(internal.CancelNexusOperation)
	require.True(t, ok)
	assert.NotZero(t, cmd.InvocationID)
}

// ── Method cancellation registry ───────────────────────────────

func TestStartOperation_CtxCancelMarksSharedRegistry(t *testing.T) {
	registry := new(NexusMethodCancellationRegistry)
	registry.Register(5)
	handler := NewNexusHandlerWithCancellationRegistry(nil, nil, zap.NewNop(), "default", registry)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	finished := make(chan struct{})
	go func() {
		handler.watchForMethodCancel(ctx, 5, done)
		close(finished)
	}()

	cancel()
	<-finished

	state, ok := registry.Lookup(5)
	require.True(t, ok)
	assert.True(t, state.Cancelled)
	assert.Contains(t, state.Reason, "canceled")
}

func TestStartOperation_DoneClosedSkipsMethodCancel(t *testing.T) {
	registry := new(NexusMethodCancellationRegistry)
	registry.Register(5)
	handler := NewNexusHandlerWithCancellationRegistry(nil, nil, zap.NewNop(), "default", registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})

	finished := make(chan struct{})
	go func() {
		handler.watchForMethodCancel(ctx, 5, done)
		close(finished)
	}()

	close(done)
	<-finished

	state, ok := registry.Lookup(5)
	require.True(t, ok)
	assert.False(t, state.Cancelled)
}

// Race guard: ctx cancels after the invocation was discarded. The watcher must
// not recreate completed state.
func TestStartOperation_CtxCancelAfterCompletionNoop(t *testing.T) {
	registry := new(NexusMethodCancellationRegistry)
	handler := NewNexusHandlerWithCancellationRegistry(nil, nil, zap.NewNop(), "default", registry)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	finished := make(chan struct{})
	go func() {
		handler.watchForMethodCancel(ctx, 777, done)
		close(finished)
	}()

	cancel()
	<-finished

	_, ok := registry.Lookup(777)
	assert.False(t, ok)
}

// blockingPool models both failure modes of the old second-Exec design:
// with one PHP worker it queues behind the active handler; with more workers
// it is free to select a different process whose PHP-local registry cannot
// know the ID.
type blockingPool struct {
	execCalls   int32
	started     chan struct{}
	release     chan struct{}
	secondExec  chan struct{}
	execContext chan context.Context
}

func newBlockingPool() *blockingPool {
	return &blockingPool{
		started:     make(chan struct{}),
		release:     make(chan struct{}),
		secondExec:  make(chan struct{}),
		execContext: make(chan context.Context, 1),
	}
}

func (p *blockingPool) Exec(ctx context.Context, _ *payload.Payload, _ chan struct{}) (chan *staticPool.PExec, error) {
	call := atomic.AddInt32(&p.execCalls, 1)
	if call == 1 {
		p.execContext <- ctx
		close(p.started)
		<-p.release
		return nil, errors.New("release blocked handler")
	}

	select {
	case <-p.secondExec:
	default:
		close(p.secondExec)
	}
	return nil, errors.New("unexpected non-affine Exec")
}

func (p *blockingPool) Workers() []*poolWorker.Process     { return nil }
func (p *blockingPool) RemoveWorker(context.Context) error { panic("not used") }
func (p *blockingPool) AddWorker() error                   { panic("not used") }
func (p *blockingPool) QueueSize() uint64                  { return 0 }
func (p *blockingPool) Reset(context.Context) error        { panic("not used") }

func TestStartOperation_ContextCancelNeverDispatchesSecondPoolRequest(t *testing.T) {
	codec := &mockCodec{}
	pool := newBlockingPool()
	registry := new(NexusMethodCancellationRegistry)
	handler := NewNexusHandlerWithCancellationRegistry(codec, pool, zap.NewNop(), "default", registry)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := handler.startOperation(ctx, "tq", "S", "o", nil, nexus.StartOperationOptions{})
		result <- err
	}()

	<-pool.started
	execCtx := <-pool.execContext
	cmd, ok := codec.encodedMsg.Command.(internal.InvokeNexusOperation)
	require.True(t, ok)
	cancel()

	assert.NoError(t, execCtx.Err(), "method cancellation must not cancel the PHP pool dispatch")
	assert.Nil(t, execCtx.Done(), "Start must use a detached pool context")
	_, hasDeadline := execCtx.Deadline()
	assert.False(t, hasDeadline, "the pool owns allocation and execution hard limits")

	require.Eventually(t, func() bool {
		state, found := registry.Lookup(cmd.InvocationID)
		return found && state.Cancelled
	}, time.Second, time.Millisecond)

	select {
	case <-pool.secondExec:
		t.Fatal("context cancellation dispatched a second, non-process-affine pool request")
	case <-time.After(50 * time.Millisecond):
	}
	assert.EqualValues(t, 1, atomic.LoadInt32(&pool.execCalls))

	close(pool.release)
	require.Error(t, <-result)

	_, found := registry.Lookup(cmd.InvocationID)
	assert.False(t, found, "Start completion must discard registry state")
}

func TestCancelOperation_ContextCancelMarksRegistryAndKeepsPoolDispatchAlive(t *testing.T) {
	codec := &mockCodec{}
	pool := newBlockingPool()
	registry := new(NexusMethodCancellationRegistry)
	handler := NewNexusHandlerWithCancellationRegistry(codec, pool, zap.NewNop(), "default", registry)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- handler.cancelOperation(ctx, "tq", "S", "o", "token", nexus.CancelOperationOptions{})
	}()

	<-pool.started
	execCtx := <-pool.execContext
	cmd, ok := codec.encodedMsg.Command.(internal.CancelNexusOperation)
	require.True(t, ok)
	cancel()

	assert.NoError(t, execCtx.Err(), "method cancellation must not cancel the PHP pool dispatch")
	assert.Nil(t, execCtx.Done(), "Cancel must use a detached pool context")
	_, hasDeadline := execCtx.Deadline()
	assert.False(t, hasDeadline, "the pool owns allocation and execution hard limits")

	require.Eventually(t, func() bool {
		state, found := registry.Lookup(cmd.InvocationID)
		return found && state.Cancelled
	}, time.Second, time.Millisecond)

	select {
	case <-pool.secondExec:
		t.Fatal("context cancellation dispatched a second, non-process-affine pool request")
	case <-time.After(50 * time.Millisecond):
	}
	assert.EqualValues(t, 1, atomic.LoadInt32(&pool.execCalls))

	close(pool.release)
	require.Error(t, <-result)

	_, found := registry.Lookup(cmd.InvocationID)
	assert.False(t, found, "Cancel completion must discard registry state")
}

// ── Failure → Nexus error mapping ──────────────────────────────────

func nexusErrorFromFailureForTest(f *failurepb.Failure) error {
	return NewNexusHandler(nil, nil, zap.NewNop(), "default").nexusErrorFromFailure(f)
}

func TestNexusErrorFromFailure_HandlerFailureInfoPreservesType(t *testing.T) {
	f := &failurepb.Failure{
		Message: "payload parsing failed",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "BAD_REQUEST",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			},
		},
	}

	err := nexusErrorFromFailureForTest(f)

	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok, "expected *nexus.HandlerError, got %T", err)
	assert.Equal(t, nexus.HandlerErrorTypeBadRequest, he.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, he.RetryBehavior)
	assert.Equal(t, "payload parsing failed", he.Message)
}

func TestNexusErrorFromFailure_RetryBehaviorRetryable(t *testing.T) {
	f := &failurepb.Failure{
		Message: "try again",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "INTERNAL",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			},
		},
	}

	err := nexusErrorFromFailureForTest(f)
	he := err.(*nexus.HandlerError)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorRetryable, he.RetryBehavior)
}

func TestNexusErrorFromFailure_RetryBehaviorUnspecifiedDefaults(t *testing.T) {
	f := &failurepb.Failure{
		Message: "",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type: "NOT_FOUND",
			},
		},
	}

	he := nexusErrorFromFailureForTest(f).(*nexus.HandlerError)
	assert.Equal(t, nexus.HandlerErrorTypeNotFound, he.Type)
	assert.Equal(t, nexus.HandlerErrorRetryBehaviorUnspecified, he.RetryBehavior)
}

func TestNexusErrorFromFailure_AllSpecErrorTypesRoundTrip(t *testing.T) {
	cases := []struct {
		wire string
		want nexus.HandlerErrorType
	}{
		{"BAD_REQUEST", nexus.HandlerErrorTypeBadRequest},
		{"UNAUTHENTICATED", nexus.HandlerErrorTypeUnauthenticated},
		{"UNAUTHORIZED", nexus.HandlerErrorTypeUnauthorized},
		{"NOT_FOUND", nexus.HandlerErrorTypeNotFound},
		{"REQUEST_TIMEOUT", nexus.HandlerErrorTypeRequestTimeout},
		{"CONFLICT", nexus.HandlerErrorTypeConflict},
		{"RESOURCE_EXHAUSTED", nexus.HandlerErrorTypeResourceExhausted},
		{"INTERNAL", nexus.HandlerErrorTypeInternal},
		{"NOT_IMPLEMENTED", nexus.HandlerErrorTypeNotImplemented},
		{"UNAVAILABLE", nexus.HandlerErrorTypeUnavailable},
		{"UPSTREAM_TIMEOUT", nexus.HandlerErrorTypeUpstreamTimeout},
	}

	for _, c := range cases {
		t.Run(c.wire, func(t *testing.T) {
			f := &failurepb.Failure{
				Message: c.wire,
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type: c.wire,
					},
				},
			}
			he := nexusErrorFromFailureForTest(f).(*nexus.HandlerError)
			assert.Equal(t, c.want, he.Type)
		})
	}
}

func TestNexusErrorFromFailure_OperationErrorFailed(t *testing.T) {
	f := &failurepb.Failure{
		Message: "user rejected",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type: "nexus.OperationError.failed",
			},
		},
	}

	err := nexusErrorFromFailureForTest(f)
	oe, ok := err.(*nexus.OperationError)
	require.True(t, ok, "expected *nexus.OperationError, got %T", err)
	assert.Equal(t, nexus.OperationStateFailed, oe.State)
	assert.Equal(t, "user rejected", oe.Message)
}

func TestNexusErrorFromFailure_OperationErrorCanceled(t *testing.T) {
	f := &failurepb.Failure{
		Message: "user canceled",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type: "nexus.OperationError.canceled",
			},
		},
	}

	oe := nexusErrorFromFailureForTest(f).(*nexus.OperationError)
	assert.Equal(t, nexus.OperationStateCanceled, oe.State)
	assert.Equal(t, "user canceled", oe.Message)
}

func TestNexusErrorFromFailure_OperationErrorUnknownStateFallsBackToFailed(t *testing.T) {
	f := &failurepb.Failure{
		Message: "weird",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type: "nexus.OperationError.weird",
			},
		},
	}

	oe := nexusErrorFromFailureForTest(f).(*nexus.OperationError)
	assert.Equal(t, nexus.OperationStateFailed, oe.State, "unknown state must not leak to the wire")
}

func TestNexusErrorFromFailure_UntaggedApplicationFailureFallsBackToInternal(t *testing.T) {
	f := &failurepb.Failure{
		Message: "boom",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type: "SomeUserType",
			},
		},
	}

	he, ok := nexusErrorFromFailureForTest(f).(*nexus.HandlerError)
	require.True(t, ok)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type, "unknown failure shape must collapse to Internal")
	assert.Equal(t, nexusInternalHandlerErrorMessage, he.Message)
	assert.Nil(t, he.Cause)
}

func TestNexusErrorFromFailure_NoFailureInfoIsInternal(t *testing.T) {
	f := &failurepb.Failure{Message: "bare failure"}

	he, ok := nexusErrorFromFailureForTest(f).(*nexus.HandlerError)
	require.True(t, ok)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
	assert.Equal(t, nexusInternalHandlerErrorMessage, he.Message)
	assert.Nil(t, he.Cause)
}

// ── RetryBehavior enum mapping ──────────────────────────────────────

func TestMapNexusRetryBehavior_AllValues(t *testing.T) {
	cases := []struct {
		in   enumspb.NexusHandlerErrorRetryBehavior
		want nexus.HandlerErrorRetryBehavior
	}{
		{enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED, nexus.HandlerErrorRetryBehaviorUnspecified},
		{enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE, nexus.HandlerErrorRetryBehaviorRetryable},
		{enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE, nexus.HandlerErrorRetryBehaviorNonRetryable},
	}

	for _, c := range cases {
		t.Run(c.in.String(), func(t *testing.T) {
			got := mapNexusRetryBehavior(c.in)
			assert.Equal(t, c.want, got)
		})
	}
}

// ── Failure-cause preservation (round-trip via failureHolder) ──

func TestNexusErrorFromFailure_PublicHandlerErrorPreservesCauseProto(t *testing.T) {
	f := &failurepb.Failure{
		Message:    "boom",
		StackTrace: "#0 /app/Handler.php(42): run()\n#1 {main}",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "BAD_REQUEST",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			},
		},
	}

	he := nexusErrorFromFailureForTest(f).(*nexus.HandlerError)
	assert.Equal(t, "boom", he.Message)

	roundTripped := temporal.GetDefaultFailureConverter().ErrorToFailure(he.Cause)
	assert.True(t, proto.Equal(f, roundTripped),
		"Cause must hold the original proto verbatim;\nwant: %v\ngot:  %v", f, roundTripped)
}

func TestNexusErrorFromFailure_PublicHandlerErrorPreservesNestedCauseProto(t *testing.T) {
	inner := &failurepb.Failure{
		Message:    "db connection failed",
		StackTrace: "at Db->connect()",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "PDOException"},
		},
	}
	outer := &failurepb.Failure{
		Message:    "handler failed",
		StackTrace: "at EchoService->echo()",
		Cause:      inner,
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "BAD_REQUEST"},
		},
	}

	he := nexusErrorFromFailureForTest(outer).(*nexus.HandlerError)
	roundTripped := temporal.GetDefaultFailureConverter().ErrorToFailure(he.Cause)
	assert.True(t, proto.Equal(outer, roundTripped),
		"recursive cause chain must survive round-trip;\nwant: %v\ngot:  %v", outer, roundTripped)
}

func TestNexusErrorFromFailure_InternalCauseIsLoggedAndHidden(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	handler := NewNexusHandler(nil, nil, zap.New(core), "default")
	f := &failurepb.Failure{
		Message:    "database password=secret",
		StackTrace: "/srv/private/Handler.php:42",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "INTERNAL"},
		},
	}

	he := handler.nexusErrorFromFailure(f).(*nexus.HandlerError)

	assert.Equal(t, nexusInternalHandlerErrorMessage, he.Message)
	assert.NotContains(t, he.Error(), "password=secret")
	assert.Nil(t, he.Cause)
	require.Len(t, logs.All(), 1)
	assert.Contains(t, logs.All()[0].ContextMap()["detail"], "password=secret")
	assert.Contains(t, logs.All()[0].ContextMap()["cause"], "password=secret")
}

func TestNexusErrorFromFailure_OperationErrorPreservesCauseProto(t *testing.T) {
	innerDetails := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     []byte(`"detail-payload-marker"`),
		}},
	}
	innerCause := &failurepb.Failure{
		Message: "inner-business-message",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:    "CustomBusinessType",
				Details: innerDetails,
			},
		},
	}
	outer := &failurepb.Failure{
		Message:    "outer-business-error",
		StackTrace: "at OrderService->process()",
		Cause:      innerCause,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         nexusOperationErrorTypePrefix + "failed",
				NonRetryable: true,
			},
		},
	}

	oe := nexusErrorFromFailureForTest(outer).(*nexus.OperationError)
	assert.Equal(t, nexus.OperationStateFailed, oe.State)
	assert.Equal(t, "outer-business-error", oe.Message)

	roundTripped := temporal.GetDefaultFailureConverter().ErrorToFailure(oe.Cause)
	assert.True(t, proto.Equal(outer, roundTripped),
		"OperationError.Cause must round-trip with full structure (type, details, recursive cause);\nwant: %v\ngot:  %v",
		outer, roundTripped)
}

// ── nexusLinksFromInternal tests ───────────────────────────────

func TestNexusLinksFromInternal_EmptyInputReturnsNil(t *testing.T) {
	assert.Nil(t, nexusLinksFromInternal(nil, zap.NewNop()))
	assert.Nil(t, nexusLinksFromInternal([]internal.NexusLink{}, zap.NewNop()))
}

func TestNexusLinksFromInternal_DropsEntriesWithEmptyFields(t *testing.T) {
	in := []internal.NexusLink{
		{URL: "", Type: "t"},
		{URL: "http://a/", Type: ""},
		{URL: "http://b/", Type: "t"},
	}
	out := nexusLinksFromInternal(in, zap.NewNop())
	require.Len(t, out, 1)
	assert.Equal(t, "http://b/", out[0].URL.String())
	assert.Equal(t, "t", out[0].Type)
}

func TestNexusLinksFromInternal_DropsUnparseableURLs(t *testing.T) {
	in := []internal.NexusLink{
		{URL: "http://[::bad", Type: "t"},
		{URL: "http://ok/", Type: "t"},
	}
	out := nexusLinksFromInternal(in, zap.NewNop())
	require.Len(t, out, 1)
	assert.Equal(t, "http://ok/", out[0].URL.String())
}

func TestNexusLinksFromInternal_PreservesOrderingAndFields(t *testing.T) {
	in := []internal.NexusLink{
		{URL: "http://a/", Type: "x.one"},
		{URL: "http://b/", Type: "x.two"},
	}
	out := nexusLinksFromInternal(in, zap.NewNop())
	require.Len(t, out, 2)
	assert.Equal(t, "http://a/", out[0].URL.String())
	assert.Equal(t, "x.one", out[0].Type)
	assert.Equal(t, "http://b/", out[1].URL.String())
	assert.Equal(t, "x.two", out[1].Type)
}

// ── decodeStartReply tests ─────────────────────────────────────

// Sync reply: Command=*NexusOperationStarted{Async:false}, Payloads carries
// the result. Decoder must wrap it as HandlerStartOperationResultSync with
// the payload preserved on the RawValue.
func TestDecodeStartReply_SyncSuccessUnwrapsPayload(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	resultPayload := &commonpb.Payload{
		Data:     []byte(`{"ok":true}`),
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
	}
	msg := &internal.Message{
		Command: &internal.NexusOperationStarted{
			Async: false,
			Links: []internal.NexusLink{{URL: "http://x/y", Type: "t"}},
		},
		Payloads: &commonpb.Payloads{Payloads: []*commonpb.Payload{resultPayload}},
	}

	res, err := handler.decodeStartReply(context.Background(), msg)
	require.NoError(t, err)
	sync, ok := res.(*nexus.HandlerStartOperationResultSync[converter.RawValue])
	require.True(t, ok, "expected HandlerStartOperationResultSync, got %T", res)
	assert.Equal(t, resultPayload, sync.Value.Payload())
}

// Sync reply with empty Payloads slice: still returns a sync result with
// nil Value — matches the pre-refactor empty-payload contract.
func TestDecodeStartReply_SyncSuccessEmptyPayloads(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	msg := &internal.Message{
		Command:  &internal.NexusOperationStarted{Async: false},
		Payloads: &commonpb.Payloads{},
	}

	res, err := handler.decodeStartReply(context.Background(), msg)
	require.NoError(t, err)
	sync, ok := res.(*nexus.HandlerStartOperationResultSync[converter.RawValue])
	require.True(t, ok)
	assert.Nil(t, sync.Value.Payload())
}

// Async reply: Command=*NexusOperationStarted{Async:true, Token}, no Payloads.
// Decoder returns HandlerStartOperationResultAsync with the token preserved.
func TestDecodeStartReply_AsyncSuccessReturnsToken(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	msg := &internal.Message{
		Command: &internal.NexusOperationStarted{
			Async: true,
			Token: "tok-1",
			Links: []internal.NexusLink{{URL: "http://x/y", Type: "t"}},
		},
	}

	res, err := handler.decodeStartReply(context.Background(), msg)
	require.NoError(t, err)
	async, ok := res.(*nexus.HandlerStartOperationResultAsync)
	require.True(t, ok, "expected HandlerStartOperationResultAsync, got %T", res)
	assert.Equal(t, "tok-1", async.OperationToken)
}

// nil Command + Failure set → existing failurepb mapping path. The mapping
// itself is exercised in ── Failure → Nexus error mapping ── above; here we
// verify routing.
func TestDecodeStartReply_NilCommandWithFailureRoutesToMapping(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	msg := &internal.Message{
		Failure: &failurepb.Failure{
			Message: "boom",
			FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
				NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "INTERNAL"},
			},
		},
	}

	res, err := handler.decodeStartReply(context.Background(), msg)
	assert.Nil(t, res)
	require.Error(t, err)
	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok, "expected *nexus.HandlerError, got %T", err)
	assert.Equal(t, nexusInternalHandlerErrorMessage, he.Message)
	assert.NotContains(t, he.Error(), "boom")
}

// nil Command + nil Failure: malformed reply → HandlerError(Internal).
func TestDecodeStartReply_EmptyReplyIsHandlerError(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	res, err := handler.decodeStartReply(context.Background(), &internal.Message{})
	assert.Nil(t, res)
	require.Error(t, err)
	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok, "expected *nexus.HandlerError, got %T", err)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
	assert.Equal(t, nexusInternalHandlerErrorMessage, he.Message)
}

// Unknown reply command → HandlerError(Internal). Defends against PHP
// emitting a command name Go doesn't recognize.
func TestDecodeStartReply_UnknownCommandIsHandlerError(t *testing.T) {
	handler := NewNexusHandler(&mockCodec{}, nil, zap.NewNop(), "default")

	msg := &internal.Message{Command: &internal.CancelNexusOperation{}}

	res, err := handler.decodeStartReply(context.Background(), msg)
	assert.Nil(t, res)
	require.Error(t, err)
	he, ok := err.(*nexus.HandlerError)
	require.True(t, ok)
	assert.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
	assert.Equal(t, nexusInternalHandlerErrorMessage, he.Message)
}
