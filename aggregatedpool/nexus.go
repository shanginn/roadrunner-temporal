package aggregatedpool

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/pool/payload"
	"github.com/temporalio/roadrunner-temporal/v5/api"
	"github.com/temporalio/roadrunner-temporal/v5/internal"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.uber.org/zap"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// Wire contract with PHP — must match FailureConverter::NEXUS_OPERATION_ERROR_TYPE_PREFIX.
const nexusOperationErrorTypePrefix = "nexus.OperationError."

const (
	nexusInternalHandlerErrorMessage    = "internal Nexus handler error"
	nexusUnavailableHandlerErrorMessage = "Nexus handler unavailable"
	nexusRequestTimeoutHeader           = "Request-Timeout"
	nexusOperationTimeoutHeader         = "Operation-Timeout"
)

// NexusHandler forwards handler-side Nexus Start/Cancel to PHP via the activity pool.
type NexusHandler struct {
	codec     api.Codec
	pool      api.Pool
	log       *zap.Logger
	namespace string
	endpoint  func(context.Context) string
	// seqID is the wire envelope ID. Invocation IDs are allocated by the shared
	// cancellation registry so plugin resets cannot reuse an ID while an old
	// watcher is still exiting.
	seqID         uint64
	pldPool       *sync.Pool
	cancellations *NexusMethodCancellationRegistry
}

func NewNexusHandler(codec api.Codec, pool api.Pool, log *zap.Logger, namespace string) *NexusHandler {
	return NewNexusHandlerWithCancellationRegistry(
		codec,
		pool,
		log,
		namespace,
		new(NexusMethodCancellationRegistry),
	)
}

// NewNexusHandlerWithCancellationRegistry creates a Nexus handler whose
// invocation-cancellation state can be queried through RoadRunner's local RPC
// service. The registry is deliberately outside the PHP worker pool: a pool
// Exec cannot target the process already executing Start, and a one-worker pool
// cannot receive another Exec until Start returns.
func NewNexusHandlerWithCancellationRegistry(
	codec api.Codec,
	pool api.Pool,
	log *zap.Logger,
	namespace string,
	cancellations *NexusMethodCancellationRegistry,
) *NexusHandler {
	if cancellations == nil {
		cancellations = new(NexusMethodCancellationRegistry)
	}

	return &NexusHandler{
		codec:         codec,
		pool:          pool,
		log:           log,
		namespace:     namespace,
		endpoint:      nexusEndpoint,
		cancellations: cancellations,
		pldPool: &sync.Pool{
			New: func() any {
				return new(payload.Payload)
			},
		},
	}
}

type nexusOperation struct {
	nexus.UnimplementedOperation[converter.RawValue, converter.RawValue]
	name        string
	serviceName string
	taskQueue   string
	handler     *NexusHandler
}

func (op *nexusOperation) Name() string {
	return op.name
}

func (op *nexusOperation) Start(ctx context.Context, input converter.RawValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[converter.RawValue], error) {
	return op.handler.startOperation(ctx, op.taskQueue, op.serviceName, op.name, input.Payload(), options)
}

func (op *nexusOperation) Cancel(ctx context.Context, token string, options nexus.CancelOperationOptions) error {
	return op.handler.cancelOperation(ctx, op.taskQueue, op.serviceName, op.name, token, options)
}

func nexusEndpoint(ctx context.Context) string {
	if !temporalnexus.IsNexusOperation(ctx) {
		return ""
	}

	return temporalnexus.GetOperationInfo(ctx).Endpoint
}

// CreateNexusService builds a nexus.Service with pass-through operations.
func (h *NexusHandler) CreateNexusService(taskQueue string, serviceName string, operationNames []string) *nexus.Service {
	svc := nexus.NewService(serviceName)
	ops := make([]nexus.RegisterableOperation, 0, len(operationNames))
	for _, name := range operationNames {
		ops = append(ops, &nexusOperation{
			name:        name,
			serviceName: serviceName,
			taskQueue:   taskQueue,
			handler:     h,
		})
	}
	svc.MustRegister(ops...)
	return svc
}

func (h *NexusHandler) startOperation(
	ctx context.Context,
	taskQueue string,
	serviceName string,
	operationName string,
	input *commonpb.Payload,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[converter.RawValue], error) {
	h.log.Debug("nexus start operation", zap.String("service", serviceName), zap.String("operation", operationName), zap.String(tq, taskQueue))

	links := nexusLinksToInternal(options.Links)

	invocationID := h.cancellations.RegisterNew()
	msg := &internal.Message{
		ID: atomic.AddUint64(&h.seqID, 1),
		Command: internal.InvokeNexusOperation{
			Service:         serviceName,
			Operation:       operationName,
			Namespace:       h.namespace,
			TaskQueue:       taskQueue,
			Endpoint:        h.endpoint(ctx),
			RequestID:       options.RequestID,
			Callback:        options.CallbackURL,
			CallbackHeaders: maps.Clone(options.CallbackHeader),
			Headers:         normalizeNexusTimeoutHeaders(ctx, options.Header),
			Links:           links,
			InvocationID:    invocationID,
		},
	}

	if input != nil {
		msg.Payloads = &commonpb.Payloads{Payloads: []*commonpb.Payload{input}}
	}

	// PHP workers are single-request processes and the RoadRunner pool does not
	// expose process-affine dispatch. Store method cancellation in the Go plugin
	// instead; PHP polls it through the independent local RPC transport.
	done := make(chan struct{})
	defer func() {
		h.cancellations.Discard(invocationID)
		close(done)
	}()
	go h.watchForMethodCancel(ctx, invocationID, done)

	// RoadRunner may derive worker allocation and supervisor contexts from the
	// context passed to Pool.Exec. The Nexus method context is intentionally
	// watched above, but must not terminate the PHP process: the PHP handler
	// needs to remain alive long enough to poll and cooperatively observe that
	// cancellation. WithoutCancel preserves request-scoped values while leaving
	// allocation_timeout and supervisor.exec_ttl as the pool's hard bounds.
	r, err := h.roundTrip(context.WithoutCancel(ctx), taskQueue, msg, "nexus request")
	if err != nil {
		return nil, err
	}

	out := make([]*internal.Message, 0, 1)
	if err := h.codec.Decode(r, &out); err != nil {
		return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorNonRetryable, "decode nexus response", err)
	}

	if len(out) != 1 {
		return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorNonRetryable, "invalid nexus worker response", nil)
	}

	return h.decodeStartReply(ctx, out[0])
}

// roundTrip encodes msg, executes it on the worker pool, and returns the raw
// reply payload. Failures come back as *nexus.HandlerError; what distinguishes
// the request kind in messages ("nexus request" / "nexus cancel request").
func (h *NexusHandler) roundTrip(ctx context.Context, taskQueue string, msg *internal.Message, what string) (*payload.Payload, error) {
	pl := h.getPld()
	defer h.putPld(pl)

	if err := h.codec.Encode(&internal.Context{TaskQueue: taskQueue}, pl, msg); err != nil {
		// Encoding our own request is a deterministic local bug, not a transient
		// fault — don't ask the server to retry it.
		return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorNonRetryable, "encode "+what, err)
	}

	ch := make(chan struct{}, 1)
	result, err := h.pool.Exec(ctx, pl, ch)
	if err != nil {
		// Pool returned before queueing — typically pool busy / exec rejected; retryable.
		return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeUnavailable, nexus.HandlerErrorRetryBehaviorRetryable, "exec "+what, err)
	}

	select {
	case pld := <-result:
		if pld.Error() != nil {
			// Worker-side execution failure: retryable per Nexus spec for INTERNAL.
			return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorUnspecified, "nexus worker exec error", pld.Error())
		}
		if pld.Payload().Flags&frame.STREAM != 0 {
			ch <- struct{}{}
			// Streaming worker replies violate the protocol; server-side fault.
			return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorNonRetryable, "streaming is not supported", nil)
		}
		return pld.Payload(), nil
	default:
		// Pool returned a result channel without a value — should not happen on a
		// healthy pool. Treat as transient.
		return nil, h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorRetryable, "nexus worker empty response", nil)
	}
}

// decodeStartReply maps a PHP→Go reply into the SDK start-result shape.
// Variants: *NexusOperationStarted (sync/async), nil+Failure, nil+nil → HandlerError.
func (h *NexusHandler) decodeStartReply(ctx context.Context, retMsg *internal.Message) (nexus.HandlerStartOperationResult[converter.RawValue], error) {
	switch reply := retMsg.Command.(type) {
	case *internal.NexusOperationStarted:
		forwardNexusLinks(ctx, reply.Links, h.log)
		if reply.Async {
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: reply.Token,
			}, nil
		}
		var p *commonpb.Payload
		if pls := retMsg.Payloads.GetPayloads(); len(pls) > 0 {
			p = pls[0]
		}
		return &nexus.HandlerStartOperationResultSync[converter.RawValue]{
			Value: converter.NewRawValue(p),
		}, nil
	case nil:
		if retMsg.Failure != nil {
			return nil, h.nexusErrorFromFailure(retMsg.Failure)
		}
		return nil, h.newNexusHandlerError(
			nexus.HandlerErrorTypeInternal,
			nexus.HandlerErrorRetryBehaviorNonRetryable,
			"nexus worker reply has neither command nor failure", nil)
	default:
		return nil, h.newNexusHandlerError(
			nexus.HandlerErrorTypeInternal,
			nexus.HandlerErrorRetryBehaviorNonRetryable,
			fmt.Sprintf("unexpected nexus reply command %T", retMsg.Command), nil)
	}
}

// nexusLinksToInternal converts SDK links to wire form, dropping URL-less entries.
func nexusLinksToInternal(links []nexus.Link) []internal.NexusLink {
	out := make([]internal.NexusLink, 0, len(links))
	for _, l := range links {
		if l.URL == nil {
			continue
		}
		out = append(out, internal.NexusLink{URL: l.URL.String(), Type: l.Type})
	}
	return out
}

// nexusLinksFromInternal: drop entries with empty url/type or unparseable URL.
func nexusLinksFromInternal(links []internal.NexusLink, log *zap.Logger) []nexus.Link {
	if len(links) == 0 {
		return nil
	}
	out := make([]nexus.Link, 0, len(links))
	for _, l := range links {
		if l.URL == "" || l.Type == "" {
			continue
		}
		u, err := url.Parse(l.URL)
		if err != nil {
			log.Warn("nexus link URL is malformed; skipping", zap.String("url", l.URL), zap.Error(err))
			continue
		}
		out = append(out, nexus.Link{URL: u, Type: l.Type})
	}
	return out
}

// forwardNexusLinks ships valid links to handler ctx; bare ctx → warn+drop.
func forwardNexusLinks(ctx context.Context, links []internal.NexusLink, log *zap.Logger) {
	out := nexusLinksFromInternal(links, log)
	if len(out) == 0 {
		return
	}
	if !nexus.IsHandlerContext(ctx) {
		log.Warn("nexus handler ctx missing; response links dropped", zap.Int("links", len(out)))
		return
	}
	nexus.AddHandlerLinks(ctx, out...)
}

// Cause holds the original proto via failureHolder so SDK-Go's ErrorToFailure
// round-trips it back without losing structure (same contract as activity.go).
func (h *NexusHandler) nexusErrorFromFailure(f *failurepb.Failure) error {
	cause := temporal.GetDefaultFailureConverter().FailureToError(f)

	if nhf := f.GetNexusHandlerFailureInfo(); nhf != nil {
		typ := nexus.HandlerErrorType(nhf.GetType())
		retry := mapNexusRetryBehavior(nhf.GetRetryBehavior())
		if typ == nexus.HandlerErrorTypeInternal || typ == nexus.HandlerErrorTypeUnavailable {
			return h.newNexusHandlerError(typ, retry, "PHP Nexus handler failure: "+f.GetMessage(), cause)
		}
		return &nexus.HandlerError{
			Type:          typ,
			Message:       f.GetMessage(),
			RetryBehavior: retry,
			Cause:         cause,
		}
	}

	if app := f.GetApplicationFailureInfo(); app != nil {
		if t := app.GetType(); strings.HasPrefix(t, nexusOperationErrorTypePrefix) {
			stateStr := strings.TrimPrefix(t, nexusOperationErrorTypePrefix)
			state := nexus.OperationState(stateStr)
			// Spec only allows failed/canceled; coerce anything else.
			if state != nexus.OperationStateFailed && state != nexus.OperationStateCanceled {
				state = nexus.OperationStateFailed
			}
			return &nexus.OperationError{
				State:   state,
				Message: f.GetMessage(),
				Cause:   cause,
			}
		}
	}

	return h.newNexusHandlerError(
		nexus.HandlerErrorTypeInternal,
		nexus.HandlerErrorRetryBehaviorUnspecified,
		"unrecognized PHP Nexus failure: "+f.GetMessage(),
		cause,
	)
}

func mapNexusRetryBehavior(b enumspb.NexusHandlerErrorRetryBehavior) nexus.HandlerErrorRetryBehavior {
	switch b {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED:
		return nexus.HandlerErrorRetryBehaviorUnspecified
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		return nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		return nexus.HandlerErrorRetryBehaviorNonRetryable
	default:
		return nexus.HandlerErrorRetryBehaviorUnspecified
	}
}

func (h *NexusHandler) cancelOperation(
	ctx context.Context,
	taskQueue string,
	serviceName string,
	operationName string,
	token string,
	options nexus.CancelOperationOptions,
) error {
	h.log.Debug("nexus cancel operation", zap.String("service", serviceName), zap.String("operation", operationName), zap.Bool("operation_token_present", token != ""), zap.String(tq, taskQueue))

	invocationID := h.cancellations.RegisterNew()
	msg := &internal.Message{
		ID: atomic.AddUint64(&h.seqID, 1),
		Command: internal.CancelNexusOperation{
			Service:        serviceName,
			Operation:      operationName,
			Namespace:      h.namespace,
			TaskQueue:      taskQueue,
			Endpoint:       h.endpoint(ctx),
			OperationToken: token,
			Headers:        normalizeNexusTimeoutHeaders(ctx, options.Header),
			InvocationID:   invocationID,
		},
	}

	done := make(chan struct{})
	defer func() {
		h.cancellations.Discard(invocationID)
		close(done)
	}()
	go h.watchForMethodCancel(ctx, invocationID, done)

	// Cancel handlers receive the same cooperative method-cancellation contract
	// as Start handlers. Keep the PHP request alive after the original Nexus
	// context is canceled so it can poll and finish its bounded cleanup.
	r, err := h.roundTrip(context.WithoutCancel(ctx), taskQueue, msg, "nexus cancel request")
	if err != nil {
		return err
	}

	return h.decodeCancelReply(r)
}

// normalizeNexusTimeoutHeaders translates Go duration syntax into the Nexus
// timeout wire grammar before handing headers to PHP.
//
// Request-Timeout is bounded by the handler context's current remaining
// deadline, not merely the duration captured when the task was received.
// Operation-Timeout has no separate deadline on the handler context, so its
// shortest supplied value remains authoritative. HTTP header names are
// case-insensitive, but nexus.Header is a Go map and may contain duplicate
// spellings; every timeout is therefore collapsed to one canonical field.
func normalizeNexusTimeoutHeaders(ctx context.Context, headers nexus.Header) nexus.Header {
	return normalizeNexusTimeoutHeadersAt(ctx, headers, time.Now())
}

func normalizeNexusTimeoutHeadersAt(ctx context.Context, headers nexus.Header, now time.Time) nexus.Header {
	normalized := maps.Clone(headers)
	var (
		requestTimeout   time.Duration
		requestFound     bool
		operationTimeout time.Duration
		operationFound   bool
	)

	for name, value := range normalized {
		var (
			timeout *time.Duration
			found   *bool
		)
		switch {
		case strings.EqualFold(name, nexusRequestTimeoutHeader):
			timeout = &requestTimeout
			found = &requestFound
		case strings.EqualFold(name, nexusOperationTimeoutHeader):
			timeout = &operationTimeout
			found = &operationFound
		default:
			continue
		}

		delete(normalized, name)
		duration, err := time.ParseDuration(value)
		if err != nil {
			duration = 0
		}
		if !*found || duration < *timeout {
			*timeout = duration
		}
		*found = true
	}

	if requestFound {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := deadline.Sub(now)
			if remaining < requestTimeout {
				requestTimeout = remaining
			}
		}
		normalized[nexusRequestTimeoutHeader] = formatNexusTimeout(requestTimeout)
	}
	if operationFound {
		normalized[nexusOperationTimeoutHeader] = formatNexusTimeout(operationTimeout)
	}

	return normalized
}

// formatNexusTimeout preserves a time.Duration's nanosecond precision as a
// non-negative decimal millisecond value. It deliberately avoids
// time.Duration.String(), whose µs, ns, h, and negative forms are outside the
// pinned Nexus timeout grammar accepted by the PHP SDK.
func formatNexusTimeout(duration time.Duration) string {
	if duration <= 0 {
		return "0ms"
	}

	milliseconds := duration / time.Millisecond
	nanoseconds := duration % time.Millisecond
	if nanoseconds == 0 {
		return fmt.Sprintf("%dms", milliseconds)
	}

	fraction := strings.TrimRight(fmt.Sprintf("%06d", nanoseconds), "0")
	return fmt.Sprintf("%d.%sms", milliseconds, fraction)
}

// decodeCancelReply maps the PHP cancel reply: always exactly one message; a
// rejected cancel carries a Failure (HandlerException on the PHP side), a
// resolved cancel carries none.
func (h *NexusHandler) decodeCancelReply(r *payload.Payload) error {
	out := make([]*internal.Message, 0, 1)
	if err := h.codec.Decode(r, &out); err != nil {
		return h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorNonRetryable, "decode nexus cancel response", err)
	}

	if len(out) != 1 {
		return h.newNexusHandlerError(nexus.HandlerErrorTypeInternal, nexus.HandlerErrorRetryBehaviorNonRetryable, "invalid nexus worker cancel response", nil)
	}

	if out[0].Failure != nil {
		return h.nexusErrorFromFailure(out[0].Failure)
	}

	return nil
}

// newNexusHandlerError records the actionable detail locally and returns a
// stable external message. Internal causes may contain PHP paths, payload
// fragments, transport addresses, or credentials and must not cross the Nexus
// boundary.
func (h *NexusHandler) newNexusHandlerError(typ nexus.HandlerErrorType, retry nexus.HandlerErrorRetryBehavior, detail string, cause error) *nexus.HandlerError {
	fields := []zap.Field{
		zap.String("type", string(typ)),
		zap.Int("retry_behavior", int(retry)),
		zap.String("detail", detail),
	}
	if cause != nil {
		fields = append(fields, zap.NamedError("cause", cause))
	}
	h.log.Error("nexus handler request failed", fields...)

	message := nexusInternalHandlerErrorMessage
	if typ == nexus.HandlerErrorTypeUnavailable {
		message = nexusUnavailableHandlerErrorMessage
	}

	return &nexus.HandlerError{
		Type:          typ,
		Message:       message,
		RetryBehavior: retry,
	}
}

// watchForMethodCancel records cancellation in a process-independent registry.
// PHP checks the registry via the temporal RPC plugin, so no pool request can
// queue behind the current Start/Cancel call or land on a different PHP process.
func (h *NexusHandler) watchForMethodCancel(ctx context.Context, invocationID uint64, done <-chan struct{}) {
	select {
	case <-ctx.Done():
		reason := "Nexus handler context cancelled" //nolint:misspell // Preserve the established PHP-visible reason.
		if cause := context.Cause(ctx); cause != nil {
			reason = cause.Error()
		}
		h.cancellations.Cancel(invocationID, reason)
	case <-done:
	}
}

func (h *NexusHandler) getPld() *payload.Payload {
	return h.pldPool.Get().(*payload.Payload)
}

func (h *NexusHandler) putPld(pld *payload.Payload) {
	pld.Codec = 0
	pld.Context = nil
	pld.Body = nil
	h.pldPool.Put(pld)
}
