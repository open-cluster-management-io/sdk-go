package clients

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/utils"
)

// recoveringTransport is a CloudEventTransport whose first Receive call returns
// a transient error spontaneously (without firing ErrorChan), and whose
// subsequent Receive calls block until the context is canceled. This models
// the production failure mode where an inbound gRPC subscribe stream returns
// e.g. codes.Canceled / codes.Unavailable while the transport-level connection
// monitor never observes the failure.
type recoveringTransport struct {
	mu             sync.Mutex
	errCh          chan error
	receiveCalls   atomic.Int32
	subscribeCalls atomic.Int32
}

func newRecoveringTransport() *recoveringTransport {
	return &recoveringTransport{errCh: make(chan error, 1)}
}

func (t *recoveringTransport) Connect(ctx context.Context) error { return nil }
func (t *recoveringTransport) Send(ctx context.Context, evt cloudevents.Event) error {
	return nil
}
func (t *recoveringTransport) Subscribe(ctx context.Context) error {
	t.subscribeCalls.Add(1)
	return nil
}

func (t *recoveringTransport) Receive(ctx context.Context, fn options.ReceiveHandlerFn) error {
	n := t.receiveCalls.Add(1)
	if n == 1 {
		// First invocation: return a transient error spontaneously.
		// Mirrors the behavior of the gRPC transport when the inbound
		// subscribe stream is canceled mid-flight without the client-side
		// connection monitor detecting it via ErrorChan.
		return fmt.Errorf("rpc error: code = Canceled desc = context canceled")
	}
	// Subsequent invocations: block until the receiver context is canceled.
	<-ctx.Done()
	return ctx.Err()
}

func (t *recoveringTransport) Close(ctx context.Context) error { return nil }
func (t *recoveringTransport) ErrorChan() <-chan error         { return t.errCh }

// TestReceiverRecoveryAfterSpontaneousReceiveError verifies that when
// transport.Receive returns an error on its own (without the transport's
// ErrorChan firing), the receiver lifecycle does not silently stay dead.
// The baseClient must trigger a resubscribe so that a new Receive goroutine
// is spawned and event processing resumes.
//
// Regression test for the production issue where ARO HCP's aro-hcp-backend
// stopped processing maestro bundle status updates for hours because the
// receiver goroutine launched inside (*baseClient).subscribe exited on a
// transient gRPC Canceled error and was never restarted.
func TestReceiverRecoveryAfterSpontaneousReceiveError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := newRecoveringTransport()
	c := newBaseClient("test-client", tr, utils.EventRateLimit{})

	if err := c.connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}

	c.subscribe(ctx, func(ctx context.Context, evt cloudevents.Event) {})

	// Wait for at least two Receive invocations: the first returns the
	// transient error, the second proves recovery (a new Receive goroutine
	// was spawned via the resubscribe path).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if tr.receiveCalls.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("receiver did not recover: receive=%d subscribe=%d (expected receive>=2)",
		tr.receiveCalls.Load(), tr.subscribeCalls.Load())
}
