package grpc

import (
	"context"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	v1 "open-cluster-management.io/api/operator/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	ocmauthn "open-cluster-management.io/sdk-go/pkg/server/grpc/authn"
	grpcspiffe "open-cluster-management.io/sdk-go/pkg/server/grpc/spiffe"
	ocmspiffe "open-cluster-management.io/sdk-go/pkg/spiffe"
	"open-cluster-management.io/sdk-go/pkg/testing/fakespire"
)

const (
	renewalHubID  = "spiffe://ocm-hub.example.org/ocm/hub/grpc-server"
	renewalWorkID = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"
	// reconnectDelay stands in for the base client's DelayFn (5s to 10s for the first retry).
	reconnectDelay = 100 * time.Millisecond
)

// testBroker is a CloudEventService that sends every published event to every open subscribe
// stream. publishGate, when set, blocks Publish until it is closed.
type testBroker struct {
	pbv1.UnimplementedCloudEventServiceServer
	mu          sync.Mutex
	subs        map[chan *pbv1.CloudEvent]struct{}
	publishGate chan struct{}
	publishing  chan struct{}
}

func newTestBroker() *testBroker {
	return &testBroker{subs: map[chan *pbv1.CloudEvent]struct{}{}, publishing: make(chan struct{}, 1)}
}

func (b *testBroker) Publish(ctx context.Context, _ *pbv1.PublishRequest) (*emptypb.Empty, error) {
	b.mu.Lock()
	gate := b.publishGate
	b.mu.Unlock()
	if gate != nil {
		b.publishing <- struct{}{}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &emptypb.Empty{}, nil
}

func (b *testBroker) Subscribe(_ *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
	if err := stream.SendHeader(metadata.Pairs(constants.GRPCSubscriptionIDKey, uuid.NewString())); err != nil {
		return err
	}
	ch := make(chan *pbv1.CloudEvent, 16)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}()
	for {
		select {
		case evt := <-ch:
			if err := stream.Send(evt); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (b *testBroker) subscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// publishFromHub sends an event with id to every subscriber.
func (b *testBroker) publishFromHub(id string) {
	evt := &pbv1.CloudEvent{
		Id:          id,
		Source:      "hub",
		SpecVersion: "1.0",
		Type:        "io.open-cluster-management.works.v1alpha1.manifests.spec.create_request",
		Attributes: map[string]*pbv1.CloudEventAttributeValue{
			"contenttype": {Attr: &pbv1.CloudEventAttributeValue_CeString{CeString: "application/json"}},
		},
		Data: &pbv1.CloudEvent_TextData{TextData: `{}`},
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		ch <- evt
	}
}

type spiffeHub struct {
	ca     *fakespire.CA
	addr   string
	reg    *grpcspiffe.Registry
	broker *testBroker
}

func startSPIFFEHub(t *testing.T) *spiffeHub {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	api := fakespire.Start(t, ca)
	api.SetSVIDs(ca.Issue(t, renewalHubID, time.Hour))
	src, err := ocmspiffe.NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(renewalHubID))
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	scheme, err := ocmauthn.NewIDScheme(v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate,
		[]string{"ocm-hub.example.org"})
	require.NoError(t, err)

	h := &spiffeHub{ca: ca, reg: grpcspiffe.NewRegistry(), broker: newTestBroker()}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	h.addr = lis.Addr().String()
	gs := grpc.NewServer(grpc.Creds(grpcspiffe.NewServerCredentials(src, scheme, h.reg)))
	pbv1.RegisterCloudEventServiceServer(gs, h.broker)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return h
}

// runAgent connects the transport and keeps it subscribed the way the base client does: on a
// transport error it closes the transport, waits, connects and subscribes again. It returns the
// IDs of the events received.
func runAgent(ctx context.Context, t *testing.T, tr *grpcTransport) <-chan string {
	t.Helper()
	events := make(chan string, 16)
	handle := func(_ context.Context, evt cloudevents.Event) { events <- evt.ID() }
	connectAndReceive := func() error {
		if err := tr.Connect(ctx); err != nil {
			return err
		}
		if err := tr.Subscribe(ctx); err != nil {
			return err
		}
		go func() { _ = tr.Receive(ctx, handle) }()
		return nil
	}
	require.NoError(t, connectAndReceive())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-tr.ErrorChan():
				t.Logf("transport error, reconnecting: %v", err)
				_ = tr.Close(ctx)
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(reconnectDelay):
					}
					if err := connectAndReceive(); err == nil {
						break
					} else {
						t.Logf("reconnect failed: %v", err)
						_ = tr.Close(ctx)
					}
				}
			}
		}
	}()
	return events
}

func requireEvent(t *testing.T, events <-chan string, id string) {
	t.Helper()
	select {
	case got := <-events:
		require.Equal(t, id, got)
	case <-time.After(10 * time.Second):
		t.Fatalf("event %s not received", id)
	}
}

func newSPIFFETransport(ctx context.Context, t *testing.T, h *spiffeHub, agentAPI *fakespire.Server) *grpcTransport {
	t.Helper()
	opts, closer, err := grpcoptions.NewSPIFFEGRPCOptions(ctx, grpcoptions.SPIFFEConfig{
		URL: h.addr, HubSPIFFEID: renewalHubID, AgentSPIFFEID: renewalWorkID, WorkloadAPISocket: agentAPI.Addr,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	return newTransport(opts, func() *pbv1.SubscriptionRequest {
		return &pbv1.SubscriptionRequest{Source: types.SourceAll, ClusterName: "cluster1", DataType: "test"}
	})
}

// TestSPIFFERenewal: the agent connects with SVID A, SPIRE rotates to SVID B, the server closes
// the connection when A expires, and the agent reconnects with B and keeps receiving events.
func TestSPIFFERenewal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := startSPIFFEHub(t)
	agentAPI := fakespire.Start(t, h.ca)
	svidA := h.ca.Issue(t, renewalWorkID, 3*time.Second)
	agentAPI.SetSVIDs(svidA)

	tr := newSPIFFETransport(ctx, t, h, agentAPI)
	events := runAgent(ctx, t, tr)
	work := spiffeid.RequireFromString(renewalWorkID)

	require.Eventually(t, func() bool { return h.broker.subscribers() == 1 }, 5*time.Second, 20*time.Millisecond)
	h.broker.publishFromHub("e1")
	requireEvent(t, events, "e1")
	require.Equal(t, []*big.Int{svidA.Certificates[0].SerialNumber}, h.reg.PeerSerials(work))

	// SPIRE rotates. The open connection keeps using A: the TLS identity is fixed per connection.
	svidB := h.ca.Issue(t, renewalWorkID, time.Hour)
	agentAPI.SetSVIDs(svidB)
	require.Equal(t, []*big.Int{svidA.Certificates[0].SerialNumber}, h.reg.PeerSerials(work))

	// A expires; the server closes the connection; the agent reconnects with B.
	require.Eventually(t, func() bool {
		serials := h.reg.PeerSerials(work)
		return len(serials) == 1 && serials[0].Cmp(svidB.Certificates[0].SerialNumber) == 0 &&
			h.broker.subscribers() == 1
	}, 10*time.Second, 50*time.Millisecond)

	h.broker.publishFromHub("e2")
	requireEvent(t, events, "e2")
}

// TestSPIFFERevocationWhileStreaming: closing the cluster's connections ends the stream, and
// the agent's reconnect is accepted at the TLS level (the authorizer refuses it later, once the
// cluster is denied).
func TestSPIFFERevocationWhileStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := startSPIFFEHub(t)
	agentAPI := fakespire.Start(t, h.ca)
	agentAPI.SetSVIDs(h.ca.Issue(t, renewalWorkID, time.Hour))

	tr := newSPIFFETransport(ctx, t, h, agentAPI)
	events := runAgent(ctx, t, tr)
	require.Eventually(t, func() bool { return h.broker.subscribers() == 1 }, 5*time.Second, 20*time.Millisecond)

	require.Equal(t, 1, h.reg.CloseCluster("cluster1"))
	require.Eventually(t, func() bool { return h.reg.Len() == 1 && h.broker.subscribers() == 1 },
		10*time.Second, 50*time.Millisecond, "the agent reconnects and subscribes again")
	h.broker.publishFromHub("e1")
	requireEvent(t, events, "e1")
}

// TestSPIFFERevocationEndsRunningCall: a unary call that is running when the cluster's
// connections are closed fails with Unavailable.
func TestSPIFFERevocationEndsRunningCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := startSPIFFEHub(t)
	h.broker.publishGate = make(chan struct{})
	defer close(h.broker.publishGate)
	agentAPI := fakespire.Start(t, h.ca)
	agentAPI.SetSVIDs(h.ca.Issue(t, renewalWorkID, time.Hour))

	tr := newSPIFFETransport(ctx, t, h, agentAPI)
	require.NoError(t, tr.Connect(ctx))
	defer tr.Close(ctx)

	sent := make(chan error, 1)
	go func() {
		evt := cloudevents.NewEvent()
		evt.SetID("slow")
		evt.SetSource("cluster1")
		evt.SetType("io.open-cluster-management.works.v1alpha1.manifests.status.update_request")
		sent <- tr.Send(ctx, evt)
	}()
	select {
	case <-h.broker.publishing:
	case <-time.After(10 * time.Second):
		t.Fatal("publish did not reach the server")
	}

	require.Equal(t, 1, h.reg.CloseCluster("cluster1"))
	select {
	case err := <-sent:
		require.Equal(t, codes.Unavailable, status.Code(err), "got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the running call did not end")
	}
}
