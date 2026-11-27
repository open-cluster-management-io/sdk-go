package spiffe

import (
	"context"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	v1 "open-cluster-management.io/api/operator/v1"
	ocmauthn "open-cluster-management.io/sdk-go/pkg/server/grpc/authn"
	ocmspiffe "open-cluster-management.io/sdk-go/pkg/spiffe"
	"open-cluster-management.io/sdk-go/pkg/testing/fakespire"
)

const (
	hubID          = "spiffe://ocm-hub.example.org/ocm/hub/grpc-server"
	cluster1Reg    = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/registration"
	cluster1Work   = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"
	cluster2Work   = "spiffe://ocm-hub.example.org/ocm/cluster/cluster2/agent/work"
	cluster1AddOn  = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/addon/my-addon/agent/my-addon-agent"
	notAnAgentID   = "spiffe://ocm-hub.example.org/ns/x/sa/y"
	eastWorkAgent  = "spiffe://east.example.org/ocm/cluster/cluster3/agent/work"
	testTimeout    = 10 * time.Second
	realTimeTTL    = 2 * time.Second
	realTimeBudget = 4 * time.Second
)

// fakeTimers replaces time.AfterFunc: it records each timer, and the test fires them.
type fakeTimers struct {
	mu     sync.Mutex
	timers []*fakeTimer
}

type fakeTimer struct {
	d       time.Duration
	f       func()
	stopped bool
	mu      sync.Mutex
}

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (t *fakeTimer) isStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func (ft *fakeTimers) afterFunc(d time.Duration, f func()) stopper {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	t := &fakeTimer{d: d, f: f}
	ft.timers = append(ft.timers, t)
	return t
}

func (ft *fakeTimers) all() []*fakeTimer {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	return append([]*fakeTimer(nil), ft.timers...)
}

func newFakeClockRegistry(now time.Time) (*Registry, *fakeTimers) {
	ft := &fakeTimers{}
	r := NewRegistry()
	r.now = func() time.Time { return now }
	r.afterFunc = ft.afterFunc
	return r, ft
}

// hub is a gRPC health server behind the SPIFFE server credentials.
type hub struct {
	ca   *fakespire.CA
	addr string
	reg  *Registry
}

func startHub(t *testing.T, reg *Registry, federate ...*fakespire.CA) *hub {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	api := fakespire.Start(t, ca)
	api.SetSVIDs(ca.Issue(t, hubID, time.Hour))
	api.Federate(federate...)
	src, err := ocmspiffe.NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(hubID))
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })

	scheme, err := ocmauthn.NewIDScheme(v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate,
		[]string{"ocm-hub.example.org"})
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer(grpc.Creds(NewServerCredentials(src, scheme, reg)))
	healthpb.RegisterHealthServer(gs, health.NewServer())
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return &hub{ca: ca, addr: lis.Addr().String(), reg: reg}
}

// agent is a client connection with its own SVID from its own fake Workload API.
type agent struct {
	conn   *grpc.ClientConn
	serial *big.Int
}

func connect(t *testing.T, h *hub, ca *fakespire.CA, id string, ttl time.Duration, federate ...*fakespire.CA) *agent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	api := fakespire.Start(t, ca)
	svid := ca.Issue(t, id, ttl)
	api.SetSVIDs(svid)
	api.Federate(federate...)
	src, err := ocmspiffe.NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(id))
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	conn, err := grpc.NewClient(h.addr, grpc.WithTransportCredentials(credentials.NewTLS(
		tlsconfig.MTLSClientConfig(src, src, tlsconfig.AuthorizeID(spiffeid.RequireFromString(hubID))))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &agent{conn: conn, serial: svid.Certificates[0].SerialNumber}
}

func (a *agent) check(ctx context.Context) error {
	_, err := healthpb.NewHealthClient(a.conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

// watch opens a server stream and returns a channel that gets its end error.
func (a *agent) watch(t *testing.T) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := healthpb.NewHealthClient(a.conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv() // the current status
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				done <- err
				return
			}
		}
	}()
	return done
}

func requireEnded(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(testTimeout):
		t.Fatal("stream did not end")
	}
}

func requireOpen(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("stream ended: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRegistryArmsTimerAtExpiry(t *testing.T) {
	t1040 := time.Date(2026, 11, 13, 10, 40, 0, 0, time.UTC)
	reg, ft := newFakeClockRegistry(t1040)
	server, client := net.Pipe()
	defer client.Close()

	tc := reg.track(server, ocmauthn.OCMIdentity{ClusterName: "cluster1"}, spiffeid.RequireFromString(cluster1Work),
		big.NewInt(1), t1040.Add(20*time.Minute))
	require.Equal(t, 1, reg.Len())
	timers := ft.all()
	require.Len(t, timers, 1)
	require.Equal(t, 20*time.Minute, timers[0].d)

	// 11:00: the timer fires and closes the connection.
	timers[0].f()
	require.Equal(t, 0, reg.Len())
	_, err := server.Write([]byte("x"))
	require.Error(t, err)
	require.NoError(t, tc.Close(), "closing twice is fine")
}

func TestRegistryClosesAtExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	reg, ft := newFakeClockRegistry(time.Now())
	h := startHub(t, reg)
	a := connect(t, h, h.ca, cluster1Work, time.Hour)
	done := a.watch(t)
	require.Equal(t, 1, reg.Len())

	timers := ft.all()
	require.Len(t, timers, 1)
	timers[0].f()

	requireEnded(t, done)
	// The client reconnects with a new handshake, which arms a new timer.
	require.NoError(t, a.check(ctx))
	require.Len(t, ft.all(), 2)
}

func TestRegistryCloseCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	reg, _ := newFakeClockRegistry(time.Now())
	h := startHub(t, reg)
	reg1 := connect(t, h, h.ca, cluster1Reg, time.Hour)
	work1 := connect(t, h, h.ca, cluster1Work, time.Hour)
	work2 := connect(t, h, h.ca, cluster2Work, time.Hour)
	done1, done2, done3 := reg1.watch(t), work1.watch(t), work2.watch(t)
	require.Equal(t, 3, reg.Len())

	require.Equal(t, 2, reg.CloseCluster("cluster1"))
	requireEnded(t, done1)
	requireEnded(t, done2)
	requireOpen(t, done3)
	require.Len(t, reg.PeerSerials(spiffeid.RequireFromString(cluster2Work)), 1)
	require.NoError(t, work2.check(ctx))
	require.Equal(t, 0, reg.CloseCluster("cluster9"))
}

func TestRegistryCloseAddOn(t *testing.T) {
	reg, _ := newFakeClockRegistry(time.Now())
	h := startHub(t, reg)
	work := connect(t, h, h.ca, cluster1Work, time.Hour)
	addon := connect(t, h, h.ca, cluster1AddOn, time.Hour)
	doneWork, doneAddOn := work.watch(t), addon.watch(t)

	require.Equal(t, 0, reg.CloseAddOn("cluster1", "other-addon"))
	require.Equal(t, 0, reg.CloseAddOn("cluster2", "my-addon"))
	require.Equal(t, 1, reg.CloseAddOn("cluster1", "my-addon"))
	requireEnded(t, doneAddOn)
	requireOpen(t, doneWork)
}

func TestRegistryCloseTrustDomain(t *testing.T) {
	reg, _ := newFakeClockRegistry(time.Now())
	h := startHub(t, reg)
	work := connect(t, h, h.ca, cluster1Work, time.Hour)
	done := work.watch(t)

	require.Equal(t, 0, reg.CloseTrustDomain(spiffeid.RequireTrustDomainFromString("east.example.org")))
	requireOpen(t, done)
	require.Equal(t, 1, reg.CloseTrustDomain(spiffeid.RequireTrustDomainFromString("ocm-hub.example.org")))
	requireEnded(t, done)
}

func TestCredentialsRejectsNonAgentID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	reg, _ := newFakeClockRegistry(time.Now())
	h := startHub(t, reg)
	a := connect(t, h, h.ca, notAnAgentID, time.Hour)
	require.Error(t, a.check(ctx))
	require.Equal(t, 0, reg.Len())
}

func TestCredentialsRejectsUnacceptedTrustDomain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	east := fakespire.NewCA(t, "east.example.org")
	reg, _ := newFakeClockRegistry(time.Now())
	// The hub's SPIRE federates with east, but east is not an accepted agent trust domain.
	h := startHub(t, reg, east)
	a := connect(t, h, east, eastWorkAgent, time.Hour, h.ca)
	err := a.check(ctx)
	require.Error(t, err)
	require.Equal(t, 0, reg.Len())
}

func TestRegistryClientClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	reg, ft := newFakeClockRegistry(time.Now())
	h := startHub(t, reg)
	a := connect(t, h, h.ca, cluster1Work, time.Hour)
	require.NoError(t, a.check(ctx))
	require.Equal(t, 1, reg.Len())

	require.NoError(t, a.conn.Close())
	require.Eventually(t, func() bool { return reg.Len() == 0 }, testTimeout, 20*time.Millisecond)
	require.True(t, ft.all()[0].isStopped())
}

func TestSVIDExpiryRealTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	reg := NewRegistry()
	h := startHub(t, reg)
	a := connect(t, h, h.ca, cluster1Work, realTimeTTL)
	require.NoError(t, a.check(ctx))
	done := a.watch(t)
	require.Len(t, reg.PeerSerials(spiffeid.RequireFromString(cluster1Work)), 1)

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(realTimeBudget):
		t.Fatal("the server did not close the connection at SVID expiry")
	}
	// The agent's only SVID has expired, so it can't connect again.
	require.Eventually(t, func() bool { return reg.Len() == 0 }, testTimeout, 20*time.Millisecond)
	require.Error(t, a.check(ctx))
}
