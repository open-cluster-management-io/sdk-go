package grpc

import (
	"context"
	"errors"
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
	"google.golang.org/grpc/peer"

	ocmspiffe "open-cluster-management.io/sdk-go/pkg/spiffe"
	"open-cluster-management.io/sdk-go/pkg/testing/fakespire"
)

const (
	testHubID  = "spiffe://ocm-hub.example.org/ocm/hub/grpc-server"
	testWorkID = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"
)

// testSPIFFEServer is a gRPC health server with SPIFFE mTLS from a fake Workload API. It
// records the serial number of the last client certificate it saw.
type testSPIFFEServer struct {
	addr string
	mu   sync.Mutex
	last *big.Int
}

func (s *testSPIFFEServer) lastSerial() *big.Int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func startSPIFFEServer(t *testing.T, api *fakespire.Server, serverID string) *testSPIFFEServer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	src, err := ocmspiffe.NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(serverID))
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &testSPIFFEServer{addr: lis.Addr().String()}
	record := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if p, ok := peer.FromContext(ctx); ok {
			if info, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(info.State.PeerCertificates) > 0 {
				s.mu.Lock()
				s.last = info.State.PeerCertificates[0].SerialNumber
				s.mu.Unlock()
			}
		}
		return h(ctx, req)
	}
	gs := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsconfig.MTLSServerConfig(src, src, tlsconfig.AuthorizeAny()))),
		grpc.UnaryInterceptor(record))
	healthpb.RegisterHealthServer(gs, health.NewServer())
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return s
}

// check dials a fresh connection with the options' TLS config and calls the health service.
func check(ctx context.Context, t *testing.T, opts *GRPCOptions) (*healthpb.HealthCheckResponse, error) {
	t.Helper()
	conn, err := grpc.NewClient(opts.Dialer.URL,
		grpc.WithTransportCredentials(credentials.NewTLS(opts.Dialer.TLSConfig)))
	require.NoError(t, err)
	defer conn.Close()
	return healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
}

func TestSPIFFEClientAcceptsOnlyHubID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	agentAPI := fakespire.Start(t, ca)
	agentAPI.SetSVIDs(ca.Issue(t, testWorkID, time.Hour))

	cases := []struct {
		name     string
		serverID string
		wantErr  string
	}{
		{"right hub", testHubID, ""},
		// gRPC escapes the quotes around the ID in the message.
		{"wrong hub, same trust domain", "spiffe://ocm-hub.example.org/ocm/something-else",
			`authentication handshake failed: unexpected ID \"spiffe://ocm-hub.example.org/ocm/something-else\"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hubAPI := fakespire.Start(t, ca)
			hubAPI.SetSVIDs(ca.Issue(t, c.serverID, time.Hour))
			srv := startSPIFFEServer(t, hubAPI, c.serverID)

			opts, closer, err := NewSPIFFEGRPCOptions(ctx, SPIFFEConfig{
				URL: srv.addr, HubSPIFFEID: testHubID, AgentSPIFFEID: testWorkID, WorkloadAPISocket: agentAPI.Addr,
			})
			require.NoError(t, err)
			defer closer.Close()

			resp, err := check(ctx, t, opts)
			if c.wantErr != "" {
				require.ErrorContains(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
		})
	}
}

func TestSPIFFEClientUsesRotatedSVID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	hubAPI, agentAPI := fakespire.Start(t, ca), fakespire.Start(t, ca)
	hubAPI.SetSVIDs(ca.Issue(t, testHubID, time.Hour))
	first := ca.Issue(t, testWorkID, time.Hour)
	agentAPI.SetSVIDs(first)
	srv := startSPIFFEServer(t, hubAPI, testHubID)

	opts, closer, err := NewSPIFFEGRPCOptions(ctx, SPIFFEConfig{
		URL: srv.addr, HubSPIFFEID: testHubID, AgentSPIFFEID: testWorkID, WorkloadAPISocket: agentAPI.Addr,
	})
	require.NoError(t, err)
	defer closer.Close()

	_, err = check(ctx, t, opts)
	require.NoError(t, err)
	require.Equal(t, first.Certificates[0].SerialNumber, srv.lastSerial())

	second := ca.Issue(t, testWorkID, time.Hour)
	agentAPI.SetSVIDs(second)
	require.Eventually(t, func() bool {
		_, err := check(ctx, t, opts)
		return err == nil && srv.lastSerial().Cmp(second.Certificates[0].SerialNumber) == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestSPIFFEClientFederatedHub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hubCA := fakespire.NewCA(t, "ocm-hub.example.org")
	eastCA := fakespire.NewCA(t, "east.example.org")
	const eastWorkID = "spiffe://east.example.org/ocm/cluster/cluster2/agent/work"

	hubAPI := fakespire.Start(t, hubCA)
	hubAPI.SetSVIDs(hubCA.Issue(t, testHubID, time.Hour))
	agentAPI := fakespire.Start(t, eastCA)
	agentAPI.SetSVIDs(eastCA.Issue(t, eastWorkID, time.Hour))

	opts, closer, err := func() (*GRPCOptions, interface{ Close() error }, error) {
		srvNoFed := startSPIFFEServer(t, hubAPI, testHubID)
		return NewSPIFFEGRPCOptions(ctx, SPIFFEConfig{
			URL: srvNoFed.addr, HubSPIFFEID: testHubID, AgentSPIFFEID: eastWorkID, WorkloadAPISocket: agentAPI.Addr,
		})
	}()
	require.NoError(t, err)
	defer closer.Close()

	// Without federation neither side can verify the other.
	_, err = check(ctx, t, opts)
	require.Error(t, err)

	// With federation on both sides, the handshake succeeds.
	hubAPI.Federate(eastCA)
	agentAPI.Federate(hubCA)
	srv := startSPIFFEServer(t, hubAPI, testHubID)
	opts.Dialer.URL = srv.addr
	require.Eventually(t, func() bool {
		resp, err := check(ctx, t, opts)
		return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
	}, 5*time.Second, 50*time.Millisecond)
}

func TestNewSPIFFEGRPCOptionsErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	api := fakespire.Start(t, ca)
	api.SetSVIDs(ca.Issue(t, "spiffe://ocm-hub.example.org/ns/open-cluster-management-agent/sa/default", time.Hour))

	cases := []struct {
		name    string
		config  SPIFFEConfig
		wantErr string
	}{
		{"no URL", SPIFFEConfig{HubSPIFFEID: testHubID, AgentSPIFFEID: testWorkID, WorkloadAPISocket: api.Addr},
			"URL is required"},
		{"bad hub ID", SPIFFEConfig{URL: "x:1", HubSPIFFEID: "https://hub", AgentSPIFFEID: testWorkID, WorkloadAPISocket: api.Addr},
			"invalid hub SPIFFE ID"},
		{"bad agent ID", SPIFFEConfig{URL: "x:1", HubSPIFFEID: testHubID, AgentSPIFFEID: "", WorkloadAPISocket: api.Addr},
			"invalid agent SPIFFE ID"},
		{"agent SVID missing", SPIFFEConfig{URL: "x:1", HubSPIFFEID: testHubID, AgentSPIFFEID: testWorkID, WorkloadAPISocket: api.Addr},
			"no SVID with ID " + testWorkID},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := NewSPIFFEGRPCOptions(ctx, c.config)
			require.ErrorContains(t, err, c.wantErr)
			if c.name == "agent SVID missing" {
				var notFound *ocmspiffe.ErrSVIDNotFound
				require.True(t, errors.As(err, &notFound))
			}
		})
	}
}
