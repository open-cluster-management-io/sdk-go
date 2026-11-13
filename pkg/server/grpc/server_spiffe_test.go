package grpc

import (
	"context"
	"fmt"
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
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authn"
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authz"
	grpcspiffe "open-cluster-management.io/sdk-go/pkg/server/grpc/spiffe"
	ocmspiffe "open-cluster-management.io/sdk-go/pkg/spiffe"
	"open-cluster-management.io/sdk-go/pkg/testing/fakespire"
)

type allowAll struct{}

func (allowAll) AuthorizeRequest(ctx context.Context, _ any) (authz.Decision, context.Context, error) {
	return authz.DecisionAllow, ctx, nil
}

func (allowAll) AuthorizeStream(_ context.Context, ss grpc.ServerStream, _ *grpc.StreamServerInfo) (authz.Decision, grpc.ServerStream, error) {
	return authz.DecisionAllow, ss, nil
}

// TestGRPCServerWithSPIFFECredentials runs the server builder the way the SPIFFE listener does:
// transport credentials from the Workload API instead of certificate files, the SPIFFE
// authenticator, and the SVID expiry interceptors after authorization.
func TestGRPCServerWithSPIFFECredentials(t *testing.T) {
	const (
		hubID  = "spiffe://ocm-hub.example.org/ocm/hub/grpc-server"
		workID = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	hubAPI, agentAPI := fakespire.Start(t, ca), fakespire.Start(t, ca)
	hubAPI.SetSVIDs(ca.Issue(t, hubID, time.Hour))
	agentAPI.SetSVIDs(ca.Issue(t, workID, time.Hour))

	hubSrc, err := ocmspiffe.NewX509Source(ctx, hubAPI.Addr, spiffeid.RequireFromString(hubID))
	require.NoError(t, err)
	defer hubSrc.Close()
	scheme, err := authn.NewIDScheme(v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate,
		[]string{"ocm-hub.example.org"})
	require.NoError(t, err)
	reg := grpcspiffe.NewRegistry()

	// The extra interceptor runs last and records who called.
	var mu sync.Mutex
	var seen []authn.OCMIdentity
	record := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if id, ok := authn.OCMIdentityFromContext(ctx); ok {
			mu.Lock()
			seen = append(seen, id)
			mu.Unlock()
		}
		return h(ctx, req)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())

	opts := NewGRPCServerOptions()
	opts.ServerBindPort = fmt.Sprintf("%d", port)
	// No certificate files exist; the server must not look for them.
	opts.TLSCertFile, opts.TLSKeyFile, opts.ClientCAFile = "/nonexistent/tls.crt", "/nonexistent/tls.key", ""
	server := NewGRPCServer(opts).
		WithTransportCredentials(grpcspiffe.NewServerCredentials(hubSrc, scheme, reg)).
		WithAuthenticator(authn.NewSPIFFEAuthenticator(scheme)).
		WithUnaryAuthorizer(allowAll{}).
		WithStreamAuthorizer(allowAll{}).
		WithInterceptors(
			[]grpc.UnaryServerInterceptor{authn.SVIDExpiryUnaryInterceptor(time.Now), record},
			[]grpc.StreamServerInterceptor{authn.SVIDExpiryStreamInterceptor(time.Now)}).
		WithRegisterFunc(func(s *grpc.Server) { healthpb.RegisterHealthServer(s, health.NewServer()) })

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- server.Run(runCtx) }()

	agentSrc, err := ocmspiffe.NewX509Source(ctx, agentAPI.Addr, spiffeid.RequireFromString(workID))
	require.NoError(t, err)
	defer agentSrc.Close()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(credentials.NewTLS(
		tlsconfig.MTLSClientConfig(agentSrc, agentSrc, tlsconfig.AuthorizeID(spiffeid.RequireFromString(hubID))))))
	require.NoError(t, err)
	defer conn.Close()

	var lastErr error
	require.Eventually(t, func() bool {
		select {
		case err := <-runErr:
			t.Fatalf("server stopped: %v", err)
		default:
		}
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		lastErr = err
		return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
	}, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, lastErr)

	mu.Lock()
	require.NotEmpty(t, seen)
	require.Equal(t, "system:open-cluster-management:cluster1:work", seen[0].User)
	mu.Unlock()
	require.Equal(t, 1, reg.Len())

	stop()
	require.NoError(t, <-runErr)
}
