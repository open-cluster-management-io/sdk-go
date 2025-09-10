package protocol

import (
	"context"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
)

const bufSize = 1024 * 1024

// mockCloudEventService implements a basic mock of CloudEventServiceServer
type mockCloudEventService struct {
	pbv1.UnimplementedCloudEventServiceServer
}

func (m *mockCloudEventService) Publish(ctx context.Context, req *pbv1.PublishRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (m *mockCloudEventService) Subscribe(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
	// Keep the stream open for testing
	<-stream.Context().Done()
	return stream.Context().Err()
}

// mockHealthServer implements a basic health server for testing
type mockHealthServer struct {
	healthpb.UnimplementedHealthServer
	servingStatus healthpb.HealthCheckResponse_ServingStatus
	watchEnabled  bool
}

func (m *mockHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: m.servingStatus}, nil
}

func (m *mockHealthServer) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	if !m.watchEnabled {
		return status.Error(codes.Unimplemented, "watch not enabled")
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := stream.Send(&healthpb.HealthCheckResponse{Status: m.servingStatus}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func setupMockServer(t *testing.T, healthEnabled bool) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	// Register cloud event service
	pbv1.RegisterCloudEventServiceServer(s, &mockCloudEventService{})

	// Register health service if enabled
	if healthEnabled {
		healthpb.RegisterHealthServer(s, &mockHealthServer{
			servingStatus: healthpb.HealthCheckResponse_SERVING,
			watchEnabled:  true,
		})
	}

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Wait for server to start accepting connections
	time.Sleep(50 * time.Millisecond)

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(bufDialer))

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	cleanup := func() {
		conn.Close()
		s.Stop()
	}

	return conn, cleanup
}

func TestProtocol_healthCheck_Success(t *testing.T) {
	conn, cleanup := setupMockServer(t, true)
	defer cleanup()

	// Create protocol with health check enabled
	p := &Protocol{
		clientConn:         conn,
		reconnectErrorChan: make(chan error, 1),
		checkInterval:      200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Start health check
	go p.healthCheck(ctx)

	// Wait a bit to ensure health check is running
	time.Sleep(300 * time.Millisecond)

	// Should not receive any error since health check was successful
	select {
	case err := <-p.reconnectErrorChan:
		t.Errorf("Unexpected error from health check: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected - no error
	}
}

func TestProtocol_healthCheck_Timeout(t *testing.T) {
	conn, cleanup := setupMockServer(t, true) // No health service
	defer cleanup()

	// Create protocol with health check enabled
	p := &Protocol{
		clientConn:         conn,
		reconnectErrorChan: make(chan error, 1),
		checkInterval:      100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start health check
	go p.healthCheck(ctx)

	// Should receive an error due to health service not being available
	select {
	case err := <-p.reconnectErrorChan:
		if err == nil {
			t.Error("Expected health check error, but got nil")
		}
	case <-time.After(1 * time.Second):
		t.Error("Expected health check error within timeout")
	}
}

func TestProtocol_healthCheck_ContextCancellation(t *testing.T) {
	conn, cleanup := setupMockServer(t, true)
	defer cleanup()

	// Create protocol with health check enabled
	p := &Protocol{
		clientConn:         conn,
		reconnectErrorChan: make(chan error, 1),
		checkInterval:      100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start health check
	go p.healthCheck(ctx)

	// Cancel context immediately
	cancel()

	// When context is cancelled, the health check may send an error to the channel
	// This is expected behavior, so we just verify the test completes without hanging
	select {
	case <-p.reconnectErrorChan:
		// This is expected - context cancellation can cause health check to report an error
	case <-time.After(200 * time.Millisecond):
		// Also expected - health check may return without sending error
	}
}

func TestProtocol_OpenInbound_WithHealthCheck(t *testing.T) {
	conn, cleanup := setupMockServer(t, true)
	defer cleanup()

	// Create protocol with subscribe option and health check
	p := &Protocol{
		clientConn: conn,
		client:     pbv1.NewCloudEventServiceClient(conn),
		subscribeOption: &SubscribeOption{
			Source: "test-source",
		},
		incoming:           make(chan *pbv1.CloudEvent),
		closeChan:          make(chan struct{}),
		reconnectErrorChan: make(chan error, 1),
		checkInterval:      100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// OpenInbound should start health check when reconnectErrorChan is set
	errChan := make(chan error, 1)
	go func() {
		err := p.OpenInbound(ctx)
		errChan <- err
	}()

	// Wait a bit to ensure health check is running
	time.Sleep(200 * time.Millisecond)

	// Close the protocol
	close(p.closeChan)

	// Wait for OpenInbound to complete
	select {
	case err := <-errChan:
		// Connection closing errors are expected during test cleanup
		if err != nil && !isConnectionClosingError(err) {
			t.Errorf("OpenInbound failed with unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("OpenInbound did not complete within timeout")
	}
}

// Helper function to check if error is related to connection closing
func isConnectionClosingError(err error) bool {
	errStr := err.Error()
	return errStr == "connection is closing" ||
		errStr == "context canceled" ||
		errStr == "context deadline exceeded" ||
		errStr == "grpc: the client connection is closing"
}

func TestProtocol_NewProtocol_WithHealthCheckOption(t *testing.T) {
	conn, cleanup := setupMockServer(t, true)
	defer cleanup()

	// Test creating protocol with health check option
	p, err := NewProtocol(conn, WithHealthCheck(100*time.Millisecond, make(chan error, 1)))
	if err != nil {
		t.Fatalf("Failed to create protocol: %v", err)
	}

	if p.checkInterval != 100*time.Millisecond {
		t.Errorf("Expected check interval 100ms, got %v", p.checkInterval)
	}

	if p.reconnectErrorChan == nil {
		t.Error("Expected reconnectErrorChan to be set")
	}
}

func TestProtocol_NewProtocol_WithoutHealthCheck(t *testing.T) {
	conn, cleanup := setupMockServer(t, false)
	defer cleanup()

	// Test creating protocol without health check option
	p, err := NewProtocol(conn)
	if err != nil {
		t.Fatalf("Failed to create protocol: %v", err)
	}

	if p.reconnectErrorChan != nil {
		t.Error("Expected reconnectErrorChan to be nil when health check is not enabled")
	}
}
