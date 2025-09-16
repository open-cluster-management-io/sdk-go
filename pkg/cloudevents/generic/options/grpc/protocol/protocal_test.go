package protocol

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/utils/ptr"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

const bufSize = 1024 * 1024

// mockCloudEventService implements a basic mock of CloudEventServiceServer
type mockCloudEventService struct {
	pbv1.UnimplementedCloudEventServiceServer
	healthEnabled bool
}

func (m *mockCloudEventService) Publish(ctx context.Context, req *pbv1.PublishRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (m *mockCloudEventService) Subscribe(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
	if m.healthEnabled {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					heartbeat := &pbv1.CloudEvent{
						SpecVersion: "1.0",
						Id:          uuid.New().String(),
						Type:        types.HeartbeatCloudEventsType,
					}

					if err := stream.Send(heartbeat); err != nil {
						return
					}
				case <-stream.Context().Done():
					return
				}
			}
		}()
	}
	// Keep the stream open for testing
	<-stream.Context().Done()
	return stream.Context().Err()
}

func setupMockServer(t *testing.T, healthEnabled bool) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	// Register cloud event service
	pbv1.RegisterCloudEventServiceServer(s, &mockCloudEventService{healthEnabled: healthEnabled})

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

func TestProtocol_Success(t *testing.T) {
	conn, cleanup := setupMockServer(t, true)
	defer cleanup()

	reconnectErrorChan := make(chan error, 1)
	p, err := NewProtocol(
		conn,
		WithSubscribeOption(&SubscribeOption{
			Source:      "test",
			ClusterName: "test-cluster",
			DataType:    "io.open-cluster-management.test",
		}),
		WithReconnectErrorChan(reconnectErrorChan),
		WithServerHealthinessTimeout(ptr.To(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		if err := p.OpenInbound(ctx); err != nil {
			p.reconnectErrorChan <- err
		}
	}()

	// Should not receive any error since health check was successful
	select {
	case err := <-p.reconnectErrorChan:
		t.Errorf("Unexpected error from health check: %v", err)
	case <-time.After(2 * time.Second):
		// Expected - no error
	}
}

func TestProtocol_Timeout(t *testing.T) {
	conn, cleanup := setupMockServer(t, false) // No health service
	defer cleanup()

	reconnectErrorChan := make(chan error, 1)
	p, err := NewProtocol(
		conn,
		WithSubscribeOption(&SubscribeOption{
			Source:      "test",
			ClusterName: "test-cluster",
			DataType:    "io.open-cluster-management.test",
		}),
		WithReconnectErrorChan(reconnectErrorChan),
		WithServerHealthinessTimeout(ptr.To(time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		if err := p.OpenInbound(ctx); err != nil {
			p.reconnectErrorChan <- err
		}
	}()

	// Should receive an error due to health service not being available
	select {
	case err := <-p.reconnectErrorChan:
		if err == nil {
			t.Errorf("Expected health check error, but got nil %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Expected health check error within timeout")
	}
}
