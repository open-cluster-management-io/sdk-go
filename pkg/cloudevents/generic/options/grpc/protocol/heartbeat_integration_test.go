package protocol

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"k8s.io/utils/ptr"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

// mockCloudEventServiceWithHeartbeat provides more control over heartbeat behavior
type mockCloudEventServiceWithHeartbeat struct {
	pbv1.UnimplementedCloudEventServiceServer
	heartbeatInterval time.Duration
	stopAfterEvents   int
	eventsSent        int
}

func (m *mockCloudEventServiceWithHeartbeat) Publish(ctx context.Context, req *pbv1.PublishRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (m *mockCloudEventServiceWithHeartbeat) Subscribe(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
	if m.heartbeatInterval > 0 {
		ticker := time.NewTicker(m.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if m.stopAfterEvents > 0 && m.eventsSent >= m.stopAfterEvents {
					return nil
				}

				heartbeat := &pbv1.CloudEvent{
					SpecVersion: "1.0",
					Id:          uuid.New().String(),
					Type:        types.HeartbeatCloudEventsType,
				}

				if err := stream.Send(heartbeat); err != nil {
					return err
				}
				m.eventsSent++

			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
	}

	// Keep the stream open indefinitely if no heartbeat interval
	<-stream.Context().Done()
	return stream.Context().Err()
}

func setupMockServerWithHeartbeat(t *testing.T, heartbeatInterval time.Duration, stopAfterEvents int) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	service := &mockCloudEventServiceWithHeartbeat{
		heartbeatInterval: heartbeatInterval,
		stopAfterEvents:   stopAfterEvents,
	}
	pbv1.RegisterCloudEventServiceServer(s, service)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

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
		lis.Close()
	}

	return conn, cleanup
}

func TestProtocol_HeartbeatIntegration(t *testing.T) {
	tests := []struct {
		name                     string
		heartbeatInterval        time.Duration
		serverHealthinessTimeout *time.Duration
		expectHealthCheckError   bool
		testDuration             time.Duration
		stopServerAfterEvents    int
	}{
		{
			name:                     "successful heartbeat with health check enabled",
			heartbeatInterval:        100 * time.Millisecond,
			serverHealthinessTimeout: ptr.To(500 * time.Millisecond),
			expectHealthCheckError:   false,
			testDuration:             800 * time.Millisecond,
			stopServerAfterEvents:    0,
		},
		{
			name:                     "health check timeout when no heartbeats",
			heartbeatInterval:        0, // No heartbeats
			serverHealthinessTimeout: ptr.To(200 * time.Millisecond),
			expectHealthCheckError:   true,
			testDuration:             500 * time.Millisecond,
			stopServerAfterEvents:    0,
		},
		{
			name:                     "health check disabled",
			heartbeatInterval:        0,   // No heartbeats
			serverHealthinessTimeout: nil, // Disabled
			expectHealthCheckError:   false,
			testDuration:             300 * time.Millisecond,
			stopServerAfterEvents:    0,
		},
		{
			name:                     "heartbeat stops mid-stream",
			heartbeatInterval:        50 * time.Millisecond,
			serverHealthinessTimeout: ptr.To(200 * time.Millisecond),
			expectHealthCheckError:   true,
			testDuration:             600 * time.Millisecond,
			stopServerAfterEvents:    3, // Stop after 3 heartbeats
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, cleanup := setupMockServerWithHeartbeat(t, tt.heartbeatInterval, tt.stopServerAfterEvents)
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
				WithServerHealthinessTimeout(tt.serverHealthinessTimeout),
			)
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.testDuration)
			defer cancel()

			go func() {
				if err := p.OpenInbound(ctx); err != nil {
					select {
					case p.reconnectErrorChan <- err:
					default:
					}
				}
			}()

			if tt.expectHealthCheckError {
				select {
				case err := <-reconnectErrorChan:
					if err == nil {
						t.Error("Expected health check error, but got nil")
					}
				case <-ctx.Done():
					t.Error("Expected health check error before context timeout")
				}
			} else {
				select {
				case err := <-reconnectErrorChan:
					t.Errorf("Unexpected error from health check: %v", err)
				case <-ctx.Done():
					// Expected - no error
				}
			}
		})
	}
}

func TestProtocol_StartEventsReceiver_HeartbeatFiltering(t *testing.T) {
	conn, cleanup := setupMockServerWithHeartbeat(t, 50*time.Millisecond, 5)
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
		WithServerHealthinessTimeout(ptr.To(1*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var receivedEvents atomic.Int32
	go func() {
		for {
			_, err := p.Receive(ctx)
			if err != nil {
				return
			}
			receivedEvents.Add(1)
		}
	}()

	go func() {
		if err := p.OpenInbound(ctx); err != nil {
			select {
			case p.reconnectErrorChan <- err:
			default:
			}
		}
	}()

	<-ctx.Done()

	// Should receive 0 events since only heartbeats are sent (which are filtered out)
	if receivedEvents.Load() > 0 {
		t.Errorf("Expected 0 events, got %d (heartbeats should be filtered out)", receivedEvents.Load())
	}
}

func TestProtocol_OpenInbound_ValidationErrors(t *testing.T) {
	tests := []struct {
		name            string
		subscribeOption *SubscribeOption
		expectError     bool
		expectedErrMsg  string
	}{
		{
			name:            "nil subscribe option",
			subscribeOption: nil,
			expectError:     true,
			expectedErrMsg:  "the subscribe option must not be nil",
		},
		{
			name: "empty source and cluster name",
			subscribeOption: &SubscribeOption{
				Source:      "",
				ClusterName: "",
				DataType:    "io.open-cluster-management.test",
			},
			expectError:    true,
			expectedErrMsg: "the source and cluster name of subscribe option cannot both be empty",
		},
		{
			name: "valid source only",
			subscribeOption: &SubscribeOption{
				Source:      "test-source",
				ClusterName: "",
				DataType:    "io.open-cluster-management.test",
			},
			expectError: false,
		},
		{
			name: "valid cluster name only",
			subscribeOption: &SubscribeOption{
				Source:      "",
				ClusterName: "test-cluster",
				DataType:    "io.open-cluster-management.test",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh connection for each test case to avoid interference
			conn, cleanup := setupMockServerWithHeartbeat(t, 0, 0) // No heartbeat for validation tests
			defer cleanup()

			reconnectErrorChan := make(chan error, 1)
			p, err := NewProtocol(
				conn,
				WithSubscribeOption(tt.subscribeOption),
				WithReconnectErrorChan(reconnectErrorChan),
			)

			// Check if validation error occurred during protocol creation
			if err != nil {
				if tt.expectError && err.Error() == tt.expectedErrMsg {
					return // Test passed - expected error occurred during creation
				}
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			err = p.OpenInbound(ctx)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error, but got nil")
				} else if err.Error() != tt.expectedErrMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.expectedErrMsg, err.Error())
				}
			} else {
				if err != nil && err != context.DeadlineExceeded {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}
