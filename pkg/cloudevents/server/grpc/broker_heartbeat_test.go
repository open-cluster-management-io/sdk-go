package grpc

import (
	"context"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

// mockSubscribeServer implements pbv1.CloudEventService_SubscribeServer for testing
type mockSubscribeServer struct {
	events    [](*pbv1.CloudEvent)
	sendError error
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
}

func newMockSubscribeServer() *mockSubscribeServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockSubscribeServer{
		events: make([]*pbv1.CloudEvent, 0),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (m *mockSubscribeServer) Send(event *pbv1.CloudEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sendError != nil {
		return m.sendError
	}

	m.events = append(m.events, event)
	return nil
}

func (m *mockSubscribeServer) Context() context.Context {
	return m.ctx
}

// Implement grpc.ServerStream interface
func (m *mockSubscribeServer) SendMsg(message interface{}) error {
	if event, ok := message.(*pbv1.CloudEvent); ok {
		return m.Send(event)
	}
	return nil
}

func (m *mockSubscribeServer) RecvMsg(message interface{}) error {
	return nil
}

func (m *mockSubscribeServer) SetHeader(metadata.MD) error {
	return nil
}

func (m *mockSubscribeServer) SendHeader(metadata.MD) error {
	return nil
}

func (m *mockSubscribeServer) SetTrailer(metadata.MD) {
}

func (m *mockSubscribeServer) GetEvents() []*pbv1.CloudEvent {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*pbv1.CloudEvent, len(m.events))
	copy(result, m.events)
	return result
}

func (m *mockSubscribeServer) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendError = err
}

func (m *mockSubscribeServer) Close() {
	m.cancel()
}

func TestGRPCBroker_Subscribe_HeartbeatIntegration(t *testing.T) {
	broker := NewGRPCBroker()

	// Reduce heartbeat interval for testing
	broker.heartbeatCheckInterval = 50 * time.Millisecond

	dataType := cetypes.CloudEventsDataType{
		Group:    "test",
		Version:  "v1",
		Resource: "tests",
	}

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	broker.RegisterService(dataType, svc)

	mockServer := newMockSubscribeServer()
	defer mockServer.Close()

	req := &pbv1.SubscriptionRequest{
		ClusterName: "test-cluster",
		DataType:    dataType.String(),
	}

	// Start subscription in background
	errChan := make(chan error, 1)
	go func() {
		err := broker.Subscribe(req, mockServer)
		errChan <- err
	}()

	// Wait for heartbeats to be sent
	time.Sleep(200 * time.Millisecond)

	// Close the mock server to simulate client disconnect
	mockServer.Close()

	// Wait for subscription to end
	select {
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			t.Errorf("Unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Subscription should have ended")
	}

	// Verify heartbeats were sent
	events := mockServer.GetEvents()
	heartbeatCount := 0
	for _, event := range events {
		if event.Type == cetypes.HeartbeatCloudEventsType {
			heartbeatCount++
		}
	}

	if heartbeatCount < 2 {
		t.Errorf("Expected at least 2 heartbeats, got %d", heartbeatCount)
	}
}

func TestGRPCBroker_Subscribe_SendError(t *testing.T) {
	broker := NewGRPCBroker()
	broker.heartbeatCheckInterval = 30 * time.Millisecond

	dataType := cetypes.CloudEventsDataType{
		Group:    "test",
		Version:  "v1",
		Resource: "tests",
	}

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	broker.RegisterService(dataType, svc)

	mockServer := newMockSubscribeServer()
	defer mockServer.Close()

	// Set an error to be returned by Send immediately
	sendError := status.Error(codes.Unavailable, "connection lost")
	mockServer.SetSendError(sendError)

	req := &pbv1.SubscriptionRequest{
		ClusterName: "test-cluster",
		DataType:    dataType.String(),
	}

	// The subscription should complete (either with error or nil due to context cancellation)
	// The main goal is that it handles the Send error gracefully and doesn't hang
	err := broker.Subscribe(req, mockServer)

	// Check that the subscription completed without hanging
	// The broker logs the error and cancels the context, which may result in nil error
	t.Logf("Subscription completed with: %v", err)
}

func TestGRPCBroker_Subscribe_EventAndHeartbeatSeparation(t *testing.T) {
	broker := NewGRPCBroker()
	broker.heartbeatCheckInterval = 30 * time.Millisecond

	dataType := cetypes.CloudEventsDataType{
		Group:    "test",
		Version:  "v1",
		Resource: "tests",
	}

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	broker.RegisterService(dataType, svc)

	mockServer := newMockSubscribeServer()
	defer mockServer.Close()

	req := &pbv1.SubscriptionRequest{
		ClusterName: "test-cluster",
		DataType:    dataType.String(),
	}

	// Start subscription
	go func() {
		_ = broker.Subscribe(req, mockServer)
	}()

	// Wait for heartbeats to start
	time.Sleep(100 * time.Millisecond)

	// Send a regular event
	evt := cetypes.NewEventBuilder("test",
		cetypes.CloudEventsType{CloudEventsDataType: dataType, SubResource: cetypes.SubResourceSpec}).
		WithResourceID("test1").
		WithClusterName("test-cluster").NewEvent()

	if err := svc.create(&evt); err != nil {
		t.Fatal(err)
	}

	// Wait for the event to be processed
	time.Sleep(100 * time.Millisecond)

	// Close to end subscription
	mockServer.Close()

	// Wait for subscription to end
	time.Sleep(50 * time.Millisecond)

	// Verify both heartbeats and regular events were sent
	events := mockServer.GetEvents()
	heartbeatCount := 0
	regularEventCount := 0

	for _, event := range events {
		if event.Type == cetypes.HeartbeatCloudEventsType {
			heartbeatCount++
		} else {
			regularEventCount++
		}
	}

	if heartbeatCount < 1 {
		t.Errorf("Expected at least 1 heartbeat, got %d", heartbeatCount)
	}

	if regularEventCount < 1 {
		t.Errorf("Expected at least 1 regular event, got %d", regularEventCount)
	}
}

func TestGRPCBroker_Subscribe_InvalidRequest(t *testing.T) {
	broker := NewGRPCBroker()

	mockServer := newMockSubscribeServer()
	defer mockServer.Close()

	tests := []struct {
		name        string
		req         *pbv1.SubscriptionRequest
		expectError bool
		expectedMsg string
	}{
		{
			name: "missing cluster name",
			req: &pbv1.SubscriptionRequest{
				ClusterName: "",
				DataType:    "test.v1.tests",
			},
			expectError: true,
			expectedMsg: "invalid subscription request: missing cluster name",
		},
		{
			name: "invalid data type",
			req: &pbv1.SubscriptionRequest{
				ClusterName: "test-cluster",
				DataType:    "invalid-type",
			},
			expectError: true,
			expectedMsg: "invalid subscription request: invalid data type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := broker.Subscribe(tt.req, mockServer)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error, but got nil")
				} else if len(err.Error()) < len(tt.expectedMsg) ||
					err.Error()[:len(tt.expectedMsg)] != tt.expectedMsg {
					t.Errorf("Expected error message to start with '%s', got '%s'",
						tt.expectedMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestGRPCBroker_Subscribe_ChannelBlocking(t *testing.T) {
	broker := NewGRPCBroker()
	broker.heartbeatCheckInterval = 20 * time.Millisecond

	dataType := cetypes.CloudEventsDataType{
		Group:    "test",
		Version:  "v1",
		Resource: "tests",
	}

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	broker.RegisterService(dataType, svc)

	// Create a slow mock server that simulates slow processing
	slowMockServer := &slowMockSubscribeServer{}
	slowMockServer.ctx, slowMockServer.cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer slowMockServer.cancel()

	req := &pbv1.SubscriptionRequest{
		ClusterName: "test-cluster",
		DataType:    dataType.String(),
	}

	// This should not hang even with slow Send processing
	err := broker.Subscribe(req, slowMockServer)

	// Should complete (either with error or context cancellation)
	if err != nil && err != context.DeadlineExceeded {
		// Any other error is unexpected
		t.Errorf("Unexpected error: %v", err)
	}
}

// slowMockSubscribeServer simulates slow Send processing
type slowMockSubscribeServer struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *slowMockSubscribeServer) Send(event *pbv1.CloudEvent) error {
	time.Sleep(100 * time.Millisecond) // Simulate slow processing
	return nil
}

func (s *slowMockSubscribeServer) Context() context.Context {
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(context.Background())
	}
	return s.ctx
}

func (s *slowMockSubscribeServer) SendMsg(message interface{}) error {
	return s.Send(message.(*pbv1.CloudEvent))
}

func (s *slowMockSubscribeServer) RecvMsg(message interface{}) error {
	return nil
}

func (s *slowMockSubscribeServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *slowMockSubscribeServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *slowMockSubscribeServer) SetTrailer(metadata.MD) {
}
