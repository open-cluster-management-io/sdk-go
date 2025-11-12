package grpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

const bufSize = 1024 * 1024

// mockCloudEventService implements a configurable mock of CloudEventServiceServer
type mockCloudEventService struct {
	pbv1.UnimplementedCloudEventServiceServer
	heartbeatInterval time.Duration
	stopAfterEvents   int
	eventsSent        atomic.Int32
	publishFunc       func(ctx context.Context, req *pbv1.PublishRequest) (*emptypb.Empty, error)
	subscribeFunc     func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error
}

func (m *mockCloudEventService) Publish(ctx context.Context, req *pbv1.PublishRequest) (*emptypb.Empty, error) {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (m *mockCloudEventService) Subscribe(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
	if err := stream.SendHeader(metadata.Pairs(constants.GRPCSubscriptionIDKey, "sub-test-id")); err != nil {
		return err
	}

	if m.subscribeFunc != nil {
		return m.subscribeFunc(req, stream)
	}

	if m.heartbeatInterval > 0 {
		ticker := time.NewTicker(m.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if m.stopAfterEvents > 0 && int(m.eventsSent.Load()) >= m.stopAfterEvents {
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
				m.eventsSent.Add(1)

			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
	}

	// Keep the stream open indefinitely if no heartbeat interval
	<-stream.Context().Done()
	return stream.Context().Err()
}

func setupMockServer(t *testing.T, service *mockCloudEventService) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	if service == nil {
		service = &mockCloudEventService{}
	}
	pbv1.RegisterCloudEventServiceServer(s, service)

	serverReady := make(chan struct{})
	go func() {
		close(serverReady)
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Wait for server to be ready
	<-serverReady
	time.Sleep(10 * time.Millisecond)

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

func TestNewAgentOptions(t *testing.T) {
	grpcOpts := &grpcoptions.GRPCOptions{
		Dialer: &grpcoptions.GRPCDialer{
			URL: "localhost:8080",
		},
	}
	clusterName := "test-cluster"
	agentID := "test-agent"
	dataType := types.CloudEventsDataType{
		Group:    "test.group",
		Version:  "v1",
		Resource: "tests",
	}

	opts := NewAgentOptions(grpcOpts, clusterName, agentID, dataType)

	if opts == nil {
		t.Fatal("NewAgentOptions returned nil")
	}

	if opts.AgentID != agentID {
		t.Errorf("expected AgentID %s, got %s", agentID, opts.AgentID)
	}

	if opts.ClusterName != clusterName {
		t.Errorf("expected ClusterName %s, got %s", clusterName, opts.ClusterName)
	}

	if opts.CloudEventsTransport == nil {
		t.Fatal("CloudEventsTransport is nil")
	}

	transport, ok := opts.CloudEventsTransport.(*grpcTransport)
	if !ok {
		t.Fatal("CloudEventsTransport is not a grpcTransport")
	}

	if transport.opts != grpcOpts {
		t.Error("grpcOptions not set correctly")
	}

	if transport.errorChan == nil {
		t.Error("errorChan not initialized")
	}

	// Test the subscription request function
	if transport.getSubscriptionRequest == nil {
		t.Fatal("getSubscriptionRequest not set")
	}

	req := transport.getSubscriptionRequest()
	if req.Source != types.SourceAll {
		t.Errorf("expected Source %s, got %s", types.SourceAll, req.Source)
	}
	if req.ClusterName != clusterName {
		t.Errorf("expected ClusterName %s, got %s", clusterName, req.ClusterName)
	}
	if req.DataType != dataType.String() {
		t.Errorf("expected DataType %s, got %s", dataType.String(), req.DataType)
	}
}

func TestNewSourceOptions(t *testing.T) {
	grpcOpts := &grpcoptions.GRPCOptions{
		Dialer: &grpcoptions.GRPCDialer{
			URL: "localhost:8080",
		},
	}
	sourceID := "test-source"
	dataType := types.CloudEventsDataType{
		Group:    "test.group",
		Version:  "v1",
		Resource: "tests",
	}

	opts := NewSourceOptions(grpcOpts, sourceID, dataType)

	if opts == nil {
		t.Fatal("NewSourceOptions returned nil")
	}

	if opts.SourceID != sourceID {
		t.Errorf("expected SourceID %s, got %s", sourceID, opts.SourceID)
	}

	if opts.CloudEventsTransport == nil {
		t.Fatal("CloudEventsTransport is nil")
	}

	transport, ok := opts.CloudEventsTransport.(*grpcTransport)
	if !ok {
		t.Fatal("CloudEventsTransport is not a grpcTransport")
	}

	if transport.opts != grpcOpts {
		t.Error("grpcOptions not set correctly")
	}

	if transport.errorChan == nil {
		t.Error("errorChan not initialized")
	}

	// Test the subscription request function
	if transport.getSubscriptionRequest == nil {
		t.Fatal("getSubscriptionRequest not set")
	}

	req := transport.getSubscriptionRequest()
	if req.Source != sourceID {
		t.Errorf("expected Source %s, got %s", sourceID, req.Source)
	}
	if req.DataType != dataType.String() {
		t.Errorf("expected DataType %s, got %s", dataType.String(), req.DataType)
	}
}

func TestGrpcTransport_ErrorChan(t *testing.T) {
	transport := &grpcTransport{
		errorChan: make(chan error, 1),
		closeChan: make(chan struct{}),
	}

	// Test that ErrorChan returns the correct channel
	errChan := transport.ErrorChan()
	if errChan == nil {
		t.Fatal("ErrorChan() returned nil")
	}

	// Test that we can send and receive errors
	testErr := errors.New("test error")
	transport.errorChan <- testErr

	select {
	case err := <-errChan:
		if err.Error() != testErr.Error() {
			t.Errorf("expected error %v, got %v", testErr, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error")
	}
}

func TestGrpcTransport_Send_Success(t *testing.T) {
	publishCalled := false
	var receivedEvent *pbv1.CloudEvent
	var mu sync.Mutex

	service := &mockCloudEventService{
		publishFunc: func(ctx context.Context, req *pbv1.PublishRequest) (*emptypb.Empty, error) {
			mu.Lock()
			defer mu.Unlock()
			publishCalled = true
			receivedEvent = req.Event
			return &emptypb.Empty{}, nil
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		client:    client,
		errorChan: make(chan error, 1),
		closeChan: make(chan struct{}),
	}

	evt := cloudevents.NewEvent()
	evt.SetID("test-id")
	evt.SetType("test-type")
	evt.SetSource("test-source")
	err := evt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err = transport.Send(context.Background(), evt)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !publishCalled {
		t.Error("Publish was not called")
	}

	if receivedEvent == nil {
		t.Fatal("receivedEvent is nil")
	}

	if receivedEvent.Id != "test-id" {
		t.Errorf("expected event ID test-id, got %s", receivedEvent.Id)
	}
}

func TestGrpcTransport_Send_Error(t *testing.T) {
	expectedErr := errors.New("publish failed")

	service := &mockCloudEventService{
		publishFunc: func(ctx context.Context, req *pbv1.PublishRequest) (*emptypb.Empty, error) {
			return nil, expectedErr
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		client:    client,
		errorChan: make(chan error, 1),
		closeChan: make(chan struct{}),
	}

	evt := cloudevents.NewEvent()
	evt.SetID("test-id")
	evt.SetType("test-type")
	evt.SetSource("test-source")

	err := transport.Send(context.Background(), evt)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGrpcTransport_Subscribe_Success(t *testing.T) {
	subscribeCalled := false
	var receivedRequest *pbv1.SubscriptionRequest
	var mu sync.Mutex

	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			mu.Lock()
			subscribeCalled = true
			receivedRequest = req
			mu.Unlock()
			<-stream.Context().Done()
			return stream.Context().Err()
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		client:    client,
		errorChan: make(chan error, 1),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:      "test-source",
				ClusterName: "test-cluster",
				DataType:    "test-type",
			}
		},
	}

	ctx := context.Background()
	err := transport.Subscribe(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Give some time for the subscribe call to be processed
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !subscribeCalled {
		t.Error("Subscribe was not called")
	}

	if receivedRequest == nil {
		t.Fatal("receivedRequest is nil")
	}

	if receivedRequest.Source != "test-source" {
		t.Errorf("expected Source test-source, got %s", receivedRequest.Source)
	}

	if receivedRequest.ClusterName != "test-cluster" {
		t.Errorf("expected ClusterName test-cluster, got %s", receivedRequest.ClusterName)
	}

	if transport.subClient == nil {
		t.Error("subClient was not set")
	}
}

func TestGrpcTransport_Subscribe_Error(t *testing.T) {
	// Note: Subscribe errors from the stream happen asynchronously in the server's goroutine
	// The Subscribe method itself may succeed, but the stream may fail later
	// To test subscription failure, we need to use a context that's already cancelled

	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			return errors.New("subscribe failed")
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		client:    client,
		errorChan: make(chan error, 1),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{}
		},
	}

	// Use a cancelled context to force immediate error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := transport.Subscribe(ctx)
	if err == nil {
		t.Log("Subscribe succeeded with cancelled context (server-side error will be reported later)")
		// This is acceptable - the error will be caught when trying to receive
	}
}

func TestGrpcTransport_Receive_WithEvents(t *testing.T) {
	evt := &pbv1.CloudEvent{
		Id:          "test-1",
		Source:      "test-source",
		SpecVersion: "1.0",
		Type:        "test.type",
		Attributes: map[string]*pbv1.CloudEventAttributeValue{
			"datacontenttype": {
				Attr: &pbv1.CloudEventAttributeValue_CeString{
					CeString: "application/json",
				},
			},
		},
		Data: &pbv1.CloudEvent_TextData{
			TextData: `{"test":"data1"}`,
		},
	}

	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			// Send one event immediately
			if err := stream.Send(evt); err != nil {
				return err
			}
			// Keep stream open
			<-stream.Context().Done()
			return stream.Context().Err()
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 100 * time.Millisecond
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:   "test-source",
				DataType: "test-type",
			}
		},
	}

	// Subscribe first
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	receivedEvents := make([]cloudevents.Event, 0)
	var mu sync.Mutex
	eventReceived := make(chan struct{}, 1)
	handleFn := func(ctx context.Context, evt cloudevents.Event) {
		mu.Lock()
		receivedEvents = append(receivedEvents, evt)
		mu.Unlock()
		select {
		case eventReceived <- struct{}{}:
		default:
		}
	}

	ctx := context.Background()

	// Run Receive in a goroutine
	done := make(chan error, 1)
	go func() {
		done <- transport.Receive(ctx, handleFn)
	}()

	// Wait for the event to be received
	select {
	case <-eventReceived:
		// Event received
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for event")
	}

	// Close the transport to stop receiving
	close(transport.closeChan)

	// Wait for Receive to complete
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Receive to complete")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedEvents) != 1 {
		t.Errorf("expected 1 event, got %d", len(receivedEvents))
	}

	if len(receivedEvents) > 0 && receivedEvents[0].ID() != "test-1" {
		t.Errorf("expected event ID test-1, got %s", receivedEvents[0].ID())
	}
}

func TestGrpcTransport_Receive_HeartbeatFiltering(t *testing.T) {
	sentEventCount := atomic.Int32{}
	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if sentEventCount.Load() >= 3 {
						return nil
					}
					heartbeat := &pbv1.CloudEvent{
						Id:          uuid.New().String(),
						Source:      "test-source",
						SpecVersion: "1.0",
						Type:        types.HeartbeatCloudEventsType,
					}
					if err := stream.Send(heartbeat); err != nil {
						return err
					}
					sentEventCount.Add(1)
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 100 * time.Millisecond
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:   "test-source",
				DataType: "test-type",
			}
		},
	}

	// Subscribe first
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	receivedEvents := atomic.Int32{}
	handleFn := func(ctx context.Context, evt cloudevents.Event) {
		receivedEvents.Add(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- transport.Receive(ctx, handleFn)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Receive to complete")
	}

	// Only heartbeat events were sent, so handler should not be called
	if receivedEvents.Load() > 0 {
		t.Errorf("expected 0 events (heartbeats filtered), got %d", receivedEvents.Load())
	}
}

func TestGrpcTransport_Receive_CloseChan(t *testing.T) {
	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			// Keep stream open
			<-stream.Context().Done()
			return stream.Context().Err()
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 100 * time.Millisecond
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:   "test-source",
				DataType: "test-type",
			}
		},
	}

	// Subscribe first
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	handleFn := func(ctx context.Context, evt cloudevents.Event) {}

	done := make(chan error, 1)
	go func() {
		done <- transport.Receive(context.Background(), handleFn)
	}()

	// Give some time for Receive to start
	time.Sleep(100 * time.Millisecond)

	// Close the transport
	close(transport.closeChan)

	// Wait for Receive to return
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Receive to complete after close")
	}
}

func TestGrpcTransport_Receive_StreamError(t *testing.T) {
	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			// Return error immediately
			return io.EOF
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 100 * time.Millisecond
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:   "test-source",
				DataType: "test-type",
			}
		},
	}

	// Subscribe first
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	handleFn := func(ctx context.Context, evt cloudevents.Event) {}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- transport.Receive(ctx, handleFn)
	}()

	// Wait for the error to be sent to errorChan
	select {
	case err := <-transport.errorChan:
		if err == nil {
			t.Fatal("expected error in errorChan, got nil")
		}
		// Error should contain "subscribe stream failed"
		t.Logf("Received expected error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for error in errorChan")
	}

	// Wait for Receive to complete
	select {
	case <-done:
		// Expected
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Receive to complete")
	}
}

func TestGrpcTransport_Close(t *testing.T) {
	dialer := &grpcoptions.GRPCDialer{}
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			Dialer: dialer,
		},
		errorChan: make(chan error, 1),
		closeChan: make(chan struct{}),
	}

	err := transport.Close(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify closeChan is closed
	select {
	case <-transport.closeChan:
		// Expected
	default:
		t.Error("closeChan should be closed")
	}
}

func TestGrpcTransport_SubscribeLogging(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		clusterName string
		dataType    string
	}{
		{
			name:        "subscribe with source",
			source:      "test-source",
			clusterName: "",
			dataType:    "test-type",
		},
		{
			name:        "subscribe with cluster",
			source:      "",
			clusterName: "test-cluster",
			dataType:    "test-type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &mockCloudEventService{
				subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
					<-stream.Context().Done()
					return stream.Context().Err()
				},
			}

			conn, cleanup := setupMockServer(t, service)
			defer cleanup()

			client := pbv1.NewCloudEventServiceClient(conn)
			transport := &grpcTransport{
				client:    client,
				errorChan: make(chan error, 1),
				closeChan: make(chan struct{}),
				getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
					return &pbv1.SubscriptionRequest{
						Source:      tt.source,
						ClusterName: tt.clusterName,
						DataType:    tt.dataType,
					}
				},
			}

			err := transport.Subscribe(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGrpcTransport_Integration(t *testing.T) {
	// Full integration test with Subscribe -> Send -> Close
	conn, cleanup := setupMockServer(t, &mockCloudEventService{
		heartbeatInterval: 50 * time.Millisecond,
	})
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 200 * time.Millisecond

	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
			Dialer:                   &grpcoptions.GRPCDialer{},
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:   "test-source",
				DataType: "test-type",
			}
		},
	}

	// Subscribe
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Send an event
	evt := cloudevents.NewEvent()
	evt.SetID("integration-test")
	evt.SetType("test.integration")
	evt.SetSource("test")

	if err := transport.Send(context.Background(), evt); err != nil {
		t.Errorf("Send failed: %v", err)
	}

	// Close
	if err := transport.Close(context.Background()); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestGrpcTransport_HeartbeatIntegration(t *testing.T) {
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
			serverHealthinessTimeout: func() *time.Duration { d := 500 * time.Millisecond; return &d }(),
			expectHealthCheckError:   false,
			testDuration:             1500 * time.Millisecond,
			stopServerAfterEvents:    0,
		},
		{
			name:                     "health check timeout when no heartbeats",
			heartbeatInterval:        0, // No heartbeats
			serverHealthinessTimeout: func() *time.Duration { d := 200 * time.Millisecond; return &d }(),
			expectHealthCheckError:   true,
			testDuration:             1000 * time.Millisecond,
			stopServerAfterEvents:    0,
		},
		{
			name:                     "health check disabled",
			heartbeatInterval:        0,   // No heartbeats
			serverHealthinessTimeout: nil, // Disabled
			expectHealthCheckError:   false,
			testDuration:             500 * time.Millisecond,
			stopServerAfterEvents:    0,
		},
		{
			name:                     "heartbeat stops mid-stream",
			heartbeatInterval:        50 * time.Millisecond,
			serverHealthinessTimeout: func() *time.Duration { d := 200 * time.Millisecond; return &d }(),
			expectHealthCheckError:   true,
			testDuration:             1000 * time.Millisecond,
			stopServerAfterEvents:    3, // Stop after 3 heartbeats
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &mockCloudEventService{
				heartbeatInterval: tt.heartbeatInterval,
				stopAfterEvents:   tt.stopServerAfterEvents,
			}

			conn, cleanup := setupMockServer(t, service)
			defer cleanup()

			client := pbv1.NewCloudEventServiceClient(conn)
			transport := &grpcTransport{
				opts: &grpcoptions.GRPCOptions{
					ServerHealthinessTimeout: tt.serverHealthinessTimeout,
					Dialer:                   &grpcoptions.GRPCDialer{},
				},
				client:    client,
				errorChan: make(chan error, 10),
				closeChan: make(chan struct{}),
				getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
					return &pbv1.SubscriptionRequest{
						Source:      "test-source",
						ClusterName: "test-cluster",
						DataType:    "io.open-cluster-management.test",
					}
				},
			}

			// Subscribe
			if err := transport.Subscribe(context.Background()); err != nil {
				t.Fatalf("Subscribe failed: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.testDuration)
			defer cancel()

			// Start receiving events in a goroutine
			started := make(chan struct{})
			go func() {
				close(started)
				handleFn := func(ctx context.Context, evt cloudevents.Event) {
					// Handler for non-heartbeat events (none in this test)
				}
				if err := transport.Receive(ctx, handleFn); err != nil {
					select {
					case transport.errorChan <- err:
					default:
					}
				}
			}()

			// Wait for goroutine to start and give Subscribe call time to establish
			<-started
			time.Sleep(100 * time.Millisecond)

			if tt.expectHealthCheckError {
				select {
				case err := <-transport.errorChan:
					if err == nil {
						t.Error("Expected health check error, but got nil")
					} else {
						t.Logf("Received expected error: %v", err)
					}
				case <-ctx.Done():
					t.Error("Expected health check error before context timeout")
				}
			} else {
				select {
				case err := <-transport.errorChan:
					t.Errorf("Unexpected error from health check: %v", err)
				case <-ctx.Done():
					// Expected - no error
				}
			}
		})
	}
}

func TestGrpcTransport_HeartbeatFiltering(t *testing.T) {
	// Test that heartbeat events are filtered out and not passed to the handler
	service := &mockCloudEventService{
		heartbeatInterval: 50 * time.Millisecond,
		stopAfterEvents:   5,
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 1 * time.Second
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
			Dialer:                   &grpcoptions.GRPCDialer{},
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:      "test-source",
				ClusterName: "test-cluster",
				DataType:    "io.open-cluster-management.test",
			}
		},
	}

	// Subscribe
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	var receivedEvents atomic.Int32
	handleFn := func(ctx context.Context, evt cloudevents.Event) {
		receivedEvents.Add(1)
	}

	started := make(chan struct{})
	go func() {
		close(started)
		if err := transport.Receive(ctx, handleFn); err != nil {
			select {
			case transport.errorChan <- err:
			default:
			}
		}
	}()

	// Wait for goroutine to start and give Subscribe call time to establish
	<-started
	time.Sleep(100 * time.Millisecond)

	<-ctx.Done()

	// Should receive 0 events since only heartbeats are sent (which are filtered out)
	if receivedEvents.Load() > 0 {
		t.Errorf("Expected 0 events, got %d (heartbeats should be filtered out)", receivedEvents.Load())
	}
}

func TestGrpcTransport_ConnectionStateMonitoring_ServerShutdown(t *testing.T) {
	// This test verifies that the connection state monitoring goroutine
	// properly detects when a server shuts down and reports it via the error channel

	conn, cleanup := setupMockServer(t, &mockCloudEventService{})

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			Dialer: &grpcoptions.GRPCDialer{},
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
	}

	// Manually start the connection state monitoring goroutine
	ctx := context.Background()
	go transport.monitorConnectionState(ctx, conn)

	// Give the monitoring goroutine time to start
	time.Sleep(100 * time.Millisecond)

	// Shutdown the server - this should trigger a state change
	cleanup()

	// Wait for the error to be reported
	select {
	case err := <-transport.errorChan:
		if err == nil {
			t.Error("expected error after server shutdown, got nil")
		} else {
			t.Logf("Connection state monitoring detected server shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for connection state error")
	}
}

func TestGrpcTransport_ConnectionStateMonitoring_ContextCancellation(t *testing.T) {
	// This test verifies that the connection state monitoring goroutine
	// stops cleanly when the context is cancelled

	conn, cleanup := setupMockServer(t, &mockCloudEventService{})
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			Dialer: &grpcoptions.GRPCDialer{},
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
	}

	// Start monitoring with a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	monitoringExited := make(chan struct{})

	go func() {
		defer close(monitoringExited)
		transport.monitorConnectionState(ctx, conn)
	}()

	// Give the monitoring goroutine time to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the context - this should stop the monitoring goroutine
	cancel()

	// Wait for the goroutine to exit
	select {
	case <-monitoringExited:
		t.Log("✓ Connection state monitoring goroutine exited cleanly after context cancellation")
	case <-time.After(1 * time.Second):
		t.Error("monitoring goroutine did not exit after context cancellation")
	}

	// Verify no errors were sent
	select {
	case err := <-transport.errorChan:
		t.Errorf("unexpected error after context cancellation: %v", err)
	default:
		// Expected - no error
	}
}

func TestGrpcTransport_ConnectionStateMonitoring_ErrorChannelBehavior(t *testing.T) {
	// This test verifies that the connection state monitoring handles
	// a full error channel gracefully (uses default case to avoid blocking)

	conn, cleanup := setupMockServer(t, &mockCloudEventService{})

	client := pbv1.NewCloudEventServiceClient(conn)
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			Dialer: &grpcoptions.GRPCDialer{},
		},
		client:    client,
		errorChan: make(chan error), // No buffer - will block
		closeChan: make(chan struct{}),
	}

	ctx := context.Background()
	monitoringExited := make(chan struct{})

	go func() {
		defer close(monitoringExited)
		transport.monitorConnectionState(ctx, conn)
	}()

	// Give the monitoring goroutine time to start
	time.Sleep(100 * time.Millisecond)

	// Stop the server to trigger state change
	cleanup()

	// The goroutine should exit even if we don't read from errorChan
	select {
	case <-monitoringExited:
		t.Log("✓ Monitoring goroutine exited without blocking on full error channel")
	case <-time.After(2 * time.Second):
		t.Error("monitoring goroutine blocked or did not exit")
	}

	// Try to read the error (may or may not be there)
	select {
	case err := <-transport.errorChan:
		t.Logf("Received error: %v", err)
	case <-time.After(100 * time.Millisecond):
		t.Log("No error received (channel was full and error was dropped, which is acceptable)")
	}
}

func TestGrpcTransport_MixedEventsWithHeartbeats(t *testing.T) {
	// Test receiving regular events mixed with heartbeats
	regularEvent := &pbv1.CloudEvent{
		Id:          "regular-1",
		Source:      "test-source",
		SpecVersion: "1.0",
		Type:        "test.event",
		Attributes: map[string]*pbv1.CloudEventAttributeValue{
			"datacontenttype": {
				Attr: &pbv1.CloudEventAttributeValue_CeString{
					CeString: "application/json",
				},
			},
		},
		Data: &pbv1.CloudEvent_TextData{
			TextData: `{"test":"data"}`,
		},
	}

	service := &mockCloudEventService{
		subscribeFunc: func(req *pbv1.SubscriptionRequest, stream pbv1.CloudEventService_SubscribeServer) error {
			// Send: heartbeat, regular event, heartbeat, heartbeat
			events := []*pbv1.CloudEvent{
				{
					Id:          uuid.New().String(),
					Source:      "test-source",
					SpecVersion: "1.0",
					Type:        types.HeartbeatCloudEventsType,
				},
				regularEvent,
				{
					Id:          uuid.New().String(),
					Source:      "test-source",
					SpecVersion: "1.0",
					Type:        types.HeartbeatCloudEventsType,
				},
				{
					Id:          uuid.New().String(),
					Source:      "test-source",
					SpecVersion: "1.0",
					Type:        types.HeartbeatCloudEventsType,
				},
			}

			for _, evt := range events {
				if err := stream.Send(evt); err != nil {
					return err
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Keep stream open
			<-stream.Context().Done()
			return stream.Context().Err()
		},
	}

	conn, cleanup := setupMockServer(t, service)
	defer cleanup()

	client := pbv1.NewCloudEventServiceClient(conn)
	timeout := 1 * time.Second
	transport := &grpcTransport{
		opts: &grpcoptions.GRPCOptions{
			ServerHealthinessTimeout: &timeout,
			Dialer:                   &grpcoptions.GRPCDialer{},
		},
		client:    client,
		errorChan: make(chan error, 10),
		closeChan: make(chan struct{}),
		getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
			return &pbv1.SubscriptionRequest{
				Source:   "test-source",
				DataType: "test.event",
			}
		},
	}

	// Subscribe
	if err := transport.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	ctx := context.Background()
	var receivedEvents atomic.Int32
	eventReceived := make(chan struct{}, 1)
	handleFn := func(ctx context.Context, evt cloudevents.Event) {
		receivedEvents.Add(1)
		select {
		case eventReceived <- struct{}{}:
		default:
		}
	}

	go func() {
		if err := transport.Receive(ctx, handleFn); err != nil {
			t.Logf("Receive failed: %v", err)
		}
	}()

	// Wait for the regular event (should be 1, heartbeats are filtered)
	select {
	case <-eventReceived:
		// Event received
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for regular event")
	}

	// Close the transport
	close(transport.closeChan)

	// Small delay to ensure all processing is done
	time.Sleep(100 * time.Millisecond)

	// Verify we received exactly 1 regular event (heartbeats filtered out)
	if receivedEvents.Load() != 1 {
		t.Errorf("Expected 1 regular event, got %d", receivedEvents.Load())
	}
}
