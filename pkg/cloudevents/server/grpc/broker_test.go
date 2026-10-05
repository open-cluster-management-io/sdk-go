package grpc

import (
	"context"
	"net"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpccli "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	grpcprotocol "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protocol"
	grpcv2 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/v2/grpc"
	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server"
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authz"
)

var dataType = cetypes.CloudEventsDataType{
	Group:    "test",
	Version:  "v1",
	Resource: "tests",
}

type testService struct {
	evts    map[string]*cloudevents.Event
	handler server.EventHandler
}

// List the cloudEvent from the service
func (s *testService) List(_ context.Context, listOpts cetypes.ListOptions) ([]*cloudevents.Event, error) {
	evts := make([]*cloudevents.Event, 0, len(s.evts))
	for _, evt := range s.evts {
		evts = append(evts, evt)
	}
	return evts, nil
}

// HandleStatusUpdate processes the resource status update from the agent.
func (s *testService) HandleStatusUpdate(ctx context.Context, evt *cloudevents.Event) error {
	s.evts[evt.ID()] = evt
	return nil
}

// RegisterHandler register the handler to the service.
func (s *testService) RegisterHandler(_ context.Context, handler server.EventHandler) {
	s.handler = handler
}

func (s *testService) create(evt *cloudevents.Event) error {
	s.evts[evt.ID()] = evt
	return s.handler.HandleEvent(context.TODO(), evt)
}

func TestPublish(t *testing.T) {
	statusUpdateType := cetypes.CloudEventsType{CloudEventsDataType: dataType, SubResource: cetypes.SubResourceStatus, Action: cetypes.UpdateRequestAction}
	newStatusEvent := func(resourceID string) *cloudevents.Event {
		evt := cetypes.NewEventBuilder("agent1", statusUpdateType).WithResourceID(resourceID).WithClusterName("cluster1").NewEvent()
		return &evt
	}
	toPublishRequest := func(evt *cloudevents.Event) *pbv1.PublishRequest {
		pbEvt := &pbv1.CloudEvent{}
		if err := grpcprotocol.WritePBMessage(context.Background(), binding.ToMessage(evt), pbEvt); err != nil {
			t.Fatalf("failed to convert event to protobuf: %v", err)
		}
		return &pbv1.PublishRequest{Event: pbEvt}
	}

	broker := NewGRPCBroker(NewBrokerOptions())
	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	broker.RegisterService(context.Background(), dataType, svc)

	t.Run("rejects a request without an event", func(t *testing.T) {
		for _, req := range []*pbv1.PublishRequest{nil, {}} {
			if _, err := broker.Publish(context.Background(), req); status.Code(err) != codes.InvalidArgument {
				t.Errorf("expected InvalidArgument, got %v", err)
			}
		}
	})

	t.Run("decodes the request event when no authorized event is in the context", func(t *testing.T) {
		evt := newStatusEvent("decoded")
		if _, err := broker.Publish(context.Background(), toPublishRequest(evt)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := svc.evts[evt.ID()]; !ok {
			t.Errorf("expected the service to receive event %s", evt.ID())
		}
	})

	t.Run("handles the authorized event from the context instead of re-decoding the request", func(t *testing.T) {
		requestEvt := newStatusEvent("request")
		authorizedEvt := newStatusEvent("authorized")

		ctx := authz.WithAuthorizedEvent(context.Background(), authorizedEvt)
		if _, err := broker.Publish(ctx, toPublishRequest(requestEvt)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, ok := svc.evts[authorizedEvt.ID()]; !ok || got != authorizedEvt {
			t.Errorf("expected the service to receive the authorized event %s", authorizedEvt.ID())
		}
		if _, ok := svc.evts[requestEvt.ID()]; ok {
			t.Errorf("expected the request event %s not to be handled", requestEvt.ID())
		}
	})
}

func TestServer(t *testing.T) {
	grpcServerOptions := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcServerOptions...)
	defer grpcServer.Stop()

	grpcEventServer := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	grpcEventServer.RegisterService(context.Background(), dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Errorf("failed to serve: %v", err)
		}
	}()

	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}
	agentOption := grpcv2.NewAgentOptions(grpcClientOptions, "cluster1", "agent1", dataType)
	if err := agentOption.CloudEventsTransport.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	if err := agentOption.CloudEventsTransport.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	evt := cetypes.NewEventBuilder("agent1",
		cetypes.CloudEventsType{CloudEventsDataType: dataType, SubResource: cetypes.SubResourceSpec}).
		WithResourceID("test1").
		WithClusterName("cluster1").NewEvent()
	evt2 := cetypes.NewEventBuilder("agent1",
		cetypes.CloudEventsType{CloudEventsDataType: dataType, SubResource: cetypes.SubResourceSpec}).
		WithResourceID("test2").
		WithClusterName("cluster1").NewEvent()

	receivedEventCh := make(chan cloudevents.Event)
	go func() {
		if err := agentOption.CloudEventsTransport.Receive(ctx, func(ctx context.Context, event cloudevents.Event) {
			receivedEventCh <- event
		}); err != nil {
			t.Error(err)
		}
	}()

	if result := agentOption.CloudEventsTransport.Send(ctx, evt); result != nil {
		t.Error(result)
	}

	if _, ok := svc.evts[evt.ID()]; !ok {
		t.Error("event not found")
	}
	if err := svc.create(&evt2); err != nil {
		t.Fatal(err)
	}

	receivedEvent := <-receivedEventCh
	if receivedEvent.ID() != evt2.ID() {
		t.Error("received event is different")
	}
}

// TestSubscriptionHeaderImmediateSend verifies that the subscription ID header
// is sent immediately upon subscription, preventing the "got 0 headers" error.
func TestSubscriptionHeaderImmediateSend(t *testing.T) {
	grpcServerOptions := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcServerOptions...)
	defer grpcServer.Stop()

	grpcEventServer := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	grpcEventServer.RegisterService(context.Background(), dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Errorf("failed to serve: %v", err)
		}
	}()

	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}
	agentOption := grpcv2.NewAgentOptions(grpcClientOptions, "cluster1", "agent1", dataType)

	// Test that connection and subscription work without "got 0 headers" error
	if err := agentOption.CloudEventsTransport.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if err := agentOption.CloudEventsTransport.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe, expected header to be sent immediately: %v", err)
	}
}

// TestReconnectionScenario simulates a client restart (disconnect and reconnect)
// to verify that subscription headers are properly sent on reconnection.
func TestReconnectionScenario(t *testing.T) {
	grpcServerOptions := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcServerOptions...)
	defer grpcServer.Stop()

	grpcEventServer := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	grpcEventServer.RegisterService(context.Background(), dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Errorf("failed to serve: %v", err)
		}
	}()

	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}

	// First connection and subscription
	agentOption := grpcv2.NewAgentOptions(grpcClientOptions, "cluster1", "agent1", dataType)
	if err := agentOption.CloudEventsTransport.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if err := agentOption.CloudEventsTransport.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe on first connection: %v", err)
	}

	// Simulate client restart by closing and reconnecting
	if err := agentOption.CloudEventsTransport.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Create a new transport for reconnection
	grpcClientOptions2 := grpccli.NewGRPCOptions()
	grpcClientOptions2.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}
	agentOption2 := grpcv2.NewAgentOptions(grpcClientOptions2, "cluster1", "agent1", dataType)

	// Reconnect
	if err := agentOption2.CloudEventsTransport.Connect(ctx); err != nil {
		t.Fatalf("failed to reconnect: %v", err)
	}

	// This should not fail with "got 0 headers" error
	if err := agentOption2.CloudEventsTransport.Subscribe(ctx); err != nil {
		t.Fatalf("failed to subscribe after reconnection: %v", err)
	}
}

// TestConcurrentSubscriptions verifies that multiple clients can subscribe
// simultaneously without header race conditions.
func TestConcurrentSubscriptions(t *testing.T) {
	grpcServerOptions := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcServerOptions...)
	defer grpcServer.Stop()

	grpcEventServer := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	grpcEventServer.RegisterService(context.Background(), dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Errorf("failed to serve: %v", err)
		}
	}()

	// Create and subscribe multiple clients concurrently
	numClients := 10
	errCh := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			grpcClientOptions := grpccli.NewGRPCOptions()
			grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}
			agentOption := grpcv2.NewAgentOptions(grpcClientOptions, "cluster1", "agent1", dataType)

			if err := agentOption.CloudEventsTransport.Connect(ctx); err != nil {
				errCh <- err
				return
			}

			if err := agentOption.CloudEventsTransport.Subscribe(ctx); err != nil {
				errCh <- err
				return
			}

			errCh <- nil
		}(i)
	}

	// Wait for all clients to complete
	for i := 0; i < numClients; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("client %d failed: %v", i, err)
		}
	}
}

// TestMultipleRapidReconnections simulates rapid reconnection scenarios
// that could trigger the race condition.
func TestMultipleRapidReconnections(t *testing.T) {
	grpcServerOptions := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcServerOptions...)
	defer grpcServer.Stop()

	grpcEventServer := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	grpcEventServer.RegisterService(context.Background(), dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Errorf("failed to serve: %v", err)
		}
	}()

	// Perform multiple rapid reconnections
	for i := 0; i < 5; i++ {
		grpcClientOptions := grpccli.NewGRPCOptions()
		grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}
		agentOption := grpcv2.NewAgentOptions(grpcClientOptions, "cluster1", "agent1", dataType)

		if err := agentOption.CloudEventsTransport.Connect(ctx); err != nil {
			t.Fatalf("reconnection %d: failed to connect: %v", i, err)
		}

		if err := agentOption.CloudEventsTransport.Subscribe(ctx); err != nil {
			t.Fatalf("reconnection %d: failed to subscribe: %v", i, err)
		}

		if err := agentOption.CloudEventsTransport.Close(ctx); err != nil {
			t.Fatalf("reconnection %d: failed to close: %v", i, err)
		}
	}
}
