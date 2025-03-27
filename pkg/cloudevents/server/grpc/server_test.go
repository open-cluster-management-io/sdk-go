package grpc

import (
	"context"
	"errors"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc"
	grpccli "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server"
	"testing"
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

func (s *testService) Get(ctx context.Context, resourceID string) (*cloudevents.Event, error) {
	evt, ok := s.evts[resourceID]
	if !ok {
		return nil, errors.New("not found")
	}
	return evt, nil
}

// List the cloudEvent from the service
func (s *testService) List(listOpts cetypes.ListOptions) ([]*cloudevents.Event, error) {
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
func (s *testService) RegisterHandler(handler server.EventHandler) {
	s.handler = handler
}

func (s *testService) create(evt *cloudevents.Event) error {
	s.evts[evt.ID()] = evt
	return s.handler.OnCreate(context.TODO(), dataType, evt.ID())
}

func TestServer(t *testing.T) {
	grpcServerOptions := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcServerOptions...)
	grpcEventServer := NewGRPCBroker(grpcServer, ":8888")

	svc := &testService{evts: make(map[string]*cloudevents.Event)}
	grpcEventServer.RegisterService(dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go grpcEventServer.Start(ctx)

	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: "localhost:8888"}
	agentOption := grpccli.NewAgentOptions(grpcClientOptions, "cluster1", "agent1")
	protocol, err := agentOption.CloudEventsOptions.Protocol(ctx, dataType)
	if err != nil {
		t.Fatal(err)
	}

	cloudEventsClient, err := cloudevents.NewClient(protocol)
	if err != nil {
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

	recivedEventCh := make(chan cloudevents.Event)
	go func() {
		if err := cloudEventsClient.StartReceiver(ctx, func(event cloudevents.Event) {
			recivedEventCh <- event
		}); err != nil {
			t.Error(err)
		}
	}()

	if result := cloudEventsClient.Send(ctx, evt); result != nil {
		t.Error(result)
	}

	if _, ok := svc.evts[evt.ID()]; !ok {
		t.Error("event not found")
	}
	if err := svc.create(&evt2); err != nil {
		t.Fatal(err)
	}

	recievedEvent := <-recivedEventCh
	if recievedEvent.ID() != evt2.ID() {
		t.Error("received event is different")
	}
}
