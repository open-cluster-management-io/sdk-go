package mq

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server"
)

type MessageQueueEventServer struct {
	services     map[types.CloudEventsDataType]server.Service
	sourceClient generic.CloudEventSourceClient[generic.ResourceObject]
}

func NewMessageQueueEventServer(client generic.CloudEventSourceClient[generic.ResourceObject]) server.AgentEventServer {
	s := &MessageQueueEventServer{
		services:     make(map[types.CloudEventsDataType]server.Service),
		sourceClient: client,
	}
	return s
}

func (s *MessageQueueEventServer) RegisterService(t types.CloudEventsDataType, service server.Service) {
	s.services[t] = service
	service.RegisterHandler(s)
}

// Start initializes and runs the event server. It starts the subscription
// to resource status update messages and the status dispatcher.
func (s *MessageQueueEventServer) Start(ctx context.Context) {
	log := klog.FromContext(ctx)
	log.Info("Starting message queue event server")

	// start subscribing to resource status update messages.
	s.startSubscription(ctx)

	// wait until context is canceled
	<-ctx.Done()
	log.Info("Shutting down message queue event server")
}

// startSubscription initiates the subscription to resource status update messages.
// It runs asynchronously in the background until the provided context is canceled.
func (s *MessageQueueEventServer) startSubscription(ctx context.Context) {
	s.sourceClient.SubscribeEvent(ctx, func(ctx context.Context, evt cloudevents.Event) {
		log := klog.FromContext(ctx)

		eventType, err := types.ParseCloudEventsType(evt.Type())
		if err != nil {
			log.Error(err, "failed to parse cloud event type")
			return
		}

		service, ok := s.services[eventType.CloudEventsDataType]
		if !ok {
			log.Error(fmt.Errorf("failed to find service for cloud event type %s", eventType.CloudEventsDataType), "")
		}

		if err := service.HandleStatusUpdate(ctx, &evt); err != nil {
			log.Error(err, "failed to handle status update")
		}
	})
}

// OnCreate will be called on each new resource creation event.
func (s *MessageQueueEventServer) OnCreate(ctx context.Context, t types.CloudEventsDataType, resourceID string) error {
	service, ok := s.services[t]
	if !ok {
		return fmt.Errorf("failed to find service for event type %s", t)
	}

	evt, err := service.Get(ctx, resourceID)
	// if the resource is not found, it indicates the resource has been processed.
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	return s.publishEventWithType(ctx, evt, t, "create_request")
}

// OnUpdate will be called on each new resource update event.
func (s *MessageQueueEventServer) OnUpdate(ctx context.Context, t types.CloudEventsDataType, resourceID string) error {
	service, ok := s.services[t]
	if !ok {
		return fmt.Errorf("failed to find service for event type %s", t)
	}

	evt, err := service.Get(ctx, resourceID)
	// if the resource is not found, it indicates the resource has been processed.
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.publishEventWithType(ctx, evt, t, "update_request")
}

// OnDelete will be called on each new resource deletion event.
func (s *MessageQueueEventServer) OnDelete(ctx context.Context, t types.CloudEventsDataType, resourceID string) error {
	service, ok := s.services[t]
	if !ok {
		return fmt.Errorf("failed to find service for event type %s", t)
	}

	evt, err := service.Get(ctx, resourceID)
	// if the resource is not found, it indicates the resource has been processed.
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.publishEventWithType(ctx, evt, t, "delete_request")
}

func (s *MessageQueueEventServer) publishEventWithType(
	ctx context.Context,
	evt *cloudevents.Event,
	t types.CloudEventsDataType,
	action types.EventAction) error {
	eventType := types.CloudEventsType{
		CloudEventsDataType: t,
		SubResource:         types.SubResourceSpec,
		Action:              action,
	}
	evt.SetType(eventType.String())
	return s.sourceClient.PublishEvent(ctx, *evt)
}
