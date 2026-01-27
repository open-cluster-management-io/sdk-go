package services

import (
	"context"
	"errors"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
)

type ResourceStatusHandler func(res *store.Resource) error

type ResourceService struct {
	handler       server.EventHandler
	statusHandler ResourceStatusHandler
	serverStore   *store.MemoryStore
	codec         *source.ResourceCodec
}

func NewResourceService(statusHandler ResourceStatusHandler, serverStore *store.MemoryStore) *ResourceService {
	return &ResourceService{
		statusHandler: statusHandler,
		serverStore:   serverStore,
		codec:         &source.ResourceCodec{},
	}
}

func (s *ResourceService) List(_ context.Context, listOpts types.ListOptions) ([]*cloudevents.Event, error) {
	resources := s.serverStore.List(listOpts.ClusterName)
	events := make([]*cloudevents.Event, 0, len(resources))
	for _, resource := range resources {
		action := types.UpdateRequestAction
		if !resource.DeletionTimestamp.IsZero() {
			action = types.DeleteRequestAction
		}

		evt, err := s.codec.Encode(resource.Source, types.CloudEventsType{
			CloudEventsDataType: payload.ManifestBundleEventDataType,
			SubResource:         types.SubResourceSpec,
			Action:              action,
		}, resource)
		if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}

	return events, nil
}

func (s *ResourceService) HandleStatusUpdate(_ context.Context, evt *cloudevents.Event) error {
	resource, err := s.codec.Decode(evt)
	if err != nil {
		return err
	}

	return s.statusHandler(resource)
}

func (s *ResourceService) RegisterHandler(_ context.Context, handler server.EventHandler) {
	s.handler = handler
}

func (s *ResourceService) UpdateResourceSpec(resource *store.Resource) error {
	action := types.UpdateRequestAction
	if !resource.DeletionTimestamp.IsZero() {
		action = types.DeleteRequestAction
	}

	evt, err := s.codec.Encode(resource.Source, types.CloudEventsType{
		CloudEventsDataType: payload.ManifestBundleEventDataType,
		SubResource:         types.SubResourceSpec,
		Action:              action,
	}, resource)
	if err != nil {
		return err
	}
	if s.handler == nil {
		return errors.New("event handler not registered")
	}
	return s.handler.HandleEvent(context.Background(), evt)
}
