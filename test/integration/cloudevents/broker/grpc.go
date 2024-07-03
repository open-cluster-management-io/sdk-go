package broker

import (
	"context"
	"fmt"
	"log"
	"net"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	cloudeventstypes "github.com/cloudevents/sdk-go/v2/types"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"k8s.io/klog/v2"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	grpcprotocol "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protocol"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/payload"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
)

type resourceHandler func(res *store.Resource) error

type GRPCBroker struct {
	pbv1.UnimplementedCloudEventServiceServer
	resourceStatusChan chan *store.Resource
	handlers           map[string]resourceHandler
	sourcerID          string
}

func NewGRPCBroker() *GRPCBroker {
	return &GRPCBroker{
		resourceStatusChan: make(chan *store.Resource),
		handlers:           make(map[string]resourceHandler),
	}
}

func (bkr *GRPCBroker) Publish(ctx context.Context, pubReq *pbv1.PublishRequest) (*emptypb.Empty, error) {
	// WARNING: don't use "evt, err := pb.FromProto(pubReq.Event)" to convert protobuf to cloudevent
	evt, err := binding.ToEvent(ctx, grpcprotocol.NewMessage(pubReq.Event))
	if err != nil {
		return nil, fmt.Errorf("failed to convert protobuf to cloudevent: %v", err)
	}

	eventType, err := types.ParseCloudEventsType(evt.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to parse cloud event type %s, %v", evt.Type(), err)
	}

	// handler resync request
	if eventType.Action == types.ResyncRequestAction {
		return &emptypb.Empty{}, nil
	}

	res, err := bkr.decode(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cloudevent: %v", err)
	}
	bkr.resourceStatusChan <- res

	return &emptypb.Empty{}, nil
}

func (bkr *GRPCBroker) Subscribe(subReq *pbv1.SubscriptionRequest, subServer pbv1.CloudEventService_SubscribeServer) error {
	if len(subReq.ClusterName) == 0 {
		return fmt.Errorf("invalid subscription request: missing cluster name")
	}
	bkr.handlers[subReq.ClusterName] = func(res *store.Resource) error {
		evt, err := bkr.encode(res)
		if err != nil {
			return fmt.Errorf("failed to encode resource %s to cloudevent: %v", res.ResourceID, err)
		}

		// WARNING: don't use "pbEvt, err := pb.ToProto(evt)" to convert cloudevent to protobuf
		pbEvt := &pbv1.CloudEvent{}
		if err = grpcprotocol.WritePBMessage(context.TODO(), binding.ToMessage(evt), pbEvt); err != nil {
			return fmt.Errorf("failed to convert cloudevent to protobuf: %v", err)
		}

		// send the cloudevent to the subscriber
		// TODO: error handling to address errors beyond network issues.
		if err := subServer.Send(pbEvt); err != nil {
			klog.Errorf("failed to send grpc event, %v", err)
		}

		return nil
	}

	<-subServer.Context().Done()

	return nil
}

func (bkr *GRPCBroker) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return err
	}
	grpcBroker := grpc.NewServer()
	pbv1.RegisterCloudEventServiceServer(grpcBroker, bkr)
	return grpcBroker.Serve(lis)
}

func (bkr *GRPCBroker) UpdateResourceSpec(resource *store.Resource) error {
	handleFn, ok := bkr.handlers[resource.Namespace]
	if !ok {
		return fmt.Errorf("failed to find handler for resource %s (%s)", resource.ResourceID, resource.Namespace)
	}

	if err := handleFn(resource); err != nil {
		return err
	}

	return nil
}

func (bkr *GRPCBroker) ResourceStatusChan() <-chan *store.Resource {
	return bkr.resourceStatusChan
}

func (bkr *GRPCBroker) SetSourceID(sourceID string) {
	bkr.sourcerID = sourceID
}

func (bkr *GRPCBroker) encode(resource *store.Resource) (*cloudevents.Event, error) {
	source := "test-source"
	if bkr.sourcerID != "" {
		source = bkr.sourcerID
	}
	eventType := types.CloudEventsType{
		CloudEventsDataType: payload.ManifestEventDataType,
		SubResource:         types.SubResourceSpec,
		Action:              "test_create_update_request",
	}

	eventBuilder := types.NewEventBuilder(source, eventType).
		WithResourceID(resource.ResourceID).
		WithResourceVersion(resource.ResourceVersion).
		WithClusterName(resource.Namespace)

	if !resource.DeletionTimestamp.IsZero() {
		eventBuilder.WithDeletionTimestamp(resource.DeletionTimestamp.Time)
	}

	evt := eventBuilder.NewEvent()

	if err := evt.SetData(cloudevents.ApplicationJSON, &payload.Manifest{Manifest: resource.Spec}); err != nil {
		return nil, fmt.Errorf("failed to encode manifest spec to cloud event: %v", err)
	}

	return &evt, nil
}

func (bkr *GRPCBroker) decode(evt *cloudevents.Event) (*store.Resource, error) {
	eventType, err := types.ParseCloudEventsType(evt.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to parse cloud event type %s, %v", evt.Type(), err)
	}

	if eventType.CloudEventsDataType != payload.ManifestEventDataType {
		return nil, fmt.Errorf("unsupported cloudevents data type %s", eventType.CloudEventsDataType)
	}

	evtExtensions := evt.Context.GetExtensions()

	resourceID, err := cloudeventstypes.ToString(evtExtensions[types.ExtensionResourceID])
	if err != nil {
		return nil, fmt.Errorf("failed to get resourceid extension: %v", err)
	}

	resourceVersion, err := cloudeventstypes.ToInteger(evtExtensions[types.ExtensionResourceVersion])
	if err != nil {
		return nil, fmt.Errorf("failed to get resourceversion extension: %v", err)
	}

	clusterName, err := cloudeventstypes.ToString(evtExtensions[types.ExtensionClusterName])
	if err != nil {
		return nil, fmt.Errorf("failed to get clustername extension: %v", err)
	}

	status := &payload.ManifestStatus{}
	if err := evt.DataAs(status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event data %s, %v", string(evt.Data()), err)
	}

	return &store.Resource{
		Source:          evt.Source(),
		ResourceID:      resourceID,
		ResourceVersion: int64(resourceVersion),
		Namespace:       clusterName,
		Status:          store.ResourceStatus{Conditions: status.Conditions},
	}, nil
}
