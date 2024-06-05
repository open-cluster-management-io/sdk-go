package source

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventstypes "github.com/cloudevents/sdk-go/v2/types"
	"k8s.io/apimachinery/pkg/util/rand"

	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workv1informers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/source/codec"
	workstore "open-cluster-management.io/sdk-go/pkg/cloudevents/work/store"
)

type ResourceCodec struct{}

var _ generic.Codec[*Resource] = &ResourceCodec{}

func (c *ResourceCodec) EventDataType() types.CloudEventsDataType {
	return payload.ManifestEventDataType
}

func (c *ResourceCodec) Encode(source string, eventType types.CloudEventsType, resource *Resource) (*cloudevents.Event, error) {
	if resource.Source != "" {
		source = resource.Source
	}

	if eventType.CloudEventsDataType != payload.ManifestEventDataType {
		return nil, fmt.Errorf("unsupported cloudevents data type %s", eventType.CloudEventsDataType)
	}

	eventBuilder := types.NewEventBuilder(source, eventType).
		WithResourceID(resource.ResourceID).
		WithResourceVersion(resource.ResourceVersion).
		WithClusterName(resource.Namespace)

	if !resource.GetDeletionTimestamp().IsZero() {
		evt := eventBuilder.WithDeletionTimestamp(resource.GetDeletionTimestamp().Time).NewEvent()
		return &evt, nil
	}

	evt := eventBuilder.NewEvent()

	if err := evt.SetData(cloudevents.ApplicationJSON, &payload.Manifest{Manifest: resource.Spec}); err != nil {
		return nil, fmt.Errorf("failed to encode manifests to cloud event: %v", err)
	}

	return &evt, nil
}

func (c *ResourceCodec) Decode(evt *cloudevents.Event) (*Resource, error) {
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

	originalSource, err := cloudeventstypes.ToString(evtExtensions[types.ExtensionOriginalSource])
	if err != nil {
		return nil, fmt.Errorf("failed to get originalsource extension: %v", err)
	}

	manifestStatus := &payload.ManifestStatus{}
	if err := evt.DataAs(manifestStatus); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event data %s, %v", string(evt.Data()), err)
	}

	resource := &Resource{
		Source:          originalSource,
		ResourceID:      resourceID,
		ResourceVersion: int64(resourceVersion),
		Namespace:       clusterName,
		Status: ResourceStatus{
			Conditions: manifestStatus.Conditions,
		},
	}

	return resource, nil
}

type resourceLister struct{}

var _ generic.Lister[*Resource] = &resourceLister{}

func (resLister *resourceLister) List(listOpts types.ListOptions) ([]*Resource, error) {
	return store.List(listOpts.ClusterName), nil
}

func StatusHashGetter(obj *Resource) (string, error) {
	statusBytes, err := json.Marshal(&workv1.ManifestWorkStatus{Conditions: obj.Status.Conditions})
	if err != nil {
		return "", fmt.Errorf("failed to marshal resource status, %v", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(statusBytes)), nil
}

func StartMQTTResourceSourceClient(ctx context.Context, config *mqtt.MQTTOptions, sourceID string, resSpecChan <-chan *Resource) (generic.CloudEventsClient[*Resource], error) {
	client, err := generic.NewCloudEventSourceClient[*Resource](
		ctx,
		mqtt.NewSourceOptions(config, fmt.Sprintf("%s-client", sourceID), sourceID),
		&resourceLister{},
		StatusHashGetter,
		&ResourceCodec{},
	)
	if err != nil {
		return nil, err
	}

	client.Subscribe(ctx, func(action types.ResourceAction, resource *Resource) error {
		return store.UpdateStatus(resource)
	})

	go func() {
		for res := range resSpecChan {
			action := "test_create_update_request"
			if !res.DeletionTimestamp.IsZero() {
				action = "test_delete_request"
			}
			err := client.Publish(ctx, types.CloudEventsType{
				CloudEventsDataType: payload.ManifestEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.EventAction(action),
			}, res)
			if err != nil {
				log.Printf("failed to publish resource to mqtt %s, %v", res.ResourceID, err)
			}
		}
	}()

	return client, nil
}

type consumerResourceLister struct{}

var _ generic.Lister[*Resource] = &consumerResourceLister{}

func (consumerResLister *consumerResourceLister) List(listOpts types.ListOptions) ([]*Resource, error) {
	return consumerStore.List(listOpts.ClusterName), nil
}

func StartGRPCResourceSourceClient(ctx context.Context, config *grpc.GRPCOptions) (generic.CloudEventsClient[*Resource], error) {
	client, err := generic.NewCloudEventSourceClient[*Resource](
		ctx,
		grpc.NewSourceOptions(config, "integration-test"),
		&consumerResourceLister{},
		StatusHashGetter,
		&ResourceCodec{},
	)
	if err != nil {
		return nil, err
	}

	client.Subscribe(ctx, func(action types.ResourceAction, resource *Resource) error {
		return consumerStore.UpdateStatus(resource)
	})

	return client, nil
}

func StartManifestWorkSourceClient(
	ctx context.Context,
	sourceID string,
	config any,
) (*work.ClientHolder, workv1informers.ManifestWorkInformer, error) {
	watcherStore := workstore.NewInformerWatcherStore(ctx)

	clientHolder, err := work.NewClientHolderBuilder(config).
		WithClientID(fmt.Sprintf("%s-%s", sourceID, rand.String(5))).
		WithSourceID(sourceID).
		WithCodecs(codec.NewManifestBundleCodec()).
		WithWorkClientWatcherStore(watcherStore).
		NewSourceClientHolder(ctx)
	if err != nil {
		return nil, nil, err
	}

	factory := workinformers.NewSharedInformerFactoryWithOptions(clientHolder.WorkInterface(), 5*time.Minute)
	informer := factory.Work().V1().ManifestWorks()

	watcherStore.SetStore(informer.Informer().GetStore())

	go informer.Informer().Run(ctx.Done())

	return clientHolder, informer, nil
}
