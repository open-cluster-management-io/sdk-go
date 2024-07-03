package cloudevents

import (
	"context"
	"fmt"
	"log"

	"github.com/onsi/ginkgo"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var _ = ginkgo.Describe("CloudEvents Clients Test - GRPC", runCloudeventsClientPubSubTest(GetGRPCSourceOptions))

// The GRPC test simulates there is a server between the source and agent, the GRPC source client
// sends/receives events to/from server, then server forward the events to agent with mqtt
func GetGRPCSourceOptions(ctx context.Context, sourceID string) (*options.CloudEventsSourceOptions, string) {
	client, err := generic.NewCloudEventSourceClient[*store.Resource](
		ctx,
		mqtt.NewSourceOptions(
			util.NewMQTTSourceOptions(mqttBrokerHost, sourceID),
			fmt.Sprintf("%s-client", sourceID),
			sourceID,
		),
		source.NewResourceLister(grpcServer.GetStore()),
		source.StatusHashGetter,
		&source.ResourceCodec{},
	)
	if err != nil {
		log.Fatal(err)
	}

	client.Subscribe(ctx, func(action types.ResourceAction, resource *store.Resource) error {
		return grpcServer.UpdateResourceStatus(resource)
	})

	// set sourceID for grpc broker
	grpcBroker.SetSourceID(sourceID)

	// forward the resource to agent and listen for the status from grpc broker
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-grpcServer.ResourceChan():
				requestType := createOrUpdateRequest
				if !res.DeletionTimestamp.IsZero() {
					requestType = deleteRequest
				}
				err := client.Publish(ctx, requestType, res)
				if err != nil {
					log.Printf("failed to publish resource to mqtt %s, %v", res.ResourceID, err)
				}
				if err := grpcBroker.UpdateResourceSpec(res); err != nil {
					log.Printf("failed to update resource spec via gRPC broker %s, %v", res.ResourceID, err)
				}
			case res := <-grpcBroker.ResourceStatusChan():
				// replace the source id with the original source id
				res.Source = sourceID
				if err := grpcServer.UpdateResourceStatus(res); err != nil {
					log.Printf("failed to update resource status %s, %v", res.ResourceID, err)
				}
			}
		}
	}()

	grpcOptions := grpc.NewGRPCOptions()
	grpcOptions.URL = grpcServerHost
	return grpc.NewSourceOptions(grpcOptions, sourceID), constants.ConfigTypeGRPC
}
