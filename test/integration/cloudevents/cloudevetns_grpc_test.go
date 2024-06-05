package cloudevents

import (
	"context"
	"fmt"
	"log"

	"github.com/onsi/ginkgo"

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
func GetGRPCSourceOptions(ctx context.Context, sourceID string) *options.CloudEventsSourceOptions {
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
		return grpcServer.GetStore().UpdateStatus(resource)
	})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-grpcServer.GetStore().GetResourceSpecChan():
				requestType := createOrUpdateRequest
				if !res.DeletionTimestamp.IsZero() {
					requestType = deleteRequest
				}
				err := client.Publish(ctx, requestType, res)
				if err != nil {
					log.Printf("failed to publish resource to mqtt %s, %v", res.ResourceID, err)
				}
			}
		}
	}()

	grpcOptions := grpc.NewGRPCOptions()
	grpcOptions.URL = grpcServerHost
	return grpc.NewSourceOptions(grpcOptions, sourceID)
}
