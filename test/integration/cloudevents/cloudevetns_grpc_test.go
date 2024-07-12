package cloudevents

import (
	"context"
	"log"

	"github.com/onsi/ginkgo"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
)

var _ = ginkgo.Describe("CloudEvents Clients Test - GRPC", runCloudeventsClientPubSubTest(GetGRPCSourceOptions))

// The GRPC test simulates there is a server between the source and agent, the GRPC source client
// sends/receives events to/from server, then server forward the events to agent via GRPC broker.
func GetGRPCSourceOptions(ctx context.Context, sourceID string) (*options.CloudEventsSourceOptions, string) {
	// set sourceID for grpc broker
	grpcBroker.SetSourceID(sourceID)

	// forward the resource to agent and listen for the status from grpc broker
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-grpcServer.ResourceChan():
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
