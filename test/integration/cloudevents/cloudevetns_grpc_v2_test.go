package cloudevents

import (
	"context"

	"github.com/onsi/ginkgo"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	grpcv2 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/v2/grpc"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var _ = ginkgo.Describe("CloudEvents Clients Test - GRPC V2", runCloudeventsClientPubSubTest(GetGRPCSourceOptionsV2))

// The GRPC test simulates there is a server between the source and agent, the GRPC source client
// sends/receives events to/from server, then server forward the events to agent via GRPC broker.
func GetGRPCSourceOptionsV2(_ context.Context, sourceID string) (*options.CloudEventsSourceOptions, string) {
	return grpcv2.NewSourceOptions(
			util.NewGRPCSourceOptions(grpcServerHost), sourceID, payload.ManifestBundleEventDataType),
		constants.ConfigTypeGRPC
}
