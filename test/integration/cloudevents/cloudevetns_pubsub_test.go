package cloudevents

import (
	"context"

	"github.com/onsi/ginkgo"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/v2/pubsub"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var _ = ginkgo.Describe("CloudEvents Clients Test - PubSub", runCloudeventsClientPubSubTest(GetPubSubSourceOptions))

func GetPubSubSourceOptions(_ context.Context, sourceID string) (*options.CloudEventsSourceOptions, string) {
	return pubsub.NewSourceOptions(
		util.NewPubSubSourceOptions(pubsubServer.Addr, pubsubProjectID, sourceID),
		sourceID,
	), constants.ConfigTypePubSub
}
