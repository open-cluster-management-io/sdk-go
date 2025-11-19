package cloudevents

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	mqttv2 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/v2/mqtt"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var _ = ginkgo.Describe("CloudEvents Clients Test - MQTT", runCloudeventsClientPubSubTest(GetMQTTSourceOptions))

func GetMQTTSourceOptions(_ context.Context, sourceID string) (*options.CloudEventsSourceOptions, string) {
	return mqttv2.NewSourceOptions(
		util.NewMQTTSourceOptions(mqttBrokerHost, sourceID),
		fmt.Sprintf("%s-client", sourceID),
		sourceID,
	), constants.ConfigTypeMQTT
}
