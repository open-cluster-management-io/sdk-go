//go:build !kafka

package kafka

import (
	"fmt"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
)

func NewSourceOptions(configMap *map[string]interface{}, sourceID string) *options.CloudEventsSourceOptions {
	return nil
}

func NewAgentOptions(configMap *map[string]interface{}, clusterName, agentID string) *options.CloudEventsAgentOptions {
	return nil
}

func BuildKafkaOptionsFromFlags(configPath string) (*map[string]interface{}, error) {
	return nil, fmt.Errorf("try adding -tags=kafka to build your app")
}
