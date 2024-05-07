//go:build !kafka

// This is the dummy code to pass the compile when Kafka is not enabled.
// Cannot enable Kafka by default due to confluent-kafka-go not supporting cross-compilation.
// Try adding -tags=kafka to build when you need Kafka.
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
	return nil, fmt.Errorf("try adding -tags=kafka to build")
}
