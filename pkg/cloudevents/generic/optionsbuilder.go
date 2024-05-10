package generic

import (
	"fmt"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

// BuildCloudEventsSourceOptions builds the cloudevents source options based on the broker type
func BuildCloudEventsSourceOptions(config any, clientId, sourceId string) (*options.CloudEventsSourceOptions, error) {
	switch config := config.(type) {
	case *mqtt.MQTTOptions:
		return mqtt.NewSourceOptions(config, clientId, sourceId), nil
	case *grpc.GRPCOptions:
		return grpc.NewSourceOptions(config, sourceId), nil
	case *map[string]interface{}:
		return kafka.NewSourceOptions(config, sourceId), nil
	default:
		return nil, fmt.Errorf("unsupported client configuration type %T", config)
	}
}

// BuildCloudEventsAgentOptions builds the cloudevents agent options based on the broker type
func BuildCloudEventsAgentOptions(config any, clusterName, clientId string) (*options.CloudEventsAgentOptions, error) {
	switch config := config.(type) {
	case *mqtt.MQTTOptions:
		return mqtt.NewAgentOptions(config, clusterName, clientId), nil
	case *grpc.GRPCOptions:
		return grpc.NewAgentOptions(config, clusterName, clientId), nil
	case *map[string]interface{}:
		return kafka.NewAgentOptions(config, clusterName, clientId), nil
	default:
		return nil, fmt.Errorf("unsupported client configuration type %T", config)
	}
}
