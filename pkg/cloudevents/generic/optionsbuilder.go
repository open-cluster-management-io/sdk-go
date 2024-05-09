package generic

import (
	"fmt"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/constants"
)

// BuildCloudEventsSourceOptions builds the cloudevents source options based on the broker type
func BuildCloudEventsSourceOptions(messageBrokerType, messageBrokerConfigPath, clientId, sourceId string) (*options.CloudEventsSourceOptions, error) {
	switch messageBrokerType {
	case constants.ConfigTypeMQTT:
		mqttOptions, err := mqtt.BuildMQTTOptionsFromFlags(messageBrokerConfigPath)
		if err != nil {
			return nil, err
		}
		return mqtt.NewSourceOptions(mqttOptions, clientId, sourceId), nil

	case constants.ConfigTypeKafka:
		kafkaConfigmap, err := kafka.BuildKafkaOptionsFromFlags(messageBrokerConfigPath)
		if err != nil {
			return nil, err
		}
		return kafka.NewSourceOptions(kafkaConfigmap, sourceId), nil

	case constants.ConfigTypeGRPC:
		grpcOptions, err := grpc.BuildGRPCOptionsFromFlags(messageBrokerConfigPath)
		if err != nil {
			return nil, err
		}
		return grpc.NewSourceOptions(grpcOptions, sourceId), nil

	}
	return nil, fmt.Errorf("unsupported message broker type %s", messageBrokerType)
}
