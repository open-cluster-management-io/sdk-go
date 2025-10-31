package builder

import (
	"fmt"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

// ConfigLoader loads a configuration object with a configuration file.
type ConfigLoader struct {
	configType string
	configPath string
}

// NewConfigLoader returns a ConfigLoader with the given configuration type and configuration file path.
//
// Available configuration types:
//   - mqtt
//   - grpc
func NewConfigLoader(configType, configPath string) *ConfigLoader {
	return &ConfigLoader{
		configType: configType,
		configPath: configPath,
	}
}

// TODO using a specified config instead of any
func (l *ConfigLoader) LoadConfig() (string, any, error) {
	switch l.configType {
	case constants.ConfigTypeMQTT:
		mqttOptions, err := mqtt.BuildMQTTOptionsFromFlags(l.configPath)
		if err != nil {
			return "", nil, err
		}

		return mqttOptions.Dialer.BrokerHost, mqttOptions, nil
	case constants.ConfigTypeGRPC:
		grpcOptions, err := grpc.BuildGRPCOptionsFromFlags(l.configPath)
		if err != nil {
			return "", nil, err
		}

		return grpcOptions.Dialer.URL, grpcOptions, nil
	}

	return "", nil, fmt.Errorf("unsupported config type %s", l.configType)
}

// BuildCloudEventsSourceOptions builds the cloudevents source options based on the broker type
func BuildCloudEventsSourceOptions(config any, clientId, sourceId string) (*options.CloudEventsSourceOptions, error) {
	switch config := config.(type) {
	case *mqtt.MQTTOptions:
		return mqtt.NewSourceOptions(config, clientId, sourceId), nil
	case *grpc.GRPCOptions:
		return grpc.NewSourceOptions(config, sourceId), nil
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
	default:
		return nil, fmt.Errorf("unsupported client configuration type %T", config)
	}
}
