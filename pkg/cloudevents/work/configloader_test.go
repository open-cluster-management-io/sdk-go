package work

import (
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"k8s.io/apimachinery/pkg/api/equality"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	kafkaoption "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

const (
	mqttConfig = `
brokerHost: mqtt
topics:
  sourceEvents: sources/hub1/clusters/+/sourceevents
  agentEvents: sources/hub1/clusters/+/agentevents
`
	grpcConfig = `
url: grpc
`
	kafkaConfig = `
configs:
  bootstrap.servers: test
topics:
  sourceEvents: spec1
  agentEvents: status1
`
)

func TestLoadConfig(t *testing.T) {
	mqttConfigFile, err := os.CreateTemp("", "mqtt-config-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mqttConfigFile.Name())

	grpcConfigFile, err := os.CreateTemp("", "grpc-config-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(grpcConfigFile.Name())

	kafkaConfigFile, err := os.CreateTemp("", "kafka-config-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(kafkaConfigFile.Name())

	if err := os.WriteFile(mqttConfigFile.Name(), []byte(mqttConfig), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(grpcConfigFile.Name(), []byte(grpcConfig), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(kafkaConfigFile.Name(), []byte(kafkaConfig), 0644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name           string
		configType     string
		configFilePath string
		expectedConfig any
	}{
		{
			name:           "mqtt config",
			configType:     "mqtt",
			configFilePath: mqttConfigFile.Name(),
			expectedConfig: &mqtt.MQTTOptions{
				BrokerHost: "mqtt",
				Topics: types.Topics{
					SourceEvents: "sources/hub1/clusters/+/sourceevents",
					AgentEvents:  "sources/hub1/clusters/+/agentevents",
				},
				KeepAlive:   60,
				PubQoS:      1,
				SubQoS:      1,
				DialTimeout: 60 * time.Second,
			},
		},
		{
			name:           "grpc config",
			configType:     "grpc",
			configFilePath: grpcConfigFile.Name(),
			expectedConfig: &grpc.GRPCOptions{URL: "grpc"},
		},
		{
			name:           "kafka config",
			configType:     "kafka",
			configFilePath: kafkaConfigFile.Name(),
			expectedConfig: &kafkaoption.KafkaOptions{
				ConfigMap: &kafka.ConfigMap{
					"bootstrap.servers": "test",
				},
				Topics: &types.Topics{
					SourceEvents: "spec1",
					AgentEvents:  "status1",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			loader := NewConfigLoader(c.configType, c.configFilePath)
			_, config, err := loader.LoadConfig()
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			if !equality.Semantic.DeepEqual(config, c.expectedConfig) {
				t.Errorf("unexpected config %v, %v", config, c.expectedConfig)
			}
		})
	}
}
