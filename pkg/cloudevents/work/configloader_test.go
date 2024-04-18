package work

import (
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"k8s.io/apimachinery/pkg/api/equality"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
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
bootstrapServer: broker1
groupID: id
clientCertFile: cert
clientKeyFile: key
caFile: ca
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

	if err := os.WriteFile(mqttConfigFile.Name(), []byte(mqttConfig), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(grpcConfigFile.Name(), []byte(grpcConfig), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(kafkaConfigFile.Name(), []byte(kafkaConfig), 0o644); err != nil {
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
			expectedConfig: &kafka.ConfigMap{
				"acks":                                  "1",
				"auto.offset.reset":                     "earliest",
				"bootstrap.servers":                     "broker1",
				"enable.auto.commit":                    true,
				"enable.auto.offset.store":              false,
				"go.events.channel.size":                1000,
				"group.id":                              "id",
				"log.connection.close":                  false,
				"queued.max.messages.kbytes":            32768,
				"retries":                               "0",
				"security.protocol":                     "ssl",
				"socket.keepalive.enable":               true,
				"ssl.ca.location":                       "ca",
				"ssl.certificate.location":              "cert",
				"ssl.endpoint.identification.algorithm": "none",
				"ssl.key.location":                      "key",
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
