package generic

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"k8s.io/apimachinery/pkg/api/equality"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka"
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
	sourceId    = "source"
	kafkaConfig = `
bootstrapServer: broker1
groupID: source
clientCertFile: cert
clientKeyFile: key
caFile: ca
`
)

func TestBuildCloudEventsSourceOptions(t *testing.T) {
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
		name                     string
		configType               string
		configFilePath           string
		expectedContainedOptions any
	}{
		{
			name:           "mqtt config",
			configType:     "mqtt",
			configFilePath: mqttConfigFile.Name(),
			expectedContainedOptions: &mqtt.MQTTOptions{
				Topics: types.Topics{
					SourceEvents: "sources/hub1/clusters/+/sourceevents",
					AgentEvents:  "sources/hub1/clusters/+/agentevents",
				},
				KeepAlive: 60,
				PubQoS:    1,
				SubQoS:    1,
				Dialer: &mqtt.MQTTDialer{
					BrokerHost: "mqtt",
					Timeout:    60 * time.Second,
				},
			},
		},
		{
			name:                     "grpc config",
			configType:               "grpc",
			configFilePath:           grpcConfigFile.Name(),
			expectedContainedOptions: &grpc.GRPCOptions{URL: "grpc"},
		},
		{
			name:           "kafka config",
			configType:     "kafka",
			configFilePath: kafkaConfigFile.Name(),
			expectedContainedOptions: &kafka.KafkaOptions{
				ConfigMap: confluentkafka.ConfigMap{
					"acks":                                  "1",
					"auto.commit.interval.ms":               5000,
					"auto.offset.reset":                     "latest",
					"bootstrap.servers":                     "broker1",
					"enable.auto.commit":                    true,
					"enable.auto.offset.store":              true,
					"go.events.channel.size":                1000,
					"group.id":                              sourceId,
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
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, config, err := NewConfigLoader(c.configType, c.configFilePath).
				LoadConfig()
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			if !equality.Semantic.DeepEqual(config, c.expectedContainedOptions) {
				t.Errorf("unexpected config %v, %v", config, c.expectedContainedOptions)
			}

			options, err := BuildCloudEventsSourceOptions(config, "client", sourceId)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			optionsRaw, _ := json.Marshal(options)
			expectedRaw, _ := json.Marshal(c.expectedContainedOptions)

			if !strings.Contains(string(optionsRaw), string(expectedRaw)) {
				t.Errorf("the results %v\n does not contain the original options %v\n", string(optionsRaw), string(expectedRaw))
			}
		})
	}
}
