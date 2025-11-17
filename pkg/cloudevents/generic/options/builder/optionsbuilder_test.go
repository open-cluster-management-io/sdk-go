package builder

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	clienttesting "open-cluster-management.io/sdk-go/pkg/testing"
)

const sourceId = "source"

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
)

type buildingCloudEventsOptionTestCase struct {
	name                  string
	configType            string
	configFile            *os.File
	expectedOptions       any
	expectedTransportType string
}

func TestBuildCloudEventsSourceOptions(t *testing.T) {
	cases := []buildingCloudEventsOptionTestCase{
		{
			name:       "mqtt config",
			configType: "mqtt",
			configFile: configFile(t, "mqtt-config-test-", []byte(mqttConfig)),
			expectedOptions: &mqtt.MQTTOptions{
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
			expectedTransportType: "*mqtt.mqttSourceTransport",
		},
		{
			name:       "grpc config",
			configType: "grpc",
			configFile: configFile(t, "grpc-config-test-", []byte(grpcConfig)),
			expectedOptions: &grpc.GRPCOptions{
				Dialer: &grpc.GRPCDialer{
					URL: "grpc",
					KeepAliveOptions: grpc.KeepAliveOptions{
						Enable:              false,
						Time:                30 * time.Second,
						Timeout:             10 * time.Second,
						PermitWithoutStream: false,
					},
				},
			},
			expectedTransportType: "*grpc.grpcTransport",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assertOptions(t, c)
		})
	}
}

func configFile(t *testing.T, prefix string, data []byte) *os.File {
	configFile, err := clienttesting.WriteToTempFile(prefix, data)
	if err != nil {
		t.Fatal(err)
	}

	return configFile
}

func assertOptions(t *testing.T, c buildingCloudEventsOptionTestCase) {
	_, config, err := NewConfigLoader(c.configType, c.configFile.Name()).
		LoadConfig()
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if !cmp.Equal(config, c.expectedOptions, cmpopts.IgnoreUnexported(mqtt.MQTTDialer{}, grpc.GRPCDialer{})) {
		t.Errorf("unexpected config %v, %v", config, c.expectedOptions)
	}

	options, err := BuildCloudEventsSourceOptions(config, "client", sourceId, types.CloudEventsDataType{})
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	tt := reflect.TypeOf(options.CloudEventsTransport)

	if tt.String() != c.expectedTransportType {
		t.Errorf("expected %s, but got %s", c.expectedTransportType, tt)
	}
}
