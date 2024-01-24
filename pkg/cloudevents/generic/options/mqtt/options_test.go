package mqtt

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestBuildMQTTOptionsFromFlags(t *testing.T) {
	file, err := os.CreateTemp("", "mqtt-config-test-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

	cases := []struct {
		name             string
		config           string
		expectedOptions  *MQTTOptions
		expectedErrorMsg string
	}{
		{
			name:             "empty config",
			config:           "",
			expectedErrorMsg: "brokerHost is required",
		},
		{
			name:             "tls config without clientCertFile",
			config:           "{\"brokerHost\":\"test\",\"clientCertFile\":\"test\"}",
			expectedErrorMsg: "either both or none of clientCertFile and clientKeyFile must be set",
		},
		{
			name:             "tls config without caFile",
			config:           "{\"brokerHost\":\"test\",\"clientCertFile\":\"test\",\"clientKeyFile\":\"test\"}",
			expectedErrorMsg: "setting clientCertFile and clientKeyFile requires caFile",
		},
		{
			name:             "without topics",
			config:           "{\"brokerHost\":\"test\",\"topics\":{}}",
			expectedErrorMsg: "topics must be set",
		},
		{
			name:   "default options",
			config: "{\"brokerHost\":\"test\"}",
			expectedOptions: &MQTTOptions{
				BrokerHost: "test",
				KeepAlive:  60,
				PubQoS:     1,
				SubQoS:     1,
				Timeout:    30 * time.Second,
				Topics: types.Topics{
					Spec:         "sources/+/clusters/+/spec",
					Status:       "sources/+/clusters/+/status",
					SpecResync:   "sources/clusters/+/specresync",
					StatusResync: "sources/+/clusters/statusresync",
				},
			},
		},
		{
			name:   "default options with yaml format",
			config: "brokerHost: test",
			expectedOptions: &MQTTOptions{
				BrokerHost: "test",
				KeepAlive:  60,
				PubQoS:     1,
				SubQoS:     1,
				Timeout:    30 * time.Second,
				Topics: types.Topics{
					Spec:         "sources/+/clusters/+/spec",
					Status:       "sources/+/clusters/+/status",
					SpecResync:   "sources/clusters/+/specresync",
					StatusResync: "sources/+/clusters/statusresync",
				},
			},
		},
		{
			name:   "customized options",
			config: "{\"brokerHost\":\"test\",\"keepAlive\":30,\"pubQoS\":0,\"subQoS\":2,\"timeout\":30s,}",
			expectedOptions: &MQTTOptions{
				BrokerHost: "test",
				KeepAlive:  30,
				PubQoS:     0,
				SubQoS:     2,
				Timeout:    30 * time.Second,
				Topics: types.Topics{
					Spec:         "sources/+/clusters/+/spec",
					Status:       "sources/+/clusters/+/status",
					SpecResync:   "sources/clusters/+/specresync",
					StatusResync: "sources/+/clusters/statusresync",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := os.WriteFile(file.Name(), []byte(c.config), 0644); err != nil {
				t.Fatal(err)
			}

			options, err := BuildMQTTOptionsFromFlags(file.Name())
			if err != nil {
				if err.Error() != c.expectedErrorMsg {
					t.Errorf("unexpected err %v", err)
				}
			}

			if !reflect.DeepEqual(options, c.expectedOptions) {
				t.Errorf("unexpected options %v", options)
			}
		})
	}
}

func TestConnectionTimeout(t *testing.T) {
	file, err := os.CreateTemp("", "mqtt-config-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	ln := newLocalListener(t)
	defer ln.Close()

	if err := os.WriteFile(file.Name(), []byte("{\"brokerHost\":\""+ln.Addr().String()+"\"}"), 0644); err != nil {
		t.Fatal(err)
	}

	options, err := BuildMQTTOptionsFromFlags(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	options.Timeout = 10 * time.Millisecond

	agentOptions := &mqttAgentOptions{
		MQTTOptions: *options,
		clusterName: "cluster1",
	}
	_, err = agentOptions.Client(context.TODO())
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
}

func newLocalListener(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln, err = net.Listen("tcp6", "[::1]:0")
	}
	if err != nil {
		t.Fatal(err)
	}
	return ln
}
