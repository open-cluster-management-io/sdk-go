//go:build kafka

package kafka

import (
	"log"
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func TestBuildKafkaOptionsFromFlags(t *testing.T) {
	file, err := os.CreateTemp("", "kafka-config-test-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

	cases := []struct {
		name             string
		config           string
		expectedOptions  *kafka.ConfigMap
		expectedErrorMsg string
	}{
		{
			name:             "empty configs",
			config:           "",
			expectedErrorMsg: "bootstrapServer is required",
		},
		{
			name:             "tls config without clientCertFile",
			config:           `{"bootstrapServer":"test","groupID":"test","clientCertFile":"test"}`,
			expectedErrorMsg: "either both or none of clientCertFile and clientKeyFile must be set",
		},
		{
			name:             "tls config without caFile",
			config:           `{"bootstrapServer":"test","groupID":"test","clientCertFile":"test","clientKeyFile":"test"}`,
			expectedErrorMsg: "setting clientCertFile and clientKeyFile requires caFile",
		},
		{
			name:   "options without ssl",
			config: `{"bootstrapServer":"testBroker","groupID":"testGroupID"}`,
			expectedOptions: &kafka.ConfigMap{
				"acks":                                  "1",
				"auto.commit.interval.ms":               5000,
				"auto.offset.reset":                     "latest",
				"bootstrap.servers":                     "testBroker",
				"enable.auto.commit":                    true,
				"enable.auto.offset.store":              true,
				"go.events.channel.size":                1000,
				"group.id":                              "testGroupID",
				"log.connection.close":                  false,
				"queued.max.messages.kbytes":            32768,
				"retries":                               "0",
				"socket.keepalive.enable":               true,
				"ssl.endpoint.identification.algorithm": "none",
			},
		},

		{
			name:   "options with ssl",
			config: `{"bootstrapServer":"broker1","groupID":"id","clientCertFile":"cert","clientKeyFile":"key","caFile":"ca"}`,
			expectedOptions: &kafka.ConfigMap{
				"acks":                                  "1",
				"auto.commit.interval.ms":               5000,
				"auto.offset.reset":                     "latest",
				"bootstrap.servers":                     "broker1",
				"enable.auto.commit":                    true,
				"enable.auto.offset.store":              true,
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
			err := os.WriteFile(file.Name(), []byte(c.config), 0o644)
			require.Nil(t, err)

			options, err := BuildKafkaOptionsFromFlags(file.Name())
			if c.expectedErrorMsg != "" {
				require.Equal(t, c.expectedErrorMsg, err.Error())
			} else {
				require.Nil(t, err, "failed to get kafkaOptions")
			}
			if c.expectedOptions != nil {
				require.EqualValues(t, c.expectedOptions, options)
			}
		})
	}
}
