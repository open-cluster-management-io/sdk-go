package kafka

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
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
		expectedOptions  *KafkaOptions
		expectedErrorMsg string
	}{
		{
			name:             "empty configs",
			config:           "",
			expectedErrorMsg: "the configs should be set",
		},
		{
			name:             "empty bootstrap.server from configs",
			config:           `{"configs":{}}`,
			expectedErrorMsg: "bootstrap.servers is required",
		},
		{
			name:   "default topics",
			config: `{"configs":{"bootstrap.servers":"test"}}`,
			expectedOptions: &KafkaOptions{
				ConfigMap: &kafka.ConfigMap{
					"bootstrap.servers": "test",
				},
				Topics: &types.Topics{
					Spec:         defaultSpecTopic,
					Status:       defaultStatusTopic,
					SpecResync:   defaultSpecResyncTopic,
					StatusResync: defaultStatusResyncTopic,
				},
			},
		},
		{
			name:             "empty topics",
			config:           `{"configs":{"bootstrap.servers":"test"},"topics":{}}`,
			expectedErrorMsg: "topics must be set",
		},
		{
			name: "with topics",
			config: `{"configs":{"bootstrap.servers":"test"},"topics":{"spec":"spec1","specResync":"spec2",
			"status":"status1","statusResync":"status2"}}`,
			expectedOptions: &KafkaOptions{
				ConfigMap: &kafka.ConfigMap{
					"bootstrap.servers": "test",
				},
				Topics: &types.Topics{
					Spec:         "spec1",
					Status:       "status1",
					SpecResync:   "spec2",
					StatusResync: "status2",
				},
			},
		},
		{
			name:   "customized options",
			config: `{"configs":{"bootstrap.servers":"test","enable.auto.commit":"true","group.id":"testid"}}`,
			expectedOptions: &KafkaOptions{
				ConfigMap: &kafka.ConfigMap{
					"bootstrap.servers":  "test",
					"enable.auto.commit": "true",
					"group.id":           "testid",
				},
				Topics: &types.Topics{
					Spec:         defaultSpecTopic,
					Status:       defaultStatusTopic,
					SpecResync:   defaultSpecResyncTopic,
					StatusResync: defaultStatusResyncTopic,
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := os.WriteFile(file.Name(), []byte(c.config), 0644); err != nil {
				t.Fatal(err)
			}

			options, err := BuildKafkaOptionsFromFlags(file.Name())
			if c.expectedErrorMsg != "" {
				assert.Equal(t, c.expectedErrorMsg, err.Error(), "the expected error message isn't matched")
			} else {
				assert.Nil(t, err)
			}
			if c.expectedOptions != nil {
				assert.True(t, reflect.DeepEqual(options, c.expectedOptions), "the option should be matched", "expected",
					c.expectedOptions, "actual", options)
			}
		})
	}
}
