//go:build kafka

package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventscontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/stretchr/testify/assert"

	confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestSourceContext(t *testing.T) {
	sourceID := "hub1"
	file, err := os.CreateTemp("", "kafka-source-config-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	if err := os.WriteFile(file.Name(), []byte(`{"bootstrapServer":"testBroker","groupID":"id"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	configMap, err := BuildKafkaOptionsFromFlags(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name          string
		event         cloudevents.Event
		expectedTopic string
		expectedKey   string
		expectErrMsg  string
	}{
		{
			name: "unsupported event",
			event: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("wrongType")
				return evt
			}(),
			expectErrMsg: fmt.Sprintf("unsupported cloudevents type format: %s", "wrongType"),
		},
		{
			name: "resync status",
			event: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: mockEventDataType,
					SubResource:         types.SubResourceStatus,
					Action:              types.ResyncRequestAction,
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				evt.SetExtension("clustername", "")
				return evt
			}(),
			expectedTopic: "sourcebroadcast.hub1",
			expectedKey:   sourceID,
		},
		{
			name: "unsupported send resource no cluster name",
			event: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: mockEventDataType,
					SubResource:         types.SubResourceSpec,
					Action:              "test",
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				return evt
			}(),
			expectErrMsg: "\"clustername\" not found",
		},
		{
			name: "send spec",
			event: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: mockEventDataType,
					SubResource:         types.SubResourceSpec,
					Action:              "test",
				}

				evt := cloudevents.NewEvent()
				evt.SetSource("agent")
				evt.SetType(eventType.String())
				evt.SetExtension("clustername", "cluster1")
				return evt
			}(),
			expectedTopic: "sourceevents.hub1.cluster1",
			expectedKey:   fmt.Sprintf("%s@%s", sourceID, "cluster1"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sourceOptions := &kafkaSourceOptions{
				configMap: configMap,
				sourceID:  sourceID,
			}

			ctx, err := sourceOptions.WithContext(context.TODO(), c.event.Context)
			if c.expectErrMsg != "" {
				assert.NotNil(t, err)
				assert.Equal(t, c.expectErrMsg, err.Error())
			} else {
				assert.Nil(t, err)
			}

			if c.expectedTopic != "" {
				assert.Equal(t, c.expectedTopic, cloudeventscontext.TopicFrom(ctx))
			}

			if c.expectedKey != "" {
				assert.Equal(t, c.expectedKey, confluent.MessageKeyFrom(ctx))
			}
		})
	}
}
