package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventscontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/stretchr/testify/assert"

	kafka_confluent "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka/protocol"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestSourceContext(t *testing.T) {
	sourceID := "hub1"
	file, err := os.CreateTemp("", "kafka-source-config-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	if err := os.WriteFile(file.Name(), []byte(`{"configs":{"bootstrap.servers":"test"}}`), 0644); err != nil {
		t.Fatal(err)
	}

	options, err := BuildKafkaOptionsFromFlags(file.Name())
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
				return evt
			}(),
			expectedTopic: defaultSpecTopic,
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
			expectedTopic: defaultSpecTopic,
			expectedKey:   fmt.Sprintf("%s@%s", sourceID, "cluster1"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sourceOptions := &kafkaSourceOptions{
				KafkaOptions: *options,
				sourceID:     sourceID,
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
				assert.Equal(t, c.expectedKey, kafka_confluent.MessageKeyFrom(ctx))
			}
		})
	}
}
