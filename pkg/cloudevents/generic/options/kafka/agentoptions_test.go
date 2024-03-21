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

var mockEventDataType = types.CloudEventsDataType{
	Group:    "resources.test",
	Version:  "v1",
	Resource: "mockresources",
}

func TestAgentContext(t *testing.T) {
	clusterName := "cluster1"
	file, err := os.CreateTemp("", "kafka-agent-config-test-")
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
			name: "resync specs",
			event: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: mockEventDataType,
					SubResource:         types.SubResourceSpec,
					Action:              types.ResyncRequestAction,
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				evt.SetExtension("clustername", clusterName)
				return evt
			}(),
			expectedTopic: defaultStatusTopic,
			expectedKey:   clusterName,
		},
		{
			name: "send status no original source",
			event: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: mockEventDataType,
					SubResource:         types.SubResourceStatus,
					Action:              "test",
				}

				evt := cloudevents.NewEvent()
				evt.SetSource("hub1")
				evt.SetType(eventType.String())
				return evt
			}(),
			expectErrMsg: "\"originalsource\" not found",
		},
		{
			name: "send status",
			event: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: mockEventDataType,
					SubResource:         types.SubResourceStatus,
					Action:              "test",
				}

				evt := cloudevents.NewEvent()
				evt.SetSource("agent")
				evt.SetType(eventType.String())
				evt.SetExtension("originalsource", "hub1")
				return evt
			}(),
			expectedTopic: defaultStatusTopic,
			expectedKey:   fmt.Sprintf("%s@%s", "hub1", clusterName),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			agentOptions := &kafkaAgentOptions{
				KafkaOptions: *options,
				clusterName:  clusterName,
			}

			ctx, err := agentOptions.WithContext(context.TODO(), c.event.Context)
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
