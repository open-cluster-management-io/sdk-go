package kafka

import (
	"context"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventscontext "github.com/cloudevents/sdk-go/v2/context"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	kafka_confluent "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka/protocol"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type kafkaAgentOptions struct {
	KafkaOptions
	clusterName string
	agentID     string
}

func NewAgentOptions(kafkaOptions *KafkaOptions, clusterName, agentID string) *options.CloudEventsAgentOptions {
	kafkaAgentOptions := &kafkaAgentOptions{
		KafkaOptions: *kafkaOptions,
		clusterName:  clusterName,
		agentID:      agentID,
	}

	return &options.CloudEventsAgentOptions{
		CloudEventsOptions: kafkaAgentOptions,
		AgentID:            agentID,
		ClusterName:        clusterName,
	}
}

// encode the source and agent to the message key
func (o *kafkaAgentOptions) WithContext(ctx context.Context, evtCtx cloudevents.EventContext) (context.Context, error) {
	eventType, err := types.ParseCloudEventsType(evtCtx.GetType())
	if err != nil {
		return nil, fmt.Errorf("unsupported event type %s, %v", eventType, err)
	}

	if eventType.Action == types.ResyncRequestAction {
		// agent publishes event to spec resync topic to request to get resources spec from all sources
		topicCtx := cloudeventscontext.WithTopic(ctx, o.Topics.SpecResync)
		return kafka_confluent.WithMessageKey(topicCtx, o.clusterName), nil
	}

	// agent publishes event to status topic to send the resource status from a specified cluster
	originalSource, err := evtCtx.GetExtension(types.ExtensionOriginalSource)
	if err != nil {
		return nil, err
	}

	topicCtx := cloudeventscontext.WithTopic(ctx, o.Topics.Status)
	messageKey := fmt.Sprintf("%s@%s", originalSource, o.clusterName)
	return kafka_confluent.WithMessageKey(topicCtx, messageKey), nil
}

func (o *kafkaAgentOptions) Client(ctx context.Context) (cloudevents.Client, error) {
	c, err := o.GetCloudEventsClient(
		kafka_confluent.WithConfigMap(o.configMap),
		kafka_confluent.WithReceiverTopics([]string{o.Topics.Spec, o.Topics.StatusResync}),
		kafka_confluent.WithSenderTopic(o.Topics.Status),
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (o *kafkaAgentOptions) ErrorChan() <-chan error {
	return nil
}
