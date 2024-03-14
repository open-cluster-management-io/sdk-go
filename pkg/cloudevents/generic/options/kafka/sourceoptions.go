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

type kafkaSourceOptions struct {
	KafkaOptions
	sourceID  string
	errorChan chan error
}

func NewSourceOptions(opts *KafkaOptions, sourceID string) *options.CloudEventsSourceOptions {
	sourceOptions := &kafkaSourceOptions{
		KafkaOptions: *opts,
		sourceID:     sourceID,
		errorChan:    make(chan error),
	}

	return &options.CloudEventsSourceOptions{
		CloudEventsOptions: sourceOptions,
		SourceID:           sourceID,
	}
}

func (o *kafkaSourceOptions) WithContext(ctx context.Context,
	evtCtx cloudevents.EventContext) (context.Context, error) {

	eventType, err := types.ParseCloudEventsType(evtCtx.GetType())
	if err != nil {
		return nil, err
	}

	topicCtx := cloudeventscontext.WithTopic(ctx, o.Topics.SourceEvents)
	if eventType.Action == types.ResyncRequestAction {
		return kafka_confluent.WithMessageKey(topicCtx, o.sourceID), nil
	}

	clusterName, err := evtCtx.GetExtension(types.ExtensionClusterName)
	if err != nil {
		return nil, err
	}

	// source publishes event to spec topic to send the resource spec to a specified cluster
	messageKey := fmt.Sprintf("%s@%s", o.sourceID, clusterName)
	return kafka_confluent.WithMessageKey(topicCtx, messageKey), nil
}

func (o *kafkaSourceOptions) Client(ctx context.Context) (cloudevents.Client, error) {
	c, err := o.GetCloudEventsClient(
		kafka_confluent.WithConfigMap(o.ConfigMap),
		kafka_confluent.WithReceiverTopics([]string{o.Topics.AgentEvents}),
		kafka_confluent.WithSenderTopic(o.Topics.SourceEvents),
		kafka_confluent.WithAutoRecover(false),
		kafka_confluent.WithErrorChan(o.errorChan),
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (o *kafkaSourceOptions) ErrorChan() <-chan error {
	return o.errorChan
}
