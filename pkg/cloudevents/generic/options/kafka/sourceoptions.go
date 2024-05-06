//go:build kafka

package kafka

import (
	"context"
	"fmt"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventscontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type kafkaSourceOptions struct {
	configMap *kafka.ConfigMap
	sourceID  string
	errorChan chan error
}

func NewSourceOptions(configMap *map[string]interface{}, sourceID string) *options.CloudEventsSourceOptions {
	kafkaConfigMap := convertToKafkaConfigMap(*configMap)
	sourceOptions := &kafkaSourceOptions{
		configMap: &kafkaConfigMap,
		sourceID:  sourceID,
		errorChan: make(chan error),
	}

	groupID, err := kafkaConfigMap.Get("group.id", "")
	if groupID == "" || err != nil {
		_ = kafkaConfigMap.SetKey("group.id", sourceID)
	}

	return &options.CloudEventsSourceOptions{
		CloudEventsOptions: sourceOptions,
		SourceID:           sourceID,
	}
}

func (o *kafkaSourceOptions) WithContext(ctx context.Context,
	evtCtx cloudevents.EventContext,
) (context.Context, error) {
	clusterName, err := evtCtx.GetExtension(types.ExtensionClusterName)
	if err != nil {
		return nil, err
	}

	topic := strings.Replace(sourceEventsTopic, "*.*", fmt.Sprintf("%s.%s", o.sourceID, clusterName), 1)
	key := fmt.Sprintf("%s@%s", o.sourceID, clusterName)
	if clusterName == types.ClusterAll {
		// source request to get resources status from all agents
		topic = strings.Replace(sourceBroadcastTopic, "*", o.sourceID, 1)
		key = o.sourceID
	}
	topicCtx := cloudeventscontext.WithTopic(ctx, topic)
	return confluent.WithMessageKey(topicCtx, key), nil
}

func (o *kafkaSourceOptions) Protocol(ctx context.Context) (options.CloudEventsProtocol, error) {
	protocol, err := confluent.New(confluent.WithConfigMap(o.configMap),
		confluent.WithReceiverTopics([]string{
			fmt.Sprintf("^%s", strings.Replace(agentEventsTopic, "*", o.sourceID, 1)),
			fmt.Sprintf("^%s", agentBroadcastTopic),
		}),
		confluent.WithSenderTopic("sourceevents"),
		confluent.WithErrorHandler(func(ctx context.Context, err kafka.Error) {
			o.errorChan <- err
		}))
	if err != nil {
		return nil, err
	}
	producerEvents, _ := protocol.Events()
	handleProduceEvents(producerEvents, o.errorChan)
	return protocol, nil
}

func (o *kafkaSourceOptions) ErrorChan() <-chan error {
	return o.errorChan
}
