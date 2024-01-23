package kafka

import (
	kafka_confluent "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka/protocol"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	// defaultSpecTopic is a default kafka topic for resource spec.
	// defaultSpecTopic = "sources/+/clusters/+/spec"
	// defaultSpecTopic = "sources/+/clusters/+/spec"

	// defaultStatusTopic is a default kafka topic for resource status.
	// defaultStatusTopic = "sources/+/clusters/+/status"

	// // defaultSpecResyncTopic is a default kafka topic for resource spec resync.
	// defaultSpecResyncTopic = "sources/clusters/+/specresync"

	// // defaultStatusResyncTopic is a default kafka topic for resource status resync.
	// defaultStatusResyncTopic = "sources/+/clusters/statusresync"

	defaultSpecTopic         = "spec"
	defaultStatusTopic       = "status"
	defaultSpecResyncTopic   = "specresync"
	defaultStatusResyncTopic = "statusreync"
)

type KafkaOptions struct {
	configMap *kafka.ConfigMap
	Topics    types.Topics
	// Producer  *kafka.Producer
	// Consumer  *kafka.Consumer
}

func NewKafkaOptions() *KafkaOptions {
	return &KafkaOptions{
		Topics: types.Topics{
			Spec:         defaultSpecTopic,
			Status:       defaultStatusTopic,
			SpecResync:   defaultSpecResyncTopic,
			StatusResync: defaultStatusResyncTopic,
		},
	}
}

func (o *KafkaOptions) GetCloudEventsClient(clientOpts ...kafka_confluent.Option) (cloudevents.Client, error) {
	protocol, err := kafka_confluent.New(clientOpts...)
	if err != nil {
		return nil, err
	}
	return cloudevents.NewClient(protocol)
}
