package kafka

import (
	"os"

	"gopkg.in/yaml.v2"
	kafka_confluent "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka/protocol"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	defaultSpecTopic         = "spec"
	defaultStatusTopic       = "status"
	defaultSpecResyncTopic   = "specresync"
	defaultStatusResyncTopic = "statusreync"
)

type KafkaOptions struct {
	// the configMap: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	configMap *kafka.ConfigMap
	Topics    types.Topics
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

// BuildKafkaOptionsFromFlags builds configs from a config filepath.
func BuildKafkaOptionsFromFlags(configPath string) (*KafkaOptions, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &kafka.ConfigMap{}
	if err := yaml.Unmarshal(configData, config); err != nil {
		return nil, err
	}

	options := &KafkaOptions{
		configMap: config,
		Topics: types.Topics{
			Spec:         defaultSpecTopic,
			Status:       defaultStatusTopic,
			SpecResync:   defaultSpecResyncTopic,
			StatusResync: defaultStatusResyncTopic,
		},
	}
	return options, nil
}
