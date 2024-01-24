package kafka

import (
	"fmt"
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
	ConfigMap *kafka.ConfigMap `json:"configs,omitempty" yaml:"configs,omitempty"`
	Topics    *types.Topics    `json:"topics,omitempty" yaml:"topics,omitempty"`
}

func NewKafkaOptions() *KafkaOptions {
	return &KafkaOptions{
		Topics: &types.Topics{
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

	var opts KafkaOptions
	if err := yaml.Unmarshal(configData, &opts); err != nil {
		return nil, err
	}

	if opts.ConfigMap == nil {
		return nil, fmt.Errorf("the configs should be set")
	}

	val, err := opts.ConfigMap.Get("bootstrap.servers", "")
	if err != nil {
		return nil, err
	}
	if val == "" {
		return nil, fmt.Errorf("bootstrap.servers is required")
	}

	if opts.Topics != nil && (opts.Topics.Spec == "" || opts.Topics.Status == "" ||
		opts.Topics.SpecResync == "" || opts.Topics.StatusResync == "") {
		return nil, fmt.Errorf("topics must be set")
	}

	options := &KafkaOptions{
		ConfigMap: opts.ConfigMap,
		Topics: &types.Topics{
			Spec:         defaultSpecTopic,
			Status:       defaultStatusTopic,
			SpecResync:   defaultSpecResyncTopic,
			StatusResync: defaultStatusResyncTopic,
		},
	}
	if opts.Topics != nil {
		options.Topics = opts.Topics
	}
	return options, nil
}
