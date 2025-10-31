package mqtt

import (
	"fmt"
	"regexp"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/eclipse/paho.golang/paho"

	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func agentPubTopic(o *MQTTOptions, clusterName string, evt cloudevents.Event) (string, error) {
	eventType, err := types.ParseCloudEventsType(evt.Type())
	if err != nil {
		return "", fmt.Errorf("unsupported event type %s, %v", eventType, err)
	}

	originalSource := evt.Extensions()[types.ExtensionOriginalSource]

	// agent request to sync resource spec from all sources
	if eventType.Action == types.ResyncRequestAction && originalSource == types.SourceAll {
		if len(o.Topics.AgentBroadcast) == 0 {
			klog.Warningf("the agent broadcast topic not set, fall back to the agent events topic")

			// TODO after supporting multiple sources, we should list each source
			return replaceLast(o.Topics.AgentEvents, "+", clusterName), nil
		}

		return strings.Replace(o.Topics.AgentBroadcast, "+", clusterName, 1), nil
	}

	topicSource, err := getSourceFromEventsTopic(o.Topics.AgentEvents)
	if err != nil {
		return "", err
	}

	// agent publishes status events or spec resync events
	eventsTopic := replaceLast(o.Topics.AgentEvents, "+", clusterName)
	return replaceLast(eventsTopic, "+", topicSource), nil
}

func agentSubTopics(o *MQTTOptions, clusterName string) ([]paho.SubscribeOptions, error) {
	subscriptions := []paho.SubscribeOptions{
		{
			// TODO support multiple sources, currently the client require the source events topic has a sourceID, in
			// the future, client may need a source list, it will subscribe to each source
			// receiving the sources events
			Topic: replaceLast(o.Topics.SourceEvents, "+", clusterName), QoS: byte(o.SubQoS),
		},
	}

	// receiving status resync events from all sources
	if len(o.Topics.SourceBroadcast) != 0 {
		subscriptions = append(subscriptions, paho.SubscribeOptions{
			Topic: o.Topics.SourceBroadcast,
			QoS:   byte(o.SubQoS),
		})
	}

	return subscriptions, nil
}

func sourcePubTopic(o *MQTTOptions, sourceID string, evt cloudevents.Event) (string, error) {
	eventType, err := types.ParseCloudEventsType(evt.Type())
	if err != nil {
		return "", fmt.Errorf("unsupported event type %s, %v", eventType, err)
	}

	clusterName, ok := evt.Extensions()[types.ExtensionClusterName]
	if !ok {
		return "", fmt.Errorf("extension clustername is missing")
	}

	if eventType.Action == types.ResyncRequestAction && clusterName == types.ClusterAll {
		// source request to get resources status from all agents
		if len(o.Topics.SourceBroadcast) == 0 {
			return "", fmt.Errorf("the source broadcast topic not set")
		}

		return strings.Replace(o.Topics.SourceBroadcast, "+", sourceID, 1), nil
	}

	// source publishes spec events or status resync events
	return strings.Replace(o.Topics.SourceEvents, "+", fmt.Sprintf("%s", clusterName), 1), nil
}

func sourceSubTopics(o *MQTTOptions, sourceID string) ([]paho.SubscribeOptions, error) {
	topicSource, err := getSourceFromEventsTopic(o.Topics.AgentEvents)
	if err != nil {
		return nil, err
	}

	if topicSource != sourceID {
		return nil, fmt.Errorf("the topic source %q does not match with the client sourceID %q",
			o.Topics.AgentEvents, sourceID)
	}

	subscriptions := []paho.SubscribeOptions{
		{
			Topic: o.Topics.AgentEvents, QoS: byte(o.SubQoS),
		},
	}

	if len(o.Topics.AgentBroadcast) != 0 {
		// receiving spec resync events from all agents
		subscriptions = append(subscriptions, paho.SubscribeOptions{
			Topic: o.Topics.AgentBroadcast,
			QoS:   byte(o.SubQoS),
		})
	}

	return subscriptions, nil
}

func validateTopics(topics *types.Topics) error {
	if topics == nil {
		return fmt.Errorf("the topics must be set")
	}

	var errs []error
	if !regexp.MustCompile(types.SourceEventsTopicPattern).MatchString(topics.SourceEvents) {
		errs = append(errs, fmt.Errorf("invalid source events topic %q, it should match `%s`",
			topics.SourceEvents, types.SourceEventsTopicPattern))
	}

	if !regexp.MustCompile(types.AgentEventsTopicPattern).MatchString(topics.AgentEvents) {
		errs = append(errs, fmt.Errorf("invalid agent events topic %q, it should match `%s`",
			topics.AgentEvents, types.AgentEventsTopicPattern))
	}

	if len(topics.SourceBroadcast) != 0 {
		if !regexp.MustCompile(types.SourceBroadcastTopicPattern).MatchString(topics.SourceBroadcast) {
			errs = append(errs, fmt.Errorf("invalid source broadcast topic %q, it should match `%s`",
				topics.SourceBroadcast, types.SourceBroadcastTopicPattern))
		}
	}

	if len(topics.AgentBroadcast) != 0 {
		if !regexp.MustCompile(types.AgentBroadcastTopicPattern).MatchString(topics.AgentBroadcast) {
			errs = append(errs, fmt.Errorf("invalid agent broadcast topic %q, it should match `%s`",
				topics.AgentBroadcast, types.AgentBroadcastTopicPattern))
		}
	}

	return errors.NewAggregate(errs)
}

func getSourceFromEventsTopic(topic string) (string, error) {
	if !regexp.MustCompile(types.EventsTopicPattern).MatchString(topic) {
		return "", fmt.Errorf("failed to get source from topic: %q", topic)
	}

	subTopics := strings.Split(topic, "/")
	// get source form share topic, e.g. $share/group/sources/+/consumers/+/agentevents
	if strings.HasPrefix(topic, "$share") {
		return subTopics[3], nil
	}

	// get source form topic, e.g. sources/+/consumers/+/agentevents
	return subTopics[1], nil
}

func replaceLast(str, old, new string) string {
	last := strings.LastIndex(str, old)
	if last == -1 {
		return str
	}
	return str[:last] + new + str[last+len(old):]
}
