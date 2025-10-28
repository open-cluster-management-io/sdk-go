package mqtt

import (
	"context"
	"fmt"
	"strings"

	cloudeventsmqtt "github.com/cloudevents/sdk-go/protocol/mqtt_paho/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventscontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/eclipse/paho.golang/paho"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/cloudevent"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type mqttAgentOptions struct {
	MQTTOptions
	errorChan   chan error
	clusterName string
	agentID     string
}

func NewAgentOptions(mqttOptions *MQTTOptions, clusterName, agentID string) *options.CloudEventsAgentOptions {
	mqttAgentOptions := &mqttAgentOptions{
		MQTTOptions: *mqttOptions,
		errorChan:   make(chan error),
		clusterName: clusterName,
		agentID:     agentID,
	}

	return &options.CloudEventsAgentOptions{
		CloudEventsOptions: mqttAgentOptions,
		AgentID:            mqttAgentOptions.agentID,
		ClusterName:        mqttAgentOptions.clusterName,
	}
}

func (o *mqttAgentOptions) WithContext(ctx context.Context, evtCtx cloudevents.EventContext) (context.Context, error) {
	topic, err := getAgentPubTopic(ctx)
	if err != nil {
		return nil, err
	}

	if topic != nil {
		return cloudeventscontext.WithTopic(ctx, string(*topic)), nil
	}

	eventType, err := types.ParseCloudEventsType(evtCtx.GetType())
	if err != nil {
		return nil, fmt.Errorf("unsupported event type %s, %v", eventType, err)
	}

	originalSource, err := evtCtx.GetExtension(types.ExtensionOriginalSource)
	if err != nil {
		return nil, err
	}

	// agent request to sync resource spec from all sources
	if eventType.Action == types.ResyncRequestAction && originalSource == types.SourceAll {
		if len(o.Topics.AgentBroadcast) == 0 {
			klog.Warningf("the agent broadcast topic not set, fall back to the agent events topic")

			// TODO after supporting multiple sources, we should list each source
			eventsTopic := replaceLast(o.Topics.AgentEvents, "+", o.clusterName)
			return cloudeventscontext.WithTopic(ctx, eventsTopic), nil
		}

		resyncTopic := strings.Replace(o.Topics.AgentBroadcast, "+", o.clusterName, 1)
		return cloudeventscontext.WithTopic(ctx, resyncTopic), nil
	}

	topicSource, err := getSourceFromEventsTopic(o.Topics.AgentEvents)
	if err != nil {
		return nil, err
	}

	// agent publishes status events or spec resync events
	eventsTopic := replaceLast(o.Topics.AgentEvents, "+", o.clusterName)
	eventsTopic = replaceLast(eventsTopic, "+", topicSource)
	return cloudeventscontext.WithTopic(ctx, eventsTopic), nil
}

func (o *mqttAgentOptions) Protocol(ctx context.Context, dataType types.CloudEventsDataType) (options.CloudEventsProtocol, error) {
	subscribe := &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				// TODO support multiple sources, currently the client require the source events topic has a sourceID, in
				// the future, client may need a source list, it will subscribe to each source
				// receiving the sources events
				Topic: replaceLast(o.Topics.SourceEvents, "+", o.clusterName), QoS: byte(o.SubQoS),
			},
		},
	}

	// receiving status resync events from all sources
	if len(o.Topics.SourceBroadcast) != 0 {
		subscribe.Subscriptions = append(subscribe.Subscriptions, paho.SubscribeOptions{
			Topic: o.Topics.SourceBroadcast,
			QoS:   byte(o.SubQoS),
		})
	}

	return o.GetCloudEventsProtocol(
		ctx,
		fmt.Sprintf("%s-client", o.agentID),
		func(err error) {
			o.errorChan <- err
		},
		cloudeventsmqtt.WithPublish(&paho.Publish{QoS: byte(o.PubQoS)}),
		cloudeventsmqtt.WithSubscribe(subscribe),
	)
}

func (o *mqttAgentOptions) ErrorChan() <-chan error {
	return o.errorChan
}

type MQTTTransport interface {
	options.EventTransport

	GetPublishTopic(evt cloudevent.Event) (string, error)
	GetSubscriptions() []paho.SubscribeOptions
}

type mqttTransport struct {
	MQTTOptions
	client      *paho.Client
	clusterName string
	agentID     string
	errorChan   chan error
	msgChan     chan *paho.Publish
	closeChan   chan struct{}
}

func (t *mqttTransport) Connect(ctx context.Context) error {
	netConn, err := t.Dialer.Dial()
	if err != nil {
		return err
	}

	config := paho.ClientConfig{
		ClientID: t.agentID,
		Conn:     netConn,
		OnClientError: func(err error) {
			t.errorChan <- err
		},
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				t.msgChan <- pr.Packet
				return true, nil
			},
		},
	}

	t.client = paho.NewClient(config)

	// Connect to the MQTT broker
	connAck, err := t.client.Connect(ctx, t.GetMQTTConnectOption(t.agentID))
	if err != nil {
		return err
	}
	if connAck.ReasonCode != 0 {
		return fmt.Errorf("failed to establish the connection: %s", connAck.String())
	}

	return nil
}

func (t *mqttTransport) Send(ctx context.Context, evt cloudevent.Event) error {
	topic, err := t.GetPublishTopic(evt)
	if err != nil {
		return err
	}
	if _, err := t.client.Publish(ctx, &paho.Publish{
		QoS:   byte(t.PubQoS),
		Topic: topic,
		Properties: &paho.PublishProperties{
			ContentType:     "json",
			CorrelationData: []byte{}, // unmarshal event
		},
	}); err != nil {
		return err
	}
	return nil
}

func (t *mqttTransport) Receive(ctx context.Context, handler options.ReceiveHandler) error {
	_, err := t.client.Subscribe(ctx, &paho.Subscribe{Subscriptions: t.GetSubscriptions()})
	if err != nil {
		return nil
	}

	select {
	case <-ctx.Done():
	case <-t.msgChan:
		// msg received
		//handler()
	}

	return nil
}

func (t *mqttTransport) ErrorChan() <-chan error {
	return t.errorChan
}

func (t *mqttTransport) Close() error {
	return t.client.Disconnect(&paho.Disconnect{ReasonCode: 0})
}

func (t *mqttTransport) GetPublishTopic(evt cloudevent.Event) (string, error) {
	eventType, err := types.ParseCloudEventsType(evt.Type)
	if err != nil {
		return "", fmt.Errorf("unsupported event type %s, %v", eventType, err)
	}

	originalSource := evt.OriginalSource
	if eventType.Action == types.ResyncRequestAction && originalSource == types.SourceAll {
		if len(t.Topics.AgentBroadcast) == 0 {
			klog.Warningf("the agent broadcast topic not set, fall back to the agent events topic")

			// TODO after supporting multiple sources, we should list each source
			return replaceLast(t.Topics.AgentEvents, "+", t.clusterName), nil
		}

		return strings.Replace(t.Topics.AgentBroadcast, "+", t.clusterName, 1), nil
	}

	topicSource, err := getSourceFromEventsTopic(t.Topics.AgentEvents)
	if err != nil {
		return "", err
	}

	// agent publishes status events or spec resync events
	eventsTopic := replaceLast(t.Topics.AgentEvents, "+", t.clusterName)
	return replaceLast(eventsTopic, "+", topicSource), nil
}

func (t *mqttTransport) GetSubscriptions() []paho.SubscribeOptions {
	subscriptions := []paho.SubscribeOptions{
		{
			// TODO support multiple sources, currently the client require the source events topic has a sourceID, in
			// the future, client may need a source list, it will subscribe to each source
			// receiving the sources events
			Topic: replaceLast(t.Topics.SourceEvents, "+", t.clusterName), QoS: byte(t.SubQoS),
		},
	}

	if len(t.Topics.SourceBroadcast) != 0 {
		subscriptions = append(subscriptions, paho.SubscribeOptions{
			Topic: t.Topics.SourceBroadcast,
			QoS:   byte(t.SubQoS),
		})
	}

	return subscriptions
}
