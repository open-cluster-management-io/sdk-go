package mqtt

import (
	"context"
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"k8s.io/klog/v2"

	"github.com/eclipse/paho.golang/paho"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
)

const (
	prefix = "ce-" // for cloudevent-sdk compatibility

	// required attributes, see https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md
	idAttrKey          = "id"
	sourceAttrKey      = "source"
	specVersionAttrKey = "specversion"
	typeAttrKey        = "type"

	// optional attributes
	timeAttrKey = "time"
)

type mqttTransport struct {
	opts *MQTTOptions

	client *paho.Client

	clientID string

	errorChan chan error
	// msgChan   chan *paho.Publish

	closeChan chan struct{}
	closed    bool

	publishTopic     func(cloudevents.Event) (string, error)
	getSubscriptions func() ([]paho.SubscribeOptions, error)
}

func NewAgentOptions(opts *MQTTOptions, clusterName, agentID string) *options.CloudEventsAgentOptions {
	return &options.CloudEventsAgentOptions{
		EventTransport: &mqttTransport{
			opts:      opts,
			clientID:  fmt.Sprintf("%s-client", agentID),
			closeChan: make(chan struct{}),
			errorChan: make(chan error),
			// msgChan:   make(chan *paho.Publish, 10),
			publishTopic: func(evt cloudevents.Event) (string, error) {
				return agentPubTopic(opts, clusterName, evt)
			},
			getSubscriptions: func() ([]paho.SubscribeOptions, error) {
				return agentSubTopics(opts, clusterName)
			},
		},
		AgentID:     agentID,
		ClusterName: clusterName,
	}
}

func NewSourceOptions(opts *MQTTOptions, clientID, sourceID string) *options.CloudEventsSourceOptions {
	return &options.CloudEventsSourceOptions{
		EventTransport: &mqttTransport{
			opts:      opts,
			clientID:  clientID,
			closeChan: make(chan struct{}),
			errorChan: make(chan error),
			// msgChan:   make(chan *paho.Publish, 10),

			publishTopic: func(evt cloudevents.Event) (string, error) {
				return sourcePubTopic(opts, sourceID, evt)
			},
			getSubscriptions: func() ([]paho.SubscribeOptions, error) {
				return sourceSubTopics(opts, sourceID)
			},
		},
		SourceID: sourceID,
	}
}

func (t *mqttTransport) Connect(ctx context.Context) error {
	netConn, err := t.opts.Dialer.Dial()
	if err != nil {
		return err
	}

	config := paho.ClientConfig{
		ClientID: t.clientID,
		Conn:     netConn,
		OnClientError: func(err error) {
			select {
			case t.errorChan <- err:
			default:
				klog.Warningf("error channel is full, dropping error: %v", err)
			}
		},
		// OnPublishReceived: []func(paho.PublishReceived) (bool, error){
		// 	func(pr paho.PublishReceived) (bool, error) {
		// 		// receive the msg
		// 		t.msgChan <- pr.Packet
		// 		return true, nil
		// 	},
		// },
	}

	t.client = paho.NewClient(config)

	connect := &paho.Connect{
		ClientID:   t.clientID,
		KeepAlive:  t.opts.KeepAlive,
		CleanStart: true,
	}

	if len(t.opts.Username) != 0 {
		connect.Username = t.opts.Username
		connect.UsernameFlag = true
	}

	if len(t.opts.Password) != 0 {
		connect.Password = []byte(t.opts.Password)
		connect.PasswordFlag = true
	}

	connAck, err := t.client.Connect(ctx, connect)
	if err != nil {
		return err
	}
	if connAck.ReasonCode != 0 {
		return fmt.Errorf("failed to establish the connection: %s", connAck.String())
	}

	return nil
}

func (t *mqttTransport) Send(ctx context.Context, evt cloudevents.Event) error {
	topic, err := t.publishTopic(evt)
	if err != nil {
		return err
	}

	msg, err := toPublishMsg(topic, t.opts.PubQoS, evt)
	if err != nil {
		return err
	}

	if _, err := t.client.Publish(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (t *mqttTransport) Receive(ctx context.Context, handleFn options.ReceiveHandler) error {
	subs, err := t.getSubscriptions()
	if err != nil {
		return err
	}

	t.client.AddOnPublishReceived(func(pr paho.PublishReceived) (bool, error) {
		evt, err := toEvent(pr.Packet)
		if err != nil {
			klog.Errorf("failed to parse event %s", err)
			return true, nil
		}

		handleFn(evt)
		return true, nil
	})

	if _, err := t.client.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs}); err != nil {
		return err
	}

	// go func() {
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		case <-t.closeChan:
	// 			return
	// 		case msg, ok := <-t.msgChan:
	// 			if !ok {
	// 				return
	// 			}

	// 			evt, err := toEvent(msg)
	// 			if err != nil {
	// 				klog.Errorf("failed to parse event %s", err)
	// 				continue
	// 			}

	// 			handleFn(evt)
	// 		}
	// 	}
	// }()

	select {
	case <-ctx.Done():
	case <-t.closeChan:
	}

	topics := []string{}
	for _, sub := range subs {
		topics = append(topics, sub.Topic)
	}

	if _, err := t.client.Unsubscribe(ctx, &paho.Unsubscribe{Topics: topics}); err != nil {
		return err
	}

	return nil
}

func (t *mqttTransport) ErrorChan() <-chan error {
	return t.errorChan
}

func (t *mqttTransport) Close(ctx context.Context) error {
	close(t.closeChan)
	return t.client.Disconnect(&paho.Disconnect{ReasonCode: 0})
}

func toPublishMsg(topic string, pubQoS int, evt cloudevents.Event) (*paho.Publish, error) {
	userProperties := []paho.UserProperty{}
	userProperties = append(userProperties, paho.UserProperty{
		Key:   prefix + sourceAttrKey,
		Value: evt.SpecVersion(),
	}, paho.UserProperty{
		Key:   prefix + idAttrKey,
		Value: evt.ID(),
	}, paho.UserProperty{
		Key:   prefix + typeAttrKey,
		Value: evt.Type(),
	}, paho.UserProperty{
		Key:   prefix + sourceAttrKey,
		Value: evt.Source(),
	}, paho.UserProperty{
		Key:   prefix + timeAttrKey,
		Value: evt.Time().Format(time.RFC3339),
	})

	for key, val := range evt.Extensions() {
		strVal, err := cetypes.Format(val)
		if err != nil {
			return nil, err
		}
		userProperties = append(userProperties, paho.UserProperty{
			Key:   prefix + key,
			Value: strVal,
		})
	}
	return &paho.Publish{
		QoS:   byte(pubQoS),
		Topic: topic,
		Properties: &paho.PublishProperties{
			User:        userProperties,
			ContentType: cloudevents.ApplicationJSON,
		},
		Payload: evt.Data(),
	}, nil
}

func toEvent(msg *paho.Publish) (cloudevents.Event, error) {
	evt := cloudevents.NewEvent()

	for _, property := range msg.Properties.User {
		switch property.Key {
		case prefix + specVersionAttrKey:
			evt.SetSpecVersion(property.Value)
		case prefix + idAttrKey:
			evt.SetID(property.Value)
		case prefix + typeAttrKey:
			evt.SetType(property.Value)
		case prefix + sourceAttrKey:
			evt.SetSource(property.Value)
		case prefix + timeAttrKey:
			t, err := time.Parse(time.RFC3339, property.Value)
			if err != nil {
				return evt, err
			}
			evt.SetTime(t)
		default:
			// using extensions to save other attributes
			evt.SetExtension(strings.TrimPrefix(property.Key, prefix), property.Value)
		}
	}

	if err := evt.SetData(cloudevents.ApplicationJSON, msg.Payload); err != nil {
		return evt, err
	}

	return evt, nil
}
