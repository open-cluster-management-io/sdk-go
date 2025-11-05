package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

var _ options.CloudEventTransport = &pubsubAgentTransport{}

// pubsubAgentTransport is a CloudEventTransport implementation for Pub/Sub for agents.
type pubsubAgentTransport struct {
	PubSubOptions
	clusterName string
	client      *pubsub.Client
	// Publisher for status updates
	publisher *pubsub.Publisher
	// Publisher for resync broadcasts
	resyncPublisher *pubsub.Publisher
	// Subscriber for spec updates
	subscriber *pubsub.Subscriber
	// Subscriber for resync broadcasts
	resyncSubscriber *pubsub.Subscriber
	// TODO: handle error channel
	errorChan chan error
}

// NewAgentOptions creates a new CloudEventsAgentOptions for Pub/Sub.
func NewAgentOptions(pubsubOptions *PubSubOptions,
	clusterName, agentID string) *options.CloudEventsAgentOptions {
	return &options.CloudEventsAgentOptions{
		CloudEventsTransport: &pubsubAgentTransport{
			PubSubOptions: *pubsubOptions,
			clusterName:   clusterName,
			errorChan:     make(chan error),
		},
		AgentID:     agentID,
		ClusterName: clusterName,
	}
}

func (o *pubsubAgentTransport) Connect(ctx context.Context) error {
	options := []option.ClientOption{}
	if o.CredentialsFile != "" {
		options = append(options, option.WithCredentialsFile(o.CredentialsFile))
	}
	if o.Endpoint != "" {
		options = append(options, option.WithEndpoint(o.Endpoint))
		if o.CredentialsFile == "" {
			// use the insecure connection for local development/testing when no credentials are provided
			pubsubConn, err := grpc.NewClient(o.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			options = append(options, option.WithGRPCConn(pubsubConn))
		}
	}

	client, err := pubsub.NewClient(ctx, o.ProjectID, options...)
	if err != nil {
		return err
	}

	// initialize pubsub client, publisher and subscriber
	o.client = client
	o.publisher = client.Publisher(o.Topics.AgentEvents)
	o.resyncPublisher = client.Publisher(o.Topics.AgentBroadcast)
	o.subscriber = client.Subscriber(o.Subscriptions.SourceEvents)
	o.resyncSubscriber = client.Subscriber(o.Subscriptions.SourceBroadcast)

	return nil
}

func (o *pubsubAgentTransport) Send(ctx context.Context, evt cloudevents.Event) error {
	msg, err := Encode(evt)
	if err != nil {
		return err
	}

	eventType, err := types.ParseCloudEventsType(evt.Context.GetType())
	if err != nil {
		return fmt.Errorf("unsupported event type %s, %v", eventType, err)
	}

	// determine publisher based on event type
	var result *pubsub.PublishResult
	if eventType.Action == types.ResyncRequestAction {
		result = o.resyncPublisher.Publish(ctx, msg)
	} else {
		result = o.publisher.Publish(ctx, msg)
	}

	// block until the result is returned
	_, err = result.Get(ctx)
	return err
}

func (o *pubsubAgentTransport) Receive(ctx context.Context, fn options.ReceiveHandlerFn) error {
	// create error channels for both subscribers
	subscriberErrChan := make(chan error, 1)
	resyncSubscriberErrChan := make(chan error, 1)

	// start the subscriber for spec updates
	go func() {
		err := o.subscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			evt, err := Decode(msg)
			if err != nil {
				// also send ACK on decode error since redelivery won't fix it.
				klog.Errorf("failed to decode pubsub message to resource spec event: %v", err)
			} else {
				fn(evt)
			}
			// send ACK after all receiver handlers complete.
			msg.Ack()
		})
		if err != nil {
			subscriberErrChan <- fmt.Errorf("subscriber is interrupted by error: %w", err)
		}
	}()

	// start the resync subscriber for broadcast messages
	go func() {
		err := o.resyncSubscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			evt, err := Decode(msg)
			if err != nil {
				// also send ACK on decode error since redelivery won't fix it.
				klog.Errorf("failed to decode pubsub message to resource statusresync event: %v", err)
			} else {
				fn(evt)
			}
			// send ACK after all receiver handlers complete.
			msg.Ack()
		})
		if err != nil {
			resyncSubscriberErrChan <- fmt.Errorf("resync subscriber is interrupted by error: %w", err)
		}
	}()

	// wait for either subscriber to error or context cancellation
	select {
	case err := <-subscriberErrChan:
		return err
	case err := <-resyncSubscriberErrChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *pubsubAgentTransport) Close(ctx context.Context) error {
	return o.client.Close()
}

func (o *pubsubAgentTransport) ErrorChan() <-chan error {
	return o.errorChan
}
