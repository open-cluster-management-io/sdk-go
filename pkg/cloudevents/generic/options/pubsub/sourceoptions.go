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

var _ options.CloudEventTransport = &pubsubSourceTransport{}

// pubsubSourceTransport is a CloudEventTransport implementation for Pub/Sub for sources.
type pubsubSourceTransport struct {
	PubSubOptions
	sourceID string
	client   *pubsub.Client
	// Publisher for spec updates
	publisher *pubsub.Publisher
	// Publisher for resync broadcasts
	resyncPublisher *pubsub.Publisher
	// Subscriber for status updates
	subscriber *pubsub.Subscriber
	// Subscriber for resync broadcasts
	resyncSubscriber *pubsub.Subscriber
	// TODO: handle error channel
	errorChan chan error
}

// NewSourceOptions creates a new CloudEventsSourceOptions for Pub/Sub.
func NewSourceOptions(pubsubOptions *PubSubOptions,
	sourceID string) *options.CloudEventsSourceOptions {
	return &options.CloudEventsSourceOptions{
		CloudEventsTransport: &pubsubSourceTransport{
			PubSubOptions: *pubsubOptions,
			sourceID:      sourceID,
			errorChan:     make(chan error),
		},
		SourceID: sourceID,
	}
}

func (o *pubsubSourceTransport) Connect(ctx context.Context) error {
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
	o.publisher = client.Publisher(o.Topics.SourceEvents)
	o.resyncPublisher = client.Publisher(o.Topics.SourceBroadcast)
	o.subscriber = client.Subscriber(o.Subscriptions.AgentEvents)
	o.resyncSubscriber = client.Subscriber(o.Subscriptions.AgentBroadcast)

	return nil
}

func (o *pubsubSourceTransport) Send(ctx context.Context, evt cloudevents.Event) error {
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

func (o *pubsubSourceTransport) Receive(ctx context.Context, fn options.ReceiveHandlerFn) error {
	// create error channels for both subscribers
	subscriberErrChan := make(chan error, 1)
	resyncSubscriberErrChan := make(chan error, 1)

	// start the subscriber for status updates
	go func() {
		err := o.subscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			evt, err := Decode(msg)
			if err != nil {
				// also send ACK on decode error since redelivery won't fix it.
				klog.Errorf("failed to decode pubsub message to resource status event: %v", err)
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
				klog.Errorf("failed to decode pubsub message to resource specresync event: %v", err)
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

func (o *pubsubSourceTransport) Close(ctx context.Context) error {
	return o.client.Close()
}

func (o *pubsubSourceTransport) ErrorChan() <-chan error {
	return o.errorChan
}
