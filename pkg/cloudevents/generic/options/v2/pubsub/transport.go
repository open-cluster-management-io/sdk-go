package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

var _ options.CloudEventTransport = &pubsubTransport{}

// pubsubTransport is a CloudEventTransport implementation for Pub/Sub.
type pubsubTransport struct {
	PubSubOptions
	// Source ID, required for source
	sourceID string
	// cluster name, required for agent
	clusterName string
	client      *pubsub.Client
	// Publisher for spec/status updates
	publisher *pubsub.Publisher
	// Publisher for resync broadcasts
	resyncPublisher *pubsub.Publisher
	// Subscriber for spec/status updates
	subscriber *pubsub.Subscriber
	// Subscriber for resync broadcasts
	resyncSubscriber *pubsub.Subscriber
	// errorChan is to send an error message to reconnect the connection
	errorChan chan error
}

func (o *pubsubTransport) Connect(ctx context.Context) error {
	clientOptions := []option.ClientOption{}
	if o.CredentialsFile != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(o.CredentialsFile))
	}
	if o.Endpoint != "" {
		clientOptions = append(clientOptions, option.WithEndpoint(o.Endpoint))
		if strings.Contains(o.Endpoint, "localhost") || strings.Contains(o.Endpoint, "127.0.0.1") {
			// use the insecure connection for local development/testing
			pubsubConn, err := grpc.NewClient(o.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			clientOptions = append(clientOptions, option.WithGRPCConn(pubsubConn))
		}
	}

	if o.KeepaliveSettings != nil {
		// config keepalive parameters for pubsub client
		clientOptions = append(clientOptions, option.WithGRPCDialOption(grpc.WithKeepaliveParams(toGRPCKeepaliveParameter(o.KeepaliveSettings))))
	}

	client, err := pubsub.NewClient(ctx, o.ProjectID, clientOptions...)
	if err != nil {
		return err
	}

	// initialize pubsub client and publishers
	o.client = client
	if o.clusterName != "" && o.sourceID == "" {
		o.publisher = client.Publisher(o.Topics.AgentEvents)
		o.resyncPublisher = client.Publisher(o.Topics.AgentBroadcast)
	} else if o.sourceID != "" && o.clusterName == "" {
		o.publisher = client.Publisher(o.Topics.SourceEvents)
		o.resyncPublisher = client.Publisher(o.Topics.SourceBroadcast)
	} else {
		return fmt.Errorf("either source ID or cluster name must be set")
	}

	return nil
}

func (o *pubsubTransport) Subscribe(ctx context.Context) error {
	if o.client == nil {
		return fmt.Errorf("failed to initialize with nil pubsub client")
	}
	// initialize subscribers
	if o.clusterName != "" && o.sourceID == "" {
		o.subscriber = o.client.Subscriber(o.Subscriptions.SourceEvents)
		o.resyncSubscriber = o.client.Subscriber(o.Subscriptions.SourceBroadcast)
	} else if o.sourceID != "" && o.clusterName == "" {
		o.subscriber = o.client.Subscriber(o.Subscriptions.AgentEvents)
		o.resyncSubscriber = o.client.Subscriber(o.Subscriptions.AgentBroadcast)
	} else {
		return fmt.Errorf("either source ID or cluster name must be set")
	}

	// configure receive settings if provided
	if o.ReceiveSettings != nil {
		receiveSettings := toPubSubReceiveSettings(o.ReceiveSettings)
		o.subscriber.ReceiveSettings = receiveSettings
		o.resyncSubscriber.ReceiveSettings = receiveSettings
	}

	return nil
}

func (o *pubsubTransport) Send(ctx context.Context, evt cloudevents.Event) error {
	msg, err := Encode(evt)
	if err != nil {
		return err
	}

	eventType, err := types.ParseCloudEventsType(evt.Context.GetType())
	if err != nil {
		return fmt.Errorf("unsupported event type %s, %v", evt.Context.GetType(), err)
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

func (o *pubsubTransport) Receive(ctx context.Context, fn options.ReceiveHandlerFn) error {
	// Use an atomic flag to ensure only one of the subscriber or resync subscriber sends an error to errorChan.
	// This prevents triggering client reconnect and receiver restart twice if both subscribers fail.
	// 0 = no error sent, 1 = error sent
	var errorSent atomic.Int32

	// start the subscriber for spec/status updates
	go o.receiveFromSubscriber(ctx, o.subscriber, fn, &errorSent)

	// start the resync subscriber for broadcast messages
	go o.receiveFromSubscriber(ctx, o.resyncSubscriber, fn, &errorSent)

	// wait for context cancellation or timeout
	<-ctx.Done()
	return ctx.Err()
}

// receiveFromSubscriber handles receiving messages from a subscriber and error handling.
func (o *pubsubTransport) receiveFromSubscriber(
	ctx context.Context,
	subscriber *pubsub.Subscriber,
	fn options.ReceiveHandlerFn,
	errorSent *atomic.Int32,
) {
	logger := klog.FromContext(ctx)
	err := subscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		evt, err := Decode(msg)
		if err != nil {
			// also send ACK on decode error since redelivery won't fix it.
			logger.Error(err, "failed to decode pubsub message")
		} else {
			fn(ctx, evt)
		}
		// send ACK after all receiver handlers complete.
		msg.Ack()
	})
	// The Pub/Sub client's Receive call automatically retries on retryable errors.
	// See: https://github.com/googleapis/google-cloud-go/blob/b8e70aa0056a3e126bc36cb7bf242d987f32c0bd/pubsub/service.go#L51
	// If Receive call returns an error, it's usually due to a non-retryable issue or service outage, eg, subscription not found.
	// In such cases, we send the error to the error channel to signal the source/agent client to reconnect later.
	if err != nil {
		// for normal shutdown, won't send error to error channel.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		// use CompareAndSwap to atomically check and set the flag. Only the first subscriber wins the race.
		if errorSent.CompareAndSwap(0, 1) {
			o.errorChan <- fmt.Errorf("subscriber is interrupted by error: %w", err)
		} else {
			logger.Error(err, "subscriber is interrupted by error but error already sent, skipping...")
		}
	}
}

func (o *pubsubTransport) Close(ctx context.Context) error {
	return o.client.Close()
}

func (o *pubsubTransport) ErrorChan() <-chan error {
	return o.errorChan
}

// toGRPCKeepaliveParameter converts our KeepaliveSettings to GRPC ClientParameters.
func toGRPCKeepaliveParameter(settings *KeepaliveSettings) keepalive.ClientParameters {
	return keepalive.ClientParameters{
		PermitWithoutStream: settings.PermitWithoutStream,
		Time:                settings.Time,
		Timeout:             settings.Timeout,
	}
}

// toPubSubReceiveSettings converts our ReceiveSettings to Pub/Sub ReceiveSettings.
func toPubSubReceiveSettings(settings *ReceiveSettings) pubsub.ReceiveSettings {
	receiveSettings := pubsub.ReceiveSettings{}

	if settings.MaxExtension > 0 {
		receiveSettings.MaxExtension = settings.MaxExtension
	}
	if settings.MaxDurationPerAckExtension > 0 {
		receiveSettings.MaxDurationPerAckExtension = settings.MaxDurationPerAckExtension
	}
	if settings.MinDurationPerAckExtension > 0 {
		receiveSettings.MinDurationPerAckExtension = settings.MinDurationPerAckExtension
	}
	if settings.MaxOutstandingMessages > 0 {
		receiveSettings.MaxOutstandingMessages = settings.MaxOutstandingMessages
	}
	if settings.MaxOutstandingBytes > 0 {
		receiveSettings.MaxOutstandingBytes = settings.MaxOutstandingBytes
	}
	if settings.NumGoroutines > 0 {
		receiveSettings.NumGoroutines = settings.NumGoroutines
	}

	return receiveSettings
}
