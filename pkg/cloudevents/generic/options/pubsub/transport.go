package pubsub

import (
	"context"
	"errors"
	"fmt"
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
		if o.CredentialsFile == "" {
			// use the insecure connection for local development/testing when no credentials are provided
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

	// initialize pubsub client, publisher and subscriber
	o.client = client
	if o.clusterName != "" && o.sourceID == "" {
		o.publisher = client.Publisher(o.Topics.AgentEvents)
		o.resyncPublisher = client.Publisher(o.Topics.AgentBroadcast)
		o.subscriber = client.Subscriber(o.Subscriptions.SourceEvents)
		o.resyncSubscriber = client.Subscriber(o.Subscriptions.SourceBroadcast)
	} else if o.sourceID != "" && o.clusterName == "" {
		o.publisher = client.Publisher(o.Topics.SourceEvents)
		o.resyncPublisher = client.Publisher(o.Topics.SourceBroadcast)
		o.subscriber = client.Subscriber(o.Subscriptions.AgentEvents)
		o.resyncSubscriber = client.Subscriber(o.Subscriptions.AgentBroadcast)
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
	go func() {
		err := o.subscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			evt, err := Decode(msg)
			if err != nil {
				// also send ACK on decode error since redelivery won't fix it.
				klog.Errorf("failed to decode pubsub message to resource spec/status event: %v", err)
			} else {
				fn(evt)
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
				klog.Warningf("subscriber is interrupted by error but error already sent, skipping: %v", err)
			}
		}
	}()

	// start the resync subscriber for broadcast messages
	go func() {
		err := o.resyncSubscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			evt, err := Decode(msg)
			if err != nil {
				// also send ACK on decode error since redelivery won't fix it.
				klog.Errorf("failed to decode pubsub message to resource resync event: %v", err)
			} else {
				fn(evt)
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
				o.errorChan <- fmt.Errorf("resync subscriber is interrupted by error: %w", err)
			} else {
				klog.Warningf("resync subscriber is interrupted by error but error already sent, skipping: %v", err)
			}
		}
	}()

	// wait for context cancellation or timeout
	<-ctx.Done()
	return ctx.Err()
}

func (o *pubsubTransport) Close(ctx context.Context) error {
	return o.client.Close()
}

func (o *pubsubTransport) ErrorChan() <-chan error {
	return o.errorChan
}

// toGRPCKeepaliveParameter converts our KeepaliveSettings to GRPC ClientParameters.
func toGRPCKeepaliveParameter(settings *KeepaliveSettings) keepalive.ClientParameters {
	keepaliveParameter := keepalive.ClientParameters{
		PermitWithoutStream: settings.PermitWithoutStream,
	}
	if settings.Time > 0 {
		keepaliveParameter.Time = settings.Time
	}
	if settings.Timeout > 0 {
		keepaliveParameter.Timeout = settings.Timeout
	}

	return keepaliveParameter
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
