package grpc

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetypes "github.com/cloudevents/sdk-go/v2/types"

	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/timestamppb"

	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/heartbeat"
)

const (
	prefix = "ce-" // for cloudevent-sdk compatibility

	timeAttrKey = "time"
)

type grpcTransport struct {
	sync.RWMutex

	opts *GRPCOptions

	client pbv1.CloudEventServiceClient

	closeChan chan struct{}
	errorChan chan error

	getSubscriptionRequest func() *pbv1.SubscriptionRequest
}

func NewAgentOptions(grpcOptions *GRPCOptions, clusterName, agentID string) *options.CloudEventsAgentOptions {
	return &options.CloudEventsAgentOptions{
		EventTransport: &grpcTransport{
			opts:      grpcOptions,
			closeChan: make(chan struct{}),
			errorChan: make(chan error),
			getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
				return &pbv1.SubscriptionRequest{
					// TODO: Update this code to determine the subscription source for the agent client.
					// Currently, the grpc agent client is not utilized, and the 'Source' field serves
					// as a placeholder with all the sources.
					Source:      types.SourceAll,
					ClusterName: clusterName,
				}
			},
		},
		AgentID:     agentID,
		ClusterName: clusterName,
	}
}

func NewSourceOptions(gRPCOptions *GRPCOptions, sourceID string) *options.CloudEventsSourceOptions {
	return &options.CloudEventsSourceOptions{
		EventTransport: &grpcTransport{
			opts:      gRPCOptions,
			closeChan: make(chan struct{}),
			errorChan: make(chan error),
			getSubscriptionRequest: func() *pbv1.SubscriptionRequest {
				return &pbv1.SubscriptionRequest{
					Source: sourceID,
				}
			},
		},
		SourceID: sourceID,
	}
}

func (t *grpcTransport) Connect(ctx context.Context) error {
	conn, err := t.opts.Dialer.Dial()
	if err != nil {
		return err
	}

	t.client = pbv1.NewCloudEventServiceClient(conn)

	// Start a goroutine to monitor the gRPC connection state changes
	go func() {
		state := conn.GetState()
		for {
			if !conn.WaitForStateChange(ctx, state) {
				// the ctx is closed, stop this watch
				klog.Infof("Stop watch grpc connection state")
				return
			}

			newState := conn.GetState()
			// If any failure in any of the steps needed to establish connection, or any failure
			// encountered while expecting successful communication on established channel, the
			// grpc client connection state will be TransientFailure.
			// When client certificate is expired, client will proactively close the connection,
			// which will result in connection state changed from Ready to Shutdown.
			// When server is closed, client will NOT close or reestablish the connection proactively,
			// it will only change the connection state from Ready to Idle.
			if newState == connectivity.TransientFailure || newState == connectivity.Shutdown ||
				newState == connectivity.Idle {
				select {
				case t.errorChan <- fmt.Errorf("grpc connection is disconnected (state=%s)", newState):
				default:
					klog.Errorf("no error channel available to report error: %v", err)
				}

				if newState != connectivity.Shutdown {
					// don't close the connection if it's already shutdown
					if err := conn.Close(); err != nil {
						klog.Errorf("failed to close gRPC connection, %v", err)
					}
				}
				return // exit the goroutine as the error handler function will handle the reconnection.
			}

			state = newState
		}
	}()

	return nil
}

func (t *grpcTransport) Send(ctx context.Context, evt cloudevents.Event) error {
	pbEvt, err := ToProtobufEvent(evt)
	if err != nil {
		return err
	}
	if _, err := t.client.Publish(ctx, &pbv1.PublishRequest{Event: pbEvt}); err != nil {
		return err
	}
	return nil
}

func (t *grpcTransport) Receive(ctx context.Context, handleFn options.ReceiveHandler) error {
	t.Lock()
	defer t.Unlock()

	subClient, err := t.client.Subscribe(ctx, t.getSubscriptionRequest())
	if err != nil {
		return err
	}

	subCtx, cancel := context.WithCancel(ctx)
	healthChecker := heartbeat.NewHealthChecker(t.opts.ServerHealthinessTimeout, t.errorChan)

	// start to receive the events from stream
	go t.startEventsReceiver(subCtx, subClient, healthChecker.Input(), handleFn)

	// start to watch the stream heartbeat
	go healthChecker.Start(subCtx)

	// Wait until external or internal context done
	select {
	case <-subCtx.Done():
	case <-t.closeChan:
	}

	// ensure the event receiver and heartbeat watcher are done
	cancel()

	return nil
}

func (t *grpcTransport) ErrorChan() <-chan error {
	return t.errorChan
}

func (t *grpcTransport) Close(ctx context.Context) error {
	close(t.closeChan)
	return t.opts.Dialer.Close()
}

func (t *grpcTransport) startEventsReceiver(ctx context.Context,
	subClient pbv1.CloudEventService_SubscribeClient,
	heartbeatCh chan *pbv1.CloudEvent,
	handleFn options.ReceiveHandler) {
	for {
		pbEvt, err := subClient.Recv()
		if err != nil {
			select {
			case t.errorChan <- fmt.Errorf("subscribe stream failed: %w", err):
			default:
			}
			return
		}

		if pbEvt.Type == types.HeartbeatCloudEventsType {
			select {
			case heartbeatCh <- pbEvt:
			case <-ctx.Done():
				return
			}
			continue
		}

		evt, err := ToCloudEvent(pbEvt)
		if err != nil {
			klog.Errorf("invalid event %v", err)
			continue
		}

		handleFn(evt)

		select {
		case <-ctx.Done():
			return
		case <-t.closeChan:
			return
		}
	}
}

func ToProtobufEvent(evt cloudevents.Event) (*pbv1.CloudEvent, error) {
	pbEvt := &pbv1.CloudEvent{
		SpecVersion: evt.SpecVersion(),
		Id:          evt.ID(),
		Type:        evt.Type(),
		Source:      evt.Source(),
		Attributes:  make(map[string]*pbv1.CloudEventAttributeValue),
		Data: &pbv1.CloudEvent_BinaryData{
			BinaryData: evt.Data(),
		},
	}

	timeAttr, err := attributeFor(evt.Time())
	if err != nil {
		return nil, err
	}
	pbEvt.Attributes[prefix+timeAttrKey] = timeAttr

	for key, val := range evt.Extensions() {
		attr, err := attributeFor(val)
		if err != nil {
			return nil, err
		}
		pbEvt.Attributes[prefix+key] = attr
	}

	return pbEvt, nil
}

func ToCloudEvent(pbEvent *pbv1.CloudEvent) (cloudevents.Event, error) {
	evt := cloudevents.NewEvent()
	evt.SetSpecVersion(pbEvent.SpecVersion)
	evt.SetID(pbEvent.Id)
	evt.SetType(pbEvent.Type)
	evt.SetSource(pbEvent.Source)

	for key, val := range pbEvent.Attributes {
		extension, err := attributeFrom(val)
		if err != nil {
			return evt, err
		}

		evt.SetExtension(strings.TrimPrefix(key, prefix), extension)
	}

	evt.SetData(cloudevents.ApplicationJSON, pbEvent.GetBinaryData())
	return evt, nil
}

func attributeFor(v interface{}) (*pbv1.CloudEventAttributeValue, error) {
	vv, err := cetypes.Validate(v)
	if err != nil {
		return nil, err
	}

	attr := &pbv1.CloudEventAttributeValue{}
	switch vt := vv.(type) {
	case bool:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeBoolean{
			CeBoolean: vt,
		}
	case int32:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeInteger{
			CeInteger: vt,
		}
	case string:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeString{
			CeString: vt,
		}
	case []byte:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeBytes{
			CeBytes: vt,
		}
	case cetypes.URI:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeUri{
			CeUri: vt.String(),
		}
	case cetypes.URIRef:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeUriRef{
			CeUriRef: vt.String(),
		}
	case cetypes.Timestamp:
		attr.Attr = &pbv1.CloudEventAttributeValue_CeTimestamp{
			CeTimestamp: timestamppb.New(vt.Time),
		}
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", vv)
	}
	return attr, nil
}

func attributeFrom(attr *pbv1.CloudEventAttributeValue) (interface{}, error) {
	var v interface{}
	switch vt := attr.Attr.(type) {
	case *pbv1.CloudEventAttributeValue_CeBoolean:
		v = vt.CeBoolean
	case *pbv1.CloudEventAttributeValue_CeInteger:
		v = vt.CeInteger
	case *pbv1.CloudEventAttributeValue_CeString:
		v = vt.CeString
	case *pbv1.CloudEventAttributeValue_CeBytes:
		v = vt.CeBytes
	case *pbv1.CloudEventAttributeValue_CeUri:
		uri, err := url.Parse(vt.CeUri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URI value %s: %s", vt.CeUri, err.Error())
		}
		v = uri
	case *pbv1.CloudEventAttributeValue_CeUriRef:
		uri, err := url.Parse(vt.CeUriRef)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URIRef value %s: %s", vt.CeUriRef, err.Error())
		}
		v = cetypes.URIRef{URL: *uri}
	case *pbv1.CloudEventAttributeValue_CeTimestamp:
		v = vt.CeTimestamp.AsTime()
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", vt)
	}
	return cetypes.Validate(v)
}
