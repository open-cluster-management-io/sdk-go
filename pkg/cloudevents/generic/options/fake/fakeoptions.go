package fake

import (
	"context"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
)

type EventChan struct {
	ErrChan chan error

	evtChan chan cloudevents.Event
	once    sync.Once
}

func NewAgentOptions(eventChan options.CloudEventTransport, clusterName, agentID string) *options.CloudEventsAgentOptions {
	return &options.CloudEventsAgentOptions{
		CloudEventsTransport: eventChan,
		AgentID:              agentID,
		ClusterName:          clusterName,
	}
}

func NewSourceOptions(eventChan options.CloudEventTransport, sourceID string) *options.CloudEventsSourceOptions {
	return &options.CloudEventsSourceOptions{
		CloudEventsTransport: eventChan,
		SourceID:             sourceID,
	}
}

func NewEventChan() *EventChan {
	return &EventChan{
		evtChan: make(chan cloudevents.Event, 5),
		ErrChan: make(chan error),
	}
}

func (c *EventChan) Connect(ctx context.Context) error {
	return nil
}

func (c *EventChan) Send(ctx context.Context, evt cloudevents.Event) error {
	select {
	case c.evtChan <- evt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("event channel is full")
	}
}

func (c *EventChan) Receive(ctx context.Context, fn options.ReceiveHandlerFn) error {
	for {
		select {
		case e, ok := <-c.evtChan:
			if !ok {
				return nil
			}
			fn(e)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *EventChan) Close(ctx context.Context) error {
	c.once.Do(func() {
		close(c.evtChan)
		close(c.ErrChan)
	})
	return nil
}

func (c *EventChan) ErrorChan() <-chan error {
	return c.ErrChan
}
