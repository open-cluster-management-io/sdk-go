package options

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type ReceiveHandler func(cloudevents.Event)

type EventTransport interface {
	Connect(ctx context.Context) error
	Send(ctx context.Context, evt cloudevents.Event) error
	Receive(ctx context.Context, handler ReceiveHandler) error
	Close(ctx context.Context) error
	ErrorChan() <-chan error
}

// EventRateLimit for limiting the event sending rate.
type EventRateLimit struct {
	// QPS indicates the maximum QPS to send the event.
	// If it's less than or equal to zero, the DefaultQPS (50) will be used.
	QPS float32

	// Maximum burst for throttle.
	// If it's less than or equal to zero, the DefaultBurst (100) will be used.
	Burst int
}

// CloudEventsSourceOptions provides the required options to build a source CloudEventsClient
type CloudEventsSourceOptions struct {
	// EventTransport provides a transport to send/receive cloudevents based on different protocol.
	EventTransport EventTransport

	// SourceID is a unique identifier for a source, for example, it can generate a source ID by hashing the hub cluster
	// URL and appending the controller name. Similarly, a RESTful service can select a unique name or generate a unique
	// ID in the associated database for its source identification.
	SourceID string

	// EventRateLimit limits the event sending rate.
	EventRateLimit EventRateLimit
}

// CloudEventsAgentOptions provides the required options to build an agent CloudEventsClient
type CloudEventsAgentOptions struct {
	// EventTransport provides a transport to send/receive cloudevents based on different protocol.
	EventTransport EventTransport

	// AgentID is a unique identifier for an agent, for example, it can consist of a managed cluster name and an agent
	// name.
	AgentID string

	// ClusterName is the name of a managed cluster on which the agent runs.
	ClusterName string

	// EventRateLimit limits the event sending rate.
	EventRateLimit EventRateLimit
}
