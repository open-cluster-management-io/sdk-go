package fake

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
)

type CloudEventsFakeOptions struct {
	protocol options.CloudEventsProtocol
}

func NewAgentOptions(protocol options.CloudEventsProtocol, clusterName, agentID string) *options.CloudEventsAgentOptions {
	return &options.CloudEventsAgentOptions{
		CloudEventsOptions: &CloudEventsFakeOptions{protocol: protocol},
		AgentID:            agentID,
		ClusterName:        clusterName,
	}
}

func NewSourceOptions(protocol options.CloudEventsProtocol, sourceID string) *options.CloudEventsSourceOptions {
	return &options.CloudEventsSourceOptions{
		CloudEventsOptions: &CloudEventsFakeOptions{protocol: protocol},
		SourceID:           sourceID,
	}
}

func (o *CloudEventsFakeOptions) WithContext(ctx context.Context, evtCtx cloudevents.EventContext) (context.Context, error) {
	return ctx, nil
}

func (o *CloudEventsFakeOptions) Protocol(ctx context.Context) (options.CloudEventsProtocol, error) {
	return o.protocol, nil
}

func (o *CloudEventsFakeOptions) ErrorChan() <-chan error {
	return nil
}
