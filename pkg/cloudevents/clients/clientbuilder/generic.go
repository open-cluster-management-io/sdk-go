package builder

import (
	"context"

	"k8s.io/klog/v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/statushash"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/store"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type GenericClientBuilder[T generic.ResourceObject] struct {
	config       any
	watcherStore store.WatcherStore[T]
	codec        generic.Codec[T]
	sourceID     string
	clusterName  string
	clientID     string
	resync       bool
}

func (b *GenericClientBuilder[T]) AgentClient(ctx context.Context) (*generic.CloudEventAgentClient[T], error) {
	options, err := generic.BuildCloudEventsAgentOptions(b.config, b.clusterName, b.clientID)
	if err != nil {
		return nil, err
	}

	cloudEventsClient, err := generic.NewCloudEventAgentClient(
		ctx,
		options,
		store.NewWatcherStoreAgentLister(b.watcherStore),
		statushash.StatusHash,
		b.codec,
	)
	if err != nil {
		return nil, err
	}

	// start to subscribe
	cloudEventsClient.Subscribe(ctx, b.watcherStore.HandleReceivedResource)

	// start a go routine to receive client reconnect signal
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-cloudEventsClient.ReconnectedChan():
				if !b.resync {
					klog.V(4).Infof("resync is disabled, do nothing")
					continue
				}

				// when receiving a client reconnected signal, we resync all sources for this agent
				// TODO after supporting multiple sources, we should only resync agent known sources
				if err := cloudEventsClient.Resync(ctx, types.SourceAll); err != nil {
					klog.Errorf("failed to send resync request, %v", err)
				}
			}
		}
	}()

	if !b.resync {
		return cloudEventsClient, nil
	}

	// start a go routine to resync the works after this client's store is initiated
	go func() {
		if store.WaitForStoreInit(ctx, b.watcherStore.HasInitiated) {
			if err := cloudEventsClient.Resync(ctx, types.SourceAll); err != nil {
				klog.Errorf("failed to send resync request, %v", err)
			}
		}
	}()

	return cloudEventsClient, nil
}

func (b *GenericClientBuilder[T]) SourceClient(ctx context.Context) (*generic.CloudEventSourceClient[T], error) {
	options, err := generic.BuildCloudEventsSourceOptions(b.config, b.clientID, b.sourceID)
	if err != nil {
		return nil, err
	}

	cloudEventsClient, err := generic.NewCloudEventSourceClient(
		ctx,
		options,
		store.NewWatcherStoreSourceLister(b.watcherStore),
		statushash.StatusHash,
		b.codec,
	)
	if err != nil {
		return nil, err
	}

	// start to subscribe
	cloudEventsClient.Subscribe(ctx, b.watcherStore.HandleReceivedResource)
	// start a go routine to receive client reconnect signal
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-cloudEventsClient.ReconnectedChan():
				if !b.resync {
					klog.V(4).Infof("resync is disabled, do nothing")
					continue
				}

				// when receiving a client reconnected signal, we resync all clusters for this source
				if err := cloudEventsClient.Resync(ctx, types.ClusterAll); err != nil {
					klog.Errorf("failed to send resync request, %v", err)
				}
			}
		}
	}()

	if !b.resync {
		return cloudEventsClient, nil
	}

	// start a go routine to resync the works after this client's store is initiated
	go func() {
		if store.WaitForStoreInit(ctx, b.watcherStore.HasInitiated) {
			if err := cloudEventsClient.Resync(ctx, types.ClusterAll); err != nil {
				klog.Errorf("failed to send resync request, %v", err)
			}
		}
	}()

	return cloudEventsClient, nil
}
