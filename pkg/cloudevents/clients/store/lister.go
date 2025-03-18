package store

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/common"
)

// WatcherStoreAgentLister list the ManifestWorks from WatcherStore
type WatcherStoreAgentLister[T generic.ResourceObject] struct {
	store WatcherStore[T]
}

func NewWatcherStoreAgentLister[T generic.ResourceObject](store WatcherStore[T]) *WatcherStoreAgentLister[T] {
	return &WatcherStoreAgentLister[T]{
		store: store,
	}
}

// List returns the ManifestWorks from a WorkClientWatcherStore with list options
func (l *WatcherStoreAgentLister[T]) List(options types.ListOptions) ([]T, error) {
	opts := metav1.ListOptions{}

	if options.Source != types.SourceAll {
		opts.LabelSelector = fmt.Sprintf("%s=%s", common.CloudEventsOriginalSourceLabelKey, options.Source)
	}

	list, err := l.store.List(options.ClusterName, opts)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// WatcherStoreSourceLister list the ManifestWorks from the WatcherStore.
type WatcherStoreSourceLister[T generic.ResourceObject] struct {
	store WatcherStore[T]
}

func NewWatcherStoreSourceLister[T generic.ResourceObject](store WatcherStore[T]) *WatcherStoreSourceLister[T] {
	return &WatcherStoreSourceLister[T]{
		store: store,
	}
}

// List returns the ManifestWorks from the WorkClientWatcherCache with list options.
func (l *WatcherStoreSourceLister[T]) List(options types.ListOptions) ([]T, error) {
	list, err := l.store.List(options.ClusterName, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return list, nil
}
