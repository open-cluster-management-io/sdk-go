package store

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
)

const syncedPollPeriod = 100 * time.Millisecond

// ResourceList is a collection of resources.
type ResourceList[T generic.ResourceObject] struct {
	// ListMeta describes list metadata
	metav1.ListMeta

	// Items is a list of resources.
	Items []T
}

// StoreInitiated is a function that can be used to determine if a store has initiated.
type StoreInitiated func() bool

// ConditionalUpdater is an optional capability of a ClientWatcherStore. Stores implementing it
// can update a resource with a compare-and-swap semantic: the update is applied only if the
// store's current resource version of the resource equals expectedResourceVersion (the value
// "0" forces the update), otherwise a conflict error is returned. The version check and the
// write are atomic with respect to the other store writers, which gives resource clients a
// read-modify-write guarantee that a concurrent writer (e.g. the handler of received resource
// events) cannot be interleaved between the version check and the write.
type ConditionalUpdater interface {
	// UpdateWithVersion updates the resource in the store if the store's current resource
	// version of the resource equals expectedResourceVersion, and returns a conflict error
	// otherwise.
	UpdateWithVersion(ctx context.Context, resource runtime.Object, expectedResourceVersion string) error
}

// ClientWatcherStore provides a watcher with a resource store.
type ClientWatcherStore[T generic.ResourceObject] interface {
	// GetWatcher returns a watcher to receive resource changes.
	GetWatcher(ctx context.Context, namespace string, opts metav1.ListOptions) (watch.Interface, error)

	// HandleReceivedResource handles the client received resource events.
	HandleReceivedResource(ctx context.Context, resource T) error

	// Add will be called by resource client when adding resources. The implementation is based on the specific
	// watcher store, in some case, it does not need to update a store, but just send a watch event.
	Add(resource runtime.Object) error

	// Update will be called by resource client when updating works. The implementation is based on the specific
	// watcher store, in some case, it does not need to update a store, but just send a watch event.
	Update(resource runtime.Object) error

	// Delete will be called by resource client when deleting works. The implementation is based on the specific
	// watcher store, in some case, it does not need to update a store, but just send a watch event.
	Delete(resource runtime.Object) error

	// List returns the resources from store for a given namespace with list options
	List(ctx context.Context, namespace string, opts metav1.ListOptions) (*ResourceList[T], error)

	// ListAll list all of the resources from store
	ListAll(ctx context.Context) ([]T, error)

	// Get returns a resource from store with resource namespace and name
	Get(ctx context.Context, namespace, name string) (T, bool, error)

	// HasInitiated marks the store has been initiated, A resync may be required after the store is initiated
	// when building a resource client.
	HasInitiated() bool
}

func WaitForStoreInit(ctx context.Context, cacheSyncs ...StoreInitiated) bool {
	logger := klog.FromContext(ctx)
	err := wait.PollUntilContextCancel(
		ctx,
		syncedPollPeriod,
		true,
		func(ctx context.Context) (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
	)
	if err != nil {
		logger.Error(err, "stop WaitForStoreInit")
		return false
	}

	return true
}
