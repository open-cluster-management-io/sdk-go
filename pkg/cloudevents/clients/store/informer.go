package store

import (
	"context"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/utils"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
)

// AgentInformerWatcherStore extends the BaseClientWatchStore.

// It gets/lists the resources from the given local store and send
// the resource add/update/delete event to the watch channel directly.
//
// It is used for building resource agent client.
type AgentInformerWatcherStore[T generic.ResourceObject] struct {
	BaseClientWatchStore[T]
	Watcher *Watcher

	// mu synchronizes the store writers, making the read-modify-write sequence in
	// HandleReceivedResource atomic with respect to the plain Add/Update/Delete writers
	// (e.g. a resource client writing back a patched resource).
	mu sync.Mutex
}

func NewAgentInformerWatcherStore[T generic.ResourceObject]() *AgentInformerWatcherStore[T] {
	return &AgentInformerWatcherStore[T]{
		BaseClientWatchStore: BaseClientWatchStore[T]{
			Store: cache.NewStore(cache.MetaNamespaceKeyFunc),
		},
		Watcher: NewWatcher(),
	}
}

func (s *AgentInformerWatcherStore[T]) Add(resource runtime.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.add(resource)
}

func (s *AgentInformerWatcherStore[T]) Update(resource runtime.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(resource)
}

func (s *AgentInformerWatcherStore[T]) Delete(resource runtime.Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delete(resource)
}

func (s *AgentInformerWatcherStore[T]) add(resource runtime.Object) error {
	s.Watcher.Receive(watch.Event{Type: watch.Added, Object: resource})
	return s.Store.Add(resource)
}

func (s *AgentInformerWatcherStore[T]) update(resource runtime.Object) error {
	s.Watcher.Receive(watch.Event{Type: watch.Modified, Object: resource})
	return s.Store.Update(resource)
}

func (s *AgentInformerWatcherStore[T]) delete(resource runtime.Object) error {
	s.Watcher.Receive(watch.Event{Type: watch.Deleted, Object: resource})
	return s.Store.Delete(resource)
}

func (s *AgentInformerWatcherStore[T]) HandleReceivedResource(ctx context.Context, resource T) error {
	// Hold the store lock for the whole read-modify-write sequence, so another store writer
	// cannot be interleaved between reading the cached resource and writing it back.
	s.mu.Lock()
	defer s.mu.Unlock()

	newRuntimeObj, err := utils.ToRuntimeObject(resource)
	if err != nil {
		return err
	}

	newMetaObj, err := meta.Accessor(newRuntimeObj)
	if err != nil {
		return err
	}

	if !newMetaObj.GetDeletionTimestamp().IsZero() {
		cachedResource, exists, err := s.findObjByUID(ctx, newMetaObj.GetUID())
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}

		cachedMetaObj, err := meta.Accessor(cachedResource)
		if err != nil {
			return err
		}

		// trigger an update event if the object is deleting.
		// Only need to update generation/finalizer/deletionTimeStamp of the object.
		if len(newMetaObj.GetFinalizers()) != 0 {
			cachedMetaObj.SetDeletionTimestamp(newMetaObj.GetDeletionTimestamp())
			cachedMetaObj.SetFinalizers(newMetaObj.GetFinalizers())
			cachedMetaObj.SetGeneration(newMetaObj.GetGeneration())
			cachedRuntimeObj, err := utils.ToRuntimeObject(cachedMetaObj)
			if err != nil {
				return err
			}
			return s.update(cachedRuntimeObj)
		}

		cachedRuntimeObj, err := utils.ToRuntimeObject(cachedMetaObj)
		if err != nil {
			return err
		}
		return s.delete(cachedRuntimeObj)
	}

	_, exists, err := s.Get(ctx, newMetaObj.GetNamespace(), newMetaObj.GetName())
	if err != nil {
		return err
	}
	if !exists {
		return s.add(newRuntimeObj)
	}

	return s.update(newRuntimeObj)
}

func (s *AgentInformerWatcherStore[T]) GetWatcher(ctx context.Context, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	// If AllowWatchBookmarks is enabled, send a bookmark event to signal the end of the initial event stream.
	// This is required by Kubernetes 1.35+ reflectors to properly initialize watches.
	if opts.AllowWatchBookmarks {
		// Send the bookmark event asynchronously to avoid blocking the watch initialization
		go func() {
			// Create a minimal object for the bookmark event with the required annotation
			// We need to use reflection to create a new instance of T
			var zero T
			bookmarkObj := reflect.New(reflect.TypeOf(zero).Elem()).Interface().(runtime.Object)
			if accessor, err := meta.Accessor(bookmarkObj); err == nil {
				accessor.SetResourceVersion(opts.ResourceVersion)
				accessor.SetAnnotations(map[string]string{
					metav1.InitialEventsAnnotationKey: "true",
				})
			}
			s.Watcher.Receive(watch.Event{Type: watch.Bookmark, Object: bookmarkObj})
		}()
	}

	return s.Watcher, nil
}

func (s *AgentInformerWatcherStore[T]) HasInitiated() bool {
	return true
}
