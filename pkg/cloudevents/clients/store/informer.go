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

	// watcherLock guards Watcher, which GetWatcher replaces once the current
	// one has been stopped.
	watcherLock sync.RWMutex
	Watcher     *Watcher
}

func NewAgentInformerWatcherStore[T generic.ResourceObject]() *AgentInformerWatcherStore[T] {
	return &AgentInformerWatcherStore[T]{
		BaseClientWatchStore: BaseClientWatchStore[T]{
			Store: cache.NewStore(cache.MetaNamespaceKeyFunc),
		},
		Watcher: NewWatcher(),
	}
}

// currentWatcher returns the watcher events should be sent to right now. It is
// not necessarily the one GetWatcher returned last, since a reflector can stop
// its watch at any time.
func (s *AgentInformerWatcherStore[T]) currentWatcher() *Watcher {
	s.watcherLock.RLock()
	defer s.watcherLock.RUnlock()

	return s.Watcher
}

func (s *AgentInformerWatcherStore[T]) Add(resource runtime.Object) error {
	s.currentWatcher().Receive(watch.Event{Type: watch.Added, Object: resource})
	return s.Store.Add(resource)
}

func (s *AgentInformerWatcherStore[T]) Update(resource runtime.Object) error {
	s.currentWatcher().Receive(watch.Event{Type: watch.Modified, Object: resource})
	return s.Store.Update(resource)
}

func (s *AgentInformerWatcherStore[T]) Delete(resource runtime.Object) error {
	s.currentWatcher().Receive(watch.Event{Type: watch.Deleted, Object: resource})
	return s.Store.Delete(resource)
}

func (s *AgentInformerWatcherStore[T]) HandleReceivedResource(ctx context.Context, resource T) error {
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
			return s.Update(cachedRuntimeObj)
		}

		cachedRuntimeObj, err := utils.ToRuntimeObject(cachedMetaObj)
		if err != nil {
			return err
		}
		return s.Delete(cachedRuntimeObj)
	}

	_, exists, err := s.Get(ctx, newMetaObj.GetNamespace(), newMetaObj.GetName())
	if err != nil {
		return err
	}
	if !exists {
		return s.Add(newRuntimeObj)
	}

	return s.Update(newRuntimeObj)
}

func (s *AgentInformerWatcherStore[T]) GetWatcher(ctx context.Context, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	// A reflector stops its watch every time its watch loop exits and then asks
	// for a new one. Handing back the stopped watcher would make every later
	// Receive a silent no-op, leaving the informer permanently deaf: the agent
	// would never see another spec change, so a ManifestWork deletion would
	// never be acted on and its finalizer would never be removed.
	s.watcherLock.Lock()
	if s.Watcher == nil || s.Watcher.IsStopped() {
		s.Watcher = NewWatcher()
	}
	watcher := s.Watcher
	s.watcherLock.Unlock()

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
			watcher.Receive(watch.Event{Type: watch.Bookmark, Object: bookmarkObj})
		}()
	}

	return watcher, nil
}

func (s *AgentInformerWatcherStore[T]) HasInitiated() bool {
	return true
}
