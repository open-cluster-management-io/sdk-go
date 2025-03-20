package store

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

// AgentInformerWatcherStore extends the baseStore.
// It gets/lists the works from the given informer store and send
// the work add/update/delete event to the watch channel directly.
//
// It is used for building ManifestWork agent client.
type AgentInformerWatcherStore[T generic.ResourceObject] struct {
	baseStore[T]
	informer cache.SharedIndexInformer
	watcher  *watcher
}

func NewAgentInformerWatcherStore[T generic.ResourceObject]() *AgentInformerWatcherStore[T] {
	return &AgentInformerWatcherStore[T]{
		baseStore: baseStore[T]{},
		watcher:   newWatcher(),
	}
}

func (s *AgentInformerWatcherStore[T]) Add(resource T) error {
	if obj, ok := any(resource).(runtime.Object); ok {
		s.watcher.Receive(watch.Event{Type: watch.Added, Object: obj.DeepCopyObject()})
	}
	return nil
}

func (s *AgentInformerWatcherStore[T]) Update(resource T) error {
	if obj, ok := any(resource).(runtime.Object); ok {
		s.watcher.Receive(watch.Event{Type: watch.Modified, Object: obj})
	}
	return nil
}

func (s *AgentInformerWatcherStore[T]) Delete(resource T) error {
	if obj, ok := any(resource).(runtime.Object); ok {
		s.watcher.Receive(watch.Event{Type: watch.Deleted, Object: obj})
	}
	return nil
}

func (s *AgentInformerWatcherStore[T]) HandleReceivedWork(action types.ResourceAction, resource T) error {
	switch action {
	case types.Added:
		return s.Add(resource)
	case types.Modified:
		new, err := meta.Accessor(resource)
		if err != nil {
			return err
		}

		last, exists, err := s.Get(new.GetNamespace(), new.GetName())
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("the resource %s/%s does not exist", new.GetNamespace(), new.GetName())
		}

		// prevent the resource from being updated if it is deleting
		if !last.GetDeletionTimestamp().IsZero() {
			klog.Warningf("the resource %s/%s is deleting, ignore the update", new.GetNamespace(), new.GetName())
			return nil
		}

		// updatedWork := work.DeepCopy()

		// // restore the fields that are maintained by local agent
		// updatedWork.Labels = lastWork.Labels
		// updatedWork.Annotations = lastWork.Annotations
		// updatedWork.Finalizers = lastWork.Finalizers
		// updatedWork.Status = lastWork.Status

		// return s.Update(updatedWork)
		return nil
	case types.Deleted:
		// the resource is deleting on the source, we just update its deletion timestamp.
		new, err := meta.Accessor(resource)
		if err != nil {
			return err
		}

		last, exists, err := s.Get(new.GetNamespace(), new.GetName())
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}

		updated := any(last).(runtime.Object).DeepCopyObject()
		updated.(metav1.Object).SetDeletionTimestamp(new.GetDeletionTimestamp())
		return s.Update(updated.(T))
	default:
		return fmt.Errorf("unsupported resource action %s", action)
	}
}

func (s *AgentInformerWatcherStore[T]) GetWatcher(namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	return s.watcher, nil
}

func (s *AgentInformerWatcherStore[T]) HasInitiated() bool {
	return s.initiated && s.informer.HasSynced()
}

func (s *AgentInformerWatcherStore[T]) SetInformer(informer cache.SharedIndexInformer) {
	s.informer = informer
	s.store = informer.GetStore()
	s.initiated = true
}
