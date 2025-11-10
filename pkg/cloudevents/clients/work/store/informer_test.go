package store

import (
	"context"
	"k8s.io/client-go/tools/cache"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	workfake "open-cluster-management.io/api/client/work/clientset/versioned/fake"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestVersioner(t *testing.T) {
	t.Run("increment version for new resource", func(t *testing.T) {
		v := newVersioner()
		version := v.increment("test-work")
		if version != 1 {
			t.Errorf("expected version 1 for new resource, got %d", version)
		}
	})

	t.Run("increment version for existing resource", func(t *testing.T) {
		v := newVersioner()
		v.increment("test-work")
		version := v.increment("test-work")
		if version != 2 {
			t.Errorf("expected version 2 for existing resource, got %d", version)
		}
	})

	t.Run("increment multiple times", func(t *testing.T) {
		v := newVersioner()
		for i := 1; i <= 5; i++ {
			version := v.increment("test-work")
			if version != int64(i) {
				t.Errorf("expected version %d, got %d", i, version)
			}
		}
	})

	t.Run("delete version", func(t *testing.T) {
		v := newVersioner()
		v.increment("test-work")
		v.delete("test-work")
		version := v.increment("test-work")
		if version != 1 {
			t.Errorf("expected version 1 after delete, got %d", version)
		}
	})

	t.Run("concurrent increments", func(t *testing.T) {
		v := newVersioner()
		done := make(chan bool)

		for i := 0; i < 10; i++ {
			go func() {
				v.increment("test-work")
				done <- true
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}

		finalVersion := v.increment("test-work")
		if finalVersion != 11 {
			t.Errorf("expected version 11 after 10 concurrent increments, got %d", finalVersion)
		}
	})
}

func TestAgentInformerWatcherStore_Add(t *testing.T) {
	store := NewAgentInformerWatcherStore()
	store.Store = cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})

	// Start consuming events to prevent blocking
	watcher, err := store.GetWatcher("", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		watcher.Stop()
	}()

	go func() {
		ch := watcher.ResultChan()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				// consume events
			}
		}
	}()

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	err = store.Add(work)
	if err != nil {
		t.Fatalf("unexpected error adding work: %v", err)
	}

	if work.ResourceVersion != "1" {
		t.Errorf("expected resource version 1, got %s", work.ResourceVersion)
	}

	// Add another work
	work2 := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	err = store.Add(work2)
	if err != nil {
		t.Fatalf("unexpected error adding work: %v", err)
	}

	if work2.ResourceVersion != "2" {
		t.Errorf("expected resource version 2 for second add, got %s", work2.ResourceVersion)
	}
}

func TestAgentInformerWatcherStore_Update(t *testing.T) {
	store := NewAgentInformerWatcherStore()
	store.Store = cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})

	// Start consuming events to prevent blocking
	watcher, err := store.GetWatcher("", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		watcher.Stop()
	}()

	go func() {
		ch := watcher.ResultChan()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				// consume events
			}
		}
	}()

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	// Add the work first
	err = store.Add(work)
	if err != nil {
		t.Fatalf("unexpected error adding work: %v", err)
	}

	// Update the work
	updatedWork := work.DeepCopy()
	updatedWork.Spec.Workload.Manifests = []workv1.Manifest{
		{
			RawExtension: runtime.RawExtension{
				Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap"}`),
			},
		},
	}

	err = store.Update(updatedWork)
	if err != nil {
		t.Fatalf("unexpected error updating work: %v", err)
	}

	if updatedWork.ResourceVersion != "2" {
		t.Errorf("expected resource version 2 after update, got %s", updatedWork.ResourceVersion)
	}

	// Update again
	err = store.Update(updatedWork)
	if err != nil {
		t.Fatalf("unexpected error updating work: %v", err)
	}

	if updatedWork.ResourceVersion != "3" {
		t.Errorf("expected resource version 3 after second update, got %s", updatedWork.ResourceVersion)
	}
}

func TestAgentInformerWatcherStore_Delete(t *testing.T) {
	store := NewAgentInformerWatcherStore()
	store.Store = cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})

	// Start consuming events to prevent blocking
	watcher, err := store.GetWatcher("", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		watcher.Stop()
	}()

	go func() {
		ch := watcher.ResultChan()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				// consume events
			}
		}
	}()

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	// Add and update to increment version
	err = store.Add(work)
	if err != nil {
		t.Fatalf("unexpected error adding work: %v", err)
	}

	err = store.Update(work)
	if err != nil {
		t.Fatalf("unexpected error updating work: %v", err)
	}

	// Delete the work
	err = store.Delete(work)
	if err != nil {
		t.Fatalf("unexpected error deleting work: %v", err)
	}

	// Add the same work again, version should be reset to 1
	newWork := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	err = store.Add(newWork)
	if err != nil {
		t.Fatalf("unexpected error adding work after delete: %v", err)
	}

	if newWork.ResourceVersion != "1" {
		t.Errorf("expected resource version 1 after delete and re-add, got %s", newWork.ResourceVersion)
	}
}

func TestAgentInformerWatcherStore_ResourceVersionIncrement(t *testing.T) {
	store := NewAgentInformerWatcherStore()
	store.Store = cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})

	// Start consuming events to prevent blocking
	watcher, err := store.GetWatcher("", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		watcher.Stop()
	}()

	go func() {
		ch := watcher.ResultChan()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				// consume events
			}
		}
	}()

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	// Verify resource versions increment properly
	expectedVersions := []string{"1", "2", "3", "4"}

	// Add
	err = store.Add(work)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if work.ResourceVersion != expectedVersions[0] {
		t.Errorf("expected version %s, got %s", expectedVersions[0], work.ResourceVersion)
	}

	// Multiple updates
	for i := 1; i < len(expectedVersions); i++ {
		err = store.Update(work)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if work.ResourceVersion != expectedVersions[i] {
			t.Errorf("expected version %s, got %s", expectedVersions[i], work.ResourceVersion)
		}
	}
}

func TestSourceInformerWatcherStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)
	workStore := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore()

	sourceStore := NewSourceInformerWatcherStore(ctx)
	sourceStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	// Start consuming watch events to prevent blocking
	watcher, err := sourceStore.GetWatcher(metav1.NamespaceAll, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}
	defer watcher.Stop()

	go func() {
		ch := watcher.ResultChan()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				// consume events
			}
		}
	}()

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	err = workStore.Add(work)
	if err != nil {
		t.Fatalf("unexpected error adding work to informer store: %v", err)
	}

	// Test Get
	retrievedWork, exists, err := sourceStore.Get("test-cluster", "test-work")
	if err != nil {
		t.Fatalf("unexpected error getting work: %v", err)
	}
	if !exists {
		t.Error("expected work to exist")
	}
	if retrievedWork.Name != "test-work" {
		t.Errorf("expected work name test-work, got %s", retrievedWork.Name)
	}

	// Test Add event
	err = sourceStore.Add(work)
	if err != nil {
		t.Fatalf("unexpected error calling Add: %v", err)
	}

	// Test Update event
	err = sourceStore.Update(work)
	if err != nil {
		t.Fatalf("unexpected error calling Update: %v", err)
	}

	// Test Delete event
	err = sourceStore.Delete(work)
	if err != nil {
		t.Fatalf("unexpected error calling Delete: %v", err)
	}
}

func TestSourceInformerWatcherStore_Watch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	sourceStore := NewSourceInformerWatcherStore(ctx)
	sourceStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	// Test GetWatcher with namespace=all
	watcher, err := sourceStore.GetWatcher(metav1.NamespaceAll, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}
	if watcher == nil {
		t.Error("expected watcher to not be nil")
	}

	// Test GetWatcher with specific namespace should fail
	_, err = sourceStore.GetWatcher("specific-namespace", metav1.ListOptions{})
	if err == nil {
		t.Error("expected error when watching specific namespace, got nil")
	}
}

func TestSourceInformerWatcherStore_HasInitiated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	sourceStore := NewSourceInformerWatcherStore(ctx)

	// Start consuming watch events to prevent blocking
	watcher, err := sourceStore.GetWatcher(metav1.NamespaceAll, metav1.ListOptions{})
	if err == nil {
		defer watcher.Stop()
		go func() {
			ch := watcher.ResultChan()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-ch:
					if !ok {
						return
					}
				}
			}
		}()
	}

	// Should not be initiated before SetInformer
	if sourceStore.HasInitiated() {
		t.Error("expected store to not be initiated before SetInformer")
	}

	sourceStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	// Start the informer
	workInformerFactory.Start(ctx.Done())
	workInformerFactory.WaitForCacheSync(ctx.Done())

	// Should be initiated after SetInformer and cache sync
	if !sourceStore.HasInitiated() {
		t.Error("expected store to be initiated after SetInformer and cache sync")
	}
}

func TestAgentInformerWatcherStore_WatchEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	store := NewAgentInformerWatcherStore()
	store.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	// Add initial work to the informer store so HandleReceivedResource can find it for updates
	initialWork := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(initialWork); err != nil {
		t.Fatalf("unexpected error adding initial work to informer: %v", err)
	}

	// Get a watcher
	watcher, err := store.GetWatcher("", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

	defer func() {
		watcher.Stop()
		cancel()
	}()

	// Channel to collect events
	events := []watch.EventType{}
	done := make(chan struct{})

	go func() {
		defer close(done)
		ch := watcher.ResultChan()
		for i := 0; i < 3; i++ {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				events = append(events, event.Type)
			}
		}
	}()

	// Add a work
	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	err = store.HandleReceivedResource(ctx, types.Added, work)
	if err != nil {
		t.Fatalf("unexpected error handling added resource: %v", err)
	}

	// Update the work
	err = store.HandleReceivedResource(ctx, types.Modified, work)
	if err != nil {
		t.Fatalf("unexpected error handling modified resource: %v", err)
	}

	// Delete the work
	deletedWork := work.DeepCopy()
	deletedWork.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	err = store.HandleReceivedResource(ctx, types.Deleted, deletedWork)
	if err != nil {
		t.Fatalf("unexpected error handling deleted resource: %v", err)
	}

	<-done

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// HandleReceivedResource with types.Deleted calls Update (not Delete),
	// so we expect Added, Modified, Modified (not Added, Modified, Deleted)
	expectedEvents := []watch.EventType{watch.Added, watch.Modified, watch.Modified}
	for i, expected := range expectedEvents {
		if events[i] != expected {
			t.Errorf("event %d: expected %s, got %s", i, expected, events[i])
		}
	}
}
