package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
)

func TestGet(t *testing.T) {
	watchStore := NewAgentInformerWatcherStore[*clusterv1.ManagedCluster]()

	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "test1"}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "test2"}}); err != nil {
		t.Error(err)
	}

	cases := []struct {
		name        string
		clusterName string
		expected    bool
	}{
		{
			name:        "test1 exists",
			clusterName: "test1",
			expected:    true,
		},
		{
			name:        "test2 exists",
			clusterName: "test2",
			expected:    true,
		},
		{
			name:        "test does not exist",
			clusterName: "test",
			expected:    false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, exists, err := watchStore.Get(context.Background(), "", c.clusterName)
			if err != nil {
				t.Error(err)
			}
			if exists != c.expected {
				t.Error("expect test1 exists, but failed")
			}
		})
	}
}

func TestList(t *testing.T) {
	watchStore := NewAgentInformerWatcherStore[*clusterv1.ManagedCluster]()

	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{
		Name:   "test1",
		Labels: map[string]string{"test": "true"},
	}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{
		Name:   "test2",
		Labels: map[string]string{"test": "true"},
	}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "test3",
	}}); err != nil {
		t.Error(err)
	}

	clusters, err := watchStore.List(context.Background(), "", metav1.ListOptions{LabelSelector: "test=true"})
	if err != nil {
		t.Error(err)
	}

	if len(clusters.Items) != 2 {
		t.Error("expect 2, but failed")
	}

	all, err := watchStore.ListAll(context.Background())
	if err != nil {
		t.Error(err)
	}

	if len(all) != 3 {
		t.Error("expect 2, but failed")
	}
}

type receiveResult struct {
	sync.RWMutex

	addedReceived    bool
	modifiedReceived bool
	deletedReceived  bool
}

func (r *receiveResult) added() {
	r.Lock()
	defer r.Unlock()
	r.addedReceived = true
}

func (r *receiveResult) updated() {
	r.Lock()
	defer r.Unlock()
	r.modifiedReceived = true
}

func (r *receiveResult) deleted() {
	r.Lock()
	defer r.Unlock()
	r.deletedReceived = true
}

func (r *receiveResult) result() bool {
	r.RLock()
	defer r.RUnlock()
	return r.addedReceived && r.modifiedReceived && r.deletedReceived
}

func TestWatch(t *testing.T) {
	watchStore := NewAgentInformerWatcherStore[*clusterv1.ManagedCluster]()
	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "test1"}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "test2"}}); err != nil {
		t.Error(err)
	}

	watcher, err := watchStore.GetWatcher(context.Background(), "", metav1.ListOptions{})
	if err != nil {
		t.Error(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		watcher.Stop()
		cancel()
	}()

	received := &receiveResult{}

	go func() {
		ch := watcher.ResultChan()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				switch event.Type {
				case watch.Added:
					received.added()
				case watch.Modified:
					received.updated()
				case watch.Deleted:
					received.deleted()
				}
			}
		}
	}()

	if err := watchStore.HandleReceivedResource(ctx, &clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "test0"}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.HandleReceivedResource(ctx, &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test1",
		},
		Status: clusterv1.ManagedClusterStatus{
			Version: clusterv1.ManagedClusterVersion{Kubernetes: "1.23"},
		}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.HandleReceivedResource(ctx, &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test1",
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
	}); err != nil {
		t.Error(err)
	}

	require.Eventually(t, func() bool {
		return received.result()
	}, 5*time.Second, time.Second)
}

// TestWatchAfterWatcherStopped covers the reflector's behaviour of stopping its
// watch whenever its watch loop exits and then asking for a new one. The store
// must hand back a working watcher: reusing the stopped one made every later
// Receive a silent no-op, so the informer never saw another event. For a work
// agent that meant a ManifestWork deletion was never acted on and its finalizer
// was never removed.
func TestWatchAfterWatcherStopped(t *testing.T) {
	watchStore := NewAgentInformerWatcherStore[*clusterv1.ManagedCluster]()

	first, err := watchStore.GetWatcher(context.Background(), "", metav1.ListOptions{})
	require.NoError(t, err)

	// What client-go's reflector does via `defer w.Stop()`.
	first.Stop()

	second, err := watchStore.GetWatcher(context.Background(), "", metav1.ListOptions{})
	require.NoError(t, err)
	require.NotSame(t, first, second, "expected a new watcher after the previous one was stopped")

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		second.Stop()
		cancel()
	}()

	events := make(chan watch.Event, 1)
	go func() {
		for evt := range second.ResultChan() {
			select {
			case events <- evt:
			case <-ctx.Done():
				return
			}
		}
	}()

	require.NoError(t, watchStore.HandleReceivedResource(ctx,
		&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "test1"}}))

	select {
	case evt := <-events:
		require.Equal(t, watch.Added, evt.Type)
	case <-time.After(10 * time.Second):
		t.Fatal("event was dropped: the store kept using the stopped watcher")
	}
}
