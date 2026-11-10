package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/store"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

// hookedCloudEventsClient is a CloudEventsClient whose Publish invokes an optional hook,
// allowing tests to interleave store writes while a publish is in flight.
type hookedCloudEventsClient struct {
	publishHook func(eventType types.CloudEventsType, work *workv1.ManifestWork)
}

var _ generic.CloudEventsClient[*workv1.ManifestWork] = &hookedCloudEventsClient{}

func (c *hookedCloudEventsClient) Resync(ctx context.Context, clusterName string) error {
	return nil
}

func (c *hookedCloudEventsClient) Publish(ctx context.Context, eventType types.CloudEventsType, work *workv1.ManifestWork) error {
	if c.publishHook != nil {
		c.publishHook(eventType, work)
	}
	return nil
}

func (c *hookedCloudEventsClient) Subscribe(ctx context.Context, handlers ...generic.ResourceHandler[*workv1.ManifestWork]) {
}

func (c *hookedCloudEventsClient) SubscribedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func newRaceTestWork() *workv1.ManifestWork {
	return &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-work",
			Namespace:  "test-cluster",
			UID:        kubetypes.UID("test-uid"),
			Finalizers: []string{"cluster.open-cluster-management.io/manifest-work-cleanup"},
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}
}

func drainWatcher(t *testing.T, ctx context.Context, watcherStore *store.AgentInformerWatcherStore) func() {
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

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

	return watcher.Stop
}

// TestManifestWorkAgentClient_Patch_DeleteEventDuringStatusPublish verifies that a delete
// event applied to the store while a status patch is publishing cannot be overwritten by
// the patch's store write: the write is rejected with a conflict and the deletion
// timestamp is preserved.
func TestManifestWorkAgentClient_Patch_DeleteEventDuringStatusPublish(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopWatcher := drainWatcher(t, ctx, watcherStore)
	defer stopWatcher()

	publishStarted := make(chan struct{})
	releasePublish := make(chan struct{})

	mockClient := &hookedCloudEventsClient{
		publishHook: func(eventType types.CloudEventsType, work *workv1.ManifestWork) {
			close(publishStarted)
			<-releasePublish
		},
	}

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, mockClient)
	client.SetNamespace("test-cluster")

	work := newRaceTestWork()
	if err := watcherStore.Add(work.DeepCopy()); err != nil {
		t.Fatalf("unexpected error adding work: %v", err)
	}

	patchErrCh := make(chan error, 1)
	go func() {
		patchData := []byte(`{"status":{"conditions":[{"type":"Applied","status":"True","reason":"AppliedManifestWorkComplete","message":"","lastTransitionTime":null}]}}`)
		_, err := client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{}, "status")
		patchErrCh <- err
	}()

	select {
	case <-publishStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the status publish to start")
	}

	// a delete event arrives from the source while the status publish is in flight
	now := metav1.Now()
	deletingWork := newRaceTestWork()
	deletingWork.DeletionTimestamp = &now
	if err := watcherStore.HandleReceivedResource(ctx, deletingWork); err != nil {
		t.Fatalf("unexpected error handling delete event: %v", err)
	}
	close(releasePublish)

	var patchErr error
	select {
	case patchErr = <-patchErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the patch to return")
	}

	if !errors.IsConflict(patchErr) {
		t.Errorf("expected a conflict error from the stale patch, got %v", patchErr)
	}

	got, exists, err := watcherStore.Get(ctx, "test-cluster", "test-work")
	if err != nil {
		t.Fatalf("unexpected error getting work: %v", err)
	}
	if !exists {
		t.Fatal("expected work to exist in the store")
	}
	if got.DeletionTimestamp.IsZero() {
		t.Error("expected the deletion timestamp to be preserved in the store")
	}
}

// TestManifestWorkAgentClient_ConcurrentStatusPatchAndDeleteEvent hammers the client with
// concurrent status patches while a delete event is applied, and verifies the deletion
// timestamp is never lost from the store. Run with -race.
func TestManifestWorkAgentClient_ConcurrentStatusPatchAndDeleteEvent(t *testing.T) {
	for i := 0; i < 50; i++ {
		func() {
			watcherStore := store.NewAgentInformerWatcherStore()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stopWatcher := drainWatcher(t, ctx, watcherStore)
			defer stopWatcher()

			client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, &hookedCloudEventsClient{})
			client.SetNamespace("test-cluster")

			work := newRaceTestWork()
			if err := watcherStore.Add(work.DeepCopy()); err != nil {
				t.Fatalf("unexpected error adding work: %v", err)
			}

			var wg sync.WaitGroup
			stop := make(chan struct{})
			for p := 0; p < 3; p++ {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						patchData := fmt.Sprintf(`{"status":{"conditions":[{"type":"Applied","status":"True","reason":"Reason%d-%d","message":"","lastTransitionTime":null}]}}`, p, n)
						_, _ = client.Patch(ctx, "test-work", kubetypes.MergePatchType, []byte(patchData), metav1.PatchOptions{}, "status")
					}
				}(p)
			}

			// let the patchers spin briefly, then deliver the delete event
			time.Sleep(time.Millisecond)
			now := metav1.Now()
			deletingWork := newRaceTestWork()
			deletingWork.DeletionTimestamp = &now
			if err := watcherStore.HandleReceivedResource(ctx, deletingWork); err != nil {
				t.Fatalf("unexpected error handling delete event: %v", err)
			}

			// let any in-flight patches complete, then stop the patchers
			time.Sleep(2 * time.Millisecond)
			close(stop)
			wg.Wait()

			got, exists, err := watcherStore.Get(ctx, "test-cluster", "test-work")
			if err != nil {
				t.Fatalf("unexpected error getting work: %v", err)
			}
			if !exists {
				t.Fatal("expected work to exist in the store")
			}
			if got.DeletionTimestamp.IsZero() {
				t.Fatalf("iteration %d: the deletion timestamp was lost from the store", i)
			}
		}()
	}
}
