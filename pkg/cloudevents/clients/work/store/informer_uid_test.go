package store

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workv1 "open-cluster-management.io/api/work/v1"
)

// drainEvents consumes watch events so the store never blocks on the watcher channel.
func drainEvents(t *testing.T, s *AgentInformerWatcherStore) func() {
	t.Helper()

	watcher, err := s.GetWatcher(context.Background(), "", metav1.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting watcher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
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

	return func() {
		cancel()
		watcher.Stop()
	}
}

func newTestWork(uid types.UID) *workv1.ManifestWork {
	return &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			UID:       uid,
		},
	}
}

// A late delete of the old work must not remove a same-name successor with a new UID.
func TestAgentInformerWatcherStore_Delete_KeepsSameNameSuccessor(t *testing.T) {
	ctx := context.Background()
	s := NewAgentInformerWatcherStore()
	defer drainEvents(t, s)()

	oldWork := newTestWork("uid-old")
	if err := s.Add(oldWork); err != nil {
		t.Fatalf("unexpected error adding old work: %v", err)
	}

	// the successor (same namespace/name, new UID) arrives before the old work is cleaned up
	if err := s.HandleReceivedResource(ctx, newTestWork("uid-new")); err != nil {
		t.Fatalf("unexpected error handling successor: %v", err)
	}

	// late cleanup of the old work
	if err := s.Delete(oldWork); err != nil {
		t.Fatalf("unexpected error deleting old work: %v", err)
	}

	got, exists, err := s.Get(ctx, "test-cluster", "test-work")
	if err != nil {
		t.Fatalf("unexpected error getting work: %v", err)
	}
	if !exists {
		t.Fatalf("the successor was removed from the store by the delete of the old work")
	}
	if got.UID != "uid-new" {
		t.Errorf("expected the successor uid-new to stay cached, got %q", got.UID)
	}

	// the version counter must not have been reset: add(1), successor add(2), update(3)
	next := got.DeepCopy()
	if err := s.Update(next); err != nil {
		t.Fatalf("unexpected error updating successor: %v", err)
	}
	if next.ResourceVersion != "3" {
		t.Errorf("expected resource version 3, got %s", next.ResourceVersion)
	}
}

// Deleting the work that is actually cached still removes it and resets its version.
func TestAgentInformerWatcherStore_Delete_RemovesMatchingUID(t *testing.T) {
	ctx := context.Background()
	s := NewAgentInformerWatcherStore()
	defer drainEvents(t, s)()

	work := newTestWork("uid-a")
	if err := s.Add(work); err != nil {
		t.Fatalf("unexpected error adding work: %v", err)
	}
	if err := s.Delete(work); err != nil {
		t.Fatalf("unexpected error deleting work: %v", err)
	}

	if _, exists, _ := s.Get(ctx, "test-cluster", "test-work"); exists {
		t.Fatalf("expected the work to be removed from the store")
	}

	readded := newTestWork("uid-b")
	if err := s.Add(readded); err != nil {
		t.Fatalf("unexpected error re-adding work: %v", err)
	}
	if readded.ResourceVersion != "1" {
		t.Errorf("expected resource version 1 after delete and re-add, got %s", readded.ResourceVersion)
	}
}
