package store

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	workv1 "open-cluster-management.io/api/work/v1"
)

// newConditionalUpdateTestStore returns an agent store with a goroutine draining the
// watch events, so the store writers do not block on the watch channel.
func newConditionalUpdateTestStore(t *testing.T) (*AgentInformerWatcherStore, context.Context, func()) {
	store := NewAgentInformerWatcherStore()

	watcher, err := store.GetWatcher(context.Background(), "", metav1.ListOptions{})
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
				// consume events
			}
		}
	}()

	return store, ctx, func() {
		cancel()
		watcher.Stop()
	}
}

func newConditionalUpdateTestWork() *workv1.ManifestWork {
	return &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			UID:       kubetypes.UID("test-uid"),
		},
	}
}

func TestAgentInformerWatcherStore_UpdateWithVersion(t *testing.T) {
	t.Run("matching version updates and increments", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error adding work: %v", err)
		}

		updated := newConditionalUpdateTestWork()
		if err := store.UpdateWithVersion(ctx, updated, "1"); err != nil {
			t.Fatalf("unexpected error updating work: %v", err)
		}

		if updated.ResourceVersion != "2" {
			t.Errorf("expected resource version 2 after update, got %s", updated.ResourceVersion)
		}
	})

	t.Run("stale version returns conflict", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error adding work: %v", err)
		}
		if err := store.Update(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error updating work: %v", err)
		}

		// the store is now at version 2, an update derived from version 1 must be rejected
		err := store.UpdateWithVersion(ctx, newConditionalUpdateTestWork(), "1")
		if !errors.IsConflict(err) {
			t.Errorf("expected a conflict error, got %v", err)
		}
	})

	t.Run("version 0 forces the update", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error adding work: %v", err)
		}
		if err := store.Update(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error updating work: %v", err)
		}

		if err := store.UpdateWithVersion(ctx, newConditionalUpdateTestWork(), "0"); err != nil {
			t.Errorf("expected forced update to succeed, got %v", err)
		}
	})

	t.Run("empty expected version returns conflict", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error adding work: %v", err)
		}

		err := store.UpdateWithVersion(ctx, newConditionalUpdateTestWork(), "")
		if !errors.IsConflict(err) {
			t.Errorf("expected a conflict error, got %v", err)
		}
	})

	t.Run("missing work returns not found", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		err := store.UpdateWithVersion(ctx, newConditionalUpdateTestWork(), "1")
		if !errors.IsNotFound(err) {
			t.Errorf("expected a not found error, got %v", err)
		}
	})

	t.Run("versioner reset after delete is detected", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error adding work: %v", err)
		}
		if err := store.Update(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error updating work: %v", err)
		}
		if err := store.Delete(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error deleting work: %v", err)
		}
		// re-adding restarts the versioner at 1, an in-flight update derived from the
		// deleted work's version 2 must not overwrite the recreated work
		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error re-adding work: %v", err)
		}

		err := store.UpdateWithVersion(ctx, newConditionalUpdateTestWork(), "2")
		if !errors.IsConflict(err) {
			t.Errorf("expected a conflict error, got %v", err)
		}
	})

	t.Run("received delete event wins over stale update", func(t *testing.T) {
		store, ctx, cleanup := newConditionalUpdateTestStore(t)
		defer cleanup()

		if err := store.Add(newConditionalUpdateTestWork()); err != nil {
			t.Fatalf("unexpected error adding work: %v", err)
		}

		// a delete event from the source sets the deletion timestamp at version 2
		now := metav1.Now()
		deletingWork := newConditionalUpdateTestWork()
		deletingWork.DeletionTimestamp = &now
		if err := store.HandleReceivedResource(ctx, deletingWork); err != nil {
			t.Fatalf("unexpected error handling delete event: %v", err)
		}

		// a stale update derived from version 1 (without the deletion timestamp) must be rejected
		err := store.UpdateWithVersion(ctx, newConditionalUpdateTestWork(), "1")
		if !errors.IsConflict(err) {
			t.Errorf("expected a conflict error, got %v", err)
		}

		work, exists, err := store.Get(ctx, "test-cluster", "test-work")
		if err != nil {
			t.Fatalf("unexpected error getting work: %v", err)
		}
		if !exists {
			t.Fatal("expected work to exist in the store")
		}
		if work.DeletionTimestamp.IsZero() {
			t.Error("expected the deletion timestamp to be preserved in the store")
		}
	})
}
