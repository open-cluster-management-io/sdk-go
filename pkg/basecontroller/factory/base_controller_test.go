package factory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// mockInformer is a mock implementation of Informer for testing
type mockInformer struct {
	handlers []cache.ResourceEventHandler
	synced   bool
}

func (m *mockInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	m.handlers = append(m.handlers, handler)
	return nil, nil
}

func (m *mockInformer) HasSynced() bool {
	return m.synced
}

// TestBaseControllerRun tests the basic Run functionality of baseController
func TestBaseControllerRun(t *testing.T) {
	tests := []struct {
		name           string
		workers        int
		cacheSynced    bool
		expectSync     bool
		resyncInterval time.Duration
		addToQueue     bool
	}{
		{
			name:        "controller runs with synced caches",
			workers:     1,
			cacheSynced: true,
			expectSync:  true,
			addToQueue:  true,
		},
		{
			name:        "controller runs with multiple workers",
			workers:     3,
			cacheSynced: true,
			expectSync:  true,
			addToQueue:  true,
		},
		{
			name:           "controller with periodic resync",
			workers:        1,
			cacheSynced:    true,
			expectSync:     true,
			resyncInterval: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := klog.NewContext(context.Background(), klog.Background())
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			syncCalled := &sync.WaitGroup{}
			if tt.expectSync {
				syncCalled.Add(1)
			}

			syncFunc := func(ctx context.Context, syncCtx SyncContext, key string) error {
				syncCalled.Done()
				return nil
			}

			mockInf := &mockInformer{synced: tt.cacheSynced}
			controller := &baseController{
				name:             "test-controller",
				sync:             syncFunc,
				syncContext:      NewSyncContext("test"),
				resyncEvery:      tt.resyncInterval,
				cachesToSync:     []cache.InformerSynced{mockInf.HasSynced},
				cacheSyncTimeout: 1 * time.Second,
			}

			// Add item to queue if needed
			if tt.addToQueue {
				controller.syncContext.Queue().Add("test-key")
			}

			// Run controller in background
			go controller.Run(ctx, tt.workers)

			// Wait for sync to be called or timeout
			done := make(chan struct{})
			go func() {
				syncCalled.Wait()
				close(done)
			}()

			select {
			case <-done:
				// Success - sync was called
			case <-time.After(1500 * time.Millisecond):
				if tt.expectSync {
					t.Error("timeout waiting for sync to be called")
				}
			}
		})
	}
}

// TestBaseControllerProcessNextWorkItem tests the processNextWorkItem function
func TestBaseControllerProcessNextWorkItem(t *testing.T) {
	tests := []struct {
		name          string
		queueKey      string
		syncError     error
		expectRequeue bool
	}{
		{
			name:          "successful sync with default queue key",
			queueKey:      DefaultQueueKey,
			syncError:     nil,
			expectRequeue: false,
		},
		{
			name:          "successful sync with custom queue key",
			queueKey:      "custom-key",
			syncError:     nil,
			expectRequeue: false,
		},
		{
			name:          "failed sync with default queue key",
			queueKey:      DefaultQueueKey,
			syncError:     errors.New("sync failed"),
			expectRequeue: true,
		},
		{
			name:          "failed sync with custom queue key",
			queueKey:      "namespace/name",
			syncError:     errors.New("sync failed"),
			expectRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := klog.NewContext(context.Background(), klog.Background())

			syncCalled := false
			syncFunc := func(ctx context.Context, syncCtx SyncContext, key string) error {
				syncCalled = true
				if key != tt.queueKey {
					t.Errorf("expected key %s, got %s", tt.queueKey, key)
				}
				return tt.syncError
			}

			controller := &baseController{
				name:        "test-controller",
				sync:        syncFunc,
				syncContext: NewSyncContext("test"),
			}

			// Add item to queue
			controller.syncContext.Queue().Add(tt.queueKey)

			// Process the item
			controller.processNextWorkItem(ctx)

			if !syncCalled {
				t.Error("sync function was not called")
			}

			// For rate limited items, we need to wait a bit
			if tt.expectRequeue {
				time.Sleep(10 * time.Millisecond)
			}

			// Check if item was requeued on error
			queueLen := controller.syncContext.Queue().Len()
			if tt.expectRequeue && queueLen == 0 {
				t.Error("expected item to be requeued but queue is empty")
			}
			if !tt.expectRequeue && queueLen > 0 {
				t.Errorf("expected queue to be empty but has %d items", queueLen)
			}

			// Cleanup
			controller.syncContext.Queue().ShutDown()
		})
	}
}

// TestBaseControllerDefaultQueueKey tests the error logging behavior with DefaultQueueKey
func TestBaseControllerDefaultQueueKey(t *testing.T) {
	ctx := klog.NewContext(context.Background(), klog.Background())

	syncError := errors.New("test error")
	syncFunc := func(ctx context.Context, syncCtx SyncContext, key string) error {
		return syncError
	}

	controller := &baseController{
		name:        "test-controller",
		sync:        syncFunc,
		syncContext: NewSyncContext("test"),
	}

	// Test with DefaultQueueKey
	controller.syncContext.Queue().Add(DefaultQueueKey)
	controller.processNextWorkItem(ctx)

	// Wait for rate limited item
	time.Sleep(10 * time.Millisecond)

	// Verify item was requeued
	if controller.syncContext.Queue().Len() == 0 {
		t.Error("expected item to be requeued")
	}

	// Test with custom key
	controller.syncContext.Queue().Add("custom/key")
	controller.processNextWorkItem(ctx)

	// Wait for rate limited item
	time.Sleep(10 * time.Millisecond)

	// Verify item was requeued
	if controller.syncContext.Queue().Len() == 0 {
		t.Error("expected item to be requeued")
	}

	// Cleanup
	controller.syncContext.Queue().ShutDown()
}

// TestBaseControllerName tests the Name() method
func TestBaseControllerName(t *testing.T) {
	controller := &baseController{
		name: "test-controller-name",
	}

	if controller.Name() != "test-controller-name" {
		t.Errorf("expected name 'test-controller-name', got '%s'", controller.Name())
	}
}

// TestBaseControllerSyncContext tests the SyncContext() method
func TestBaseControllerSyncContext(t *testing.T) {
	syncCtx := NewSyncContext("test")
	controller := &baseController{
		syncContext: syncCtx,
	}

	if controller.SyncContext() != syncCtx {
		t.Error("SyncContext() returned different context than expected")
	}
}

// TestBaseControllerSync tests the Sync() method
func TestBaseControllerSync(t *testing.T) {
	ctx := klog.NewContext(context.Background(), klog.Background())

	syncCalled := false
	expectedKey := "test-key"
	syncFunc := func(ctx context.Context, syncCtx SyncContext, key string) error {
		syncCalled = true
		if key != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, key)
		}
		return nil
	}

	syncCtx := NewSyncContext("test")
	controller := &baseController{
		sync:        syncFunc,
		syncContext: syncCtx,
	}

	err := controller.Sync(ctx, syncCtx, expectedKey)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !syncCalled {
		t.Error("sync function was not called")
	}
}

// TestBaseControllerRunPeriodicalResync tests periodic resync functionality
func TestBaseControllerRunPeriodicalResync(t *testing.T) {
	ctx := klog.NewContext(context.Background(), klog.Background())
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	syncCtx := NewSyncContext("test")
	controller := &baseController{
		syncContext: syncCtx,
		resyncEvery: 50 * time.Millisecond,
	}

	// Run periodic resync
	go controller.runPeriodicalResync(ctx, controller.resyncEvery)

	// Wait for items to be added to queue
	time.Sleep(400 * time.Millisecond)

	// Check that queue has items (should have at least 1-2 items added due to timing variations)
	queueLen := controller.syncContext.Queue().Len()
	if queueLen < 1 {
		t.Errorf("expected at least 1 item in queue from periodic resync, got %d", queueLen)
	}

	// Verify the items are DefaultQueueKey
	for i := 0; i < queueLen; i++ {
		item, _ := controller.syncContext.Queue().Get()
		if item != DefaultQueueKey {
			t.Errorf("expected DefaultQueueKey, got %v", item)
		}
		controller.syncContext.Queue().Done(item)
	}

	// Cleanup
	controller.syncContext.Queue().ShutDown()
}

// TestDefaultQueueKeysFunc tests the DefaultQueueKeysFunc function
func TestDefaultQueueKeysFunc(t *testing.T) {
	tests := []struct {
		name   string
		obj    runtime.Object
		expect []string
	}{
		{
			name:   "nil object",
			obj:    nil,
			expect: []string{DefaultQueueKey},
		},
		{
			name:   "non-nil object",
			obj:    &runtime.Unknown{},
			expect: []string{DefaultQueueKey},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DefaultQueueKeysFunc(tt.obj)
			if len(result) != len(tt.expect) {
				t.Errorf("expected %d keys, got %d", len(tt.expect), len(result))
			}
			for i, key := range result {
				if key != tt.expect[i] {
					t.Errorf("expected key %s at index %d, got %s", tt.expect[i], i, key)
				}
			}
		})
	}
}
