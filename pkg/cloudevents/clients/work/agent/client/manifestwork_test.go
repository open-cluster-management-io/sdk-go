package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/common"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/store"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

// mockCloudEventsClient is a mock implementation of CloudEventsClient for testing
type mockCloudEventsClient struct {
	mu             sync.Mutex
	publishedWorks []*workv1.ManifestWork
	publishedTypes []types.CloudEventsType
	publishError   error
	subscribedCh   chan struct{}
}

func (m *mockCloudEventsClient) Resync(ctx context.Context, clusterName string) error {
	return nil
}

func (m *mockCloudEventsClient) Publish(ctx context.Context, eventType types.CloudEventsType, work *workv1.ManifestWork) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.publishError != nil {
		return m.publishError
	}

	m.publishedWorks = append(m.publishedWorks, work.DeepCopy())
	m.publishedTypes = append(m.publishedTypes, eventType)
	return nil
}

func (m *mockCloudEventsClient) Subscribe(ctx context.Context, handlers ...generic.ResourceHandler[*workv1.ManifestWork]) {
	// No-op for testing
}

func (m *mockCloudEventsClient) SubscribedChan() <-chan struct{} {
	if m.subscribedCh == nil {
		m.subscribedCh = make(chan struct{})
		close(m.subscribedCh) // Already subscribed for testing
	}
	return m.subscribedCh
}

func (m *mockCloudEventsClient) getPublishedWorks() []*workv1.ManifestWork {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*workv1.ManifestWork{}, m.publishedWorks...)
}

func TestVersionCompare(t *testing.T) {
	tests := []struct {
		name           string
		newVersion     string
		oldVersion     string
		expectError    bool
		errorSubstring string
	}{
		{
			name:        "new version greater than old version",
			newVersion:  "5",
			oldVersion:  "3",
			expectError: false,
		},
		{
			name:        "new version equal to old version",
			newVersion:  "3",
			oldVersion:  "3",
			expectError: false,
		},
		{
			name:           "new version less than old version",
			newVersion:     "2",
			oldVersion:     "5",
			expectError:    true,
			errorSubstring: "outdated",
		},
		{
			name:           "invalid new version",
			newVersion:     "invalid",
			oldVersion:     "5",
			expectError:    true,
			errorSubstring: "Internal error",
		},
		{
			name:           "invalid old version",
			newVersion:     "5",
			oldVersion:     "invalid",
			expectError:    true,
			errorSubstring: "Internal error",
		},
		{
			name:        "zero new version (force bypass)",
			newVersion:  "0",
			oldVersion:  "5",
			expectError: false,
		},
		{
			name:        "large version numbers",
			newVersion:  "999999",
			oldVersion:  "999998",
			expectError: false,
		},
		{
			name:           "empty new version (conflict error)",
			newVersion:     "",
			oldVersion:     "5",
			expectError:    true,
			errorSubstring: "resource version of the work cannot be empty",
		},
		{
			name:           "empty old version (parse error)",
			newVersion:     "5",
			oldVersion:     "",
			expectError:    true,
			errorSubstring: "Internal error",
		},
		{
			name:           "both versions empty",
			newVersion:     "",
			oldVersion:     "",
			expectError:    true,
			errorSubstring: "resource version of the work cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newWork := &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-work",
					Namespace:       "test-cluster",
					ResourceVersion: tt.newVersion,
				},
			}

			oldWork := &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-work",
					Namespace:       "test-cluster",
					ResourceVersion: tt.oldVersion,
				},
			}

			err := versionCompare(newWork, oldWork)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
				} else {
					assert.Contains(t, err.Error(), tt.errorSubstring)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestManifestWorkAgentClient_Get(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	// Add work to informer store
	err = watcherStore.Store.Add(work)
	require.NoError(t, err)

	// Test Get existing work
	retrievedWork, err := client.Get(ctx, "test-work", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "test-work", retrievedWork.Name)
	assert.Equal(t, "test-cluster", retrievedWork.Namespace)

	// Test Get non-existing work
	_, err = client.Get(ctx, "non-existing", metav1.GetOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestManifestWorkAgentClient_List(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	// Add multiple works
	for i := 0; i < 3; i++ {
		work := &workv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-work-%d", i),
				Namespace: "test-cluster",
			},
		}
		err = watcherStore.Store.Add(work)
		require.NoError(t, err)
	}

	// Test List
	workList, err := client.List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, workList.Items, 3)
}

func TestManifestWorkAgentClient_Watch(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Test Watch
	watcher, err := client.Watch(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.NotNil(t, watcher)
	defer watcher.Stop()
}

func TestManifestWorkAgentClient_UnsupportedMethods(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	// Test Create - should return error
	_, err := client.Create(ctx, work, metav1.CreateOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test Update - should return error
	_, err = client.Update(ctx, work, metav1.UpdateOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test UpdateStatus - should return error
	_, err = client.UpdateStatus(ctx, work, metav1.UpdateOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test Delete - should return error
	err = client.Delete(ctx, "test-work", metav1.DeleteOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test DeleteCollection - should return error
	err = client.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestManifestWorkAgentClient_Patch_WorkNotFound(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	patchData := []byte(`{"metadata":{"labels":{"test":"label"}}}`)

	// Test Patch on non-existing work
	_, err := client.Patch(context.Background(), "non-existing", kubetypes.MergePatchType, patchData, metav1.PatchOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestManifestWorkAgentClient_Patch_UnsupportedSubresource(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Store.Add(work)
	require.NoError(t, err)

	patchData := []byte(`{"metadata":{"labels":{"test":"label"}}}`)

	// Test Patch with unsupported subresource
	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{}, "unsupported")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not allow this method")
}

func TestManifestWorkAgentClient_Patch_ResourceVersionConflict(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Add(work)
	require.NoError(t, err)

	// Update to increment version
	err = watcherStore.Update(work)
	require.NoError(t, err)

	// Try to patch with an outdated resource version
	// Create a patch that sets an older resource version
	patchData, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"resourceVersion": "1", // older version
			"labels": map[string]string{
				"test": "label",
			},
		},
	})
	require.NoError(t, err)

	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{})
	require.Error(t, err)
	// The test uses the watcherStore directly (not the informer store),
	// so the work doesn't actually exist in the informer store backing the client
	assert.Contains(t, err.Error(), "the resource version of the work is outdated")
}

func TestManifestWorkAgentClient_Patch_InvalidPatch(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Store.Add(work)
	require.NoError(t, err)

	// Test Patch with invalid JSON
	invalidPatchData := []byte(`{invalid json}`)

	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, invalidPatchData, metav1.PatchOptions{})
	require.Error(t, err)
}

func TestManifestWorkAgentClient_Patch_MissingDataTypeAnnotation(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			// Missing the required annotation
		},
	}

	err = watcherStore.Store.Add(work)
	require.NoError(t, err)

	patchData := []byte(`{"metadata":{"labels":{"test":"label"}}}`)

	// Test Patch without required annotation
	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{})
	require.Error(t, err)
}

func TestManifestWorkAgentClient_SetNamespace(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "new-cluster", watcherStore, nil)
	assert.Equal(t, "", client.namespace)

	client.SetNamespace("new-cluster")
	assert.Equal(t, "new-cluster", client.namespace)
}

func TestManifestWorkAgentClient_ConcurrentPatches(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Add(work)
	require.NoError(t, err)

	// The Patch method has a mutex lock, so concurrent patches should be serialized
	done := make(chan bool, 5)

	for i := 0; i < 5; i++ {
		go func(index int) {
			patchData, _ := json.Marshal(map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]string{
						"patch": fmt.Sprintf("label-%d", index),
					},
				},
			})

			// Note: This will likely fail for most goroutines due to resource version conflicts
			// but the test ensures the mutex prevents race conditions
			_, _ = client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{})
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 5; i++ {
		<-done
	}

	// If we get here without panics or data races, the mutex is working
	t.Log("Concurrent patches completed successfully (mutex working)")
}

func TestManifestWorkAgentClient_PeriodicDeletionCheck(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	mockClient := &mockCloudEventsClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, mockClient)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	// Create a work with deletion timestamp and no finalizers
	now := metav1.Now()
	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-work-to-delete",
			Namespace:         "test-cluster",
			DeletionTimestamp: &now,
			Finalizers:        []string{}, // No finalizers
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Add(work)
	require.NoError(t, err)

	// Wait for at least one deletion check cycle to complete
	// The check runs every 2 seconds, so we wait a bit longer
	time.Sleep(workDeletionCheckInterval + 1*time.Second)

	// Verify that the work was published with deletion status
	publishedWorks := mockClient.getPublishedWorks()
	require.Greater(t, len(publishedWorks), 0, "Expected at least one work to be published")

	// Find the published work with our name
	var foundWork *workv1.ManifestWork
	for _, pw := range publishedWorks {
		if pw.Name == "test-work-to-delete" {
			foundWork = pw
			break
		}
	}
	require.NotNil(t, foundWork, "Expected to find the deleted work in published works")

	// Verify the deletion condition was set
	var deletedCondition *metav1.Condition
	for i := range foundWork.Status.Conditions {
		if foundWork.Status.Conditions[i].Type == common.ResourceDeleted {
			deletedCondition = &foundWork.Status.Conditions[i]
			break
		}
	}
	require.NotNil(t, deletedCondition, "Expected Deleted condition to be set")
	assert.Equal(t, metav1.ConditionTrue, deletedCondition.Status)
	assert.Equal(t, "ManifestsDeleted", deletedCondition.Reason)

	// Verify the work was removed from the store
	_, exists, err := watcherStore.Get(ctx, "test-cluster", "test-work-to-delete")
	require.NoError(t, err)
	assert.False(t, exists, "Expected work to be deleted from store")
}

func TestManifestWorkAgentClient_PeriodicDeletionCheck_WorkWithFinalizers(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	mockClient := &mockCloudEventsClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, mockClient)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	// Create a work with deletion timestamp but WITH finalizers
	now := metav1.Now()
	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-work-with-finalizers",
			Namespace:         "test-cluster",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test-finalizer"}, // Has finalizers
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Add(work)
	require.NoError(t, err)

	// Wait for at least one deletion check cycle
	time.Sleep(workDeletionCheckInterval + 1*time.Second)

	// Verify that the work was NOT published (because it has finalizers)
	publishedWorks := mockClient.getPublishedWorks()
	for _, pw := range publishedWorks {
		assert.NotEqual(t, "test-work-with-finalizers", pw.Name, "Work with finalizers should not be deleted")
	}

	// Verify the work is still in the store
	_, exists, err := watcherStore.Get(ctx, "test-cluster", "test-work-with-finalizers")
	require.NoError(t, err)
	assert.True(t, exists, "Work with finalizers should still exist in store")
}

func TestManifestWorkAgentClient_PeriodicDeletionCheck_WorkWithoutDeletionTimestamp(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	mockClient := &mockCloudEventsClient{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, mockClient)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher(ctx, "", metav1.ListOptions{})
	require.NoError(t, err)
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

	// Create a work WITHOUT deletion timestamp
	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work-normal",
			Namespace: "test-cluster",
			Annotations: map[string]string{
				"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifests",
			},
		},
	}

	err = watcherStore.Add(work)
	require.NoError(t, err)

	// Wait for at least one deletion check cycle
	time.Sleep(workDeletionCheckInterval + 1*time.Second)

	// Verify that the work was NOT published (because it has no deletion timestamp)
	publishedWorks := mockClient.getPublishedWorks()
	for _, pw := range publishedWorks {
		assert.NotEqual(t, "test-work-normal", pw.Name, "Normal work should not be deleted")
	}

	// Verify the work is still in the store
	_, exists, err := watcherStore.Get(ctx, "test-cluster", "test-work-normal")
	require.NoError(t, err)
	assert.True(t, exists, "Normal work should still exist in store")
}

func TestManifestWorkAgentClient_PeriodicDeletionCheck_ContextCancellation(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	mockClient := &mockCloudEventsClient{}

	ctx, cancel := context.WithCancel(context.Background())

	_ = NewManifestWorkAgentClient(ctx, "test-cluster", watcherStore, mockClient)

	// Cancel the context immediately
	cancel()

	// Wait a bit to ensure the goroutine exits
	time.Sleep(100 * time.Millisecond)

	// The test succeeds if no goroutines panic or hang
	t.Log("Context cancellation handled successfully")
}
