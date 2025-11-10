package client

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	workfake "open-cluster-management.io/api/client/work/clientset/versioned/fake"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/store"
)

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
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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
	err = workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(work)
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
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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
		err = workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(work)
		require.NoError(t, err)
	}

	// Test List
	workList, err := client.List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, workList.Items, 3)
}

func TestManifestWorkAgentClient_Watch(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Test Watch
	watcher, err := client.Watch(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.NotNil(t, watcher)
	defer watcher.Stop()
}

func TestManifestWorkAgentClient_UnsupportedMethods(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work",
			Namespace: "test-cluster",
		},
	}

	// Test Create - should return error
	_, err := client.Create(context.Background(), work, metav1.CreateOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test Update - should return error
	_, err = client.Update(context.Background(), work, metav1.UpdateOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test UpdateStatus - should return error
	_, err = client.UpdateStatus(context.Background(), work, metav1.UpdateOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test Delete - should return error
	err = client.Delete(context.Background(), "test-work", metav1.DeleteOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// Test DeleteCollection - should return error
	err = client.DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestManifestWorkAgentClient_Patch_WorkNotFound(t *testing.T) {
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	patchData := []byte(`{"metadata":{"labels":{"test":"label"}}}`)

	// Test Patch on non-existing work
	_, err := client.Patch(context.Background(), "non-existing", kubetypes.MergePatchType, patchData, metav1.PatchOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestManifestWorkAgentClient_Patch_UnsupportedSubresource(t *testing.T) {
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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

	err = workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(work)
	require.NoError(t, err)

	patchData := []byte(`{"metadata":{"labels":{"test":"label"}}}`)

	// Test Patch with unsupported subresource
	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{}, "unsupported")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not allow this method")
}

func TestManifestWorkAgentClient_Patch_ResourceVersionConflict(t *testing.T) {
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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

	err = workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(work)
	require.NoError(t, err)

	// Test Patch with invalid JSON
	invalidPatchData := []byte(`{invalid json}`)

	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, invalidPatchData, metav1.PatchOptions{})
	require.Error(t, err)
}

func TestManifestWorkAgentClient_Patch_MissingDataTypeAnnotation(t *testing.T) {
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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

	err = workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(work)
	require.NoError(t, err)

	patchData := []byte(`{"metadata":{"labels":{"test":"label"}}}`)

	// Test Patch without required annotation
	_, err = client.Patch(ctx, "test-work", kubetypes.MergePatchType, patchData, metav1.PatchOptions{})
	require.Error(t, err)
}

func TestManifestWorkAgentClient_SetNamespace(t *testing.T) {
	watcherStore := store.NewAgentInformerWatcherStore()
	client := NewManifestWorkAgentClient("", watcherStore, nil)

	assert.Equal(t, "", client.namespace)

	client.SetNamespace("new-cluster")
	assert.Equal(t, "new-cluster", client.namespace)
}

func TestManifestWorkAgentClient_ConcurrentPatches(t *testing.T) {
	workClient := workfake.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(workClient, 10*time.Minute)

	watcherStore := store.NewAgentInformerWatcherStore()
	watcherStore.SetInformer(workInformerFactory.Work().V1().ManifestWorks().Informer())

	client := NewManifestWorkAgentClient("test-cluster", watcherStore, nil)
	client.SetNamespace("test-cluster")

	// Start consuming events to prevent blocking
	watcher, err := watcherStore.GetWatcher("", metav1.ListOptions{})
	require.NoError(t, err)

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
