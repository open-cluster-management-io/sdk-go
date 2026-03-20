package watcher

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testNamespace     = "test-ns"
	testConfigMapName = "test-cm"

	// noEventWait is how long "no trigger expected" tests wait before declaring success.
	// Kept generous enough to avoid flakiness under loaded CI.
	noEventWait = 500 * time.Millisecond
)

func newCM(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testConfigMapName,
			Namespace: testNamespace,
		},
		Data: data,
	}
}

// --- hashConfigMapData ---

func TestHashConfigMapData(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]string
		wantHash string
		wantSame string // another data map that must produce the same hash (optional)
	}{
		{
			name:     "nil map returns empty string",
			data:     nil,
			wantHash: "",
		},
		{
			name:     "empty map returns empty string",
			data:     map[string]string{},
			wantHash: "",
		},
		{
			name:     "single entry",
			data:     map[string]string{"key": "value"},
			wantHash: `"key"="value"`,
		},
		{
			name:     "multiple entries are sorted by key",
			data:     map[string]string{"b": "2", "a": "1"},
			wantHash: `"a"="1"|"b"="2"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hashConfigMapData(tt.data)
			if got != tt.wantHash {
				t.Errorf("hashConfigMapData() = %q, want %q", got, tt.wantHash)
			}
		})
	}
}

func TestHashConfigMapData_OrderIndependent(t *testing.T) {
	// Inserting keys in different iteration orders must yield the same hash.
	a := hashConfigMapData(map[string]string{"x": "1", "y": "2", "z": "3"})
	b := hashConfigMapData(map[string]string{"z": "3", "x": "1", "y": "2"})
	if a != b {
		t.Errorf("hash not order-independent: %q vs %q", a, b)
	}
}

func TestHashConfigMapData_CollisionResistance(t *testing.T) {
	// A value that contains separators must not collide with a split across two keys.
	h1 := hashConfigMapData(map[string]string{"a": "b|c=d"})
	h2 := hashConfigMapData(map[string]string{"a": "b", "c": "d"})
	if h1 == h2 {
		t.Errorf("hash collision detected: %q == %q", h1, h2)
	}
}

func TestHashConfigMapData_DifferentValues(t *testing.T) {
	h1 := hashConfigMapData(map[string]string{"k": "v1"})
	h2 := hashConfigMapData(map[string]string{"k": "v2"})
	if h1 == h2 {
		t.Errorf("different values produced the same hash")
	}
}

// --- NewConfigMapWatcher ---

func TestNewConfigMapWatcher(t *testing.T) {
	client := fake.NewClientset()
	initData := map[string]string{"minTLSVersion": "VersionTLS12"}
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, initData)

	if w.namespace != testNamespace {
		t.Errorf("namespace = %q, want %q", w.namespace, testNamespace)
	}
	if w.configMapName != testConfigMapName {
		t.Errorf("configMapName = %q, want %q", w.configMapName, testConfigMapName)
	}
	want := hashConfigMapData(initData)
	if w.initialHash != want {
		t.Errorf("initialHash = %q, want %q", w.initialHash, want)
	}
}

func TestNewConfigMapWatcher_NilInitData(t *testing.T) {
	client := fake.NewClientset()
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, nil)
	if w.initialHash != "" {
		t.Errorf("expected empty initialHash for nil initData, got %q", w.initialHash)
	}
}

// --- Start: AddFunc ---

func TestStart_AddFunc_NoInitData_SetsBaseline(t *testing.T) {
	// When there is no initData (hash=""), the first Add should set the baseline
	// and NOT trigger a restart.
	cm := newCM(map[string]string{"k": "v"})
	client := fake.NewClientset(cm)

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, nil)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	select {
	case <-triggered:
		t.Error("onChangeFunc triggered unexpectedly when no initData was provided")
	case <-time.After(noEventWait):
		// expected: no trigger
	}
}

func TestStart_AddFunc_MatchingInitData_NoTrigger(t *testing.T) {
	// When initData matches the CM content, Add should NOT trigger a restart.
	data := map[string]string{"minTLSVersion": "VersionTLS12"}
	cm := newCM(data)
	client := fake.NewClientset(cm)

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, data)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	select {
	case <-triggered:
		t.Error("onChangeFunc triggered unexpectedly when initData matched CM")
	case <-time.After(noEventWait):
		// expected: no trigger
	}
}

func TestStart_AddFunc_DifferingInitData_Triggers(t *testing.T) {
	// When initData differs from the CM content, Add should trigger a restart.
	cmData := map[string]string{"minTLSVersion": "VersionTLS13"}
	initData := map[string]string{"minTLSVersion": "VersionTLS12"}
	cm := newCM(cmData)
	client := fake.NewClientset(cm)

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, initData)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	select {
	case <-triggered:
		// expected
	case <-ctx.Done():
		t.Error("timed out waiting for onChangeFunc to be triggered")
	}
}

// --- Start: UpdateFunc ---

func TestStart_UpdateFunc_SameData_NoTrigger(t *testing.T) {
	data := map[string]string{"k": "v"}
	cm := newCM(data)
	client := fake.NewClientset(cm)

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, data)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Update with identical data.
	_, err := client.CoreV1().ConfigMaps(testNamespace).Update(ctx, newCM(data), metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	select {
	case <-triggered:
		t.Error("onChangeFunc triggered unexpectedly on no-op update")
	case <-time.After(noEventWait):
		// expected
	}
}

func TestStart_UpdateFunc_ChangedData_Triggers(t *testing.T) {
	original := map[string]string{"minTLSVersion": "VersionTLS12"}
	cm := newCM(original)
	client := fake.NewClientset(cm)

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, original)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	updated := newCM(map[string]string{"minTLSVersion": "VersionTLS13"})
	_, err := client.CoreV1().ConfigMaps(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	select {
	case <-triggered:
		// expected
	case <-ctx.Done():
		t.Error("timed out waiting for onChangeFunc to be triggered on update")
	}
}

// --- Start: DeleteFunc ---

func TestStart_DeleteFunc_Triggers(t *testing.T) {
	data := map[string]string{"minTLSVersion": "VersionTLS12"}
	cm := newCM(data)
	client := fake.NewClientset(cm)

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, data)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	err := client.CoreV1().ConfigMaps(testNamespace).Delete(ctx, testConfigMapName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	select {
	case <-triggered:
		// expected
	case <-ctx.Done():
		t.Error("timed out waiting for onChangeFunc to be triggered on delete")
	}
}

// --- Start: name filtering ---

func TestStart_WrongConfigMapName_Ignored(t *testing.T) {
	// A CM with a different name must not trigger the callback.
	otherCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "other-cm", Namespace: testNamespace},
		Data:       map[string]string{"k": "v"},
	}
	client := fake.NewClientset()

	triggered := make(chan struct{}, 1)
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, nil)
	w.SetOnChangeFunc(func() { triggered <- struct{}{} })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	_, err := client.CoreV1().ConfigMaps(testNamespace).Create(ctx, otherCM, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	select {
	case <-triggered:
		t.Error("onChangeFunc triggered for a CM with a different name")
	case <-time.After(noEventWait):
		// expected
	}
}

// --- Start: cancelFunc fallback ---

func TestStart_CancelFuncCalledWhenNoOnChangeFunc(t *testing.T) {
	data := map[string]string{"k": "v"}
	cm := newCM(data)
	client := fake.NewClientset(cm)

	cancelCalled := make(chan struct{}, 1)
	// Use a dedicated context for the watcher so that cancelWrapper cancelling it
	// does not race with the test's own timeout select case.
	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	defer watcherCancel()
	cancelWrapper := func() {
		watcherCancel()
		select {
		case cancelCalled <- struct{}{}:
		default:
		}
	}

	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, cancelWrapper, data)
	// Do NOT call SetOnChangeFunc — cancelFunc should be the fallback.

	if err := w.Start(watcherCtx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	updated := newCM(map[string]string{"k": "new-value"})
	updateCtx, updateCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer updateCancel()
	_, err := client.CoreV1().ConfigMaps(testNamespace).Update(updateCtx, updated, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	select {
	case <-cancelCalled:
		// expected
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for cancelFunc to be called")
	}
}

// --- Start: context cancellation ---

func TestStart_ContextCancelledBeforeSync(t *testing.T) {
	client := fake.NewClientset()
	w := NewConfigMapWatcher(client, testNamespace, testConfigMapName, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := w.Start(ctx)
	if err == nil {
		t.Error("expected error when context is already cancelled, got nil")
	}
}
