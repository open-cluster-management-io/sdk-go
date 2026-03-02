package patcher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	clusterfake "open-cluster-management.io/api/client/cluster/clientset/versioned/fake"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
)

func TestPatcherMetrics(t *testing.T) {
	// Create a new registry for testing
	registry := prometheus.NewRegistry()
	RegisterPatcherMetrics(registry)

	// Reset metrics before test
	ResetPatcherMetrics()

	cases := []struct {
		name              string
		operation         func() error
		expectedOperation string
		expectedStatus    string
		expectMetric      bool
	}{
		{
			name: "successful add finalizer",
			operation: func() error {
				clusterClient := clusterfake.NewSimpleClientset(newManagedClusterWithFinalizer())
				patcher := NewPatcher[
					*clusterv1.ManagedCluster, clusterv1.ManagedClusterSpec, clusterv1.ManagedClusterStatus](
					clusterClient.ClusterV1().ManagedClusters())
				_, err := patcher.AddFinalizer(context.TODO(), newManagedClusterWithFinalizer(), "test-finalizer")
				return err
			},
			expectedOperation: OperationAddFinalizer,
			expectedStatus:    StatusSuccess,
			expectMetric:      true,
		},
		{
			name: "successful remove finalizer",
			operation: func() error {
				clusterClient := clusterfake.NewSimpleClientset(newManagedClusterWithFinalizer("test-finalizer"))
				patcher := NewPatcher[
					*clusterv1.ManagedCluster, clusterv1.ManagedClusterSpec, clusterv1.ManagedClusterStatus](
					clusterClient.ClusterV1().ManagedClusters())
				return patcher.RemoveFinalizer(context.TODO(), newManagedClusterWithFinalizer("test-finalizer"), "test-finalizer")
			},
			expectedOperation: OperationRemoveFinalizer,
			expectedStatus:    StatusSuccess,
			expectMetric:      true,
		},
		{
			name: "successful patch spec",
			operation: func() error {
				obj := newManagedClusterWithTaint(clusterv1.Taint{Key: "key1"})
				clusterClient := clusterfake.NewSimpleClientset(obj)
				patcher := NewPatcher[
					*clusterv1.ManagedCluster, clusterv1.ManagedClusterSpec, clusterv1.ManagedClusterStatus](
					clusterClient.ClusterV1().ManagedClusters())
				newObj := newManagedClusterWithTaint(clusterv1.Taint{Key: "key2"})
				_, err := patcher.PatchSpec(context.TODO(), obj, newObj.Spec, obj.Spec)
				return err
			},
			expectedOperation: OperationPatchSpec,
			expectedStatus:    StatusSuccess,
			expectMetric:      true,
		},
		{
			name: "successful patch status",
			operation: func() error {
				obj := newManagedClusterWithConditions(metav1.Condition{Type: "Type1"})
				clusterClient := clusterfake.NewSimpleClientset(obj)
				patcher := NewPatcher[
					*clusterv1.ManagedCluster, clusterv1.ManagedClusterSpec, clusterv1.ManagedClusterStatus](
					clusterClient.ClusterV1().ManagedClusters())
				newObj := newManagedClusterWithConditions(metav1.Condition{Type: "Type2"})
				_, err := patcher.PatchStatus(context.TODO(), obj, newObj.Status, obj.Status)
				return err
			},
			expectedOperation: OperationPatchStatus,
			expectedStatus:    StatusSuccess,
			expectMetric:      true,
		},
		{
			name: "successful patch label annotations",
			operation: func() error {
				obj := newManagedClusterWithLabelAnnotations(nil, nil)
				clusterClient := clusterfake.NewSimpleClientset(obj)
				patcher := NewPatcher[
					*clusterv1.ManagedCluster, clusterv1.ManagedClusterSpec, clusterv1.ManagedClusterStatus](
					clusterClient.ClusterV1().ManagedClusters())
				newObj := newManagedClusterWithLabelAnnotations(map[string]string{"key": "value"}, nil)
				_, err := patcher.PatchLabelAnnotations(context.TODO(), obj, newObj.ObjectMeta, obj.ObjectMeta)
				return err
			},
			expectedOperation: OperationPatchLabelAnnotation,
			expectedStatus:    StatusSuccess,
			expectMetric:      true,
		},
		{
			name: "no patch needed - add finalizer",
			operation: func() error {
				clusterClient := clusterfake.NewSimpleClientset(newManagedClusterWithFinalizer("test-finalizer"))
				patcher := NewPatcher[
					*clusterv1.ManagedCluster, clusterv1.ManagedClusterSpec, clusterv1.ManagedClusterStatus](
					clusterClient.ClusterV1().ManagedClusters())
				_, err := patcher.AddFinalizer(context.TODO(), newManagedClusterWithFinalizer("test-finalizer"), "test-finalizer")
				return err
			},
			expectedOperation: OperationAddFinalizer,
			expectedStatus:    StatusSuccess,
			expectMetric:      false, // no metric should be recorded
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Reset metrics before each test
			ResetPatcherMetrics()

			// Get initial counter value
			// Use "ManagedCluster.cluster.open-cluster-management.io" as the resource GVK
			testResource := "ManagedCluster.cluster.open-cluster-management.io"
			initialCount := promtest.ToFloat64(PatcherOperationsCounterMetric.WithLabelValues(c.expectedOperation, testResource, c.expectedStatus))

			// Execute operation
			err := c.operation()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Check counter metric
			finalCount := promtest.ToFloat64(PatcherOperationsCounterMetric.WithLabelValues(c.expectedOperation, testResource, c.expectedStatus))
			if c.expectMetric {
				if finalCount != initialCount+1 {
					t.Errorf("expected counter to increase by 1, got %f -> %f", initialCount, finalCount)
				}

				// For histograms, we can't easily check individual label values with testutil
				// But we can verify the metric was recorded by checking that operations were counted
			} else {
				if finalCount != initialCount {
					t.Errorf("expected no metric change, got %f -> %f", initialCount, finalCount)
				}
			}
		})
	}
}

func TestRecordPatcherOperation(t *testing.T) {
	// Create a new registry for testing
	registry := prometheus.NewRegistry()
	RegisterPatcherMetrics(registry)
	ResetPatcherMetrics()

	testResource := "ManagedCluster.cluster.open-cluster-management.io"

	// Record a few operations
	RecordPatcherOperation(OperationAddFinalizer, testResource, nil, 100*time.Millisecond)
	RecordPatcherOperation(OperationAddFinalizer, testResource, fmt.Errorf("some error"), 50*time.Millisecond)
	RecordPatcherOperation(OperationPatchSpec, testResource, nil, 200*time.Millisecond)
	RecordPatcherOperation(OperationPatchStatus, testResource, errors.NewConflict(schema.GroupResource{}, "test", fmt.Errorf("conflict")), 75*time.Millisecond)

	// Verify counter metrics
	successCount := promtest.ToFloat64(PatcherOperationsCounterMetric.WithLabelValues(OperationAddFinalizer, testResource, StatusSuccess))
	if successCount != 1 {
		t.Errorf("expected 1 successful add_finalizer operation, got %f", successCount)
	}

	errorCount := promtest.ToFloat64(PatcherOperationsCounterMetric.WithLabelValues(OperationAddFinalizer, testResource, StatusError))
	if errorCount != 1 {
		t.Errorf("expected 1 failed add_finalizer operation, got %f", errorCount)
	}

	patchSpecCount := promtest.ToFloat64(PatcherOperationsCounterMetric.WithLabelValues(OperationPatchSpec, testResource, StatusSuccess))
	if patchSpecCount != 1 {
		t.Errorf("expected 1 successful patch_spec operation, got %f", patchSpecCount)
	}

	conflictCount := promtest.ToFloat64(PatcherOperationsCounterMetric.WithLabelValues(OperationPatchStatus, testResource, StatusConflict))
	if conflictCount != 1 {
		t.Errorf("expected 1 conflict patch_status operation, got %f", conflictCount)
	}

	// Verify histogram metrics by gathering metrics from the registry
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Find the histogram metric
	var histogramFound bool
	for _, mf := range metricFamilies {
		if mf.GetName() == "patcher_operation_duration_seconds" {
			histogramFound = true
			// Check that we have the expected number of observations
			var totalCount uint64
			for _, m := range mf.GetMetric() {
				if m.GetHistogram() != nil {
					totalCount += m.GetHistogram().GetSampleCount()
				}
			}
			if totalCount != 4 { // 2 add_finalizer + 1 patch_spec + 1 patch_status
				t.Errorf("expected 4 total histogram observations, got %d", totalCount)
			}
			break
		}
	}

	if !histogramFound {
		t.Error("histogram metric not found")
	}
}

func TestMetricsRegistration(t *testing.T) {
	// Create a new registry
	registry := prometheus.NewRegistry()

	// Register metrics
	RegisterPatcherMetrics(registry)

	// Verify metrics are registered by checking we can collect them
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Check that we have at least our patcher metrics
	foundCounter := false
	foundHistogram := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "patcher_operations_total" {
			foundCounter = true
		}
		if mf.GetName() == "patcher_operation_duration_seconds" {
			foundHistogram = true
		}
	}

	if !foundCounter {
		t.Error("counter metric not found in registry")
	}
	if !foundHistogram {
		t.Error("histogram metric not found in registry")
	}
}
