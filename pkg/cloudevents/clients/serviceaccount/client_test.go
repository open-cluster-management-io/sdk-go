package serviceaccount

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

func TestNewServiceAccountClient(t *testing.T) {
	clusterName := "test-cluster"
	saClient := NewServiceAccountClient(clusterName, nil)

	if saClient == nil {
		t.Fatal("expected non-nil ServiceAccountClient")
	}

	if saClient.clusterName != clusterName {
		t.Errorf("expected clusterName %s, got %s", clusterName, saClient.clusterName)
	}

	// Verify it implements the interface
	var _ corev1client.ServiceAccountInterface = saClient
}

func TestUnsupportedMethods(t *testing.T) {
	saClient := &ServiceAccountClient{
		clusterName: "test-cluster",
	}

	ctx := context.Background()
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sa",
			Namespace: "test-ns",
		},
	}

	tests := []struct {
		name string
		fn   func() error
	}{
		{
			name: "Create",
			fn: func() error {
				_, err := saClient.Create(ctx, sa, metav1.CreateOptions{})
				return err
			},
		},
		{
			name: "Update",
			fn: func() error {
				_, err := saClient.Update(ctx, sa, metav1.UpdateOptions{})
				return err
			},
		},
		{
			name: "Delete",
			fn: func() error {
				return saClient.Delete(ctx, "test-sa", metav1.DeleteOptions{})
			},
		},
		{
			name: "DeleteCollection",
			fn: func() error {
				return saClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			},
		},
		{
			name: "Get",
			fn: func() error {
				_, err := saClient.Get(ctx, "test-sa", metav1.GetOptions{})
				return err
			},
		},
		{
			name: "List",
			fn: func() error {
				_, err := saClient.List(ctx, metav1.ListOptions{})
				return err
			},
		},
		{
			name: "Watch",
			fn: func() error {
				_, err := saClient.Watch(ctx, metav1.ListOptions{})
				return err
			},
		},
		{
			name: "Patch",
			fn: func() error {
				_, err := saClient.Patch(ctx, "test-sa", types.StrategicMergePatchType, []byte("{}"), metav1.PatchOptions{})
				return err
			},
		},
		{
			name: "Apply",
			fn: func() error {
				_, err := saClient.Apply(ctx, nil, metav1.ApplyOptions{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.IsMethodNotSupported(err) {
				t.Errorf("expected MethodNotSupported error, got: %v", err)
			}
		})
	}
}
