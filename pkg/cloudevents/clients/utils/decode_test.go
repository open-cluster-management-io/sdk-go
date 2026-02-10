package utils

import (
	"encoding/json"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestDecodeWithDeletionHandling(t *testing.T) {
	now := time.Now()
	testUID := "test-uid-12345"
	testConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-configmap",
			Namespace: "default",
			UID:       kubetypes.UID(testUID),
		},
		Data: map[string]string{
			"key": "value",
		},
	}

	cases := []struct {
		name        string
		event       func() *cloudevents.Event
		factory     func() *corev1.ConfigMap
		validate    func(t *testing.T, obj *corev1.ConfigMap, err error)
		expectedErr bool
	}{
		{
			name: "deletion event without data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceID, testUID)
				evt.SetExtension(types.ExtensionDeletionTimestamp, now)
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if obj.GetUID() != kubetypes.UID(testUID) {
					t.Errorf("expected UID %s, got %s", testUID, obj.GetUID())
				}
				if obj.GetDeletionTimestamp() == nil {
					t.Errorf("expected DeletionTimestamp to be set")
				}
				if !obj.GetDeletionTimestamp().Time.Equal(now) {
					t.Errorf("expected DeletionTimestamp %v, got %v", now, obj.GetDeletionTimestamp().Time)
				}
			},
			expectedErr: false,
		},
		{
			name: "deletion event with data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionDeletionTimestamp, now)
				data, _ := json.Marshal(testConfigMap)
				if err := evt.SetData(cloudevents.ApplicationJSON, data); err != nil {
					t.Fatal(err)
				}
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if obj.Name != testConfigMap.Name {
					t.Errorf("expected Name %s, got %s", testConfigMap.Name, obj.Name)
				}
				if obj.Namespace != testConfigMap.Namespace {
					t.Errorf("expected Namespace %s, got %s", testConfigMap.Namespace, obj.Namespace)
				}
				if obj.UID != testConfigMap.UID {
					t.Errorf("expected UID %s, got %s", testConfigMap.UID, obj.UID)
				}
				if obj.Data["key"] != "value" {
					t.Errorf("expected Data[key] = 'value', got %s", obj.Data["key"])
				}
			},
			expectedErr: false,
		},
		{
			name: "regular event with data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				data, _ := json.Marshal(testConfigMap)
				if err := evt.SetData(cloudevents.ApplicationJSON, data); err != nil {
					t.Fatal(err)
				}
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if obj.Name != testConfigMap.Name {
					t.Errorf("expected Name %s, got %s", testConfigMap.Name, obj.Name)
				}
				if obj.Namespace != testConfigMap.Namespace {
					t.Errorf("expected Namespace %s, got %s", testConfigMap.Namespace, obj.Namespace)
				}
				if obj.UID != testConfigMap.UID {
					t.Errorf("expected UID %s, got %s", testConfigMap.UID, obj.UID)
				}
			},
			expectedErr: false,
		},
		{
			name: "deletion event without data - missing resource ID",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionDeletionTimestamp, now)
				// Missing ExtensionResourceID
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err == nil {
					t.Errorf("expected error for missing resource ID, got nil")
				}
			},
			expectedErr: true,
		},
		{
			name: "deletion event without data - invalid resource ID type",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceID, 12345) // Invalid type
				evt.SetExtension(types.ExtensionDeletionTimestamp, now)
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err == nil {
					t.Errorf("expected error for invalid resource ID type, got nil")
				}
			},
			expectedErr: true,
		},
		{
			name: "deletion event without data - invalid deletion timestamp type",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceID, testUID)
				evt.SetExtension(types.ExtensionDeletionTimestamp, "invalid-timestamp")
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err == nil {
					t.Errorf("expected error for invalid deletion timestamp type, got nil")
				}
			},
			expectedErr: true,
		},
		{
			name: "deletion event with invalid data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionDeletionTimestamp, now)
				if err := evt.SetData(cloudevents.ApplicationJSON, []byte("invalid json")); err != nil {
					t.Fatal(err)
				}
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err == nil {
					t.Errorf("expected error for invalid data, got nil")
				}
			},
			expectedErr: true,
		},
		{
			name: "regular event with invalid data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				if err := evt.SetData(cloudevents.ApplicationJSON, []byte("invalid json")); err != nil {
					t.Fatal(err)
				}
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				if err == nil {
					t.Errorf("expected error for invalid data, got nil")
				}
			},
			expectedErr: true,
		},
		{
			name: "regular event without data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				return &evt
			},
			factory: func() *corev1.ConfigMap {
				return &corev1.ConfigMap{}
			},
			validate: func(t *testing.T, obj *corev1.ConfigMap, err error) {
				// Empty data should be handled gracefully
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			},
			expectedErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			evt := c.event()
			obj, err := DecodeWithDeletionHandling(evt, c.factory)
			c.validate(t, obj, err)
		})
	}
}

func TestDecodeWithDeletionHandling_DifferentTypes(t *testing.T) {
	now := time.Now()
	testUID := "pod-uid-67890"

	t.Run("decode pod with deletion event", func(t *testing.T) {
		evt := cloudevents.NewEvent()
		evt.SetExtension(types.ExtensionResourceID, testUID)
		evt.SetExtension(types.ExtensionDeletionTimestamp, now)

		obj, err := DecodeWithDeletionHandling(&evt, func() *corev1.Pod {
			return &corev1.Pod{}
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if obj.GetUID() != kubetypes.UID(testUID) {
			t.Errorf("expected UID %s, got %s", testUID, obj.GetUID())
		}
		if obj.GetDeletionTimestamp() == nil {
			t.Errorf("expected DeletionTimestamp to be set")
		}
	})

	t.Run("decode namespace with data", func(t *testing.T) {
		testNamespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-namespace",
				UID:  kubetypes.UID("ns-uid-123"),
			},
		}

		evt := cloudevents.NewEvent()
		data, _ := json.Marshal(testNamespace)
		if err := evt.SetData(cloudevents.ApplicationJSON, data); err != nil {
			t.Fatal(err)
		}

		obj, err := DecodeWithDeletionHandling(&evt, func() *corev1.Namespace {
			return &corev1.Namespace{}
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if obj.Name != testNamespace.Name {
			t.Errorf("expected Name %s, got %s", testNamespace.Name, obj.Name)
		}
		if obj.UID != testNamespace.UID {
			t.Errorf("expected UID %s, got %s", testNamespace.UID, obj.UID)
		}
	})
}
