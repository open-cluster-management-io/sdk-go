package utils

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	workpayload "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

// mockResourceObject implements the generic.ResourceObject interface for testing
type mockResourceObject struct {
	uid               kubetypes.UID
	resourceVersion   string
	generation        int64
	deletionTimestamp *metav1.Time
}

func (m *mockResourceObject) GetUID() kubetypes.UID {
	return m.uid
}

func (m *mockResourceObject) GetResourceVersion() string {
	return m.resourceVersion
}

func (m *mockResourceObject) GetGeneration() int64 {
	return m.generation
}

func (m *mockResourceObject) GetDeletionTimestamp() *metav1.Time {
	return m.deletionTimestamp
}

func TestSetResourceVersion(t *testing.T) {
	tests := []struct {
		name              string
		eventType         types.CloudEventsType
		obj               *mockResourceObject
		expectedExtension any
	}{
		{
			name: "ManifestBundleEventDataType uses generation",
			eventType: types.CloudEventsType{
				CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			},
			obj: &mockResourceObject{
				generation:      5,
				resourceVersion: "12345",
			},
			expectedExtension: int64(5),
		},
		{
			name: "other event types use resource version",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			obj: &mockResourceObject{
				generation:      3,
				resourceVersion: "67890",
			},
			expectedExtension: "67890",
		},
		{
			name: "empty resource version - no extension set",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			obj: &mockResourceObject{
				generation:      3,
				resourceVersion: "",
			},
			expectedExtension: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := cloudevents.NewEvent()
			SetResourceVersion(tt.eventType, &evt, tt.obj)

			ext := evt.Extensions()[types.ExtensionResourceVersion]
			if tt.expectedExtension == nil {
				if ext != nil {
					t.Errorf("expected no extension to be set, but got %v", ext)
				}
				return
			}

			// Handle different types
			switch expected := tt.expectedExtension.(type) {
			case int64:
				// CloudEvents may store integers as int32 or int64
				var actual int64
				switch v := ext.(type) {
				case int32:
					actual = int64(v)
				case int64:
					actual = v
				default:
					t.Errorf("expected extension type int32/int64, got %T with value %v", ext, ext)
					return
				}
				if actual != expected {
					t.Errorf("expected extension value %d, got %d", expected, actual)
				}
			case string:
				actual, ok := ext.(string)
				if !ok {
					t.Errorf("expected extension type string, got %T with value %v", ext, ext)
					return
				}
				if actual != expected {
					t.Errorf("expected extension value %s, got %s", expected, actual)
				}
			default:
				t.Errorf("unexpected expected type %T", expected)
			}
		})
	}
}

func TestGetResourceVersionFromObject(t *testing.T) {
	tests := []struct {
		name        string
		eventType   types.CloudEventsType
		obj         *mockResourceObject
		expected    int64
		expectError bool
	}{
		{
			name: "ManifestBundleEventDataType returns generation",
			eventType: types.CloudEventsType{
				CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			},
			obj: &mockResourceObject{
				generation:      10,
				resourceVersion: "12345",
			},
			expected:    10,
			expectError: false,
		},
		{
			name: "other event types parse resource version",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			obj: &mockResourceObject{
				generation:      5,
				resourceVersion: "67890",
			},
			expected:    67890,
			expectError: false,
		},
		{
			name: "invalid resource version returns error",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			obj: &mockResourceObject{
				generation:      5,
				resourceVersion: "invalid-version",
			},
			expected:    0,
			expectError: true,
		},
		{
			name: "empty resource version returns error",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			obj: &mockResourceObject{
				generation:      5,
				resourceVersion: "",
			},
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetResourceVersionFromObject(tt.eventType, tt.obj)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, result)
				}
			}
		})
	}
}

func TestGetResourceVersionFromEvent(t *testing.T) {
	tests := []struct {
		name        string
		eventType   types.CloudEventsType
		setupEvent  func() cloudevents.Event
		expected    int64
		expectError bool
	}{
		{
			name: "ManifestBundleEventDataType extracts generation as integer",
			eventType: types.CloudEventsType{
				CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			},
			setupEvent: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceVersion, 42)
				return evt
			},
			expected:    42,
			expectError: false,
		},
		{
			name: "other event types extract resource version as string and parse",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			setupEvent: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceVersion, "98765")
				return evt
			},
			expected:    98765,
			expectError: false,
		},
		{
			name: "invalid resource version string returns error",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			setupEvent: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceVersion, "not-a-number")
				return evt
			},
			expected:    0,
			expectError: true,
		},
		{
			name: "missing extension returns error",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test.io",
					Version:  "v1",
					Resource: "testresources",
				},
			},
			setupEvent: func() cloudevents.Event {
				return cloudevents.NewEvent()
			},
			expected:    0,
			expectError: true,
		},
		{
			name: "invalid type for ManifestBundleEventDataType returns error",
			eventType: types.CloudEventsType{
				CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			},
			setupEvent: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetExtension(types.ExtensionResourceVersion, "should-be-int")
				return evt
			},
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := tt.setupEvent()
			result, err := GetResourceVersionFromEvent(tt.eventType, evt)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, result)
				}
			}
		})
	}
}
