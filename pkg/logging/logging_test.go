package logging

import (
	"context"
	"encoding/json"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

func TestSetLogTracingByObject(t *testing.T) {
	tests := []struct {
		name   string
		object metav1.Object
	}{
		{
			name:   "nil object returns logger unchanged",
			object: nil,
		},
		{
			name: "object with no tracing annotations",
			object: &metav1.ObjectMeta{
				Name: "test",
				Annotations: map[string]string{
					"key1": "value1",
				},
			},
		},
		{
			name: "object with tracing annotations adds values to logger",
			object: &metav1.ObjectMeta{
				Name: "test",
				Annotations: map[string]string{
					"key1":                        "value1",
					LogTracingPrefix + "trace-id": "12345",
					LogTracingPrefix + "user":     "testuser",
				},
			},
		},
		{
			name: "object with no annotations",
			object: &metav1.ObjectMeta{
				Name: "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := klog.Background()
			result := SetLogTracingByObject(logger, tt.object)
			if result.GetSink() == nil {
				t.Errorf("expected logger to be returned")
			}
			// Note: We can't easily verify the logger's internal values,
			// but we verify that it returns a valid logger
		})
	}
}

func TestSetLogTracingByCloudEvent(t *testing.T) {
	tests := []struct {
		name  string
		event *cloudevents.Event
	}{
		{
			name:  "nil event returns logger unchanged",
			event: nil,
		},
		{
			name: "event with no tracing extensions",
			event: func() *cloudevents.Event {
				e := cloudevents.NewEvent()
				e.SetID("test")
				e.SetSource("test")
				e.SetType("test")
				return &e
			}(),
		},
		{
			name: "event with tracing extension adds values to logger",
			event: func() *cloudevents.Event {
				e := cloudevents.NewEvent()
				e.SetID("test")
				e.SetSource("test")
				e.SetType("test")
				tracingMap := map[string]string{
					LogTracingPrefix + "trace-id": "12345",
					LogTracingPrefix + "user":     "testuser",
				}
				tracingJSON, _ := json.Marshal(tracingMap)
				e.SetExtension(ExtensionLogTracing, string(tracingJSON))
				return &e
			}(),
		},
		{
			name: "event with non-tracing extensions",
			event: func() *cloudevents.Event {
				e := cloudevents.NewEvent()
				e.SetID("test")
				e.SetSource("test")
				e.SetType("test")
				e.SetExtension("other-key", "other-value")
				return &e
			}(),
		},
		{
			name: "event with invalid JSON in tracing extension logs error but returns logger",
			event: func() *cloudevents.Event {
				e := cloudevents.NewEvent()
				e.SetID("test")
				e.SetSource("test")
				e.SetType("test")
				e.SetExtension(ExtensionLogTracing, "invalid-json")
				return &e
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := klog.Background()
			result := SetLogTracingByCloudEvent(logger, tt.event)
			if result.GetSink() == nil {
				t.Errorf("expected logger to be returned")
			}
			// Note: We can't easily verify the logger's internal values,
			// but we verify that it always returns a valid logger.
			// Errors are now logged internally rather than returned.
		})
	}
}

func TestSetLogTracingFromContext(t *testing.T) {
	t.Run("adds annotations from context with default keys", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextTracingOPIDKey, "operation-123")

		obj := &metav1.ObjectMeta{
			Name: "test-object",
		}

		SetLogTracingFromContext(ctx, obj)

		annotations := obj.GetAnnotations()
		if annotations[LogTracingPrefix+"op-id"] != "operation-123" {
			t.Errorf("expected op-id annotation to be set, got %v", annotations)
		}
	})

	t.Run("adds annotations with existing annotations", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextTracingOPIDKey, "operation-456")

		obj := &metav1.ObjectMeta{
			Name: "test-object",
			Annotations: map[string]string{
				"existing-key": "existing-value",
			},
		}

		SetLogTracingFromContext(ctx, obj)

		annotations := obj.GetAnnotations()
		if annotations[LogTracingPrefix+"op-id"] != "operation-456" {
			t.Errorf("expected op-id annotation to be set, got %v", annotations)
		}
		if annotations["existing-key"] != "existing-value" {
			t.Errorf("expected existing annotation to be preserved, got %v", annotations)
		}
	})

	t.Run("handles missing context values", func(t *testing.T) {
		ctx := context.Background()

		obj := &metav1.ObjectMeta{
			Name: "test-object",
		}

		SetLogTracingFromContext(ctx, obj)

		annotations := obj.GetAnnotations()
		if len(annotations) != 0 {
			t.Errorf("expected no annotations when context has no values, got %v", annotations)
		}
	})

	t.Run("handles multiple values in default keys", func(t *testing.T) {
		// Temporarily modify DefaultContextTracingKeys for this test
		originalKeys := DefaultContextTracingKeys
		DefaultContextTracingKeys = []ContextTracingKey{"op-id", "request-id", "session-id"}
		defer func() { DefaultContextTracingKeys = originalKeys }()

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextTracingOPIDKey, "op-789")
		ctx = context.WithValue(ctx, ContextTracingKey("request-id"), "req-101")
		ctx = context.WithValue(ctx, ContextTracingKey("session-id"), "sess-202")

		obj := &metav1.ObjectMeta{
			Name: "test-object",
		}

		SetLogTracingFromContext(ctx, obj)

		annotations := obj.GetAnnotations()
		if annotations[LogTracingPrefix+"op-id"] != "op-789" {
			t.Errorf("expected op-id annotation, got %v", annotations)
		}
		if annotations[LogTracingPrefix+"request-id"] != "req-101" {
			t.Errorf("expected request-id annotation, got %v", annotations)
		}
		if annotations[LogTracingPrefix+"session-id"] != "sess-202" {
			t.Errorf("expected session-id annotation, got %v", annotations)
		}
	})

	t.Run("handles partial context values", func(t *testing.T) {
		// Temporarily modify DefaultContextTracingKeys for this test
		originalKeys := DefaultContextTracingKeys
		DefaultContextTracingKeys = []ContextTracingKey{ContextTracingOPIDKey, "missing-key"}
		defer func() { DefaultContextTracingKeys = originalKeys }()

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextTracingOPIDKey, "op-999")
		// "missing-key" is not set in context

		obj := &metav1.ObjectMeta{
			Name: "test-object",
		}

		SetLogTracingFromContext(ctx, obj)

		annotations := obj.GetAnnotations()
		if annotations[LogTracingPrefix+"op-id"] != "op-999" {
			t.Errorf("expected op-id annotation, got %v", annotations)
		}
		if _, ok := annotations["missing-key"]; ok {
			t.Errorf("expected missing-key to not be set, got %v", annotations)
		}
	})
}

func TestLogTracingFromEventToObject(t *testing.T) {
	t.Run("nil event", func(t *testing.T) {
		obj := &metav1.ObjectMeta{Name: "test"}
		err := LogTracingFromEventToObject(nil, obj)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("nil object", func(t *testing.T) {
		e := cloudevents.NewEvent()
		err := LogTracingFromEventToObject(&e, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("event with tracing extensions to object with existing annotations", func(t *testing.T) {
		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		tracingMap := map[string]string{
			LogTracingPrefix + "trace-id": "12345",
		}
		tracingJSON, _ := json.Marshal(tracingMap)
		e.SetExtension(ExtensionLogTracing, string(tracingJSON))

		obj := &metav1.ObjectMeta{
			Name: "test",
			Annotations: map[string]string{
				"existing-key": "existing-value",
			},
		}

		err := LogTracingFromEventToObject(&e, obj)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		annotations := obj.GetAnnotations()
		if annotations[LogTracingPrefix+"trace-id"] != "12345" {
			t.Errorf("expected tracing annotation to be set, got %v", annotations)
		}
		if annotations["existing-key"] != "existing-value" {
			t.Errorf("expected existing annotation to be preserved, got %v", annotations)
		}
	})

	t.Run("event with tracing extensions to object with no annotations", func(t *testing.T) {
		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		tracingMap := map[string]string{
			LogTracingPrefix + "trace-id": "67890",
			LogTracingPrefix + "user":     "testuser",
		}
		tracingJSON, _ := json.Marshal(tracingMap)
		e.SetExtension(ExtensionLogTracing, string(tracingJSON))

		obj := &metav1.ObjectMeta{
			Name: "test",
		}

		err := LogTracingFromEventToObject(&e, obj)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		annotations := obj.GetAnnotations()
		if len(annotations) == 0 {
			t.Errorf("expected annotations to be set")
		}
		if annotations[LogTracingPrefix+"trace-id"] != "67890" {
			t.Errorf("expected trace-id annotation, got %v", annotations)
		}
		if annotations[LogTracingPrefix+"user"] != "testuser" {
			t.Errorf("expected user annotation, got %v", annotations)
		}
	})

	t.Run("event with non-tracing extensions", func(t *testing.T) {
		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		tracingMap := map[string]string{
			"other-key": "other-value",
		}
		tracingJSON, _ := json.Marshal(tracingMap)
		e.SetExtension(ExtensionLogTracing, string(tracingJSON))

		obj := &metav1.ObjectMeta{
			Name: "test",
			Annotations: map[string]string{
				"existing-key": "existing-value",
			},
		}

		err := LogTracingFromEventToObject(&e, obj)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		annotations := obj.GetAnnotations()
		if _, ok := annotations["other-key"]; ok {
			t.Errorf("non-tracing key should not be copied to annotations")
		}
	})

	t.Run("event with no tracing extension", func(t *testing.T) {
		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		obj := &metav1.ObjectMeta{
			Name: "test",
		}

		err := LogTracingFromEventToObject(&e, obj)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		annotations := obj.GetAnnotations()
		if len(annotations) != 0 {
			t.Errorf("expected no annotations to be set when no tracing exists, got %v", annotations)
		}
	})
}

func TestLogTracingFromObjectToEvent(t *testing.T) {
	t.Run("nil object", func(t *testing.T) {
		e := cloudevents.NewEvent()
		err := LogTracingFromObjectToEvent(nil, &e)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("nil event", func(t *testing.T) {
		obj := &metav1.ObjectMeta{Name: "test"}
		err := LogTracingFromObjectToEvent(obj, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("object with tracing annotations", func(t *testing.T) {
		obj := &metav1.ObjectMeta{
			Name: "test",
			Annotations: map[string]string{
				LogTracingPrefix + "trace-id": "12345",
				LogTracingPrefix + "user":     "testuser",
			},
		}

		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		err := LogTracingFromObjectToEvent(obj, &e)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		tracingMap, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("failed to get tracing map: %v", err)
		}

		if tracingMap[LogTracingPrefix+"trace-id"] != "12345" {
			t.Errorf("expected trace-id extension, got %v", tracingMap)
		}
		if tracingMap[LogTracingPrefix+"user"] != "testuser" {
			t.Errorf("expected user extension, got %v", tracingMap)
		}
	})

	t.Run("object with non-tracing annotations", func(t *testing.T) {
		obj := &metav1.ObjectMeta{
			Name: "test",
			Annotations: map[string]string{
				"other-key": "other-value",
			},
		}

		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		err := LogTracingFromObjectToEvent(obj, &e)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		tracingMap, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("failed to get tracing map: %v", err)
		}

		if len(tracingMap) != 0 {
			t.Errorf("expected empty tracing map, got %v", tracingMap)
		}
	})

	t.Run("object with no annotations", func(t *testing.T) {
		obj := &metav1.ObjectMeta{
			Name: "test",
		}

		e := cloudevents.NewEvent()
		e.SetID("test-id")
		e.SetSource("test-source")
		e.SetType("test-type")

		err := LogTracingFromObjectToEvent(obj, &e)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		tracingMap, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("failed to get tracing map: %v", err)
		}

		if len(tracingMap) != 0 {
			t.Errorf("expected empty tracing map, got %v", tracingMap)
		}
	})
}

func TestGetTracingMapFromExtension(t *testing.T) {
	t.Run("event with valid JSON tracing extension", func(t *testing.T) {
		e := cloudevents.NewEvent()
		tracingMap := map[string]string{
			LogTracingPrefix + "key1": "value1",
			LogTracingPrefix + "key2": "value2",
		}
		tracingJSON, _ := json.Marshal(tracingMap)
		e.SetExtension(ExtensionLogTracing, string(tracingJSON))

		result, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(result) != 2 {
			t.Errorf("expected 2 items in tracing map, got %d", len(result))
		}
		if result[LogTracingPrefix+"key1"] != "value1" {
			t.Errorf("expected key1 value, got %v", result)
		}
	})

	t.Run("event with no tracing extension", func(t *testing.T) {
		e := cloudevents.NewEvent()
		result, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty map, got %v", result)
		}
	})

	t.Run("event with invalid JSON tracing extension", func(t *testing.T) {
		e := cloudevents.NewEvent()
		e.SetExtension(ExtensionLogTracing, "not-valid-json")
		_, err := getTracingMapFromExtension(&e)
		if err == nil {
			t.Errorf("expected error for invalid JSON")
		}
	})
}

func TestSetTracingMapToExtension(t *testing.T) {
	t.Run("sets valid tracing map", func(t *testing.T) {
		e := cloudevents.NewEvent()
		tracingMap := map[string]string{
			LogTracingPrefix + "key1": "value1",
			LogTracingPrefix + "key2": "value2",
		}

		err := setTracingMapToExtension(&e, tracingMap)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		result, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("failed to get tracing map: %v", err)
		}
		if len(result) != 2 {
			t.Errorf("expected 2 items, got %d", len(result))
		}
	})

	t.Run("sets empty tracing map", func(t *testing.T) {
		e := cloudevents.NewEvent()
		tracingMap := map[string]string{}

		err := setTracingMapToExtension(&e, tracingMap)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		result, err := getTracingMapFromExtension(&e)
		if err != nil {
			t.Errorf("failed to get tracing map: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected empty map, got %v", result)
		}
	})
}
