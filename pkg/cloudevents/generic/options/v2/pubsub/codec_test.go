package pubsub

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func TestEncode(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name          string
		event         cloudevents.Event
		expectedData  []byte
		validateAttrs func(t *testing.T, attrs map[string]string)
		expectedErr   bool
	}{
		{
			name: "basic event with required attributes",
			event: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetID("7a756974-a661-41b8-88d6-dc29b458a970")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request")
				evt.SetSource("test-source")
				err := evt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"})
				if err != nil {
					t.Fatalf("failed to set data for event: %v", err)
				}
				return evt
			}(),
			expectedData: []byte(`{"key":"value"}`),
			validateAttrs: func(t *testing.T, attrs map[string]string) {
				if attrs["ce-id"] != "7a756974-a661-41b8-88d6-dc29b458a970" {
					t.Errorf("expected ce-id to be '7a756974-a661-41b8-88d6-dc29b458a970', got %q", attrs["ce-id"])
				}
				if attrs["ce-type"] != "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request" {
					t.Errorf("expected ce-type to be 'io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request', got %q", attrs["ce-type"])
				}
				if attrs["ce-source"] != "test-source" {
					t.Errorf("expected ce-source to be 'test-source', got %q", attrs["ce-source"])
				}
				if attrs["ce-specversion"] != "1.0" {
					t.Errorf("expected ce-specversion to be '1.0', got %q", attrs["ce-specversion"])
				}
				if attrs["Content-Type"] != "application/json" {
					t.Errorf("expected Content-Type to be 'application/json', got %q", attrs["Content-Type"])
				}
			},
			expectedErr: false,
		},
		{
			name: "event with optional attributes",
			event: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetID("577e5aec-a860-422e-ba75-b3e993f52913")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.update_request")
				evt.SetSource("test-agent")
				evt.SetSubject("test-subject")
				evt.SetTime(now)
				evt.SetDataSchema("http://example.com/schema")
				err := evt.SetData(cloudevents.ApplicationJSON, map[string]int{"count": 42})
				if err != nil {
					t.Fatalf("failed to set data for event: %v", err)
				}
				return evt
			}(),
			expectedData: []byte(`{"count":42}`),
			validateAttrs: func(t *testing.T, attrs map[string]string) {
				if attrs["ce-subject"] != "test-subject" {
					t.Errorf("expected ce-subject to be 'test-subject', got %q", attrs["ce-subject"])
				}
				if attrs["ce-dataschema"] != "http://example.com/schema" {
					t.Errorf("expected ce-dataschema to be 'http://example.com/schema', got %q", attrs["ce-dataschema"])
				}
				if _, ok := attrs["ce-time"]; !ok {
					t.Error("expected ce-time to be present")
				}
			},
			expectedErr: false,
		},
		{
			name: "event with extensions",
			event: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetID("f4193172-10bd-46a8-8821-9b421f684cd5")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.update_request")
				evt.SetSource("test-source")
				evt.SetExtension("clustername", "cluster1")
				evt.SetExtension("resourceid", "c15786fe-daeb-568d-9a66-3edba2305c7a")
				evt.SetExtension("resourceversion", 2)
				err := evt.SetData(cloudevents.ApplicationJSON, "test data")
				if err != nil {
					t.Fatalf("failed to set data for event: %v", err)
				}
				return evt
			}(),
			expectedData: []byte(`"test data"`),
			validateAttrs: func(t *testing.T, attrs map[string]string) {
				if attrs["ce-clustername"] != "cluster1" {
					t.Errorf("expected ce-clustername to be 'cluster1', got %q", attrs["ce-clustername"])
				}
				if attrs["ce-resourceid"] != "c15786fe-daeb-568d-9a66-3edba2305c7a" {
					t.Errorf("expected ce-resourceid to be 'c15786fe-daeb-568d-9a66-3edba2305c7a', got %q", attrs["ce-resourceid"])
				}
				if attrs["ce-resourceversion"] != "2" {
					t.Errorf("expected ce-resourceversion to be '2', got %q", attrs["ce-resourceversion"])
				}
			},
			expectedErr: false,
		},
		{
			name: "event without data",
			event: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetID("318451b6-b20c-4bc5-b1e4-7ec36ecb11f5")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.delete_request")
				evt.SetSource("test-source")
				evt.SetExtension("deletiontimestamp", now)
				return evt
			}(),
			expectedData: nil,
			validateAttrs: func(t *testing.T, attrs map[string]string) {
				if attrs["ce-id"] != "318451b6-b20c-4bc5-b1e4-7ec36ecb11f5" {
					t.Errorf("expected ce-id to be '318451b6-b20c-4bc5-b1e4-7ec36ecb11f5', got %q", attrs["ce-id"])
				}
				if _, ok := attrs["ce-deletiontimestamp"]; !ok {
					t.Error("expected ce-deletiontimestamp to be present")
				}
			},
			expectedErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg, err := Encode(c.event)
			if c.expectedErr {
				if err == nil {
					t.Errorf("expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if msg == nil {
				t.Fatal("expected message, got nil")
			}

			if string(msg.Data) != string(c.expectedData) {
				t.Errorf("expected data %q, got %q", string(c.expectedData), string(msg.Data))
			}

			if c.validateAttrs != nil {
				c.validateAttrs(t, msg.Attributes)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		name          string
		message       *pubsub.Message
		validateEvent func(t *testing.T, evt cloudevents.Event)
		expectedErr   bool
		errorContains string
	}{
		{
			name: "valid message with required attributes",
			message: &pubsub.Message{
				Data: []byte(`{"key":"value"}`),
				Attributes: map[string]string{
					"ce-specversion": "1.0",
					"ce-id":          "25fcf378-2bae-49b8-95ab-4f43b7d11554",
					"ce-type":        "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request",
					"ce-source":      "test-source",
					"Content-Type":   "application/json",
				},
			},
			validateEvent: func(t *testing.T, evt cloudevents.Event) {
				if evt.ID() != "25fcf378-2bae-49b8-95ab-4f43b7d11554" {
					t.Errorf("expected ID to be '25fcf378-2bae-49b8-95ab-4f43b7d11554', got %q", evt.ID())
				}
				if evt.Type() != "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request" {
					t.Errorf("expected Type to be 'io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request', got %q", evt.Type())
				}
				if evt.Source() != "test-source" {
					t.Errorf("expected Source to be 'test-source', got %q", evt.Source())
				}
				if evt.DataContentType() != "application/json" {
					t.Errorf("expected DataContentType to be 'application/json', got %q", evt.DataContentType())
				}
			},
			expectedErr: false,
		},
		{
			name: "valid message with optional attributes",
			message: &pubsub.Message{
				Data: []byte(`{"count":42}`),
				Attributes: map[string]string{
					"ce-specversion": "1.0",
					"ce-id":          "ca6d48ab-387d-487a-aa53-b0a3a65cfe50",
					"ce-type":        "io.open-cluster-management.works.v1alpha1.manifestbundles.status.update_request",
					"ce-source":      "test-agent",
					"ce-subject":     "test-subject",
					"ce-dataschema":  "http://example.com/schema",
					"ce-time":        "2023-01-01T00:00:00Z",
					"Content-Type":   "application/json",
				},
			},
			validateEvent: func(t *testing.T, evt cloudevents.Event) {
				if evt.Subject() != "test-subject" {
					t.Errorf("expected Subject to be 'test-subject', got %q", evt.Subject())
				}
				if evt.DataSchema() != "http://example.com/schema" {
					t.Errorf("expected DataSchema to be 'http://example.com/schema', got %q", evt.DataSchema())
				}
				if evt.Time().IsZero() {
					t.Error("expected Time to be set")
				}
			},
			expectedErr: false,
		},
		{
			name: "valid message with extensions",
			message: &pubsub.Message{
				Data: []byte(`"test data"`),
				Attributes: map[string]string{
					"ce-specversion":     "1.0",
					"ce-id":              "f4193172-10bd-46a8-8821-9b421f684cd5",
					"ce-type":            "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.update_request",
					"ce-source":          "test-source",
					"ce-clustername":     "cluster1",
					"ce-resourceversion": "1",
					"Content-Type":       "application/json",
				},
			},
			validateEvent: func(t *testing.T, evt cloudevents.Event) {
				if val, ok := evt.Extensions()["clustername"]; !ok || val != "cluster1" {
					t.Errorf("expected clustername to be 'cluster1', got %v", val)
				}
				if val, ok := evt.Extensions()["resourceversion"]; !ok || val != "1" {
					t.Errorf("expected resourceversion to be '1', got %v", val)
				}
			},
			expectedErr: false,
		},
		{
			name: "message without specversion defaults to V1",
			message: &pubsub.Message{
				Data: []byte(`{}`),
				Attributes: map[string]string{
					"ce-id":     "318451b6-b20c-4bc5-b1e4-7ec36ecb11f5",
					"ce-type":   "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.delete_request",
					"ce-source": "test-source",
				},
			},
			validateEvent: func(t *testing.T, evt cloudevents.Event) {
				if evt.SpecVersion() != "1.0" {
					t.Errorf("expected SpecVersion to default to '1.0', got %q", evt.SpecVersion())
				}
			},
			expectedErr: false,
		},
		{
			name:          "nil message",
			message:       nil,
			expectedErr:   true,
			errorContains: "cannot decode nil",
		},
		{
			name: "missing required attribute - id",
			message: &pubsub.Message{
				Data: []byte(`{}`),
				Attributes: map[string]string{
					"ce-specversion": "1.0",
					"ce-type":        "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.update_request",
					"ce-source":      "test-source",
				},
			},
			expectedErr:   true,
			errorContains: "missing required attribute",
		},
		{
			name: "missing required attribute - type",
			message: &pubsub.Message{
				Data: []byte(`{}`),
				Attributes: map[string]string{
					"ce-specversion": "1.0",
					"ce-id":          "6c17cf0a-6aa3-4361-854f-09ced7d7e7bd",
					"ce-source":      "test-source",
				},
			},
			expectedErr:   true,
			errorContains: "missing required attribute",
		},
		{
			name: "missing required attribute - source",
			message: &pubsub.Message{
				Data: []byte(`{}`),
				Attributes: map[string]string{
					"ce-specversion": "1.0",
					"ce-id":          "6c17cf0a-6aa3-4361-854f-09ced7d7e7bd",
					"ce-type":        "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.update_request",
				},
			},
			expectedErr:   true,
			errorContains: "missing required attribute",
		},
		{
			name: "non-ce prefixed attributes are ignored",
			message: &pubsub.Message{
				Data: []byte(`{}`),
				Attributes: map[string]string{
					"ce-specversion": "1.0",
					"ce-id":          "6c17cf0a-6aa3-4361-854f-09ced7d7e7bd",
					"ce-type":        "io.open-cluster-management.works.v1alpha1.manifestbundles.spec.update_request",
					"ce-source":      "test-source",
					"random-attr":    "should-be-ignored",
				},
			},
			validateEvent: func(t *testing.T, evt cloudevents.Event) {
				if _, ok := evt.Extensions()["random-attr"]; ok {
					t.Error("expected non-ce-prefixed attribute to be ignored")
				}
			},
			expectedErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			evt, err := Decode(c.message)
			if c.expectedErr {
				if err == nil {
					t.Errorf("expected error, but got none")
				} else if c.errorContains != "" && !strings.Contains(err.Error(), c.errorContains) {
					t.Errorf("expected error to contain %q, got %q", c.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if c.validateEvent != nil {
				c.validateEvent(t, evt)
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	// Create a comprehensive event
	originalEvent := cloudevents.NewEvent()
	originalEvent.SetID("db6d6e20-a7ab-47d4-b035-9bd15250cc51")
	originalEvent.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.update_request")
	originalEvent.SetSource("test-source")
	originalEvent.SetSubject("roundtrip-subject")
	originalEvent.SetDataSchema("http://example.com/roundtrip-schema")
	originalEvent.SetTime(time.Now().Truncate(time.Second)) // Truncate to avoid precision issues
	originalEvent.SetExtension("clustername", "cluster1")
	originalEvent.SetExtension("resourceversion", 1)
	err := originalEvent.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"message": "roundtrip test",
		"count":   99,
	})
	if err != nil {
		t.Fatalf("failed to set data for event: %v", err)
	}

	// Encode
	msg, err := Encode(originalEvent)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Decode
	decodedEvent, err := Decode(msg)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	// Verify all attributes match
	if decodedEvent.ID() != originalEvent.ID() {
		t.Errorf("ID mismatch: expected %q, got %q", originalEvent.ID(), decodedEvent.ID())
	}
	if decodedEvent.Type() != originalEvent.Type() {
		t.Errorf("Type mismatch: expected %q, got %q", originalEvent.Type(), decodedEvent.Type())
	}
	if decodedEvent.Source() != originalEvent.Source() {
		t.Errorf("Source mismatch: expected %q, got %q", originalEvent.Source(), decodedEvent.Source())
	}
	if decodedEvent.Subject() != originalEvent.Subject() {
		t.Errorf("Subject mismatch: expected %q, got %q", originalEvent.Subject(), decodedEvent.Subject())
	}
	if decodedEvent.DataSchema() != originalEvent.DataSchema() {
		t.Errorf("DataSchema mismatch: expected %q, got %q", originalEvent.DataSchema(), decodedEvent.DataSchema())
	}
	if !decodedEvent.Time().Equal(originalEvent.Time()) {
		t.Errorf("Time mismatch: expected %v, got %v", originalEvent.Time(), decodedEvent.Time())
	}
	if val, ok := decodedEvent.Extensions()["clustername"]; !ok || val != "cluster1" {
		t.Errorf("Extension mismatch: expected 'cluster1', got %v", val)
	}
	if val, ok := decodedEvent.Extensions()["resourceversion"]; !ok || val != "1" {
		t.Errorf("Extension mismatch: expected '1', got %v", val)
	}
}
