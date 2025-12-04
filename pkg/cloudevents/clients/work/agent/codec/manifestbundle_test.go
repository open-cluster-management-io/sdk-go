package codec

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestManifestBundleEventDataType(t *testing.T) {
	codec := NewManifestBundleCodec()

	if codec.EventDataType() != payload.ManifestBundleEventDataType {
		t.Errorf("unexpected event data type %s", codec.EventDataType())
	}
}

func TestManifestBundleEncode(t *testing.T) {
	cases := []struct {
		name        string
		eventType   types.CloudEventsType
		work        *workv1.ManifestWork
		expectedErr bool
	}{
		{
			name: "unsupported cloudevents data type",
			eventType: types.CloudEventsType{
				CloudEventsDataType: types.CloudEventsDataType{
					Group:    "test",
					Version:  "v1",
					Resource: "test",
				},
			},
			expectedErr: true,
		},
		{
			name: "bad resourceversion",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					ResourceVersion: "abc",
				},
			},
			expectedErr: true,
		},
		{
			name: "empty resourceversion (should use 0 as default)",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:             "test",
					ResourceVersion: "",
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "no originalsource",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					ResourceVersion: "13",
				},
			},
			expectedErr: true,
		},
		{
			name: "encode a manifestwork status",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:             "test",
					ResourceVersion: "13",
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
					},
				},
				Status: workv1.ManifestWorkStatus{
					Conditions:     []metav1.Condition{},
					ResourceStatus: workv1.ManifestResourceStatus{},
				},
			},
		},
		{
			name: "encode a manifestwork status with metadata",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:             "test",
					ResourceVersion: "13",
					Name:            "test-work",
					Namespace:       "test-namespace",
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
						"test-label": "test-value",
					},
					Annotations: map[string]string{
						"test-annotation": "test-value",
					},
					Finalizers: []string{"test-finalizer"},
				},
				Status: workv1.ManifestWorkStatus{
					Conditions:     []metav1.Condition{},
					ResourceStatus: workv1.ManifestResourceStatus{},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			evt, err := NewManifestBundleCodec().Encode("cluster1-work-agent", c.eventType, c.work)
			if c.expectedErr {
				if err == nil {
					t.Errorf("expected an error, but failed")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			// Verify that ExtensionWorkMeta is set when encoding manifestwork status
			if c.work != nil && evt != nil {
				workMetaExt := evt.Extensions()[types.ExtensionWorkMeta]
				if workMetaExt == nil {
					t.Errorf("expected ExtensionWorkMeta to be set")
				} else {
					// Verify the metadata can be unmarshaled
					var metaObj metav1.ObjectMeta
					if err := json.Unmarshal([]byte(workMetaExt.(string)), &metaObj); err != nil {
						t.Errorf("failed to unmarshal metadata: %v", err)
					}
					if metaObj.UID != c.work.ObjectMeta.UID {
						t.Errorf("expected UID %s, got %s", c.work.ObjectMeta.UID, metaObj.UID)
					}
				}
			}
		})
	}
}

func TestManifestBundleDecode(t *testing.T) {
	cases := []struct {
		name        string
		event       *cloudevents.Event
		expectWork  *workv1.ManifestWork
		expectedErr bool
	}{
		{
			name: "bad cloudevents type",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "unsupported cloudevents data type",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("test-group.v1.test.spec.test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "no resourceid",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "no resourceversion",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "no clustername",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "bad data",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "has deletion time",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				evt.SetExtension("clustername", "cluster1")
				evt.SetExtension("deletiontimestamp", "1985-04-12T23:20:50.52Z")
				return &evt
			}(),
			expectWork: func() *workv1.ManifestWork {
				deletionTime, _ := time.Parse(time.RFC3339, "1985-04-12T23:20:50.52Z")
				return &workv1.ManifestWork{
					ObjectMeta: metav1.ObjectMeta{
						UID:               "test",
						Name:              "test",
						Generation:        13,
						Namespace:         "cluster1",
						DeletionTimestamp: &metav1.Time{Time: deletionTime},
						Annotations: map[string]string{
							"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifestbundles",
						},
						Labels: map[string]string{
							"cloudevents.open-cluster-management.io/originalsource": "source1",
						},
						Finalizers: []string{"cluster.open-cluster-management.io/manifest-work-cleanup"},
					},
				}
			}(),
		},
		{
			name: "decode an invalid cloudevent",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				evt.SetExtension("clustername", "cluster1")
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundle{}); err != nil {
					t.Fatal(err)
				}
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "decode a cloudevent",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				evt.SetExtension("clustername", "cluster1")
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundle{
					Manifests: []workv1.Manifest{
						{
							RawExtension: runtime.RawExtension{
								Raw: toConfigMap(t),
							},
						},
					},
				}); err != nil {
					t.Fatal(err)
				}
				return &evt
			}(),
			expectWork: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:        "test",
					Name:       "test",
					Generation: 13,
					Namespace:  "cluster1",
					Annotations: map[string]string{
						"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifestbundles",
					},
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
					},
				},
				Spec: workv1.ManifestWorkSpec{
					Workload: workv1.ManifestsTemplate{
						Manifests: []workv1.Manifest{
							{
								RawExtension: runtime.RawExtension{
									Raw: toConfigMap(t),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "decode a cloudevent with metadata extension",
			event: func() *cloudevents.Event {
				metaJson, err := json.Marshal(metav1.ObjectMeta{
					UID:             "original-uid",
					ResourceVersion: "5",
					Name:            "original-name",
					Namespace:       "original-namespace",
					Labels:          map[string]string{"original-label": "original-value"},
					Annotations:     map[string]string{"original-annotation": "original-value"},
					Finalizers:      []string{"original-finalizer"},
				})
				if err != nil {
					t.Fatal(err)
				}
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				evt.SetExtension("clustername", "cluster1")
				evt.SetExtension(types.ExtensionWorkMeta, string(metaJson))
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundle{
					Manifests: []workv1.Manifest{
						{
							RawExtension: runtime.RawExtension{
								Raw: toConfigMap(t),
							},
						},
					},
					Executer: &workv1.ManifestWorkExecutor{
						Subject: workv1.ManifestWorkExecutorSubject{
							Type: workv1.ExecutorSubjectTypeServiceAccount,
							ServiceAccount: &workv1.ManifestWorkSubjectServiceAccount{
								Name:      "test-sa",
								Namespace: "test-ns",
							},
						},
					},
				}); err != nil {
					t.Fatal(err)
				}
				return &evt
			}(),
			expectWork: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:        "test",
					Name:       "original-name",
					Generation: 13,
					Namespace:  "cluster1",
					Annotations: map[string]string{
						"cloudevents.open-cluster-management.io/datatype": "io.open-cluster-management.works.v1alpha1.manifestbundles",
						"original-annotation":                             "original-value",
					},
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
						"original-label": "original-value",
					},
					Finalizers: []string{"original-finalizer"},
				},
				Spec: workv1.ManifestWorkSpec{
					Workload: workv1.ManifestsTemplate{
						Manifests: []workv1.Manifest{
							{
								RawExtension: runtime.RawExtension{
									Raw: toConfigMap(t),
								},
							},
						},
					},
					Executor: &workv1.ManifestWorkExecutor{
						Subject: workv1.ManifestWorkExecutorSubject{
							Type: workv1.ExecutorSubjectTypeServiceAccount,
							ServiceAccount: &workv1.ManifestWorkSubjectServiceAccount{
								Name:      "test-sa",
								Namespace: "test-ns",
							},
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			work, err := NewManifestBundleCodec().Decode(c.event)
			if c.expectedErr {
				if err == nil {
					t.Errorf("expected an error, but failed")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			assert.Equal(t, c.expectWork, work, "decoded work is not correct")
		})
	}
}

func toConfigMap(t *testing.T) []byte {
	data, err := json.Marshal(&corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind: "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return data
}

func TestManifestBundleAgentLogTracing(t *testing.T) {
	codec := NewManifestBundleCodec()

	t.Run("encode preserves log tracing annotations to event", func(t *testing.T) {
		work := &workv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				UID:             "test-uid",
				ResourceVersion: "2",
				Namespace:       "cluster1",
				Name:            "test-work",
				Generation:      2,
				Labels: map[string]string{
					"cloudevents.open-cluster-management.io/originalsource": "source1",
				},
				Annotations: map[string]string{
					"logging.open-cluster-management.io/agent-id": "agent-456",
					"logging.open-cluster-management.io/cluster":  "cluster1",
					"non-tracing-annotation":                      "value",
				},
			},
			Status: workv1.ManifestWorkStatus{
				Conditions: []metav1.Condition{
					{
						Type:   "Applied",
						Status: metav1.ConditionTrue,
					},
				},
			},
		}

		eventType := types.CloudEventsType{
			CloudEventsDataType: payload.ManifestBundleEventDataType,
			SubResource:         types.SubResourceStatus,
			Action:              "test_update",
		}

		evt, err := codec.Encode("test-agent", eventType, work)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify that log tracing annotations are transferred to event extensions
		extensions := evt.Extensions()
		if logTracingExt, ok := extensions["logtracing"]; ok {
			logTracingJSON, ok := logTracingExt.(string)
			if !ok {
				t.Errorf("logtracing extension is not a string")
			}

			var tracingMap map[string]string
			if err := json.Unmarshal([]byte(logTracingJSON), &tracingMap); err != nil {
				t.Errorf("failed to unmarshal logtracing: %v", err)
			}

			if tracingMap["logging.open-cluster-management.io/agent-id"] != "agent-456" {
				t.Errorf("expected agent-id in logtracing, got %v", tracingMap)
			}
			if tracingMap["logging.open-cluster-management.io/cluster"] != "cluster1" {
				t.Errorf("expected cluster in logtracing, got %v", tracingMap)
			}
			if _, ok := tracingMap["non-tracing-annotation"]; ok {
				t.Errorf("non-tracing annotation should not be in logtracing")
			}
		} else {
			t.Errorf("expected logtracing extension to be set")
		}
	})

	t.Run("decode transfers log tracing from event to work annotations", func(t *testing.T) {
		evt := cloudevents.NewEvent()
		evt.SetSource("source1")
		evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request")
		evt.SetExtension("resourceid", "test-uid")
		evt.SetExtension("resourceversion", "3")
		evt.SetExtension("clustername", "cluster1")

		// Add log tracing extension
		tracingMap := map[string]string{
			"logging.open-cluster-management.io/request-id": "req-789",
			"logging.open-cluster-management.io/source-id":  "src-123",
		}
		tracingJSON, _ := json.Marshal(tracingMap)
		evt.SetExtension("logtracing", string(tracingJSON))

		if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundle{
			Manifests: []workv1.Manifest{
				{
					RawExtension: runtime.RawExtension{
						Raw: toConfigMap(t),
					},
				},
			},
		}); err != nil {
			t.Fatalf("failed to set event data: %v", err)
		}

		work, err := codec.Decode(&evt)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify that log tracing was transferred to work annotations
		if work.Annotations["logging.open-cluster-management.io/request-id"] != "req-789" {
			t.Errorf("expected request-id annotation, got %v", work.Annotations)
		}
		if work.Annotations["logging.open-cluster-management.io/source-id"] != "src-123" {
			t.Errorf("expected source-id annotation, got %v", work.Annotations)
		}
	})

	t.Run("encode with no tracing annotations", func(t *testing.T) {
		work := &workv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				UID:        "test-uid",
				Namespace:  "cluster1",
				Generation: 1,
				Labels: map[string]string{
					"cloudevents.open-cluster-management.io/originalsource": "source1",
				},
				Annotations: map[string]string{
					"regular-annotation": "regular-value",
				},
			},
			Status: workv1.ManifestWorkStatus{},
		}

		eventType := types.CloudEventsType{
			CloudEventsDataType: payload.ManifestBundleEventDataType,
			SubResource:         types.SubResourceStatus,
			Action:              "test_update",
		}

		evt, err := codec.Encode("test-agent", eventType, work)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify that logtracing extension is either not set or is empty
		if logTracingExt, ok := evt.Extensions()["logtracing"]; ok {
			logTracingJSON, ok := logTracingExt.(string)
			if !ok {
				t.Errorf("logtracing extension is not a string")
			}
			var tracingMap map[string]string
			if err := json.Unmarshal([]byte(logTracingJSON), &tracingMap); err != nil {
				t.Errorf("failed to unmarshal logtracing: %v", err)
			}
			if len(tracingMap) != 0 {
				t.Errorf("expected empty logtracing map, got %v", tracingMap)
			}
		}
	})

	t.Run("decode with no tracing extension", func(t *testing.T) {
		evt := cloudevents.NewEvent()
		evt.SetSource("source1")
		evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request")
		evt.SetExtension("resourceid", "test-uid")
		evt.SetExtension("resourceversion", "1")
		evt.SetExtension("clustername", "cluster1")

		if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundle{
			Manifests: []workv1.Manifest{
				{
					RawExtension: runtime.RawExtension{
						Raw: toConfigMap(t),
					},
				},
			},
		}); err != nil {
			t.Fatalf("failed to set event data: %v", err)
		}

		work, err := codec.Decode(&evt)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify that no tracing annotations are present (except system annotations)
		foundTracingAnnotation := false
		for key := range work.Annotations {
			if key != "cloudevents.open-cluster-management.io/datatype" {
				foundTracingAnnotation = true
				break
			}
		}
		if foundTracingAnnotation {
			t.Errorf("unexpected tracing annotations found: %v", work.Annotations)
		}
	})

	t.Run("round trip encode and decode preserves log tracing", func(t *testing.T) {
		// First create a work via decode (simulating receiving from source)
		evt1 := cloudevents.NewEvent()
		evt1.SetSource("source1")
		evt1.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.spec.create_request")
		evt1.SetExtension("resourceid", "test-uid")
		evt1.SetExtension("resourceversion", "5")
		evt1.SetExtension("clustername", "cluster1")

		tracingMap1 := map[string]string{
			"logging.open-cluster-management.io/original-request-id": "orig-123",
		}
		tracingJSON1, _ := json.Marshal(tracingMap1)
		evt1.SetExtension("logtracing", string(tracingJSON1))

		if err := evt1.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundle{
			Manifests: []workv1.Manifest{
				{
					RawExtension: runtime.RawExtension{
						Raw: toConfigMap(t),
					},
				},
			},
		}); err != nil {
			t.Fatalf("failed to set event data: %v", err)
		}

		work, err := codec.Decode(&evt1)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		// Verify the tracing was transferred
		if work.Annotations["logging.open-cluster-management.io/original-request-id"] != "orig-123" {
			t.Errorf("expected original-request-id annotation, got %v", work.Annotations)
		}

		// Now encode the work back (simulating status update)
		work.Labels = map[string]string{
			"cloudevents.open-cluster-management.io/originalsource": "source1",
		}
		work.Status = workv1.ManifestWorkStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Applied",
					Status: metav1.ConditionTrue,
				},
			},
		}

		eventType := types.CloudEventsType{
			CloudEventsDataType: payload.ManifestBundleEventDataType,
			SubResource:         types.SubResourceStatus,
			Action:              "test_update",
		}

		evt2, err := codec.Encode("test-agent", eventType, work)
		if err != nil {
			t.Fatalf("encode error: %v", err)
		}

		// Verify the tracing was preserved in the encoded event
		if logTracingExt, ok := evt2.Extensions()["logtracing"]; ok {
			var tracingMap2 map[string]string
			if err := json.Unmarshal([]byte(logTracingExt.(string)), &tracingMap2); err != nil {
				t.Errorf("failed to unmarshal logtracing: %v", err)
			}
			if tracingMap2["logging.open-cluster-management.io/original-request-id"] != "orig-123" {
				t.Errorf("expected original-request-id to be preserved, got %v", tracingMap2)
			}
		} else {
			t.Errorf("expected logtracing extension in status update event")
		}
	})
}
