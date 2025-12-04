package codec

import (
	"encoding/json"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubetypes "k8s.io/apimachinery/pkg/types"

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
			name: "encode a manifestwork",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:             "test",
					ResourceVersion: "0",
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
					},
				},
				Spec: workv1.ManifestWorkSpec{},
			},
		},
		{
			name: "encode a manifestwork with executor",
			eventType: types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              "test",
			},
			work: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:             "test",
					ResourceVersion: "0",
					Name:            "test-work",
					Namespace:       "test-namespace",
					Labels: map[string]string{
						"cloudevents.open-cluster-management.io/originalsource": "source1",
						"test-label": "test-value",
					},
					Annotations: map[string]string{
						"test-annotation": "test-value",
					},
				},
				Spec: workv1.ManifestWorkSpec{
					Executor: &workv1.ManifestWorkExecutor{
						Subject: workv1.ManifestWorkExecutorSubject{
							Type: workv1.ExecutorSubjectTypeServiceAccount,
							ServiceAccount: &workv1.ManifestWorkSubjectServiceAccount{
								Name:      "test-executor-sa",
								Namespace: "test-executor-ns",
							},
						},
					},
					DeleteOption: &workv1.DeleteOption{
						PropagationPolicy: workv1.DeletePropagationPolicyTypeForeground,
					},
					ManifestConfigs: []workv1.ManifestConfigOption{
						{
							ResourceIdentifier: workv1.ResourceIdentifier{Name: "test-config"},
						},
					},
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

			// Verify encoding includes Executor field when present
			if c.work != nil && c.work.Spec.Executor != nil && evt != nil {
				var manifestBundle payload.ManifestBundle
				if err := evt.DataAs(&manifestBundle); err != nil {
					t.Errorf("failed to unmarshal event data: %v", err)
				} else {
					if manifestBundle.Executer == nil {
						t.Errorf("expected Executer to be set in encoded data")
					} else if manifestBundle.Executer.Subject.ServiceAccount.Name != c.work.Spec.Executor.Subject.ServiceAccount.Name {
						t.Errorf("expected Executer ServiceAccount name %s, got %s",
							c.work.Spec.Executor.Subject.ServiceAccount.Name,
							manifestBundle.Executer.Subject.ServiceAccount.Name)
					}
				}
			}
		})
	}
}

func TestManifestBundleDecode(t *testing.T) {
	cases := []struct {
		name         string
		event        *cloudevents.Event
		expectedWork *workv1.ManifestWork
		expectedErr  bool
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
				evt.SetType("test-group.v1.test.status.test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "no resourceid",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "no resourceversion",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.test")
				evt.SetExtension("resourceid", "test")
				return &evt
			}(),
			expectedErr: true,
		},
		{
			name: "decode a manifestbundle status cloudevent",
			event: func() *cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				evt.SetExtension("sequenceid", "1834773391719010304")
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundleStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Test",
							Status: metav1.ConditionTrue,
						},
					},
				}); err != nil {
					t.Fatal(err)
				}
				return &evt
			}(),
			expectedWork: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:        kubetypes.UID("test"),
					Generation: 13,
					Annotations: map[string]string{
						"cloudevents.open-cluster-management.io/sequenceid": "1834773391719010304",
					},
					Name: "test",
				},
				Status: workv1.ManifestWorkStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Test",
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
		},
		{
			name: "decode a manifestbundle status cloudevent with meta and spec",
			event: func() *cloudevents.Event {
				metaJson, err := json.Marshal(metav1.ObjectMeta{
					UID:         kubetypes.UID("test"),
					Generation:  13,
					Name:        "work1",
					Namespace:   "cluster1",
					Labels:      map[string]string{"test1": "test1"},
					Annotations: map[string]string{"test2": "test2"},
					Finalizers:  []string{"test"},
				})
				if err != nil {
					t.Fatal(err)
				}
				evt := cloudevents.NewEvent()
				evt.SetSource("source1")
				evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.test")
				evt.SetExtension("resourceid", "test")
				evt.SetExtension("resourceversion", "13")
				evt.SetExtension("metadata", string(metaJson))
				evt.SetExtension("sequenceid", "1834773391719010304")
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundleStatus{
					ManifestBundle: &payload.ManifestBundle{
						Manifests: []workv1.Manifest{
							{
								RawExtension: runtime.RawExtension{
									Raw: []byte("{\"test\":\"test\"}"),
								},
							},
						},
						DeleteOption: &workv1.DeleteOption{
							PropagationPolicy: workv1.DeletePropagationPolicyTypeForeground,
						},
						ManifestConfigs: []workv1.ManifestConfigOption{
							{
								ResourceIdentifier: workv1.ResourceIdentifier{Name: "test"},
							},
						},
						Executer: &workv1.ManifestWorkExecutor{
							Subject: workv1.ManifestWorkExecutorSubject{
								Type: workv1.ExecutorSubjectTypeServiceAccount,
								ServiceAccount: &workv1.ManifestWorkSubjectServiceAccount{
									Name:      "test-executor-sa",
									Namespace: "test-executor-ns",
								},
							},
						},
					},
					Conditions: []metav1.Condition{
						{
							Type:   "Test",
							Status: metav1.ConditionTrue,
						},
					},
					ResourceStatus: []workv1.ManifestCondition{
						{
							ResourceMeta: workv1.ManifestResourceMeta{
								Name: "test",
							},
						},
					},
				}); err != nil {
					t.Fatal(err)
				}
				return &evt
			}(),
			expectedWork: &workv1.ManifestWork{
				ObjectMeta: metav1.ObjectMeta{
					UID:        kubetypes.UID("test"),
					Generation: 13,
					Name:       "work1",
					Namespace:  "cluster1",
					Labels:     map[string]string{"test1": "test1"},
					Annotations: map[string]string{
						"cloudevents.open-cluster-management.io/sequenceid": "1834773391719010304",
						"test2": "test2",
					},
					Finalizers: []string{"test"},
				},
				Spec: workv1.ManifestWorkSpec{
					Workload: workv1.ManifestsTemplate{
						Manifests: []workv1.Manifest{
							{
								RawExtension: runtime.RawExtension{
									Raw: []byte("{\"test\":\"test\"}"),
								},
							},
						},
					},
					DeleteOption: &workv1.DeleteOption{
						PropagationPolicy: workv1.DeletePropagationPolicyTypeForeground,
					},
					ManifestConfigs: []workv1.ManifestConfigOption{
						{
							ResourceIdentifier: workv1.ResourceIdentifier{Name: "test"},
						},
					},
					Executor: &workv1.ManifestWorkExecutor{
						Subject: workv1.ManifestWorkExecutorSubject{
							Type: workv1.ExecutorSubjectTypeServiceAccount,
							ServiceAccount: &workv1.ManifestWorkSubjectServiceAccount{
								Name:      "test-executor-sa",
								Namespace: "test-executor-ns",
							},
						},
					},
				},
				Status: workv1.ManifestWorkStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Test",
							Status: metav1.ConditionTrue,
						},
					},
					ResourceStatus: workv1.ManifestResourceStatus{
						Manifests: []workv1.ManifestCondition{
							{
								ResourceMeta: workv1.ManifestResourceMeta{
									Name: "test",
								},
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

			if !equality.Semantic.DeepEqual(c.expectedWork, work) {
				t.Errorf("expected %v, but got %v", c.expectedWork, work)
			}
		})
	}
}

func TestManifestBundleLogTracing(t *testing.T) {
	codec := NewManifestBundleCodec()

	t.Run("encode preserves log tracing annotations to event", func(t *testing.T) {
		work := &workv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				UID:             "test-uid",
				ResourceVersion: "1",
				Namespace:       "cluster1",
				Name:            "test-work",
				Generation:      1,
				Annotations: map[string]string{
					"logging.open-cluster-management.io/trace-id": "trace-123",
					"logging.open-cluster-management.io/user":     "test-user",
					"other-annotation":                            "other-value",
				},
			},
			Spec: workv1.ManifestWorkSpec{},
		}

		eventType := types.CloudEventsType{
			CloudEventsDataType: payload.ManifestBundleEventDataType,
			SubResource:         types.SubResourceSpec,
			Action:              "test_create",
		}

		evt, err := codec.Encode("test-source", eventType, work)
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

			if tracingMap["logging.open-cluster-management.io/trace-id"] != "trace-123" {
				t.Errorf("expected trace-id in logtracing, got %v", tracingMap)
			}
			if tracingMap["logging.open-cluster-management.io/user"] != "test-user" {
				t.Errorf("expected user in logtracing, got %v", tracingMap)
			}
			if _, ok := tracingMap["other-annotation"]; ok {
				t.Errorf("non-tracing annotation should not be in logtracing")
			}
		} else {
			t.Errorf("expected logtracing extension to be set")
		}
	})

	t.Run("decode transfers log tracing from event to work annotations", func(t *testing.T) {
		evt := cloudevents.NewEvent()
		evt.SetSource("source1")
		evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.test")
		evt.SetExtension("resourceid", "test-uid")
		evt.SetExtension("resourceversion", "5")
		evt.SetExtension("sequenceid", "1834773391719010304")

		// Add log tracing extension
		tracingMap := map[string]string{
			"logging.open-cluster-management.io/request-id": "req-456",
			"logging.open-cluster-management.io/session":    "sess-789",
		}
		tracingJSON, _ := json.Marshal(tracingMap)
		evt.SetExtension("logtracing", string(tracingJSON))

		if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundleStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Applied",
					Status: metav1.ConditionTrue,
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
		if work.Annotations["logging.open-cluster-management.io/request-id"] != "req-456" {
			t.Errorf("expected request-id annotation, got %v", work.Annotations)
		}
		if work.Annotations["logging.open-cluster-management.io/session"] != "sess-789" {
			t.Errorf("expected session annotation, got %v", work.Annotations)
		}
	})

	t.Run("encode with no tracing annotations", func(t *testing.T) {
		work := &workv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				UID:        "test-uid",
				Namespace:  "cluster1",
				Generation: 1,
				Annotations: map[string]string{
					"regular-annotation": "regular-value",
				},
			},
			Spec: workv1.ManifestWorkSpec{},
		}

		eventType := types.CloudEventsType{
			CloudEventsDataType: payload.ManifestBundleEventDataType,
			SubResource:         types.SubResourceSpec,
			Action:              "test_create",
		}

		evt, err := codec.Encode("test-source", eventType, work)
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
		evt.SetType("io.open-cluster-management.works.v1alpha1.manifestbundles.status.test")
		evt.SetExtension("resourceid", "test-uid")
		evt.SetExtension("resourceversion", "3")
		evt.SetExtension("sequenceid", "1834773391719010304")

		if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ManifestBundleStatus{}); err != nil {
			t.Fatalf("failed to set event data: %v", err)
		}

		work, err := codec.Decode(&evt)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify that only sequenceid annotation is present (no tracing annotations)
		foundTracingAnnotation := false
		for key := range work.Annotations {
			if key != "cloudevents.open-cluster-management.io/sequenceid" {
				foundTracingAnnotation = true
				break
			}
		}
		if foundTracingAnnotation {
			t.Errorf("unexpected tracing annotations found: %v", work.Annotations)
		}
	})
}
