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
