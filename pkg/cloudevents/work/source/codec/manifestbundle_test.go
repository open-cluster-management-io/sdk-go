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
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/payload"
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewManifestBundleCodec().Encode("cluster1-work-agent", c.eventType, c.work)
			if c.expectedErr {
				if err == nil {
					t.Errorf("expected an error, but failed")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error %v", err)
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
					UID:             kubetypes.UID("test"),
					ResourceVersion: "13",
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
					UID:             kubetypes.UID("test"),
					ResourceVersion: "13",
					Name:            "test",
					Namespace:       "cluster1",
					Labels:          map[string]string{"test1": "test1"},
					Annotations:     map[string]string{"test2": "test2"},
					Finalizers:      []string{"test"},
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
					UID:             kubetypes.UID("test"),
					ResourceVersion: "13",
					Name:            "test",
					Namespace:       "cluster1",
					Labels:          map[string]string{"test1": "test1"},
					Annotations:     map[string]string{"test2": "test2"},
					Finalizers:      []string{"test"},
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
