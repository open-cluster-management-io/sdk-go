package applier

import (
	"bytes"
	"context"
	"testing"

	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/apps/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	fakework "open-cluster-management.io/api/client/work/clientset/versioned/fake"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workapiv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// assertActions asserts the actual actions have the expected action verb
func assertActions(t *testing.T, actualActions []clienttesting.Action, expectedVerbs ...string) {
	if len(actualActions) != len(expectedVerbs) {
		t.Fatalf("expected %d call but got: %#v", len(expectedVerbs), actualActions)
	}
	for i, expected := range expectedVerbs {
		if actualActions[i].GetVerb() != expected {
			t.Errorf("expected %s action but got: %#v", expected, actualActions[i])
		}
	}
}

// assertNoActions asserts no actions are happened
func assertNoActions(t *testing.T, actualActions []clienttesting.Action) {
	assertActions(t, actualActions)
}

func newUnstructured(apiVersion, kind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
		},
	}
}

func newFakeWork(name, namespace string, obj runtime.Object) *workapiv1.ManifestWork {
	rawObject, _ := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	rawObject = bytes.TrimRight(rawObject, "\n")

	return &workapiv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: workapiv1.ManifestWorkSpec{
			Workload: workapiv1.ManifestsTemplate{
				Manifests: []workapiv1.Manifest{
					{
						RawExtension: runtime.RawExtension{Raw: rawObject},
					},
				},
			},
			DeleteOption:    nil,
			ManifestConfigs: nil,
		},
	}
}

func TestWorkApplierWithTypedClient(t *testing.T) {
	fakeWorkClient := fakework.NewSimpleClientset()
	workInformerFactory := workinformers.NewSharedInformerFactory(fakeWorkClient, 10*time.Minute)
	fakeWorkLister := workInformerFactory.Work().V1().ManifestWorks().Lister()
	workApplier := NewWorkApplierWithTypedClient(fakeWorkClient, fakeWorkLister)

	work := newFakeWork("test", "test", newUnstructured("batch/v1", "Job", "default", "test"))
	_, err := workApplier.Apply(context.TODO(), work)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertActions(t, fakeWorkClient.Actions(), "create")
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(work); err != nil {
		t.Errorf("failed to add work to store with err %v", err)
	}

	// IF work is not changed, we should not update
	newWorkCopy := work.DeepCopy()
	fakeWorkClient.ClearActions()
	_, err = workApplier.Apply(context.TODO(), newWorkCopy)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertNoActions(t, fakeWorkClient.Actions())

	// Update work spec to update it
	newWork := newFakeWork("test", "test", newUnstructured("batch/v1", "Job", "default", "test"))
	newWork.Spec.DeleteOption = &workapiv1.DeleteOption{PropagationPolicy: workapiv1.DeletePropagationPolicyTypeOrphan}
	fakeWorkClient.ClearActions()
	appliedWork, err := workApplier.Apply(context.TODO(), newWork)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertActions(t, fakeWorkClient.Actions(), "patch")
	if !apiequality.Semantic.DeepEqual(appliedWork.Spec.DeleteOption, newWork.Spec.DeleteOption) {
		t.Errorf("unexpected applied work %v", appliedWork.Spec.DeleteOption)
	}
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(newWork); err != nil {
		t.Errorf("failed to add work to store with err %v", err)
	}

	// update work annotation
	newWork = appliedWork.DeepCopy()
	newWork.SetAnnotations(map[string]string{workapiv1.ManifestConfigSpecHashAnnotationKey: "hash"})
	fakeWorkClient.ClearActions()
	appliedWork, err = workApplier.Apply(context.TODO(), newWork)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertActions(t, fakeWorkClient.Actions(), "patch")
	if !apiequality.Semantic.DeepEqual(appliedWork.Annotations, newWork.Annotations) {
		t.Errorf("unexpected applied work %v", appliedWork.Annotations)
	}
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Update(newWork); err != nil {
		t.Errorf("failed to add work to store with err %v", err)
	}

	// remove work annotation
	newWork = appliedWork.DeepCopy()
	newWork.Annotations = nil
	fakeWorkClient.ClearActions()
	appliedWork, err = workApplier.Apply(context.TODO(), newWork)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertActions(t, fakeWorkClient.Actions(), "patch")
	if !apiequality.Semantic.DeepEqual(appliedWork.Annotations, newWork.Annotations) {
		t.Errorf("unexpected applied work %v", appliedWork.Annotations)
	}
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Update(newWork); err != nil {
		t.Errorf("failed to add work to store with err %v", err)
	}

	// Do not update if generation is not changed
	work.Spec.DeleteOption = &workapiv1.DeleteOption{PropagationPolicy: workapiv1.DeletePropagationPolicyTypeForeground}
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Update(work); err != nil {
		t.Errorf("failed to update work with err %v", err)
	}

	fakeWorkClient.ClearActions()
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Update(work); err != nil {
		t.Errorf("failed to update work with err %v", err)
	}
	_, err = workApplier.Apply(context.TODO(), newWork)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertNoActions(t, fakeWorkClient.Actions())

	// change generation will cause update
	work.Generation = 1
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Update(work); err != nil {
		t.Errorf("failed to update work with err %v", err)
	}

	fakeWorkClient.ClearActions()
	if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Update(work); err != nil {
		t.Errorf("failed to update work with err %v", err)
	}
	_, err = workApplier.Apply(context.TODO(), newWork)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	assertActions(t, fakeWorkClient.Actions(), "patch")

	fakeWorkClient.ClearActions()
	err = workApplier.Delete(context.TODO(), newWork.Namespace, newWork.Name)
	if err != nil {
		t.Errorf("failed to delete work with err %v", err)
	}
	assertActions(t, fakeWorkClient.Actions(), "delete")
}

func getWork(t *testing.T, c client.Client, namespace, name string) *workapiv1.ManifestWork {
	t.Helper()
	work := &workapiv1.ManifestWork{}
	if err := c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: name}, work); err != nil {
		t.Fatalf("failed to get work %s/%s: %v", namespace, name, err)
	}
	return work
}

func assertWorkState(t *testing.T, c client.Client, namespace, name string, desired *workapiv1.ManifestWork) {
	t.Helper()
	actual := getWork(t, c, namespace, name)
	if diff := cmp.Diff(desired.Spec, actual.Spec); diff != "" {
		t.Fatalf("spec of %s/%s mismatch (-want +got):\n%s", namespace, name, diff)
	}
	if diff := cmp.Diff(desired.Annotations, actual.Annotations); diff != "" {
		t.Fatalf("annotations of %s/%s mismatch (-want +got):\n%s", namespace, name, diff)
	}
}

func TestWorkApplierWithRuntimeClient(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := workapiv1.Install(scheme); err != nil {
		t.Fatalf("failed to add work scheme: %v", err)
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	workApplier := NewWorkApplierWithRuntimeClient(fakeClient)
	ctx := context.TODO()

	baseWork := newFakeWork("test", "test", newUnstructured("batch/v1", "Job", "default", "test"))
	desired := baseWork.DeepCopy()

	// Create: apply a MW that doesn't exist, verify it's persisted
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to create work: %v", err)
	}
	assertWorkState(t, fakeClient, "test", "test", desired)

	// Update: change the desired spec, verify the object is patched
	desired.Spec.DeleteOption = &workapiv1.DeleteOption{PropagationPolicy: workapiv1.DeletePropagationPolicyTypeForeground}
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to update work: %v", err)
	}
	assertWorkState(t, fakeClient, "test", "test", desired)

	// Annotation add: verify annotations are patched
	desired.SetAnnotations(map[string]string{workapiv1.ManifestConfigSpecHashAnnotationKey: "hash"})
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to add annotation: %v", err)
	}
	assertWorkState(t, fakeClient, "test", "test", desired)

	// Annotation remove: verify annotations are cleared
	desired.Annotations = nil
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to remove annotation: %v", err)
	}
	assertWorkState(t, fakeClient, "test", "test", desired)

	// Cache hit: same desired, verify no write via unchanged resourceVersion
	rvBefore := getWork(t, fakeClient, "test", "test").ResourceVersion
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to re-apply unchanged work: %v", err)
	}
	rvAfter := getWork(t, fakeClient, "test", "test").ResourceVersion
	if rvBefore != rvAfter {
		t.Fatalf("expected no write, but resourceVersion changed from %s to %s", rvBefore, rvAfter)
	}

	// Cache hit when generation unchanged: externally modify the spec without
	// bumping generation. The cache still sees matching generation + desired
	// hash, so it skips the apply.
	tampered := getWork(t, fakeClient, "test", "test").DeepCopy()
	tampered.Spec.DeleteOption = &workapiv1.DeleteOption{PropagationPolicy: workapiv1.DeletePropagationPolicyTypeOrphan}
	if err := fakeClient.Update(ctx, tampered); err != nil {
		t.Fatalf("failed to externally modify work: %v", err)
	}
	rvBefore = getWork(t, fakeClient, "test", "test").ResourceVersion
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to re-apply after external modification without generation bump: %v", err)
	}
	rvAfter = getWork(t, fakeClient, "test", "test").ResourceVersion
	if rvBefore != rvAfter {
		t.Fatalf("expected no write when generation unchanged, but resourceVersion changed from %s to %s", rvBefore, rvAfter)
	}

	// External modification with generation bump: simulate a real API server
	// spec change (e.g., kubectl edit or another addon manager).
	// The applier should revert the work back to its desired state.
	tampered.Generation++
	if err := fakeClient.Update(ctx, tampered); err != nil {
		t.Fatalf("failed to externally modify work: %v", err)
	}
	if _, err := workApplier.Apply(ctx, desired.DeepCopy()); err != nil {
		t.Fatalf("failed to restore drifted work: %v", err)
	}
	assertWorkState(t, fakeClient, "test", "test", desired)

	// Delete: verify object is removed
	if err := workApplier.Delete(ctx, "test", "test"); err != nil {
		t.Fatalf("failed to delete work: %v", err)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "test", Namespace: "test"}, &workapiv1.ManifestWork{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound after delete, got: %v", err)
	}

	// Delete nonexistent: verify idempotency (no error)
	if err := workApplier.Delete(ctx, "test", "nonexistent"); err != nil {
		t.Fatalf("expected no error deleting nonexistent work, got: %v", err)
	}
}

var deploymentJson = `{
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "labels": {
            "app": "helloworld-agent"
        },
        "name": "helloworld-agent",
        "namespace": "default"
    },
    "spec": {
        "replicas": 1,
        "selector": {
            "matchLabels": {
                "app": "helloworld-agent"
            }
        },
		"strategy": {
            "rollingUpdate": {
                "maxSurge": "25%",
                "maxUnavailable": "25%"
            },
            "type": "RollingUpdate"
        },
        "template": {
            "metadata": {
                "labels": {
                    "app": "helloworld-agent"
                }
            },
            "spec": {
                "containers": [
                    {
                        "args": [
                            "/helloworld"
                        ],
                        "image": "quay.io/open-cluster-management/addon-examples:latest",
						"imagePullPolicy": "IfNotPresent",
                        "name": "helloworld-agent",
                        "resources": {}
                    }
                ]
            }
        }
    },
    "status":{}
}
`

// the raw in object has no creationTimestamp
func NewManifestFromJson() runtime.Object {
	obj := &unstructured.Unstructured{}
	_ = obj.UnmarshalJSON([]byte(deploymentJson))
	return obj
}

// the raw in object has creationTimestamp
func NewManifestFromDecoder() runtime.Object {
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	object, _, _ := decoder.Decode([]byte(deploymentJson), nil, nil)
	return object
}

func Test_ManifestWorkEqual(t *testing.T) {
	cases := []struct {
		name         string
		requiredWork func() *workapiv1.ManifestWork
		existingWork func() *workapiv1.ManifestWork
		expected     bool
	}{
		{
			name: "required and existing with same labels",
			requiredWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromJson())
				work.SetLabels(map[string]string{"test": "test"})
				return work
			},
			existingWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromDecoder())
				work.SetLabels(map[string]string{"test": "test"})
				return work
			},
			expected: true,
		},
		{
			name: "required and existing add labels",
			requiredWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromJson())
				work.SetLabels(map[string]string{"addonname": "test"})
				return work
			},
			existingWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromDecoder())
				return work
			},
			expected: false,
		},
		{
			name: "required and existing remove labels",
			requiredWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromJson())
				return work
			},
			existingWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromDecoder())
				work.SetLabels(map[string]string{"addonname": "test"})
				return work
			},
			expected: false,
		},
		{
			name: "required and existing update labels",
			requiredWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromJson())
				work.SetLabels(map[string]string{"test": "test"})
				return work
			},
			existingWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromDecoder())
				work.SetLabels(map[string]string{"addonname": "test"})
				return work
			},
			expected: false,
		},
		{
			name: "required and existing with same spec",
			requiredWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromJson())
				work.SetLabels(map[string]string{"test": "test"})
				work.Spec.ManifestConfigs = []workapiv1.ManifestConfigOption{
					{
						ResourceIdentifier: workapiv1.ResourceIdentifier{},
						FeedbackRules: []workapiv1.FeedbackRule{
							{
								Type: workapiv1.WellKnownStatusType,
							},
						},
					},
				}

				return work
			},
			existingWork: func() *workapiv1.ManifestWork {
				work := newFakeWork("test", "test", NewManifestFromDecoder())
				work.SetLabels(map[string]string{"test": "test"})
				work.Spec.ManifestConfigs = []workapiv1.ManifestConfigOption{
					{
						ResourceIdentifier: workapiv1.ResourceIdentifier{},
						FeedbackRules: []workapiv1.FeedbackRule{
							{
								Type: "WellKnownStatus",
							},
						},
					},
				}
				return work
			},
			expected: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := ManifestWorkEqual(c.requiredWork(), c.existingWork())
			if c.expected != actual {
				t.Errorf("expected %v, but got %v", c.expected, actual)
			}

		})
	}
}

func TestCreateWork(t *testing.T) {
	fakeWorkClient := fakework.NewSimpleClientset()
	fakeWorkClient.ClearActions()

	workInformerFactory := workinformers.NewSharedInformerFactory(fakeWorkClient, 10*time.Minute)
	fakeWorkLister := workInformerFactory.Work().V1().ManifestWorks().Lister()
	workApplier := NewWorkApplierWithTypedClient(fakeWorkClient, fakeWorkLister)

	fakeWorkClient.PrependReactor("create", "manifestworks", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, apierrors.NewAlreadyExists(workapiv1.Resource("manifestworks"), "test")
	})
	work := newFakeWork("test", "test", newUnstructured("batch/v1", "Job", "default", "test"))
	_, err := workApplier.Apply(context.TODO(), work)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	if workApplier.cache.safeToSkipApply(work, work) {
		t.Errorf("should not create work")
	}
	fakeWorkClient.ReactionChain = []clienttesting.Reactor{}
	_, err = workApplier.Apply(context.TODO(), work)
	if err != nil {
		t.Errorf("failed to apply work with err %v", err)
	}
	if !workApplier.cache.safeToSkipApply(work, work) {
		t.Errorf("should create work")
	}
}
