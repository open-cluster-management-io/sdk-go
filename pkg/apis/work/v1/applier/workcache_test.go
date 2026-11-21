package applier

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	workapiv1 "open-cluster-management.io/api/work/v1"
)

func TestCache(t *testing.T) {
	cache := newWorkCache()

	requiredWork := &workapiv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "cluster1",
		},
		Spec: workapiv1.ManifestWorkSpec{
			Workload: workapiv1.ManifestsTemplate{
				Manifests: []workapiv1.Manifest{
					{
						RawExtension: runtime.RawExtension{
							Object: newUnstructured("v1", "ConfigMap", "test", "default"),
						},
					},
				},
			},
		},
	}

	existingWork := requiredWork.DeepCopy()
	existingWork.Generation = 1

	cache.updateCache(requiredWork, existingWork)

	if !cache.safeToSkipApply(requiredWork, existingWork) {
		t.Errorf("Expect skip apply")
	}

	existingWork.Generation = 2

	if cache.safeToSkipApply(requiredWork, existingWork) {
		t.Errorf("should update work when generation is changed")
	}

	existingWork.Generation = 1
	requiredWork.Spec.Workload.Manifests = []workapiv1.Manifest{
		{
			RawExtension: runtime.RawExtension{
				Object: newUnstructured("v1", "ConfigMap", "test1", "default"),
			},
		},
	}

	if cache.safeToSkipApply(requiredWork, existingWork) {
		t.Errorf("should update work when required spec is changed")
	}

	cache.removeCache(requiredWork.Name, requiredWork.Namespace)

	if cache.safeToSkipApply(requiredWork, existingWork) {
		t.Errorf("should update work if related cache is not found")
	}
}

func TestCacheDetectsExternalMetadataChange(t *testing.T) {
	cases := []struct {
		name     string
		required metav1.ObjectMeta
		tamper   func(*workapiv1.ManifestWork)
	}{
		{
			name:     "labels",
			required: metav1.ObjectMeta{Name: "test", Namespace: "cluster1", Labels: map[string]string{"managed-by": "addon"}},
			tamper:   func(w *workapiv1.ManifestWork) { w.Labels["managed-by"] = "tampered" },
		},
		{
			name:     "annotations",
			required: metav1.ObjectMeta{Name: "test", Namespace: "cluster1", Annotations: map[string]string{"config-hash": "abc123"}},
			tamper:   func(w *workapiv1.ManifestWork) { w.Annotations["config-hash"] = "tampered" },
		},
		{
			name: "owner references",
			required: metav1.ObjectMeta{Name: "test", Namespace: "cluster1", OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "ConfigMap", Name: "owner", UID: "abc"},
			}},
			tamper: func(w *workapiv1.ManifestWork) { w.OwnerReferences[0].Name = "tampered" },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := newWorkCache()

			requiredWork := &workapiv1.ManifestWork{ObjectMeta: c.required}
			existingWork := requiredWork.DeepCopy()
			existingWork.Generation = 1

			cache.updateCache(requiredWork, existingWork)

			if !cache.safeToSkipApply(requiredWork, existingWork) {
				t.Errorf("should skip apply when nothing changed")
			}

			c.tamper(existingWork)

			if cache.safeToSkipApply(requiredWork, existingWork) {
				t.Errorf("should not skip apply when %s are externally modified", c.name)
			}
		})
	}
}
