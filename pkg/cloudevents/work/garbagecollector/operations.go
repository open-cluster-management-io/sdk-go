package garbagecollector

import (
	"context"
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	workapiv1 "open-cluster-management.io/api/work/v1"
)

func (gc *GarbageCollector) getManifestwork(ctx context.Context, namespacedName types.NamespacedName) (*workapiv1.ManifestWork, error) {
	return gc.workClient.ManifestWorks(namespacedName.Namespace).Get(ctx, namespacedName.Name, metav1.GetOptions{})
}

func (gc *GarbageCollector) patchManifestwork(ctx context.Context, namespacedName types.NamespacedName, patch []byte, pt types.PatchType) (*workapiv1.ManifestWork, error) {
	return gc.workClient.ManifestWorks(namespacedName.Namespace).Patch(ctx, namespacedName.Name, pt, patch, metav1.PatchOptions{})
}

func (gc *GarbageCollector) deleteManifestwork(ctx context.Context, namespacedName types.NamespacedName) error {
	return gc.workClient.ManifestWorks(namespacedName.Namespace).Delete(ctx, namespacedName.Name, metav1.DeleteOptions{})
}

// objectForOwnerRefsPatch defines object struct for owner references patch operation.
type objectForOwnerRefsPatch struct {
	ObjectMetaForOwnerRefsPatch `json:"metadata"`
}

// ObjectMetaForOwnerRefsPatch defines object meta struct for owner references patch operation.
type ObjectMetaForOwnerRefsPatch struct {
	ResourceVersion string                  `json:"resourceVersion"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
}

// returns JSON merge patch that removes the ownerReferences matching ownerUID.
func generateDeleteOwnerRefJSONMergePatch(obj metav1.Object, ownerUID types.UID) ([]byte, error) {
	expectedObjectMeta := objectForOwnerRefsPatch{}
	expectedObjectMeta.ResourceVersion = obj.GetResourceVersion()
	refs := obj.GetOwnerReferences()
	for _, ref := range refs {
		if ref.UID != ownerUID {
			expectedObjectMeta.OwnerReferences = append(expectedObjectMeta.OwnerReferences, ref)
		}
	}
	return json.Marshal(expectedObjectMeta)
}
