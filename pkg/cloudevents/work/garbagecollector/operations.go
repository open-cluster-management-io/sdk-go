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

// type restMappingError struct {
// 	kind    string
// 	version string
// }

// func (r *restMappingError) Error() string {
// 	versionKind := fmt.Sprintf("%s/%s", r.version, r.kind)
// 	return fmt.Sprintf("unable to get REST mapping for %s.", versionKind)
// }

// // Message prints more details
// func (r *restMappingError) Message() string {
// 	versionKind := fmt.Sprintf("%s/%s", r.version, r.kind)
// 	errMsg := fmt.Sprintf("unable to get REST mapping for %s. ", versionKind)
// 	errMsg += fmt.Sprintf(" If %s is an invalid resource, then you should manually remove ownerReferences that refer %s objects.", versionKind, versionKind)
// 	return errMsg
// }

// func newRESTMappingError(kind, version string) *restMappingError {
// 	return &restMappingError{kind: kind, version: version}
// }

// type ownerObjectReference struct {
// 	metav1.OwnerReference
// 	namespace string
// }

// // String is used when logging an ownerObjectReference in text format.
// func (o ownerObjectReference) String() string {
// 	return fmt.Sprintf("[%s/%s, namespace: %s, name: %s, uid: %s]", o.APIVersion, o.Kind, o.namespace, o.Name, o.UID)
// }

// // cluster scoped owner resources don't have namespaces, namespace scoped owner resources default to the manifestwork's namespace
// func resourceDefaultNamespace(namespaced bool, defaultNamespace string) string {
// 	if namespaced {
// 		return defaultNamespace
// 	}
// 	return ""
// }

// // apiResource consults the REST mapper to translate an <apiVersion, kind, namespace>
// // tuple to a unversioned.APIResource struct.
// func (gc *GarbageCollector) apiResource(apiVersion, kind string) (schema.GroupVersionResource, bool, error) {
// 	fqKind := schema.FromAPIVersionAndKind(apiVersion, kind)
// 	mapping, err := gc.restMapper.RESTMapping(fqKind.GroupKind(), fqKind.Version)
// 	if err != nil {
// 		return schema.GroupVersionResource{}, false, newRESTMappingError(kind, apiVersion)
// 	}
// 	return mapping.Resource, mapping.Scope == meta.RESTScopeNamespace, nil
// }

// func (gc *GarbageCollector) getOwnerObject(ctx context.Context, owner ownerObjectReference) (*metav1.PartialObjectMetadata, error) {
// 	resource, namespaced, err := gc.apiResource(owner.APIVersion, owner.Kind)
// 	if err != nil {
// 		return nil, err
// 	}
// 	namespace := resourceDefaultNamespace(namespaced, owner.namespace)
// 	if namespaced && len(namespace) == 0 {
// 		// the owner type is namespaced, but we have no namespace coordinate.
// 		return nil, fmt.Errorf("empty namespace for namespace scoped owner object %s/%s - %s/%s", owner.APIVersion, owner.Kind, namespace, owner.Name)
// 	}
// 	return gc.metadataClient.Resource(resource).Namespace(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
// }

// func (gc *GarbageCollector) patchOwnerObject(ctx context.Context, owner ownerObjectReference, patch []byte, pt types.PatchType) (*metav1.PartialObjectMetadata, error) {
// 	resource, namespaced, err := gc.apiResource(owner.APIVersion, owner.Kind)
// 	if err != nil {
// 		return nil, err
// 	}
// 	namespace := resourceDefaultNamespace(namespaced, owner.namespace)
// 	if namespaced && len(namespace) == 0 {
// 		// the owner type is namespaced, but we have no namespace coordinate.
// 		return nil, fmt.Errorf("empty namespace for namespace scoped owner object %s/%s - %s/%s", owner.APIVersion, owner.Kind, namespace, owner.Name)
// 	}
// 	return gc.metadataClient.Resource(resource).Namespace(namespace).Patch(ctx, owner.Name, pt, patch, metav1.PatchOptions{})
// }

// func (gc *GarbageCollector) deleteOwnerObject(ctx context.Context, owner ownerObjectReference, policy *metav1.DeletionPropagation) error {
// 	resource, namespaced, err := gc.apiResource(owner.APIVersion, owner.Kind)
// 	if err != nil {
// 		return err
// 	}
// 	namespace := resourceDefaultNamespace(namespaced, owner.namespace)
// 	if namespaced && len(namespace) == 0 {
// 		// the owner type is namespaced, but we have no namespace coordinate.
// 		return fmt.Errorf("empty namespace for namespace scoped owner object %s/%s - %s/%s", owner.APIVersion, owner.Kind, namespace, owner.Name)
// 	}
// 	uid := owner.UID
// 	preconditions := metav1.Preconditions{UID: &uid}
// 	deleteOptions := metav1.DeleteOptions{Preconditions: &preconditions, PropagationPolicy: policy}
// 	return gc.metadataClient.Resource(resource).Namespace(namespace).Delete(ctx, owner.Name, deleteOptions)
// }

// func (gc *GarbageCollector) removeFinalizer(ctx context.Context, owner ownerObjectReference, targetFinalizer string) error {
// 	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
// 		ownerObject, err := gc.getOwnerObject(ctx, owner)
// 		if errors.IsNotFound(err) {
// 			return nil
// 		}
// 		if err != nil {
// 			return fmt.Errorf("cannot finalize owner %s, because cannot get it: %v. The garbage collector will retry later", &owner, err)
// 		}
// 		accessor, err := meta.Accessor(ownerObject)
// 		if err != nil {
// 			return fmt.Errorf("cannot access the owner object %v: %v. The garbage collector will retry later", ownerObject, err)
// 		}
// 		finalizers := accessor.GetFinalizers()
// 		var newFinalizers []string
// 		found := false
// 		for _, f := range finalizers {
// 			if f == targetFinalizer {
// 				found = true
// 				continue
// 			}
// 			newFinalizers = append(newFinalizers, f)
// 		}
// 		if !found {
// 			// logger.V(5).Info("finalizer already removed from object", "finalizer", targetFinalizer, "object", owner)
// 			return nil
// 		}

// 		// remove the finalizer
// 		patch, err := json.Marshal(&objectForFinalizersPatch{
// 			ObjectMetaForFinalizersPatch: ObjectMetaForFinalizersPatch{
// 				ResourceVersion: accessor.GetResourceVersion(),
// 				Finalizers:      newFinalizers,
// 			},
// 		})
// 		if err != nil {
// 			return fmt.Errorf("unable to finalize %s due to an error serializing patch: %v", owner, err)
// 		}
// 		_, err = gc.patchOwnerObject(ctx, owner, patch, types.MergePatchType)
// 		return err
// 	})
// 	if errors.IsConflict(err) {
// 		return fmt.Errorf("updateMaxRetries(%d) has reached. The garbage collector will retry later for owner %v", retry.DefaultBackoff.Steps, owner)
// 	}
// 	return err
// }

// func (gc *GarbageCollector) addFinalizer(ctx context.Context, owner ownerObjectReference, targetFinalizer string) error {
// 	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
// 		ownerObject, err := gc.getOwnerObject(ctx, owner)
// 		if errors.IsNotFound(err) {
// 			return nil
// 		}
// 		if err != nil {
// 			return fmt.Errorf("cannot finalize owner %s, because cannot get it: %v. The garbage collector will retry later", &owner, err)
// 		}
// 		accessor, err := meta.Accessor(ownerObject)
// 		if err != nil {
// 			return fmt.Errorf("cannot access the owner object %v: %v. The garbage collector will retry later", ownerObject, err)
// 		}
// 		finalizers := accessor.GetFinalizers()
// 		for _, f := range finalizers {
// 			if f == targetFinalizer {
// 				// logger.V(5).Info("finalizer already added to object", "finalizer", targetFinalizer, "object", owner)
// 				return nil
// 			}
// 		}

// 		// add the finalizer
// 		finalizers = append(finalizers, targetFinalizer)
// 		patch, err := json.Marshal(&objectForFinalizersPatch{
// 			ObjectMetaForFinalizersPatch: ObjectMetaForFinalizersPatch{
// 				ResourceVersion: accessor.GetResourceVersion(),
// 				Finalizers:      finalizers,
// 			},
// 		})
// 		if err != nil {
// 			return fmt.Errorf("unable to finalize %s due to an error serializing patch: %v", owner, err)
// 		}
// 		_, err = gc.patchOwnerObject(ctx, owner, patch, types.MergePatchType)
// 		return err
// 	})
// 	if errors.IsConflict(err) {
// 		return fmt.Errorf("updateMaxRetries(%d) has reached. The garbage collector will retry later for owner %v", retry.DefaultBackoff.Steps, owner)
// 	}
// 	return err
// }

// type objectForFinalizersPatch struct {
// 	ObjectMetaForFinalizersPatch `json:"metadata"`
// }

// // ObjectMetaForFinalizersPatch defines object meta struct for finalizers patch operation.
// type ObjectMetaForFinalizersPatch struct {
// 	ResourceVersion string   `json:"resourceVersion"`
// 	Finalizers      []string `json:"finalizers"`
// }

type objectForOwnerRefsPatch struct {
	ObjectMetaForOwnerRefsPatch `json:"metadata"`
}

// ObjectMetaForOwnerRefsPatch defines object meta struct for owner references patch operation.
type ObjectMetaForOwnerRefsPatch struct {
	ResourceVersion string                  `json:"resourceVersion"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
}

type objectForDeleteOwnerRefStrategicMergePatch struct {
	ObjectMetaForOwnerRefsStrategicMergePatch `json:"metadata"`
}

type ObjectMetaForOwnerRefsStrategicMergePatch struct {
	UID             types.UID           `json:"uid"`
	OwnerReferences []map[string]string `json:"ownerReferences"`
}

// returns a strategic merge patch that removes the owner reference matching ownerUID.
func generateDeleteOwnerRefStrategicMergeBytes(uid types.UID, ownerUID types.UID) ([]byte, error) {
	ownerReferences := []map[string]string{
		{
			"$patch": "delete",
			"uid":    string(ownerUID),
		},
	}
	patch := &objectForDeleteOwnerRefStrategicMergePatch{
		ObjectMetaForOwnerRefsStrategicMergePatch: ObjectMetaForOwnerRefsStrategicMergePatch{
			UID:             uid,
			OwnerReferences: ownerReferences,
		},
	}
	return json.Marshal(patch)
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
