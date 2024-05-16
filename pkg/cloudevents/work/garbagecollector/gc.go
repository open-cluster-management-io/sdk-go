package garbagecollector

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"strings"
// 	"time"

// 	"github.com/openshift/library-go/pkg/controller/factory"
// 	"github.com/openshift/library-go/pkg/operator/events"
// 	"k8s.io/apimachinery/pkg/api/meta"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"k8s.io/apimachinery/pkg/runtime/schema"
// 	"k8s.io/apimachinery/pkg/types"
// 	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
// 	"k8s.io/client-go/metadata"
// 	"k8s.io/client-go/metadata/metadatainformer"
// 	"k8s.io/client-go/tools/cache"
// 	"k8s.io/klog/v2"
// 	workv1 "open-cluster-management.io/api/client/work/clientset/versioned/typed/work/v1"
// 	workv1informers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
// 	worklisterv1 "open-cluster-management.io/api/client/work/listers/work/v1"
// 	workapiv1 "open-cluster-management.io/api/work/v1"
// )

// const (
// 	manifestWorkByOwner = "manifestWorkByOwner"
// )

// type GC struct {
// 	workClient  workv1.WorkV1Interface
// 	workLister  worklisterv1.ManifestWorkLister
// 	workIndexer cache.Indexer
// }

// func NewGC(recorder events.Recorder,
// 	metadataClient metadata.Interface,
// 	workClient workv1.WorkV1Interface,
// 	workInformer workv1informers.ManifestWorkInformer,
// 	ownerGVRFilters map[schema.GroupVersionResource]*metav1.ListOptions) factory.Controller {
// 	err := workInformer.Informer().AddIndexers(
// 		cache.Indexers{
// 			manifestWorkByOwner: indexManifestWorkByOwner,
// 		})
// 	if err != nil {
// 		utilruntime.HandleError(err)
// 	}

// 	gc := &GC{
// 		workClient:  workClient,
// 		workIndexer: workInformer.Informer().GetIndexer(),
// 		workLister:  workInformer.Lister(),
// 	}

// 	ownerInformers := make([]factory.Informer, len(ownerGVRFilters))
// 	i := 0
// 	for gvr, listOptions := range ownerGVRFilters {
// 		informer := metadatainformer.NewFilteredMetadataInformer(metadataClient, gvr,
// 			metav1.NamespaceAll, 10*time.Minute,
// 			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
// 			func(options *metav1.ListOptions) {
// 				if listOptions != nil {
// 					options.FieldSelector = listOptions.FieldSelector
// 					options.LabelSelector = listOptions.LabelSelector
// 				}
// 			})
// 		ownerInformers[i] = informer.Informer()
// 		go informer.Informer().Run(context.Background().Done())

// 		i++
// 	}

// 	return factory.New().
// 		WithInformersQueueKeyFunc(queueKeyByMetaNamespaceName, workInformer.Informer()).
// 		WithFilteredEventsInformersQueueKeysFunc(gc.ownerQueueKeysFunc, ownerEventFilterFunc, ownerInformers...).
// 		WithSync(gc.sync).ToController("GC", recorder)
// }

// func queueKeyByMetaNamespaceName(obj runtime.Object) string {
// 	key, err := cache.MetaNamespaceKeyFunc(obj)
// 	if err != nil {
// 		utilruntime.HandleError(err)
// 		return ""
// 	}
// 	return key
// }

// func (gc *GC) sync(ctx context.Context, controllerContext factory.SyncContext) error {
// 	key := controllerContext.QueueKey()
// 	klog.V(4).Infof("Reconciling ManifestWork %q", key)
// 	fmt.Printf("============ Reconciling ManifestWork %q\n", key)

// 	parts := strings.Split(key, "/")
// 	if len(parts) != 3 {
// 		if len(parts) == 2 {
// 			fmt.Printf("============ manifestwork no need to reconcile: %s\n", key)
// 			return nil
// 		}
// 		return fmt.Errorf("unexpected key format: %q", key)
// 	}

// 	ownerUID, namespace, name := parts[0], parts[1], parts[2]
// 	manifestWork, err := gc.workLister.ManifestWorks(namespace).Get(name)
// 	if err != nil {
// 		return fmt.Errorf("failed to get ManifestWork %s/%s: %v", namespace, name, err)
// 	}

// 	if manifestWork == nil {
// 		klog.V(4).Infof("ManifestWork %s/%s not found", namespace, name)
// 		return nil
// 	}

// 	ownerRefs := manifestWork.GetOwnerReferences()
// 	if len(ownerRefs) == 0 {
// 		klog.V(4).Infof("ManifestWork %s/%s has no owner", namespace, name)
// 		if err := gc.workClient.ManifestWorks(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
// 			return fmt.Errorf("failed to delete ManifestWork %s/%s: %v", namespace, name, err)
// 		}
// 		return nil
// 	}

// 	newOwnerRefs := []metav1.OwnerReference{}
// 	for _, ownerRef := range manifestWork.GetOwnerReferences() {
// 		if string(ownerRef.UID) == ownerUID {
// 			continue
// 		}
// 		newOwnerRefs = append(newOwnerRefs, ownerRef)
// 	}

// 	expectedObjectMeta := objectForOwnerRefsPatch{
// 		ObjectMetaForOwnerRefsPatch: ObjectMetaForOwnerRefsPatch{
// 			ResourceVersion: manifestWork.GetResourceVersion(),
// 			OwnerReferences: newOwnerRefs,
// 		},
// 	}
// 	jmp, err := json.Marshal(expectedObjectMeta)
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal JSON merge patch: %v", err)
// 	}

// 	if _, err = gc.workClient.ManifestWorks(namespace).Patch(ctx, name, types.MergePatchType, jmp, metav1.PatchOptions{}); err != nil {
// 		return fmt.Errorf("failed to patch ManifestWork %s/%s: %v", namespace, name, err)
// 	}

// 	if len(newOwnerRefs) == 0 {
// 		klog.V(4).Infof("ManifestWork %s/%s has no owner after patch, deleting", namespace, name)
// 		if err = gc.workClient.ManifestWorks(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
// 			return fmt.Errorf("failed to delete ManifestWork %s/%s: %v", namespace, name, err)
// 		}
// 	}

// 	return nil
// }

// func (gc *GC) ownerQueueKeysFunc(obj runtime.Object) []string {
// 	// delta fifo may wrap the object in a cache.DeletedFinalStateUnknown, unwrap it
// 	o := interface{}(obj)
// 	if deletedFinalStateUnknown, ok := o.(cache.DeletedFinalStateUnknown); ok {
// 		o = deletedFinalStateUnknown.Obj
// 	}

// 	accessor, err := meta.Accessor(o)
// 	if err != nil {
// 		utilruntime.HandleError(err)
// 		return []string{}
// 	}

// 	ownerUID := string(accessor.GetUID())
// 	objs, err := gc.workIndexer.ByIndex(manifestWorkByOwner, ownerUID)
// 	if err != nil {
// 		utilruntime.HandleError(err)
// 		return []string{}
// 	}

// 	var keys []string
// 	for _, o := range objs {
// 		manifestWork := o.(*workapiv1.ManifestWork)
// 		klog.V(4).Infof("enqueue manifestWork %s/%s, because of owner %s", manifestWork.Namespace, manifestWork.Name, ownerUID)
// 		fmt.Printf("============ enqueue manifestWork %s/%s, because of owner %s\n", manifestWork.Namespace, manifestWork.Name, ownerUID)
// 		keys = append(keys, fmt.Sprintf("%s/%s/%s", ownerUID, manifestWork.Namespace, manifestWork.Name))
// 	}

// 	return keys
// }

// func ownerEventFilterFunc(obj interface{}) bool {
// 	accessor, _ := meta.Accessor(obj)
// 	fmt.Printf("============ ownerEventFilterFunc %s/%s, deletetime: %s\n", accessor.GetNamespace(), accessor.GetName(), accessor.GetDeletionTimestamp())
// 	// return !accessor.GetDeletionTimestamp().IsZero()
// 	return true
// }

// // func indexManifestWorkByOwner(obj interface{}) ([]string, error) {
// // 	manifestWork, ok := obj.(*workapiv1.ManifestWork)
// // 	if !ok {
// // 		return []string{}, fmt.Errorf("obj %T is not a ManifestWork", obj)
// // 	}
// // 	fmt.Printf("============ indexManifestWorkByOwner for ManifestWork %s/%s\n", manifestWork.Namespace, manifestWork.Name)

// // 	var ownerKeys []string
// // 	for _, ownerRef := range manifestWork.GetOwnerReferences() {
// // 		ownerKeys = append(ownerKeys, string(ownerRef.UID))
// // 	}

// // 	return ownerKeys, nil
// // }
