package garbagecollector

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog/v2"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/apimachinery/pkg/util/wait"

	workv1 "open-cluster-management.io/api/client/work/clientset/versioned/typed/work/v1"
	workv1informers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"
)

type eventType int

func (e eventType) String() string {
	switch e {
	case addEvent:
		return "add"
	case updateEvent:
		return "update"
	case deleteEvent:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

const (
	addEvent eventType = iota
	updateEvent
	deleteEvent
)

type event struct {
	eventType eventType
	obj       interface{}
	// the update event comes with an old object, but it's not used by the garbage collector.
	oldObj interface{}
	gvr    schema.GroupVersionResource
}

type monitors map[schema.GroupVersionResource]cache.Controller

type dependent struct {
	ownerUID       types.UID
	namespacedName types.NamespacedName
}

type ownerFilters map[schema.GroupVersionResource]metav1.ListOptions

type GarbageCollector struct {
	// workClient from cloudevents client builder
	workClient workv1.WorkV1Interface
	// workInformer from cloudevents client builder
	workInformer workv1informers.ManifestWorkInformer

	// owner resources to monitor
	ownerGVRs []schema.GroupVersionResource

	// metadataClient to operate on the owner resources
	metadataClient metadata.Interface

	// objectOrMetadataInformerFactory gives access to informers for typed resources
	// and dynamic resources by their metadata. All generic controllers currently use
	// object metadata - if a future controller needs access to the full object this
	// would become GenericInformerFactory and take a dynamic client.
	objectOrMetadataInformerFactory informerfactory.InformerFactory

	// each monitor list/watches a resource (including manifestwork),
	// the results are funneled to the ownerChanges
	monitors monitors

	// monitors are the producer of the ownerChanges queue, garbage collector alters
	// the in-memory owner relationship according to the changes.
	ownerChanges workqueue.RateLimitingInterface

	// ownerToDependents is a map of owner UID to the UIDs of the dependent manifestworks.
	ownerToDependents     map[types.UID][]types.NamespacedName
	ownerToDependentsLock sync.RWMutex

	// garbage collector attempts to delete the items in attemptToDelete queue when the time is ripe.
	attemptToDelete workqueue.RateLimitingInterface
}

func NewGarbageCollector(
	workClient workv1.WorkV1Interface,
	workInformer workv1informers.ManifestWorkInformer,
	ownerGVRs []schema.GroupVersionResource,
	ownerListOptions *metav1.ListOptions,
	kubeClient kubernetes.Interface,
	metadataClient metadata.Interface) *GarbageCollector {
	sharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 10*time.Minute, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.FieldSelector = ownerListOptions.FieldSelector
		options.LabelSelector = ownerListOptions.LabelSelector
	}))
	metadataInformerFactory := metadatainformer.NewFilteredSharedInformerFactory(metadataClient, 10*time.Minute, metav1.NamespaceAll, func(options *metav1.ListOptions) {
		options.FieldSelector = ownerListOptions.FieldSelector
		options.LabelSelector = ownerListOptions.LabelSelector
	})
	return &GarbageCollector{
		workClient:                      workClient,
		workInformer:                    workInformer,
		ownerGVRs:                       ownerGVRs,
		metadataClient:                  metadataClient,
		objectOrMetadataInformerFactory: informerfactory.NewInformerFactory(sharedInformerFactory, metadataInformerFactory),
		ownerToDependents:               make(map[types.UID][]types.NamespacedName),
		ownerChanges:                    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_owner_changes"),
		attemptToDelete:                 workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_attempt_to_delete"),
	}
}

// Run starts garbage collector monitors and workers.
func (gc *GarbageCollector) Run(ctx context.Context, workers int) {
	defer gc.attemptToDelete.ShutDown()
	defer gc.ownerChanges.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting garbage collector")
	defer logger.Info("Shutting down garbage collector")

	// start monitors
	gc.startMonitors(ctx, logger)

	// wait for the controller cache to sync
	if !cache.WaitForNamedCacheSync("garbage collector", ctx.Done(), func() bool {
		return gc.HasSynced(logger)
	}) {
		return
	}

	logger.Info("All resource monitors have synced, proceeding to collect garbage")

	// run gc workers
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, gc.runProcessOwnerChangesWorker, 1*time.Second)
		go wait.UntilWithContext(ctx, gc.runAttemptToDeleteWorker, 1*time.Second)
	}

	<-ctx.Done()
}

// startMonitors starts the monitor list/watches a resource (including manifestwork),
// the results are funneled to the ownerChanges.
func (gc *GarbageCollector) startMonitors(ctx context.Context, logger klog.Logger) error {
	logger.Info("Starting monitors")
	gc.monitors = make(monitors)
	// add monitor for manifestwork
	gc.monitors[workapiv1.SchemeGroupVersion.WithResource("manifestworks")] = gc.workController(logger)

	// add monitor for owner resources
	for _, gvr := range gc.ownerGVRs {
		ctr, err := gc.controllerFor(logger, gvr)
		if err != nil {
			return err
		}
		gc.monitors[gvr] = ctr
	}

	// start monitors
	gc.objectOrMetadataInformerFactory.Start(ctx.Done())
	started := 0
	for _, ctr := range gc.monitors {
		go ctr.Run(ctx.Done())
		started++
	}

	logger.Info("Started monitors", "started", started, "total", len(gc.monitors))
	return nil
}

// workController create controller for manifestwork
func (gc *GarbageCollector) workController(logger klog.Logger) cache.Controller {
	workGVR := workapiv1.SchemeGroupVersion.WithResource("manifestworks")
	handlers := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			event := &event{
				eventType: addEvent,
				obj:       obj,
				gvr:       workGVR,
			}
			accessor, err := meta.Accessor(obj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
				return
			}
			logger.Info("Manifestwork added", "namespace", accessor.GetNamespace(), "name", accessor.GetName())
			gc.ownerChanges.Add(event)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			event := &event{
				eventType: updateEvent,
				obj:       newObj,
				oldObj:    oldObj,
				gvr:       workGVR,
			}
			newAccessor, err := meta.Accessor(newObj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
				return
			}
			oldAccessor, err := meta.Accessor(oldObj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
				return
			}
			// ignore the update if the owner references are the same
			if reflect.DeepEqual(newAccessor.GetOwnerReferences(), oldAccessor.GetOwnerReferences()) {
				return
			}
			logger.Info("Manifestwork updated", "namespace", newAccessor.GetNamespace(), "name", newAccessor.GetName())
			gc.ownerChanges.Add(event)
		},
		DeleteFunc: func(obj interface{}) {
			// delta fifo may wrap the object in a cache.DeletedFinalStateUnknown, unwrap it
			if deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = deletedFinalStateUnknown.Obj
			}
			event := &event{
				eventType: deleteEvent,
				obj:       obj,
				gvr:       workGVR,
			}
			accessor, err := meta.Accessor(obj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
				return
			}
			logger.Info("Manifestwork deleted", "namespace", accessor.GetNamespace(), "name", accessor.GetName())
			gc.ownerChanges.Add(event)
		},
	}
	gc.workInformer.Informer().AddEventHandlerWithResyncPeriod(handlers, 0)

	return gc.workInformer.Informer().GetController()
}

// controllerFor create controller for owner resource
func (gc *GarbageCollector) controllerFor(logger klog.Logger, gvr schema.GroupVersionResource) (cache.Controller, error) {
	handlers := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// event := &event{
			// 	eventType: addEvent,
			// 	obj:       obj,
			// 	gvr:       gvr,
			// }
			// logger.Info("owner added", "resources", gvr)
			// gc.ownerChanges.Add(event)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// TODO: check if there are differences in the ownerRefs,
			// finalizers, and DeletionTimestamp; if not, ignore the update.
			// event := &event{
			// 	eventType: updateEvent,
			// 	obj:       newObj,
			// 	oldObj:    oldObj,
			// 	gvr:       gvr,
			// }
			// logger.Info("owner updated", "resources", gvr)
			// gc.ownerChanges.Add(event)
		},
		DeleteFunc: func(obj interface{}) {
			// delta fifo may wrap the object in a cache.DeletedFinalStateUnknown, unwrap it
			if deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = deletedFinalStateUnknown.Obj
			}
			event := &event{
				eventType: deleteEvent,
				obj:       obj,
				gvr:       gvr,
			}
			accessor, err := meta.Accessor(obj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
				return
			}
			gc.ownerToDependentsLock.RLock()
			defer gc.ownerToDependentsLock.RUnlock()
			// only add the event to the ownerChanges if the owner has dependents
			if _, exist := gc.ownerToDependents[accessor.GetUID()]; exist {
				gc.ownerChanges.Add(event)
				logger.Info("Owner deleted", "resource", gvr, "namespace", accessor.GetNamespace(), "name", accessor.GetName())
			}
		},
	}

	sharedInformer, err := gc.objectOrMetadataInformerFactory.ForResource(gvr)
	if err != nil {
		logger.Error(err, "unable to use a shared informer", "resource", gvr)
		return nil, err
	}
	sharedInformer.Informer().AddEventHandlerWithResyncPeriod(handlers, 0)
	return sharedInformer.Informer().GetController(), nil
}

// HasSynced returns true if any monitors exist AND all those monitors'
// controllers HasSynced functions return true. This means HasSynced could return
// true at one time, and then later return false if all monitors were
// reconstructed.
func (gc *GarbageCollector) HasSynced(logger klog.Logger) bool {
	if len(gc.monitors) == 0 {
		logger.V(4).Info("garbage collector monitors are not synced: no monitors")
		return false
	}

	for resource, ctr := range gc.monitors {
		if !ctr.HasSynced() {
			logger.V(4).Info("garbage controller monitor is not yet synced", "resource", resource)
			return false
		}
	}

	return true
}

// runProcessOwnerChangesWorker start work to process the owner relationship changes.
func (gc *GarbageCollector) runProcessOwnerChangesWorker(ctx context.Context) {
	for gc.processOwnerChangesWorker(ctx) {
	}
}

func (gc *GarbageCollector) processOwnerChangesWorker(ctx context.Context) bool {
	logger := klog.FromContext(ctx)
	item, quit := gc.ownerChanges.Get()
	if quit {
		return false
	}
	defer gc.ownerChanges.Done(item)
	event, ok := item.(*event)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect a *event, got %v", item))
		return true
	}

	obj := event.obj
	accessor, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
		return true
	}
	logger.Info("Processing owner changes", "event", event.eventType, "resource", event.gvr, "namespace", accessor.GetNamespace(), "name", accessor.GetName())

	var oldAccessor metav1.Object
	if event.eventType == updateEvent {
		oldAccessor, err = meta.Accessor(event.oldObj)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("cannot access old obj: %v", err))
			return true
		}
	}

	if event.gvr.String() == workapiv1.SchemeGroupVersion.WithResource("manifestworks").String() {
		owners := accessor.GetOwnerReferences()
		switch {
		case event.eventType == addEvent:
			for _, owner := range owners {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; !exist {
					// TODO: add finalizer to owner object to block onwer deletion
					gc.ownerToDependents[owner.UID] = []types.NamespacedName{}
				}
				gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID], types.NamespacedName{Namespace: accessor.GetNamespace(), Name: accessor.GetName()})
				gc.ownerToDependentsLock.Unlock()
			}
		case event.eventType == updateEvent:
			oldOwners := oldAccessor.GetOwnerReferences()
			added, removed, _ := ownerReferencesDiffs(oldOwners, owners)
			for _, owner := range added {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; !exist {
					// TODO: add finalizer to owner object to block onwer deletion
					gc.ownerToDependents[owner.UID] = []types.NamespacedName{}
				}
				gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID], types.NamespacedName{Namespace: accessor.GetNamespace(), Name: accessor.GetName()})
				gc.ownerToDependentsLock.Unlock()
			}
			for _, owner := range removed {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; exist {
					for i, namespacedName := range gc.ownerToDependents[owner.UID] {
						if namespacedName.Name == accessor.GetName() && namespacedName.Namespace == accessor.GetNamespace() {
							gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID][:i], gc.ownerToDependents[owner.UID][i+1:]...)
							break
						}
					}
					if len(gc.ownerToDependents[owner.UID]) == 0 {
						delete(gc.ownerToDependents, owner.UID)
					}
				}
				gc.ownerToDependentsLock.Unlock()
			}
		case event.eventType == deleteEvent:
			for _, owner := range owners {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; exist {
					for i, namespacedName := range gc.ownerToDependents[owner.UID] {
						if namespacedName.Name == accessor.GetName() && namespacedName.Namespace == accessor.GetNamespace() {
							gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID][:i], gc.ownerToDependents[owner.UID][i+1:]...)
							break
						}
					}
					if len(gc.ownerToDependents[owner.UID]) == 0 {
						delete(gc.ownerToDependents, owner.UID)
					}
				}
				gc.ownerToDependentsLock.Unlock()
			}
		}
	} else {
		// only handle delete event for owner resources
		if event.eventType == deleteEvent {
			gc.ownerToDependentsLock.RLock()
			dependents, exist := gc.ownerToDependents[accessor.GetUID()]
			if exist {
				for _, namespacedName := range dependents {
					gc.attemptToDelete.Add(&dependent{ownerUID: accessor.GetUID(), namespacedName: namespacedName})
				}
			}
			gc.ownerToDependentsLock.RUnlock()
		}
	}

	return true
}

type ownerReferenceChange struct {
	oldOwnerRef metav1.OwnerReference
	newOwnerRef metav1.OwnerReference
}

func ownerReferencesDiffs(old []metav1.OwnerReference, new []metav1.OwnerReference) (added []metav1.OwnerReference, removed []metav1.OwnerReference, changed []ownerReferenceChange) {
	oldUIDToRef := make(map[string]metav1.OwnerReference)
	for _, value := range old {
		oldUIDToRef[string(value.UID)] = value
	}
	oldUIDSet := sets.StringKeySet(oldUIDToRef)
	for _, value := range new {
		newUID := string(value.UID)
		if oldUIDSet.Has(newUID) {
			if !reflect.DeepEqual(oldUIDToRef[newUID], value) {
				changed = append(changed, ownerReferenceChange{oldOwnerRef: oldUIDToRef[newUID], newOwnerRef: value})
			}
			oldUIDSet.Delete(newUID)
		} else {
			added = append(added, value)
		}
	}
	for oldUID := range oldUIDSet {
		removed = append(removed, oldUIDToRef[oldUID])
	}

	return added, removed, changed
}

// runAttemptToDeleteWorker start work to process the attemptToDelete queue.
func (gc *GarbageCollector) runAttemptToDeleteWorker(ctx context.Context) {
	for gc.processAttemptToDeleteWorker(ctx) {
	}
}

func (gc *GarbageCollector) processAttemptToDeleteWorker(ctx context.Context) bool {
	item, quit := gc.attemptToDelete.Get()
	if quit {
		return false
	}
	defer gc.attemptToDelete.Done(item)

	action := gc.attemptToDeleteWorker(ctx, item)
	switch action {
	case forgetItem:
		gc.attemptToDelete.Forget(item)
	case requeueItem:
		gc.attemptToDelete.AddRateLimited(item)
	}

	return true
}

type workQueueItemAction int

const (
	requeueItem = iota
	forgetItem
)

func (gc *GarbageCollector) attemptToDeleteWorker(ctx context.Context, item interface{}) workQueueItemAction {
	dep, ok := item.(*dependent)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect a *dependent, got %v", item))
		return forgetItem
	}

	logger := klog.FromContext(ctx)
	logger.Info("Attempting to delete manifestwork", "ownerUID", dep.ownerUID, "namespacedName", dep.namespacedName)

	latest, err := gc.getManifestwork(ctx, dep.namespacedName)
	if err != nil {
		return requeueItem
	}

	// may happen when manifestwork deletion failed in last attempt
	ownerReferences := latest.GetOwnerReferences()
	if len(ownerReferences) == 0 {
		logger.Info("Manifestwork has no owner references, deleting it")
		if err := gc.deleteManifestwork(ctx, dep.namespacedName); err != nil {
			return requeueItem
		}
		return forgetItem
	}

	found := false
	for _, owner := range ownerReferences {
		if owner.UID == dep.ownerUID {
			found = true
			break
		}
	}

	if found {
		// remove the owner reference from the manifestwork
		logger.Info("Removing owner reference from manifestwork", "owner", dep.ownerUID, "manifestwork", dep.namespacedName)
		jmp, err := generateDeleteOwnerRefJSONMergePatch(latest, dep.ownerUID)
		if err != nil {
			logger.Info("Failed to generate JSON merge patch", "error", err)
			return requeueItem
		}
		if _, err = gc.patchManifestwork(ctx, dep.namespacedName, jmp, types.MergePatchType); err != nil {
			logger.Info("Failed to patch manifestwork with json patch", "error", err)
			return requeueItem
		}

		logger.Info("Successfully removed owner reference from manifestwork", "owner", dep.ownerUID, "manifestwork", dep.namespacedName)

		// if the deleted owner reference is the only owner reference then delete the manifestwork
		if len(ownerReferences) == 1 {
			logger.Info("All owner references are deleted for manifestwork, deleting the manifestwork itself")
			if err := gc.deleteManifestwork(ctx, dep.namespacedName); err != nil {
				return requeueItem
			}
		}
	}

	return forgetItem
}
