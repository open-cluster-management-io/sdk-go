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
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog/v2"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
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
	gvk    schema.GroupVersionKind
}

// monitor runs a Controller with a local stop channel.
type monitor struct {
	controller cache.Controller

	// stopCh stops Controller. If stopCh is nil, the monitor is considered to be
	// not yet started.
	stopCh chan struct{}
}

// Run is intended to be called in a goroutine. Multiple calls of this is an error.
func (m *monitor) Run() {
	m.controller.Run(m.stopCh)
}

type monitors map[schema.GroupVersionResource]*monitor

type dependent struct {
	ownerUID       types.UID
	namespacedName types.NamespacedName
}

type GarbageCollector struct {
	workClient workv1.WorkV1Interface
	// workInformer from cloudevents client builder
	workInformer workv1informers.ManifestWorkInformer

	// metadataClient is used to operator on the owner resources.
	metadataClient metadata.Interface
	// sharedInformers is used to create informers for the owner resources.
	sharedInformers informerfactory.InformerFactory
	restMapper      meta.RESTMapper

	// each monitor list/watches a resource (including manifestwork),
	// the results are funneled to the ownerChanges
	monitors    monitors
	monitorLock sync.RWMutex

	// monitors are the producer of the ownerChanges queue, garbage collector alters
	// the in-memory owner relationship according to the changes.
	ownerChanges workqueue.RateLimitingInterface

	// owners is a map of resource to the number of manifestworks that it owns.
	owners     map[schema.GroupVersionResource]int
	ownersLock sync.RWMutex

	// ownerToDependents is a map of owner UID to the UIDs of the dependent manifestworks.
	ownerToDependents     map[types.UID][]types.NamespacedName
	ownerToDependentsLock sync.RWMutex

	// garbage collector attempts to delete the items in attemptToDelete queue when the time is ripe.
	attemptToDelete workqueue.RateLimitingInterface
	// garbage collector attempts to orphan the dependents of the items in the attemptToOrphan queue, then deletes the items.
	attemptToOrphan workqueue.RateLimitingInterface

	// workerLock ensures workers are paused to avoid processing events before informers have resynced.
	workerLock sync.RWMutex
}

func NewGarbageCollector(
	workClient workv1.WorkV1Interface,
	metadataClient metadata.Interface,
	restMapper meta.RESTMapper,
	workInformer workv1informers.ManifestWorkInformer,
	sharedInformers informerfactory.InformerFactory) *GarbageCollector {
	return &GarbageCollector{
		workClient:        workClient,
		metadataClient:    metadataClient,
		restMapper:        restMapper,
		workInformer:      workInformer,
		sharedInformers:   sharedInformers,
		owners:            make(map[schema.GroupVersionResource]int),
		ownerToDependents: make(map[types.UID][]types.NamespacedName),
		ownerChanges:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_owner_changes"),
		attemptToDelete:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_attempt_to_delete"),
		attemptToOrphan:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "garbage_collector_attempt_to_orphan"),
	}
}

// Run starts garbage collector monitors and workers.
func (gc *GarbageCollector) Run(ctx context.Context, workers int) {
	defer gc.attemptToDelete.ShutDown()
	defer gc.attemptToOrphan.ShutDown()
	defer gc.ownerChanges.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting garbage collector")
	defer logger.Info("Shutting down garbage collector")

	// start monitors
	gc.startMonitors(ctx, logger)

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
		go wait.UntilWithContext(ctx, gc.runAttemptToOrphanWorker, 1*time.Second)
	}

	<-ctx.Done()
	// stop monitors
	gc.stopMonitors(logger)
}

func (gc *GarbageCollector) startMonitors(ctx context.Context, logger klog.Logger) {
	// add monitor for manifestwork
	gc.monitorLock.Lock()
	defer gc.monitorLock.Unlock()
	gc.monitors = make(monitors)
	gc.monitors[workapiv1.SchemeGroupVersion.WithResource("manifestworks")] = gc.manifestWorkMonitor(logger)

	logger.Info("Starting monitors")
	started := 0
	for gvr, monitor := range gc.monitors {
		if monitor.stopCh == nil {
			logger.Info("Starting new monitor", "GVR", gvr)
			monitor.stopCh = make(chan struct{})
			gc.sharedInformers.Start(ctx.Done())
			go monitor.Run()
			started++
		}
	}
	logger.Info("Started monitors", "started", started, "total", len(gc.monitors))
}

func (gc *GarbageCollector) stopMonitors(logger klog.Logger) {
	gc.monitorLock.Lock()
	defer gc.monitorLock.Unlock()
	logger.Info("Stopping monitors")
	monitors := gc.monitors
	stopped := 0
	for gvr, monitor := range monitors {
		if monitor.stopCh != nil {
			logger.Info("Stopping monitor", "GVR", gvr)
			stopped++
			close(monitor.stopCh)
		}
	}

	// reset monitors so that the garbage collector can be safely re-run.
	gc.monitors = nil
	logger.Info("Stopped monitors", "stopped", stopped, "total", len(monitors))
}

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
	logger.Info("Processing owner changes", "event", event.eventType, "gvk", event.gvk, "namespace", accessor.GetNamespace(), "name", accessor.GetName())

	var oldAccessor metav1.Object
	if event.eventType == updateEvent {
		oldAccessor, err = meta.Accessor(event.oldObj)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
			return true
		}
	}

	if event.gvk.String() == workapiv1.SchemeGroupVersion.WithKind("ManifestWork").String() {
		addedOwnerResource := make([]schema.GroupVersionResource, 0)
		removedOwnerResource := make([]schema.GroupVersionResource, 0)
		owners := accessor.GetOwnerReferences()

		switch {
		case event.eventType == addEvent:
			for _, owner := range owners {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; !exist {
					gc.ownerToDependents[owner.UID] = make([]types.NamespacedName, 0)
					// TODO: add finalizer to owner object to block onwer deletion
				}
				gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID], types.NamespacedName{Namespace: accessor.GetNamespace(), Name: accessor.GetName()})
				gc.ownerToDependentsLock.Unlock()

				ownerGVK := schema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind)
				mapping, err := gc.restMapper.RESTMapping(ownerGVK.GroupKind(), ownerGVK.Version)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("cannot get mapping for %v: %v", ownerGVK, err))
				}
				gc.ownersLock.Lock()
				if _, exist := gc.owners[mapping.Resource]; !exist {
					addedOwnerResource = append(addedOwnerResource, mapping.Resource)
					gc.owners[mapping.Resource] = 1
				} else {
					gc.owners[mapping.Resource] = gc.owners[mapping.Resource] + 1
				}
				gc.ownersLock.Unlock()
			}
		case event.eventType == updateEvent:
			oldOwners := oldAccessor.GetOwnerReferences()
			added, removed, _ := ownerReferencesDiffs(oldOwners, owners)
			for _, owner := range added {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; !exist {
					gc.ownerToDependents[owner.UID] = make([]types.NamespacedName, 0)
					// TODO: add finalizer to owner object to block onwer deletion
				}
				gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID], types.NamespacedName{Namespace: accessor.GetNamespace(), Name: accessor.GetName()})
				gc.ownerToDependentsLock.Unlock()

				ownerGVK := schema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind)
				mapping, err := gc.restMapper.RESTMapping(ownerGVK.GroupKind(), ownerGVK.Version)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("cannot get mapping for %v: %v", ownerGVK, err))
				}
				gc.ownersLock.Lock()
				if _, exist := gc.owners[mapping.Resource]; !exist {
					addedOwnerResource = append(addedOwnerResource, mapping.Resource)
					gc.owners[mapping.Resource] = 1
				} else {
					gc.owners[mapping.Resource] = gc.owners[mapping.Resource] + 1
				}
				gc.ownersLock.Unlock()
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
				}
				gc.ownerToDependentsLock.Unlock()

				ownerGVK := schema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind)
				mapping, err := gc.restMapper.RESTMapping(ownerGVK.GroupKind(), ownerGVK.Version)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("cannot get mapping for %v: %v", ownerGVK, err))
				}

				gc.ownersLock.Lock()
				if _, exist := gc.owners[mapping.Resource]; exist {
					gc.owners[mapping.Resource] = gc.owners[mapping.Resource] - 1
					if gc.owners[mapping.Resource] == 0 {
						removedOwnerResource = append(removedOwnerResource, mapping.Resource)
						delete(gc.owners, mapping.Resource)
					}
					gc.ownersLock.Unlock()
				}
			}
			// TODO: handle changed owner references
		case event.eventType == deleteEvent:
			// TODO: block deletion of manifestwork if the owner is not being deleted
			for _, owner := range owners {
				gc.ownerToDependentsLock.Lock()
				if _, exist := gc.ownerToDependents[owner.UID]; exist {
					for i, namespacedName := range gc.ownerToDependents[owner.UID] {
						if namespacedName.Name == accessor.GetName() && namespacedName.Namespace == accessor.GetNamespace() {
							gc.ownerToDependents[owner.UID] = append(gc.ownerToDependents[owner.UID][:i], gc.ownerToDependents[owner.UID][i+1:]...)
							break
						}
					}
				}
				gc.ownerToDependentsLock.Unlock()

				ownerGVK := schema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind)
				mapping, err := gc.restMapper.RESTMapping(ownerGVK.GroupKind(), ownerGVK.Version)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("cannot get mapping for %v: %v", ownerGVK, err))
				}
				if _, exist := gc.owners[mapping.Resource]; exist {
					gc.ownersLock.Lock()
					gc.owners[mapping.Resource] = gc.owners[mapping.Resource] - 1
					if gc.owners[mapping.Resource] == 0 {
						removedOwnerResource = append(removedOwnerResource, mapping.Resource)
						delete(gc.owners, mapping.Resource)
					}
					gc.ownersLock.Unlock()
				}
			}
		}

		// sync monitors
		logger := klog.FromContext(ctx)
		if len(addedOwnerResource) > 0 || len(removedOwnerResource) > 0 {
			if err := gc.syncMonitors(ctx, logger, addedOwnerResource, removedOwnerResource); err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to sync monitors: %v", err))
			}
		}
	} else {
		// only handle delete event for owner resources
		if event.eventType == deleteEvent {
			gc.ownerToDependentsLock.RLock()
			dependents, exist := gc.ownerToDependents[accessor.GetUID()]
			gc.ownerToDependentsLock.RUnlock()
			if exist {
				if hasFinalizer(accessor, metav1.FinalizerDeleteDependents) || hasAnnotationKey(accessor, "test.com/forgroundfeletion") {
					// forground deletion of dependents
					for _, namespacedName := range dependents {
						gc.attemptToDelete.Add(&dependent{ownerUID: accessor.GetUID(), namespacedName: namespacedName})
					}
				} else if hasFinalizer(accessor, metav1.FinalizerOrphanDependents) || hasAnnotationKey(accessor, "test.com/orphan") {
					// orphan dependents
					for _, namespacedName := range dependents {
						gc.attemptToOrphan.Add(&dependent{ownerUID: accessor.GetUID(), namespacedName: namespacedName})
					}
				} else {
					// default: background deletion of dependents
					for _, namespacedName := range dependents {
						gc.attemptToDelete.Add(&dependent{ownerUID: accessor.GetUID(), namespacedName: namespacedName})
					}
				}

			}
		}
	}

	return true
}

func hasFinalizer(accessor metav1.Object, matchingFinalizer string) bool {
	finalizers := accessor.GetFinalizers()
	for _, finalizer := range finalizers {
		if finalizer == matchingFinalizer {
			return true
		}
	}
	return false
}

func hasAnnotationKey(accessor metav1.Object, annotationKey string) bool {
	annotations := accessor.GetAnnotations()
	for key := range annotations {
		if key == annotationKey {
			return true
		}
	}
	return false
}

// func (gc *GarbageCollector) addFinalizerForOwner(ctx context.Context, logger klog.Logger, owner metav1.OwnerReference) {
// 	ownerGVK := schema.FromAPIVersionAndKind(owner.APIVersion, owner.Kind)
// 	mapping, err := gc.restMapper.RESTMapping(ownerGVK.GroupKind(), ownerGVK.Version)
// 	if err != nil {
// 		utilruntime.HandleError(fmt.Errorf("cannot get mapping for %v: %v", ownerGVK, err))
// 		return
// 	}
// }

// syncMonitors adds/removes the monitor set according to the supplied added/removed resources,
// creating or deleting monitors as necessary. It will return any error encountered, but will
// make an attempt to create a monitor for each resource instead of immediately exiting on an error.
// It may be called before or after gc.Run().
func (gc *GarbageCollector) syncMonitors(ctx context.Context, logger klog.Logger, addedOwnerResource, removedOwnerResource []schema.GroupVersionResource) error {
	gc.monitorLock.Lock()
	// Ensure workers are paused to avoid processing events before informers
	// have resynced.
	gc.workerLock.Lock()
	defer gc.workerLock.Unlock()

	logger.Info("Syncing monitors")
	monitors := gc.monitors
	errs := []error{}
	started := 0
	for _, resource := range addedOwnerResource {
		if _, exist := monitors[resource]; !exist {
			kind, err := gc.restMapper.KindFor(resource)
			if err != nil {
				errs = append(errs, fmt.Errorf("couldn't look up resource %q: %v", resource, err))
				continue
			}
			c, err := gc.controllerFor(logger, resource, kind)
			if err != nil {
				errs = append(errs, fmt.Errorf("couldn't start monitor for resource %q: %v", resource, err))
				continue
			}
			logger.Info("Starting new monitor", "GVR", resource)
			monitor := &monitor{controller: c, stopCh: make(chan struct{})}
			gc.sharedInformers.Start(ctx.Done())
			go monitor.Run()
			started++
			monitors[resource] = monitor
		}
	}
	logger.Info("Started monitors", "started", started, "total", len(monitors))

	if utilerrors.NewAggregate(errs) != nil {
		return utilerrors.NewAggregate(errs)
	}

	stopped := 0
	for _, resource := range removedOwnerResource {
		if monitor, exist := monitors[resource]; exist {
			if monitor.stopCh != nil {
				logger.Info("Stopping monitor", "GVR", resource)
				stopped++
				close(monitor.stopCh)
			}
			delete(monitors, resource)
		}
	}
	logger.Info("Stopped monitors", "stoped", stopped, "total", len(monitors))

	// update monitors
	gc.monitors = monitors

	logger.Info("Syncing monitors", "added", len(addedOwnerResource), "removed", len(removedOwnerResource))
	gc.monitorLock.Unlock()

	if !cache.WaitForNamedCacheSync("garbage collector", ctx.Done(), func() bool {
		return gc.HasSynced(logger)
	}) {
		return fmt.Errorf("timed out waiting for caches to sync")
	}

	logger.Info("All resource monitors have synced, proceeding to collect garbage")
	return nil
}

func (gc *GarbageCollector) controllerFor(logger klog.Logger, resource schema.GroupVersionResource, kind schema.GroupVersionKind) (cache.Controller, error) {
	logger.Info("Initializing new controller", "resource", resource)
	handlers := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// event := &event{
			// 	eventType: addEvent,
			// 	obj:       obj,
			// 	gvk:       kind,
			// }
			// logger.Info("owner added")
			// gc.ownerChanges.Add(event)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// TODO: check if there are differences in the ownerRefs,
			// finalizers, and DeletionTimestamp; if not, ignore the update.
			// event := &event{
			// 	eventType: updateEvent,
			// 	obj:       newObj,
			// 	oldObj:    oldObj,
			// 	gvk:       kind,
			// }
			// logger.Info("owner updated")
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
				gvk:       kind,
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
				logger.Info("Owner deleted", "GVR", resource, "namespace", accessor.GetNamespace(), "name", accessor.GetName())
			}
		},
	}

	shared, err := gc.sharedInformers.ForResource(resource)
	if err != nil {
		logger.V(4).Error(err, "unable to use a shared informer", "resource", resource, "kind", kind)
		return nil, err
	}
	logger.V(4).Info("using a shared informer", "resource", resource, "kind", kind)
	// need to clone because it's from a shared cache
	shared.Informer().AddEventHandlerWithResyncPeriod(handlers, 0)
	return shared.Informer().GetController(), nil
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

// HasSynced returns true if any monitors exist AND all those monitors'
// controllers HasSynced functions return true. This means HasSynced could return
// true at one time, and then later return false if all monitors were
// reconstructed.
func (gc *GarbageCollector) HasSynced(logger klog.Logger) bool {
	gc.monitorLock.RLock()
	defer gc.monitorLock.RUnlock()

	if len(gc.monitors) == 0 {
		logger.V(4).Info("garbage controller monitor not synced: no monitors")
		return false
	}

	for resource, monitor := range gc.monitors {
		if !monitor.controller.HasSynced() {
			logger.V(4).Info("garbage controller monitor not yet synced", "resource", resource)
			return false
		}
	}

	return true
}

func (gc *GarbageCollector) runAttemptToDeleteWorker(ctx context.Context) {
	for gc.processAttemptToDeleteWorker(ctx) {
	}
}

func (gc *GarbageCollector) runAttemptToOrphanWorker(ctx context.Context) {
	for gc.processAttemptToOrphanWorker(ctx) {
	}
}

func (gc *GarbageCollector) processAttemptToDeleteWorker(ctx context.Context) bool {
	item, quit := gc.attemptToDelete.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
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

func (gc *GarbageCollector) attemptToDeleteWorker(ctx context.Context, item interface{}) workQueueItemAction {
	return gc.attemptToDeleteOrOrphan(ctx, item, false)
}

func (gc *GarbageCollector) processAttemptToOrphanWorker(ctx context.Context) bool {
	item, quit := gc.attemptToOrphan.Get()
	gc.workerLock.RLock()
	defer gc.workerLock.RUnlock()
	if quit {
		return false
	}
	defer gc.attemptToOrphan.Done(item)

	action := gc.attemptToOrphanWorker(ctx, item)
	switch action {
	case forgetItem:
		gc.attemptToOrphan.Forget(item)
	case requeueItem:
		gc.attemptToOrphan.AddRateLimited(item)
	}

	return true
}

func (gc *GarbageCollector) attemptToOrphanWorker(ctx context.Context, item interface{}) workQueueItemAction {
	return gc.attemptToDeleteOrOrphan(ctx, item, true)
}

type workQueueItemAction int

const (
	requeueItem = iota
	forgetItem
)

func (gc *GarbageCollector) attemptToDeleteOrOrphan(ctx context.Context, item interface{}, orphan bool) workQueueItemAction {
	dep, ok := item.(*dependent)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect a *dependent, got %v", item))
		return forgetItem
	}

	logger := klog.FromContext(ctx)
	if orphan {
		logger.Info("Attempting to orphan manifestwork", "ownerUID", dep.ownerUID, "namespacedName", dep.namespacedName)
	} else {
		logger.Info("Attempting to delete manifestwork", "ownerUID", dep.ownerUID, "namespacedName", dep.namespacedName)
	}

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
		// TODO: use strategic merge patch to patch the owner references, if it's not supported, use JSON merge patch instead
		// smp, err := generateDeleteOwnerRefStrategicMergeBytes(latest.GetUID(), dep.ownerUID)
		// if err != nil {
		// 	logger.Info("Filed to generate strategic merge patch", "error", err)
		// 	return requeueItem
		// }

		// if _, err := gc.patchManifestwork(ctx, dep.namespacedName, smp, types.StrategicMergePatchType); err != nil {
		// 	if errors.IsUnsupportedMediaType(err) {
		// 		logger.Info("Strategic merge patch is not supported, using JSON merge patch instead")
		// 		// StrategicMergePatch is not supported, use JSON merge patch instead
		// 		jmp, err := generateDeleteOwnerRefJSONMergePatch(latest, dep.ownerUID)
		// 		if err != nil {
		// 			logger.Info("Failed to generate JSON merge patch", "error", err)
		// 			return requeueItem
		// 		}
		// 		if _, err = gc.patchManifestwork(ctx, dep.namespacedName, jmp, types.MergePatchType); err != nil {
		// 			logger.Info("Failed to patch manifestwork with json patch", "error", err)
		// 			return requeueItem
		// 		}
		// 	}
		// }

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

		// if the deleted owner reference is the only owner reference and the owner deletion policy is not orphan,
		// then delete the manifestwork
		if len(ownerReferences) == 1 && !orphan {
			if err := gc.deleteManifestwork(ctx, dep.namespacedName); err != nil {
				return requeueItem
			}
		}
	}

	return forgetItem
}

func (gc *GarbageCollector) manifestWorkMonitor(logger klog.Logger) *monitor {
	kind := workapiv1.SchemeGroupVersion.WithKind("ManifestWork")
	handlers := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			event := &event{
				eventType: addEvent,
				obj:       obj,
				gvk:       kind,
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
				gvk:       kind,
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
				gvk:       kind,
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
	return &monitor{
		controller: gc.workInformer.Informer().GetController(),
	}
}
