package garbagecollector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"

	addonapiv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
	workclientset "open-cluster-management.io/api/client/work/clientset/versioned"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workapiv1 "open-cluster-management.io/api/work/v1"
)

func createEvent(eventType eventType, namespace, name string, newOwners, oldOwners []string) *event {
	var newOwnerReferences []metav1.OwnerReference
	var oldOwnerReferences []metav1.OwnerReference
	for i := 0; i < len(newOwners); i++ {
		newOwnerReferences = append(newOwnerReferences, metav1.OwnerReference{UID: types.UID(newOwners[i])})
	}
	for i := 0; i < len(oldOwners); i++ {
		oldOwnerReferences = append(oldOwnerReferences, metav1.OwnerReference{UID: types.UID(oldOwners[i])})
	}

	return &event{
		eventType: eventType,
		obj: &workapiv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       namespace,
				OwnerReferences: newOwnerReferences,
			},
		},
		oldObj: &workapiv1.ManifestWork{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       namespace,
				OwnerReferences: oldOwnerReferences,
			},
		},
		gvr: workapiv1.GroupVersion.WithResource("manifestworks"),
	}
}

func TestProcessOwnerChangesEvent(t *testing.T) {
	cases := []struct {
		name string
		// a series of events that will be supplied to the ownerChanges queue
		events                  []*event
		expectedUIDToDependents map[types.UID][]types.NamespacedName
	}{
		{
			name: "test1",
			events: []*event{
				createEvent(addEvent, "ns1", "work1", []string{}, []string{}),
				createEvent(addEvent, "ns1", "work2", []string{"1"}, []string{}),
				createEvent(addEvent, "ns1", "work3", []string{"1", "2"}, []string{}),
				createEvent(updateEvent, "ns1", "work1", []string{"1"}, []string{}),
				createEvent(updateEvent, "ns1", "work2", []string{"1", "3"}, []string{"1"}),
				createEvent(deleteEvent, "ns1", "work3", []string{"1", "2"}, []string{}),
			},
			expectedUIDToDependents: map[types.UID][]types.NamespacedName{
				"1": {types.NamespacedName{Name: "work2", Namespace: "ns1"}, types.NamespacedName{Name: "work1", Namespace: "ns1"}},
				"3": {types.NamespacedName{Name: "work2", Namespace: "ns1"}},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc := setupGC(t, &rest.Config{})
	go gc.runProcessOwnerChangesWorker(ctx)
	for _, testCase := range cases {
		for _, event := range testCase.events {
			gc.ownerChanges.Add(event)
		}
		time.Sleep(1 * time.Second)
		gc.ownerToDependentsLock.RLock()
		if !reflect.DeepEqual(gc.ownerToDependents, testCase.expectedUIDToDependents) {
			t.Errorf("expected %v but got %v", testCase.expectedUIDToDependents, gc.ownerToDependents)
		}
		gc.ownerToDependentsLock.RUnlock()
	}
}

// fakeAction records information about requests to aid in testing.
type fakeAction struct {
	method string
	path   string
	query  string
}

// String returns method=path to aid in testing
func (f *fakeAction) String() string {
	return strings.Join([]string{f.method, f.path}, "=")
}

type FakeResponse struct {
	statusCode int
	content    []byte
}

// fakeActionHandler holds a list of fakeActions received
type fakeActionHandler struct {
	// statusCode and content returned by this handler for different method + path.
	response map[string]FakeResponse

	lock    sync.Mutex
	actions []fakeAction
}

// ServeHTTP logs the action that occurred and always returns the associated status code
func (f *fakeActionHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	func() {
		f.lock.Lock()
		defer f.lock.Unlock()

		f.actions = append(f.actions, fakeAction{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery})
		fakeResponse, ok := f.response[request.Method+request.URL.Path]
		if !ok {
			fakeResponse.statusCode = 200
			fakeResponse.content = []byte(`{"apiVersion": "v1", "kind": "List"}`)
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(fakeResponse.statusCode)
		_, err := response.Write(fakeResponse.content)
		if err != nil {
			return
		}
	}()

	// This is to allow the fakeActionHandler to simulate a watch being opened
	if strings.Contains(request.URL.RawQuery, "watch=true") {
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		time.Sleep(30 * time.Second)
	}
}

// testServerAndClientConfig returns a server that listens and a config that can reference it
func testServerAndClientConfig(handler func(http.ResponseWriter, *http.Request)) (*httptest.Server, *rest.Config) {
	srv := httptest.NewServer(http.HandlerFunc(handler))
	config := &rest.Config{
		Host: srv.URL,
	}
	return srv, config
}

func setupGC(t *testing.T, config *rest.Config) *GarbageCollector {
	workClient, err := workclientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	workInformer := workinformers.NewSharedInformerFactory(workClient, 0)

	listOptions := &metav1.ListOptions{
		LabelSelector: "test=test",
		FieldSelector: "metadata.name=test",
	}
	ownerGVRFilters := map[schema.GroupVersionResource]*metav1.ListOptions{
		addonapiv1alpha1.SchemeGroupVersion.WithResource("managedclusteraddons"): listOptions,
	}

	return NewGarbageCollector(workClient.WorkV1(), metadataClient, workInformer.Work().V1().ManifestWorks(), ownerGVRFilters)
}

func getWork(workName, workNamespace, workUID string, ownerReferences []metav1.OwnerReference) *workapiv1.ManifestWork {
	return &workapiv1.ManifestWork{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ManifestWork",
			APIVersion: "work.open-cluster-management.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            workName,
			Namespace:       workNamespace,
			UID:             types.UID(workUID),
			OwnerReferences: ownerReferences,
		},
	}
}

func serilizeOrDie(t *testing.T, object interface{}) []byte {
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestAttemptToDeleteItem(t *testing.T) {
	workName := "ToBeDeletedWork"
	workNamespace := "ns1"
	workUID := "123"
	ownerUID := "456"
	ownerName := "addon1"
	work := getWork(workName, workNamespace, workUID, []metav1.OwnerReference{
		{
			APIVersion: "addon.open-cluster-management.io/v1alpha1",
			Kind:       "ManagedClusterAddon",
			Name:       ownerName,
			UID:        types.UID(ownerUID),
		},
	})
	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/apis/addon.open-cluster-management.io/v1alpha1/namespaces/ns1/managedclusteraddons/addon1": {
				404,
				[]byte{},
			},
			"GET" + "/apis/work.open-cluster-management.io/v1/namespaces/ns1/manifestworks/ToBeDeletedWork": {
				200,
				serilizeOrDie(t, work),
			},
			"PATCH" + "/apis/work.open-cluster-management.io/v1/namespaces/ns1/manifestworks/ToBeDeletedWork": {
				200,
				serilizeOrDie(t, work),
			},
		},
	}
	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc := setupGC(t, clientConfig)
	item := &dependent{
		ownerUID: types.UID(ownerUID),
		namespacedName: types.NamespacedName{
			Name:      workName,
			Namespace: workNamespace,
		},
	}
	itemAction := gc.attemptToDeleteWorker(ctx, item)
	if itemAction != forgetItem {
		t.Errorf("attemptToDeleteWorker returned unexpected action: %v", itemAction)
	}
	expectedActionSet := sets.NewString()
	expectedActionSet.Insert("GET=/apis/work.open-cluster-management.io/v1/namespaces/ns1/manifestworks/ToBeDeletedWork")
	expectedActionSet.Insert("PATCH=/apis/work.open-cluster-management.io/v1/namespaces/ns1/manifestworks/ToBeDeletedWork")
	expectedActionSet.Insert("DELETE=/apis/work.open-cluster-management.io/v1/namespaces/ns1/manifestworks/ToBeDeletedWork")

	actualActionSet := sets.NewString()
	for _, action := range testHandler.actions {
		actualActionSet.Insert(action.String())
	}
	if !expectedActionSet.Equal(actualActionSet) {
		t.Errorf("expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet,
			actualActionSet, expectedActionSet.Difference(actualActionSet))
	}
}
