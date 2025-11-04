package testing

import (
	"fmt"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

var MockEventDataType = types.CloudEventsDataType{
	Group:    "resources.test",
	Version:  "v1",
	Resource: "mockresources",
}

type MockResource struct {
	UID               kubetypes.UID `json:"uid"`
	ResourceVersion   string        `json:"resourceVersion"`
	Generation        int64         `json:"generation"`
	DeletionTimestamp *metav1.Time  `json:"deletionTimestamp,omitempty"`
	Namespace         string
	Spec              string `json:"spec"`
	Status            string `json:"status"`
}

func (r *MockResource) GetUID() kubetypes.UID {
	return r.UID
}

func (r *MockResource) GetResourceVersion() string {
	return r.ResourceVersion
}

func (r *MockResource) GetGeneration() int64 {
	return r.Generation
}

func (r *MockResource) GetDeletionTimestamp() *metav1.Time {
	return r.DeletionTimestamp
}

type MockResourceLister struct {
	resources []*MockResource
}

func NewMockResourceLister(resources ...*MockResource) *MockResourceLister {
	return &MockResourceLister{
		resources: resources,
	}
}

func (l *MockResourceLister) List(opt types.ListOptions) ([]*MockResource, error) {
	return l.resources, nil
}

func StatusHash(r *MockResource) (string, error) {
	return r.Status, nil
}

type MockResourceCodec struct{}

func NewMockResourceCodec() *MockResourceCodec {
	return &MockResourceCodec{}
}

func (c *MockResourceCodec) EventDataType() types.CloudEventsDataType {
	return MockEventDataType
}

func (c *MockResourceCodec) Encode(source string, eventType types.CloudEventsType, obj *MockResource) (*cloudevents.Event, error) {
	evt := cloudevents.NewEvent()
	evt.SetID(uuid.New().String())
	evt.SetSource(source)
	evt.SetType(eventType.String())
	evt.SetTime(time.Now())
	evt.SetExtension("resourceid", string(obj.UID))
	evt.SetExtension("resourceversion", strconv.FormatInt(obj.Generation, 10))
	evt.SetExtension("clustername", obj.Namespace)
	if obj.GetDeletionTimestamp() != nil {
		evt.SetExtension("deletiontimestamp", obj.DeletionTimestamp.Time)
	}
	if err := evt.SetData(cloudevents.TextPlain, obj.Status); err != nil {
		return nil, err
	}
	return &evt, nil
}

func (c *MockResourceCodec) Decode(evt *cloudevents.Event) (*MockResource, error) {
	resourceID, err := evt.Context.GetExtension("resourceid")
	if err != nil {
		return nil, fmt.Errorf("failed to get resource ID: %v", err)
	}

	resourceVersion, err := evt.Context.GetExtension("resourceversion")
	if err != nil {
		return nil, fmt.Errorf("failed to get resource version: %v", err)
	}

	clusterName, err := evt.Context.GetExtension("clustername")
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster name: %v", err)
	}

	generation, err := strconv.ParseInt(fmt.Sprintf("%s", resourceVersion), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse resource generation: %v", err)
	}

	res := &MockResource{
		UID:             kubetypes.UID(fmt.Sprintf("%s", resourceID)),
		ResourceVersion: fmt.Sprintf("%s", resourceVersion),
		Namespace:       fmt.Sprintf("%s", clusterName),
		Generation:      generation,
		Status:          string(evt.Data()),
	}

	deletionTimestamp, err := evt.Context.GetExtension("deletiontimestamp")
	if err == nil {
		timestamp, err := time.Parse(time.RFC3339, fmt.Sprintf("%s", deletionTimestamp))
		if err != nil {
			return nil, fmt.Errorf("failed to parse deletiontimestamp - %v to time.Time", deletionTimestamp)
		}
		res.DeletionTimestamp = &metav1.Time{Time: timestamp}
	}

	return res, nil
}
