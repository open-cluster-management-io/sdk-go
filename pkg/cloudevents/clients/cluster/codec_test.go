package cluster

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "open-cluster-management.io/api/cluster/v1"

	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestManagedClusterCodec_EventDataType(t *testing.T) {
	codec := NewManagedClusterCodec()
	dataType := codec.EventDataType()

	if dataType.Group != clusterv1.GroupVersion.Group {
		t.Errorf("expected Group %s, got %s", clusterv1.GroupVersion.Group, dataType.Group)
	}
	if dataType.Version != clusterv1.GroupVersion.Version {
		t.Errorf("expected Version %s, got %s", clusterv1.GroupVersion.Version, dataType.Version)
	}
	if dataType.Resource != "managedclusters" {
		t.Errorf("expected Resource managedclusters, got %s", dataType.Resource)
	}
}

func TestManagedClusterCodec_Encode(t *testing.T) {
	codec := NewManagedClusterCodec()
	source := "test-source"
	clusterName := "test-cluster"
	clusterUID := types.UID("test-cluster-uid")

	cluster := &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			UID:             clusterUID,
			Name:            clusterName,
			ResourceVersion: "123",
		},
		Spec: clusterv1.ManagedClusterSpec{
			HubAcceptsClient: true,
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: ManagedClusterEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if evt == nil {
		t.Fatal("expected non-nil cloud event")
	}

	if evt.Source() != source {
		t.Errorf("expected source %s, got %s", source, evt.Source())
	}

	resourceID, ok := evt.Extensions()["resourceid"]
	if !ok {
		t.Error("expected resourceid extension")
	}
	if resourceID != string(clusterUID) {
		t.Errorf("expected resourceid %s, got %v", clusterUID, resourceID)
	}

	eventClusterName, ok := evt.Extensions()["clustername"]
	if !ok {
		t.Error("expected clustername extension")
	}
	if eventClusterName != clusterName {
		t.Errorf("expected clustername %s, got %v", clusterName, eventClusterName)
	}
}

func TestManagedClusterCodec_Encode_WithDeletionTimestamp(t *testing.T) {
	codec := NewManagedClusterCodec()
	source := "test-source"
	clusterName := "test-cluster"
	clusterUID := types.UID("test-cluster-uid")
	now := metav1.Now()

	cluster := &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			UID:               clusterUID,
			Name:              clusterName,
			ResourceVersion:   "123",
			DeletionTimestamp: &now,
		},
		Spec: clusterv1.ManagedClusterSpec{
			HubAcceptsClient: true,
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: ManagedClusterEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.DeleteRequestAction,
	}

	evt, err := codec.Encode(source, eventType, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := evt.Extensions()["deletiontimestamp"]; !ok {
		t.Error("expected deletiontimestamp extension")
	}

	if len(evt.Data()) != 0 {
		t.Error("expected empty data for deletion event")
	}
}

func TestManagedClusterCodec_Encode_UnsupportedDataType(t *testing.T) {
	codec := NewManagedClusterCodec()
	source := "test-source"

	cluster := &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster",
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: cetypes.CloudEventsDataType{
			Group:    "wrong",
			Version:  "v1",
			Resource: "wrong",
		},
		SubResource: cetypes.SubResourceSpec,
		Action:      cetypes.CreateRequestAction,
	}

	_, err := codec.Encode(source, eventType, cluster)
	if err == nil {
		t.Error("expected error for unsupported data type, got nil")
	}
}

func TestManagedClusterCodec_Decode(t *testing.T) {
	codec := NewManagedClusterCodec()
	source := "test-source"
	clusterName := "test-cluster"
	clusterUID := types.UID("test-cluster-uid")

	cluster := &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			UID:             clusterUID,
			Name:            clusterName,
			ResourceVersion: "123",
		},
		Spec: clusterv1.ManagedClusterSpec{
			HubAcceptsClient: true,
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: ManagedClusterEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, cluster)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	decoded, err := codec.Decode(evt)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.UID != clusterUID {
		t.Errorf("expected UID %s, got %s", clusterUID, decoded.UID)
	}
	if decoded.Name != clusterName {
		t.Errorf("expected Name %s, got %s", clusterName, decoded.Name)
	}
	if decoded.Spec.HubAcceptsClient != true {
		t.Errorf("expected HubAcceptsClient true, got %v", decoded.Spec.HubAcceptsClient)
	}
}

func TestManagedClusterCodec_RoundTrip(t *testing.T) {
	codec := NewManagedClusterCodec()
	source := "test-source"

	tests := []struct {
		name    string
		cluster *clusterv1.ManagedCluster
	}{
		{
			name: "simple cluster",
			cluster: &clusterv1.ManagedCluster{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-1"),
					Name:            "cluster-1",
					ResourceVersion: "100",
				},
				Spec: clusterv1.ManagedClusterSpec{
					HubAcceptsClient: true,
				},
			},
		},
		{
			name: "cluster with labels and annotations",
			cluster: &clusterv1.ManagedCluster{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-2"),
					Name:            "cluster-2",
					ResourceVersion: "200",
					Labels: map[string]string{
						"cloud": "aws",
					},
					Annotations: map[string]string{
						"region": "us-east-1",
					},
				},
				Spec: clusterv1.ManagedClusterSpec{
					HubAcceptsClient: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventType := cetypes.CloudEventsType{
				CloudEventsDataType: ManagedClusterEventDataType,
				SubResource:         cetypes.SubResourceSpec,
				Action:              cetypes.CreateRequestAction,
			}

			evt, err := codec.Encode(source, eventType, tt.cluster)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := codec.Decode(evt)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if decoded.UID != tt.cluster.UID {
				t.Errorf("UID mismatch: expected %s, got %s", tt.cluster.UID, decoded.UID)
			}
			if decoded.Name != tt.cluster.Name {
				t.Errorf("Name mismatch: expected %s, got %s", tt.cluster.Name, decoded.Name)
			}
			if decoded.Spec.HubAcceptsClient != tt.cluster.Spec.HubAcceptsClient {
				t.Errorf("HubAcceptsClient mismatch: expected %v, got %v", tt.cluster.Spec.HubAcceptsClient, decoded.Spec.HubAcceptsClient)
			}
		})
	}
}
