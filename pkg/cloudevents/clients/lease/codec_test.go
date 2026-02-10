package lease

import (
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestLeaseCodec_EventDataType(t *testing.T) {
	codec := NewLeaseCodec()
	dataType := codec.EventDataType()

	if dataType.Group != coordinationv1.SchemeGroupVersion.Group {
		t.Errorf("expected Group %s, got %s", coordinationv1.SchemeGroupVersion.Group, dataType.Group)
	}
	if dataType.Version != coordinationv1.SchemeGroupVersion.Version {
		t.Errorf("expected Version %s, got %s", coordinationv1.SchemeGroupVersion.Version, dataType.Version)
	}
	if dataType.Resource != "leases" {
		t.Errorf("expected Resource leases, got %s", dataType.Resource)
	}
}

func TestLeaseCodec_Encode(t *testing.T) {
	codec := NewLeaseCodec()
	source := "test-source"
	clusterName := "test-cluster"
	leaseUID := types.UID("test-lease-uid")
	now := metav1.NewMicroTime(time.Now())

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			UID:             leaseUID,
			Name:            "test-lease",
			Namespace:       clusterName,
			ResourceVersion: "123",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: func() *string { s := "holder-1"; return &s }(),
			RenewTime:      &now,
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: LeaseEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, lease)
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
	if resourceID != string(leaseUID) {
		t.Errorf("expected resourceid %s, got %v", leaseUID, resourceID)
	}

	eventClusterName, ok := evt.Extensions()["clustername"]
	if !ok {
		t.Error("expected clustername extension")
	}
	if eventClusterName != clusterName {
		t.Errorf("expected clustername %s, got %v", clusterName, eventClusterName)
	}
}

func TestLeaseCodec_Encode_WithDeletionTimestamp(t *testing.T) {
	codec := NewLeaseCodec()
	source := "test-source"
	clusterName := "test-cluster"
	leaseUID := types.UID("test-lease-uid")
	deletionTime := metav1.Now()

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			UID:               leaseUID,
			Name:              "test-lease",
			Namespace:         clusterName,
			ResourceVersion:   "123",
			DeletionTimestamp: &deletionTime,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: func() *string { s := "holder-1"; return &s }(),
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: LeaseEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.DeleteRequestAction,
	}

	evt, err := codec.Encode(source, eventType, lease)
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

func TestLeaseCodec_Encode_UnsupportedDataType(t *testing.T) {
	codec := NewLeaseCodec()
	source := "test-source"

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-lease",
			Namespace: "test-cluster",
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

	_, err := codec.Encode(source, eventType, lease)
	if err == nil {
		t.Error("expected error for unsupported data type, got nil")
	}
}

func TestLeaseCodec_Decode(t *testing.T) {
	codec := NewLeaseCodec()
	source := "test-source"
	clusterName := "test-cluster"
	leaseUID := types.UID("test-lease-uid")
	now := metav1.NewMicroTime(time.Now())

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			UID:             leaseUID,
			Name:            "test-lease",
			Namespace:       clusterName,
			ResourceVersion: "123",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: func() *string { s := "holder-1"; return &s }(),
			RenewTime:      &now,
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: LeaseEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, lease)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	decoded, err := codec.Decode(evt)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.UID != leaseUID {
		t.Errorf("expected UID %s, got %s", leaseUID, decoded.UID)
	}
	if decoded.Name != "test-lease" {
		t.Errorf("expected Name test-lease, got %s", decoded.Name)
	}
	if decoded.Namespace != clusterName {
		t.Errorf("expected Namespace %s, got %s", clusterName, decoded.Namespace)
	}
	if *decoded.Spec.HolderIdentity != "holder-1" {
		t.Errorf("expected HolderIdentity holder-1, got %s", *decoded.Spec.HolderIdentity)
	}
}

func TestLeaseCodec_RoundTrip(t *testing.T) {
	codec := NewLeaseCodec()
	source := "test-source"
	now := metav1.NewMicroTime(time.Now())

	tests := []struct {
		name  string
		lease *coordinationv1.Lease
	}{
		{
			name: "simple lease",
			lease: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-1"),
					Name:            "lease-1",
					Namespace:       "cluster-1",
					ResourceVersion: "100",
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity: func() *string { s := "holder-1"; return &s }(),
					RenewTime:      &now,
				},
			},
		},
		{
			name: "lease with lease duration",
			lease: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-2"),
					Name:            "lease-2",
					Namespace:       "cluster-2",
					ResourceVersion: "200",
					Labels: map[string]string{
						"type": "heartbeat",
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       func() *string { s := "holder-2"; return &s }(),
					LeaseDurationSeconds: func() *int32 { i := int32(60); return &i }(),
					RenewTime:            &now,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventType := cetypes.CloudEventsType{
				CloudEventsDataType: LeaseEventDataType,
				SubResource:         cetypes.SubResourceSpec,
				Action:              cetypes.CreateRequestAction,
			}

			evt, err := codec.Encode(source, eventType, tt.lease)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := codec.Decode(evt)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if decoded.UID != tt.lease.UID {
				t.Errorf("UID mismatch: expected %s, got %s", tt.lease.UID, decoded.UID)
			}
			if decoded.Name != tt.lease.Name {
				t.Errorf("Name mismatch: expected %s, got %s", tt.lease.Name, decoded.Name)
			}
			if decoded.Namespace != tt.lease.Namespace {
				t.Errorf("Namespace mismatch: expected %s, got %s", tt.lease.Namespace, decoded.Namespace)
			}
			if *decoded.Spec.HolderIdentity != *tt.lease.Spec.HolderIdentity {
				t.Errorf("HolderIdentity mismatch: expected %s, got %s", *tt.lease.Spec.HolderIdentity, *decoded.Spec.HolderIdentity)
			}
		})
	}
}
