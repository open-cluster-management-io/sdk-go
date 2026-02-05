package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	addonapiv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"

	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestManagedClusterAddOnCodec_EventDataType(t *testing.T) {
	codec := NewManagedClusterAddOnCodec()
	dataType := codec.EventDataType()

	if dataType.Group != addonapiv1alpha1.GroupVersion.Group {
		t.Errorf("expected Group %s, got %s", addonapiv1alpha1.GroupVersion.Group, dataType.Group)
	}
	if dataType.Version != addonapiv1alpha1.GroupVersion.Version {
		t.Errorf("expected Version %s, got %s", addonapiv1alpha1.GroupVersion.Version, dataType.Version)
	}
	if dataType.Resource != "managedclusteraddons" {
		t.Errorf("expected Resource managedclusteraddons, got %s", dataType.Resource)
	}
}

func TestManagedClusterAddOnCodec_Encode(t *testing.T) {
	codec := NewManagedClusterAddOnCodec()
	source := "test-source"
	clusterName := "test-cluster"
	addonUID := types.UID("test-addon-uid")

	addon := &addonapiv1alpha1.ManagedClusterAddOn{
		ObjectMeta: metav1.ObjectMeta{
			UID:             addonUID,
			Name:            "test-addon",
			Namespace:       clusterName,
			ResourceVersion: "123",
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: ManagedClusterAddOnEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, addon)
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
	if resourceID != string(addonUID) {
		t.Errorf("expected resourceid %s, got %v", addonUID, resourceID)
	}

	eventClusterName, ok := evt.Extensions()["clustername"]
	if !ok {
		t.Error("expected clustername extension")
	}
	if eventClusterName != clusterName {
		t.Errorf("expected clustername %s, got %v", clusterName, eventClusterName)
	}
}

func TestManagedClusterAddOnCodec_Encode_WithDeletionTimestamp(t *testing.T) {
	codec := NewManagedClusterAddOnCodec()
	source := "test-source"
	clusterName := "test-cluster"
	addonUID := types.UID("test-addon-uid")
	now := metav1.Now()

	addon := &addonapiv1alpha1.ManagedClusterAddOn{
		ObjectMeta: metav1.ObjectMeta{
			UID:               addonUID,
			Name:              "test-addon",
			Namespace:         clusterName,
			ResourceVersion:   "123",
			DeletionTimestamp: &now,
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: ManagedClusterAddOnEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.DeleteRequestAction,
	}

	evt, err := codec.Encode(source, eventType, addon)
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

func TestManagedClusterAddOnCodec_Encode_UnsupportedDataType(t *testing.T) {
	codec := NewManagedClusterAddOnCodec()
	source := "test-source"

	addon := &addonapiv1alpha1.ManagedClusterAddOn{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-addon",
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

	_, err := codec.Encode(source, eventType, addon)
	if err == nil {
		t.Error("expected error for unsupported data type, got nil")
	}
}

func TestManagedClusterAddOnCodec_Decode(t *testing.T) {
	codec := NewManagedClusterAddOnCodec()
	source := "test-source"
	clusterName := "test-cluster"
	addonUID := types.UID("test-addon-uid")

	addon := &addonapiv1alpha1.ManagedClusterAddOn{
		ObjectMeta: metav1.ObjectMeta{
			UID:             addonUID,
			Name:            "test-addon",
			Namespace:       clusterName,
			ResourceVersion: "123",
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: ManagedClusterAddOnEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, addon)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	decoded, err := codec.Decode(evt)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.UID != addonUID {
		t.Errorf("expected UID %s, got %s", addonUID, decoded.UID)
	}
	if decoded.Name != "test-addon" {
		t.Errorf("expected Name test-addon, got %s", decoded.Name)
	}
	if decoded.Namespace != clusterName {
		t.Errorf("expected Namespace %s, got %s", clusterName, decoded.Namespace)
	}
}

func TestManagedClusterAddOnCodec_RoundTrip(t *testing.T) {
	codec := NewManagedClusterAddOnCodec()
	source := "test-source"

	tests := []struct {
		name  string
		addon *addonapiv1alpha1.ManagedClusterAddOn
	}{
		{
			name: "simple addon",
			addon: &addonapiv1alpha1.ManagedClusterAddOn{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-1"),
					Name:            "addon-1",
					Namespace:       "cluster-1",
					ResourceVersion: "100",
				},
			},
		},
		{
			name: "addon with labels and annotations",
			addon: &addonapiv1alpha1.ManagedClusterAddOn{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-2"),
					Name:            "addon-2",
					Namespace:       "cluster-2",
					ResourceVersion: "200",
					Labels: map[string]string{
						"key1": "value1",
					},
					Annotations: map[string]string{
						"anno1": "annoval1",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventType := cetypes.CloudEventsType{
				CloudEventsDataType: ManagedClusterAddOnEventDataType,
				SubResource:         cetypes.SubResourceSpec,
				Action:              cetypes.CreateRequestAction,
			}

			evt, err := codec.Encode(source, eventType, tt.addon)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := codec.Decode(evt)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if decoded.UID != tt.addon.UID {
				t.Errorf("UID mismatch: expected %s, got %s", tt.addon.UID, decoded.UID)
			}
			if decoded.Name != tt.addon.Name {
				t.Errorf("Name mismatch: expected %s, got %s", tt.addon.Name, decoded.Name)
			}
			if decoded.Namespace != tt.addon.Namespace {
				t.Errorf("Namespace mismatch: expected %s, got %s", tt.addon.Namespace, decoded.Namespace)
			}
		})
	}
}
