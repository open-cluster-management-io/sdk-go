package csr

import (
	"testing"

	certificatev1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "open-cluster-management.io/api/cluster/v1"

	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestCSRCodec_EventDataType(t *testing.T) {
	codec := NewCSRCodec()
	dataType := codec.EventDataType()

	if dataType.Group != certificatev1.SchemeGroupVersion.Group {
		t.Errorf("expected Group %s, got %s", certificatev1.SchemeGroupVersion.Group, dataType.Group)
	}
	if dataType.Version != certificatev1.SchemeGroupVersion.Version {
		t.Errorf("expected Version %s, got %s", certificatev1.SchemeGroupVersion.Version, dataType.Version)
	}
	if dataType.Resource != "certificatesigningrequests" {
		t.Errorf("expected Resource certificatesigningrequests, got %s", dataType.Resource)
	}
}

func TestCSRCodec_Encode(t *testing.T) {
	codec := NewCSRCodec()
	source := "test-source"
	clusterName := "test-cluster"
	csrUID := types.UID("test-csr-uid")

	csr := &certificatev1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID:             csrUID,
			Name:            "test-csr",
			ResourceVersion: "123",
			Labels: map[string]string{
				clusterv1.ClusterNameLabelKey: clusterName,
			},
		},
		Spec: certificatev1.CertificateSigningRequestSpec{
			Request: []byte("test-request"),
			Usages: []certificatev1.KeyUsage{
				certificatev1.UsageDigitalSignature,
			},
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: CSREventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, csr)
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
	if resourceID != string(csrUID) {
		t.Errorf("expected resourceid %s, got %v", csrUID, resourceID)
	}

	eventClusterName, ok := evt.Extensions()["clustername"]
	if !ok {
		t.Error("expected clustername extension")
	}
	if eventClusterName != clusterName {
		t.Errorf("expected clustername %s, got %v", clusterName, eventClusterName)
	}
}

func TestCSRCodec_Encode_NoClusterLabel(t *testing.T) {
	codec := NewCSRCodec()
	source := "test-source"
	csrUID := types.UID("test-csr-uid")

	csr := &certificatev1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID:             csrUID,
			Name:            "test-csr",
			ResourceVersion: "123",
		},
		Spec: certificatev1.CertificateSigningRequestSpec{
			Request: []byte("test-request"),
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: CSREventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	_, err := codec.Encode(source, eventType, csr)
	if err == nil {
		t.Error("expected error for missing cluster label, got nil")
	}
}

func TestCSRCodec_Encode_WithDeletionTimestamp(t *testing.T) {
	codec := NewCSRCodec()
	source := "test-source"
	clusterName := "test-cluster"
	csrUID := types.UID("test-csr-uid")
	now := metav1.Now()

	csr := &certificatev1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID:             csrUID,
			Name:            "test-csr",
			ResourceVersion: "123",
			Labels: map[string]string{
				clusterv1.ClusterNameLabelKey: clusterName,
			},
			DeletionTimestamp: &now,
		},
		Spec: certificatev1.CertificateSigningRequestSpec{
			Request: []byte("test-request"),
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: CSREventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.DeleteRequestAction,
	}

	evt, err := codec.Encode(source, eventType, csr)
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

func TestCSRCodec_Encode_UnsupportedDataType(t *testing.T) {
	codec := NewCSRCodec()
	source := "test-source"

	csr := &certificatev1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-csr",
			Labels: map[string]string{
				clusterv1.ClusterNameLabelKey: "test-cluster",
			},
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

	_, err := codec.Encode(source, eventType, csr)
	if err == nil {
		t.Error("expected error for unsupported data type, got nil")
	}
}

func TestCSRCodec_Decode(t *testing.T) {
	codec := NewCSRCodec()
	source := "test-source"
	clusterName := "test-cluster"
	csrUID := types.UID("test-csr-uid")

	csr := &certificatev1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID:             csrUID,
			Name:            "test-csr",
			ResourceVersion: "123",
			Labels: map[string]string{
				clusterv1.ClusterNameLabelKey: clusterName,
			},
		},
		Spec: certificatev1.CertificateSigningRequestSpec{
			Request: []byte("test-request"),
			Usages: []certificatev1.KeyUsage{
				certificatev1.UsageDigitalSignature,
			},
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: CSREventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, csr)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	decoded, err := codec.Decode(evt)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.UID != csrUID {
		t.Errorf("expected UID %s, got %s", csrUID, decoded.UID)
	}
	if decoded.Name != "test-csr" {
		t.Errorf("expected Name test-csr, got %s", decoded.Name)
	}
	if string(decoded.Spec.Request) != "test-request" {
		t.Errorf("expected Request test-request, got %s", string(decoded.Spec.Request))
	}
}

func TestCSRCodec_RoundTrip(t *testing.T) {
	codec := NewCSRCodec()
	source := "test-source"

	tests := []struct {
		name string
		csr  *certificatev1.CertificateSigningRequest
	}{
		{
			name: "simple csr",
			csr: &certificatev1.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-1"),
					Name:            "csr-1",
					ResourceVersion: "100",
					Labels: map[string]string{
						clusterv1.ClusterNameLabelKey: "cluster-1",
					},
				},
				Spec: certificatev1.CertificateSigningRequestSpec{
					Request: []byte("request-1"),
					Usages: []certificatev1.KeyUsage{
						certificatev1.UsageDigitalSignature,
					},
				},
			},
		},
		{
			name: "csr with multiple usages",
			csr: &certificatev1.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					UID:             types.UID("uid-2"),
					Name:            "csr-2",
					ResourceVersion: "200",
					Labels: map[string]string{
						clusterv1.ClusterNameLabelKey: "cluster-2",
						"extra":                       "label",
					},
				},
				Spec: certificatev1.CertificateSigningRequestSpec{
					Request: []byte("request-2"),
					Usages: []certificatev1.KeyUsage{
						certificatev1.UsageDigitalSignature,
						certificatev1.UsageKeyEncipherment,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventType := cetypes.CloudEventsType{
				CloudEventsDataType: CSREventDataType,
				SubResource:         cetypes.SubResourceSpec,
				Action:              cetypes.CreateRequestAction,
			}

			evt, err := codec.Encode(source, eventType, tt.csr)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := codec.Decode(evt)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			if decoded.UID != tt.csr.UID {
				t.Errorf("UID mismatch: expected %s, got %s", tt.csr.UID, decoded.UID)
			}
			if decoded.Name != tt.csr.Name {
				t.Errorf("Name mismatch: expected %s, got %s", tt.csr.Name, decoded.Name)
			}
			if string(decoded.Spec.Request) != string(tt.csr.Spec.Request) {
				t.Errorf("Request mismatch: expected %s, got %s", string(tt.csr.Spec.Request), string(decoded.Spec.Request))
			}
		})
	}
}
