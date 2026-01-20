package serviceaccount

import (
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestTokenRequestCodec_EventDataType(t *testing.T) {
	codec := NewTokenRequestCodec()
	dataType := codec.EventDataType()

	if dataType.Group != authenticationv1.GroupName {
		t.Errorf("expected Group %s, got %s", authenticationv1.GroupName, dataType.Group)
	}
	if dataType.Version != "v1" {
		t.Errorf("expected Version v1, got %s", dataType.Version)
	}
	if dataType.Resource != "tokenrequests" {
		t.Errorf("expected Resource tokenrequests, got %s", dataType.Resource)
	}
}

func TestTokenRequestCodec_Encode(t *testing.T) {
	codec := NewTokenRequestCodec()
	source := "test-source"
	clusterName := "test-cluster"
	requestID := types.UID("test-request-id")

	tokenRequest := &authenticationv1.TokenRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID:       requestID,
			Name:      "test-sa",
			Namespace: clusterName,
		},
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{"test-audience"},
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: TokenRequestDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	evt, err := codec.Encode(source, eventType, tokenRequest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if evt == nil {
		t.Fatal("expected non-nil cloud event")
	}

	if evt.Source() != source {
		t.Errorf("expected source %s, got %s", source, evt.Source())
	}

	// Verify the event has the correct resource ID extension
	resourceID, ok := evt.Extensions()["resourceid"]
	if !ok {
		t.Error("expected resourceid extension")
	}
	if resourceID != string(requestID) {
		t.Errorf("expected resourceid %s, got %v", requestID, resourceID)
	}

	// Verify the event has the correct cluster name extension
	eventClusterName, ok := evt.Extensions()["clustername"]
	if !ok {
		t.Error("expected clustername extension")
	}
	if eventClusterName != clusterName {
		t.Errorf("expected clustername %s, got %v", clusterName, eventClusterName)
	}
}

func TestTokenRequestCodec_Encode_UnsupportedDataType(t *testing.T) {
	codec := NewTokenRequestCodec()
	source := "test-source"

	tokenRequest := &authenticationv1.TokenRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sa",
			Namespace: "test-cluster",
		},
	}

	// Use an unsupported data type
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: cetypes.CloudEventsDataType{
			Group:    "wrong",
			Version:  "v1",
			Resource: "wrong",
		},
		SubResource: cetypes.SubResourceSpec,
		Action:      cetypes.CreateRequestAction,
	}

	_, err := codec.Encode(source, eventType, tokenRequest)
	if err == nil {
		t.Error("expected error for unsupported data type, got nil")
	}
}

func TestTokenRequestCodec_Decode(t *testing.T) {
	codec := NewTokenRequestCodec()
	source := "test-source"
	clusterName := "test-cluster"
	requestID := types.UID("test-request-id")

	tokenRequest := &authenticationv1.TokenRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID:       requestID,
			Name:      "test-sa",
			Namespace: clusterName,
		},
		Spec: authenticationv1.TokenRequestSpec{
			Audiences:         []string{"test-audience"},
			ExpirationSeconds: func() *int64 { v := int64(3600); return &v }(),
		},
	}

	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: TokenRequestDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}

	// First encode to get a cloud event
	evt, err := codec.Encode(source, eventType, tokenRequest)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	// Now decode it back
	decoded, err := codec.Decode(evt)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.UID != requestID {
		t.Errorf("expected UID %s, got %s", requestID, decoded.UID)
	}
	if decoded.Name != "test-sa" {
		t.Errorf("expected Name test-sa, got %s", decoded.Name)
	}
	if decoded.Namespace != clusterName {
		t.Errorf("expected Namespace %s, got %s", clusterName, decoded.Namespace)
	}
	if len(decoded.Spec.Audiences) != 1 || decoded.Spec.Audiences[0] != "test-audience" {
		t.Errorf("expected Audiences [test-audience], got %v", decoded.Spec.Audiences)
	}
}

func TestTokenRequestCodec_RoundTrip(t *testing.T) {
	codec := NewTokenRequestCodec()
	source := "test-source"

	tests := []struct {
		name         string
		tokenRequest *authenticationv1.TokenRequest
	}{
		{
			name: "simple token request",
			tokenRequest: &authenticationv1.TokenRequest{
				ObjectMeta: metav1.ObjectMeta{
					UID:       types.UID("uid-1"),
					Name:      "sa-1",
					Namespace: "cluster-1",
				},
				Spec: authenticationv1.TokenRequestSpec{
					Audiences: []string{"audience-1"},
				},
			},
		},
		{
			name: "token request with expiration",
			tokenRequest: &authenticationv1.TokenRequest{
				ObjectMeta: metav1.ObjectMeta{
					UID:       types.UID("uid-2"),
					Name:      "sa-2",
					Namespace: "cluster-2",
				},
				Spec: authenticationv1.TokenRequestSpec{
					Audiences:         []string{"audience-1", "audience-2"},
					ExpirationSeconds: func() *int64 { v := int64(7200); return &v }(),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventType := cetypes.CloudEventsType{
				CloudEventsDataType: TokenRequestDataType,
				SubResource:         cetypes.SubResourceSpec,
				Action:              cetypes.CreateRequestAction,
			}

			// Encode
			evt, err := codec.Encode(source, eventType, tt.tokenRequest)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Decode
			decoded, err := codec.Decode(evt)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Verify
			if decoded.UID != tt.tokenRequest.UID {
				t.Errorf("UID mismatch: expected %s, got %s", tt.tokenRequest.UID, decoded.UID)
			}
			if decoded.Name != tt.tokenRequest.Name {
				t.Errorf("Name mismatch: expected %s, got %s", tt.tokenRequest.Name, decoded.Name)
			}
			if decoded.Namespace != tt.tokenRequest.Namespace {
				t.Errorf("Namespace mismatch: expected %s, got %s", tt.tokenRequest.Namespace, decoded.Namespace)
			}
		})
	}
}
