package protocol

import (
	"context"
	"strings"
	"testing"

	"github.com/cloudevents/sdk-go/v2/binding"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
)

func strAttr(v string) *pbv1.CloudEventAttributeValue {
	return &pbv1.CloudEventAttributeValue{Attr: &pbv1.CloudEventAttributeValue_CeString{CeString: v}}
}

func TestValidateAttributeNames(t *testing.T) {
	cases := []struct {
		name       string
		attributes map[string]*pbv1.CloudEventAttributeValue
		wantErr    string
	}{
		{
			name: "nil attributes",
		},
		{
			name: "valid attributes",
			attributes: map[string]*pbv1.CloudEventAttributeValue{
				"ce-clustername": strAttr("cluster1"),
				"ce-subject":     strAttr("subject"),
				contenttype:      strAttr("application/json"),
			},
		},
		{
			name:       "upper-case attribute name",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"ce-ClusterName": strAttr("cluster1")},
			wantErr:    "must be lower-case",
		},
		{
			name:       "missing ce- prefix",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"clustername": strAttr("cluster1")},
			wantErr:    `expected the "ce-" prefix`,
		},
		{
			name:       "specversion in attributes",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"ce-specversion": strAttr("1.0")},
			wantErr:    "carried in a dedicated field",
		},
		{
			name:       "id in attributes",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"ce-id": strAttr("id")},
			wantErr:    "carried in a dedicated field",
		},
		{
			name:       "source in attributes",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"ce-source": strAttr("source")},
			wantErr:    "carried in a dedicated field",
		},
		{
			name:       "type in attributes",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"ce-type": strAttr("spoofed.type")},
			wantErr:    "carried in a dedicated field",
		},
		{
			name:       "datacontenttype in attributes",
			attributes: map[string]*pbv1.CloudEventAttributeValue{"ce-datacontenttype": strAttr("application/json")},
			wantErr:    `carried as "contenttype"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAttributeNames(tc.attributes)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestMessageRejectsInvalidAttributeNames(t *testing.T) {
	msg := &pbv1.CloudEvent{
		SpecVersion: "1.0",
		Id:          "ABC-123",
		Source:      "test-source",
		Type:        "io.test.original",
		Attributes: map[string]*pbv1.CloudEventAttributeValue{
			"ce-type": strAttr("io.test.spoofed"),
		},
	}

	message := NewMessage(msg)
	if err := message.ReadBinary(context.Background(), (*pbEventWriter)(&pbv1.CloudEvent{})); err == nil {
		t.Fatal("expected ReadBinary to fail for a spoofed ce-type attribute")
	}
	if err := message.ReadStructured(context.Background(), (*pbEventWriter)(&pbv1.CloudEvent{})); err == nil {
		t.Fatal("expected ReadStructured to fail for a spoofed ce-type attribute")
	}

	if _, err := binding.ToEvent(context.Background(), NewMessage(msg)); err == nil {
		t.Fatal("expected ToEvent to fail for a spoofed ce-type attribute")
	}
}
