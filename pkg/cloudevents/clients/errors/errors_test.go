package errors

import (
	"fmt"
	"testing"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/common"
)

func TestPublishError(t *testing.T) {
	err := NewPublishError(common.ManagedClusterGR, "test", fmt.Errorf("failed to publish resource"))
	if !IsPublishError(err) {
		t.Errorf("expected publish error, but failed")
	}
}
