package authz

import (
	"context"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func TestAuthorizedEventContext(t *testing.T) {
	if evt, ok := AuthorizedEventFrom(context.Background()); ok || evt != nil {
		t.Fatalf("expected no authorized event in an empty context, got %v (ok=%v)", evt, ok)
	}

	if evt, ok := AuthorizedEventFrom(WithAuthorizedEvent(context.Background(), nil)); ok || evt != nil {
		t.Fatalf("expected a nil authorized event to be reported as absent, got %v (ok=%v)", evt, ok)
	}

	want := cloudevents.NewEvent()
	want.SetID("test-id")
	want.SetType("test-type")

	got, ok := AuthorizedEventFrom(WithAuthorizedEvent(context.Background(), &want))
	if !ok {
		t.Fatal("expected the authorized event to be present")
	}
	if got != &want {
		t.Fatalf("expected the same event pointer to be returned, got %v", got)
	}
}
