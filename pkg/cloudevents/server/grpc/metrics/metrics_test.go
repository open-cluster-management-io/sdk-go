package metrics

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"

	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
)

func TestCloudEventsMetricsUnaryInterceptorRejectsMissingEvent(t *testing.T) {
	interceptor := NewCloudEventsMetricsUnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: pbv1.CloudEventService_Publish_FullMethodName}

	for _, req := range []any{(*pbv1.PublishRequest)(nil), &pbv1.PublishRequest{}} {
		_, err := interceptor(context.Background(), req, info, func(context.Context, any) (any, error) {
			t.Errorf("handler must not be called for request %#v", req)
			return nil, nil
		})
		if err == nil || !strings.Contains(err.Error(), "missing event") {
			t.Errorf("expected a missing event error for request %#v, got %v", req, err)
		}
	}
}

func TestSplitMethod(t *testing.T) {
	tests := []struct {
		name            string
		fullMethod      string
		expectedService string
		expectedMethod  string
	}{
		{"empty full method", "", "unknown", "unknown"},
		{"no leading slash", "io.cloudevents.v1.CloudEventService/Subscribe", "io.cloudevents.v1.CloudEventService", "Subscribe"},
		{"leading slash", "/io.cloudevents.v1.CloudEventService/Subscribe", "io.cloudevents.v1.CloudEventService", "Subscribe"},
		{"no slash", "io.cloudevents.v1.CloudEventService", "io.cloudevents.v1.CloudEventService", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotService, gotMethod := SplitMethod(tt.fullMethod)
			if gotService != tt.expectedService {
				t.Errorf("splitMethod(%s) gotService = %s, want %s", tt.fullMethod, gotService, tt.expectedService)
			}
			if gotMethod != tt.expectedMethod {
				t.Errorf("splitMethod(%s) gotMethod = %s, want %s", tt.fullMethod, gotMethod, tt.expectedMethod)
			}
		})
	}
}
