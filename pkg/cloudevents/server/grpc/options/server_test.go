package options

import (
	"context"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/authn"
	"testing"
)

type testHook struct{}

func (h *testHook) Run(ctx context.Context) {}

func TestRunServer(t *testing.T) {
	opt := NewGRPCServerOptions()
	server := NewServer(opt).WithAuthenticator(authn.NewMtlsAuthenticator()).WithPreStartHooks(&testHook{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := server.Run(ctx)
		if err != nil {
			t.Errorf("server run error: %v", err)
		}
	}()
}
