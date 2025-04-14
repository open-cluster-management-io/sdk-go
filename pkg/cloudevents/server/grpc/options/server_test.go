package options

import (
	"context"
	certutil "k8s.io/client-go/util/cert"
	"net"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/authn"
	"os"
	"testing"
)

type testHook struct{}

func (h *testHook) Run(ctx context.Context) {}

func TestRunServer(t *testing.T) {
	cert, key, err := certutil.GenerateSelfSignedCertKey("localhost", []net.IP{net.ParseIP("127.0.0.1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	path, err := os.MkdirTemp("", "certs")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(path+"/tls.crt", cert, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(path+"/tls.key", key, 0600)
	if err != nil {
		t.Fatal(err)
	}
	opt := NewGRPCServerOptions()
	opt.TLSKeyFile = path + "/tls.key"
	opt.TLSCertFile = path + "/tls.crt"
	server := NewServer(opt).WithAuthenticator(authn.NewMtlsAuthenticator()).WithPreStartHooks(&testHook{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := server.Run(ctx)
		if err != nil {
			t.Errorf("server run error: %v", err)
		}
		cancel()
	}()
}
