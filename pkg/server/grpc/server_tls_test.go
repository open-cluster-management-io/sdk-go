package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	certutil "k8s.io/client-go/util/cert"
)

func writeServerCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	cert, key, err := certutil.GenerateSelfSignedCertKey("localhost", []net.IP{net.ParseIP("127.0.0.1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, cert, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// certFileCredentials builds the TLS of the existing gRPC listener from files. These cases
// cover the client CA and cipher options it moved from Run.
func TestCertFileCredentials(t *testing.T) {
	certFile, keyFile := writeServerCert(t)
	dir := t.TempDir()
	badCA := filepath.Join(dir, "bad-ca.crt")
	if err := os.WriteFile(badCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		clientCA string
		ciphers  []uint16
		wantErr  string
	}{
		{name: "no client CA"},
		{name: "a client CA", clientCA: certFile},
		{name: "TLS 1.2 cipher suites", ciphers: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}},
		{name: "a missing client CA file", clientCA: filepath.Join(dir, "missing.crt"), wantErr: "failed to read server client CA file"},
		{name: "a client CA file without certificates", clientCA: badCA, wantErr: "failed to append server client CA"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opt := NewGRPCServerOptions()
			opt.TLSCertFile, opt.TLSKeyFile, opt.ClientCAFile = certFile, keyFile, c.clientCA
			opt.tlsMinVersion = tls.VersionTLS12
			opt.cipherSuiteIDs = c.ciphers
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			creds, err := NewGRPCServer(opt).certFileCredentials(ctx)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("error %v, want %q", err, c.wantErr)
				}
				return
			}
			if err != nil || creds == nil {
				t.Fatalf("creds %v, error %v", creds, err)
			}
		})
	}
}

type failingWatcher struct{ started bool }

func (w *failingWatcher) Start(context.Context) error {
	w.started = true
	return errors.New("failed to add watches")
}

// A watcher that stops with an error is logged; the listener keeps the certificate it loaded.
func TestRunCertWatcher(t *testing.T) {
	w := &failingWatcher{}
	runCertWatcher(context.Background(), w)
	if !w.started {
		t.Error("the watcher was not started")
	}
}
