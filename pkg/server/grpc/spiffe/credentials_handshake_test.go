package spiffe

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc/credentials"

	ocmauthn "open-cluster-management.io/sdk-go/pkg/server/grpc/authn"
	"open-cluster-management.io/sdk-go/pkg/testing/fakespire"
)

// fakeCreds stands in for the SPIFFE TLS credentials, to reach the checks that the real TLS
// handshake normally makes unreachable.
type fakeCreds struct {
	credentials.TransportCredentials
	info credentials.AuthInfo
	err  error
}

func (f fakeCreds) ServerHandshake(c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if f.err != nil {
		return nil, nil, f.err
	}
	return c, f.info, nil
}

func (f fakeCreds) Clone() credentials.TransportCredentials { return f }

func testScheme(t *testing.T) *ocmauthn.IDScheme {
	t.Helper()
	scheme, err := ocmauthn.NewIDScheme("spiffe://{trustDomain}/ocm/cluster/{cluster}/agent/{role}",
		"spiffe://{trustDomain}/ocm/cluster/{cluster}/addon/{addon}/agent/{agent}", []string{"ocm-hub.example.org"})
	if err != nil {
		t.Fatal(err)
	}
	return scheme
}

func tlsInfo(certs ...*x509.Certificate) credentials.TLSInfo {
	return credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: certs}}
}

// The handshake refuses, and closes the connection, whenever the peer isn't a mapped SVID.
func TestServerHandshakeRefusals(t *testing.T) {
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	other := ca.Issue(t, "spiffe://ocm-hub.example.org/not/an/ocm/agent", time.Hour)
	noURI := &x509.Certificate{Raw: []byte("x")}

	cases := []struct {
		name  string
		creds fakeCreds
		want  string
	}{
		{"the TLS handshake fails", fakeCreds{err: errors.New("bad handshake")}, "bad handshake"},
		{"not TLS", fakeCreds{info: ocmauthn.SPIFFEAuthInfo{}}, "no client certificate"},
		{"no client certificate", fakeCreds{info: tlsInfo()}, "no client certificate"},
		{"a certificate without a SPIFFE ID", fakeCreds{info: tlsInfo(noURI)}, "URI"},
		{"an ID outside the OCM scheme", fakeCreds{info: tlsInfo(other.Certificates[0])}, "ocm"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reg := NewRegistry()
			creds := &trackingCreds{TransportCredentials: c.creds, scheme: testScheme(t), reg: reg}
			server, client := net.Pipe()
			defer client.Close()
			_, _, err := creds.ServerHandshake(server)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want it to mention %q", err, c.want)
			}
			if reg.Len() != 0 {
				t.Errorf("a refused connection was registered")
			}
		})
	}
}

func TestTrackingCredsClone(t *testing.T) {
	reg := NewRegistry()
	scheme := testScheme(t)
	creds := &trackingCreds{TransportCredentials: fakeCreds{}, scheme: scheme, reg: reg}
	clone, ok := creds.Clone().(*trackingCreds)
	if !ok {
		t.Fatalf("Clone returned %T", creds.Clone())
	}
	if clone == creds || clone.reg != reg || clone.scheme != scheme {
		t.Errorf("clone %+v doesn't share the registry and scheme of %+v", clone, creds)
	}
}

// An SVID that has already expired is closed by its timer before track stores the timer; track
// then stops the timer instead of leaving it behind.
func TestTrackAlreadyExpired(t *testing.T) {
	ft := &fakeTimers{}
	reg := NewRegistry()
	reg.afterFunc = func(d time.Duration, f func()) stopper {
		timer := ft.afterFunc(d, f)
		f() // fires at once, as time.AfterFunc does for a negative duration
		return timer
	}
	server, client := net.Pipe()
	defer client.Close()
	id := ocmauthn.OCMIdentity{ClusterName: "cluster1", Role: ocmauthn.RoleWork}
	reg.track(server, id, spiffeid.RequireFromString("spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"),
		nil, time.Now().Add(-time.Minute))
	if reg.Len() != 0 {
		t.Errorf("an expired connection is still registered")
	}
	if timers := ft.all(); len(timers) != 1 || !timers[0].isStopped() {
		t.Errorf("the timer of an expired connection wasn't stopped: %+v", timers)
	}
}
