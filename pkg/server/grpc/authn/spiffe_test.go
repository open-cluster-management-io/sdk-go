package authn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	v1 "open-cluster-management.io/api/operator/v1"
)

var t0 = time.Date(2026, 11, 13, 10, 40, 0, 0, time.UTC)

func newSVID(t *testing.T, id string, notAfter time.Time) *x509.Certificate {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    t0.Add(-time.Hour),
		NotAfter:     notAfter,
	}
	if id != "" {
		u, err := url.Parse(id)
		require.NoError(t, err)
		tmpl.URIs = []*url.URL{u}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

// ctxWithPeerCert builds the peer the SPIFFE server credentials produce for cert.
func ctxWithPeerCert(t *testing.T, cert *x509.Certificate) context.Context {
	t.Helper()
	id, err := x509svid.IDFromCert(cert)
	require.NoError(t, err)
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: SPIFFEAuthInfo{
			TLSInfo:  credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}},
			SPIFFEID: id,
			NotAfter: cert.NotAfter,
		},
	})
}

func TestSPIFFEAuthenticator(t *testing.T) {
	scheme, err := NewIDScheme(v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate,
		[]string{"ocm-hub.example.org"})
	require.NoError(t, err)
	a := NewSPIFFEAuthenticator(scheme)
	a.now = func() time.Time { return t0 }

	const workID = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"

	t.Run("valid work agent", func(t *testing.T) {
		notAfter := t0.Add(time.Hour)
		ctx, err := a.Authenticate(ctxWithPeerCert(t, newSVID(t, workID, notAfter)))
		require.NoError(t, err)
		require.Equal(t, "system:open-cluster-management:cluster1:work", ctx.Value(ContextUserKey))
		require.Contains(t, ctx.Value(ContextGroupsKey), "system:authenticated")
		require.Equal(t, spiffeid.RequireFromString(workID), ctx.Value(ContextSPIFFEIDKey))
		require.Equal(t, notAfter, ctx.Value(ContextSVIDExpiryKey))
		id, ok := OCMIdentityFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, "cluster1", id.ClusterName)
		require.Equal(t, RoleWork, id.Role)
	})

	errCases := []struct {
		name    string
		ctx     func(t *testing.T) context.Context
		wantMsg string
	}{
		{"expired SVID", func(t *testing.T) context.Context {
			return ctxWithPeerCert(t, newSVID(t, workID, t0.Add(-time.Minute)))
		}, "expired"},
		{"no peer", func(t *testing.T) context.Context { return context.Background() }, "no peer found"},
		// A connection from other credentials, even with a verified SVID chain, is refused: only
		// the SPIFFE server credentials vouch for the client.
		{"plain TLS with a verified SVID chain", func(t *testing.T) context.Context {
			cert := newSVID(t, workID, t0.Add(time.Hour))
			return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{cert},
				VerifiedChains:   [][]*x509.Certificate{{cert}},
			}}})
		}, "not verified by the SPIFFE server credentials"},
		{"no auth info", func(t *testing.T) context.Context {
			return peer.NewContext(context.Background(), &peer.Peer{})
		}, "not verified by the SPIFFE server credentials"},
		{"other trust domain", func(t *testing.T) context.Context {
			return ctxWithPeerCert(t, newSVID(t, "spiffe://other.example.org/ocm/cluster/cluster1/agent/work", t0.Add(time.Hour)))
		}, "not in agentTrustDomains"},
		{"generic ns/sa ID", func(t *testing.T) context.Context {
			return ctxWithPeerCert(t, newSVID(t, "spiffe://ocm-hub.example.org/ns/open-cluster-management-agent/sa/klusterlet-work-sa", t0.Add(time.Hour)))
		}, "matches neither"},
	}
	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := a.Authenticate(c.ctx(t))
			require.Equal(t, codes.Unauthenticated, status.Code(err))
			require.ErrorContains(t, err, c.wantMsg)
		})
	}
}

func TestSPIFFEAuthenticatorNamespaceTemplate(t *testing.T) {
	ns, err := NewIDScheme(namespaceAgentTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate, []string{"ocm-hub.example.org"})
	require.NoError(t, err)
	def, err := NewIDScheme(v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate, []string{"ocm-hub.example.org"})
	require.NoError(t, err)

	cert := newSVID(t, "spiffe://ocm-hub.example.org/k8s/cluster1/ns/team-a-ocm/ocm/work", t0.Add(time.Hour))

	a := NewSPIFFEAuthenticator(ns)
	a.now = func() time.Time { return t0 }
	ctx, err := a.Authenticate(ctxWithPeerCert(t, cert))
	require.NoError(t, err)
	require.Equal(t, "system:open-cluster-management:cluster1:work", ctx.Value(ContextUserKey))

	b := NewSPIFFEAuthenticator(def)
	b.now = func() time.Time { return t0 }
	_, err = b.Authenticate(ctxWithPeerCert(t, cert))
	require.ErrorContains(t, err, "matches neither")
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeStream) Context() context.Context     { return f.ctx }
func (f *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)       {}

func TestSVIDExpiryInterceptors(t *testing.T) {
	now := func() time.Time { return t0 }
	valid := context.WithValue(context.Background(), ContextSVIDExpiryKey, t0.Add(time.Minute))
	expired := context.WithValue(context.Background(), ContextSVIDExpiryKey, t0.Add(-time.Second))
	atExpiry := context.WithValue(context.Background(), ContextSVIDExpiryKey, t0)

	unary := SVIDExpiryUnaryInterceptor(now)
	stream := SVIDExpiryStreamInterceptor(now)

	cases := []struct {
		name       string
		ctx        context.Context
		wantCalled bool
	}{
		{"valid", valid, true},
		{"expired", expired, false},
		{"at expiry", atExpiry, false},
		{"no expiry in context", context.Background(), false},
	}
	for _, c := range cases {
		t.Run("unary "+c.name, func(t *testing.T) {
			calls := 0
			_, err := unary(c.ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
				calls++
				return nil, nil
			})
			if c.wantCalled {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				return
			}
			require.Equal(t, codes.Unauthenticated, status.Code(err))
			require.Zero(t, calls)
		})
		t.Run("stream "+c.name, func(t *testing.T) {
			calls := 0
			err := stream(nil, &fakeStream{ctx: c.ctx}, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error {
				calls++
				return nil
			})
			if c.wantCalled {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				return
			}
			require.Equal(t, codes.Unauthenticated, status.Code(err))
			require.Zero(t, calls)
		})
	}
}

func TestSPIFFEAuthInfoAuthType(t *testing.T) {
	require.Equal(t, "spiffe", SPIFFEAuthInfo{}.AuthType())
}
