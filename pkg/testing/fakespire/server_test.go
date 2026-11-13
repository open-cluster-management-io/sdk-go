package fakespire

import (
	"context"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const workID = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"

func TestFakeWorkloadAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := NewCA(t, "ocm-hub.example.org")
	srv := Start(t, ca)

	first := ca.Issue(t, workID, time.Hour)
	srv.SetSVIDs(first)

	src, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(srv.Addr)))
	require.NoError(t, err)
	defer src.Close()

	got, err := src.GetX509SVID()
	require.NoError(t, err)
	require.Equal(t, first.ID, got.ID)
	require.Equal(t, first.Certificates[0].SerialNumber, got.Certificates[0].SerialNumber)

	bundle, err := src.GetX509BundleForTrustDomain(ca.TrustDomain)
	require.NoError(t, err)
	require.True(t, bundle.Equal(ca.Bundle()))

	// The SVID verifies against the bundle the source holds.
	_, _, err = x509svid.Verify(got.Certificates, src)
	require.NoError(t, err)

	// Rotate.
	second := ca.Issue(t, workID, time.Hour)
	srv.SetSVIDs(second)
	require.Eventually(t, func() bool {
		s, err := src.GetX509SVID()
		return err == nil && s.Certificates[0].SerialNumber.Cmp(second.Certificates[0].SerialNumber) == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestFakeWorkloadAPITwoSVIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := NewCA(t, "ocm-hub.example.org")
	srv := Start(t, ca)
	srv.SetSVIDs(
		ca.Issue(t, "spiffe://ocm-hub.example.org/ns/open-cluster-management-agent/sa/default", time.Hour),
		ca.Issue(t, workID, time.Hour),
	)

	client, err := workloadapi.New(ctx, workloadapi.WithAddr(srv.Addr))
	require.NoError(t, err)
	defer client.Close()
	x509ctx, err := client.FetchX509Context(ctx)
	require.NoError(t, err)
	require.Len(t, x509ctx.SVIDs, 2)
	require.Equal(t, spiffeid.RequireFromString(workID), x509ctx.SVIDs[1].ID)
}

func TestFakeWorkloadAPIFederation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	east := NewCA(t, "east.example.org")
	hub := NewCA(t, "ocm-hub.example.org")
	srv := Start(t, east)
	srv.SetSVIDs(east.Issue(t, "spiffe://east.example.org/ocm/cluster/cluster2/agent/work", time.Hour))
	srv.Federate(hub)

	src, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(srv.Addr)))
	require.NoError(t, err)
	defer src.Close()

	got, err := src.GetX509BundleForTrustDomain(hub.TrustDomain)
	require.NoError(t, err)
	require.True(t, got.Equal(hub.Bundle()))
}

func TestFakeWorkloadAPINoSVID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv := Start(t, NewCA(t, "ocm-hub.example.org"))

	client, err := workloadapi.New(ctx, workloadapi.WithAddr(srv.Addr))
	require.NoError(t, err)
	defer client.Close()
	_, err = client.FetchX509Context(ctx)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
