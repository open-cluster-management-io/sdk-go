package spiffe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/require"

	"open-cluster-management.io/sdk-go/pkg/testing/fakespire"
)

const (
	workID    = "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"
	genericID = "spiffe://ocm-hub.example.org/ns/open-cluster-management-agent/sa/default"
)

func TestNewX509SourcePicksByID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	api := fakespire.Start(t, ca)
	// The wanted SVID is second: the default picker would take the first.
	work := ca.Issue(t, workID, time.Hour)
	api.SetSVIDs(ca.Issue(t, genericID, time.Hour), work)

	src, err := NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(workID))
	require.NoError(t, err)
	defer src.Close()

	got, err := src.GetX509SVID()
	require.NoError(t, err)
	require.Equal(t, workID, got.ID.String())
	require.Equal(t, work.Certificates[0].SerialNumber, got.Certificates[0].SerialNumber)
	require.NoError(t, src.Ready())
	require.Equal(t, workID, src.ID().String())

	bundle, err := src.GetX509BundleForTrustDomain(ca.TrustDomain)
	require.NoError(t, err)
	require.True(t, bundle.Equal(ca.Bundle()))
}

func TestNewX509SourceMissingID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	api := fakespire.Start(t, ca)
	api.SetSVIDs(ca.Issue(t, genericID, time.Hour))

	_, err := NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(workID))
	var notFound *ErrSVIDNotFound
	require.True(t, errors.As(err, &notFound), "got %v", err)
	require.Equal(t, workID, notFound.Want.String())
	require.Equal(t, []spiffeid.ID{spiffeid.RequireFromString(genericID)}, notFound.Got)
	require.ErrorContains(t, err, "no SVID with ID "+workID+" from the Workload API; received ["+genericID+"]")
}

func TestX509SourceLosesID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := fakespire.NewCA(t, "ocm-hub.example.org")
	api := fakespire.Start(t, ca)
	api.SetSVIDs(ca.Issue(t, workID, time.Hour))

	src, err := NewX509Source(ctx, api.Addr, spiffeid.RequireFromString(workID))
	require.NoError(t, err)
	defer src.Close()

	// The platform removes the agent's entry; only the generic SVID is left.
	api.SetSVIDs(ca.Issue(t, genericID, time.Hour))
	require.Eventually(t, func() bool {
		var notFound *ErrSVIDNotFound
		return errors.As(src.Ready(), &notFound)
	}, 5*time.Second, 50*time.Millisecond)

	// The entry comes back.
	api.SetSVIDs(ca.Issue(t, workID, time.Hour))
	require.Eventually(t, func() bool { return src.Ready() == nil }, 5*time.Second, 50*time.Millisecond)
}

// Opening the Workload API fails, for example when the agent stops before it connects.
func TestNewX509SourceWorkloadAPIError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewX509Source(ctx, "unix:///nonexistent/spire-agent.sock", spiffeid.RequireFromString(workID))
	require.Error(t, err)
	var notFound *ErrSVIDNotFound
	require.False(t, errors.As(err, &notFound), "a Workload API error is not a missing SVID: %v", err)
}
