package sar

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	authv1 "k8s.io/api/authorization/v1"
	certificatesv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	addonv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/addon/v1beta1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/cluster"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/csr"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/lease"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/serviceaccount"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authn"
	"open-cluster-management.io/sdk-go/pkg/server/grpc/authz"
)

// spiffeContext builds the context SPIFFEAuthenticator produces for id.
func spiffeContext(id authn.OCMIdentity) context.Context {
	ctx := context.WithValue(context.Background(), authn.ContextUserKey, id.User)
	ctx = context.WithValue(ctx, authn.ContextGroupsKey, id.Groups)
	return context.WithValue(ctx, authn.ContextOCMIdentityKey, id)
}

func agentIdentity(cluster, role string) authn.OCMIdentity {
	return authn.OCMIdentity{
		TrustDomain: "ocm-hub.example.org",
		ClusterName: cluster,
		Role:        authn.AgentRole(role),
		User:        "system:open-cluster-management:" + cluster + ":" + role,
		Groups: []string{
			"system:open-cluster-management:" + cluster,
			"system:open-cluster-management:managed-clusters",
			"system:open-cluster-management:spiffe-agents",
			"system:authenticated",
		},
	}
}

func addOnIdentity(cluster, addon string) authn.OCMIdentity {
	return authn.OCMIdentity{
		TrustDomain: "ocm-hub.example.org",
		ClusterName: cluster,
		AddOnName:   addon,
		AgentName:   addon + "-agent",
		User:        "system:open-cluster-management:cluster:" + cluster + ":addon:" + addon + ":agent:" + addon + "-agent",
		Groups: []string{
			"system:open-cluster-management:cluster:" + cluster + ":addon:" + addon,
			"system:open-cluster-management:addon:" + addon,
			"system:authenticated",
		},
	}
}

// allowAllSARClient answers every SubjectAccessReview with allowed, so only the SPIFFE checks
// decide, and records what it was asked.
func allowAllSARClient(t *testing.T) (*fake.Clientset, *[]*authv1.SubjectAccessReview) {
	client := fake.NewSimpleClientset()
	var seen []*authv1.SubjectAccessReview
	client.Fake.PrependReactor("create", "subjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			sar := action.(clienttesting.CreateAction).GetObject().(*authv1.SubjectAccessReview)
			seen = append(seen, sar)
			return true, &authv1.SubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: true}}, nil
		})
	return client, &seen
}

func TestSARAuthorizeRequestSPIFFE(t *testing.T) {
	workStatus := types.CloudEventsType{CloudEventsDataType: payload.ManifestBundleEventDataType,
		SubResource: types.SubResourceStatus, Action: types.UpdateRequestAction}
	clusterStatus := types.CloudEventsType{CloudEventsDataType: cluster.ManagedClusterEventDataType,
		SubResource: types.SubResourceStatus, Action: types.UpdateRequestAction}
	csrCreate := types.CloudEventsType{CloudEventsDataType: csr.CSREventDataType,
		SubResource: types.SubResourceSpec, Action: types.CreateRequestAction}
	addonStatus := types.CloudEventsType{CloudEventsDataType: v1beta1.ManagedClusterAddOnEventDataType,
		SubResource: types.SubResourceStatus, Action: types.UpdateRequestAction}
	tokenCreate := types.CloudEventsType{CloudEventsDataType: serviceaccount.TokenRequestDataType,
		SubResource: types.SubResourceSpec, Action: types.CreateRequestAction}

	managedCluster := func(name string) *clusterv1.ManagedCluster {
		return &clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	addon := func(ns, name string) *addonv1beta1.ManagedClusterAddOn {
		return &addonv1beta1.ManagedClusterAddOn{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	}

	cases := []struct {
		name    string
		id      authn.OCMIdentity
		req     func(t *testing.T) *pbv1.PublishRequest
		wantErr string
	}{
		{"cluster1 work agent updates cluster1's work status", agentIdentity("cluster1", "work"),
			func(t *testing.T) *pbv1.PublishRequest { return newPublishRequest(t, workStatus, "cluster1", nil) }, ""},
		{"cluster2 work agent updates cluster1's work status", agentIdentity("cluster2", "work"),
			func(t *testing.T) *pbv1.PublishRequest { return newPublishRequest(t, workStatus, "cluster1", nil) },
			`may not act on cluster "cluster1"`},
		{"cluster1 registration agent updates its ManagedCluster", agentIdentity("cluster1", "registration"),
			func(t *testing.T) *pbv1.PublishRequest {
				return newPublishRequest(t, clusterStatus, "cluster1", managedCluster("cluster1"))
			}, ""},
		{"cluster1 registration agent creates a CSR", agentIdentity("cluster1", "registration"),
			func(t *testing.T) *pbv1.PublishRequest {
				return newPublishRequest(t, csrCreate, "cluster1", &certificatesv1.CertificateSigningRequest{
					ObjectMeta: metav1.ObjectMeta{Name: "csr1", Labels: map[string]string{clusterv1.ClusterNameLabelKey: "cluster1"}},
				})
			}, "not served on the SPIFFE listener"},
		{"cluster1 my-addon agent updates my-addon", addOnIdentity("cluster1", "my-addon"),
			func(t *testing.T) *pbv1.PublishRequest {
				return newPublishRequest(t, addonStatus, "cluster1", addon("cluster1", "my-addon"))
			}, ""},
		{"cluster1 my-addon agent updates other-addon", addOnIdentity("cluster1", "my-addon"),
			func(t *testing.T) *pbv1.PublishRequest {
				return newPublishRequest(t, addonStatus, "cluster1", addon("cluster1", "other-addon"))
			}, `may not act on add-on "other-addon"`},
		{"cluster1 my-addon agent updates my-addon in cluster2", addOnIdentity("cluster1", "my-addon"),
			func(t *testing.T) *pbv1.PublishRequest {
				return newPublishRequest(t, addonStatus, "cluster2", addon("cluster2", "my-addon"))
			}, `may not act on cluster "cluster2"`},
		{"cluster1 my-addon agent requests a token", addOnIdentity("cluster1", "my-addon"),
			func(t *testing.T) *pbv1.PublishRequest {
				return newPublishRequest(t, tokenCreate, "cluster1",
					&metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "cluster1", Name: "my-addon-agent"}})
			}, "not served on the SPIFFE listener"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, seen := allowAllSARClient(t)
			decision, _, err := NewSARAuthorizer(client).AuthorizeRequest(spiffeContext(c.id), c.req(t))
			if c.wantErr != "" {
				require.Equal(t, authz.DecisionDeny, decision)
				require.ErrorContains(t, err, c.wantErr)
				require.Empty(t, *seen, "denied before the SubjectAccessReview")
				return
			}
			require.NoError(t, err)
			require.Equal(t, authz.DecisionAllow, decision)
			require.Len(t, *seen, 1)
			require.Equal(t, c.id.User, (*seen)[0].Spec.User)
			require.ElementsMatch(t, c.id.Groups, (*seen)[0].Spec.Groups)
		})
	}
}

type spiffeSubscribeStream struct {
	grpc.ServerStream
	ctx context.Context
	req *pbv1.SubscriptionRequest
}

func (s *spiffeSubscribeStream) Context() context.Context { return s.ctx }

func (s *spiffeSubscribeStream) RecvMsg(m any) error {
	msg := m.(*pbv1.SubscriptionRequest)
	msg.ClusterName, msg.Source, msg.DataType = s.req.ClusterName, s.req.Source, s.req.DataType
	return nil
}

func TestSARAuthorizeStreamSPIFFE(t *testing.T) {
	cases := []struct {
		name    string
		id      authn.OCMIdentity
		cluster string
		wantErr string
	}{
		{"own cluster", agentIdentity("cluster1", "work"), "cluster1", ""},
		{"other cluster", agentIdentity("cluster2", "work"), "cluster1", `may not act on cluster "cluster1"`},
		{"add-on agent, own cluster", addOnIdentity("cluster1", "my-addon"), "cluster1", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, seen := allowAllSARClient(t)
			decision, _, err := NewSARAuthorizer(client).AuthorizeStream(spiffeContext(c.id),
				&spiffeSubscribeStream{ctx: spiffeContext(c.id), req: &pbv1.SubscriptionRequest{
					ClusterName: c.cluster, DataType: lease.LeaseEventDataType.String()}},
				&grpc.StreamServerInfo{FullMethod: pbv1.CloudEventService_Subscribe_FullMethodName, IsServerStream: true})
			if c.wantErr != "" {
				require.Equal(t, authz.DecisionDeny, decision)
				require.ErrorContains(t, err, c.wantErr)
				require.Empty(t, *seen)
				return
			}
			require.NoError(t, err)
			require.Equal(t, authz.DecisionAllow, decision)
		})
	}
}

// TestSARSPIFFECheck checks that the extra SPIFFE check gets the data type of publishes and
// subscriptions, denies before the SubjectAccessReview, and is skipped for requests without an
// SVID.
func TestSARSPIFFECheck(t *testing.T) {
	refuseWork := func(_ authn.OCMIdentity, cluster string, et types.CloudEventsType) error {
		if et.CloudEventsDataType == payload.ManifestBundleEventDataType {
			return fmt.Errorf("no work for %s", cluster)
		}
		return nil
	}
	workStatus := types.CloudEventsType{CloudEventsDataType: payload.ManifestBundleEventDataType,
		SubResource: types.SubResourceStatus, Action: types.UpdateRequestAction}
	clusterStatus := types.CloudEventsType{CloudEventsDataType: cluster.ManagedClusterEventDataType,
		SubResource: types.SubResourceStatus, Action: types.UpdateRequestAction}
	id := agentIdentity("cluster1", "klusterlet")

	t.Run("publish work: refused", func(t *testing.T) {
		client, seen := allowAllSARClient(t)
		decision, _, err := NewSARAuthorizer(client).WithSPIFFECheck(refuseWork).
			AuthorizeRequest(spiffeContext(id), newPublishRequest(t, workStatus, "cluster1", nil))
		require.Equal(t, authz.DecisionDeny, decision)
		require.ErrorContains(t, err, "no work for cluster1")
		require.Empty(t, *seen)
	})
	t.Run("publish ManagedCluster: allowed", func(t *testing.T) {
		client, seen := allowAllSARClient(t)
		decision, _, err := NewSARAuthorizer(client).WithSPIFFECheck(refuseWork).
			AuthorizeRequest(spiffeContext(id), newPublishRequest(t, clusterStatus, "cluster1",
				&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}}))
		require.NoError(t, err)
		require.Equal(t, authz.DecisionAllow, decision)
		require.Len(t, *seen, 1)
	})
	subscribe := func(t *testing.T, ctx context.Context, dataType types.CloudEventsDataType) (authz.Decision, error, int) {
		client, seen := allowAllSARClient(t)
		decision, _, err := NewSARAuthorizer(client).WithSPIFFECheck(refuseWork).AuthorizeStream(ctx,
			&spiffeSubscribeStream{ctx: ctx, req: &pbv1.SubscriptionRequest{
				ClusterName: "cluster1", DataType: dataType.String()}},
			&grpc.StreamServerInfo{FullMethod: pbv1.CloudEventService_Subscribe_FullMethodName, IsServerStream: true})
		return decision, err, len(*seen)
	}
	t.Run("subscribe to work: refused", func(t *testing.T) {
		decision, err, sars := subscribe(t, spiffeContext(id), payload.ManifestBundleEventDataType)
		require.Equal(t, authz.DecisionDeny, decision)
		require.ErrorContains(t, err, "no work for cluster1")
		require.Zero(t, sars)
	})
	t.Run("subscribe to ManagedCluster: allowed", func(t *testing.T) {
		decision, err, _ := subscribe(t, spiffeContext(id), cluster.ManagedClusterEventDataType)
		require.NoError(t, err)
		require.Equal(t, authz.DecisionAllow, decision)
	})
	t.Run("no SVID: check skipped", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), authn.ContextUserKey, "user1")
		decision, err, sars := subscribe(t, ctx, payload.ManifestBundleEventDataType)
		require.NoError(t, err)
		require.Equal(t, authz.DecisionAllow, decision)
		require.Equal(t, 1, sars)
	})
}
