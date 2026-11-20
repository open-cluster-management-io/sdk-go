package authn

import (
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/require"

	v1 "open-cluster-management.io/api/operator/v1"
)

const namespaceAgentTemplate = "spiffe://{trustDomain}/k8s/{cluster}/ns/{namespace}/ocm/{role}"

var testTrustDomains = []string{"ocm-hub.example.org", "east.example.org"}

func mustScheme(t *testing.T, agent, addOn string) *IDScheme {
	t.Helper()
	s, err := NewIDScheme(agent, addOn, testTrustDomains)
	require.NoError(t, err)
	return s
}

func TestIDSchemeParse(t *testing.T) {
	def := mustScheme(t, v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate)
	cases := []struct {
		name     string
		id       string
		wantUser string
		wantErr  string
	}{
		{"registration", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/registration",
			"system:open-cluster-management:cluster1:registration", ""},
		{"work", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work",
			"system:open-cluster-management:cluster1:work", ""},
		{"klusterlet", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/klusterlet",
			"system:open-cluster-management:cluster1:klusterlet", ""},
		{"addon", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/addon/my-addon/agent/my-addon-agent",
			"system:open-cluster-management:cluster:cluster1:addon:my-addon:agent:my-addon-agent", ""},
		{"listed trust domain", "spiffe://east.example.org/ocm/cluster/cluster2/agent/work",
			"system:open-cluster-management:cluster2:work", ""},
		{"unlisted trust domain", "spiffe://south.example.org/ocm/cluster/cluster4/agent/work", "",
			"not in agentTrustDomains"},
		{"hub id", "spiffe://ocm-hub.example.org/ocm/hub/grpc-server", "", "matches neither"},
		{"unknown role", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/admin", "", "unknown agent role"},
		{"something else", "spiffe://ocm-hub.example.org/ocm/something-else", "", "matches neither"},
		{"generic ns/sa id", "spiffe://ocm-hub.example.org/ns/open-cluster-management-agent/sa/klusterlet-work-sa", "", "matches neither"},
		{"extra segment", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work/x", "", "matches neither"},
		{"bad cluster name", "spiffe://ocm-hub.example.org/ocm/cluster/Cluster_1/agent/work", "", "invalid cluster name"},
		{"bad add-on name", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/addon/My_Addon/agent/a", "", "invalid add-on name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := def.Parse(spiffeid.RequireFromString(c.id))
			if c.wantErr != "" {
				require.ErrorContains(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.wantUser, got.User)
			require.Contains(t, got.Groups, "system:authenticated")
			require.Equal(t, spiffeid.RequireFromString(c.id).TrustDomain().Name(), got.TrustDomain)
		})
	}
}

func TestIDSchemeGroups(t *testing.T) {
	def := mustScheme(t, v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate)

	work, err := def.Parse(spiffeid.RequireFromString("spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"))
	require.NoError(t, err)
	require.Equal(t, OCMIdentity{
		TrustDomain: "ocm-hub.example.org",
		ClusterName: "cluster1",
		Role:        RoleWork,
		User:        "system:open-cluster-management:cluster1:work",
		Groups: []string{
			"system:open-cluster-management:cluster1",
			"system:open-cluster-management:managed-clusters",
			"system:open-cluster-management:spiffe-agents",
			"system:authenticated",
		},
	}, work)
	require.False(t, work.IsAddOn())

	addon, err := def.Parse(spiffeid.RequireFromString(
		"spiffe://ocm-hub.example.org/ocm/cluster/cluster1/addon/my-addon/agent/my-addon-agent"))
	require.NoError(t, err)
	require.Equal(t, OCMIdentity{
		TrustDomain: "ocm-hub.example.org",
		ClusterName: "cluster1",
		AddOnName:   "my-addon",
		AgentName:   "my-addon-agent",
		User:        "system:open-cluster-management:cluster:cluster1:addon:my-addon:agent:my-addon-agent",
		Groups: []string{
			"system:open-cluster-management:cluster:cluster1:addon:my-addon",
			"system:open-cluster-management:addon:my-addon",
			"system:authenticated",
		},
	}, addon)
	require.True(t, addon.IsAddOn())
}

func TestIDSchemeParseNamespaceTemplate(t *testing.T) {
	ns := mustScheme(t, namespaceAgentTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate)
	cases := []struct {
		name          string
		id            string
		wantUser      string
		wantNamespace string
		wantErr       string
	}{
		{"work", "spiffe://ocm-hub.example.org/k8s/cluster1/ns/team-a-ocm/ocm/work",
			"system:open-cluster-management:cluster1:work", "team-a-ocm", ""},
		{"bad namespace", "spiffe://ocm-hub.example.org/k8s/cluster1/ns/Team_A/ocm/work", "", "", "invalid namespace name"},
		{"default layout", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work", "", "", "matches neither"},
		{"add-on, default layout", "spiffe://ocm-hub.example.org/ocm/cluster/cluster1/addon/my-addon/agent/my-addon-agent",
			"system:open-cluster-management:cluster:cluster1:addon:my-addon:agent:my-addon-agent", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ns.Parse(spiffeid.RequireFromString(c.id))
			if c.wantErr != "" {
				require.ErrorContains(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.wantUser, got.User)
			require.Equal(t, c.wantNamespace, got.Namespace)
		})
	}

	// The namespace never changes who the agent is.
	a, err := ns.Parse(spiffeid.RequireFromString("spiffe://ocm-hub.example.org/k8s/cluster1/ns/team-a/ocm/work"))
	require.NoError(t, err)
	b, err := ns.Parse(spiffeid.RequireFromString("spiffe://ocm-hub.example.org/k8s/cluster1/ns/team-b/ocm/work"))
	require.NoError(t, err)
	require.Equal(t, a.User, b.User)
	require.Equal(t, a.Groups, b.Groups)
}

func TestNewIDScheme(t *testing.T) {
	cases := []struct {
		name         string
		agent, addOn string
		tds          []string
		wantErr      string
	}{
		{"defaults", v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate, testTrustDomains, ""},
		{"overlap", "spiffe://{trustDomain}/ocm/{cluster}/x/{role}", "spiffe://{trustDomain}/ocm/{cluster}/{addon}/{agent}",
			testTrustDomains, "can match the same SPIFFE ID"},
		{"bad agent template", "spiffe://{trustDomain}/{cluster}/{role}", v1.DefaultSPIFFEAddOnAgentIDTemplate,
			testTrustDomains, "agentIDTemplate"},
		{"bad add-on template", v1.DefaultSPIFFEAgentIDTemplate, "spiffe://{trustDomain}/ocm/{cluster}/{addon}",
			testTrustDomains, "addOnAgentIDTemplate"},
		{"no trust domains", v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate, nil, "no agent trust domain"},
		{"bad trust domain", v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate, []string{"OCM_HUB"}, "OCM_HUB"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewIDScheme(c.agent, c.addOn, c.tds)
			if c.wantErr != "" {
				require.ErrorContains(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestIDSchemeCheckServerID(t *testing.T) {
	def := mustScheme(t, v1.DefaultSPIFFEAgentIDTemplate, v1.DefaultSPIFFEAddOnAgentIDTemplate)
	require.NoError(t, def.CheckServerID(spiffeid.RequireFromString("spiffe://ocm-hub.example.org/ocm/hub/grpc-server")))
	require.ErrorContains(t,
		def.CheckServerID(spiffeid.RequireFromString("spiffe://ocm-hub.example.org/ocm/cluster/hub/agent/work")),
		"agents could present it")
	require.Len(t, def.TrustDomains(), 2)
}
