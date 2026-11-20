package idtemplate

import (
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/require"
)

const (
	defaultAgent  = "spiffe://{trustDomain}/ocm/cluster/{cluster}/agent/{role}"
	defaultAddOn  = "spiffe://{trustDomain}/ocm/cluster/{cluster}/addon/{addon}/agent/{agent}"
	namespaceTmpl = "spiffe://{trustDomain}/k8s/{cluster}/ns/{namespace}/ocm/{role}"
)

func TestCompile(t *testing.T) {
	cases := []struct {
		name    string
		kind    Kind
		tmpl    string
		wantErr string
	}{
		{"default agent", KindAgent, defaultAgent, ""},
		{"default add-on", KindAddOnAgent, defaultAddOn, ""},
		{"with namespace", KindAgent, namespaceTmpl, ""},
		{"add-on with namespace", KindAddOnAgent, "spiffe://{trustDomain}/k8s/{cluster}/ns/{namespace}/addon/{addon}/{agent}", ""},
		{"wrong prefix", KindAgent, "spiffe://ocm-hub.example.org/ocm/{cluster}/{role}", "must start with spiffe://{trustDomain}/"},
		{"missing role", KindAgent, "spiffe://{trustDomain}/ocm/cluster/{cluster}", "must contain {role}"},
		{"missing cluster", KindAgent, "spiffe://{trustDomain}/ocm/agent/{role}", "must contain {cluster}"},
		{"missing agent", KindAddOnAgent, "spiffe://{trustDomain}/ocm/{cluster}/{addon}", "must contain {agent}"},
		{"unknown", KindAgent, "spiffe://{trustDomain}/ocm/{clutser}/{role}", "unknown placeholder {clutser}"},
		{"add-on placeholder in agent template", KindAgent, "spiffe://{trustDomain}/ocm/{cluster}/{role}/{addon}", "unknown placeholder {addon}"},
		{"role in add-on template", KindAddOnAgent, "spiffe://{trustDomain}/ocm/{cluster}/{addon}/{agent}/{role}", "unknown placeholder {role}"},
		{"trust domain in path", KindAgent, "spiffe://{trustDomain}/ocm/{trustDomain}/{cluster}/{role}", "unknown placeholder {trustDomain}"},
		{"twice", KindAgent, "spiffe://{trustDomain}/ocm/{cluster}/{role}/{cluster}", "appears more than once"},
		{"partial segment", KindAgent, "spiffe://{trustDomain}/ocm/cluster-{cluster}/{role}", "must fill a whole path segment"},
		{"no fixed segment", KindAgent, "spiffe://{trustDomain}/{cluster}/{role}", "needs at least one fixed path segment"},
		{"empty segment", KindAgent, "spiffe://{trustDomain}/ocm//{cluster}/{role}", "segment"},
		{"trailing slash", KindAgent, "spiffe://{trustDomain}/ocm/{cluster}/{role}/", "segment"},
		{"bad character", KindAgent, "spiffe://{trustDomain}/ocm/{cluster}/{role}/a?b", "segment"},
		{"dot segment", KindAgent, "spiffe://{trustDomain}/ocm/../{cluster}/{role}", "segment"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl, err := Compile(c.kind, c.tmpl)
			if c.wantErr != "" {
				require.ErrorContains(t, err, c.wantErr)
				require.ErrorContains(t, err, string(c.kind))
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.tmpl, tmpl.String())
		})
	}
}

func TestRenderMatch(t *testing.T) {
	cases := []struct {
		name   string
		kind   Kind
		tmpl   string
		values map[string]string
		wantID string
	}{
		{"default agent", KindAgent, defaultAgent,
			map[string]string{Cluster: "cluster1", Role: "work", Namespace: "open-cluster-management-agent"},
			"spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work"},
		{"default add-on", KindAddOnAgent, defaultAddOn,
			map[string]string{Cluster: "cluster1", AddOn: "my-addon", Agent: "my-addon-agent"},
			"spiffe://ocm-hub.example.org/ocm/cluster/cluster1/addon/my-addon/agent/my-addon-agent"},
		{"namespace template", KindAgent, namespaceTmpl,
			map[string]string{Cluster: "cluster1", Role: "work", Namespace: "open-cluster-management-agent"},
			"spiffe://ocm-hub.example.org/k8s/cluster1/ns/open-cluster-management-agent/ocm/work"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl, err := Compile(c.kind, c.tmpl)
			require.NoError(t, err)
			id, err := tmpl.Render("ocm-hub.example.org", c.values)
			require.NoError(t, err)
			require.Equal(t, c.wantID, id.String())

			got, ok := tmpl.Match(id)
			require.True(t, ok)
			for name, v := range got {
				require.Equal(t, c.values[name], v, name)
			}
			for _, name := range []string{Cluster, Role, AddOn, Agent, Namespace} {
				if tmpl.Has(name) {
					require.Contains(t, got, name)
				} else {
					require.NotContains(t, got, name)
				}
			}
		})
	}
}

func TestRenderErrors(t *testing.T) {
	tmpl, err := Compile(KindAgent, defaultAgent)
	require.NoError(t, err)

	_, err = tmpl.Render("ocm-hub.example.org", map[string]string{Role: "work"})
	require.ErrorContains(t, err, "no value for {cluster}")

	_, err = tmpl.Render("OCM_HUB", map[string]string{Cluster: "cluster1", Role: "work"})
	require.Error(t, err)

	_, err = tmpl.Render("ocm-hub.example.org", map[string]string{Cluster: "a/b", Role: "work"})
	require.Error(t, err, "a value with a slash would add a segment")
}

func TestMatchRejects(t *testing.T) {
	tmpl, err := Compile(KindAgent, defaultAgent)
	require.NoError(t, err)
	for _, id := range []string{
		"spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent",
		"spiffe://ocm-hub.example.org/ocm/cluster/cluster1/agent/work/x",
		"spiffe://ocm-hub.example.org/ocm/clusters/cluster1/agent/work",
		"spiffe://ocm-hub.example.org/ns/open-cluster-management-agent/sa/klusterlet-work-sa",
		"spiffe://ocm-hub.example.org/ocm/hub/grpc-server",
	} {
		_, ok := tmpl.Match(spiffeid.RequireFromString(id))
		require.False(t, ok, id)
	}
}

func TestOverlaps(t *testing.T) {
	mustAgent := func(s string) *Template {
		tmpl, err := Compile(KindAgent, s)
		require.NoError(t, err)
		return tmpl
	}
	mustAddOn := func(s string) *Template {
		tmpl, err := Compile(KindAddOnAgent, s)
		require.NoError(t, err)
		return tmpl
	}
	cases := []struct {
		name         string
		agent, addOn string
		want         bool
	}{
		{"defaults", defaultAgent, defaultAddOn, false},
		{"different lengths", "spiffe://{trustDomain}/ocm/{cluster}/{role}", "spiffe://{trustDomain}/ocm/{cluster}/{addon}/{agent}", false},
		{"same length, different fixed text", "spiffe://{trustDomain}/ocm/{cluster}/agent/{role}", "spiffe://{trustDomain}/ocm/{cluster}/{addon}/{agent}", true},
		{"fixed text against placeholder", "spiffe://{trustDomain}/ocm/{cluster}/x/{role}", "spiffe://{trustDomain}/ocm/{cluster}/{addon}/{agent}", true},
		{"two fixed segments differ", "spiffe://{trustDomain}/agents/{cluster}/{role}/x", "spiffe://{trustDomain}/addons/{cluster}/{addon}/{agent}", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, Overlaps(mustAgent(c.agent), mustAddOn(c.addOn)))
		})
	}
}
