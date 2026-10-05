package authn

import (
	"context"
	"strings"
	"testing"
)

func TestValidateClusterIdentity(t *testing.T) {
	cases := []struct {
		name        string
		ctx         context.Context
		clusterName string
		wantErr     string
	}{
		{
			name:        "no identity in context",
			ctx:         context.Background(),
			clusterName: "cluster1",
			wantErr:     "no authenticated identity",
		},
		{
			name:        "empty identity",
			ctx:         context.WithValue(context.Background(), ContextUserKey, ""),
			clusterName: "cluster1",
			wantErr:     "no authenticated identity",
		},
		{
			name:        "identity with unexpected type",
			ctx:         context.WithValue(context.Background(), ContextUserKey, 42),
			clusterName: "cluster1",
			wantErr:     "no authenticated identity",
		},
		{
			name:        "non cluster identity is not bound to a cluster",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:serviceaccount:open-cluster-management:agent-registration-bootstrap"),
			clusterName: "cluster1",
		},
		{
			name:        "cluster agent identity matches",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management:cluster1:agent-abc"),
			clusterName: "cluster1",
		},
		{
			name:        "cluster agent identity for another cluster",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management:cluster1:agent-abc"),
			clusterName: "cluster2",
			wantErr:     "is not allowed to act on cluster \"cluster2\"",
		},
		{
			name:        "addon agent identity matches",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management:cluster:cluster1:addon:test-addon:agent:agent-abc"),
			clusterName: "cluster1",
		},
		{
			name:        "addon agent identity for another cluster",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management:cluster:cluster1:addon:test-addon:agent:agent-abc"),
			clusterName: "cluster2",
			wantErr:     "is not allowed to act on cluster \"cluster2\"",
		},
		{
			name:        "cluster identity without agent name",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management:cluster1"),
			clusterName: "cluster1",
			wantErr:     "does not encode a cluster name",
		},
		{
			name:        "cluster identity with empty cluster name",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management::agent-abc"),
			clusterName: "",
			wantErr:     "does not encode a cluster name",
		},
		{
			name:        "addon identity with wrong layout",
			ctx:         context.WithValue(context.Background(), ContextUserKey, "system:open-cluster-management:cluster:cluster1:foo:test-addon:agent:agent-abc"),
			clusterName: "cluster1",
			wantErr:     "does not encode a cluster name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateClusterIdentity(tc.ctx, tc.clusterName)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
