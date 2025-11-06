package pubsub

import (
	"testing"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestNewAgentOptions(t *testing.T) {
	cases := []struct {
		name        string
		pubsubOpts  *PubSubOptions
		clusterName string
		agentID     string
	}{
		{
			name: "valid agent options",
			pubsubOpts: &PubSubOptions{
				ProjectID: "test-project",
				Endpoint:  "localhost:8085",
				Topics: types.Topics{
					AgentEvents:    "projects/test-project/topics/agentevents",
					AgentBroadcast: "projects/test-project/topics/agentbroadcast",
				},
				Subscriptions: types.Subscriptions{
					SourceEvents:    "projects/test-project/subscriptions/sourceevents-cluster1",
					SourceBroadcast: "projects/test-project/subscriptions/sourcebroadcast-cluster1",
				},
			},
			clusterName: "cluster1",
			agentID:     "agent-1",
		},
		{
			name: "agent options with credentials",
			pubsubOpts: &PubSubOptions{
				ProjectID:       "test-project-2",
				Endpoint:        "https://pubsub.googleapis.com",
				CredentialsFile: "/path/to/credentials.json",
				Topics: types.Topics{
					AgentEvents:    "projects/test-project-2/topics/agentevents",
					AgentBroadcast: "projects/test-project-2/topics/agentbroadcast",
				},
				Subscriptions: types.Subscriptions{
					SourceEvents:    "projects/test-project-2/subscriptions/sourceevents-cluster2",
					SourceBroadcast: "projects/test-project-2/subscriptions/sourcebroadcast-cluster2",
				},
			},
			clusterName: "cluster2",
			agentID:     "agent-2",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := NewAgentOptions(c.pubsubOpts, c.clusterName, c.agentID)

			if opts == nil {
				t.Fatal("expected options, got nil")
			}

			if opts.AgentID != c.agentID {
				t.Errorf("expected AgentID to be %q, got %q", c.agentID, opts.AgentID)
			}

			if opts.ClusterName != c.clusterName {
				t.Errorf("expected ClusterName to be %q, got %q", c.clusterName, opts.ClusterName)
			}

			if opts.CloudEventsTransport == nil {
				t.Fatal("expected CloudEventsTransport to be set, got nil")
			}

			transport, ok := opts.CloudEventsTransport.(*pubsubTransport)
			if !ok {
				t.Fatalf("expected transport to be *pubsubTransport, got %T", opts.CloudEventsTransport)
			}

			if transport.clusterName != c.clusterName {
				t.Errorf("expected transport clusterName to be %q, got %q", c.clusterName, transport.clusterName)
			}

			if transport.ProjectID != c.pubsubOpts.ProjectID {
				t.Errorf("expected transport ProjectID to be %q, got %q", c.pubsubOpts.ProjectID, transport.ProjectID)
			}

			if transport.Endpoint != c.pubsubOpts.Endpoint {
				t.Errorf("expected transport Endpoint to be %q, got %q", c.pubsubOpts.Endpoint, transport.Endpoint)
			}

			if transport.CredentialsFile != c.pubsubOpts.CredentialsFile {
				t.Errorf("expected transport CredentialsFile to be %q, got %q", c.pubsubOpts.CredentialsFile, transport.CredentialsFile)
			}

			if transport.errorChan == nil {
				t.Error("expected errorChan to be initialized, got nil")
			}
		})
	}
}

func TestPubsubAgentTransport_Structure(t *testing.T) {
	pubsubOpts := &PubSubOptions{
		ProjectID: "test-project",
		Topics: types.Topics{
			AgentEvents:    "projects/test-project/topics/agentevents",
			AgentBroadcast: "projects/test-project/topics/agentbroadcast",
		},
		Subscriptions: types.Subscriptions{
			SourceEvents:    "projects/test-project/subscriptions/sourceevents-cluster1",
			SourceBroadcast: "projects/test-project/subscriptions/sourcebroadcast-cluster1",
		},
	}

	opts := NewAgentOptions(pubsubOpts, "test-cluster", "test-agent")
	transport := opts.CloudEventsTransport.(*pubsubTransport)

	// Verify topics are set correctly for agent
	if transport.Topics.AgentEvents != pubsubOpts.Topics.AgentEvents {
		t.Errorf("expected AgentEvents topic %q, got %q", pubsubOpts.Topics.AgentEvents, transport.Topics.AgentEvents)
	}
	if transport.Topics.AgentBroadcast != pubsubOpts.Topics.AgentBroadcast {
		t.Errorf("expected AgentBroadcast topic %q, got %q", pubsubOpts.Topics.AgentBroadcast, transport.Topics.AgentBroadcast)
	}

	// Verify subscriptions are set correctly for agent
	if transport.Subscriptions.SourceEvents != pubsubOpts.Subscriptions.SourceEvents {
		t.Errorf("expected SourceEvents subscription %q, got %q", pubsubOpts.Subscriptions.SourceEvents, transport.Subscriptions.SourceEvents)
	}
	if transport.Subscriptions.SourceBroadcast != pubsubOpts.Subscriptions.SourceBroadcast {
		t.Errorf("expected SourceBroadcast subscription %q, got %q", pubsubOpts.Subscriptions.SourceBroadcast, transport.Subscriptions.SourceBroadcast)
	}

	// Verify client is initially nil
	if transport.client != nil {
		t.Error("expected client to be nil before Connect is called")
	}

	// Verify publishers are initially nil
	if transport.publisher != nil {
		t.Error("expected publisher to be nil before Connect is called")
	}
	if transport.resyncPublisher != nil {
		t.Error("expected resyncPublisher to be nil before Connect is called")
	}

	// Verify subscribers are initially nil
	if transport.subscriber != nil {
		t.Error("expected subscriber to be nil before Connect is called")
	}
	if transport.resyncSubscriber != nil {
		t.Error("expected resyncSubscriber to be nil before Connect is called")
	}
}
