package pubsub

import (
	"testing"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestNewSourceOptions(t *testing.T) {
	cases := []struct {
		name       string
		pubsubOpts *PubSubOptions
		sourceID   string
	}{
		{
			name: "valid source options",
			pubsubOpts: &PubSubOptions{
				ProjectID: "test-project",
				Endpoint:  "localhost:8085",
				Topics: types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
			},
			sourceID: "source-1",
		},
		{
			name: "source options with credentials",
			pubsubOpts: &PubSubOptions{
				ProjectID:       "test-project-2",
				Endpoint:        "https://pubsub.googleapis.com",
				CredentialsFile: "/path/to/credentials.json",
				Topics: types.Topics{
					SourceEvents:    "projects/test-project-2/topics/sourceevents",
					SourceBroadcast: "projects/test-project-2/topics/sourcebroadcast",
				},
				Subscriptions: types.Subscriptions{
					AgentEvents:    "projects/test-project-2/subscriptions/agentevents-source2",
					AgentBroadcast: "projects/test-project-2/subscriptions/agentbroadcast-source2",
				},
			},
			sourceID: "source-2",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := NewSourceOptions(c.pubsubOpts, c.sourceID)

			if opts == nil {
				t.Fatal("expected options, got nil")
			}

			if opts.SourceID != c.sourceID {
				t.Errorf("expected SourceID to be %q, got %q", c.sourceID, opts.SourceID)
			}

			if opts.CloudEventsTransport == nil {
				t.Fatal("expected CloudEventsTransport to be set, got nil")
			}

			transport, ok := opts.CloudEventsTransport.(*pubsubTransport)
			if !ok {
				t.Fatalf("expected transport to be *pubsubTransport, got %T", opts.CloudEventsTransport)
			}

			if transport.sourceID != c.sourceID {
				t.Errorf("expected transport sourceID to be %q, got %q", c.sourceID, transport.sourceID)
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

func TestPubsubSourceTransport_Structure(t *testing.T) {
	pubsubOpts := &PubSubOptions{
		ProjectID: "test-project",
		Topics: types.Topics{
			SourceEvents:    "projects/test-project/topics/sourceevents",
			SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
		},
		Subscriptions: types.Subscriptions{
			AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
			AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
		},
	}

	opts := NewSourceOptions(pubsubOpts, "test-source")
	transport := opts.CloudEventsTransport.(*pubsubTransport)

	// Verify topics are set correctly for source
	if transport.Topics.SourceEvents != pubsubOpts.Topics.SourceEvents {
		t.Errorf("expected SourceEvents topic %q, got %q", pubsubOpts.Topics.SourceEvents, transport.Topics.SourceEvents)
	}
	if transport.Topics.SourceBroadcast != pubsubOpts.Topics.SourceBroadcast {
		t.Errorf("expected SourceBroadcast topic %q, got %q", pubsubOpts.Topics.SourceBroadcast, transport.Topics.SourceBroadcast)
	}

	// Verify subscriptions are set correctly for source
	if transport.Subscriptions.AgentEvents != pubsubOpts.Subscriptions.AgentEvents {
		t.Errorf("expected AgentEvents subscription %q, got %q", pubsubOpts.Subscriptions.AgentEvents, transport.Subscriptions.AgentEvents)
	}
	if transport.Subscriptions.AgentBroadcast != pubsubOpts.Subscriptions.AgentBroadcast {
		t.Errorf("expected AgentBroadcast subscription %q, got %q", pubsubOpts.Subscriptions.AgentBroadcast, transport.Subscriptions.AgentBroadcast)
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
