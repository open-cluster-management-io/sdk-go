package pubsub

import (
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	clienttesting "open-cluster-management.io/sdk-go/pkg/testing"
)

const (
	// Source configuration - includes source topics and agent subscriptions
	testSourceYamlConfig = `
projectID: test-project
topics:
  sourceEvents: projects/test-project/topics/sourceevents
  sourceBroadcast: projects/test-project/topics/sourcebroadcast
subscriptions:
  agentEvents: projects/test-project/subscriptions/agentevents-source1
  agentBroadcast: projects/test-project/subscriptions/agentbroadcast-source1
`
	// Agent configuration - includes agent topics and source subscriptions
	testAgentYamlConfig = `
projectID: test-project
topics:
  agentEvents: projects/test-project/topics/agentevents
  agentBroadcast: projects/test-project/topics/agentbroadcast
subscriptions:
  sourceEvents: projects/test-project/subscriptions/sourceevents-cluster1
  sourceBroadcast: projects/test-project/subscriptions/sourcebroadcast-cluster1
`
	testSourceYamlConfigWithOptionalFields = `
projectID: test-project
endpoint: https://us-west1-pubsub.googleapis.com
credentialsFile: /path/to/credentials.json
topics:
  sourceEvents: projects/test-project/topics/sourceevents
  sourceBroadcast: projects/test-project/topics/sourcebroadcast
subscriptions:
  agentEvents: projects/test-project/subscriptions/agentevents-source1
  agentBroadcast: projects/test-project/subscriptions/agentbroadcast-source1
`
	testSourceYamlConfigWithKeepaliveAndReceiveSettings = `
projectID: test-project
topics:
  sourceEvents: projects/test-project/topics/sourceevents
  sourceBroadcast: projects/test-project/topics/sourcebroadcast
subscriptions:
  agentEvents: projects/test-project/subscriptions/agentevents-source1
  agentBroadcast: projects/test-project/subscriptions/agentbroadcast-source1
keepaliveSettings:
  time: 10m
  timeout: 30s
  permitWithoutStream: true
receiveSettings:
  maxExtension: 600s
  maxDurationPerAckExtension: 10s
  minDurationPerAckExtension: 1s
  maxOutstandingMessages: 1000
  maxOutstandingBytes: 1000000000
  numGoroutines: 10
`
	testSourceJSONConfig = `
{
	"projectID": "test-project",
	"topics": {
		"sourceEvents": "projects/test-project/topics/sourceevents",
		"sourceBroadcast": "projects/test-project/topics/sourcebroadcast"
	},
	"subscriptions": {
		"agentEvents": "projects/test-project/subscriptions/agentevents-source1",
		"agentBroadcast": "projects/test-project/subscriptions/agentbroadcast-source1"
	}
}
`
)

func TestBuildPubSubOptionsFromFlags(t *testing.T) {
	cases := []struct {
		name             string
		config           string
		expectedOptions  *PubSubOptions
		expectedErrorMsg string
	}{
		{
			name:             "empty config",
			config:           "",
			expectedErrorMsg: "projectID is required",
		},
		{
			name:             "missing projectID",
			config:           "{\"topics\":{\"sourceEvents\":\"projects/test-project/topics/sourceevents\"}}",
			expectedErrorMsg: "projectID is required",
		},
		{
			name:             "invalid projectID - too short",
			config:           "{\"projectID\":\"short\",\"topics\":{\"sourceEvents\":\"projects/short/topics/sourceevents\",\"sourceBroadcast\":\"projects/short/topics/sourcebroadcast\"},\"subscriptions\":{\"agentEvents\":\"projects/short/subscriptions/agentevents-source1\",\"agentBroadcast\":\"projects/short/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "must be 6-30 characters long",
		},
		{
			name:             "invalid projectID - starts with number",
			config:           "{\"projectID\":\"1project\",\"topics\":{\"sourceEvents\":\"projects/1project/topics/sourceevents\",\"sourceBroadcast\":\"projects/1project/topics/sourcebroadcast\"},\"subscriptions\":{\"agentEvents\":\"projects/1project/subscriptions/agentevents-source1\",\"agentBroadcast\":\"projects/1project/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "must start with a lowercase letter",
		},
		{
			name:             "invalid projectID - ends with hyphen",
			config:           "{\"projectID\":\"project-\",\"topics\":{\"sourceEvents\":\"projects/project-/topics/sourceevents\",\"sourceBroadcast\":\"projects/project-/topics/sourcebroadcast\"},\"subscriptions\":{\"agentEvents\":\"projects/project-/subscriptions/agentevents-source1\",\"agentBroadcast\":\"projects/project-/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "cannot end with a hyphen",
		},
		{
			name:             "invalid projectID - contains uppercase",
			config:           "{\"projectID\":\"MyProject\",\"topics\":{\"sourceEvents\":\"projects/MyProject/topics/sourceevents\",\"sourceBroadcast\":\"projects/MyProject/topics/sourcebroadcast\"},\"subscriptions\":{\"agentEvents\":\"projects/MyProject/subscriptions/agentevents-source1\",\"agentBroadcast\":\"projects/MyProject/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "must start with a lowercase letter",
		},
		{
			name:             "missing topics",
			config:           "{\"projectID\":\"test-project\"}",
			expectedErrorMsg: "the topics must be set",
		},
		{
			name:             "missing subscriptions",
			config:           "{\"projectID\":\"test-project\",\"topics\":{\"sourceEvents\":\"projects/test-project/topics/sourceevents\",\"sourceBroadcast\":\"projects/test-project/topics/sourcebroadcast\"}}",
			expectedErrorMsg: "the subscriptions must be set",
		},
		{
			name:             "invalid topic/subscription combination - missing sourceBroadcast for source",
			config:           "{\"projectID\":\"test-project\",\"topics\":{\"sourceEvents\":\"projects/test-project/topics/sourceevents\"},\"subscriptions\":{\"agentEvents\":\"projects/test-project/subscriptions/agentevents-source1\",\"agentBroadcast\":\"projects/test-project/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "invalid topic/subscription combination",
		},
		{
			name:             "invalid topic/subscription combination - missing agentBroadcast for agent",
			config:           "{\"projectID\":\"test-project\",\"topics\":{\"agentEvents\":\"projects/test-project/topics/agentevents\"},\"subscriptions\":{\"sourceEvents\":\"projects/test-project/subscriptions/sourceevents-cluster1\",\"sourceBroadcast\":\"projects/test-project/subscriptions/sourcebroadcast-cluster1\"}}",
			expectedErrorMsg: "invalid topic/subscription combination",
		},
		{
			name:             "invalid source events topic format",
			config:           "{\"projectID\":\"test-project\",\"topics\":{\"sourceEvents\":\"invalid-topic\",\"sourceBroadcast\":\"projects/test-project/topics/sourcebroadcast\"},\"subscriptions\":{\"agentEvents\":\"projects/test-project/subscriptions/agentevents-source1\",\"agentBroadcast\":\"projects/test-project/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "invalid source events topic \"invalid-topic\"",
		},
		{
			name:             "invalid agent events subscription format",
			config:           "{\"projectID\":\"test-project\",\"topics\":{\"sourceEvents\":\"projects/test-project/topics/sourceevents\",\"sourceBroadcast\":\"projects/test-project/topics/sourcebroadcast\"},\"subscriptions\":{\"agentEvents\":\"invalid-subscription\",\"agentBroadcast\":\"projects/test-project/subscriptions/agentbroadcast-source1\"}}",
			expectedErrorMsg: "invalid agent events subscription \"invalid-subscription\"",
		},
		{
			name:   "valid source yaml config",
			config: testSourceYamlConfig,
			expectedOptions: &PubSubOptions{
				ProjectID: "test-project",
				Topics: types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
				KeepaliveSettings: &KeepaliveSettings{
					Time:                5 * time.Minute,
					Timeout:             20 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "valid source json config",
			config: testSourceJSONConfig,
			expectedOptions: &PubSubOptions{
				ProjectID: "test-project",
				Topics: types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
				KeepaliveSettings: &KeepaliveSettings{
					Time:                5 * time.Minute,
					Timeout:             20 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "valid source config with optional fields",
			config: testSourceYamlConfigWithOptionalFields,
			expectedOptions: &PubSubOptions{
				ProjectID:       "test-project",
				Endpoint:        "https://us-west1-pubsub.googleapis.com",
				CredentialsFile: "/path/to/credentials.json",
				Topics: types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
				KeepaliveSettings: &KeepaliveSettings{
					Time:                5 * time.Minute,
					Timeout:             20 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "valid agent yaml config",
			config: testAgentYamlConfig,
			expectedOptions: &PubSubOptions{
				ProjectID: "test-project",
				Topics: types.Topics{
					AgentEvents:    "projects/test-project/topics/agentevents",
					AgentBroadcast: "projects/test-project/topics/agentbroadcast",
				},
				Subscriptions: types.Subscriptions{
					SourceEvents:    "projects/test-project/subscriptions/sourceevents-cluster1",
					SourceBroadcast: "projects/test-project/subscriptions/sourcebroadcast-cluster1",
				},
				KeepaliveSettings: &KeepaliveSettings{
					Time:                5 * time.Minute,
					Timeout:             20 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "valid config with keepalive and receive settings",
			config: testSourceYamlConfigWithKeepaliveAndReceiveSettings,
			expectedOptions: &PubSubOptions{
				ProjectID: "test-project",
				Topics: types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
				KeepaliveSettings: &KeepaliveSettings{
					Time:                10 * time.Minute,
					Timeout:             30 * time.Second,
					PermitWithoutStream: true,
				},
				ReceiveSettings: &ReceiveSettings{
					MaxExtension:               600 * time.Second,
					MaxDurationPerAckExtension: 10 * time.Second,
					MinDurationPerAckExtension: 1 * time.Second,
					MaxOutstandingMessages:     1000,
					MaxOutstandingBytes:        1000000000,
					NumGoroutines:              10,
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			file, err := clienttesting.WriteToTempFile("pubsub-config-test-", []byte(c.config))
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(file.Name())

			options, err := BuildPubSubOptionsFromFlags(file.Name())
			if err != nil {
				if c.expectedErrorMsg == "" {
					t.Errorf("unexpected error: %v", err)
				} else if !strings.Contains(err.Error(), c.expectedErrorMsg) {
					t.Errorf("expected error to contain %q, got %q", c.expectedErrorMsg, err.Error())
				}
				return
			}

			if c.expectedErrorMsg != "" {
				t.Errorf("expected error %q, but got none", c.expectedErrorMsg)
				return
			}

			if !equality.Semantic.DeepEqual(options, c.expectedOptions) {
				t.Errorf("unexpected options: got %+v, want %+v", options, c.expectedOptions)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	cases := []struct {
		name             string
		config           string
		expectedConfig   *PubSubConfig
		expectedErrorMsg string
	}{
		{
			name:             "file not found",
			config:           "",
			expectedConfig:   nil,
			expectedErrorMsg: "no such file or directory",
		},
		{
			name:   "valid source yaml config",
			config: testSourceYamlConfig,
			expectedConfig: &PubSubConfig{
				ProjectID: "test-project",
				Topics: &types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: &types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
			},
		},
		{
			name:   "valid source json config",
			config: testSourceJSONConfig,
			expectedConfig: &PubSubConfig{
				ProjectID: "test-project",
				Topics: &types.Topics{
					SourceEvents:    "projects/test-project/topics/sourceevents",
					SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
				},
				Subscriptions: &types.Subscriptions{
					AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
					AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var configPath string
			if c.config != "" {
				file, err := clienttesting.WriteToTempFile("pubsub-config-test-", []byte(c.config))
				if err != nil {
					t.Fatal(err)
				}
				defer os.Remove(file.Name())
				configPath = file.Name()
			} else {
				configPath = "/non/existent/path"
			}

			config, err := LoadConfig(configPath)
			if err != nil {
				if c.expectedErrorMsg == "" {
					t.Errorf("unexpected error: %v", err)
				} else if !strings.Contains(err.Error(), c.expectedErrorMsg) {
					t.Errorf("expected error to contain %q, got %q", c.expectedErrorMsg, err.Error())
				}
				return
			}

			if c.expectedErrorMsg != "" {
				t.Errorf("expected error containing %q, but got none", c.expectedErrorMsg)
				return
			}

			if !equality.Semantic.DeepEqual(config, c.expectedConfig) {
				t.Errorf("unexpected config: got %+v, want %+v", config, c.expectedConfig)
			}
		})
	}
}

func TestValidateTopicsAndSubscriptions(t *testing.T) {
	cases := []struct {
		name          string
		topics        *types.Topics
		subscriptions *types.Subscriptions
		projectID     string
		expectedErr   bool
	}{
		{
			name:          "nil topics",
			topics:        nil,
			subscriptions: &types.Subscriptions{},
			projectID:     "test-project",
			expectedErr:   true,
		},
		{
			name:          "nil subscriptions",
			topics:        &types.Topics{},
			subscriptions: nil,
			projectID:     "test-project",
			expectedErr:   true,
		},
		{
			name: "valid source configuration",
			topics: &types.Topics{
				SourceEvents:    "projects/test-project/topics/sourceevents",
				SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
			},
			subscriptions: &types.Subscriptions{
				AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
				AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project",
			expectedErr: false,
		},
		{
			name: "valid agent configuration",
			topics: &types.Topics{
				AgentEvents:    "projects/test-project/topics/agentevents",
				AgentBroadcast: "projects/test-project/topics/agentbroadcast",
			},
			subscriptions: &types.Subscriptions{
				SourceEvents:    "projects/test-project/subscriptions/sourceevents-cluster1",
				SourceBroadcast: "projects/test-project/subscriptions/sourcebroadcast-cluster1",
			},
			projectID:   "test-project",
			expectedErr: false,
		},
		{
			name: "invalid - missing sourceBroadcast topic for source",
			topics: &types.Topics{
				SourceEvents: "projects/test-project/topics/sourceevents",
			},
			subscriptions: &types.Subscriptions{
				AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
				AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "invalid - missing agentEvents subscription for source",
			topics: &types.Topics{
				SourceEvents:    "projects/test-project/topics/sourceevents",
				SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
			},
			subscriptions: &types.Subscriptions{
				AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "invalid - missing agentEvents topic for agent",
			topics: &types.Topics{
				AgentBroadcast: "projects/test-project/topics/agentbroadcast",
			},
			subscriptions: &types.Subscriptions{
				SourceEvents:    "projects/test-project/subscriptions/sourceevents-cluster1",
				SourceBroadcast: "projects/test-project/subscriptions/sourcebroadcast-cluster1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "invalid - missing SourceBroadcast subscription for agent",
			topics: &types.Topics{
				AgentEvents:    "projects/test-project/topics/agentevents",
				AgentBroadcast: "projects/test-project/topics/agentbroadcast",
			},
			subscriptions: &types.Subscriptions{
				SourceEvents: "projects/test-project/subscriptions/sourceevents-cluster1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "invalid source events topic format",
			topics: &types.Topics{
				SourceEvents:    "invalid-topic",
				SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
			},
			subscriptions: &types.Subscriptions{
				AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
				AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "invalid agent events subscription format",
			topics: &types.Topics{
				SourceEvents:    "projects/test-project/topics/sourceevents",
				SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
			},
			subscriptions: &types.Subscriptions{
				AgentEvents:    "invalid-subscription",
				AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "wrong project id in topic",
			topics: &types.Topics{
				SourceEvents:    "projects/different-project/topics/sourceevents",
				SourceBroadcast: "projects/test-project/topics/sourcebroadcast",
			},
			subscriptions: &types.Subscriptions{
				AgentEvents:    "projects/test-project/subscriptions/agentevents-source1",
				AgentBroadcast: "projects/test-project/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project",
			expectedErr: true,
		},
		{
			name: "project id with special chars",
			topics: &types.Topics{
				SourceEvents:    "projects/test-project-123/topics/sourceevents",
				SourceBroadcast: "projects/test-project-123/topics/sourcebroadcast",
			},
			subscriptions: &types.Subscriptions{
				AgentEvents:    "projects/test-project-123/subscriptions/agentevents-source1",
				AgentBroadcast: "projects/test-project-123/subscriptions/agentbroadcast-source1",
			},
			projectID:   "test-project-123",
			expectedErr: false,
		},
		{
			name: "subscription with UUID",
			topics: &types.Topics{
				AgentEvents:    "projects/test-project/topics/agentevents",
				AgentBroadcast: "projects/test-project/topics/agentbroadcast",
			},
			subscriptions: &types.Subscriptions{
				SourceEvents:    "projects/test-project/subscriptions/sourceevents-5328eff5-b0c7-48f3-b82e-10052abbf51d",
				SourceBroadcast: "projects/test-project/subscriptions/sourcebroadcast-cluster1",
			},
			projectID:   "test-project",
			expectedErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateTopicsAndSubscriptions(c.topics, c.subscriptions, c.projectID)
			if c.expectedErr {
				if err == nil {
					t.Errorf("expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestKeepaliveSettingsDefaults(t *testing.T) {
	// Test that default keepalive settings are applied when not provided
	config := `
projectID: test-project
topics:
  sourceEvents: projects/test-project/topics/sourceevents
  sourceBroadcast: projects/test-project/topics/sourcebroadcast
subscriptions:
  agentEvents: projects/test-project/subscriptions/agentevents-source1
  agentBroadcast: projects/test-project/subscriptions/agentbroadcast-source1
`
	file, err := clienttesting.WriteToTempFile("pubsub-config-test-", []byte(config))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	options, err := BuildPubSubOptionsFromFlags(file.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if options.KeepaliveSettings == nil {
		t.Fatal("expected KeepaliveSettings to be set with defaults, got nil")
	}

	if options.KeepaliveSettings.Time != 5*time.Minute {
		t.Errorf("expected default Time to be 5m, got %v", options.KeepaliveSettings.Time)
	}

	if options.KeepaliveSettings.Timeout != 20*time.Second {
		t.Errorf("expected default Timeout to be 20s, got %v", options.KeepaliveSettings.Timeout)
	}

	if options.KeepaliveSettings.PermitWithoutStream != false {
		t.Errorf("expected default PermitWithoutStream to be false, got %v", options.KeepaliveSettings.PermitWithoutStream)
	}
}

func TestKeepaliveSettingsOverride(t *testing.T) {
	// Test that custom keepalive settings override defaults
	config := `
projectID: test-project
topics:
  sourceEvents: projects/test-project/topics/sourceevents
  sourceBroadcast: projects/test-project/topics/sourcebroadcast
subscriptions:
  agentEvents: projects/test-project/subscriptions/agentevents-source1
  agentBroadcast: projects/test-project/subscriptions/agentbroadcast-source1
keepaliveSettings:
  time: 1m
  timeout: 10s
  permitWithoutStream: true
`
	file, err := clienttesting.WriteToTempFile("pubsub-config-test-", []byte(config))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	options, err := BuildPubSubOptionsFromFlags(file.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if options.KeepaliveSettings == nil {
		t.Fatal("expected KeepaliveSettings to be set, got nil")
	}

	if options.KeepaliveSettings.Time != 1*time.Minute {
		t.Errorf("expected Time to be 1m, got %v", options.KeepaliveSettings.Time)
	}

	if options.KeepaliveSettings.Timeout != 10*time.Second {
		t.Errorf("expected Timeout to be 10s, got %v", options.KeepaliveSettings.Timeout)
	}

	if options.KeepaliveSettings.PermitWithoutStream != true {
		t.Errorf("expected PermitWithoutStream to be true, got %v", options.KeepaliveSettings.PermitWithoutStream)
	}
}

func TestValidateProjectID(t *testing.T) {
	cases := []struct {
		name             string
		projectID        string
		expectedErrorMsg string
	}{
		{
			name:             "empty project ID",
			projectID:        "",
			expectedErrorMsg: "projectID is required",
		},
		{
			name:             "too short - 5 characters",
			projectID:        "abcde",
			expectedErrorMsg: "must be 6-30 characters long",
		},
		{
			name:             "too long - 31 characters",
			projectID:        "a123456789012345678901234567890",
			expectedErrorMsg: "must be 6-30 characters long",
		},
		{
			name:             "starts with number",
			projectID:        "1project",
			expectedErrorMsg: "must start with a lowercase letter",
		},
		{
			name:             "starts with hyphen",
			projectID:        "-project",
			expectedErrorMsg: "must start with a lowercase letter",
		},
		{
			name:             "ends with hyphen",
			projectID:        "project-",
			expectedErrorMsg: "cannot end with a hyphen",
		},
		{
			name:             "contains uppercase letters",
			projectID:        "myProject",
			expectedErrorMsg: "must start with a lowercase letter",
		},
		{
			name:             "contains special characters",
			projectID:        "my_project",
			expectedErrorMsg: "contain only lowercase letters, numbers, and hyphens",
		},
		{
			name:             "contains spaces",
			projectID:        "my project",
			expectedErrorMsg: "contain only lowercase letters, numbers, and hyphens",
		},
		{
			name:      "valid - minimum length 6",
			projectID: "abcdef",
		},
		{
			name:      "valid - maximum length 30",
			projectID: "abc123456789012345678901234567",
		},
		{
			name:      "valid - with hyphens",
			projectID: "my-project",
		},
		{
			name:      "valid - with numbers",
			projectID: "project123",
		},
		{
			name:      "valid - complex",
			projectID: "my-project-123",
		},
		{
			name:      "valid - starts with letter ends with number",
			projectID: "project1",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateProjectID(c.projectID)
			if c.expectedErrorMsg != "" {
				if err == nil {
					t.Errorf("expected error containing %q, but got none", c.expectedErrorMsg)
					return
				}
				if !strings.Contains(err.Error(), c.expectedErrorMsg) {
					t.Errorf("expected error to contain %q, got %q", c.expectedErrorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
