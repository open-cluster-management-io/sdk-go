package grpc

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestNewBrokerOptions(t *testing.T) {
	opts := NewBrokerOptions()

	if opts.HeartbeatDisabled {
		t.Errorf("Expected HeartbeatDisabled to be false by default (heartbeat enabled), got true")
	}

	if opts.HeartbeatCheckInterval != 10*time.Second {
		t.Errorf("Expected HeartbeatCheckInterval to be 10s by default, got %v", opts.HeartbeatCheckInterval)
	}
}

func TestBrokerOptions_AddFlags(t *testing.T) {
	opts := NewBrokerOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	opts.AddFlags(fs)

	// Test that flags are registered
	if fs.Lookup("broker-heartbeat-disabled") == nil {
		t.Error("broker-heartbeat-disabled flag not registered")
	}

	if fs.Lookup("broker-heartbeat-interval") == nil {
		t.Error("broker-heartbeat-interval flag not registered")
	}

	// Test parsing flags
	args := []string{
		"--broker-heartbeat-disabled=true",
		"--broker-heartbeat-interval=30s",
	}

	if err := fs.Parse(args); err != nil {
		t.Fatalf("Failed to parse flags: %v", err)
	}

	if !opts.HeartbeatDisabled {
		t.Errorf("Expected HeartbeatDisabled to be true after parsing, got false")
	}

	if opts.HeartbeatCheckInterval != 30*time.Second {
		t.Errorf("Expected HeartbeatCheckInterval to be 30s after parsing, got %v", opts.HeartbeatCheckInterval)
	}
}

func TestNewGRPCBroker_WithOptions(t *testing.T) {
	// Test with custom options
	opts := &BrokerOptions{
		HeartbeatDisabled:      true,
		HeartbeatCheckInterval: 30 * time.Second,
	}

	broker := NewGRPCBroker(opts)

	if !broker.heartbeatDisabled {
		t.Errorf("Expected heartbeatDisabled to be true, got false")
	}

	if broker.heartbeatCheckInterval != 30*time.Second {
		t.Errorf("Expected heartbeatCheckInterval to be 30s, got %v", broker.heartbeatCheckInterval)
	}
}

func TestNewGRPCBroker_WithDefaultOptions(t *testing.T) {
	broker := NewGRPCBroker(NewBrokerOptions())

	if broker.heartbeatDisabled {
		t.Errorf("Expected heartbeatDisabled to be false by default (heartbeat enabled), got true")
	}

	if broker.heartbeatCheckInterval != 10*time.Second {
		t.Errorf("Expected heartbeatCheckInterval to be 10s by default, got %v", broker.heartbeatCheckInterval)
	}
}

func TestBrokerOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        *BrokerOptions
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid default options",
			opts: &BrokerOptions{
				HeartbeatDisabled:      false,
				HeartbeatCheckInterval: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "valid options with longer interval",
			opts: &BrokerOptions{
				HeartbeatDisabled:      false,
				HeartbeatCheckInterval: 30 * time.Second,
			},
			expectError: false,
		},
		{
			name: "valid options with heartbeat disabled and short interval",
			opts: &BrokerOptions{
				HeartbeatDisabled:      true,
				HeartbeatCheckInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "invalid - heartbeat enabled with interval < 10s",
			opts: &BrokerOptions{
				HeartbeatDisabled:      false,
				HeartbeatCheckInterval: 5 * time.Second,
			},
			expectError: true,
			errorMsg:    "heartbeat_check_interval (5s) must be at least 10 seconds when heartbeat is enabled",
		},
		{
			name: "invalid - heartbeat enabled with interval = 0",
			opts: &BrokerOptions{
				HeartbeatDisabled:      false,
				HeartbeatCheckInterval: 0,
			},
			expectError: true,
			errorMsg:    "heartbeat_check_interval (0s) must be at least 10 seconds when heartbeat is enabled",
		},
		{
			name: "valid - heartbeat enabled with interval exactly 10s",
			opts: &BrokerOptions{
				HeartbeatDisabled:      false,
				HeartbeatCheckInterval: 10 * time.Second,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}
