package grpc

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestLoadGRPCServerOptions(t *testing.T) {
	defaultOpts := NewGRPCServerOptions()

	testCases := []struct {
		name          string
		setup         func(t *testing.T) string
		expectedOpts  *GRPCServerOptions
		expectErr     bool
		checkDefaults bool
	}{
		{
			name: "Successful load with all options",
			setup: func(t *testing.T) string {
				content := `
tls_cert_file: /test/tls.crt
tls_key_file: /test/tls.key
client_ca_file: /test/ca.crt
server_bind_port: "9999"
max_concurrent_streams: 100
max_receive_message_size: 2048
max_send_message_size: 2048
write_buffer_size: 1024
read_buffer_size: 1024
connection_timeout: 60s
max_connection_age: 1h
client_min_ping_interval: 10s
server_ping_interval: 60s
server_ping_timeout: 20s
permit_ping_without_stream: true
cert_watch_interval: 60s
`
				tmpFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				if _, err := tmpFile.Write([]byte(content)); err != nil {
					t.Fatalf("Failed to write to temp file: %v", err)
				}
				tmpFile.Close()
				return tmpFile.Name()
			},
			expectedOpts: &GRPCServerOptions{
				TLSCertFile:             "/test/tls.crt",
				TLSKeyFile:              "/test/tls.key",
				ClientCAFile:            "/test/ca.crt",
				TLSMinVersion:           tls.VersionTLS12,
				TLSMaxVersion:           tls.VersionTLS13,
				ServerBindPort:          "9999",
				MaxConcurrentStreams:    100,
				MaxReceiveMessageSize:   2048,
				MaxSendMessageSize:      2048,
				WriteBufferSize:         1024,
				ReadBufferSize:          1024,
				ConnectionTimeout:       60 * time.Second,
				MaxConnectionAge:        1 * time.Hour,
				ClientMinPingInterval:   10 * time.Second,
				ServerPingInterval:      60 * time.Second,
				ServerPingTimeout:       20 * time.Second,
				PermitPingWithoutStream: true,
				CertWatchInterval:       60 * time.Second,
			},
			expectErr: false,
		},
		{
			name: "File not found",
			setup: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "non-existent-file.yaml")
			},
			expectedOpts: defaultOpts,
			expectErr:    false,
		},
		{
			name: "Invalid YAML content",
			setup: func(t *testing.T) string {
				content := "this: is: not: valid: yaml"
				tmpFile, err := os.CreateTemp(t.TempDir(), "invalid-*.yaml")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				if _, err := tmpFile.Write([]byte(content)); err != nil {
					t.Fatalf("Failed to write to temp file: %v", err)
				}
				tmpFile.Close()
				return tmpFile.Name()
			},
			expectedOpts: nil,
			expectErr:    true,
		},
		{
			name: "Empty config file",
			setup: func(t *testing.T) string {
				tmpFile, err := os.CreateTemp(t.TempDir(), "empty-*.yaml")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				tmpFile.Close()
				return tmpFile.Name()
			},
			expectedOpts:  defaultOpts,
			expectErr:     false,
			checkDefaults: true,
		},
		{
			name: "Partial config file",
			setup: func(t *testing.T) string {
				content := `
server_bind_port: "8888"
connection_timeout: 90s
`
				tmpFile, err := os.CreateTemp(t.TempDir(), "partial-*.yaml")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				if _, err := tmpFile.Write([]byte(content)); err != nil {
					t.Fatalf("Failed to write to temp file: %v", err)
				}
				tmpFile.Close()
				return tmpFile.Name()
			},
			expectedOpts: &GRPCServerOptions{
				ServerBindPort:          "8888",
				ClientCAFile:            "/var/run/secrets/hub/grpc/ca/ca-bundle.crt",
				TLSCertFile:             "/var/run/secrets/hub/grpc/serving-cert/tls.crt",
				TLSKeyFile:              "/var/run/secrets/hub/grpc/serving-cert/tls.key",
				TLSMinVersion:           defaultOpts.TLSMinVersion,
				TLSMaxVersion:           defaultOpts.TLSMaxVersion,
				MaxConcurrentStreams:    defaultOpts.MaxConcurrentStreams,
				MaxReceiveMessageSize:   defaultOpts.MaxReceiveMessageSize,
				MaxSendMessageSize:      defaultOpts.MaxSendMessageSize,
				WriteBufferSize:         defaultOpts.WriteBufferSize,
				ReadBufferSize:          defaultOpts.ReadBufferSize,
				ConnectionTimeout:       90 * time.Second,
				MaxConnectionAge:        defaultOpts.MaxConnectionAge,
				ClientMinPingInterval:   defaultOpts.ClientMinPingInterval,
				ServerPingInterval:      defaultOpts.ServerPingInterval,
				ServerPingTimeout:       defaultOpts.ServerPingTimeout,
				PermitPingWithoutStream: false, // a bool's zero value is false
				CertWatchInterval:       defaultOpts.CertWatchInterval,
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configPath := tc.setup(t)

			opts, err := LoadGRPCServerOptions(configPath)

			if tc.expectErr {
				if err == nil {
					t.Errorf("Expected an error, but got none")
				}
				if opts != nil {
					t.Errorf("Expected nil options on error, but got %+v", opts)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !cmp.Equal(opts, tc.expectedOpts) {
				t.Errorf("Loaded options do not match expected options.\nGot: %+v\nWant:%+v", opts, tc.expectedOpts)
			}

			if tc.checkDefaults {
				if !cmp.Equal(opts, defaultOpts) {
					t.Errorf("Expected default options, but got different values.\nGot: %+v\nWant:%+v", opts, defaultOpts)
				}
			}
		})
	}
}

func TestGRPCServerOptions_Validate_CertWatchInterval(t *testing.T) {
	tests := []struct {
		name              string
		certWatchInterval time.Duration
		expectError       bool
		errorContains     string
	}{
		{
			name:              "valid interval - 1 minute",
			certWatchInterval: 1 * time.Minute,
			expectError:       false,
		},
		{
			name:              "Interval less than 30 seconds",
			certWatchInterval: 20 * time.Second,
			expectError:       true,
		},
		{
			name:              "zero interval",
			certWatchInterval: 0,
			expectError:       true,
			errorContains:     "cert_watch_interval",
		},
		{
			name:              "negative interval",
			certWatchInterval: -1 * time.Minute,
			expectError:       true,
			errorContains:     "cert_watch_interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewGRPCServerOptions()
			opts.CertWatchInterval = tt.certWatchInterval

			err := opts.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected validation error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected validation error: %v", err)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
