package pubsub

import (
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/grpc/keepalive"
)

func TestToGRPCKeepaliveParameter(t *testing.T) {
	cases := []struct {
		name     string
		settings *KeepaliveSettings
		expected keepalive.ClientParameters
	}{
		{
			name: "all settings provided",
			settings: &KeepaliveSettings{
				Time:                10 * time.Minute,
				Timeout:             30 * time.Second,
				PermitWithoutStream: true,
			},
			expected: keepalive.ClientParameters{
				Time:                10 * time.Minute,
				Timeout:             30 * time.Second,
				PermitWithoutStream: true,
			},
		},
		{
			name: "only time provided",
			settings: &KeepaliveSettings{
				Time: 5 * time.Minute,
			},
			expected: keepalive.ClientParameters{
				Time:                5 * time.Minute,
				Timeout:             0,
				PermitWithoutStream: false,
			},
		},
		{
			name: "only timeout provided",
			settings: &KeepaliveSettings{
				Timeout: 20 * time.Second,
			},
			expected: keepalive.ClientParameters{
				Time:                0,
				Timeout:             20 * time.Second,
				PermitWithoutStream: false,
			},
		},
		{
			name: "zero values",
			settings: &KeepaliveSettings{
				Time:                0,
				Timeout:             0,
				PermitWithoutStream: false,
			},
			expected: keepalive.ClientParameters{
				Time:                0,
				Timeout:             0,
				PermitWithoutStream: false,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := toGRPCKeepaliveParameter(c.settings)

			if result.Time != c.expected.Time {
				t.Errorf("expected Time to be %v, got %v", c.expected.Time, result.Time)
			}

			if result.Timeout != c.expected.Timeout {
				t.Errorf("expected Timeout to be %v, got %v", c.expected.Timeout, result.Timeout)
			}

			if result.PermitWithoutStream != c.expected.PermitWithoutStream {
				t.Errorf("expected PermitWithoutStream to be %v, got %v", c.expected.PermitWithoutStream, result.PermitWithoutStream)
			}
		})
	}
}

func TestToPubSubReceiveSettings(t *testing.T) {
	cases := []struct {
		name     string
		settings *ReceiveSettings
		expected pubsub.ReceiveSettings
	}{
		{
			name: "all settings provided",
			settings: &ReceiveSettings{
				MaxExtension:               600 * time.Second,
				MaxDurationPerAckExtension: 10 * time.Second,
				MinDurationPerAckExtension: 1 * time.Second,
				MaxOutstandingMessages:     1000,
				MaxOutstandingBytes:        1000000000,
				NumGoroutines:              10,
			},
			expected: pubsub.ReceiveSettings{
				MaxExtension:               600 * time.Second,
				MaxDurationPerAckExtension: 10 * time.Second,
				MinDurationPerAckExtension: 1 * time.Second,
				MaxOutstandingMessages:     1000,
				MaxOutstandingBytes:        1000000000,
				NumGoroutines:              10,
			},
		},
		{
			name: "partial settings",
			settings: &ReceiveSettings{
				MaxExtension:           300 * time.Second,
				MaxOutstandingMessages: 500,
			},
			expected: pubsub.ReceiveSettings{
				MaxExtension:               300 * time.Second,
				MaxDurationPerAckExtension: 0,
				MinDurationPerAckExtension: 0,
				MaxOutstandingMessages:     500,
				MaxOutstandingBytes:        0,
				NumGoroutines:              0,
			},
		},
		{
			name: "zero values",
			settings: &ReceiveSettings{
				MaxExtension:               0,
				MaxDurationPerAckExtension: 0,
				MinDurationPerAckExtension: 0,
				MaxOutstandingMessages:     0,
				MaxOutstandingBytes:        0,
				NumGoroutines:              0,
			},
			expected: pubsub.ReceiveSettings{
				MaxExtension:               0,
				MaxDurationPerAckExtension: 0,
				MinDurationPerAckExtension: 0,
				MaxOutstandingMessages:     0,
				MaxOutstandingBytes:        0,
				NumGoroutines:              0,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := toPubSubReceiveSettings(c.settings)

			if result.MaxExtension != c.expected.MaxExtension {
				t.Errorf("expected MaxExtension to be %v, got %v", c.expected.MaxExtension, result.MaxExtension)
			}

			if result.MaxDurationPerAckExtension != c.expected.MaxDurationPerAckExtension {
				t.Errorf("expected MaxDurationPerAckExtension to be %v, got %v", c.expected.MaxDurationPerAckExtension, result.MaxDurationPerAckExtension)
			}

			if result.MinDurationPerAckExtension != c.expected.MinDurationPerAckExtension {
				t.Errorf("expected MinDurationPerAckExtension to be %v, got %v", c.expected.MinDurationPerAckExtension, result.MinDurationPerAckExtension)
			}

			if result.MaxOutstandingMessages != c.expected.MaxOutstandingMessages {
				t.Errorf("expected MaxOutstandingMessages to be %v, got %v", c.expected.MaxOutstandingMessages, result.MaxOutstandingMessages)
			}

			if result.MaxOutstandingBytes != c.expected.MaxOutstandingBytes {
				t.Errorf("expected MaxOutstandingBytes to be %v, got %v", c.expected.MaxOutstandingBytes, result.MaxOutstandingBytes)
			}

			if result.NumGoroutines != c.expected.NumGoroutines {
				t.Errorf("expected NumGoroutines to be %v, got %v", c.expected.NumGoroutines, result.NumGoroutines)
			}
		})
	}
}
