package grpc

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// BrokerOptions contains configuration options for the GRPCBroker.
type BrokerOptions struct {
	// HeartbeatEnabled controls whether heartbeat mechanism is enabled.
	// Default: true
	HeartbeatEnabled bool

	// HeartbeatCheckInterval is the interval for heartbeat checks.
	// Default: 10 seconds
	HeartbeatCheckInterval time.Duration
}

// NewBrokerOptions creates a new BrokerOptions with default values.
func NewBrokerOptions() *BrokerOptions {
	return &BrokerOptions{
		HeartbeatEnabled:       true,
		HeartbeatCheckInterval: 10 * time.Second,
	}
}

// AddFlags adds flags for configuring the broker options.
func (o *BrokerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.HeartbeatEnabled, "broker-heartbeat-enabled", o.HeartbeatEnabled,
		"Enable heartbeat mechanism for gRPC broker")
	fs.DurationVar(&o.HeartbeatCheckInterval, "broker-heartbeat-interval", o.HeartbeatCheckInterval,
		"Interval for heartbeat checks in gRPC broker")
}

// Validate checks the broker options for valid values.
func (o *BrokerOptions) Validate() error {
	// Validate heartbeat check interval if heartbeat is enabled
	if o.HeartbeatEnabled && o.HeartbeatCheckInterval < 10*time.Second {
		return fmt.Errorf("heartbeat_check_interval (%v) must be at least 10 seconds when heartbeat is enabled", o.HeartbeatCheckInterval)
	}
	return nil
}
