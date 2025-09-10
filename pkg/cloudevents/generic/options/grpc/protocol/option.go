package protocol

import (
	"fmt"
	"time"
)

// Option is the function signature
type Option func(*Protocol) error

// SubscribeOption
type SubscribeOption struct {
	Source      string
	ClusterName string
	DataType    string // data type for the client, eg. "io.open-cluster-management.works.v1alpha1.manifestbundles"
}

// WithSubscribeOption sets the Subscribe configuration for the client.
func WithSubscribeOption(subscribeOpt *SubscribeOption) Option {
	return func(p *Protocol) error {
		if subscribeOpt == nil {
			return fmt.Errorf("the subscribe option must not be nil")
		}
		p.subscribeOption = subscribeOpt
		return nil
	}
}

func WithReconnectErrorOption(reconnectError chan error, interval time.Duration) Option {
	return func(p *Protocol) error {
		if reconnectError == nil {
			return fmt.Errorf("the reconnect error option must not be nil")
		}
		p.reconnectErrorChan = reconnectError
		if interval <= 0 {
			p.checkInterval = 20 * time.Second
		} else {
			p.checkInterval = interval
		}
		return nil
	}
}
