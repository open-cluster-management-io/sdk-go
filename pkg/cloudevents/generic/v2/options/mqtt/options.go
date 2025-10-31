package mqtt

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"gopkg.in/yaml.v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type MQTTDialer struct {
	TLSConfig  *tls.Config
	BrokerHost string
	Timeout    time.Duration

	conn net.Conn
}

func (d *MQTTDialer) Dial() (net.Conn, error) {
	if d.TLSConfig != nil {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: d.Timeout}, "tcp", d.BrokerHost, d.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to MQTT broker %s, %v", d.BrokerHost, err)
		}

		// ensure parallel writes are thread-Safe
		d.conn = packets.NewThreadSafeConn(conn)
		return d.conn, nil
	}

	conn, err := net.DialTimeout("tcp", d.BrokerHost, d.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker %s, %v", d.BrokerHost, err)
	}

	// ensure parallel writes are thread-Safe
	d.conn = packets.NewThreadSafeConn(conn)
	return d.conn, nil
}

func (d *MQTTDialer) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// MQTTOptions holds the options that are used to build MQTT client.
type MQTTOptions struct {
	Topics    types.Topics
	Username  string
	Password  string
	KeepAlive uint16
	PubQoS    int
	SubQoS    int

	Dialer *MQTTDialer
}

// MQTTConfig holds the information needed to build connect to MQTT broker as a given user.
type MQTTConfig struct {
	cert.CertConfig `json:",inline" yaml:",inline"`

	// BrokerHost is the host of the MQTT broker (hostname:port).
	BrokerHost string `json:"brokerHost" yaml:"brokerHost"`

	// Username is the username for basic authentication to connect the MQTT broker.
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	// Password is the password for basic authentication to connect the MQTT broker.
	Password string `json:"password,omitempty" yaml:"password,omitempty"`

	// KeepAlive is the keep alive time in seconds for MQTT clients, by default is 60s
	KeepAlive *uint16 `json:"keepAlive,omitempty" yaml:"keepAlive,omitempty"`

	// DialTimeout is the timeout when establishing a MQTT TCP connection, by default is 60s
	DialTimeout *time.Duration `json:"dialTimeout,omitempty" yaml:"dialTimeout,omitempty"`

	// PubQoS is the QoS for publish, by default is 1
	PubQoS *int `json:"pubQoS,omitempty" yaml:"pubQoS,omitempty"`
	// SubQoS is the Qos for subscribe, by default is 1
	SubQoS *int `json:"subQoS,omitempty" yaml:"subQoS,omitempty"`

	// Topics are MQTT topics for resource spec, status and resync.
	Topics *types.Topics `json:"topics,omitempty" yaml:"topics,omitempty"`
}

func LoadConfig(configPath string) (*MQTTConfig, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &MQTTConfig{}
	if err := yaml.Unmarshal(configData, config); err != nil {
		return nil, err
	}

	if err := config.CertConfig.EmbedCerts(); err != nil {
		return nil, err
	}

	return config, nil
}

// BuildMQTTOptionsFromFlags builds configs from a config filepath.
func BuildMQTTOptionsFromFlags(configPath string) (*MQTTOptions, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	if config.BrokerHost == "" {
		return nil, fmt.Errorf("brokerHost is required")
	}

	if err := config.CertConfig.Validate(); err != nil {
		return nil, err
	}

	if err := validateTopics(config.Topics); err != nil {
		return nil, err
	}

	options := &MQTTOptions{
		Username:  config.Username,
		Password:  config.Password,
		KeepAlive: 60,
		PubQoS:    1,
		SubQoS:    1,
		Topics:    *config.Topics,
	}

	if config.KeepAlive != nil {
		options.KeepAlive = *config.KeepAlive
	}

	if config.PubQoS != nil {
		options.PubQoS = *config.PubQoS
	}

	if config.SubQoS != nil {
		options.SubQoS = *config.SubQoS
	}

	dialTimeout := 60 * time.Second
	if config.DialTimeout != nil {
		dialTimeout = *config.DialTimeout
	}

	options.Dialer = &MQTTDialer{
		BrokerHost: config.BrokerHost,
		Timeout:    dialTimeout,
	}

	if config.CertConfig.HasCerts() {
		// Set up TLS configuration for the MQTT connection if the client certificate and key are provided.
		// the certificates will be reloaded periodically.
		options.Dialer.TLSConfig, err = cert.AutoLoadTLSConfig(
			config.CertConfig,
			func() (*cert.CertConfig, error) {
				config, err := LoadConfig(configPath)
				if err != nil {
					return nil, err
				}
				return &config.CertConfig, nil
			},
			options.Dialer,
		)
		if err != nil {
			return nil, err
		}
	}

	return options, nil
}
