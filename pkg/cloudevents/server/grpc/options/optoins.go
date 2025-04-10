package options

import (
	"github.com/spf13/pflag"
	"math"
	"time"
)

type GRPCServerOptions struct {
	TLSCertFile             string
	TLSKeyFile              string
	ClientCAFile            string
	ServerBindPort          string
	MaxConcurrentStreams    uint32
	MaxReceiveMessageSize   int
	MaxSendMessageSize      int
	ConnectionTimeout       time.Duration
	WriteBufferSize         int
	ReadBufferSize          int
	MaxConnectionAge        time.Duration
	ClientMinPingInterval   time.Duration
	ServerPingInterval      time.Duration
	ServerPingTimeout       time.Duration
	PermitPingWithoutStream bool
}

func NewGRPCServerOptions() *GRPCServerOptions {
	return &GRPCServerOptions{}
}

func (o *GRPCServerOptions) AddFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.ServerBindPort, "grpc-server-bindport", "8090", "gPRC server bind port")
	flags.Uint32Var(&o.MaxConcurrentStreams, "grpc-max-concurrent-streams", math.MaxUint32, "gPRC max concurrent streams")
	flags.IntVar(&o.MaxReceiveMessageSize, "grpc-max-receive-message-size", 1024*1024*4, "gPRC max receive message size")
	flags.IntVar(&o.MaxSendMessageSize, "grpc-max-send-message-size", math.MaxInt32, "gPRC max send message size")
	flags.DurationVar(&o.ConnectionTimeout, "grpc-connection-timeout", 120*time.Second, "gPRC connection timeout")
	flags.DurationVar(&o.MaxConnectionAge, "grpc-max-connection-age", time.Duration(math.MaxInt64), "A duration for the maximum amount of time connection may exist before closing")
	flags.DurationVar(&o.ClientMinPingInterval, "grpc-client-min-ping-interval", 5*time.Second, "Server will terminate the connection if the client pings more than once within this duration")
	flags.DurationVar(&o.ServerPingInterval, "grpc-server-ping-interval", 30*time.Second, "Duration after which the server pings the client if no activity is detected")
	flags.DurationVar(&o.ServerPingTimeout, "grpc-server-ping-timeout", 10*time.Second, "Duration the client waits for a response after sending a keepalive ping")
	flags.BoolVar(&o.PermitPingWithoutStream, "permit-ping-without-stream", false, "Allow keepalive pings even when there are no active streams")
	flags.IntVar(&o.WriteBufferSize, "grpc-write-buffer-size", 32*1024, "gPRC write buffer size")
	flags.IntVar(&o.ReadBufferSize, "grpc-read-buffer-size", 32*1024, "gPRC read buffer size")
	flags.StringVar(&o.TLSCertFile, "grpc-tls-cert-file", "", "The path to the tls.crt file")
	flags.StringVar(&o.TLSKeyFile, "grpc-tls-key-file", "", "The path to the tls.key file")
	flags.StringVar(&o.ClientCAFile, "grpc-client-ca-file", "", "The path to the client ca file, must specify if using mtls authentication type")
}
