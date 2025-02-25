package util

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
)

func NewGRPCAgentOptions(brokerURL string) *grpc.GRPCOptions {
	return newGRPCOptions(nil, brokerURL, "")
}

func NewGRPCSourceOptions(certPool *x509.CertPool, brokerURL, tokenFile string) *grpc.GRPCOptions {
	return newGRPCOptions(certPool, brokerURL, tokenFile)
}

func newGRPCOptions(certPool *x509.CertPool, brokerURL, tokenFile string) *grpc.GRPCOptions {
	grpcOptions := &grpc.GRPCOptions{
		Dialer: &grpc.GRPCDialer{
			URL: brokerURL,
			KeepAliveOptions: grpc.KeepAliveOptions{
				Enable:              true,
				Time:                10 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		},
	}

	if certPool != nil {
		grpcOptions.Dialer.TLSConfig = &tls.Config{
			RootCAs: certPool,
		}
	}

	if tokenFile != "" {
		grpcOptions.Dialer.TokenFile = tokenFile
	}

	return grpcOptions
}
