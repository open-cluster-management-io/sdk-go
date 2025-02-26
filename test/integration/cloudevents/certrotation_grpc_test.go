package cloudevents

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"time"

	"github.com/onsi/ginkgo"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
)

var _ = ginkgo.Describe("CloudEvents Certificate Rotation Test - GRPC", runCloudeventsCertRotationTest(GetGRPCAgentOptions))

func GetGRPCAgentOptions(ctx context.Context, agentID, clusterName, clientCertFile, clientKeyFile string) *options.CloudEventsAgentOptions {
	grpcOptions := newTLSGRPCOptions(ctx, certPool, grpcTLSBrokerHost, clientCertFile, clientKeyFile)
	return grpc.NewAgentOptions(grpcOptions, clusterName, agentID)
}

func newTLSGRPCOptions(ctx context.Context, certPool *x509.CertPool, brokerHost, clientCertFile, clientKeyFile string) *grpc.GRPCOptions {
	o := &grpc.GRPCOptions{
		Dialer: &grpc.GRPCDialer{
			URL: brokerHost,
			TLSConfig: &tls.Config{
				RootCAs: certPool,
				GetClientCertificate: func(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
					return cert.CachingCertificateLoader(clientCertFile, clientKeyFile)()
				},
			},
			KeepAliveOptions: grpc.KeepAliveOptions{
				Enable:              true,
				Time:                10 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		},
	}

	cert.StartClientCertRotating(o.Dialer.TLSConfig.GetClientCertificate, o.Dialer)

	// start a goroutine to receive resource status
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-grpcTLSBroker.ResourceStatusChan():
			}
		}
	}()

	return o
}
