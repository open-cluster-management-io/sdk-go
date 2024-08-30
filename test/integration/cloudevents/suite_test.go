package cloudevents

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"

	clienttesting "open-cluster-management.io/sdk-go/pkg/testing"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/broker"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/server"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

const (
	mqttBrokerHost    = "127.0.0.1:1883"
	mqttTLSBrokerHost = "127.0.0.1:8883"
	grpcBrokerHost    = "127.0.0.1:8882"
	grpcServerHost    = "127.0.0.1:8881"
	grpcStaticToken   = "test-static-token"
)

var (
	mqttBroker *mochimqtt.Server
	grpcBroker *broker.GRPCBroker
	grpcServer *server.GRPCServer

	serverCertPairs *util.ServerCertPairs
	certPool        *x509.CertPool
	serverCAFile    string
	tokenFile       string
)

type GetSourceOptionsFn func(context.Context, string) (*options.CloudEventsSourceOptions, string)

func init() {
	klog.InitFlags(nil)
	klog.SetOutput(ginkgo.GinkgoWriter)
}

func TestIntegration(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "CloudEvents Client Integration Suite")
}

var _ = ginkgo.BeforeSuite(func(done ginkgo.Done) {
	// crank up the connection reset speed
	generic.DelayFn = func() time.Duration { return 2 * time.Second }
	// crank up the cert check speed
	cert.CertCallbackRefreshDuration = 2 * time.Second

	ginkgo.By("bootstrapping test environment")

	// start a MQTT broker
	mqttBroker = mochimqtt.New(&mochimqtt.Options{})
	err := mqttBroker.AddHook(new(util.AllowHook), nil)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	err = mqttBroker.AddListener(listeners.NewTCP(listeners.Config{
		ID:      "mqtt-test-broker",
		Address: mqttBrokerHost,
	}))
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	serverCertPairs, err = util.NewServerCertPairs()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	certPool, err = util.AppendCAToCertPool(serverCertPairs.CA)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	err = mqttBroker.AddListener(listeners.NewTCP(
		listeners.Config{
			ID:      "mqtt-tls-test-broker",
			Address: mqttTLSBrokerHost,
			TLSConfig: &tls.Config{
				ClientCAs:    certPool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
				Certificates: []tls.Certificate{serverCertPairs.ServerTLSCert},
			},
		}))
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	go func() {
		err := mqttBroker.Serve()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	// start the grpc broker
	grpcBroker = broker.NewGRPCBroker()
	go func() {
		err := grpcBroker.Start(grpcBrokerHost)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	// start the resource grpc server
	grpcServer = server.NewGRPCServer()
	go func() {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCertPairs.ServerTLSCert},
			MinVersion:   tls.VersionTLS13,
			MaxVersion:   tls.VersionTLS13,
		}
		err := grpcServer.Start(grpcServerHost, []grpc.ServerOption{grpc.UnaryInterceptor(ensureValidTokenUnary), grpc.StreamInterceptor(ensureValidTokenStream), grpc.Creds(credentials.NewTLS(tlsConfig))})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	// write the server CA and token to tmp files
	serverCAFile, err = util.WriteCertToTempFile(serverCertPairs.CA)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	tokenFile, err = util.WriteTokenToTempFile(grpcStaticToken)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	close(done)
}, 300)

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("tearing down the test environment")

	err := mqttBroker.Close()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	// remove the temp files
	err = clienttesting.RemoveTempFile(serverCAFile)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = clienttesting.RemoveTempFile(tokenFile)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
})
