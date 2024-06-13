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
	"k8s.io/klog/v2"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"

	"open-cluster-management.io/sdk-go/test/integration/cloudevents/server"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

const (
	mqttBrokerHost    = "127.0.0.1:1883"
	mqttTLSBrokerHost = "127.0.0.1:8883"
	grpcServerHost    = "127.0.0.1:8881"
)

var (
	mqttBroker *mochimqtt.Server

	grpcServer *server.GRPCServer

	serverCertPairs *util.ServerCertPairs
	certPool        *x509.CertPool
)

type GetSourceOptionsFn func(context.Context, string) *options.CloudEventsSourceOptions

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

	err = mqttBroker.AddListener(listeners.NewTCP("mqtt-test-broker", mqttBrokerHost, nil))
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	serverCertPairs, err = util.NewServerCertPairs()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	certPool, err = util.AppendCAToCertPool(serverCertPairs.CA)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	err = mqttBroker.AddListener(listeners.NewTCP("mqtt-tls-test-broker", mqttTLSBrokerHost, &listeners.Config{
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

	// start the resource grpc server
	grpcServer = server.NewGRPCServer()
	go func() {
		err := grpcServer.Start(grpcServerHost)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	close(done)
}, 300)

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("tearing down the test environment")

	err := mqttBroker.Close()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
})
