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
	serveroptions "open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc/options"

	clienttesting "open-cluster-management.io/sdk-go/pkg/testing"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/broker"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/broker/services"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/server"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

const (
	mqttBrokerHost    = "127.0.0.1:1883"
	mqttTLSBrokerHost = "127.0.0.1:8883"
	grpcBrokerHost    = "127.0.0.1:8090"
	grpcServerHost    = "127.0.0.1:1881"
	grpcStaticToken   = "test-client"
)

var (
	mqttBroker     *mochimqtt.Server
	resourceServer *server.GRPCServer
	grpcBroker     *serveroptions.Server

	serverCertPairs *util.ServerCertPairs
	clientCertPairs *util.ClientCertPairs
	certPool        *x509.CertPool
	caFile          string
	serverCertFile  string
	serverKeyFile   string
	tokenFile       string
)

type GetSourceOptionsFn func(context.Context, string) (*options.CloudEventsSourceOptions, string)
type GetAgentOptionsFn func(context.Context, string, string, string, string) *options.CloudEventsAgentOptions

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

	clientCertPairs, err = util.SignClientCert(serverCertPairs.CA, serverCertPairs.CAKey, 10*time.Second)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	// write the server CA and token to tmp files
	caFile, err = util.WriteCertToTempFile(serverCertPairs.CA)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	serverCertFile, err = util.WriteCertToTempFile(serverCertPairs.ServerTLSCert.Leaf)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	serverKeyFile, err = util.WriteKeyToTempFile(serverCertPairs.ServerTLSCert.PrivateKey)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	tokenFile, err = util.WriteTokenToTempFile(grpcStaticToken)
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

	serverStore := store.NewMemoryStore()
	// start the resource grpc server
	resourceServer = server.NewGRPCServer(serverStore)
	go func() {
		err := resourceServer.Start(grpcServerHost, nil)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	service := services.NewResourceService(resourceServer.UpdateResourceStatus, serverStore)
	resourceServer.SetResourceService(service)

	// start the grpc broker
	opt := serveroptions.NewGRPCServerOptions()
	opt.ClientCAFile = caFile
	opt.TLSCertFile = serverCertFile
	opt.TLSKeyFile = serverKeyFile
	grpcBroker = broker.NewGRPCBrokerServer(opt, service)
	go func() {
		err := grpcBroker.Run(context.Background())
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	close(done)
}, 300)

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("tearing down the test environment")

	err := mqttBroker.Close()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	// remove the temp files
	err = clienttesting.RemoveTempFile(caFile)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = clienttesting.RemoveTempFile(tokenFile)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
})
