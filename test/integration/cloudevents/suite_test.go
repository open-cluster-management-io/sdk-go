package cloudevents

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"testing"
	"time"

	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

const mqttBrokerHost = "127.0.0.1:1883"
const mqttTLSBrokerHost = "127.0.0.1:8883"

const grpcServerHost = "127.0.0.1:8881"

const sourceID = "integration-test"

var mqttBroker *mochimqtt.Server
var mqttOptions *mqtt.MQTTOptions
var mqttSourceCloudEventsClient generic.CloudEventsClient[*source.Resource]

var grpcServer *source.GRPCServer
var grpcOptions *grpcoptions.GRPCOptions
var grpcSourceCloudEventsClient generic.CloudEventsClient[*source.Resource]

var eventBroadcaster *source.EventBroadcaster
var store *source.MemoryStore
var consumerStore *source.MemoryStore

var serverCertPairs *util.ServerCertPairs
var certPool *x509.CertPool

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
	ctx := context.TODO()

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

	ginkgo.By("init the event hub")
	eventBroadcaster = source.NewEventBroadcaster()
	go func() {
		eventBroadcaster.Start(ctx)
	}()

	ginkgo.By("init the resource store")
	store, consumerStore = source.InitStore(eventBroadcaster)

	ginkgo.By("start the resource grpc server")
	grpcServer = source.NewGRPCServer(store, eventBroadcaster)
	go func() {
		err := grpcServer.Start(grpcServerHost)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
	}()

	ginkgo.By("start the resource grpc source client")
	grpcOptions = grpcoptions.NewGRPCOptions()
	grpcOptions.URL = grpcServerHost
	grpcSourceCloudEventsClient, err = source.StartGRPCResourceSourceClient(ctx, grpcOptions)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	ginkgo.By("start the resource mqtt source client")
	mqttOptions = newMQTTOptions(types.Topics{
		SourceEvents: fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
		AgentEvents:  fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
	})

	mqttSourceCloudEventsClient, err = source.StartMQTTResourceSourceClient(ctx, mqttOptions, sourceID, store.GetResourceSpecChan())
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	close(done)
}, 300)

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("tearing down the test environment")

	err := mqttBroker.Close()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
})

func newMQTTOptions(topics types.Topics) *mqtt.MQTTOptions {
	return &mqtt.MQTTOptions{
		KeepAlive: 60,
		PubQoS:    1,
		SubQoS:    1,
		Topics:    topics,
		Dialer: &mqtt.MQTTDialer{
			BrokerHost: mqttBrokerHost,
			Timeout:    5 * time.Second,
		},
	}
}
