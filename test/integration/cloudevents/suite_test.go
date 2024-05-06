package cloudevents

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	kafkav2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

const (
	mqttBrokerHost    = "127.0.0.1:1883"
	mqttTLSBrokerHost = "127.0.0.1:8883"
	grpcServerHost    = "127.0.0.1:8881"
	sourceID          = "integration-test"
)

var (
	// TODO: need a brokerInterface to consolidate the transport configurations
	testEnv                     *envtest.Environment
	testEnvConfig               *rest.Config
	mqttBroker                  *mochimqtt.Server
	mqttOptions                 *mqtt.MQTTOptions
	mqttSourceCloudEventsClient generic.CloudEventsClient[*source.Resource]
	grpcServer                  *source.GRPCServer
	grpcOptions                 *grpcoptions.GRPCOptions
	grpcSourceCloudEventsClient generic.CloudEventsClient[*source.Resource]
	eventBroadcaster            *source.EventBroadcaster
	store                       *source.MemoryStore
	consumerStore               *source.MemoryStore
	serverCertPairs             *util.ServerCertPairs
	certPool                    *x509.CertPool
	kafkaCluster                *kafkav2.MockCluster
	kafkaOptions                *kafka.KafkaOptions
)

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
	var err error

	// start a kube-apiserver
	testEnv = &envtest.Environment{
		ErrorIfCRDPathMissing: true,
		CRDDirectoryPaths: []string{
			filepath.Join(".", "vendor", "open-cluster-management.io", "api", "addon", "v1alpha1"),
		},
	}
	testEnvConfig, err = testEnv.Start()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	gomega.Expect(testEnvConfig).ToNot(gomega.BeNil())

	// start a MQTT broker
	mqttBroker = mochimqtt.New(&mochimqtt.Options{})
	err = mqttBroker.AddHook(new(util.AllowHook), nil)
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

	ginkgo.By("init the kafka broker and topics")
	kafkaCluster, err = kafkav2.NewMockCluster(1)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	kafkaOptions = &kafka.KafkaOptions{
		ConfigMap: kafkav2.ConfigMap{
			"bootstrap.servers": kafkaCluster.BootstrapServers(),
		},
	}
	err = kafkaCluster.CreateTopic("sourcebroadcast.source1", 1, 1)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = kafkaCluster.CreateTopic("sourceevents.source1.cluster1", 1, 1)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = kafkaCluster.CreateTopic("agentevents.source1.cluster1", 1, 1)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = kafkaCluster.CreateTopic("agentbroadcast.cluster1", 1, 1)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	close(done)
}, 300)

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("tearing down the test environment")

	err := mqttBroker.Close()
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	err = testEnv.Stop()
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
