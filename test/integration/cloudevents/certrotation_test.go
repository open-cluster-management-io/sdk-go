package cloudevents

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/util/rand"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/cert"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/payload"
	"open-cluster-management.io/sdk-go/pkg/testing"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

const certDuration = 5 * time.Second

var _ = ginkgo.Describe("Auto rotating client certs", func() {
	ginkgo.Context("Auto rotating mqtt client certs", func() {
		var ctx context.Context
		var cancel context.CancelFunc

		var agentID string
		var clusterName = "cert-rotation-test"

		var clientCertFile *os.File
		var clientKeyFile *os.File

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())

			agentID = fmt.Sprintf("%s-%s", clusterName, rand.String(5))

			clientCertPairs, err := util.SignClientCert(serverCertPairs.CA, serverCertPairs.CAKey, certDuration)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			clientCertFile, err = testing.WriteToTempFile("mqtt-client-cert-*.pem", clientCertPairs.ClientCert)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			clientKeyFile, err = testing.WriteToTempFile("client-key-*.pem", clientCertPairs.ClientKey)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			cancel()

			if clientCertFile != nil {
				os.Remove(clientCertFile.Name())
			}

			if clientKeyFile != nil {
				os.Remove(clientKeyFile.Name())
			}
		})

		ginkgo.It("Should be able to send events after the client cert renewed", func() {
			ginkgo.By("Create an agent client with short time cert")
			mqttOptions := newTLSMQTTOptions(certPool, mqttTLSBrokerHost, clientCertFile.Name(), clientKeyFile.Name())
			agentOptions := mqtt.NewAgentOptions(mqttOptions, clusterName, agentID)
			agentClient, err := generic.NewCloudEventAgentClient[*store.Resource](
				ctx,
				agentOptions,
				nil,
				nil,
				&resourceCodec{},
			)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			evtType := types.CloudEventsType{
				CloudEventsDataType: payload.ManifestEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              types.EventAction("test_cert_rotation"),
			}

			ginkgo.By("Publishes an event")
			err = agentClient.Publish(ctx, evtType, &store.Resource{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			ginkgo.By("Renew the client cert")
			newClientCertPairs, err := util.SignClientCert(serverCertPairs.CA, serverCertPairs.CAKey, 60*time.Second)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			err = os.WriteFile(clientCertFile.Name(), newClientCertPairs.ClientCert, 0o644)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			err = os.WriteFile(clientKeyFile.Name(), newClientCertPairs.ClientKey, 0o644)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			ginkgo.By("Wait for the first cert to expire (10s)")
			<-time.After(certDuration * 2)

			ginkgo.By("Publishes an event again")
			err = agentClient.Publish(ctx, evtType, &store.Resource{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})
	})
})

func newTLSMQTTOptions(certPool *x509.CertPool, brokerHost, clientCertFile, clientKeyFile string) *mqtt.MQTTOptions {
	o := &mqtt.MQTTOptions{
		KeepAlive: 60,
		PubQoS:    1,
		SubQoS:    1,
		Topics: types.Topics{
			SourceEvents: "sources/certrotationtest/clusters/+/sourceevents",
			AgentEvents:  "sources/certrotationtest/clusters/+/agentevents",
		},
		Dialer: &mqtt.MQTTDialer{
			BrokerHost: brokerHost,
			Timeout:    5 * time.Second,
			TLSConfig: &tls.Config{
				RootCAs: certPool,
				GetClientCertificate: func(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
					return cert.CachingCertificateLoader(clientCertFile, clientKeyFile)()
				},
			},
		},
	}

	cert.StartClientCertRotating(o.Dialer.TLSConfig.GetClientCertificate, o.Dialer)

	return o
}

type resourceCodec struct{}

func (c *resourceCodec) EventDataType() types.CloudEventsDataType {
	return payload.ManifestEventDataType
}

func (c *resourceCodec) Encode(source string, eventType types.CloudEventsType, resource *store.Resource) (*cloudevents.Event, error) {
	evt := types.NewEventBuilder(source, eventType).NewEvent()
	if err := evt.SetData(cloudevents.ApplicationJSON, &payload.Manifest{Manifest: resource.Spec}); err != nil {
		return nil, fmt.Errorf("failed to encode manifests to cloud event: %v", err)
	}

	return &evt, nil
}

func (c *resourceCodec) Decode(evt *cloudevents.Event) (*store.Resource, error) {
	// do nothing
	return nil, nil
}
