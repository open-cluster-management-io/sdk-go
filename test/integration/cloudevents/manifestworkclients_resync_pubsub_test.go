package cloudevents

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"

	workv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/common"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work"
	agentcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/agent/codec"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	sourcecodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/source/codec"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/clients"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/v2/pubsub"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/agent"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

// emptyManifestWorkLister is a simple lister that returns empty list
// Used for testing where we only need to publish events, not list resources
type emptyManifestWorkLister struct{}

func (e *emptyManifestWorkLister) List(ctx context.Context, options types.ListOptions) ([]*workv1.ManifestWork, error) {
	return []*workv1.ManifestWork{}, nil
}

// This test simulates the PubSub race condition by sending create_request and resync_response events concurrently
// to a running work agent, to ensure pubsub transport can handle messages arriving on the different subscriptions simultaneously.
var _ = ginkgo.Describe("ManifestWork Clients Test - Resync PubSub", func() {
	ginkgo.Context("Concurrent message delivery on pubsub", func() {
		var ctx context.Context
		var cancel context.CancelFunc

		var sourceID string
		var clusterName string
		var workName string

		var agentClientHolder *work.ClientHolder
		var sourceCloudEventsClient generic.CloudEventsClient[*workv1.ManifestWork]

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			sourceID = fmt.Sprintf("mw-pubsub-race-%s", rand.String(5))
			clusterName = fmt.Sprintf("cluster-race-%s", rand.String(5))
			workName = "race-test-work"

			// Setup PubSub topics and subscriptions
			gomega.Expect(setupTopicsAndSubscriptions(ctx, clusterName, sourceID)).ToNot(gomega.HaveOccurred())

			// Start the agent and keep it running
			ginkgo.By("starting the agent")
			pubsubAgentOptions := util.NewPubSubAgentOptions(pubsubServer.Addr, pubsubProjectID, clusterName, true)
			var err error
			agentClientHolder, _, err = agent.StartWorkAgent(ctx, clusterName, pubsubAgentOptions, agentcodec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// wait for agent ready
			<-time.After(time.Second)

			// Create a pure CloudEvents client (source) to send events directly
			ginkgo.By("creating pure cloudevents client to send events")
			pubsubSourceOptions := util.NewPubSubSourceOptions(pubsubServer.Addr, pubsubProjectID, sourceID, true)
			sourceOptions := pubsub.NewSourceOptions(pubsubSourceOptions, sourceID)

			// Use a simple lister that returns empty list (we're not listing, just publishing)
			lister := &emptyManifestWorkLister{}
			hashGetter := func(obj *workv1.ManifestWork) (string, error) {
				return "", nil // not used for this test
			}

			sourceCloudEventsClient, err = clients.NewCloudEventSourceClient(
				ctx,
				sourceOptions,
				lister,
				hashGetter,
				sourcecodec.NewManifestBundleCodec(),
			)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// wait for source client ready
			<-time.After(time.Second)
		})

		ginkgo.AfterEach(func() {
			cancel()
		})

		ginkgo.It("should handle concurrent create_request and resync_response without race", func() {
			// Create the manifestwork that we'll send events for
			work := util.NewManifestWork(clusterName, workName, true)
			work.UID = "test-uid-123"
			work.Labels = map[string]string{
				common.CloudEventsOriginalSourceLabelKey: sourceID,
			}

			// Define the event types we'll send concurrently
			createRequest := types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.CreateRequestAction,
			}

			resyncResponse := types.CloudEventsType{
				CloudEventsDataType: payload.ManifestBundleEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              types.ResyncResponseAction,
			}

			// THE RACE CONDITION TEST:
			// Send both create_request and resync_response events concurrently.
			// Both arrive on the agent's sourceevents and sourcebroadcast subscriptions from the source.
			// Without the sequential channel fix: two goroutines in receiveFromSubscriber
			// could process these concurrently, causing race conditions in the agent store
			// and potential resource version conflicts when controllers patch the work.
			// With the fix: events are funneled through a single channel for sequential processing.
			ginkgo.By("sending create_request and resync_response concurrently")

			start := make(chan struct{})
			errChan := make(chan error, 2)

			// Send create_request
			go func() {
				<-start
				err := sourceCloudEventsClient.Publish(ctx, createRequest, work)
				errChan <- err
			}()

			// Send resync_response
			go func() {
				<-start
				err := sourceCloudEventsClient.Publish(ctx, resyncResponse, work.DeepCopy())
				errChan <- err
			}()

			close(start)

			// Wait for both publishes to complete
			for range 2 {
				err := <-errChan
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			}

			// Verify the work is correctly applied in the agent despite concurrent message delivery
			ginkgo.By("verifying manifestwork is applied correctly")
			gomega.Eventually(func() error {
				retrievedWork, err := agentClientHolder.ManifestWorks(clusterName).Get(
					ctx, workName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if len(retrievedWork.Spec.Workload.Manifests) == 0 {
					return fmt.Errorf("work has no manifests")
				}
				return nil
			}, 10*time.Second, 500*time.Millisecond).Should(gomega.Succeed())
		})
	})
})
