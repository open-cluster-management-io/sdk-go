package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work"
	agentcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/agent/codec"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/agent"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var _ = ginkgo.Describe("Client Patch Race Test", func() {
	var ctx context.Context
	var cancel context.CancelFunc

	var sourceClient *work.ClientHolder
	var agentClient *work.ClientHolder

	var sourceID string
	var clusterName string
	var workName string

	ginkgo.BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())

		sourceID = fmt.Sprintf("patch-race-test-%s", rand.String(5))
		clusterName = fmt.Sprintf("patch-race-cluster-%s", rand.String(5))
		workName = fmt.Sprintf("patch-race-work-%s", rand.String(5))
	})

	ginkgo.AfterEach(func() {
		// cancel the context to stop clients gracefully
		cancel()
	})

	ginkgo.Context("Testing patch race conditions on agent client", func() {
		ginkgo.JustBeforeEach(func() {
			// Start source client
			sourceOptions := util.NewMQTTSourceOptionsWithSourceBroadcast(mqttBrokerHost, sourceID)
			var err error
			sourceClient, _, err = source.StartManifestWorkSourceClient(ctx, sourceID, sourceOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Start agent client
			agentOptions := util.NewMQTTAgentOptionsWithSourceBroadcast(mqttBrokerHost, sourceID, clusterName)
			agentClient, _, err = agent.StartWorkAgent(ctx, clusterName, agentOptions, agentcodec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Wait for clients to sync
			time.Sleep(3 * time.Second)
		})

		ginkgo.It("should detect conflict when patching with stale resourceVersion", func() {
			work := util.NewManifestWork(clusterName, workName, true)

			ginkgo.By("create a work with source client", func() {
				_, err := sourceClient.ManifestWorks(clusterName).Create(ctx, work, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			// Wait for work to be synced to agent
			ginkgo.By("wait for work to be synced to agent", func() {
				gomega.Eventually(func() error {
					_, err := agentClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
					return err
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			// Get the resource twice to simulate race condition
			var firstGet, secondGet *workv1.ManifestWork
			var firstResourceVersion, secondResourceVersion string

			ginkgo.By("get the resource twice with the same resourceVersion", func() {
				var err error
				firstGet, err = agentClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				firstResourceVersion = firstGet.GetResourceVersion()

				secondGet, err = agentClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				secondResourceVersion = secondGet.GetResourceVersion()

				// Verify both Gets have the same resource version
				gomega.Expect(firstResourceVersion).To(gomega.Equal(secondResourceVersion))
				gomega.Expect(firstResourceVersion).ToNot(gomega.BeEmpty())
			})

			// Patch the first copy successfully
			ginkgo.By("patch using the first copy - should succeed", func() {
				patchBytes, err := json.Marshal(map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid":             firstGet.GetUID(),
						"resourceVersion": firstResourceVersion,
						"finalizers":      []string{"test-finalizer-1"},
					},
				})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				_, err = agentClient.ManifestWorks(clusterName).Patch(
					ctx,
					workName,
					types.MergePatchType,
					patchBytes,
					metav1.PatchOptions{},
				)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			// Verify the resource version has changed
			ginkgo.By("verify resource version has changed after first patch", func() {
				gomega.Eventually(func() error {
					updatedWork, err := agentClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
					if err != nil {
						return err
					}
					if updatedWork.GetResourceVersion() == firstResourceVersion {
						return fmt.Errorf("resource version has not changed yet")
					}
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			// Patch using the second copy (with stale resource version) - should fail
			ginkgo.By("patch using the second copy with stale resourceVersion - should get conflict", func() {
				patchBytes, err := json.Marshal(map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid":             secondGet.GetUID(),
						"resourceVersion": secondResourceVersion,
						"finalizers":      []string{"test-finalizer-2"},
					},
				})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				_, err = agentClient.ManifestWorks(clusterName).Patch(
					ctx,
					workName,
					types.MergePatchType,
					patchBytes,
					metav1.PatchOptions{},
				)

				// Should get a conflict error
				gomega.Expect(err).To(gomega.HaveOccurred())
				gomega.Expect(errors.IsConflict(err)).To(gomega.BeTrue())
			})

			ginkgo.By("verify the work has the first patch finalizer, not the second", func() {
				finalWork, err := agentClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				gomega.Expect(finalWork.Finalizers).To(gomega.Equal([]string{"test-finalizer-1"}))
			})
		})
	})
})
