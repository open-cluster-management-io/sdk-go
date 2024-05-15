package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	kafkaoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/kafka"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/agent/codec"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/payload"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
)

// go test -tags=kafka ./test/integration/cloudevents -ginkgo.focus "CloudeventKafkaClient" -v
var _ = ginkgo.Describe("CloudeventKafkaClient", func() {
	var ctx context.Context
	var cancel context.CancelFunc

	ginkgo.BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
	})
	ginkgo.AfterEach(func() {
		cancel()
	})

	ginkgo.It("publish event from source to agent", func() {
		ginkgo.By("Start an agent on cluster1")
		clusterName := "cluster1"
		agentCtx, agentCancel := context.WithCancel(context.Background())
		agentID := clusterName + "-" + rand.String(5)
		agentClientHolder, err := work.NewClientHolderBuilder(kafkaOptions).
			WithClientID(agentID).
			WithClusterName(clusterName).
			WithCodecs(codec.NewManifestCodec(nil)).
			NewAgentClientHolder(agentCtx)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		go agentClientHolder.ManifestWorkInformer().Informer().Run(agentCtx.Done())

		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		// agentManifestLister := agentClientHolder.ManifestWorkInformer().Lister().ManifestWorks(clusterName)
		agentManifestClient := agentClientHolder.ManifestWorks(clusterName)

		ginkgo.By("Start an source cloudevent client")
		sourceStoreLister := NewResourceLister()
		sourceCloudEventClient, err := generic.NewCloudEventSourceClient[*source.Resource](
			ctx,
			kafkaoptions.NewSourceOptions(kafkaOptions, "source1"),
			sourceStoreLister,
			source.StatusHashGetter,
			&source.ResourceCodec{},
		)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("Subscribe agent topics to update resource status")
		sourceCloudEventClient.Subscribe(ctx, func(action types.ResourceAction, resource *source.Resource) error {
			return sourceStoreLister.store.UpdateStatus(resource)
		})

		ginkgo.By("Publish manifest from source to agent")
		var manifestWork *workv1.ManifestWork
		var resourceName1 string
		gomega.Eventually(func() error {
			ginkgo.By("Create the manifest resource and publish it to agent")
			resourceName := "resource-" + rand.String(5)
			newResource := source.NewResource(clusterName, resourceName)
			err = sourceCloudEventClient.Publish(ctx, types.CloudEventsType{
				CloudEventsDataType: payload.ManifestEventDataType,
				SubResource:         types.SubResourceSpec,
				Action:              "test_create_request",
			}, newResource)
			if err != nil {
				return err
			}

			// wait until the agent receive manifestworks
			time.Sleep(2 * time.Second)

			// ensure the work can be get by work client
			workName := source.ResourceID(clusterName, resourceName)
			manifestWork, err = agentManifestClient.Get(ctx, workName, metav1.GetOptions{})
			if err != nil {
				return err
			}

			// add to the source store if the resource is synced successfully,
			sourceStoreLister.store.Add(newResource)
			resourceName1 = resourceName

			fmt.Println("agent get spec workName1", manifestWork.Name)
			return nil
		}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

		ginkgo.By("Update the resource status on the agent cluster")
		newWork := manifestWork.DeepCopy()
		newWork.Status = workv1.ManifestWorkStatus{
			Conditions: []metav1.Condition{{
				Type:   "Created",
				Status: metav1.ConditionTrue,
			}},
		}

		oldData, err := json.Marshal(manifestWork)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		newData, err := json.Marshal(newWork)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("Report(updating) the resource status from agent cluster to source cluster")
		_, err = agentManifestClient.Patch(ctx, manifestWork.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("Verify the resource status is synced to the source cluster")
		gomega.Eventually(func() error {
			storeResource, err := sourceStoreLister.store.Get(manifestWork.Name)
			if err != nil {
				return err
			}
			if !meta.IsStatusConditionTrue(storeResource.Status.Conditions, "Created") {
				return fmt.Errorf("unexpected status %v", storeResource.Status.Conditions)
			}
			return nil
		}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

		ginkgo.By("Agent resync resource from source")
		// add a new resource to the source
		resourceName2 := "resource1-" + rand.String(5)
		sourceStoreLister.store.Add(source.NewResource(clusterName, resourceName2))

		// agent resync resources from sources
		_, err = agentManifestClient.List(ctx, metav1.ListOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		workName := source.ResourceID(clusterName, resourceName2)

		gomega.Eventually(func() error {
			work, err := agentManifestClient.Get(ctx, workName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			fmt.Println("agent get resync workName2", work.Name)
			return nil
		}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

		ginkgo.By("Resync resource from source with a new agent instance")
		agentCancel()
		// wait until the consumer is closed
		time.Sleep(2 * time.Second)

		newAgentCtx, newAgentCancel := context.WithCancel(context.Background())
		defer newAgentCancel()
		agentID = clusterName + "-" + rand.String(5)
		newAgentHolder, err := work.NewClientHolderBuilder(kafkaOptions).
			WithClientID(agentID).
			WithClusterName(clusterName).
			WithCodecs(codec.NewManifestCodec(nil)).
			NewAgentClientHolder(newAgentCtx)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		// Note: Different configuration for the new agent will have different behavior
		//   Case1: new agentID(group.id) + "auto.offset.reset": latest
		//        The agent has to wait until the consumer is ready to send message, like time.Sleep(5 * time.Second)
		//   Case2: keep the same agentID for the new agent, it will ignore the "auto.offset.reset" automatically
		//        Then we don't need wait the consumer ready, cause it will consume message from last committed
		//        But the agent will wait a long time(test result is 56 seconds) to receive the message
		time.Sleep(5 * time.Second)
		go newAgentHolder.ManifestWorkInformer().Informer().Run(newAgentCtx.Done())
		newAgentManifestClient := newAgentHolder.ManifestWorks(clusterName)

		gomega.Eventually(func() error {
			workName1 := source.ResourceID(clusterName, resourceName1)
			workName2 := source.ResourceID(clusterName, resourceName2)

			work1, err := newAgentManifestClient.Get(ctx, workName1, metav1.GetOptions{})
			if err != nil {
				return err
			}
			fmt.Println("new agent get resync workName1", work1.Name)
			work2, err := newAgentManifestClient.Get(ctx, workName2, metav1.GetOptions{})
			if err != nil {
				return err
			}
			fmt.Println("new agent get resync workName2", work2.Name)
			return nil
		}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
	})
})

type resourceLister struct {
	store *source.MemoryStore
}

var _ generic.Lister[*source.Resource] = &resourceLister{}

func NewResourceLister() *resourceLister {
	return &resourceLister{
		store: source.NewMemoryStore(),
	}
}

func (resLister *resourceLister) List(listOpts types.ListOptions) ([]*source.Resource, error) {
	return resLister.store.List(listOpts.ClusterName), nil
}
