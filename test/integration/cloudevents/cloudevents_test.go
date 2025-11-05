package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"

	pubsubv2 "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/agent/codec"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/agent"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var (
	createRequest = types.CloudEventsType{
		CloudEventsDataType: payload.ManifestBundleEventDataType,
		SubResource:         types.SubResourceSpec,
		Action:              types.CreateRequestAction,
	}
	updateRequest = types.CloudEventsType{
		CloudEventsDataType: payload.ManifestBundleEventDataType,
		SubResource:         types.SubResourceSpec,
		Action:              types.UpdateRequestAction,
	}
	deleteRequest = types.CloudEventsType{
		CloudEventsDataType: payload.ManifestBundleEventDataType,
		SubResource:         types.SubResourceSpec,
		Action:              types.DeleteRequestAction,
	}
)

func runCloudeventsClientPubSubTest(getSourceOptionsFn GetSourceOptionsFn) func() {
	return func() {
		var err error

		var ctx context.Context
		var cancel context.CancelFunc

		var sourceStore *store.MemoryStore

		var sourceID string
		var clusterName string
		var resourceName string

		var sourceOptions *options.CloudEventsSourceOptions
		var driver string
		var agentOptions any

		var sourceCloudEventsClient generic.CloudEventsClient[*store.Resource]

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())

			sourceID = fmt.Sprintf("cloudevents-test-%s", rand.String(5))
			clusterName = fmt.Sprintf("cluster-%s", rand.String(5))
			resourceName = fmt.Sprintf("resource-%s", rand.String(5))

			sourceStore = store.NewMemoryStore()

			sourceOptions, driver = getSourceOptionsFn(ctx, sourceID)
			switch driver {
			case constants.ConfigTypeMQTT:
				agentOptions = util.NewMQTTAgentOptions(mqttBrokerHost, sourceID, clusterName)
			case constants.ConfigTypeGRPC:
				agentOptions = util.NewGRPCAgentOptions(certPool, grpcBrokerHost, tokenFile)
			case constants.ConfigTypePubSub:
				agentOptions = util.NewPubSubAgentOptions(pubsubServer.Addr, pubsubProjectID, clusterName)
				// prepare topics and subscriptions
				pubsubConn, err := grpc.NewClient(pubsubServer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				defer pubsubConn.Close()

				pubsubClient, err := pubsubv2.NewClient(ctx, pubsubProjectID, option.WithGRPCConn(pubsubConn))
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				defer pubsubClient.Close()

				sourceEventsTopic := fmt.Sprintf("projects/%s/topics/sourceevents", pubsubProjectID)
				if _, err = pubsubClient.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: sourceEventsTopic}); err != nil {
					_, err = pubsubClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: sourceEventsTopic})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				sourceEventsSubscription := fmt.Sprintf("projects/%s/subscriptions/sourceevents-%s", pubsubProjectID, clusterName)
				if _, err = pubsubClient.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sourceEventsSubscription}); err != nil {
					_, err = pubsubClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
						Name:   sourceEventsSubscription,
						Topic:  sourceEventsTopic,
						Filter: fmt.Sprintf("attributes.\"ce-clustername\"=\"%s\"", clusterName),
					})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				sourceBroadcastTopic := fmt.Sprintf("projects/%s/topics/sourcebroadcast", pubsubProjectID)
				if _, err = pubsubClient.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: sourceBroadcastTopic}); err != nil {
					_, err = pubsubClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: sourceBroadcastTopic})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				sourceBroadcastSubscription := fmt.Sprintf("projects/%s/subscriptions/sourcebroadcast-%s", pubsubProjectID, clusterName)
				if _, err = pubsubClient.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sourceBroadcastSubscription}); err != nil {
					_, err = pubsubClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
						Name:  sourceBroadcastSubscription,
						Topic: sourceBroadcastTopic,
					})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				agentEventsTopic := fmt.Sprintf("projects/%s/topics/agentevents", pubsubProjectID)
				if _, err = pubsubClient.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: agentEventsTopic}); err != nil {
					_, err = pubsubClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: agentEventsTopic})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				agentEventsSubscription := fmt.Sprintf("projects/%s/subscriptions/agentevents-%s", pubsubProjectID, sourceID)
				if _, err = pubsubClient.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: agentEventsSubscription}); err != nil {
					_, err = pubsubClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
						Name:   agentEventsSubscription,
						Topic:  agentEventsTopic,
						Filter: fmt.Sprintf("attributes.\"ce-originalsource\"=\"%s\"", sourceID),
					})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				agentBroadcastTopic := fmt.Sprintf("projects/%s/topics/agentbroadcast", pubsubProjectID)
				if _, err = pubsubClient.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: agentBroadcastTopic}); err != nil {
					_, err = pubsubClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: agentBroadcastTopic})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
				agentBroadcastSubscription := fmt.Sprintf("projects/%s/subscriptions/agentbroadcast-%s", pubsubProjectID, sourceID)
				if _, err = pubsubClient.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: agentBroadcastSubscription}); err != nil {
					_, err = pubsubClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
						Name:  agentBroadcastSubscription,
						Topic: agentBroadcastTopic,
					})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}
			}

			sourceCloudEventsClient, err = source.StartResourceSourceClient(
				ctx,
				sourceOptions,
				sourceID,
				source.NewResourceLister(sourceStore),
			)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			// cancel the context to gracefully shutdown the agent
			cancel()
		})

		ginkgo.Context("PubSub a resource", func() {
			ginkgo.It("CRUD a resource by cloudevents clients", func() {
				crudResource(
					ctx,
					agentOptions,
					sourceStore,
					sourceCloudEventsClient,
					clusterName,
					resourceName,
					true,
				)
			})
		})

		ginkgo.Context("PubSub a resource without version", func() {
			ginkgo.It("CRUD a resource without version by cloudevents clients", func() {
				crudResource(
					ctx,
					agentOptions,
					sourceStore,
					sourceCloudEventsClient,
					clusterName,
					resourceName,
					false,
				)
			})
		})
	}
}

func crudResource(
	ctx context.Context,
	config any,
	sourceStore *store.MemoryStore,
	sourceCloudEventsClient generic.CloudEventsClient[*store.Resource],
	clusterName, resourceName string,
	withVersion bool,
) {
	ginkgo.By("start a work agent")
	clientHolder, _, err := agent.StartWorkAgent(ctx, clusterName, config, codec.NewManifestBundleCodec())
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	agentWorkClient := clientHolder.ManifestWorks(clusterName)
	time.Sleep(3 * time.Second) // sleep for the agent is subscribed to the broker

	ginkgo.By("create a resource by source")
	resourceVersion := 0
	if withVersion {
		resourceVersion = 1
	}
	resource := store.NewResource(clusterName, resourceName, int64(resourceVersion))
	sourceStore.Add(resource)
	err = sourceCloudEventsClient.Publish(ctx, createRequest, resource)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	ginkgo.By("update the resource status by work agent")
	gomega.Eventually(func() error {
		workName := store.ResourceID(clusterName, resourceName)
		if err := util.AddWorkFinalizer(ctx, agentWorkClient, workName); err != nil {
			return err
		}

		if err := util.AssertWorkFinalizers(ctx, agentWorkClient, workName); err != nil {
			return err
		}

		if err := util.UpdateWorkStatus(ctx, agentWorkClient, workName, util.WorkCreatedCondition); err != nil {
			return err
		}

		return nil
	}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("ensure the work status subscribed by source")
	gomega.Eventually(func() error {
		resource, err = sourceStore.Get(store.ResourceID(clusterName, resourceName))
		if err != nil {
			return err
		}

		if !meta.IsStatusConditionTrue(resource.Status.Conditions, "Created") {
			return fmt.Errorf("unexpected status %v", resource.Status.Conditions)
		}

		return nil
	}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("update the resource by source")
	resource, err = sourceStore.Get(store.ResourceID(clusterName, resourceName))
	if withVersion {
		resource.ResourceVersion = resource.ResourceVersion + 1
	}
	resource.Spec.Object["data"] = "test"
	err = sourceStore.Update(resource)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = sourceCloudEventsClient.Publish(ctx, updateRequest, resource)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	ginkgo.By("received resource updated by agent")
	gomega.Eventually(func() error {
		workName := store.ResourceID(clusterName, resourceName)
		work, err := agentWorkClient.Get(ctx, workName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if len(work.Spec.Workload.Manifests) != 1 {
			return fmt.Errorf("expected manifests in the work, but got %v", work)
		}

		workload := map[string]any{}
		if err := json.Unmarshal(work.Spec.Workload.Manifests[0].Raw, &workload); err != nil {
			return err
		}

		if workload["data"] != "test" {
			return fmt.Errorf("unexpected workload %v", workload)
		}

		return nil
	}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("mark the resource deleting by source")
	resource, err = sourceStore.Get(store.ResourceID(clusterName, resourceName))
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	resource.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	err = sourceStore.Update(resource)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	err = sourceCloudEventsClient.Publish(ctx, deleteRequest, resource)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	ginkgo.By("delete the resource from agent")
	gomega.Eventually(func() error {
		workName := store.ResourceID(clusterName, resourceName)
		return util.RemoveWorkFinalizer(ctx, agentWorkClient, workName)
	}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

	ginkgo.By("delete the resource from source")
	gomega.Eventually(func() error {
		resourceID := store.ResourceID(clusterName, resourceName)
		resource, err = sourceStore.Get(resourceID)
		if err != nil {
			return err
		}

		if meta.IsStatusConditionTrue(resource.Status.Conditions, "Deleted") {
			sourceStore.Delete(resourceID)
		}

		return nil
	}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
}
