package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apitypes "k8s.io/apimachinery/pkg/types"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/agent/codec"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/common"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/utils"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/agent"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
)

var _ = ginkgo.Describe("ManifestWork source client test", func() {
	ginkgo.Context("Publish a manifestwork", func() {
		var sourceID string
		var clusterName string
		var workName string
		var sourceClientHolder *work.ClientHolder
		var agentClientHolder *work.ClientHolder

		ginkgo.BeforeEach(func() {
			sourceID = "integration-mw-test"
			clusterName = "cluster-a"
			workName = "test"

			var err error
			sourceMQTTOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			sourceClientHolder, err = source.StartManifestWorkSourceClient(context.TODO(), sourceID, sourceMQTTOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)

			agentMqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentClientHolder, err = agent.StartWorkAgent(context.TODO(), clusterName, agentMqttOptions, codec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)
		})

		ginkgo.It("CRUD a manifestwork with manifestwork source client and agent client", func() {
			ginkgo.By("create a work with source client", func() {
				_, err := sourceClientHolder.ManifestWorks(clusterName).Create(context.TODO(), newManifestWork(clusterName, workName), metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent update the work status", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newWork := work.DeepCopy()
					newWork.Finalizers = []string{"test-finalizer"}
					patchBytes := patchWork(work, newWork)
					updateWork, err := agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newWork = updateWork.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source update the work again", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work.Status.Conditions)
					}

					// source update the work
					newWork := work.DeepCopy()
					newWork.Annotations[common.CloudEventsGenerationAnnotationKey] = "2"
					newWork.Spec.Workload.Manifests = []workv1.Manifest{
						newManifest("test1"),
						newManifest("test2"),
					}
					patchBytes := patchWork(work, newWork)
					_, err = sourceClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent update the work status again", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if len(work.Spec.Workload.Manifests) != 2 {
						return fmt.Errorf("unexpected work spec %v", work.Spec.Workload.Manifests)
					}

					newWork := work.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Updated", Status: metav1.ConditionTrue}}}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source mark the work is deleting", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work.Status.Conditions, "Updated") {
						return fmt.Errorf("unexpected status %v", work.Status.Conditions)
					}

					err = sourceClientHolder.ManifestWorks(clusterName).Delete(context.Background(), workName, metav1.DeleteOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent delete the work", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if work.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					newWork := work.DeepCopy()
					newWork.Finalizers = []string{}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), workID, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source delete the work", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(context.TODO(), workName, metav1.GetOptions{})
					if errors.IsNotFound(err) {
						return nil
					}

					if err != nil {
						return err
					}

					return fmt.Errorf("the work is not deleted, %v", work.Status)
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})
		})
	})

	ginkgo.Context("Publish a manifestwork without version", func() {
		var sourceID string
		var clusterName string
		var workName string
		var sourceClientHolder *work.ClientHolder
		var agentClientHolder *work.ClientHolder

		ginkgo.BeforeEach(func() {
			sourceID = "integration-mw-test"
			clusterName = "cluster-a"
			workName = "test"

			var err error
			sourceMQTTOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			sourceClientHolder, err = source.StartManifestWorkSourceClient(context.TODO(), sourceID, sourceMQTTOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)

			agentMqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentClientHolder, err = agent.StartWorkAgent(context.TODO(), clusterName, agentMqttOptions, codec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)
		})

		ginkgo.It("CRUD a manifestwork with manifestwork source client and agent client", func() {
			ginkgo.By("create a work with source client", func() {
				_, err := sourceClientHolder.ManifestWorks(clusterName).Create(context.TODO(), newManifestWorkWithoutVersion(clusterName, workName), metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent update the work status", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newWork := work.DeepCopy()
					newWork.Finalizers = []string{"test-finalizer"}
					patchBytes := patchWork(work, newWork)
					updateWork, err := agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newWork = updateWork.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source update the work again", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work.Status.Conditions)
					}

					// source update the work
					newWork := work.DeepCopy()
					newWork.Spec.Workload.Manifests = []workv1.Manifest{
						newManifest("test1"),
						newManifest("test2"),
					}
					patchBytes := patchWork(work, newWork)
					_, err = sourceClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent update the work status again", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if len(work.Spec.Workload.Manifests) != 2 {
						return fmt.Errorf("unexpected work spec %v", work.Spec.Workload.Manifests)
					}

					newWork := work.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Updated", Status: metav1.ConditionTrue}}}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source mark the work is deleting", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work.Status.Conditions, "Updated") {
						return fmt.Errorf("unexpected status %v", work.Status.Conditions)
					}

					err = sourceClientHolder.ManifestWorks(clusterName).Delete(context.Background(), workName, metav1.DeleteOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent delete the work", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(context.TODO(), workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if work.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					newWork := work.DeepCopy()
					newWork.Finalizers = []string{}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(context.TODO(), workID, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source delete the work", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(context.TODO(), workName, metav1.GetOptions{})
					if errors.IsNotFound(err) {
						return nil
					}

					if err != nil {
						return err
					}

					return fmt.Errorf("the work is not deleted, %v", work.Status)
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})
		})
	})

	ginkgo.Context("Resync manifestworks", func() {
		var sourceID string
		var clusterName string
		var workNamePrefix string

		ginkgo.BeforeEach(func() {
			sourceID = "integration-mw-resync-test"
			clusterName = "cluster-b"
			workNamePrefix = "resync-test"
			mqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			agentClientHolder, err := agent.StartWorkAgent(context.TODO(), clusterName, mqttOptions, codec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// wait for informer started
			<-time.After(time.Second)

			// add two works in the agent cache
			store := agentClientHolder.ManifestWorkInformer().Informer().GetStore()
			work1UID := utils.UID(sourceID, clusterName, fmt.Sprintf("%s-1", workNamePrefix))
			work1 := newManifestWorkWithStatus(clusterName, work1UID)
			work1.UID = apitypes.UID(work1UID)
			work1.ResourceVersion = "1"
			work1.Labels = map[string]string{common.CloudEventsOriginalSourceLabelKey: sourceID}
			gomega.Expect(store.Add(work1)).ToNot(gomega.HaveOccurred())

			work2UID := utils.UID(sourceID, clusterName, fmt.Sprintf("%s-2", workNamePrefix))
			work2 := newManifestWorkWithStatus(clusterName, work2UID)
			work2.UID = apitypes.UID(work2UID)
			work2.ResourceVersion = "1"
			work2.Labels = map[string]string{common.CloudEventsOriginalSourceLabelKey: sourceID}
			gomega.Expect(store.Add(work2)).ToNot(gomega.HaveOccurred())

			// wait for cache ready
			<-time.After(time.Second)
		})

		ginkgo.It("resync manifestworks with manifestwork source client", func() {

			mqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			// simulate a source client restart, recover two works
			sourceClientHolder, err := source.StartManifestWorkSourceClient(context.TODO(), sourceID, mqttOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			_, err = sourceClientHolder.ManifestWorks(clusterName).Create(context.TODO(), newManifestWork(clusterName, fmt.Sprintf("%s-1", workNamePrefix)), metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			_, err = sourceClientHolder.ManifestWorks(clusterName).Create(context.TODO(), newManifestWork(clusterName, fmt.Sprintf("%s-2", workNamePrefix)), metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			ginkgo.By("the manifestworks are synced", func() {
				gomega.Eventually(func() error {
					work1, err := sourceClientHolder.ManifestWorks(clusterName).Get(context.TODO(), fmt.Sprintf("%s-1", workNamePrefix), metav1.GetOptions{})
					if err != nil {
						return err
					}
					if !meta.IsStatusConditionTrue(work1.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work1.Status.Conditions)
					}

					work2, err := sourceClientHolder.ManifestWorks(clusterName).Get(context.TODO(), fmt.Sprintf("%s-2", workNamePrefix), metav1.GetOptions{})
					if err != nil {
						return err
					}
					if !meta.IsStatusConditionTrue(work2.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work2.Status.Conditions)
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})
		})
	})
})

func newManifestWork(namespace, name string) *workv1.ManifestWork {
	return &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				common.CloudEventsGenerationAnnotationKey: "1",
			},
		},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: []workv1.Manifest{
					newManifest("test"),
				},
			},
		},
	}
}

func newManifestWorkWithoutVersion(namespace, name string) *workv1.ManifestWork {
	return &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: []workv1.Manifest{
					newManifest("test"),
				},
			},
		},
	}
}

func newManifestWorkWithStatus(namespace, name string) *workv1.ManifestWork {
	work := newManifestWork(namespace, name)
	work.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
	return work
}

func newManifest(name string) workv1.Manifest {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]interface{}{
				"namespace": "test",
				"name":      name,
			},
			"data": "test",
		},
	}
	objectStr, _ := obj.MarshalJSON()
	manifest := workv1.Manifest{}
	manifest.Raw = objectStr
	return manifest
}

func patchWork(old, new *workv1.ManifestWork) []byte {
	oldData, err := json.Marshal(old)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	newData, err := json.Marshal(new)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	return patchBytes
}
