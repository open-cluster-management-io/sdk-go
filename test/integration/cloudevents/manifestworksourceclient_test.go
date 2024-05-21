package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
	fakemetadata "k8s.io/client-go/metadata/fake"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/agent/codec"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/common"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/garbagecollector"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/utils"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/agent"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/source"
)

var _ = ginkgo.Describe("ManifestWork source client test", func() {
	ginkgo.Context("Publish a manifestwork", func() {
		var ctx context.Context
		var cancel context.CancelFunc
		var sourceID string
		var clusterName string
		var workName string
		var sourceClientHolder *work.ClientHolder
		var agentClientHolder *work.ClientHolder

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			sourceID = "integration-mw-test"
			clusterName = "cluster-a"
			workName = "test"

			var err error
			sourceMQTTOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			sourceClientHolder, err = source.StartManifestWorkSourceClient(ctx, sourceID, sourceMQTTOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)

			agentMqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentClientHolder, err = agent.StartWorkAgent(ctx, clusterName, agentMqttOptions, codec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)
		})

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("CRUD a manifestwork with manifestwork source client and agent client", func() {
			ginkgo.By("create a work with source client", func() {
				_, err := sourceClientHolder.ManifestWorks(clusterName).Create(ctx, newManifestWork(clusterName, workName), metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent update the work status", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newWork := work.DeepCopy()
					newWork.Finalizers = []string{"test-finalizer"}
					patchBytes := patchWork(work, newWork)
					updateWork, err := agentClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newWork = updateWork.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source update the work again", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
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
					_, err = sourceClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent update the work status again", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if len(work.Spec.Workload.Manifests) != 2 {
						return fmt.Errorf("unexpected work spec %v", work.Spec.Workload.Manifests)
					}

					newWork := work.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Updated", Status: metav1.ConditionTrue}}}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source mark the work is deleting", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work.Status.Conditions, "Updated") {
						return fmt.Errorf("unexpected status %v", work.Status.Conditions)
					}

					err = sourceClientHolder.ManifestWorks(clusterName).Delete(ctx, workName, metav1.DeleteOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent delete the work", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if work.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					newWork := work.DeepCopy()
					newWork.Finalizers = []string{}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source delete the work", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
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

	ginkgo.Context("Publish a manifestwork with owner reference", func() {
		var ctx context.Context
		var cancel context.CancelFunc
		var sourceID string
		var clusterName string
		var workName1 string
		var workName2 string
		var sourceClientHolder *work.ClientHolder
		var agentClientHolder *work.ClientHolder
		var metadataClient metadata.Interface

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			sourceID = "integration-mw-test"
			clusterName = "default"
			workName1 = "test1"
			workName2 = "test2"

			var err error
			sourceMQTTOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			cm := &metav1.PartialObjectMetadata{
				TypeMeta: metav1.TypeMeta{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "ConfigMap",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: clusterName,
					UID:       "123",
					Labels:    map[string]string{"test": "test"},
				},
			}
			srt := &metav1.PartialObjectMetadata{
				TypeMeta: metav1.TypeMeta{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "Secret",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: clusterName,
					UID:       "456",
					Labels:    map[string]string{"test": "test"},
				},
			}
			scheme := fakemetadata.NewTestScheme()
			err = metav1.AddMetaToScheme(scheme)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			metadataClient = fakemetadata.NewSimpleMetadataClient(scheme, cm, srt)
			sourceClientHolder, err = source.StartManifestWorkSourceClient(ctx, sourceID, sourceMQTTOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			listOptions := &metav1.ListOptions{
				LabelSelector: "test=test",
				FieldSelector: "metadata.name=test",
			}
			ownerGVRFilters := map[schema.GroupVersionResource]*metav1.ListOptions{
				corev1.SchemeGroupVersion.WithResource("configmaps"): listOptions,
				corev1.SchemeGroupVersion.WithResource("secrets"):    listOptions,
			}
			garbageCollector := garbagecollector.NewGarbageCollector(sourceClientHolder, metadataClient, ownerGVRFilters)
			go garbageCollector.Run(ctx, 1)

			// wait for cache ready
			<-time.After(time.Second)

			agentMqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentClientHolder, err = agent.StartWorkAgent(ctx, clusterName, agentMqttOptions, codec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)
		})

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("CRUD a manifestwork with manifestwork source client and agent client", func() {
			cmVGR := corev1.SchemeGroupVersion.WithResource("configmaps")
			srtGVR := corev1.SchemeGroupVersion.WithResource("secrets")
			cmObj, err := metadataClient.Resource(corev1.SchemeGroupVersion.WithResource("configmaps")).Namespace(clusterName).Get(ctx, "test", metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			srtObj, err := metadataClient.Resource(corev1.SchemeGroupVersion.WithResource("secrets")).Namespace(clusterName).Get(ctx, "test", metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			work1 := newManifestWork(clusterName, workName1)
			work2 := newManifestWork(clusterName, workName2)
			pTrue := true
			ownerReference1 := metav1.OwnerReference{
				APIVersion:         corev1.SchemeGroupVersion.String(),
				Kind:               "ConfigMap",
				Name:               cmObj.Name,
				UID:                cmObj.UID,
				BlockOwnerDeletion: &pTrue,
			}
			ownerReference2 := metav1.OwnerReference{
				APIVersion:         corev1.SchemeGroupVersion.String(),
				Kind:               "Secret",
				Name:               srtObj.Name,
				UID:                srtObj.UID,
				BlockOwnerDeletion: &pTrue,
			}
			work1.SetOwnerReferences([]metav1.OwnerReference{ownerReference1})
			work2.SetOwnerReferences([]metav1.OwnerReference{ownerReference1, ownerReference2})

			ginkgo.By("create work with owner by source client", func() {
				_, err := sourceClientHolder.ManifestWorks(clusterName).Create(ctx, work1, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				_, err = sourceClientHolder.ManifestWorks(clusterName).Create(ctx, work2, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent update the work status", func() {
				gomega.Eventually(func() error {
					workID1 := utils.UID(sourceID, clusterName, workName1)
					appliedWork, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID1, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newAppliedWork := appliedWork.DeepCopy()
					newAppliedWork.Finalizers = []string{"test-finalizer"}
					patchBytes := patchWork(appliedWork, newAppliedWork)
					updateWork, err := agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID1, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newAppliedWork = updateWork.DeepCopy()
					newAppliedWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newAppliedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID1, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					workID2 := utils.UID(sourceID, clusterName, workName2)
					appliedWork, err = agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID2, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newAppliedWork = appliedWork.DeepCopy()
					newAppliedWork.Finalizers = []string{"test-finalizer"}
					patchBytes = patchWork(appliedWork, newAppliedWork)
					updateWork, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID2, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newAppliedWork = updateWork.DeepCopy()
					newAppliedWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newAppliedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID2, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source check the work status", func() {
				gomega.Eventually(func() error {
					work1, err = sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName1, metav1.GetOptions{})
					if err != nil {
						return err
					}
					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work1.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work1.Status.Conditions)
					}

					work2, err = sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName2, metav1.GetOptions{})
					if err != nil {
						return err
					}
					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work2.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work2.Status.Conditions)
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("delete namespace-scoped owner of work from source", func() {
				// envtest does't have GC controller
				err := metadataClient.Resource(cmVGR).Namespace(clusterName).Delete(ctx, cmObj.Name, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent delete the first work with single owner", func() {
				gomega.Eventually(func() error {
					workID1 := utils.UID(sourceID, clusterName, workName1)
					appliedWork, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID1, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if appliedWork.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					updatedWork := appliedWork.DeepCopy()
					updatedWork.Finalizers = []string{}
					patchBytes := patchWork(appliedWork, updatedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID1, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source check the work deletion with single owner", func() {
				gomega.Eventually(func() error {
					work1, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName1, metav1.GetOptions{})
					if err == nil || !errors.IsNotFound(err) {
						return fmt.Errorf("the work %s/%s is not deleted", work1.GetNamespace(), work1.GetName())
					}

					work2, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName2, metav1.GetOptions{})
					if err != nil {
						return err
					}
					if len(work2.GetOwnerReferences()) != 1 {
						return fmt.Errorf("unexpected owner references (%v) for the work %s/%s", work2.GetOwnerReferences(), work2.GetNamespace(), work2.GetName())
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("delete cluster-scoped owner of work from source", func() {
				// envtest does't have GC controller
				err = metadataClient.Resource(srtGVR).Namespace(clusterName).Delete(ctx, srtObj.Name, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent delete the work with two owners", func() {
				gomega.Eventually(func() error {
					workID2 := utils.UID(sourceID, clusterName, workName2)
					appliedWork, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID2, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if appliedWork.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					updatedWork := appliedWork.DeepCopy()
					updatedWork.Finalizers = []string{}
					patchBytes := patchWork(appliedWork, updatedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID2, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source check the work deletion with two owners", func() {
				gomega.Eventually(func() error {
					work2, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName2, metav1.GetOptions{})
					if err == nil || !errors.IsNotFound(err) {
						return fmt.Errorf("the work %s/%s is not deleted", work2.GetNamespace(), work2.GetName())
					}
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})
		})
	})

	ginkgo.Context("Publish a manifestwork without version", func() {
		var ctx context.Context
		var cancel context.CancelFunc
		var sourceID string
		var clusterName string
		var workName string
		var sourceClientHolder *work.ClientHolder
		var agentClientHolder *work.ClientHolder

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			sourceID = "integration-mw-test"
			clusterName = "cluster-a"
			workName = "test"

			var err error
			sourceMQTTOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			sourceClientHolder, err = source.StartManifestWorkSourceClient(ctx, sourceID, sourceMQTTOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)

			agentMqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentClientHolder, err = agent.StartWorkAgent(ctx, clusterName, agentMqttOptions, codec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// wait for cache ready
			<-time.After(time.Second)
		})

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("CRUD a manifestwork with manifestwork source client and agent client", func() {
			ginkgo.By("create a work with source client", func() {
				_, err := sourceClientHolder.ManifestWorks(clusterName).Create(ctx, newManifestWorkWithoutVersion(clusterName, workName), metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent update the work status", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newWork := work.DeepCopy()
					newWork.Finalizers = []string{"test-finalizer"}
					patchBytes := patchWork(work, newWork)
					updateWork, err := agentClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newWork = updateWork.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source update the work again", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
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
					_, err = sourceClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent update the work status again", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if len(work.Spec.Workload.Manifests) != 2 {
						return fmt.Errorf("unexpected work spec %v", work.Spec.Workload.Manifests)
					}

					newWork := work.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Updated", Status: metav1.ConditionTrue}}}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source mark the work is deleting", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work.Status.Conditions, "Updated") {
						return fmt.Errorf("unexpected status %v", work.Status.Conditions)
					}

					err = sourceClientHolder.ManifestWorks(clusterName).Delete(ctx, workName, metav1.DeleteOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("agent delete the work", func() {
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if work.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					newWork := work.DeepCopy()
					newWork.Finalizers = []string{}
					patchBytes := patchWork(work, newWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source delete the work", func() {
				gomega.Eventually(func() error {
					work, err := sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
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
		var ctx context.Context
		var cancel context.CancelFunc
		var sourceID string
		var clusterName string
		var workNamePrefix string

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			sourceID = "integration-mw-resync-test"
			clusterName = "cluster-b"
			workNamePrefix = "resync-test"
			mqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			agentClientHolder, err := agent.StartWorkAgent(ctx, clusterName, mqttOptions, codec.NewManifestBundleCodec())
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

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("resync manifestworks with manifestwork source client", func() {

			mqttOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			// simulate a source client restart, recover two works
			sourceClientHolder, err := source.StartManifestWorkSourceClient(ctx, sourceID, mqttOptions)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			_, err = sourceClientHolder.ManifestWorks(clusterName).Create(ctx, newManifestWork(clusterName, fmt.Sprintf("%s-1", workNamePrefix)), metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			_, err = sourceClientHolder.ManifestWorks(clusterName).Create(ctx, newManifestWork(clusterName, fmt.Sprintf("%s-2", workNamePrefix)), metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			ginkgo.By("the manifestworks are synced", func() {
				gomega.Eventually(func() error {
					work1, err := sourceClientHolder.ManifestWorks(clusterName).Get(ctx, fmt.Sprintf("%s-1", workNamePrefix), metav1.GetOptions{})
					if err != nil {
						return err
					}
					if !meta.IsStatusConditionTrue(work1.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work1.Status.Conditions)
					}

					work2, err := sourceClientHolder.ManifestWorks(clusterName).Get(ctx, fmt.Sprintf("%s-2", workNamePrefix), metav1.GetOptions{})
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
