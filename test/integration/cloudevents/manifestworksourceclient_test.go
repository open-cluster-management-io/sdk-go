package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	addonapiv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
	addonv1alpha1client "open-cluster-management.io/api/client/addon/clientset/versioned"
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
			sourceClientHolder, err = source.StartManifestWorkSourceClient(ctx, testEnvConfig, sourceID, sourceMQTTOptions)
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

	ginkgo.Context("Publish a manifestwork with owner", func() {
		var ctx context.Context
		var cancel context.CancelFunc
		var sourceID string
		var clusterName string
		var workName1 string
		var workName2 string
		var workName3 string
		var workName4 string
		var sourceClientHolder *work.ClientHolder
		var agentClientHolder *work.ClientHolder
		var kubeClient kubernetes.Interface
		var addonClient addonv1alpha1client.Interface

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			sourceID = "integration-mw-test"
			clusterName = "cluster-a"
			workName1 = "test1"
			workName2 = "test2"
			workName3 = "test3"
			workName4 = "test4"

			var err error
			kubeClient, err = kubernetes.NewForConfig(testEnvConfig)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			addonClient, err = addonv1alpha1client.NewForConfig(testEnvConfig)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			sourceMQTTOptions := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			sourceClientHolder, err = source.StartManifestWorkSourceClient(ctx, testEnvConfig, sourceID, sourceMQTTOptions)
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
			ns, err := kubeClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
				},
			}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			cm, err := kubeClient.CoreV1().ConfigMaps(ns.Name).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Labels: map[string]string{
						"test": "test",
					},
				},
				Data: map[string]string{
					"test": "test",
				},
			}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			mwc, err := kubeClient.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(ctx, &admissionregistrationv1.MutatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Labels: map[string]string{
						"test": "test",
					},
				},
				Webhooks: []admissionregistrationv1.MutatingWebhook{},
			}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			mca, err := addonClient.AddonV1alpha1().ManagedClusterAddOns(ns.Name).Create(ctx, &addonapiv1alpha1.ManagedClusterAddOn{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Labels: map[string]string{
						"test": "test",
					},
				},
				Spec: addonapiv1alpha1.ManagedClusterAddOnSpec{
					InstallNamespace: "open-cluster-management-addon",
				},
			}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			cma, err := addonClient.AddonV1alpha1().ClusterManagementAddOns().Create(ctx, &addonapiv1alpha1.ClusterManagementAddOn{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
					Labels: map[string]string{
						"test": "test",
					},
				},
				Spec: addonapiv1alpha1.ClusterManagementAddOnSpec{
					InstallStrategy: addonapiv1alpha1.InstallStrategy{
						Type: addonapiv1alpha1.AddonInstallStrategyManual,
					},
				},
			}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			work1 := newManifestWork(clusterName, workName1)
			work2 := newManifestWork(clusterName, workName2)
			work3 := newManifestWork(clusterName, workName3)
			work4 := newManifestWork(clusterName, workName4)
			pTrue := true
			ownerReference1 := metav1.OwnerReference{
				APIVersion:         "v1",
				Kind:               "ConfigMap",
				Name:               cm.Name,
				UID:                cm.UID,
				BlockOwnerDeletion: &pTrue,
			}
			ownerReference2 := metav1.OwnerReference{
				APIVersion:         "v1",
				Kind:               "PersistentVolume",
				Name:               mwc.Name,
				UID:                mwc.UID,
				BlockOwnerDeletion: &pTrue,
			}
			ownerReference3 := metav1.OwnerReference{
				APIVersion:         addonapiv1alpha1.GroupVersion.String(),
				Kind:               "ManagedClusterAddon",
				Name:               mca.Name,
				UID:                mca.UID,
				BlockOwnerDeletion: &pTrue,
			}
			ownerReference4 := metav1.OwnerReference{
				APIVersion:         addonapiv1alpha1.GroupVersion.String(),
				Kind:               "ClusterManagementAddOn",
				Name:               cma.Name,
				UID:                cma.UID,
				BlockOwnerDeletion: &pTrue,
			}
			work1.SetOwnerReferences([]metav1.OwnerReference{ownerReference1})
			work2.SetOwnerReferences([]metav1.OwnerReference{ownerReference1, ownerReference2})
			work3.SetOwnerReferences([]metav1.OwnerReference{ownerReference3})
			work4.SetOwnerReferences([]metav1.OwnerReference{ownerReference3, ownerReference4})

			ginkgo.By("create work with owner by source client", func() {
				_, err := sourceClientHolder.ManifestWorks(clusterName).Create(ctx, work1, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				_, err = sourceClientHolder.ManifestWorks(clusterName).Create(ctx, work2, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				_, err = sourceClientHolder.ManifestWorks(clusterName).Create(ctx, work3, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				_, err = sourceClientHolder.ManifestWorks(clusterName).Create(ctx, work4, metav1.CreateOptions{})
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

					workID3 := utils.UID(sourceID, clusterName, workName3)
					appliedWork, err = agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID3, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newAppliedWork = appliedWork.DeepCopy()
					newAppliedWork.Finalizers = []string{"test-finalizer"}
					patchBytes = patchWork(appliedWork, newAppliedWork)
					updateWork, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID3, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newAppliedWork = updateWork.DeepCopy()
					newAppliedWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newAppliedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID3, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					workID4 := utils.UID(sourceID, clusterName, workName4)
					appliedWork, err = agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID4, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newAppliedWork = appliedWork.DeepCopy()
					newAppliedWork.Finalizers = []string{"test-finalizer"}
					patchBytes = patchWork(appliedWork, newAppliedWork)
					updateWork, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID3, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newAppliedWork = updateWork.DeepCopy()
					newAppliedWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}}
					patchBytes = patchWork(updateWork, newAppliedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID3, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
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

					work3, err = sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName3, metav1.GetOptions{})
					if err != nil {
						return err
					}
					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work3.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work3.Status.Conditions)
					}

					work4, err = sourceClientHolder.ManifestWorks(clusterName).Get(ctx, workName3, metav1.GetOptions{})
					if err != nil {
						return err
					}
					// ensure the resource status is synced
					if !meta.IsStatusConditionTrue(work4.Status.Conditions, "Created") {
						return fmt.Errorf("unexpected status %v", work4.Status.Conditions)
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("delete work owner 1 from source", func() {
				// envtest does't have GC controller
				err = kubeClient.CoreV1().ConfigMaps(ns.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent delete the work 1", func() {
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

			ginkgo.By("source check the work 1 deletion", func() {
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

			ginkgo.By("delete work owner 2 from source", func() {
				// envtest does't have GC controller
				err = kubeClient.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, mwc.Name, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent delete the work 2", func() {
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

			ginkgo.By("source check the work 2 deletion", func() {
				gomega.Eventually(func() error {
					work2, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName2, metav1.GetOptions{})
					if err == nil || !errors.IsNotFound(err) {
						return fmt.Errorf("the work %s/%s is not deleted", work2.GetNamespace(), work2.GetName())
					}
					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("delete work owner 3 from source", func() {
				// envtest does't have GC controller
				err = addonClient.AddonV1alpha1().ManagedClusterAddOns(mca.GetNamespace()).Delete(ctx, mca.GetName(), metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent delete the work 3", func() {
				gomega.Eventually(func() error {
					workID3 := utils.UID(sourceID, clusterName, workName3)
					appliedWork, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID3, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if appliedWork.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					updatedWork := appliedWork.DeepCopy()
					updatedWork.Finalizers = []string{}
					patchBytes := patchWork(appliedWork, updatedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID3, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source check the work 3 deletion", func() {
				gomega.Eventually(func() error {
					work3, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName3, metav1.GetOptions{})
					if err == nil || !errors.IsNotFound(err) {
						return fmt.Errorf("the work %s/%s is not deleted", work3.GetNamespace(), work3.GetName())
					}

					work4, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName4, metav1.GetOptions{})
					if err != nil {
						return err
					}
					if len(work4.GetOwnerReferences()) != 1 {
						return fmt.Errorf("unexpected owner references (%v) for the work %s/%s", work4.GetOwnerReferences(), work4.GetNamespace(), work4.GetName())
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("delete work owner 4 from source", func() {
				// envtest does't have GC controller
				err = addonClient.AddonV1alpha1().ClusterManagementAddOns().Delete(ctx, cma.GetName(), metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("agent delete the work 4", func() {
				gomega.Eventually(func() error {
					workID4 := utils.UID(sourceID, clusterName, workName4)
					appliedWork, err := agentClientHolder.ManifestWorks(clusterName).Get(ctx, workID4, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if appliedWork.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					updatedWork := appliedWork.DeepCopy()
					updatedWork.Finalizers = []string{}
					patchBytes := patchWork(appliedWork, updatedWork)
					_, err = agentClientHolder.ManifestWorks(clusterName).Patch(ctx, workID4, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 30*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("source check the work 4 deletion", func() {
				gomega.Eventually(func() error {
					work4, err = sourceClientHolder.WorkInterface().WorkV1().ManifestWorks(clusterName).Get(ctx, workName4, metav1.GetOptions{})
					if err == nil || !errors.IsNotFound(err) {
						return fmt.Errorf("the work %s/%s is not deleted", work4.GetNamespace(), work4.GetName())
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
			sourceClientHolder, err = source.StartManifestWorkSourceClient(ctx, testEnvConfig, sourceID, sourceMQTTOptions)
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
			sourceClientHolder, err := source.StartManifestWorkSourceClient(ctx, testEnvConfig, sourceID, mqttOptions)
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
