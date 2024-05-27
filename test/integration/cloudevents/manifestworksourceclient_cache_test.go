package cloudevents

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	workv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work"
	agentcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/work/agent/codec"
	sourcecodec "open-cluster-management.io/sdk-go/pkg/cloudevents/work/source/codec"
	workstore "open-cluster-management.io/sdk-go/pkg/cloudevents/work/store"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/work/utils"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/agent"
)

var _ = ginkgo.Describe("ManifestWork source client test", func() {
	ginkgo.Context("CRUD the manifestwork with source client", func() {

		var ctx context.Context
		var cancel context.CancelFunc

		var sourceClient *work.ClientHolder

		var sourceID string
		var clusterName string
		var workName string

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())

			sourceID = fmt.Sprintf("watch-test-%s", rand.String(5))
			clusterName = fmt.Sprintf("watch-test-cluster-%s", rand.String(5))
			workName = fmt.Sprintf("watch-test-work-%s", rand.String(5))

			sourceConfig := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			watcherStore, err := workstore.NewLocalWatcherStore(ctx, func(ctx context.Context) ([]*workv1.ManifestWork, error) {
				return []*workv1.ManifestWork{}, nil
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			sourceClient, err = work.NewClientHolderBuilder(sourceConfig).
				WithClientID(fmt.Sprintf("%s-%s", sourceID, rand.String(5))).
				WithSourceID(sourceID).
				WithCodecs(sourcecodec.NewManifestBundleCodec()).
				WithWorkClientWatcherStore(watcherStore).
				NewSourceClientHolder(ctx)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("create/update/delete a manifestwork with source client", func() {
			work := newManifestWork(clusterName, workName)
			work.Spec.Workload.Manifests = []workv1.Manifest{newManifest("test1")}
			ginkgo.By("create a work with source client", func() {
				_, err := sourceClient.ManifestWorks(clusterName).Create(ctx, work, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				_, err = sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("update the work with source client", func() {
				found, err := sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				newWork := found.DeepCopy()
				newWork.Spec.Workload.Manifests = []workv1.Manifest{
					newManifest("test1"),
					newManifest("test2"),
				}
				patchBytes := patchWork(found, newWork)
				_, err = sourceClient.ManifestWorks(clusterName).Patch(
					ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				updatedWork, err := sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				gomega.Expect(len(updatedWork.Spec.Workload.Manifests)).To(gomega.BeEquivalentTo(2))
			})

			ginkgo.By("delete the work with source client", func() {
				// delete the work from source
				err := sourceClient.ManifestWorks(clusterName).Delete(ctx, workName, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				_, err = sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(errors.IsNotFound(err)).To(gomega.BeTrue())
			})
		})
	})

	ginkgo.Context("Watching the manifestworks with source client", func() {
		var ctx context.Context
		var cancel context.CancelFunc

		// this store save the works that are watched by source client
		var sourceWatcherStore cache.Store

		var sourceClient *work.ClientHolder
		var agentClient *work.ClientHolder

		var sourceID string
		var clusterName string
		var workName string

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())

			sourceID = fmt.Sprintf("watch-test-%s", rand.String(5))
			clusterName = fmt.Sprintf("watch-test-cluster-%s", rand.String(5))
			workName = fmt.Sprintf("watch-test-work-%s", rand.String(5))
			sourceWatcherStore = cache.NewStore(cache.MetaNamespaceKeyFunc)

			sourceConfig := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentConfig := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			watcherStore, err := workstore.NewLocalWatcherStore(ctx, func(ctx context.Context) ([]*workv1.ManifestWork, error) {
				return []*workv1.ManifestWork{}, nil
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			sourceClient, err = work.NewClientHolderBuilder(sourceConfig).
				WithClientID(fmt.Sprintf("%s-%s", sourceID, rand.String(5))).
				WithSourceID(sourceID).
				WithCodecs(sourcecodec.NewManifestBundleCodec()).
				WithWorkClientWatcherStore(watcherStore).
				NewSourceClientHolder(ctx)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			agentClient, _, err = agent.StartWorkAgent(
				ctx, clusterName, agentConfig, agentcodec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			watcher, err := sourceClient.ManifestWorks(clusterName).Watch(ctx, metav1.ListOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			go func() {
				ch := watcher.ResultChan()
				for {
					select {
					case <-ctx.Done():
						return
					case event, ok := <-ch:
						if !ok {
							return
						}

						switch event.Type {
						case watch.Added:
							err := sourceWatcherStore.Add(event.Object)
							gomega.Expect(err).ToNot(gomega.HaveOccurred())
						case watch.Modified:
							err := sourceWatcherStore.Update(event.Object)
							gomega.Expect(err).ToNot(gomega.HaveOccurred())
						case watch.Deleted:
							err := sourceWatcherStore.Delete(event.Object)
							gomega.Expect(err).ToNot(gomega.HaveOccurred())
						}
					}
				}
			}()
		})

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("watch the Added/Modified/Deleted events", func() {
			work := newManifestWork(clusterName, workName)
			work.Spec.Workload.Manifests = []workv1.Manifest{newManifest("test1")}

			ginkgo.By("create a work with source client", func() {
				_, err := sourceClient.ManifestWorks(clusterName).Create(ctx, work, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				_, err = sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				// this created work should be watched
				gomega.Eventually(func() error {
					_, exists, err := sourceWatcherStore.GetByKey(clusterName + "/" + workName)
					if err != nil {
						return err
					}
					if !exists {
						return fmt.Errorf("the new work is not watched")
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("update a work with source client", func() {
				found, err := sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				newWork := found.DeepCopy()
				newWork.Spec.Workload.Manifests = []workv1.Manifest{
					newManifest("test1"),
					newManifest("test2"),
				}
				patchBytes := patchWork(found, newWork)
				_, err = sourceClient.ManifestWorks(clusterName).Patch(
					ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClient.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					// add finalizers
					newWork := work.DeepCopy()
					newWork.Finalizers = []string{"test-finalizer"}
					patchBytes := patchWork(work, newWork)
					updateWork, err := agentClient.ManifestWorks(clusterName).Patch(
						ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// update the work status
					newWork = updateWork.DeepCopy()
					newWork.Status = workv1.ManifestWorkStatus{Conditions: []metav1.Condition{
						{Type: "Created", Status: metav1.ConditionTrue},
					}}
					patchBytes = patchWork(updateWork, newWork)
					_, err = agentClient.ManifestWorks(clusterName).Patch(
						ctx, work.Name, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

				// this updated work should be watched
				gomega.Eventually(func() error {
					work, exists, err := sourceWatcherStore.GetByKey(clusterName + "/" + workName)
					if err != nil {
						return err
					}
					if !exists {
						return fmt.Errorf("the work does not exist")
					}

					if len(work.(*workv1.ManifestWork).Spec.Workload.Manifests) != 2 {
						return fmt.Errorf("the updated work is not watched")
					}

					if len(work.(*workv1.ManifestWork).Status.Conditions) == 0 {
						return fmt.Errorf("the updated work status is not watched")
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})

			ginkgo.By("delete the work with source client", func() {
				// delete the work from source
				err := sourceClient.ManifestWorks(clusterName).Delete(ctx, workName, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				// agent delete the work and send deleted status to source
				gomega.Eventually(func() error {
					workID := utils.UID(sourceID, clusterName, workName)
					work, err := agentClient.ManifestWorks(clusterName).Get(ctx, workID, metav1.GetOptions{})
					if err != nil {
						return err
					}

					if work.DeletionTimestamp.IsZero() {
						return fmt.Errorf("work deletion timestamp is zero")
					}

					newWork := work.DeepCopy()
					newWork.Finalizers = []string{}
					patchBytes := patchWork(work, newWork)
					_, err = agentClient.ManifestWorks(clusterName).Patch(
						ctx, workID, apitypes.MergePatchType, patchBytes, metav1.PatchOptions{})
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())

				// the deleted work should be watched
				gomega.Eventually(func() error {
					_, exists, err := sourceWatcherStore.GetByKey(clusterName + "/" + workName)
					if err != nil {
						return err
					}
					if exists {
						return fmt.Errorf("the work is not deleted")
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})
		})
	})

	ginkgo.Context("Resync the manifestworks by agent", func() {
		var ctx context.Context
		var cancel context.CancelFunc

		var sourceID string
		var clusterName string
		var workName string

		ginkgo.BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())

			sourceID = fmt.Sprintf("watch-test-%s", rand.String(5))
			clusterName = fmt.Sprintf("watch-test-cluster-%s", rand.String(5))
			workName = fmt.Sprintf("watch-test-work-%s", rand.String(5))

			watcherStore, err := workstore.NewLocalWatcherStore(ctx, func(ctx context.Context) ([]*workv1.ManifestWork, error) {
				work := newManifestWork(clusterName, workName)
				work.UID = apitypes.UID(utils.UID(sourceID, clusterName, workName))
				work.ResourceVersion = "0"
				work.Spec.Workload.Manifests = []workv1.Manifest{newManifest("test1")}
				work.Status.Conditions = []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}
				return []*workv1.ManifestWork{work}, nil
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			sourceConfig := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			_, err = work.NewClientHolderBuilder(sourceConfig).
				WithClientID(fmt.Sprintf("%s-%s", sourceID, rand.String(5))).
				WithSourceID(sourceID).
				WithCodecs(sourcecodec.NewManifestBundleCodec()).
				WithWorkClientWatcherStore(watcherStore).
				NewSourceClientHolder(ctx)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			// cancel the context to stop the source client gracefully
			cancel()
		})

		ginkgo.It("agent resync the source manifestworks", func() {
			agentConfig := newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})
			agentClient, _, err := agent.StartWorkAgent(
				ctx, clusterName, agentConfig, agentcodec.NewManifestBundleCodec())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			ginkgo.By("get the work from agent", func() {
				gomega.Eventually(func() error {
					_, err := agentClient.ManifestWorks(clusterName).Get(
						ctx, utils.UID(sourceID, clusterName, workName), metav1.GetOptions{})
					if err != nil {
						return err
					}

					return nil
				}, 10*time.Second, 1*time.Second).Should(gomega.Succeed())
			})
		})
	})

	ginkgo.Context("Using the source client with customized lister", func() {
		var ctx context.Context
		var cancel context.CancelFunc

		// simulate a server to receive the works from source and works status from work agent
		var server cache.Store
		var serverListFn workstore.ListLocalWorksFunc

		var sourceConfig *mqtt.MQTTOptions

		var sourceClient *work.ClientHolder

		var sourceID string
		var clusterName string
		var workName string

		var manifestWork *workv1.ManifestWork

		ginkgo.BeforeEach(func() {
			sourceID = fmt.Sprintf("watch-test-%s", rand.String(5))
			clusterName = fmt.Sprintf("watch-test-cluster-%s", rand.String(5))
			workName = fmt.Sprintf("watch-test-work-%s", rand.String(5))

			server = cache.NewStore(cache.MetaNamespaceKeyFunc)
			serverListFn = func(ctx context.Context) ([]*workv1.ManifestWork, error) {
				works := []*workv1.ManifestWork{}
				for _, obj := range server.List() {
					if work, ok := obj.(*workv1.ManifestWork); ok {
						if len(work.Status.Conditions) != 0 {
							works = append(works, work)
						}
					}
				}
				return works, nil
			}

			sourceConfig = newMQTTOptions(types.Topics{
				SourceEvents:    fmt.Sprintf("sources/%s/consumers/+/sourceevents", sourceID),
				AgentEvents:     fmt.Sprintf("sources/%s/consumers/+/agentevents", sourceID),
				SourceBroadcast: "sources/+/sourcebroadcast",
			})

			manifestWork = newManifestWork(clusterName, workName)
			manifestWork.Spec.Workload.Manifests = []workv1.Manifest{newManifest("test1")}
		})

		ginkgo.AfterEach(func() {
			if cancel != nil {
				cancel()
			}
		})

		ginkgo.It("restart a source client with customized lister", func() {
			ginkgo.By("start a source client with customized lister", func() {
				ctx, cancel = context.WithCancel(context.Background())

				watcherStore, err := workstore.NewLocalWatcherStore(ctx, serverListFn)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				sourceClient, err = work.NewClientHolderBuilder(sourceConfig).
					WithClientID(fmt.Sprintf("%s-%s", sourceID, rand.String(5))).
					WithSourceID(sourceID).
					WithCodecs(sourcecodec.NewManifestBundleCodec()).
					WithWorkClientWatcherStore(watcherStore).
					NewSourceClientHolder(ctx)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("create a work by source client", func() {
				created, err := sourceClient.ManifestWorks(clusterName).Create(ctx, manifestWork, metav1.CreateOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				// simulate the server receive this work
				err = server.Add(created)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("stop the source client", func() {
				cancel()
			})

			ginkgo.By("update the work status on the server after source client is down", func() {
				// simulate the server receive this work status from agent
				obj, exists, err := server.GetByKey(clusterName + "/" + workName)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				gomega.Expect(exists).To(gomega.BeTrue())

				work, ok := obj.(*workv1.ManifestWork)
				gomega.Expect(ok).To(gomega.BeTrue())

				work.Status.Conditions = []metav1.Condition{{Type: "Created", Status: metav1.ConditionTrue}}
				err = server.Update(work)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			// start the source client again
			ginkgo.By("restart a source client with customized lister", func() {
				ctx, cancel = context.WithCancel(context.Background())

				watcherStore, err := workstore.NewLocalWatcherStore(ctx, serverListFn)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())

				sourceClient, err = work.NewClientHolderBuilder(sourceConfig).
					WithClientID(fmt.Sprintf("%s-%s", sourceID, rand.String(5))).
					WithSourceID(sourceID).
					WithCodecs(sourcecodec.NewManifestBundleCodec()).
					WithWorkClientWatcherStore(watcherStore).
					NewSourceClientHolder(ctx)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
			})

			ginkgo.By("the source client is able to get the latest work status", func() {
				found, err := sourceClient.ManifestWorks(clusterName).Get(ctx, workName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				gomega.Expect(meta.IsStatusConditionTrue(found.Status.Conditions, "Created")).To(gomega.BeTrue())
			})
		})
	})
})
