package store

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	"testing"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/common"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestAgentLister(t *testing.T) {
	watchStore := NewAgentInformerWatcherStore[*clusterv1.ManagedCluster]()

	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{
		Name:   "test1",
		Labels: map[string]string{common.CloudEventsOriginalSourceLabelKey: "source1"},
	}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.Store.Add(&clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{
		Name:   "test2",
		Labels: map[string]string{common.CloudEventsOriginalSourceLabelKey: "source1"},
	}}); err != nil {
		t.Error(err)
	}

	agentLister := NewAgentWatcherStoreLister(watchStore)
	clusters, err := agentLister.List(types.ListOptions{Source: "source1"})
	if err != nil {
		t.Error(err)
	}
	if len(clusters) != 2 {
		t.Error("unexpected clusters")
	}
}

func TestSourceLister(t *testing.T) {
	watchStore := NewAgentInformerWatcherStore[*workv1.ManifestWork]()

	if err := watchStore.Store.Add(&workv1.ManifestWork{ObjectMeta: metav1.ObjectMeta{
		Name:      "test1",
		Namespace: "cluster1",
	}}); err != nil {
		t.Error(err)
	}
	if err := watchStore.Store.Add(&workv1.ManifestWork{ObjectMeta: metav1.ObjectMeta{
		Name:      "test2",
		Namespace: "cluster2",
	}}); err != nil {
		t.Error(err)
	}

	agentLister := NewSourceWatcherStoreLister(watchStore)
	works, err := agentLister.List(types.ListOptions{ClusterName: "cluster1"})
	if err != nil {
		t.Error(err)
	}
	if len(works) != 1 {
		t.Error("unexpected works")
	}
}
