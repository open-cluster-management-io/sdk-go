package v1beta1

import (
	"context"
	"encoding/json"
	jsonpatch "github.com/evanphx/json-patch/v5"
	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/statushash"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/store"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/clients"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/fake"
)

func TestPatch(t *testing.T) {
	cases := []struct {
		name        string
		clusterName string
		addon       *addonapiv1beta1.ManagedClusterAddOn
		patch       []byte
	}{
		{
			name:        "patch addon",
			clusterName: "cluster1",
			addon: &addonapiv1beta1.ManagedClusterAddOn{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "cluster1"},
			},
			patch: func() []byte {
				old := &addonapiv1beta1.ManagedClusterAddOn{
					ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "cluster1"},
				}
				oldData, err := json.Marshal(old)
				if err != nil {
					t.Error(err)
				}

				new := old.DeepCopy()
				new.Status = addonapiv1beta1.ManagedClusterAddOnStatus{
					Namespace: "install",
				}

				newData, err := json.Marshal(new)
				if err != nil {
					t.Error(err)
				}

				patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
				if err != nil {
					t.Error(err)
				}
				return patchBytes
			}(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			watcherStore := store.NewAgentInformerWatcherStore[*addonapiv1beta1.ManagedClusterAddOn]()

			ceClientOpt := fake.NewAgentOptions(fake.NewEventChan(), c.clusterName, c.clusterName+"agent")
			ceClient, err := clients.NewCloudEventAgentClient(
				ctx,
				ceClientOpt,
				store.NewAgentWatcherStoreLister(watcherStore),
				statushash.StatusHash,
				NewManagedClusterAddOnCodec())
			if err != nil {
				t.Error(err)
			}
			addonClient := NewAddonClientWrapper(NewManagedClusterAddOnClient(ceClient, watcherStore))
			if err := watcherStore.Store.Add(c.addon); err != nil {
				t.Error(err)
			}

			if _, err = addonClient.ManagedClusterAddOns(c.clusterName).Patch(
				ctx,
				c.addon.Name,
				types.MergePatchType,
				c.patch,
				metav1.PatchOptions{},
				"status",
			); err != nil {
				t.Error(err)
			}
		})
	}
}
