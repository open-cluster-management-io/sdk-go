package v1beta1

import (
	"fmt"

	cloudeventstypes "github.com/cloudevents/sdk-go/v2/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

var ManagedClusterAddOnEventDataType = types.CloudEventsDataType{
	Group:    addonapiv1beta1.GroupVersion.Group,
	Version:  addonapiv1beta1.GroupVersion.Version,
	Resource: "managedclusteraddons",
}

// ManagedClusterAddOnCodec is a codec to encode/decode a ManagedClusterAddOn/cloudevent for an agent.
type ManagedClusterAddOnCodec struct{}

func NewManagedClusterAddOnCodec() *ManagedClusterAddOnCodec {
	return &ManagedClusterAddOnCodec{}
}

// EventDataType always returns the event data type `addon.open-cluster-management.io.v1beta1.managedclusteraddons`.
func (c *ManagedClusterAddOnCodec) EventDataType() types.CloudEventsDataType {
	return ManagedClusterAddOnEventDataType
}

// Encode the ManagedClusterAddOn to a cloudevent
func (c *ManagedClusterAddOnCodec) Encode(source string, eventType types.CloudEventsType, addon *addonapiv1beta1.ManagedClusterAddOn) (*cloudevents.Event, error) {
	if eventType.CloudEventsDataType != ManagedClusterAddOnEventDataType {
		return nil, fmt.Errorf("unsupported cloudevents data type %s", eventType.CloudEventsDataType)
	}

	evt := types.NewEventBuilder(source, eventType).
		WithResourceID(addon.Name).
		WithClusterName(addon.Namespace).
		NewEvent()

	if addon.ResourceVersion != "" {
		evt.SetExtension(types.ExtensionResourceVersion, addon.ResourceVersion)
	}

	newAddon := addon.DeepCopy()
	newAddon.TypeMeta = metav1.TypeMeta{
		APIVersion: addonapiv1beta1.GroupVersion.String(),
		Kind:       "ManagedClusterAddOn",
	}

	if err := evt.SetData(cloudevents.ApplicationJSON, newAddon); err != nil {
		return nil, fmt.Errorf("failed to encode managedclusteraddon to a cloudevent: %v", err)
	}

	return &evt, nil
}

// Decode a cloudevent to a ManagedClusterAddOn
func (c *ManagedClusterAddOnCodec) Decode(evt *cloudevents.Event) (*addonapiv1beta1.ManagedClusterAddOn, error) {
	evtExtensions := evt.Context.GetExtensions()
	if _, ok := evtExtensions[types.ExtensionDeletionTimestamp]; ok {
		resourceID, err := cloudeventstypes.ToString(evtExtensions[types.ExtensionResourceID])
		if err != nil {
			return nil, fmt.Errorf("failed to get resourceid extension: %v", err)
		}

		namespace, name, err := cache.SplitMetaNamespaceKey(resourceID)
		if err != nil {
			return nil, fmt.Errorf("invalid resourceID: %v", err)
		}

		deletionTimestamp, err := cloudeventstypes.ToTime(evtExtensions[types.ExtensionDeletionTimestamp])
		if err != nil {
			return nil, fmt.Errorf("failed to get deletiontimestamp, %v", err)
		}

		return &addonapiv1beta1.ManagedClusterAddOn{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         namespace,
				DeletionTimestamp: &metav1.Time{Time: deletionTimestamp},
			},
		}, nil
	}

	addon := &addonapiv1beta1.ManagedClusterAddOn{}
	if err := evt.DataAs(addon); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event data %s, %v", string(evt.Data()), err)
	}

	return addon, nil
}
