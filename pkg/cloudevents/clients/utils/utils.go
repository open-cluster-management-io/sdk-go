package utils

import (
	"fmt"

	"github.com/bwmarrin/snowflake"
	"github.com/google/uuid"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
)

// ListResourcesWithOptions retrieves the resources from store which matches the options.
func ListResourcesWithOptions[T generic.ResourceObject](store cache.Store, namespace string, opts metav1.ListOptions) ([]T, error) {
	var err error

	labelSelector := labels.Everything()
	fieldSelector := fields.Everything()

	if len(opts.LabelSelector) != 0 {
		labelSelector, err = labels.Parse(opts.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid labels selector %q: %v", opts.LabelSelector, err)
		}
	}

	if len(opts.FieldSelector) != 0 {
		fieldSelector, err = fields.ParseSelector(opts.FieldSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid fields selector %q: %v", opts.FieldSelector, err)
		}
	}

	resources := []T{}
	// list with labels
	if err := cache.ListAll(store, labelSelector, func(obj interface{}) {
		resourceMeta, ok := obj.(metav1.ObjectMeta)
		if !ok {
			return
		}

		if namespace != metav1.NamespaceAll && resourceMeta.Namespace != namespace {
			return
		}

		workFieldSet := fields.Set{
			"metadata.name":      resourceMeta.Name,
			"metadata.namespace": resourceMeta.Namespace,
		}

		if !fieldSelector.Matches(workFieldSet) {
			return
		}

		resource, ok := obj.(T)
		if !ok {
			return
		}

		resources = append(resources, resource)
	}); err != nil {
		return nil, err
	}

	return resources, nil
}

// CompareSnowflakeSequenceIDs compares two snowflake sequence IDs.
// Returns true if the current ID is greater than the last.
// If the last sequence ID is empty, then the current is greater.
func CompareSnowflakeSequenceIDs(last, current string) (bool, error) {
	if current != "" && last == "" {
		return true, nil
	}

	lastSID, err := snowflake.ParseString(last)
	if err != nil {
		return false, fmt.Errorf("unable to parse last sequence ID: %s, %v", last, err)
	}

	currentSID, err := snowflake.ParseString(current)
	if err != nil {
		return false, fmt.Errorf("unable to parse current sequence ID: %s %v", current, err)
	}

	if currentSID.Node() != lastSID.Node() {
		return false, fmt.Errorf("sequence IDs (%s,%s) are not from the same node", last, current)
	}

	if currentSID.Time() != lastSID.Time() {
		return currentSID.Time() > lastSID.Time(), nil
	}

	return currentSID.Step() > lastSID.Step(), nil
}

// UID returns a v5 UUID based on sourceID, groupResource, namespace and name to make sure it is consistent
func UID(sourceID, groupResource, namespace, name string) string {
	id := fmt.Sprintf("%s-%s-%s-%s", sourceID, groupResource, namespace, name)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(id)).String()
}
