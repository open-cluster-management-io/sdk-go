package store

import (
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/clients/utils"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
)

type baseStore[T generic.ResourceObject] struct {
	sync.RWMutex

	store     cache.Store
	initiated bool
	zero      T
}

// List the works from the store with the list options
func (b *baseStore[T]) List(namespace string, opts metav1.ListOptions) ([]T, error) {
	b.RLock()
	defer b.RUnlock()

	resources, err := utils.ListResourcesWithOptions[T](b.store, namespace, opts)
	if err != nil {
		return nil, err
	}

	return resources, nil
}

// Get a works from the store
func (b *baseStore[T]) Get(namespace, name string) (T, bool, error) {
	b.RLock()
	defer b.RUnlock()

	key := name
	if len(namespace) != 0 {
		key = fmt.Sprintf("%s/%s", namespace, name)
	}

	obj, exists, err := b.store.GetByKey(key)
	if err != nil {
		return b.zero, false, err
	}

	if !exists {
		return b.zero, false, nil
	}

	resource, ok := obj.(T)
	if !ok {
		return b.zero, false, fmt.Errorf("unknown type %T", obj)
	}

	return resource, true, nil
}

// List all of works from the store
func (b *baseStore[T]) ListAll() ([]T, error) {
	b.RLock()
	defer b.RUnlock()

	resources := []T{}
	for _, obj := range b.store.List() {
		if res, ok := obj.(T); ok {
			resources = append(resources, res)
		}
	}

	return resources, nil
}
