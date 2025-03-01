package store

import (
	"fmt"
	"sync"
)

type MemoryStore struct {
	sync.RWMutex

	resources map[string]*Resource
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		resources: make(map[string]*Resource),
	}
}

func (s *MemoryStore) Add(resource *Resource) {
	s.Lock()
	defer s.Unlock()

	_, ok := s.resources[resource.ResourceID]
	if !ok {
		s.resources[resource.ResourceID] = resource
	}
}

func (s *MemoryStore) Update(resource *Resource) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.resources[resource.ResourceID]
	if !ok {
		return fmt.Errorf("the resource %s does not exist", resource.ResourceID)
	}

	s.resources[resource.ResourceID] = resource
	return nil
}

func (s *MemoryStore) UpSert(resource *Resource) {
	s.Lock()
	defer s.Unlock()

	s.resources[resource.ResourceID] = resource
}

func (s *MemoryStore) UpdateStatus(resource *Resource) error {
	s.Lock()
	defer s.Unlock()

	last, ok := s.resources[resource.ResourceID]
	if !ok {
		return fmt.Errorf("the resource %s does not exist", resource.ResourceID)
	}

	last.Status = resource.Status
	s.resources[resource.ResourceID] = last
	return nil
}

func (s *MemoryStore) Delete(resourceID string) {
	s.Lock()
	defer s.Unlock()

	delete(s.resources, resourceID)
}

func (s *MemoryStore) Get(resourceID string) (*Resource, error) {
	s.RLock()
	defer s.RUnlock()

	resource, ok := s.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("failed to find resource %s", resourceID)
	}

	return resource, nil
}

func (s *MemoryStore) List(namespace string) []*Resource {
	s.RLock()
	defer s.RUnlock()

	resources := []*Resource{}
	for _, res := range s.resources {
		if res.Namespace != namespace {
			continue
		}

		resources = append(resources, res)
	}
	return resources
}
