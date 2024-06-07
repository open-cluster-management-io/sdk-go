package store

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// resourceHandler is a function that can handle resource status change events.
type resourceHandler func(res *Resource) error

// eventClient is a client that can receive and handle resource status change events.
type eventClient struct {
	source  string
	handler resourceHandler
	errChan chan<- error
}

// EventBroadcaster is a component that can broadcast resource status change events to registered clients.
type EventBroadcaster struct {
	mu sync.RWMutex

	// registered clients.
	clients map[string]*eventClient

	// inbound messages from the clients.
	broadcast chan *Resource
}

// NewEventBroadcaster creates a new event broadcaster.
func NewEventBroadcaster() *EventBroadcaster {
	return &EventBroadcaster{
		clients:   make(map[string]*eventClient),
		broadcast: make(chan *Resource),
	}
}

// Register registers a client for source and return client id and error channel.
func (eb *EventBroadcaster) Register(source string, handler resourceHandler) (string, <-chan error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	id := uuid.NewString()
	errChan := make(chan error)
	eb.clients[id] = &eventClient{
		source:  source,
		handler: handler,
		errChan: errChan,
	}

	return id, errChan
}

// Unregister unregisters a client by id
func (eb *EventBroadcaster) Unregister(id string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	close(eb.clients[id].errChan)
	delete(eb.clients, id)
}

// Broadcast broadcasts a resource status change event to all registered clients.
func (eb *EventBroadcaster) Broadcast(res *Resource) {
	eb.broadcast <- res
}

// Start starts the event broadcaster and waits for events to broadcast.
func (eb *EventBroadcaster) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case res := <-eb.broadcast:
			eb.mu.RLock()
			for _, client := range eb.clients {
				if client.source == res.Source {
					if err := client.handler(res); err != nil {
						client.errChan <- err
					}
				}
			}
			eb.mu.RUnlock()
		}
	}
}
