package server

import (
	"context"
)

// AgentEventServer handles resource-related events between grpc server and agents:
// 1. Resource spec events (create, update and delete) from the resource controller.
// 2. Resource status update events from the agent.
type AgentEventServer interface {
	EventHandler

	// Start initiates the EventServer.
	Start(ctx context.Context)
}

type EventHandler interface {
	// OnCreate is the callback when resource is created in the service.
	OnCreate(ctx context.Context, resourceID string) error

	// OnUpdate is the callback when resource is updated in the service.
	OnUpdate(ctx context.Context, resourceID string) error

	// OnDelete is the callback when resource is deleted from the service.
	OnDelete(ctx context.Context, resourceID string) error
}

// TODO SourceEventServer to handle the grpc conversation between consumers and grpcserver.
