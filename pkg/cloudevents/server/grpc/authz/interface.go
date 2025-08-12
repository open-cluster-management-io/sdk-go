package authz

import (
	"context"
	"google.golang.org/grpc"
)

type UnaryAuthorizer interface {
	AuthorizeRequest(ctx context.Context, req any) error
}

type StreamAuthorizer interface {
	AuthorizeStream(ctx context.Context, ss grpc.ServerStream, info *grpc.StreamServerInfo) (grpc.ServerStream, error)
}
