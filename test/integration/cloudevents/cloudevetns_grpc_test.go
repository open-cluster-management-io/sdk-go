package cloudevents

import (
	"context"
	"log"
	"strings"

	"github.com/onsi/ginkgo"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/constants"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var _ = ginkgo.Describe("CloudEvents Clients Test - GRPC", runCloudeventsClientPubSubTest(GetGRPCSourceOptions))

// The GRPC test simulates there is a server between the source and agent, the GRPC source client
// sends/receives events to/from server, then server forward the events to agent via GRPC broker.
func GetGRPCSourceOptions(ctx context.Context, sourceID string) (*options.CloudEventsSourceOptions, string) {
	// set sourceID for grpc broker
	grpcBroker.SetSourceID(sourceID)

	// forward the resource to agent and listen for the status from grpc broker
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-grpcServer.ResourceChan():
				if err := grpcBroker.UpdateResourceSpec(res); err != nil {
					log.Printf("failed to update resource spec via gRPC broker %s, %v", res.ResourceID, err)
				}
			case res := <-grpcBroker.ResourceStatusChan():
				// replace the source id with the original source id
				res.Source = sourceID
				if err := grpcServer.UpdateResourceStatus(res); err != nil {
					log.Printf("failed to update resource status %s, %v", res.ResourceID, err)
				}
			}
		}
	}()

	return grpcoptions.NewSourceOptions(util.NewGRPCSourceOptions(certPool, grpcServerHost, tokenFile), sourceID), constants.ConfigTypeGRPC
}

// ensureValidTokenUnary ensures a valid token exists within a request's metadata. If the token is missing or invalid, the interceptor blocks execution of the
// handler and returns an error. Otherwise, the interceptor invokes the unary handler.
func ensureValidTokenUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "missing metadata")
	}

	if !valid(md["authorization"]) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token")
	}
	// Continue execution of handler after ensuring a valid token.
	return handler(ctx, req)
}

// ensureValidTokenStream ensures a valid token exists within a request's metadata. If the token is missing or invalid, the interceptor blocks execution of the
// handler and returns an error. Otherwise, the interceptor invokes the stream handler.
func ensureValidTokenStream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing metadata")
	}

	if !valid(md["authorization"]) {
		return status.Errorf(codes.Unauthenticated, "invalid token")
	}
	// Continue execution of handler after ensuring a valid token.
	return handler(srv, stream)
}

// valid validates the authorization token.
func valid(authorization []string) bool {
	if len(authorization) < 1 {
		return false
	}
	token := strings.TrimPrefix(authorization[0], "Bearer ")
	return token == grpcStaticToken
}
