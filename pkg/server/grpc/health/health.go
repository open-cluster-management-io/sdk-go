package health

import (
	"context"
	"google.golang.org/grpc"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Custom health server with periodic broadcasts
type heartbeatHealthServer struct {
	interval time.Duration
	healthpb.UnimplementedHealthServer
	status healthpb.HealthCheckResponse_ServingStatus
}

func RegisterHeartbeatHealthServer(srv *grpc.Server, interval time.Duration) {
	healthpb.RegisterHealthServer(srv, &heartbeatHealthServer{
		interval: interval,
	})
}

func (s *heartbeatHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: s.status}, nil
}

func (s *heartbeatHealthServer) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	ticker := time.NewTicker(s.interval) // send every 5s
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := stream.Send(&healthpb.HealthCheckResponse{Status: s.status}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
