package health

import (
	"context"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestHeartbeatHealthServer_Check(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	RegisterHeartbeatHealthServer(s, 1*time.Second)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()

	// Wait for server to start accepting connections
	time.Sleep(50 * time.Millisecond)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(bufDialer))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)

	// Test Check method
	ctx := context.Background()
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("Expected status SERVING, got %v", resp.Status)
	}
}

func TestHeartbeatHealthServer_Watch(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	interval := 100 * time.Millisecond
	RegisterHeartbeatHealthServer(s, interval)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()

	// Wait for server to start accepting connections
	time.Sleep(50 * time.Millisecond)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(bufDialer))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := healthpb.NewHealthClient(conn)

	// Test Watch method
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Receive multiple health check responses
	receivedCount := 0
	for receivedCount < 3 {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Failed to receive health check response: %v", err)
		}

		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			t.Errorf("Expected status SERVING, got %v", resp.Status)
		}

		receivedCount++
	}

	if receivedCount < 3 {
		t.Errorf("Expected to receive at least 3 health check responses, got %d", receivedCount)
	}
}

func TestHeartbeatHealthServer_WatchCancellation(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	interval := 50 * time.Millisecond
	RegisterHeartbeatHealthServer(s, interval)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()

	// Wait for server to start accepting connections
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(bufDialer))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)

	// Test Watch method with cancellation
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Receive one response
	_, err = stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive health check response: %v", err)
	}

	// Cancel the context
	cancel()

	// Next receive should fail due to context cancellation
	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error after context cancellation, but got none")
	}
}

func TestRegisterHeartbeatHealthServer(t *testing.T) {
	s := grpc.NewServer()
	interval := 1 * time.Second

	// This should not panic
	RegisterHeartbeatHealthServer(s, interval)

	// Verify the health server is registered by checking service info
	info := s.GetServiceInfo()
	if _, exists := info["grpc.health.v1.Health"]; !exists {
		t.Error("Health service was not registered")
	}
}
