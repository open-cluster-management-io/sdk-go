package protocol

import (
	"context"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestWithSubscribeOption(t *testing.T) {
	lis := bufconn.Listen(1024)
	defer lis.Close()

	s := grpc.NewServer()
	defer s.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Wait for connection to be ready
	time.Sleep(50 * time.Millisecond)

	// Test valid subscribe option
	subscribeOpt := &SubscribeOption{
		Source:      "test-source",
		ClusterName: "test-cluster",
		DataType:    "test-type",
	}

	p, err := NewProtocol(conn, WithSubscribeOption(subscribeOpt))
	if err != nil {
		t.Fatalf("Failed to create protocol: %v", err)
	}

	if p.subscribeOption != subscribeOpt {
		t.Error("Subscribe option was not set correctly")
	}

	if p.subscribeOption.Source != "test-source" {
		t.Errorf("Expected source 'test-source', got '%s'", p.subscribeOption.Source)
	}

	// Test nil subscribe option
	_, err = NewProtocol(conn, WithSubscribeOption(nil))
	if err == nil {
		t.Error("Expected error for nil subscribe option")
	}
}

func TestWithHealthCheck(t *testing.T) {
	lis := bufconn.Listen(1024)
	defer lis.Close()

	s := grpc.NewServer()
	defer s.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Wait for connection to be ready
	time.Sleep(50 * time.Millisecond)

	// Test WithHealthCheck
	errorChan := make(chan error, 1)
	interval := 3 * time.Second

	p, err := NewProtocol(conn, WithHealthCheck(interval, errorChan))
	if err != nil {
		t.Fatalf("Failed to create protocol: %v", err)
	}

	if p.reconnectErrorChan != errorChan {
		t.Error("Health check error channel was not set correctly")
	}

	if p.checkInterval != interval {
		t.Errorf("Expected health check interval %v, got %v", interval, p.checkInterval)
	}

	// Test nil error channel for health check
	_, err = NewProtocol(conn, WithHealthCheck(interval, nil))
	if err == nil {
		t.Error("Expected error for nil health check error channel")
	}
}

func TestMultipleOptions(t *testing.T) {
	lis := bufconn.Listen(1024)
	defer lis.Close()

	s := grpc.NewServer()
	defer s.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Wait for connection to be ready
	time.Sleep(50 * time.Millisecond)

	// Test applying multiple options
	subscribeOpt := &SubscribeOption{
		Source:   "test-source",
		DataType: "test-type",
	}
	errorChan := make(chan error, 1)
	interval := 2 * time.Second

	p, err := NewProtocol(conn,
		WithSubscribeOption(subscribeOpt),
		WithHealthCheck(interval, errorChan),
	)
	if err != nil {
		t.Fatalf("Failed to create protocol: %v", err)
	}

	// Verify both options were applied
	if p.subscribeOption != subscribeOpt {
		t.Error("Subscribe option was not set correctly")
	}

	if p.reconnectErrorChan != errorChan {
		t.Error("Health check error channel was not set correctly")
	}

	if p.checkInterval != interval {
		t.Errorf("Expected health check interval %v, got %v", interval, p.checkInterval)
	}
}
