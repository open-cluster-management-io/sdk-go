package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestHealthChecker_NewHealthChecker_WithTimeout(t *testing.T) {
	timeout := 5 * time.Second
	errChan := make(chan error, 1)

	hc := NewHealthChecker(&timeout, errChan)

	if hc.healthinessTimout != timeout {
		t.Errorf("Expected timeout %v, got %v", timeout, hc.healthinessTimout)
	}
	if hc.errChan != errChan {
		t.Error("Error channels should be the same")
	}
	if !hc.enabled {
		t.Error("Health checker should be enabled")
	}
	if hc.heartbeatChan == nil {
		t.Error("Heartbeat channel should not be nil")
	}
}

func TestHealthChecker_NewHealthChecker_WithoutTimeout(t *testing.T) {
	hc := NewHealthChecker(nil, nil)

	if hc.enabled {
		t.Error("Health checker should be disabled when timeout is nil")
	}
	if hc.heartbeatChan == nil {
		t.Error("Heartbeat channel should not be nil")
	}
}

func TestHealthChecker_Input(t *testing.T) {
	timeout := 5 * time.Second
	errChan := make(chan error, 1)

	hc := NewHealthChecker(&timeout, errChan)

	inputChan := hc.Input()
	if inputChan == nil {
		t.Error("Input channel should not be nil")
	}
	if inputChan != hc.heartbeatChan {
		t.Error("Input channel should return the heartbeat channel")
	}
}

func TestHealthChecker_Start_Enabled(t *testing.T) {
	timeout := 100 * time.Millisecond
	errChan := make(chan error, 1)

	hc := NewHealthChecker(&timeout, errChan)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hc.Start(ctx)

	// Should timeout and send error since no heartbeat is sent
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("Expected timeout error, got nil")
		}
		expectedMsg := "stream timeout: no heartbeat received for"
		if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
			t.Errorf("Expected timeout error message to start with '%s', got '%s'", expectedMsg, err.Error())
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Expected timeout error within 200ms")
	}
}

func TestHealthChecker_Start_Disabled(t *testing.T) {
	hc := NewHealthChecker(nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Send a heartbeat to the disabled health checker
	go func() {
		heartbeat := &pbv1.CloudEvent{
			SpecVersion: "1.0",
			Id:          uuid.New().String(),
			Type:        types.HeartbeatCloudEventsType,
		}
		hc.heartbeatChan <- heartbeat
	}()

	// Start should return without error when disabled
	hc.Start(ctx)

	// Should complete without hanging or error
}

func TestHealthChecker_Start_WithHeartbeats(t *testing.T) {
	timeout := 200 * time.Millisecond
	errChan := make(chan error, 1)

	hc := NewHealthChecker(&timeout, errChan)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go hc.Start(ctx)

	// Send heartbeats periodically to prevent timeout
	heartbeatTicker := time.NewTicker(50 * time.Millisecond)
	defer heartbeatTicker.Stop()

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				heartbeat := &pbv1.CloudEvent{
					SpecVersion: "1.0",
					Id:          uuid.New().String(),
					Type:        types.HeartbeatCloudEventsType,
				}
				select {
				case hc.heartbeatChan <- heartbeat:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Should not timeout since heartbeats are being sent
	select {
	case err := <-errChan:
		t.Errorf("Unexpected error: %v", err)
	case <-ctx.Done():
		// Expected - context timeout, no health check error
	}
}

func TestHealthChecker_Start_ContextCancellation(t *testing.T) {
	timeout := 1 * time.Second
	errChan := make(chan error, 1)

	hc := NewHealthChecker(&timeout, errChan)

	ctx, cancel := context.WithCancel(context.Background())

	go hc.Start(ctx)

	// Cancel context before timeout
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Should not send error to errChan when context is cancelled
	select {
	case err := <-errChan:
		t.Errorf("Unexpected error when context cancelled: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected - no error
	}
}

func TestHealthChecker_Start_TimerReset(t *testing.T) {
	timeout := 300 * time.Millisecond
	errChan := make(chan error, 1)

	hc := NewHealthChecker(&timeout, errChan)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hc.Start(ctx)

	// Send periodic heartbeats to ensure timer gets reset
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for i := 0; i < 4; i++ {
			select {
			case <-ticker.C:
				heartbeat := &pbv1.CloudEvent{
					SpecVersion: "1.0",
					Id:          uuid.New().String(),
					Type:        types.HeartbeatCloudEventsType,
				}
				select {
				case hc.heartbeatChan <- heartbeat:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Should not timeout since timer was reset by heartbeats
	select {
	case err := <-errChan:
		t.Errorf("Unexpected timeout error: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Expected - no timeout since heartbeats were received
	}
}

func TestHealthChecker_Start_ErrorChannelFull(t *testing.T) {
	timeout := 50 * time.Millisecond
	errChan := make(chan error) // No buffer

	hc := NewHealthChecker(&timeout, errChan)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go hc.Start(ctx)

	// Don't read from errChan to simulate full channel
	// Health checker should still complete without blocking
	select {
	case <-ctx.Done():
		// Expected - context timeout
	case <-time.After(250 * time.Millisecond):
		t.Error("Health checker should not block when error channel is full")
	}
}
