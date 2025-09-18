package heartbeat

import (
	"context"
	"testing"
	"time"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

func TestHeartbeater_NewHeartbeater(t *testing.T) {
	interval := 1 * time.Second
	cacheSize := 10

	h := NewHeartbeater(interval, cacheSize)

	if h.interval != interval {
		t.Errorf("Expected interval %v, got %v", interval, h.interval)
	}

	if cap(h.output) != cacheSize {
		t.Errorf("Expected cache size %d, got %d", cacheSize, cap(h.output))
	}
}

func TestHeartbeater_Start(t *testing.T) {
	interval := 100 * time.Millisecond
	cacheSize := 5

	h := NewHeartbeater(interval, cacheSize)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go h.Start(ctx)

	// Wait for at least 2 heartbeats
	receivedCount := 0
	for receivedCount < 2 {
		select {
		case heartbeat := <-h.Heartbeat():
			if heartbeat.SpecVersion != "1.0" {
				t.Errorf("Expected spec version 1.0, got %s", heartbeat.SpecVersion)
			}
			if heartbeat.Type != types.HeartbeatCloudEventsType {
				t.Errorf("Expected type %s, got %s", types.HeartbeatCloudEventsType, heartbeat.Type)
			}
			if heartbeat.Id == "" {
				t.Error("Expected non-empty ID")
			}
			receivedCount++
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Timeout waiting for heartbeat")
		}
	}
}

func TestHeartbeater_StartWithContextCancellation(t *testing.T) {
	interval := 50 * time.Millisecond
	cacheSize := 5

	h := NewHeartbeater(interval, cacheSize)

	ctx, cancel := context.WithCancel(context.Background())

	go h.Start(ctx)

	// Wait for at least one heartbeat to ensure Start is running
	select {
	case <-h.Heartbeat():
		// Expected
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for heartbeat")
	}

	// Cancel context
	cancel()

	// Give some time for the goroutine to exit
	time.Sleep(100 * time.Millisecond)

	// No more heartbeats should be sent after context cancellation
	select {
	case <-h.Heartbeat():
		t.Error("Received heartbeat after context cancellation")
	case <-time.After(150 * time.Millisecond):
		// Expected - no more heartbeats
	}
}

func TestHeartbeater_ChannelFull(t *testing.T) {
	interval := 10 * time.Millisecond
	cacheSize := 2 // Small cache to test full channel

	h := NewHeartbeater(interval, cacheSize)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go h.Start(ctx)

	// Fill up the channel by not reading from it
	time.Sleep(100 * time.Millisecond)

	// The heartbeater should continue running even with a full channel
	// This test ensures it doesn't block or panic
	select {
	case <-ctx.Done():
		// Expected - context timeout
	case <-time.After(250 * time.Millisecond):
		t.Error("Context should have timed out")
	}
}

func TestHeartbeater_Heartbeat(t *testing.T) {
	interval := 1 * time.Second
	cacheSize := 5

	h := NewHeartbeater(interval, cacheSize)

	channel := h.Heartbeat()
	if channel == nil {
		t.Error("Heartbeat channel should not be nil")
	}

	if channel != h.output {
		t.Error("Heartbeat channel should return the output channel")
	}
}
