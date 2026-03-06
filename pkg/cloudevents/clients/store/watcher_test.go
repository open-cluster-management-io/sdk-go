package store

import (
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TestWatcherConcurrentStopAndReceive tests that Stop() and Receive() can be called
// concurrently without causing panics or data races.
func TestWatcherConcurrentStopAndReceive(t *testing.T) {
	// Run the test multiple times to increase chance of catching race conditions
	for i := 0; i < 10; i++ {
		w := NewWatcher()

		var wg sync.WaitGroup
		wg.Add(3)

		// Goroutine 1: Send events rapidly
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				w.Receive(watch.Event{
					Type: watch.Added,
					Object: &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-pod",
						},
					},
				})
			}
		}()

		// Goroutine 2: Also send events rapidly
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				w.Receive(watch.Event{
					Type: watch.Modified,
					Object: &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-pod",
						},
					},
				})
			}
		}()

		// Goroutine 3: Stop the watcher after a short delay
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			w.Stop()
		}()

		// Consume events from the result channel
		consumerDone := make(chan struct{})
		go func() {
			defer close(consumerDone)
			for range w.ResultChan() {
				// Drain events
			}
		}()

		wg.Wait()
		select {
		case <-consumerDone:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("result consumer did not exit after Stop()")
		}
	}
}

// TestWatcherStopThenReceive tests that calling Receive() after Stop() is safe
func TestWatcherStopThenReceive(t *testing.T) {
	w := NewWatcher()

	// Stop first
	w.Stop()

	// Then try to receive - should not panic
	w.Receive(watch.Event{
		Type: watch.Added,
		Object: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
			},
		},
	})
}

// TestWatcherMultipleStops tests that calling Stop() multiple times is safe
func TestWatcherMultipleStops(t *testing.T) {
	w := NewWatcher()

	// Call Stop multiple times - should not panic
	w.Stop()
	w.Stop()
	w.Stop()
}

// TestWatcherReceiveAfterStop tests that events sent after Stop() are dropped
// and the result channel is closed
func TestWatcherReceiveAfterStop(t *testing.T) {
	w := NewWatcher()

	// Start a goroutine to consume events until channel closes
	done := make(chan bool)
	eventCount := 0
	go func() {
		for range w.ResultChan() {
			eventCount++
		}
		done <- true
	}()

	// Stop the watcher
	w.Stop()

	// Try to send an event - should be dropped
	w.Receive(watch.Event{
		Type: watch.Added,
		Object: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
			},
		},
	})

	// Wait for the consumer to exit (channel should be closed)
	select {
	case <-done:
		// Expected - channel was closed
		if eventCount > 0 {
			t.Errorf("Expected no events after Stop(), but received %d", eventCount)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected result channel to be closed after Stop(), but it's still open")
	}
}

// TestResultClosedAfterWatcherStop tests w.result should be closed after watcher stop.
func TestResultClosedAfterWatcherStop(t *testing.T) {
	w := NewWatcher()

	// Call Stop() once
	w.Stop()

	// Verify that w.done is closed
	select {
	case <-w.done:
		// Expected - done channel should be closed
	default:
		t.Error("Expected done channel to be closed after Stop()")
	}

	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Error("expected result channel to be closed after Stop()")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected result channel to be closed after Stop()")
	}
}

// TestWatcherRaceCondition tests for the specific race condition where
// Receive() checks isStopped(), then Stop() runs and closes the channel,
// then Receive() tries to send on the closed channel.
func TestWatcherRaceCondition(t *testing.T) {
	// This test tries to trigger the race window between isStopped() check
	// and the channel send in Receive()
	for i := 0; i < 1000; i++ {
		w := NewWatcher()

		done := make(chan bool)

		// Goroutine that will call Receive
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic in Receive: %v", r)
				}
				done <- true
			}()

			w.Receive(watch.Event{
				Type: watch.Added,
				Object: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-pod",
					},
				},
			})
		}()

		// Goroutine that will call Stop
		go func() {
			defer func() {
				done <- true
			}()
			// Call Stop() twice to trigger the channel close
			w.Stop()
			w.Stop()
		}()

		// Wait for both goroutines
		<-done
		<-done
	}
}
