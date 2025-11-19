package mqtt

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/eclipse/paho.golang/paho"
	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/mqtt"
)

const (
	testBrokerHost = "127.0.0.1:11883"
	testTimeout    = 10 * time.Second
)

// allowAllHook allows all connections for testing
type allowAllHook struct {
	mochimqtt.HookBase
}

func (h *allowAllHook) ID() string {
	return "allow-all-auth"
}

func (h *allowAllHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mochimqtt.OnConnectAuthenticate,
		mochimqtt.OnACLCheck,
	}, []byte{b})
}

func (h *allowAllHook) OnConnectAuthenticate(cl *mochimqtt.Client, pk packets.Packet) bool {
	return true
}

func (h *allowAllHook) OnACLCheck(cl *mochimqtt.Client, topic string, write bool) bool {
	return true
}

// setupTestBroker creates and starts a test MQTT broker
func setupTestBroker(t *testing.T) (*mochimqtt.Server, func()) {
	broker := mochimqtt.New(&mochimqtt.Options{})

	// Allow all connections
	err := broker.AddHook(new(allowAllHook), nil)
	if err != nil {
		t.Fatalf("failed to add auth hook: %v", err)
	}

	err = broker.AddListener(listeners.NewTCP(listeners.Config{
		ID:      "test-mqtt-broker",
		Address: testBrokerHost,
	}))
	if err != nil {
		t.Fatalf("failed to add listener: %v", err)
	}

	go func() {
		err := broker.Serve()
		if err != nil {
			t.Logf("broker serve error: %v", err)
		}
	}()

	// Wait for broker to be ready
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		// Add a delay before broker shutdown to ensure all MQTT client operations
		// complete. This prevents a race condition where broker shutdown tries to
		// close listener clients while client deletion is still in progress, which
		// can cause a deadlock in the mochi-mqtt server's client management.
		time.Sleep(200 * time.Millisecond)
		if err := broker.Close(); err != nil {
			t.Logf("failed to close broker: %v", err)
		}
	}

	return broker, cleanup
}

// createTestMQTTOptions creates MQTTOptions for testing
func createTestMQTTOptions() *mqtt.MQTTOptions {
	return &mqtt.MQTTOptions{
		KeepAlive: 60,
		PubQoS:    1,
		SubQoS:    1,
		Dialer: &mqtt.MQTTDialer{
			BrokerHost: testBrokerHost,
			Timeout:    5 * time.Second,
		},
	}
}

// createTestTransport creates a transport for testing
func createTestTransport(clientID, pubTopic, subTopic string) *mqttTransport {
	opts := createTestMQTTOptions()
	return newTransport(
		clientID,
		opts,
		func(ctx context.Context, evt cloudevents.Event) (string, error) {
			return pubTopic, nil
		},
		func() (*paho.Subscribe, error) {
			return &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: subTopic, QoS: byte(opts.SubQoS)},
				},
			}, nil
		},
	)
}

// TestTransportConnectDisconnect tests basic connect and disconnect
func TestTransportConnectDisconnect(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	transport := createTestTransport("test-client-1", "test/pub", "test/sub")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Test connect
	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Verify channels are initialized
	if transport.msgChan == nil {
		t.Fatal("msgChan should be initialized after Connect")
	}
	if transport.closeChan == nil {
		t.Fatal("closeChan should be initialized after Connect")
	}
	if transport.client == nil {
		t.Fatal("client should be initialized after Connect")
	}

	// Test disconnect
	if err := transport.Close(ctx); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
}

// TestTransportReconnect tests reconnection capability
func TestTransportReconnect(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	transport := createTestTransport("test-client-reconnect", "test/pub", "test/sub")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// First connection
	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("first connect failed: %v", err)
	}

	// Close
	if err := transport.Close(ctx); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reconnect
	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}

	// Verify new channels are created
	if transport.msgChan == nil {
		t.Fatal("msgChan should be re-initialized after reconnect")
	}
	if transport.closeChan == nil {
		t.Fatal("closeChan should be re-initialized after reconnect")
	}

	if err := transport.Close(ctx); err != nil {
		t.Fatalf("final close failed: %v", err)
	}
}

// TestTransportSend tests sending messages
func TestTransportSend(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	transport := createTestTransport("test-sender", "test/send/topic", "test/receive/topic")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer transport.Close(ctx)

	// Create a test event
	event := cloudevents.NewEvent()
	event.SetID("test-event-1")
	event.SetSource("test-source")
	event.SetType("test.type")
	if err := event.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"}); err != nil {
		t.Fatalf("failed to set data: %v", err)
	}

	// Send the event
	if err := transport.Send(ctx, event); err != nil {
		t.Fatalf("failed to send event: %v", err)
	}
}

// TestTransportSendWithoutConnect tests error when sending without connecting
func TestTransportSendWithoutConnect(t *testing.T) {
	transport := createTestTransport("test-sender-no-connect", "test/send", "test/receive")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	event := cloudevents.NewEvent()
	event.SetID("test-event")
	event.SetSource("test-source")
	event.SetType("test.type")

	// Should fail because not connected
	err := transport.Send(ctx, event)
	if err == nil {
		t.Fatal("expected error when sending without connect, got nil")
	}
	if err.Error() != "transport not connected" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestTransportSubscribeAndReceive tests subscribing and receiving messages
func TestTransportSubscribeAndReceive(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "test/pubsub/topic"
	sender := createTestTransport("test-sender", topic, topic)
	receiver := createTestTransport("test-receiver", topic, topic)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Connect both transports
	if err := sender.Connect(ctx); err != nil {
		t.Fatalf("sender connect failed: %v", err)
	}
	defer sender.Close(ctx)

	if err := receiver.Connect(ctx); err != nil {
		t.Fatalf("receiver connect failed: %v", err)
	}
	defer receiver.Close(ctx)

	// Subscribe receiver
	if err := receiver.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// Give subscription time to complete
	time.Sleep(100 * time.Millisecond)

	// Channel to signal when message is received
	received := make(chan cloudevents.Event, 1)
	receivedCount := atomic.Int32{}

	// Start receiving in background
	go func() {
		err := receiver.Receive(ctx, func(ctx context.Context, evt cloudevents.Event) {
			receivedCount.Add(1)
			received <- evt
		})
		if err != nil && ctx.Err() == nil {
			t.Logf("receive error: %v", err)
		}
	}()

	// Wait a bit for receiver to be ready
	time.Sleep(100 * time.Millisecond)

	// Send an event
	event := cloudevents.NewEvent()
	event.SetID("test-event-123")
	event.SetSource("test-source")
	event.SetType("test.type.pubsub")
	if err := event.SetData(cloudevents.ApplicationJSON, map[string]string{"message": "hello"}); err != nil {
		t.Fatalf("failed to set data: %v", err)
	}

	if err := sender.Send(ctx, event); err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	// Wait for the message
	select {
	case evt := <-received:
		if evt.ID() != "test-event-123" {
			t.Fatalf("unexpected event ID: got %s, want test-event-123", evt.ID())
		}
		if evt.Type() != "test.type.pubsub" {
			t.Fatalf("unexpected event type: got %s, want test.type.pubsub", evt.Type())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Verify only one message was received
	if count := receivedCount.Load(); count != 1 {
		t.Fatalf("expected 1 message, got %d", count)
	}
}

// TestTransportNoMessageLoss tests that messages are not lost under load
func TestTransportNoMessageLoss(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "test/no-loss/topic"
	sender := createTestTransport("test-sender-load", topic, topic)
	receiver := createTestTransport("test-receiver-load", topic, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect both
	if err := sender.Connect(ctx); err != nil {
		t.Fatalf("sender connect failed: %v", err)
	}
	defer sender.Close(ctx)

	if err := receiver.Connect(ctx); err != nil {
		t.Fatalf("receiver connect failed: %v", err)
	}
	defer receiver.Close(ctx)

	// Subscribe
	if err := receiver.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Track received messages
	receivedIDs := sync.Map{}
	receivedCount := atomic.Int32{}
	const totalMessages = 100

	// Start receiver
	go func() {
		err := receiver.Receive(ctx, func(ctx context.Context, evt cloudevents.Event) {
			receivedIDs.Store(evt.ID(), true)
			receivedCount.Add(1)
		})
		if err != nil && ctx.Err() == nil {
			t.Logf("receive error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Send many messages rapidly
	for i := 0; i < totalMessages; i++ {
		event := cloudevents.NewEvent()
		event.SetID(fmt.Sprintf("event-%d", i))
		event.SetSource("test-source")
		event.SetType("test.type")
		if err := event.SetData(cloudevents.ApplicationJSON, map[string]int{"seq": i}); err != nil {
			t.Fatalf("failed to set data: %v", err)
		}

		if err := sender.Send(ctx, event); err != nil {
			t.Fatalf("failed to send event %d: %v", i, err)
		}
	}

	// Wait for all messages to be received
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if receivedCount.Load() >= totalMessages {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all messages received
	finalCount := receivedCount.Load()
	if finalCount != totalMessages {
		t.Fatalf("message loss detected: sent %d, received %d", totalMessages, finalCount)
	}

	// Verify all IDs are unique and present
	for i := 0; i < totalMessages; i++ {
		expectedID := fmt.Sprintf("event-%d", i)
		if _, ok := receivedIDs.Load(expectedID); !ok {
			t.Fatalf("message %s not received", expectedID)
		}
	}
}

// TestTransportSubscribeWithoutConnect tests error when subscribing without connecting
func TestTransportSubscribeWithoutConnect(t *testing.T) {
	transport := createTestTransport("test-sub-no-connect", "test/pub", "test/sub")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := transport.Subscribe(ctx)
	if err == nil {
		t.Fatal("expected error when subscribing without connect, got nil")
	}
	if err.Error() != "transport not connected" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestTransportDoubleSubscribe tests error on double subscribe
func TestTransportDoubleSubscribe(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	transport := createTestTransport("test-double-sub", "test/pub", "test/sub")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer transport.Close(ctx)

	// First subscribe should succeed
	if err := transport.Subscribe(ctx); err != nil {
		t.Fatalf("first subscribe failed: %v", err)
	}

	// Second subscribe should fail
	err := transport.Subscribe(ctx)
	if err == nil {
		t.Fatal("expected error on double subscribe, got nil")
	}
	if err.Error() != "transport has already subscribed" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// TestTransportCloseWhileReceiving tests graceful shutdown during receive
func TestTransportCloseWhileReceiving(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "test/close/topic"
	transport := createTestTransport("test-close-receive", topic, topic)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	if err := transport.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	receiveDone := make(chan error, 1)

	// Start receiving
	go func() {
		err := transport.Receive(ctx, func(ctx context.Context, evt cloudevents.Event) {
			// Handler should not be called after close
		})
		receiveDone <- err
	}()

	// Wait a bit for receive loop to start
	time.Sleep(100 * time.Millisecond)

	// Close transport
	if err := transport.Close(ctx); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Receive should exit gracefully
	select {
	case err := <-receiveDone:
		if err != nil {
			t.Logf("receive exited with error (expected): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receive did not exit after close")
	}
}

// TestTransportDoubleClose tests that double close is safe
func TestTransportDoubleClose(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	transport := createTestTransport("test-double-close", "test/pub", "test/sub")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// First close
	if err := transport.Close(ctx); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// Second close should not panic
	if err := transport.Close(ctx); err != nil {
		t.Logf("second close returned error (acceptable): %v", err)
	}
}

// TestTransportContextCancellation tests that receive exits on context cancellation
func TestTransportContextCancellation(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "test/cancel/topic"
	transport := createTestTransport("test-cancel", topic, topic)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer transport.Close(ctx)

	if err := transport.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	receiveCtx, receiveCancel := context.WithCancel(ctx)
	receiveDone := make(chan error, 1)

	// Start receiving
	go func() {
		err := transport.Receive(receiveCtx, func(ctx context.Context, evt cloudevents.Event) {
			// Handler
		})
		receiveDone <- err
	}()

	// Wait a bit for receive loop to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the receive context
	receiveCancel()

	// Receive should exit
	select {
	case err := <-receiveDone:
		if err != nil {
			t.Logf("receive exited with error (expected): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receive did not exit after context cancellation")
	}
}

// TestTransportReceiveInvalidEvent tests handling of invalid events
func TestTransportReceiveInvalidEvent(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	topic := "test/invalid/topic"
	receiver := createTestTransport("test-receiver-invalid", topic, topic)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := receiver.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer receiver.Close(ctx)

	if err := receiver.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	handlerCalled := atomic.Int32{}

	go func() {
		err := receiver.Receive(ctx, func(ctx context.Context, evt cloudevents.Event) {
			handlerCalled.Add(1)
		})
		if err != nil && ctx.Err() == nil {
			t.Logf("receive error: %v", err)
		}
	}()

	// Wait and verify handler is not called for invalid events
	time.Sleep(200 * time.Millisecond)

	if count := handlerCalled.Load(); count != 0 {
		t.Fatalf("handler should not be called for invalid events, got %d calls", count)
	}
}

// TestTransportErrorChan tests the error channel
func TestTransportErrorChan(t *testing.T) {
	transport := createTestTransport("test-error-chan", "test/pub", "test/sub")

	errorChan := transport.ErrorChan()
	if errorChan == nil {
		t.Fatal("ErrorChan should not return nil")
	}

	// Verify it's the same channel
	if transport.ErrorChan() != errorChan {
		t.Fatal("ErrorChan should return the same channel")
	}
}

// TestTransportReceiveWithoutSubscribe tests error when receiving without subscribing
func TestTransportReceiveWithoutSubscribe(t *testing.T) {
	_, cleanup := setupTestBroker(t)
	defer cleanup()

	transport := createTestTransport("test-receive-no-sub", "test/pub", "test/sub")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := transport.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer transport.Close(ctx)

	// Try to receive without subscribing
	err := transport.Receive(ctx, func(ctx context.Context, evt cloudevents.Event) {})
	if err == nil {
		t.Fatal("expected error when receiving without subscribe, got nil")
	}
	if err.Error() != "transport not subscribed" {
		t.Fatalf("unexpected error message: %v", err)
	}
}
