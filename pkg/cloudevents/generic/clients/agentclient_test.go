package clients

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/require"

	kubetypes "k8s.io/apimachinery/pkg/types"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/fake"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/payload"
	generictesting "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/testing"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

const testAgentName = "mock-agent"

type receiveEvent struct {
	event cloudevents.Event
	err   error
}

func TestAgentResync(t *testing.T) {
	cases := []struct {
		name          string
		clusterName   string
		resources     []*generictesting.MockResource
		eventType     types.CloudEventsType
		expectedItems int
	}{
		{
			name:          "no cached resources",
			clusterName:   "cluster1",
			resources:     []*generictesting.MockResource{},
			eventType:     types.CloudEventsType{SubResource: types.SubResourceSpec},
			expectedItems: 0,
		},
		{
			name:        "has cached resources",
			clusterName: "cluster2",
			resources: []*generictesting.MockResource{
				{UID: kubetypes.UID("test1"), ResourceVersion: "2"},
				{UID: kubetypes.UID("test2"), ResourceVersion: "3"},
			},
			eventType:     types.CloudEventsType{SubResource: types.SubResourceSpec},
			expectedItems: 2,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			lister := generictesting.NewMockResourceLister(c.resources...)
			agent, err := NewCloudEventAgentClient(
				ctx,
				fake.NewAgentOptions(fake.NewEventChan(), c.clusterName, testAgentName),
				lister, generictesting.StatusHash,
				generictesting.NewMockResourceCodec(),
			)
			require.NoError(t, err)

			// start a cloudevents receiver client go to receive the event
			eventChan := make(chan receiveEvent)
			stop := make(chan bool)

			go func() {
				transport := agent.(*CloudEventAgentClient[*generictesting.MockResource]).transport
				err = transport.Receive(ctx, func(ctx context.Context, event cloudevents.Event) {
					select {
					case eventChan <- receiveEvent{event: event}:
					case <-ctx.Done():
						return
					}
				})
				if err != nil && err != context.Canceled {
					select {
					case eventChan <- receiveEvent{err: err}:
					case <-ctx.Done():
					}
				}
				stop <- true
			}()

			err = agent.Resync(ctx, types.SourceAll)
			require.NoError(t, err)

			receivedEvent := <-eventChan
			require.NoError(t, receivedEvent.err)
			require.NotNil(t, receivedEvent.event)

			eventOut := receivedEvent.event
			clusterName, err := eventOut.Context.GetExtension("clustername")
			require.NoError(t, err)
			require.Equal(t, c.clusterName, clusterName)

			resourceList, err := payload.DecodeSpecResyncRequest(eventOut)
			require.NoError(t, err)
			require.Equal(t, c.expectedItems, len(resourceList.Versions))

			cancel()
			<-stop
		})
	}
}

func TestAgentPublish(t *testing.T) {
	cases := []struct {
		name        string
		clusterName string
		resources   *generictesting.MockResource
		eventType   types.CloudEventsType
	}{
		{
			name:        "publish status",
			clusterName: "cluster1",
			resources: &generictesting.MockResource{
				UID:             kubetypes.UID("1234"),
				Generation:      2,
				ResourceVersion: "2",
				Status:          "test-status",
				Namespace:       "cluster1",
			},
			eventType: types.CloudEventsType{
				CloudEventsDataType: generictesting.MockEventDataType,
				SubResource:         types.SubResourceStatus,
				Action:              "test_update_request",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			agentOptions := fake.NewAgentOptions(fake.NewEventChan(), c.clusterName, testAgentName)
			lister := generictesting.NewMockResourceLister()
			agent, err := NewCloudEventAgentClient(
				ctx,
				agentOptions,
				lister,
				generictesting.StatusHash,
				generictesting.NewMockResourceCodec(),
			)
			require.Nil(t, err)

			// start a cloudevents receiver client go to receive the event
			eventChan := make(chan receiveEvent)
			stop := make(chan bool)
			go func() {
				cloudEventsClient := agent.(*CloudEventAgentClient[*generictesting.MockResource]).transport
				err = cloudEventsClient.Receive(ctx, func(ctx context.Context, event cloudevents.Event) {
					select {
					case eventChan <- receiveEvent{event: event}:
					case <-ctx.Done():
						return
					}
				})
				if err != nil && err != context.Canceled {
					select {
					case eventChan <- receiveEvent{err: err}:
					case <-ctx.Done():
					}
				}
				stop <- true
			}()

			err = agent.Publish(ctx, c.eventType, c.resources)
			require.Nil(t, err)

			receivedEvent := <-eventChan
			require.NoError(t, receivedEvent.err)
			require.NotNil(t, receivedEvent.event)

			eventOut := receivedEvent.event
			resourceID, err := eventOut.Context.GetExtension("resourceid")
			require.Equal(t, c.resources.UID, kubetypes.UID(fmt.Sprintf("%s", resourceID)))

			resourceVersion, err := eventOut.Context.GetExtension("resourceversion")
			require.NoError(t, err)
			require.Equal(t, c.resources.ResourceVersion, resourceVersion)

			clusterName, err := eventOut.Context.GetExtension("clustername")
			require.NoError(t, err)
			require.Equal(t, c.clusterName, clusterName)

			cancel()
			<-stop
		})
	}
}

func TestStatusResyncResponse(t *testing.T) {
	cases := []struct {
		name         string
		clusterName  string
		requestEvent cloudevents.Event
		resources    []*generictesting.MockResource
		validate     func([]cloudevents.Event)
	}{
		{
			name:        "unsupported event type",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				evt := cloudevents.NewEvent()
				evt.SetType("unsupported")
				return evt
			}(),
			validate: func(pubEvents []cloudevents.Event) {
				if len(pubEvents) != 0 {
					t.Errorf("unexpected publish events %v", pubEvents)
				}
			},
		},
		{
			name:        "unsupported resync event type",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: generictesting.MockEventDataType,
					SubResource:         types.SubResourceSpec,
					Action:              types.ResyncRequestAction,
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				return evt
			}(),
			validate: func(pubEvents []cloudevents.Event) {
				if len(pubEvents) != 0 {
					t.Errorf("unexpected publish events %v", pubEvents)
				}
			},
		},
		{
			name:        "resync all status",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: generictesting.MockEventDataType,
					SubResource:         types.SubResourceStatus,
					Action:              types.ResyncRequestAction,
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ResourceStatusHashList{}); err != nil {
					t.Fatal(err)
				}
				return evt
			}(),
			resources: []*generictesting.MockResource{
				{UID: kubetypes.UID("test1"), ResourceVersion: "2", Status: "test1"},
				{UID: kubetypes.UID("test2"), ResourceVersion: "3", Status: "test2"},
			},
			validate: func(pubEvents []cloudevents.Event) {
				if len(pubEvents) != 2 {
					t.Errorf("expected all publish events, but got %v", pubEvents)
				}
			},
		},
		{
			name:        "resync status",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: generictesting.MockEventDataType,
					SubResource:         types.SubResourceStatus,
					Action:              types.ResyncRequestAction,
				}

				statusHashes := &payload.ResourceStatusHashList{
					Hashes: []payload.ResourceStatusHash{
						{ResourceID: "test1", StatusHash: "test1"},
						{ResourceID: "test2", StatusHash: "test2"},
					},
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				if err := evt.SetData(cloudevents.ApplicationJSON, statusHashes); err != nil {
					t.Fatal(err)
				}
				return evt
			}(),
			resources: []*generictesting.MockResource{
				{UID: kubetypes.UID("test0"), ResourceVersion: "2", Status: "test0"},
				{UID: kubetypes.UID("test1"), ResourceVersion: "2", Status: "test1"},
				{UID: kubetypes.UID("test2"), ResourceVersion: "3", Status: "test2-updated"},
			},
			validate: func(pubEvents []cloudevents.Event) {
				if len(pubEvents) != 1 {
					t.Errorf("expected one publish events, but got %v", pubEvents)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			agentOptions := fake.NewAgentOptions(fake.NewEventChan(), c.clusterName, testAgentName)
			lister := generictesting.NewMockResourceLister(c.resources...)
			agent, err := NewCloudEventAgentClient(
				ctx,
				agentOptions,
				lister,
				generictesting.StatusHash,
				generictesting.NewMockResourceCodec(),
			)
			require.NoError(t, err)

			// start receiver
			receivedEvents := []cloudevents.Event{}
			stop := make(chan bool)
			mutex := &sync.Mutex{}

			go func() {
				cloudEventsClient := agent.(*CloudEventAgentClient[*generictesting.MockResource]).transport
				_ = cloudEventsClient.Receive(ctx, func(ctx context.Context, event cloudevents.Event) {
					mutex.Lock()
					defer mutex.Unlock()
					receivedEvents = append(receivedEvents, event)
				})
				stop <- true
			}()

			// receive resync request and publish associated resources
			agent.(*CloudEventAgentClient[*generictesting.MockResource]).receive(ctx, c.requestEvent)
			// wait 1 seconds to receive the response resources
			time.Sleep(1 * time.Second)

			mutex.Lock()
			c.validate(receivedEvents)
			mutex.Unlock()

			cancel()
			<-stop
		})
	}
}

func TestReceiveResourceSpec(t *testing.T) {
	cases := []struct {
		name         string
		clusterName  string
		requestEvent cloudevents.Event
		resources    []*generictesting.MockResource
		validate     func(resource *generictesting.MockResource)
	}{
		{
			name:        "unsupported sub resource",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: generictesting.MockEventDataType,
					SubResource:         types.SubResourceStatus,
					Action:              "test_create_request",
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				return evt
			}(),
			validate: func(resource *generictesting.MockResource) {
				if resource != nil {
					t.Errorf("should not be invoked")
				}
			},
		},
		{
			name:        "no registered codec for the resource",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					SubResource: types.SubResourceSpec,
					Action:      "test_create_request",
				}

				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				return evt
			}(),
			validate: func(resource *generictesting.MockResource) {
				if resource != nil {
					t.Errorf("should not be invoked")
				}
			},
		},
		{
			name:        "receive a resource",
			clusterName: "cluster1",
			requestEvent: func() cloudevents.Event {
				eventType := types.CloudEventsType{
					CloudEventsDataType: generictesting.MockEventDataType,
					SubResource:         types.SubResourceSpec,
					Action:              "test_create_request",
				}

				evt, _ := generictesting.NewMockResourceCodec().Encode(
					testAgentName,
					eventType,
					&generictesting.MockResource{
						UID:             kubetypes.UID("test1"),
						Generation:      1,
						ResourceVersion: "1",
						Namespace:       "cluster1",
					})
				return *evt
			}(),
			validate: func(resource *generictesting.MockResource) {
				if resource.UID != "test1" {
					t.Errorf("unexpected resource %v", resource)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			agentOptions := fake.NewAgentOptions(fake.NewEventChan(), c.clusterName, testAgentName)
			lister := generictesting.NewMockResourceLister(c.resources...)
			agent, err := NewCloudEventAgentClient(
				context.TODO(),
				agentOptions,
				lister,
				generictesting.StatusHash, generictesting.NewMockResourceCodec(),
			)
			require.NoError(t, err)

			var actualRes *generictesting.MockResource
			agent.(*CloudEventAgentClient[*generictesting.MockResource]).receive(
				context.TODO(),
				c.requestEvent,
				func(_ context.Context, resource *generictesting.MockResource) error {
					actualRes = resource
					return nil
				})

			c.validate(actualRes)
		})
	}
}

func TestAgentClientReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agentOptions := fake.NewAgentOptions(fake.NewEventChan(), "cluster1", testAgentName)
	lister := generictesting.NewMockResourceLister()
	agent, err := NewCloudEventAgentClient(
		ctx,
		agentOptions,
		lister,
		generictesting.StatusHash,
		generictesting.NewMockResourceCodec(),
	)
	require.NoError(t, err)

	// Connected after creation
	require.True(t, agent.IsConnected())
	// Subscribed is initially false before subscribe is called
	require.False(t, agent.IsSubscribed())
	require.False(t, agent.IsReady())

	agent.Subscribe(ctx, func(ctx context.Context, obj *generictesting.MockResource) error {
		return nil
	})

	// Subscribe kicks off subscription asynchronously; IsSubscribed/IsReady become true
	// once the transport's Subscribe call completes.
	require.Eventually(t, func() bool {
		return agent.IsSubscribed()
	}, 5*time.Second, 10*time.Millisecond)
	require.True(t, agent.IsReady())
}

func TestAgentClientReadinessAfterTransportError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Override the package-level reconnect backoff to a short, non-zero delay:
	// long enough to reliably observe the transient disconnected window below,
	// but short enough that the background reconnect goroutine settles (and
	// stops touching DelayFn) well before this test returns - avoiding a
	// leaked goroutine that sleeps on the real ~5s default backoff and races
	// with other tests (e.g. TestReconnectMetrics) that also mutate DelayFn.
	originalDelayFn := DelayFn
	DelayFn = func() time.Duration { return 50 * time.Millisecond }
	defer func() { DelayFn = originalDelayFn }()

	eventChan := fake.NewEventChan()
	agentOptions := fake.NewAgentOptions(eventChan, "cluster1", testAgentName)
	lister := generictesting.NewMockResourceLister()
	agent, err := NewCloudEventAgentClient(
		ctx,
		agentOptions,
		lister,
		generictesting.StatusHash,
		generictesting.NewMockResourceCodec(),
	)
	require.NoError(t, err)

	agent.Subscribe(ctx, func(ctx context.Context, obj *generictesting.MockResource) error {
		return nil
	})
	require.Eventually(t, func() bool {
		return agent.IsReady()
	}, 5*time.Second, 10*time.Millisecond)

	// Simulate a transport connection drop.
	eventChan.ErrChan <- fmt.Errorf("simulated transport error")

	// A dropped connection must clear both connected and subscribed state: the
	// previous subscription is no longer live and a fresh Subscribe is required
	// once reconnected, so IsReady must not report a false positive during this window.
	// Poll tightly since the disconnected window is only ~50ms (the overridden backoff).
	require.Eventually(t, func() bool {
		return !agent.IsConnected() && !agent.IsSubscribed() && !agent.IsReady()
	}, 200*time.Millisecond, time.Millisecond)

	// The background reconnect goroutine reconnects after the overridden 50ms
	// backoff and re-subscribes; wait for IsReady so the client has fully recovered
	// and the goroutine has fully settled (and stopped touching the package-level DelayFn)
	// before this test returns.
	require.Eventually(t, func() bool {
		return agent.IsReady()
	}, 5*time.Second, 10*time.Millisecond)
}
