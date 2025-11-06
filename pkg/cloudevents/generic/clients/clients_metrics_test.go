package clients

import (
	"context"
	"fmt"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/stretchr/testify/require"

	kubetypes "k8s.io/apimachinery/pkg/types"

	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/metrics"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/fake"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/payload"
	generictesting "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/testing"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type testResyncType string

const (
	testSpecResync   testResyncType = "spec"
	testStatusResync testResyncType = "status"
)

func TestCloudEventsMetrics(t *testing.T) {
	cases := []struct {
		name        string
		clusterName string
		sourceID    string
		resources   []*generictesting.MockResource
		dataType    types.CloudEventsDataType
		subresource types.EventSubResource
		action      types.EventAction
	}{
		{
			name:        "receive single resource",
			clusterName: "cluster1",
			sourceID:    "source1",
			resources: []*generictesting.MockResource{
				{Namespace: "cluster1", UID: kubetypes.UID("test1"), ResourceVersion: "2", Status: "test1"},
			},
			dataType:    generictesting.MockEventDataType,
			subresource: types.SubResourceSpec,
			action:      types.EventAction("test_create_request"),
		},
		{
			name:        "receive multiple resources",
			clusterName: "cluster1",
			sourceID:    "source1",
			resources: []*generictesting.MockResource{
				{Namespace: "cluster1", UID: kubetypes.UID("test1"), ResourceVersion: "2", Status: "test1"},
				{Namespace: "cluster1", UID: kubetypes.UID("test2"), ResourceVersion: "3", Status: "test2"},
			},
			dataType:    generictesting.MockEventDataType,
			subresource: types.SubResourceSpec,
			action:      types.EventAction("test_create_request"),
		},
	}
	for _, c := range cases {
		// reset metrics
		metrics.ResetSourceCloudEventsMetrics()
		metrics.ResetClientCloudEventsMetrics()
		// run test
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			evtChan := fake.NewEventChan()

			// initialize source client
			sourceOptions := fake.NewSourceOptions(evtChan, c.sourceID)
			lister := generictesting.NewMockResourceLister([]*generictesting.MockResource{}...)
			source, err := NewCloudEventSourceClient(
				ctx,
				sourceOptions,
				lister,
				generictesting.StatusHash,
				generictesting.NewMockResourceCodec(),
			)
			require.NoError(t, err)

			// initialize agent client
			agentOptions := fake.NewAgentOptions(evtChan, c.clusterName, testAgentName)
			agentLister := generictesting.NewMockResourceLister([]*generictesting.MockResource{}...)
			agent, err := NewCloudEventAgentClient(
				ctx,
				agentOptions,
				agentLister,
				generictesting.StatusHash,
				generictesting.NewMockResourceCodec(),
			)
			require.NoError(t, err)

			// start agent subscription
			agent.Subscribe(ctx, func(_ context.Context, action types.ResourceAction, obj *generictesting.MockResource) error {
				// do nothing
				return nil
			})

			eventType := types.CloudEventsType{
				CloudEventsDataType: c.dataType,
				SubResource:         c.subresource,
				Action:              c.action,
			}

			// publish resources to agent
			for _, resource := range c.resources {
				err = source.Publish(ctx, eventType, resource)
				require.NoError(t, err)
			}

			// wait 1 second for agent receive the resources
			time.Sleep(time.Second)

			// ensure metrics are updated
			sentTotal := metrics.CloudeventsSentFromSourceCounterMetric.WithLabelValues(
				c.sourceID, c.clusterName, c.dataType.String(), string(c.subresource), string(c.action))
			require.Equal(t, len(c.resources), int(toFloat64Counter(sentTotal)))
			receivedTotal := metrics.CloudeventsReceivedByClientCounterMetric.WithLabelValues(
				c.sourceID, c.dataType.String(), string(c.subresource), string(c.action))
			require.Equal(t, len(c.resources), int(toFloat64Counter(receivedTotal)))
		})
	}
}

func TestReconnectMetrics(t *testing.T) {
	// reset metrics
	metrics.ResetSourceCloudEventsMetrics()
	ctx, cancel := context.WithCancel(context.Background())

	originalDelayFn := DelayFn
	// override DelayFn to avoid waiting for backoff
	DelayFn = func() time.Duration { return 0 }
	defer func() {
		// reset DelayFn
		DelayFn = originalDelayFn
	}()

	evtChan := fake.NewEventChan()

	agentOptions := fake.NewAgentOptions(evtChan, "cluster1", testAgentName)
	agentLister := generictesting.NewMockResourceLister([]*generictesting.MockResource{}...)
	_, err := NewCloudEventAgentClient(
		ctx,
		agentOptions,
		agentLister,
		generictesting.StatusHash,
		generictesting.NewMockResourceCodec(),
	)
	require.NoError(t, err)

	// mimic agent disconnection by sending an error
	evtChan.ErrChan <- fmt.Errorf("test error")
	// sleep second to wait for the agent to reconnect
	time.Sleep(time.Second)

	reconnectTotal := metrics.ClientReconnectedCounterMetric.WithLabelValues(testAgentName)
	require.Equal(t, 1.0, toFloat64Counter(reconnectTotal))

	cancel()
}

// toFloat64Counter returns the count of a counter metric
func toFloat64Counter(c prometheus.Counter) float64 {
	var (
		m      prometheus.Metric
		mCount int
		mChan  = make(chan prometheus.Metric)
		done   = make(chan struct{})
	)

	go func() {
		for m = range mChan {
			mCount++
		}
		close(done)
	}()

	c.Collect(mChan)
	close(mChan)
	<-done

	if mCount != 1 {
		panic(fmt.Errorf("collected %d metrics instead of exactly 1", mCount))
	}

	pb := &dto.Metric{}
	if err := m.Write(pb); err != nil {
		panic(fmt.Errorf("metric write failed, err=%v", err))
	}

	if pb.Counter != nil {
		return pb.Counter.GetValue()
	}
	panic(fmt.Errorf("collected a non-counter metric: %s", pb))
}

func TestResyncSpecMetrics(t *testing.T) {
	cases := []struct {
		name        string
		resyncType  testResyncType
		clusterName string
		sourceID    string
		resources   []*generictesting.MockResource
		dataType    types.CloudEventsDataType
	}{
		{
			name:        "resync spec",
			resyncType:  testSpecResync,
			clusterName: "cluster1",
			sourceID:    "source1",
			resources: []*generictesting.MockResource{
				{Namespace: "cluster1", UID: kubetypes.UID("test1"), ResourceVersion: "2", Status: "test1"},
				{Namespace: "cluster1", UID: kubetypes.UID("test2"), ResourceVersion: "3", Status: "test2"},
			},
			dataType: generictesting.MockEventDataType,
		},
	}

	for _, c := range cases {
		// reset metrics
		metrics.ResetSourceCloudEventsMetrics()
		// run test
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			if c.resyncType == testSpecResync {
				sourceOptions := fake.NewSourceOptions(fake.NewEventChan(), c.sourceID)
				lister := generictesting.NewMockResourceLister(c.resources...)
				source, err := NewCloudEventSourceClient(
					ctx,
					sourceOptions,
					lister,
					generictesting.StatusHash,
					generictesting.NewMockResourceCodec(),
				)
				require.NoError(t, err)

				eventType := types.CloudEventsType{
					CloudEventsDataType: c.dataType,
					SubResource:         types.SubResourceSpec,
					Action:              types.ResyncRequestAction,
				}
				evt := cloudevents.NewEvent()
				evt.SetSource(c.clusterName)
				evt.SetType(eventType.String())
				evt.SetExtension("clustername", c.clusterName)
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ResourceVersionList{}); err != nil {
					t.Errorf("failed to set data for event: %v", err)
				}

				// receive resync request and publish associated resources
				source.receive(ctx, evt)

				receivedTotal := metrics.CloudeventsReceivedBySourceCounterMetric.WithLabelValues(
					c.clusterName, c.clusterName, c.dataType.String(),
					string(types.SubResourceSpec), string(types.ResyncRequestAction))
				require.Equal(t, 1, int(toFloat64Counter(receivedTotal)))

				// wait 1 seconds to respond to the spec resync request
				time.Sleep(1 * time.Second)

				// check spec resync duration metric as a histogram
				h := metrics.ResourceSpecResyncDurationMetric.WithLabelValues(
					c.sourceID, c.clusterName, c.dataType.String())
				count, sum := toFloat64HistCountAndSum(h)
				require.Equal(t, uint64(1), count)
				require.Greater(t, sum, 0.0)
				require.Less(t, sum, 1.0)

				sentTotal := metrics.CloudeventsSentFromSourceCounterMetric.WithLabelValues(
					c.sourceID, c.clusterName, c.dataType.String(),
					string(types.SubResourceSpec), string(types.ResyncResponseAction))
				require.Equal(t, len(c.resources), int(toFloat64Counter(sentTotal)))
			}

			cancel()
		})
	}
}

func TestResyncStatusMetrics(t *testing.T) {
	cases := []struct {
		name        string
		resyncType  testResyncType
		clusterName string
		sourceID    string
		resources   []*generictesting.MockResource
		dataType    types.CloudEventsDataType
	}{
		{
			name:        "resync status",
			resyncType:  testStatusResync,
			clusterName: "cluster1",
			sourceID:    "source1",
			resources: []*generictesting.MockResource{
				{Namespace: "cluster1", UID: kubetypes.UID("test1"), ResourceVersion: "2", Status: "test1"},
			},
			dataType: generictesting.MockEventDataType,
		},
	}

	for _, c := range cases {
		// reset metrics
		metrics.ResetClientCloudEventsMetrics()
		// run test
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			if c.resyncType == testStatusResync {
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

				eventType := types.CloudEventsType{
					CloudEventsDataType: c.dataType,
					SubResource:         types.SubResourceStatus,
					Action:              types.ResyncRequestAction,
				}
				evt := cloudevents.NewEvent()
				evt.SetSource(c.sourceID)
				evt.SetType(eventType.String())
				evt.SetExtension("clustername", c.clusterName)
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ResourceStatusHashList{}); err != nil {
					t.Errorf("failed to set data for event: %v", err)
				}

				// receive resync request and publish associated resources
				agent.(*CloudEventAgentClient[*generictesting.MockResource]).receive(ctx, evt)

				receivedTotal := metrics.CloudeventsReceivedByClientCounterMetric.WithLabelValues(
					c.sourceID, c.dataType.String(), string(types.SubResourceStatus),
					string(types.ResyncRequestAction))
				require.Equal(t, 1, int(toFloat64Counter(receivedTotal)))

				// wait 1 seconds to respond to the resync request
				time.Sleep(1 * time.Second)

				// check status resync duration metric as a histogram
				h := metrics.ResourceStatusResyncDurationMetric.WithLabelValues(
					c.sourceID, c.clusterName, c.dataType.String())
				count, sum := toFloat64HistCountAndSum(h)
				require.Equal(t, uint64(1), count)
				require.Greater(t, sum, 0.0)
				require.Less(t, sum, 1.0)

				sentTotal := metrics.CloudeventsSentFromClientCounterMetric.WithLabelValues(
					testAgentName, metrics.NoneOriginalSource, c.dataType.String(),
					string(types.SubResourceStatus), string(types.ResyncResponseAction))
				require.Equal(t, len(c.resources), int(toFloat64Counter(sentTotal)))
			}

			cancel()
		})
	}
}

// toFloat64HistCountAndSum returns the count and sum of a histogram metric
func toFloat64HistCountAndSum(h prometheus.Observer) (uint64, float64) {
	var (
		m      prometheus.Metric
		mCount int
		mChan  = make(chan prometheus.Metric)
		done   = make(chan struct{})
	)

	go func() {
		for m = range mChan {
			mCount++
		}
		close(done)
	}()

	c, ok := h.(prometheus.Collector)
	if !ok {
		panic(fmt.Errorf("observer is not a collector; got: %T", h))
	}

	c.Collect(mChan)
	close(mChan)
	<-done

	if mCount != 1 {
		panic(fmt.Errorf("collected %d metrics instead of exactly 1", mCount))
	}

	pb := &dto.Metric{}
	if err := m.Write(pb); err != nil {
		panic(fmt.Errorf("metric write failed, err=%v", err))
	}

	if pb.Histogram != nil {
		return pb.Histogram.GetSampleCount(), pb.Histogram.GetSampleSum()
	}
	panic(fmt.Errorf("collected a non-histogram metric: %s", pb))
}
