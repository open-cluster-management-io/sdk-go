package generic

import (
	"context"
	"fmt"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol/gochan"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/fake"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/payload"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
)

type testResyncType string

const (
	testSpecResync   testResyncType = "spec"
	testStatusResync testResyncType = "status"
)

func TestResyncMetrics(t *testing.T) {
	cases := []struct {
		name        string
		rescType    testResyncType
		clusterName string
		sourceID    string
		dataType    types.CloudEventsDataType
	}{
		{
			name:        "resync spec",
			rescType:    testSpecResync,
			clusterName: "cluster1",
			sourceID:    "source1",
			dataType:    mockEventDataType,
		},
		{
			name:        "resync status",
			rescType:    testStatusResync,
			clusterName: "cluster1",
			sourceID:    "source1",
			dataType:    mockEventDataType,
		},
	}

	// register metrics
	RegisterResourceResyncMetrics()
	// unregister metrics
	defer UnregisterResourceResyncMetrics()
	for _, c := range cases {
		// reset metrics
		ResetResourceResyncMetricsCollectors()
		// run test
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			if c.rescType == testSpecResync {
				sourceOptions := fake.NewSourceOptions(gochan.New(), c.sourceID)
				lister := newMockResourceLister([]*mockResource{}...)
				source, err := NewCloudEventSourceClient[*mockResource](ctx, sourceOptions, lister, statusHash, newMockResourceCodec())
				require.NoError(t, err)

				eventType := types.CloudEventsType{
					CloudEventsDataType: c.dataType,
					SubResource:         types.SubResourceSpec,
					Action:              types.ResyncRequestAction,
				}
				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				evt.SetExtension("clustername", c.clusterName)
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ResourceStatusHashList{}); err != nil {
					t.Errorf("failed to set data for event: %v", err)
				}

				// receive resync request and publish associated resources
				source.receive(ctx, evt)
				// wait 1 seconds to respond to the resync request
				time.Sleep(2 * time.Second)

				// check spec resync duration metric as a histogram
				h := resourceSpecResyncDurationMetric.WithLabelValues(c.sourceID, c.clusterName, mockEventDataType.String())
				count, sum := toFloat64HistCountAndSum(h)
				require.Equal(t, uint64(1), count)
				require.Greater(t, sum, 0.0)
				require.Less(t, sum, 1.0)
			}

			if c.rescType == testStatusResync {
				agentOptions := fake.NewAgentOptions(gochan.New(), c.clusterName, testAgentName)
				lister := newMockResourceLister([]*mockResource{}...)
				agent, err := NewCloudEventAgentClient[*mockResource](ctx, agentOptions, lister, statusHash, newMockResourceCodec())
				require.NoError(t, err)

				eventType := types.CloudEventsType{
					CloudEventsDataType: c.dataType,
					SubResource:         types.SubResourceStatus,
					Action:              types.ResyncRequestAction,
				}
				evt := cloudevents.NewEvent()
				evt.SetType(eventType.String())
				evt.SetSource(c.sourceID)
				if err := evt.SetData(cloudevents.ApplicationJSON, &payload.ResourceStatusHashList{}); err != nil {
					t.Errorf("failed to set data for event: %v", err)
				}

				// receive resync request and publish associated resources
				agent.receive(ctx, evt)
				// wait 1 seconds to respond to the resync request
				time.Sleep(1 * time.Second)

				// check status resync duration metric as a histogram
				h := resourceStatusResyncDurationMetric.WithLabelValues(c.sourceID, c.clusterName, mockEventDataType.String())
				count, sum := toFloat64HistCountAndSum(h)
				require.Equal(t, uint64(1), count)
				require.Greater(t, sum, 0.0)
				require.Less(t, sum, 1.0)
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
