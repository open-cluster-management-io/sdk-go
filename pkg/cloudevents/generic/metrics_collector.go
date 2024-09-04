package generic

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Subsystem used to define the metrics:
const metricsSubsystem = "resources"

// Names of the labels added to metrics:
const (
	metricsSourceLabel   = "source"
	metricsClusterLabel  = "cluster"
	metrucsDataTypeLabel = "type"
)

// metricsLabels - Array of labels added to metrics:
var metricsLabels = []string{
	metricsSourceLabel,
	metricsClusterLabel,
	metrucsDataTypeLabel,
}

// Names of the metrics:
const (
	specResyncDurationMetric   = "spec_resync_duration_seconds"
	statusResyncDurationMetric = "status_resync_duration_seconds"
)

// Description of the resource spec resync duration metric:
var resourceSpecResyncDurationMetric = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Subsystem: metricsSubsystem,
		Name:      specResyncDurationMetric,
		Help:      "The duration of the resource spec resync in seconds.",
		Buckets: []float64{
			0.1,
			0.2,
			0.5,
			1.0,
			2.0,
			10.0,
			30.0,
		},
	},
	metricsLabels,
)

// Description of the resource status resync duration metric:
var resourceStatusResyncDurationMetric = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Subsystem: metricsSubsystem,
		Name:      statusResyncDurationMetric,
		Help:      "The duration of the resource status resync in seconds.",
		Buckets: []float64{
			0.1,
			0.2,
			0.5,
			1.0,
			2.0,
			10.0,
			30.0,
		},
	},
	metricsLabels,
)

// Register the metrics:
func RegisterResourceResyncMetrics() {
	prometheus.MustRegister(resourceSpecResyncDurationMetric)
	prometheus.MustRegister(resourceStatusResyncDurationMetric)
}

// Unregister the metrics:
func UnregisterResourceResyncMetrics() {
	prometheus.Unregister(resourceStatusResyncDurationMetric)
	prometheus.Unregister(resourceStatusResyncDurationMetric)
}

// ResetResourceResyncMetricsCollectors resets all collectors
func ResetResourceResyncMetricsCollectors() {
	resourceSpecResyncDurationMetric.Reset()
	resourceStatusResyncDurationMetric.Reset()
}

// updateResourceSpecResyncDurationMetric updates the resource spec resync duration metric:
func updateResourceSpecResyncDurationMetric(source, cluster, dataType string, startTime time.Time) {
	labels := prometheus.Labels{
		metricsSourceLabel:   source,
		metricsClusterLabel:  cluster,
		metrucsDataTypeLabel: dataType,
	}
	duration := time.Since(startTime)
	resourceSpecResyncDurationMetric.With(labels).Observe(duration.Seconds())
}

// updateResourceStatusResyncDurationMetric updates the resource status resync duration metric:
func updateResourceStatusResyncDurationMetric(source, cluster, dataType string, startTime time.Time) {
	labels := prometheus.Labels{
		metricsSourceLabel:   source,
		metricsClusterLabel:  cluster,
		metrucsDataTypeLabel: dataType,
	}
	duration := time.Since(startTime)
	resourceStatusResyncDurationMetric.With(labels).Observe(duration.Seconds())
}
