package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	AsyncEventsTotal   prometheus.Counter
	SyncEventsTotal    prometheus.Counter
	AsyncDuration      prometheus.Histogram
	SyncDuration       prometheus.Histogram
	BatchFlushTotal    prometheus.Counter
	BatchFlushDuration prometheus.Histogram
	BatchSize          prometheus.Histogram
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		AsyncEventsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_async_total",
			Help: "Total events accepted on the async path",
		}),
		SyncEventsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_sync_total",
			Help: "Total events accepted on the sync path",
		}),
		AsyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "events_async_duration_seconds",
			Help:    "Latency of async ingestion: handler to Redis stream write",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1},
		}),
		SyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "events_sync_duration_seconds",
			Help:    "Latency of sync ingestion: handler to PostgreSQL write",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
		BatchFlushTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_flush_total",
			Help: "Total number of batch flushes to PostgreSQL",
		}),
		BatchFlushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_flush_duration_seconds",
			Help:    "Duration of each batch flush",
			Buckets: prometheus.DefBuckets,
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_size_events",
			Help:    "Number of events in each batch flush",
			Buckets: []float64{10, 50, 100, 250, 500, 750, 1000},
		}),
	}
	reg.MustRegister(
		m.AsyncEventsTotal,
		m.SyncEventsTotal,
		m.AsyncDuration,
		m.SyncDuration,
		m.BatchFlushTotal,
		m.BatchFlushDuration,
		m.BatchSize,
	)
	return m
}
