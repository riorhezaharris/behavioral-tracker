package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/riorhezaharris/behavioral-tracker/internal/metrics"
	"github.com/riorhezaharris/behavioral-tracker/internal/model"
	"github.com/riorhezaharris/behavioral-tracker/internal/store"
	"github.com/riorhezaharris/behavioral-tracker/internal/stream"
)

const (
	batchSize     = 1000
	flushInterval = 5 * time.Second
	readCount     = 100
	blockDuration = 100 * time.Millisecond
)

type BatchWorker struct {
	id      int
	stream  *stream.RedisStream
	store   *store.PostgresStore
	metrics *metrics.Metrics
}

func New(id int, s *stream.RedisStream, p *store.PostgresStore, m *metrics.Metrics) *BatchWorker {
	return &BatchWorker{id: id, stream: s, store: p, metrics: m}
}

// Run is the main worker loop. It reads from the Event Stream, accumulates
// events into a buffer, and flushes to PostgreSQL when the buffer reaches
// batchSize OR flushInterval elapses — whichever comes first.
func (w *BatchWorker) Run(ctx context.Context) {
	name := fmt.Sprintf("worker-%d", w.id)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	buffer := make([]model.EventEnvelope, 0, batchSize)
	pendingIDs := make([]string, 0, batchSize)

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		start := time.Now()
		count := len(buffer)

		if err := w.store.BulkInsert(ctx, buffer); err != nil {
			log.Printf("worker-%d: bulk insert failed: %v", w.id, err)
			return
		}
		w.stream.Ack(ctx, pendingIDs...)

		w.metrics.BatchFlushTotal.Inc()
		w.metrics.BatchFlushDuration.Observe(time.Since(start).Seconds())
		w.metrics.BatchSize.Observe(float64(count))

		buffer = buffer[:0]
		pendingIDs = pendingIDs[:0]
	}

	for {
		events, ids, err := w.stream.ReadGroup(ctx, name, readCount, blockDuration)
		if err != nil {
			if ctx.Err() != nil {
				flush()
				return
			}
			continue
		}

		if len(events) > 0 {
			buffer = append(buffer, events...)
			pendingIDs = append(pendingIDs, ids...)
			if len(buffer) >= batchSize {
				flush()
			}
		}

		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		default:
		}
	}
}
