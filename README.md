# Behavioral Tracker

A high-frequency e-commerce user analytics backend built to showcase **high-write system design**. The system captures clicks, scrolls, and page interactions without blocking the client — demonstrating how decoupling input capacity from database write speed prevents disk I/O from becoming a bottleneck under load.

The project includes a **live benchmark** that fires 1,000 req/s against both an async and a sync path simultaneously, with results visible in real-time on a pre-configured Grafana dashboard.

---

## What it showcases

| Pattern | Implementation |
|---|---|
| Fire-and-forget ingestion | `POST /events/async` returns HTTP 202 after a single Redis XADD |
| Async decoupling | Redis Streams buffers events between the API and database writers |
| Batch inserts | Workers accumulate up to 1,000 events before a single bulk `INSERT` |
| Hybrid flush trigger | Flush on size threshold (1,000 events) OR timer (5s), whichever first |
| Consumer group parallelism | Configurable worker pool — each goroutine is an independent stream consumer |
| Effectively-once delivery | Client-generated `event_id` + `ON CONFLICT DO NOTHING` prevents duplicates across retries |
| Sync baseline for comparison | `POST /events/sync` blocks on every PostgreSQL write — the "before" state |

---

## Architecture

```
                        ┌─────────────────────────────────┐
                        │          Gin HTTP Server        │
                        │                                 │
  POST /events/async ──►│  AsyncHandler                   │
                        │    XADD → Redis Streams         │──► HTTP 202 (immediate)
                        │                                 │
  POST /events/sync  ──►│  SyncHandler                    │
                        │    INSERT → PostgreSQL          │──► HTTP 200 (after disk write)
                        └─────────────────────────────────┘
                                        │
                                        │ XADD
                                        ▼
                        ┌─────────────────────────────────┐
                        │         Redis Streams           │
                        │         (Event Stream)          │
                        │                                 │
                        │  Consumer Group: batch-workers  │
                        └──────────────┬──────────────────┘
                                       │ XREADGROUP
                              ┌────────┴────────┐
                              │                 │
                         worker-0          worker-N
                         (goroutine)       (goroutine)     ← WORKER_COUNT workers
                              │                 │
                              └────────┬────────┘
                                       │ Bulk INSERT
                                       │ (1000 rows or 5s)
                                       ▼
                        ┌─────────────────────────────────┐
                        │           PostgreSQL            │
                        │                                 │
                        │  events (event_id PK)           │
                        │  ON CONFLICT DO NOTHING         │
                        └─────────────────────────────────┘
```

---

## Project structure

```
.
├── cmd/server/main.go              # Entry point — wires all components, starts workers
├── internal/
│   ├── model/event.go              # EventEnvelope — the single type flowing through the system
│   ├── stream/redis.go             # Redis Streams client (XADD, XREADGROUP, XACK)
│   ├── store/postgres.go           # BulkInsert (multi-row) + InsertOne (sync path)
│   ├── worker/worker.go            # Batch worker — hybrid size/time flush loop
│   ├── api/
│   │   ├── handler.go              # AsyncHandler and SyncHandler
│   │   └── routes.go               # Route registration + /metrics + /health
│   └── metrics/metrics.go          # Prometheus metrics definitions
├── migrations/
│   └── 001_create_events.sql       # events table + indexes
├── k6/
│   └── benchmark.js                # Dual constant-arrival-rate benchmark (500 RPS × 2)
├── docker/
│   ├── prometheus.yml              # Scrape config — app:8080/metrics every 5s
│   └── grafana/provisioning/       # Auto-provisioned datasource + dashboard
├── docker-compose.yml              # All services: app, redis, postgres, prometheus, grafana
├── Dockerfile                      # Multi-stage Go build → alpine
├── CONTEXT.md                      # Domain language glossary
└── docs/adr/                       # Architecture Decision Records
    ├── 0001-redis-streams-as-event-queue.md
    ├── 0002-client-generated-event-id-for-deduplication.md
    └── 0003-dual-endpoint-benchmark-design.md
```

---

## Event schema

All events use a generic envelope with a freeform `properties` blob. The client generates both `event_id` and `session_id` as UUID v4s before transmission.

```json
{
  "event_id":   "uuid-v4",
  "type":       "click",
  "session_id": "uuid-v4",
  "page":       "/products/shoes",
  "timestamp":  "2024-01-01T12:00:00Z",
  "properties": {
    "x":          420,
    "y":          810,
    "element_id": "btn-add-to-cart"
  }
}
```

`event_id` must be client-generated so that retried requests carry the same ID — enabling the `ON CONFLICT DO NOTHING` deduplication strategy to work end-to-end, not just within the worker.

---

## Key design decisions

### Why Redis Streams over RabbitMQ

Redis Streams has two properties that matter here: consumer groups (multiple workers claim different messages with no duplicates) and the append-only log (pending messages survive a worker crash and are re-claimed via `XAUTOCLAIM`). RabbitMQ's routing sophistication is unnecessary for a single producer → single consumer type topology. Running Redis as both cache and queue avoids a second infrastructure service.

### Why the hybrid flush trigger

A size-only flush means events sit in memory indefinitely during low traffic. A time-only flush means unbounded memory growth during a traffic spike. The hybrid (1,000 events OR 5 seconds) handles both extremes cleanly.

### Why bulk INSERT over COPY

PostgreSQL's `COPY` protocol is faster but does not support `ON CONFLICT`. The multi-row `INSERT ... ON CONFLICT DO NOTHING` sacrifices some throughput for deduplication correctness. For 1,000-row batches the difference is negligible.

### Why two endpoints instead of a config flag

Running both paths on the same service under the same load conditions makes the comparison fair — identical infrastructure, identical request shapes, measured simultaneously. A flag-switched single endpoint would require two separate benchmark runs against potentially different system states.

---

## Running the project

### Prerequisites

- Docker and Docker Compose

### Start all services

```bash
docker compose up --build
```

This starts:
- **App** — Gin API on `http://localhost:8080`
- **Redis** — Stream broker on port 6379
- **PostgreSQL** — Event store on port 5432
- **Prometheus** — Metrics scraper on `http://localhost:9090`
- **Grafana** — Dashboard on `http://localhost:3000` (admin / admin)

### Verify the API

```bash
# Health check
curl http://localhost:8080/health

# Send an event via the async path (returns 202 immediately)
curl -X POST http://localhost:8080/events/async \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id":   "00000000-0000-0000-0000-000000000001",
    "type":       "click",
    "session_id": "00000000-0000-0000-0000-000000000002",
    "page":       "/products/shoes",
    "timestamp":  "2024-01-01T12:00:00Z",
    "properties": {"x": 420, "y": 810, "element_id": "btn-add-to-cart"}
  }'

# Send an event via the sync path (returns 200 after DB write)
curl -X POST http://localhost:8080/events/sync \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id":   "00000000-0000-0000-0000-000000000003",
    "type":       "scroll",
    "session_id": "00000000-0000-0000-0000-000000000002",
    "page":       "/products/shoes",
    "timestamp":  "2024-01-01T12:00:01Z",
    "properties": {"scroll_depth_pct": 72}
  }'
```

---

## Running the benchmark

```bash
docker compose --profile benchmark run --rm k6
```

The benchmark fires **500 req/s at each endpoint simultaneously** for 60 seconds (1,000 req/s total). k6 pushes metrics to Prometheus via remote write, so everything appears live in Grafana.

**Open the Grafana dashboard:** `http://localhost:3000` → "Behavioral Tracker — Async vs Sync"

Set the time range to **Last 5 minutes** to see the active run.

### Benchmark results (500 RPS per path)

```
async path    p95: ~1ms     avg: ~500µs    ← Redis XADD only
sync  path    p95: ~2ms     avg: ~1ms      ← PostgreSQL write blocks the response

throughput:   1,000 req/s sustained, 0 failures, all checks passed
```

The async path runs 2× faster at 500 RPS. The gap widens significantly at higher concurrency because PostgreSQL becomes the bottleneck while Redis continues to accept writes at memory speed.

### Pushing the load higher

Edit `k6/benchmark.js` and increase the rate:

```js
// k6/benchmark.js
rate: 2000,  // was 500
```

Then rerun. Watch the sync path's VU count climb as PostgreSQL struggles to keep up while the async path stays flat — the Redis Stream absorbs the burst and the batch workers drain it at their own pace.

---

## Configuration

All configuration is via environment variables, set in `docker-compose.yml`:

| Variable | Default | Description |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `DATABASE_URL` | `postgres://tracker:tracker@localhost:5432/behavioral_tracker?sslmode=disable` | PostgreSQL DSN |
| `WORKER_COUNT` | `4` | Number of parallel batch worker goroutines |
| `PORT` | `8080` | HTTP listen port |

Increasing `WORKER_COUNT` adds more Redis Streams consumers to the group, increasing parallel flush throughput. The throughput-vs-workers relationship is visible on the Grafana "Batch Flush Rate" panel.

---

## Observability

### Prometheus metrics (exposed at `/metrics`)

| Metric | Type | Description |
|---|---|---|
| `events_async_total` | Counter | Events accepted on the async path |
| `events_sync_total` | Counter | Events accepted on the sync path |
| `events_async_duration_seconds` | Histogram | Async handler latency (handler → Redis write) |
| `events_sync_duration_seconds` | Histogram | Sync handler latency (handler → PostgreSQL write) |
| `batch_flush_total` | Counter | Total batch flushes performed |
| `batch_flush_duration_seconds` | Histogram | Time taken per batch flush |
| `batch_size_events` | Histogram | Number of events per flush |

### Grafana dashboard panels

1. **Request Rate** — async vs sync req/s over time
2. **p95 Latency** — async vs sync response time percentiles
3. **Batch Flush Rate** — how often workers flush to PostgreSQL
4. **Median Batch Size** — average events per flush (approaches 1,000 under load)
5. **Batch Flush Duration p95** — time cost of each bulk insert
6. **Total Events Stored** — cumulative counters for both paths

---

## Database schema

```sql
CREATE TABLE events (
    event_id    UUID        PRIMARY KEY,
    type        TEXT        NOT NULL,
    session_id  UUID        NOT NULL,
    page        TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    properties  JSONB       NOT NULL DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_session_id ON events (session_id);
CREATE INDEX idx_events_page       ON events (page);
CREATE INDEX idx_events_timestamp  ON events (timestamp);
```

`event_id` is the primary key and the deduplication key. The `properties` JSONB column stores event-type-specific data without requiring schema changes for new event types.

---

## Tech stack

| Component | Technology | Reason |
|---|---|---|
| API server | Go + Gin | Low GC overhead keeps benchmark noise minimal; goroutines are a natural fit for the worker pool |
| Message queue | Redis Streams | Consumer groups, append-only log with replay, no second broker needed |
| Database | PostgreSQL | Universally understood; the COPY/bulk-INSERT optimization story is clear and explainable |
| Load testing | k6 | Prometheus remote write output; constant-arrival-rate executor for precise RPS control |
| Metrics | Prometheus + Grafana | Auto-provisioned dashboard; k6 and app metrics on the same timeline |
| Containerization | Docker Compose | Single command to start the full stack |
