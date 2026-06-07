# Dual-Endpoint Design for Async vs Sync Benchmark

The Ingestion API intentionally exposes two endpoints on the same service: `POST /events/async` (the Redis Streams path) and `POST /events/sync` (direct PostgreSQL write). The Sync Path is not a feature — it exists solely as a benchmark baseline to demonstrate the throughput and latency cost of synchronous disk writes under load.

A runtime config flag (single service, two modes) was considered but rejected: two endpoints allow k6 to hit both paths simultaneously against identical infrastructure, making the comparison fair and the results visible on the same Grafana dashboard at the same moment.
