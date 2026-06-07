# Behavioral Tracker

A high-frequency e-commerce analytics system that captures user interactions without impacting the core shopping experience. Built as a portfolio project to showcase high-write backend system design.

## Language

### Core Domain

**Event**:
A single user interaction (click, scroll, page view) captured and semantically enriched by the client at the point of emission.
_Avoid_: action, hit, signal, log entry

**Event Envelope**:
The generic wrapper transmitted to the server — fixed outer fields (`event_id`, `type`, `session_id`, `page`, `timestamp`) plus a freeform `properties` blob specific to the event type.
_Avoid_: event schema, event payload

**Session**:
A client-generated UUID that groups Events from a single browsing session. Not a first-class server entity — used only as a grouping key on stored Events.
_Avoid_: user session, auth session, visit

### Write Path

**Ingestion API**:
The Gin HTTP service that accepts Event Envelopes. Exposes two paths for benchmark comparison: the Async Path (`POST /events/async`) and the Sync Path (`POST /events/sync`).
_Avoid_: collector, event API, ingest service

**Async Path**:
The high-write path: the Ingestion API writes the Event to the Event Stream and immediately returns HTTP 202 Accepted. Processing happens out-of-band.
_Avoid_: async mode, queue path

**Sync Path**:
The baseline path: the Ingestion API writes the Event directly to PostgreSQL and returns HTTP 200 only after the write completes. Exists solely for benchmark comparison against the Async Path.
_Avoid_: sync mode, direct path

**Event Stream**:
The Redis Streams queue that buffers Events between the Ingestion API and the Batch Worker on the Async Path.
_Avoid_: queue, message queue, channel, topic

**Batch Worker**:
A goroutine in a configurable pool that reads Events from the Event Stream and bulk-inserts them into PostgreSQL. Worker count is set via `WORKER_COUNT` at startup.
_Avoid_: consumer, processor, writer

**Batch Flush**:
The act of bulk-inserting the accumulated Event buffer into PostgreSQL. Triggered when the buffer reaches 1000 Events OR 5 seconds have elapsed — whichever comes first.
_Avoid_: flush, drain, commit

## Relationships

- An **Event** is always wrapped in an **Event Envelope** before transmission
- A **Session** is a client-generated UUID stamped on every **Event Envelope** — the server never creates or validates Sessions
- The **Ingestion API** routes **Event Envelopes** to either the **Async Path** or the **Sync Path**
- On the **Async Path**, the **Ingestion API** writes to the **Event Stream**; the **Batch Worker** reads from it and triggers a **Batch Flush**
- On the **Sync Path**, the **Ingestion API** writes directly to PostgreSQL with no **Event Stream** or **Batch Worker** involved

## Example dialogue

> **Dev:** "When a browser sends a click, does the server validate the Session before accepting the Event?"
> **Domain expert:** "No — the Session is just a UUID the client stamps on the Event Envelope. The server never validates it. It's a grouping key, nothing more."

> **Dev:** "What triggers a Batch Flush?"
> **Domain expert:** "Either the buffer hits 1000 Events or 5 seconds pass — whichever comes first. This handles both traffic spikes and quiet periods."

## Flagged ambiguities

- "session" was used loosely early on — resolved: a **Session** is a client-generated grouping key, not a server-side entity. It has no lifecycle events (start/end) in this system.
