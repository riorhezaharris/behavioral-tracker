# Redis Streams as the Event Queue

We use Redis Streams (not RabbitMQ) as the Event Stream between the Ingestion API and the Batch Worker. Redis Streams provides consumer groups for parallel worker processing and an append-only log that supports message replay on worker crash — both critical for the high-write correctness story. RabbitMQ's routing sophistication (exchanges, bindings) is unnecessary here since the event flow is a single producer to one consumer type. Running Redis as both the cache layer and the queue also avoids operating a second infrastructure service.
