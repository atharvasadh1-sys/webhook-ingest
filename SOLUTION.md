# Solution

## What was broken

The webhook ingestion path was not idempotent. Duplicate deliveries could cause
the same event to be stored and account statistics to be incremented more than
once. The root cause was that `event_id` was not enforced as a unique key at the
database level.

I fixed this by enforcing uniqueness on `events.event_id` and using
`ON CONFLICT (event_id) DO NOTHING`. `InsertEvent` reports whether a new row was
inserted, so duplicate deliveries skip call updates and statistics updates.
Tests cover both repeated and concurrent duplicate deliveries.

Recording processing was previously handled by an in-memory goroutine. Work
could therefore disappear when the service restarted during deployment. I
changed this to a Redis-backed queue with a processing list. Jobs remain
recoverable if the worker stops while processing, and graceful shutdown was
added for the worker.

## Why this deduplication strategy

I chose PostgreSQL as the source of truth for deduplication because `event_id`
is already stored with each webhook and PostgreSQL can enforce uniqueness
atomically. This also protects against races between concurrent deliveries.

An in-memory lock would not work across multiple service instances or restarts.
Using Redis for deduplication would add another source of truth and would still
require careful handling of expiration and failures. The PostgreSQL constraint
is durable, simple, and guarantees that only one delivery can win the insert.

## Scaling to 10,000 webhooks/sec

At 10,000 webhooks/sec, I would separate ingestion from downstream processing.
The HTTP layer should validate the request, persist the event/idempotency key,
and acknowledge it quickly. Processing of calls, statistics, and recordings
should happen asynchronously through a durable message queue.

I would partition the queue by account or event key so multiple consumers can
process events concurrently while preserving ordering where required. PostgreSQL
would remain the durable source of truth, with appropriate indexes, batching,
connection-pool tuning, and possibly partitioning for the events table.

Redis could continue to be used for fast operational state, but it should not
replace the durable PostgreSQL idempotency guarantee. I would also add metrics
for queue depth, processing latency, duplicate rate, failures, and retry counts,
along with backpressure and dead-letter handling for failed jobs.
