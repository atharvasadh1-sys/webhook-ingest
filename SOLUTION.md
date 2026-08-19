# Solution

## What was broken

The webhook ingestion path was not fully idempotent. Duplicate deliveries could
cause account statistics to be incremented more than once. The root cause was
that `event_id` was not enforced as a unique key at the database level.

I fixed this by enforcing uniqueness on `events.event_id` and using
`ON CONFLICT (event_id) DO NOTHING`. `ProcessEvent` performs the event insert,
call update, and account-stat update in one PostgreSQL transaction and reports
whether the event was newly inserted. Duplicate deliveries therefore skip all
business updates. Tests cover both repeated and concurrent duplicate deliveries.

Recording processing also had two reliability problems. Recording work could
disappear when the service restarted, and there was a DB-to-Redis dual-write
gap: PostgreSQL could commit successfully while the subsequent Redis enqueue
failed. A retry would then see the event as a duplicate and could skip the
recording entirely.

I fixed this with a transactional outbox. When a webhook contains a recording,
the recording job is written to `recording_outbox` in the same PostgreSQL
transaction as the event, call, and account statistics. A background relay
publishes pending outbox jobs to Redis and removes them from the outbox only
after the Redis publish succeeds. The existing Redis processing list provides
recovery for jobs that were already being processed when the service stopped.

## Why this deduplication strategy

I chose PostgreSQL as the source of truth for deduplication because `event_id`
is already stored with each webhook and PostgreSQL can enforce uniqueness
atomically. This also protects against races between concurrent deliveries.

An in-memory lock would not work across multiple service instances or restarts.
Using Redis for deduplication would add another source of truth and would still
require careful handling of expiration and failures. The PostgreSQL constraint
is durable, simple, and guarantees that only one delivery can win the insert.

The transactional outbox also keeps PostgreSQL and Redis from having an
unrecoverable dual-write gap. Redis is used for asynchronous processing, while
PostgreSQL remains the durable source of truth.

## Scaling to 10,000 webhooks/sec

At 10,000 webhooks/sec, I would separate ingestion from downstream processing.
The HTTP layer should validate the request, persist the event and idempotency
key, and acknowledge it quickly. Processing of calls, statistics, and
recordings should happen asynchronously through a durable message queue.

I would partition the queue by account or event key so multiple consumers can
process events concurrently while preserving ordering where required.
PostgreSQL would remain the durable source of truth, with appropriate indexes,
batching, connection-pool tuning, and possibly partitioning for the events and
outbox tables.

Redis could continue to be used for fast operational state, but it should not
replace the durable PostgreSQL idempotency guarantee. I would also add metrics
for queue depth, processing latency, duplicate rate, failures, and retry
counts, along with backpressure and dead-letter handling for failed jobs.