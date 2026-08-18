# Solution

## What was broken

The webhook ingestion path was not idempotent. Duplicate deliveries could cause
the same event to be stored and account statistics to be incremented more than
once. The root cause was that `event_id` was not enforced as a unique key at
the database level.

I fixed this by adding a unique index on `events.event_id` and using
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
Using Redis as the deduplication source would introduce another source of truth
and require handling Redis failures and key expiration. The PostgreSQL unique
constraint keeps the idempotency guarantee close to the data being protected.

## Scaling to 10,000 webhooks/sec

At 10,000 webhooks/sec, I would horizontally scale the API layer and keep
PostgreSQL as the durable idempotency boundary. Background recording work would
be handled by a scalable worker pool or message broker. Database connections,
indexes, Redis capacity, queue depth, retries, and backpressure would need to
be monitored and tuned.

I would also add metrics for request latency, duplicate rate, queue depth,
processing failures, and database/Redis saturation, with retry and backoff
policies for failed background jobs.