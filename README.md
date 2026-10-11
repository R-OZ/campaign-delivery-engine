# Campaign Delivery Engine

A small, retry-aware notification dispatcher written in Go.

The central idea is to schedule retries in a coordinator rather than inside workers. Failed messages wait in a delayed queue while workers remain available to process new messages, avoiding unnecessary throughput bottlenecks during backoff.

### Features

- Bounded worker concurrency
- Shared rate limiting across workers
- Centralized retry scheduling with exponential backoff
- Idempotency keys for safe retries when supported by the provider
- Context-aware cancellation

### Run

```bash
go run campaign_delivery_engine.go
```

Uses only the Go standard library.

### Demo

The simulated provider demonstrates two failure scenarios:

1. A message is rejected before acceptance and retried.
2. A message is accepted, but its acknowledgement is lost. The subsequent retry is deduplicated.

Four messages are successfully delivered across six attempts, with no duplicate deliveries.

### Note

This is a standalone engineering sample, not a production messaging service. Exactly-once delivery depends on the external provider honoring idempotency keys.
