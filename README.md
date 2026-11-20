# xeyqor

<p align="center">
  <img src="logo.png" alt="Xeyqor logo" width="240">
</p>

**xeyqor** is an early-stage Go project exploring a Redis-backed job queue. The aim is to build a small, understandable queue with atomic job and worker operations, safe claiming, and clear retry behavior.

> This project is under active development. The API and Redis data layout may change, and it is not ready for production use.

## What works today

- Create a named queue with `NewQueue`.
- Enqueue a job with a generated UUID and required description.
- Store job data in a Redis hash and add its ID to the queue's pending list.
- Optionally provide an idempotency key. The enqueue Lua script checks and reserves that key in the same atomic Redis operation that creates the job. Idempotency keys expire after 24 hours.

The current API reports an error when an idempotency key has already been used. It does not yet return the original job ID for a repeated request.

## Planned work

- Claim jobs atomically for workers.
- Use fencing tokens to protect against stale workers.
- Define job acknowledgement, failure, and retry behavior.
- Expand tests for validation, idempotency, and concurrent queue operations.

## Requirements

- Go version declared in [`go.mod`](go.mod).
- A Redis server reachable by the `go-redis` client.

## Enqueue a job

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
defer rdb.Close()

queue, err := xeyqor.NewQueue(ctx, xeyqor.QueueConfig{
	Name:  "emails",
	Redis: rdb,
})
if err != nil {
	return err
}

jobID, err := queue.Enqueue(ctx, xeyqor.Job{
	Description:    "email:send",
	IdempotencyKey: "order-123-confirmation", // optional
})
if err != nil {
	return err
}

fmt.Println("enqueued job:", jobID)
```

## Redis keys

For a queue named `emails`, the current implementation uses the `xeyqor:{emails}:` prefix:

| Key | Purpose |
| --- | --- |
| `pending` | List of pending job IDs. |
| `notifications` | List populated when a job is enqueued. |
| `job:<id>` | Hash with the job's current fields. |
| `idempotency:<key>` | Mapping from an idempotency key to a job ID; expires after 24 hours. |

This describes the current implementation, not a stable storage contract.

## Development

Run the Go test suite with:

```sh
go test ./...
```

Contributions and feedback are welcome while the API is taking shape.
