# xeyqor

<p align="center">
  <img src="logo.png" alt="Xeyqor logo" width="400">
</p>

**xeyqor** is a small Redis-backed job queue for Go. Every state transition is a
single Lua script, so job claiming and completion are atomic by construction.

> Early-stage. The API and the Redis layout will change; not ready for
> production use.

## Status

Enqueue and consumption work today. `Worker.Run` claims up to `Concurrency`
jobs at a time, renews their leases while the handler runs, and records
completion. Retries, expired-lease recovery and dead-lettering are not
implemented yet.

## Requirements

- Go 1.27+ (see [`go.mod`](go.mod))
- Redis 6.2+, for `LMOVE`

## Usage

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

jobID, err := queue.Add(ctx, xeyqor.Job{
	Description:    "email:send",
	IdempotencyKey: "order-123-confirmation", // optional
	MaxRetries:     3,                        // optional
})
```

`Add` rejects a job without a description, one that already has a worker, a
negative fencing token or retry count, and one that does not start at zero
attempts.

Reusing an idempotency key returns the original job ID instead of enqueuing a
duplicate. Keys expire after 24 hours.

## Run a worker

```go
worker, err := xeyqor.NewWorker(ctx, xeyqor.WorkerConfig{
	Name:        "emails",
	Redis:       rdb,
	Concurrency: 4,
	Handler: func(ctx context.Context, job xeyqor.Job) error {
		return sendEmail(ctx, job.Description)
	},
})
if err != nil {
	return err
}

return worker.Run(ctx) // blocks until ctx is cancelled
```

`Concurrency` defaults to 1 and `Handler` is required. `Run` keeps up to
`Concurrency` jobs in flight and claims the next job as soon as one finishes, so
a slow job never idles the other slots. When nothing is pending it blocks on the
queue's notification list.

A job whose handler fails stays in `running` until its lease expires; retries
are not implemented yet.

## Redis keys

For a queue named `emails`, all keys share the `xeyqor:{emails}:` prefix. The
hash tag keeps them in one cluster slot, which is what allows the scripts to
touch several keys atomically.

| Key | Type | Purpose |
| --- | --- | --- |
| `pending` | list | Job IDs waiting to be claimed, oldest on the right. |
| `notifications` | list | Wake-up signal for idle workers. |
| `running` | list | Job IDs held by a worker. |
| `leases` | sorted set | Job ID → lease expiry in milliseconds. |
| `completed` | list | Job IDs that finished successfully. |
| `job:<id>` | hash | Job fields: description, worker, fencing token, attempts, retries. |
| `idempotency:<key>` | string | Idempotency key → job ID. |

The layout is an implementation detail, not a stable contract.

## Development

```sh
go test ./...
```

Tests run against [`miniredis`](https://github.com/alicebob/miniredis), so no
Redis server is required. The race detector needs cgo:

```sh
CGO_ENABLED=1 go test -race ./...
```

## Roadmap

- Expired-lease recovery: requeue up to `MaxRetries`, then dead-letter.
- Failure and acknowledgement semantics.
- Cleanup for completed jobs and the `completed` list.
- Tests for claim, heartbeat, completion, lease expiry and concurrent workers.

## License

MIT — see [`LICENSE`](LICENSE).
