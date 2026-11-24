package xeyqor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Handler func(ctx context.Context, job Job) error

const (
	leaseDuration        = 30 * time.Second
	leaseRenewalDuration = 10 * time.Second
)

var claimScript = redis.NewScript(
	`local leases = KEYS[1]
	local pending = KEYS[2]
	local running = KEYS[3]
	local worker_id = ARGV[1]
	local job_prefix = ARGV[2]

	local job_id = redis.call("LMOVE", pending, running, "RIGHT", "LEFT")

	if not job_id then
		return nil
	end

	local t = redis.call("TIME")
	local seconds = tonumber(t[1])
	local microseconds = tonumber(t[2])
	local milliseconds = (seconds * 1000) + math.floor(microseconds / 1000)

	local lease_expiry = milliseconds + (30 * 1000)
	redis.call("ZADD", leases, lease_expiry, job_id)

	local job_key = job_prefix .. "job:" .. job_id
	local fencing_token = redis.call("HINCRBY", job_key, "fencing_token", 1)
	redis.call("HSET", job_key, "worker_id", worker_id)

	return {job_id, fencing_token}
`)

var heartbeatScript = redis.NewScript(
	`local running = KEYS[1]
	local job_key = KEYS[2]
	local leases = KEYS[3]
	local job_id = ARGV[1]
	local worker_id = ARGV[2]
	local fencing_token = ARGV[3]
	local lease_duration = ARGV[4]

	if redis.call("LPOS", running, job_id) == false then
		return 0
	end

	if redis.call("HGET", job_key, "worker_id") ~= worker_id then
		return 0
	end

	if tonumber(redis.call("HGET", job_key, "fencing_token")) ~= tonumber(fencing_token) then
		return 0
	end

	local lease_expiry = redis.call("ZSCORE", leases, job_id)
	
	if not lease_expiry then
		return 0
	end

	local t = redis.call("TIME")
	local seconds = tonumber(t[1])
	local microseconds = tonumber(t[2])
	local milliseconds = (seconds * 1000) + math.floor(microseconds / 1000)

	if tonumber(lease_expiry) <= milliseconds then
		return 0
	end

	local new_lease = milliseconds + tonumber(lease_duration)

	redis.call("ZADD", leases, new_lease, job_id)
	return 1`)

var completeScript = redis.NewScript(
	`local running = KEYS[1]
	local leases = KEYS[2]
	local completed = KEYS[3]
	local job = KEYS[4]

	local job_id = ARGV[1]
	local worker_id = ARGV[2]
	local fencing_token = tonumber(ARGV[3])

	if redis.call("HGET", job, "worker_id") ~= worker_id then
		return 0
	end

	local stored_token = tonumber(redis.call("HGET", job, "fencing_token"))
	if not stored_token or stored_token ~= fencing_token then
		return 0
	end

	if redis.call("LPOS", running, job_id) == false then
		return 0
	end

	local lease_expiry = redis.call("ZSCORE", leases, job_id)
	if not lease_expiry then
		return 0
	end

	local t = redis.call("TIME")
	local milliseconds = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
	if tonumber(lease_expiry) <= milliseconds then
		return 0
	end

	if redis.call("LREM", running, 1, job_id) ~= 1 then
		return 0
	end

	redis.call("ZREM", leases, job_id)
	redis.call("RPUSH", completed, job_id)
	redis.call("HSET", job, "status", "completed", "completed_at", milliseconds)
	return 1`)

type Worker struct {
	name        string // must match the name of the queue its listening to
	handler     Handler
	redisClient redis.UniversalClient
}

func NewWorker(ctx context.Context, name string, redisClient redis.UniversalClient) (*Worker, error) {

	// confirm the redis client is provided
	if redisClient == nil {
		return nil, errors.New("xeyqor: redis client is required")
	}

	// ping redis
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("xeyqor: ping redis %w", err)
	}

	//confirm queue name is provided
	if name == "" {
		return nil, errors.New("xeyqor: queue name is required")
	}

	return &Worker{
		name:        name,
		redisClient: redisClient,
	}, nil
}

func (w *Worker) claim(ctx context.Context, workerID string) (Job, error) {
	if workerID == "" {
		return Job{}, errors.New("xeyqor: worker ID is required")
	}

	for {
		prefix := "xeyqor:{" + w.name + "}:"
		keys := []string{prefix + "leases", prefix + "pending", prefix + "running"}

		result, err := claimScript.Run(ctx, w.redisClient, keys, workerID, prefix).Result()

		if err != nil {
			return Job{}, err
		}

		if result == nil {
			_, err := w.redisClient.BLPop(ctx, 0, prefix+"notifications").Result()

			if err != nil {
				return Job{}, err
			}

			continue
		}

		values, ok := result.([]interface{})
		if !ok || len(values) != 2 {
			return Job{}, fmt.Errorf("unexpected claim result: %T", result)
		}

		jobID, ok1 := values[0].(string)
		fencingToken, ok2 := values[1].(int64)

		if !ok1 || !ok2 {
			return Job{}, fmt.Errorf("unexpected claim result types")
		}

		jobKey := prefix + "job:" + jobID
		res, err := w.redisClient.HGetAll(ctx, jobKey).Result()
		if err != nil {
			return Job{}, err
		}

		if len(res) == 0 {
			return Job{}, fmt.Errorf("job has no data: %s", jobID)
		}

		var job Job
		if err := w.redisClient.HGetAll(ctx, jobKey).Scan(&job); err != nil {
			return Job{}, err
		}

		job.FencingToken = fencingToken
		job.WorkerID = workerID

		return job, nil
	}
}

func (w *Worker) heartbeat(ctx context.Context, jobID string, workerID string, fencingToken int64, leaseDuration time.Duration, ticker *time.Ticker) error {

	if workerID == "" {
		return errors.New("xeyqor: worker ID is required")
	}
	if ticker == nil {
		return errors.New("xeyqor: ticker is required")
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("lease duration cannot be 0 or less")
	}

	prefix := "xeyqor:{" + w.name + "}:"
	keys := []string{prefix + "running", prefix + "job:" + jobID, prefix + "leases"}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			count, err := heartbeatScript.Run(ctx, w.redisClient, keys, jobID, workerID, fencingToken, leaseDuration.Milliseconds()).Int64()

			if err != nil {
				return err
			}

			switch count {
			case 0:
				return fmt.Errorf("heartbeat rejected: worker ownership of lease validation failed")
			case 1:
				continue
			default:
				return fmt.Errorf("unexpected heartbeat result %T", count)
			}
		}
	}

}

func (w *Worker) complete(ctx context.Context, jobID, workerID string, fencingToken int64) error {
	if jobID == "" {
		return errors.New("xeyqor: job ID is required")
	}
	if workerID == "" {
		return errors.New("xeyqor: worker ID is required")
	}
	if fencingToken <= 0 {
		return errors.New("xeyqor: fencing token must be positive")
	}

	prefix := "xeyqor:{" + w.name + "}:"
	keys := []string{
		prefix + "running",
		prefix + "leases",
		prefix + "completed",
		prefix + "job:" + jobID,
	}

	count, err := completeScript.Run(ctx, w.redisClient, keys, jobID, workerID, fencingToken).Int64()
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("xeyqor: job completion rejected: ownership or lease validation failed")
	}
	if count != 1 {
		return fmt.Errorf("xeyqor: unexpected complete result: %d", count)
	}

	return nil
}

func (w *Worker) processOne(ctx context.Context, job Job) error {
	if w.handler == nil {
		return errors.New("xeyqor: worker handler is required")
	}
	if job.WorkerID == "" {
		return errors.New("xeyqor: job has no worker ID")
	}
	if job.FencingToken <= 0 {
		return errors.New("xeyqor: job has an invalid fencing token")
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(leaseRenewalDuration)
	defer ticker.Stop()

	handlerDone := make(chan error, 1)
	heartbeatDone := make(chan error, 1)

	go func() {
		heartbeatDone <- w.heartbeat(
			jobCtx,
			job.ID,
			job.WorkerID,
			job.FencingToken,
			leaseDuration,
			ticker,
		)
	}()

	go func() {
		handlerDone <- w.handler(jobCtx, job)
	}()

	select {
	case err := <-heartbeatDone:
		if err == nil {
			return errors.New("xeyqor: heartbeat stopped unexpectedly")
		}
		return fmt.Errorf("xeyqor: heartbeat failed: %w", err)
	case err := <-handlerDone:
		if err != nil {
			return fmt.Errorf("xeyqor: handler failed: %w", err)
		}
	}

	if err := w.complete(jobCtx, job.ID, job.WorkerID, job.FencingToken); err != nil {
		return fmt.Errorf("xeyqor: complete job: %w", err)
	}

	return nil
}
