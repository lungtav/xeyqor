package xeyqor

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID           string `redis:"id"`
	WorkerID     string `redis:"worker_id"`
	Description  string `redis:"description"`
	FencingToken int64  `redis:"fencing_token"`
	MaxRetries   int64  `redis:"max_retries"`
	Attempts     int64  `redis:"attempts"`
}

type QueueConfig struct {
	Name  string
	Redis redis.UniversalClient
}

type Queue struct {
	Name   string
	Config QueueConfig
}

func NewQueue(ctx context.Context, cfg QueueConfig) (*Queue, error) {

	if cfg.Name == "" {
		return nil, errors.New("xeyqor: queue name is required")
	}

	if cfg.Redis == nil {
		return nil, errors.New("xeyqor: redis client is required")
	}

	if err := cfg.Redis.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("xeyqor: ping redis %w", err)
	}

	return &Queue{
		Name:   cfg.Name,
		Config: cfg,
	}, nil
}

func (q *Queue) Enqueue(ctx context.Context, job Job) error {

	script := `
		local pending = KEYS[1]
		local job = KEYS[2]
		local notifications = KEYS[3]
		local job_id = ARGV[1]
		local job_desc = ARGV[2]
		local job_fencing_token = ARGV[3]
		local job_retries = ARGV[4]
		local job_worker_id = ARGV[5]
		local job_attempts = ARGV[6]

		--checking if hash exists

		if redis.call("EXISTS", job) == 1 then
			return 0
		end
		

		redis.call("HSET", job, "id", job_id, "description", job_desc, "fencing_token", job_fencing_token, "worker_id", job_worker_id, "attempts", job_attempts, "max_retries", job_retries)

		redis.call("LPUSH", pending, job_id)

		redis.call("LPUSH", notifications, job_id)

		return 1
	`

	if job.ID == "" {
		return errors.New("xeyqor: job ID is required")
	}

	if job.Description == "" {
		return errors.New("xeyqor: job description is required")
	}

	if job.WorkerID != "" {
		return fmt.Errorf("xeyqor: job to enqueue should have no worker id: %s", job.WorkerID)
	}

	if job.FencingToken < 0 {
		return fmt.Errorf("xeyqor: invalid fencing token: %d", job.FencingToken)
	}

	if job.MaxRetries < 0 {
		return fmt.Errorf("xeyqor: invalid maximum job retries: %d", job.MaxRetries)
	}

	if job.Attempts != 0 {
		return fmt.Errorf("xeyqor: new job must start at 0 attempts %d", job.Attempts)
	}

	prefix := "xeyqor:" + q.Name

	enq := q.Config.Redis.Eval(ctx, script, []string{prefix + ":pending", prefix + ":job:" + job.ID, prefix + ":notifications"}, job.ID, job.Description, job.FencingToken, job.MaxRetries, job.WorkerID, job.Attempts)

	result, err := enq.Result()

	if err != nil {
		return err
	}

	count, ok := result.(int64)

	if !ok {
		return errors.New("xeyqor: type assertion failed")
	}

	switch count {
	case 0:
		return errors.New("xeyqor: job already exists in queue")
	case 1:
		return nil
	default:
		return fmt.Errorf("unexpected Lua script result: %d", count)
	}
}
