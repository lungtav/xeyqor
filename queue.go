package xeyqor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID             string `redis:"id"`
	WorkerID       string `redis:"worker_id"`
	Description    string `redis:"description"`
	FencingToken   int64  `redis:"fencing_token"`
	MaxRetries     int64  `redis:"max_retries"`
	Attempts       int64  `redis:"attempts"`
	IdempotencyKey string `redis:"idempotency_key"`
}

type QueueConfig struct {
	Name  string
	Redis redis.UniversalClient
}

type Queue struct {
	Name   string
	Config QueueConfig
}

var IDEMPOTENCY_KEY_TTL = time.Hour * 24

var enqueueScript = redis.NewScript(
	`local pending = KEYS[1]
	local job = KEYS[2]
	local notifications = KEYS[3]
	local idempotency_key = KEYS[4]
	local job_id = ARGV[1]
	local job_desc = ARGV[2]
	local job_fencing_token = ARGV[3]
	local job_retries = ARGV[4]
	local job_worker_id = ARGV[5]
	local job_attempts = ARGV[6]
	local idempotency_ttl = ARGV[7]

	-- Serialize idempotency lookup, job creation and key reservation in one
	-- Redis operation so concurrent enqueue attempts cannot both succeed.
	if idempotency_key ~= '' then
		local existing_job_id = redis.call("GET", idempotency_key)
		if existing_job_id then
			-- A retry gets the result of the original enqueue operation.
			return "duplicate:" .. existing_job_id
		end
	end

	if redis.call("EXISTS", job) == 1 then
		return "exists"
	end

	redis.call("HSET", job, "id", job_id, "description", job_desc, "fencing_token", job_fencing_token, "worker_id", job_worker_id, "attempts", job_attempts, "max_retries", job_retries)

	redis.call("LPUSH", pending, job_id)

	redis.call("LPUSH", notifications, job_id)

	if idempotency_key ~= '' then
		redis.call("SET", idempotency_key, job_id, "EX", idempotency_ttl)
	end

	return "created:" .. job_id
`)

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

func (q *Queue) Add(ctx context.Context, job Job) (jobId string, err error) {

	jobID := uuid.NewString()

	if job.Description == "" {
		return "", errors.New("xeyqor: job description is required")
	}

	if job.WorkerID != "" {
		return "", fmt.Errorf("xeyqor: job to enqueue should have no worker id: %s", job.WorkerID)
	}

	if job.FencingToken < 0 {
		return "", fmt.Errorf("xeyqor: invalid fencing token: %d", job.FencingToken)
	}

	if job.MaxRetries < 0 {
		return "", fmt.Errorf("xeyqor: invalid maximum job retries: %d", job.MaxRetries)
	}

	if job.Attempts != 0 {
		return "", fmt.Errorf("xeyqor: new job must start at 0 attempts %d", job.Attempts)
	}

	prefix := "xeyqor:{" + q.Name + "}:"

	idempotencyKey := ""
	if job.IdempotencyKey != "" {
		idempotencyKey = prefix + "idempotency:" + job.IdempotencyKey
	}

	keys := []string{
		prefix + "pending",
		prefix + "job:" + jobID,
		prefix + "notifications",
		idempotencyKey,
	}

	result, err := enqueueScript.Run(
		ctx,
		q.Config.Redis,
		keys,
		jobID,
		job.Description,
		job.FencingToken,
		job.MaxRetries,
		job.WorkerID,
		job.Attempts,
		int64(IDEMPOTENCY_KEY_TTL.Seconds()),
	).Result()

	if err != nil {
		return "", err
	}

	status, ok := result.(string)
	if !ok {
		return "", errors.New("xeyqor: unexpected Lua script result type")
	}

	switch {
	case strings.HasPrefix(status, "created:"):
		return strings.TrimPrefix(status, "created:"), nil
	case strings.HasPrefix(status, "duplicate:"):
		return strings.TrimPrefix(status, "duplicate:"), nil
	case status == "exists":
		return "", errors.New("xeyqor: job already exists in queue")
	default:
		return "", fmt.Errorf("xeyqor: unexpected Lua script result: %q", status)
	}
}
