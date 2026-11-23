package xeyqor

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Handler func()

type Worker struct {
	name  string // must match the name of the queue its listening to
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
		name: name,
		redisClient: redisClient,
	}, nil
}

func (w *Worker) Claim(ctx context.Context, workerID string) (Job, error) {
	for {
		prefix := "xeyqor:{" + w.name + "}:"
		keys:= []string{prefix + "leases", prefix + "pending", prefix + "running"}

		result, err := claimScript.Run(ctx, w.redisClient, keys, workerID).Result()

		if err != nil {
			return Job{}, err
		}

		if result == nil {
			_, err := w.redisClient.BLPop(ctx, 0, "veyqor:notifications").Result()

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

		jobKey := "veyqor:job:" + jobID
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