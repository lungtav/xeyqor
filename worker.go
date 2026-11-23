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

