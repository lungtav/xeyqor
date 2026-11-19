package xeyqor

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type QueueConfig struct{
	Name string
	Redis redis.UniversalClient
}

type Queue struct{
	Name string
	Config QueueConfig
}

func NewQueue(ctx context.Context, cfg QueueConfig) (*Queue, error){

	if cfg.Name == ""{
		return nil, errors.New("xeyqor: queue name is required")
	}

	if cfg.Redis == nil{
		return nil, errors.New("xeyqor: redis client is required")
	}

	if err:= cfg.Redis.Ping(ctx).Err(); err!=nil{
		return nil, errors.New("xeyqor: redis client invalid or connection refused")
	}

	return &Queue{
		Name: cfg.Name,
		Config: cfg,
	}, nil
}