package xeyqor_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/lungtav/xeyqor"
	"github.com/redis/go-redis/v9"
)

func TestNewWorker(t *testing.T) {
	mr := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { _ = rdb.Close() })

	workerHandler := func(ctx context.Context, job xeyqor.Job) error {
		return nil
	}

	tests := []struct {
		name              string
		cfg               xeyqor.WorkerConfig
		concurrencyWanted int64
		wantErr           bool
	}{
		{
			name: "empty name string", cfg: xeyqor.WorkerConfig{Name: "", Redis: rdb, Handler: workerHandler, Concurrency: 1}, concurrencyWanted: 0, wantErr: true},
		{
			name: "no handler", cfg: xeyqor.WorkerConfig{Name: "emails", Redis: rdb, Concurrency: 1}, concurrencyWanted: 0, wantErr: true,
		},
		{
			name: "negative concurrency", cfg: xeyqor.WorkerConfig{Name: "emails", Redis: rdb, Handler: workerHandler, Concurrency: -2}, concurrencyWanted: 0, wantErr: true,
		},
		{
			name: "no name", cfg: xeyqor.WorkerConfig{Redis: rdb, Handler: workerHandler, Concurrency: 1}, concurrencyWanted: 0, wantErr: true,
		},
		{
			name: "default concurrency", cfg: xeyqor.WorkerConfig{Name: "emails", Redis: rdb, Handler: workerHandler}, concurrencyWanted: 1, wantErr: false,
		},
		{
			name: "no redis", cfg: xeyqor.WorkerConfig{Name: "no redis", Handler: workerHandler, Concurrency: 1}, concurrencyWanted: 0, wantErr: true,
		},
		{
			name: "configured", cfg: xeyqor.WorkerConfig{Name: "emails", Redis: rdb, Handler: workerHandler, Concurrency: 4}, concurrencyWanted: 4, wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker, err := xeyqor.NewWorker(t.Context(), tt.cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}

			if err != nil {
				t.Fatalf("NewWorker: %v", err)
			}

			if worker.Concurrency != tt.concurrencyWanted {
				t.Fatalf("concurrency wanted %d, concurency got %d", tt.concurrencyWanted, worker.Concurrency)
				return
			}
		})
	}
}
