package xeyqor_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/lungtav/xeyqor"
	"github.com/redis/go-redis/v9"
)

func TestQueueAddIsIdempotent(t *testing.T) {
	ctx := t.Context()
	mr := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { _ = rdb.Close() })
	emails, err := xeyqor.NewQueue(ctx, xeyqor.QueueConfig{
		Name:  "emails",
		Redis: rdb,
	})

	if err != nil {
		t.Fatalf("queue error %v", err)
		return
	}

	first, err := emails.Add(ctx, xeyqor.Job{
		Description:    "email:send",
		IdempotencyKey: "345",
	})

	if err != nil {
		t.Fatalf("first add %v", err)
		return
	}

	second, err := emails.Add(ctx, xeyqor.Job{
		Description:    "email:send",
		IdempotencyKey: "345",
	})

	if err != nil {
		t.Fatalf("second add %v", err)
		return
	}

	if first != second {
		t.Fatalf("duplicate add returned %q, want %q", second, first)
		return
	}

	pending, err := rdb.LRange(ctx, "xeyqor:{emails}:pending", 0, -1).Result()

	if err != nil {
		t.Fatalf("read pending %v", err)
		return
	}

	if len(pending) != 1 || pending[0] != first {
		t.Fatalf("pending = %v, want [%s]", pending, first)
	}
}
