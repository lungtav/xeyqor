package xeyqor_test

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/lungtav/xeyqor"
	"github.com/redis/go-redis/v9"
)

func TestNewQueueAndEnqueue(t *testing.T) {
	ctx:= t.Context()
	mr:=  miniredis.RunT(t)

	rdb:= redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	emails, err:= xeyqor.NewQueue(ctx, xeyqor.QueueConfig{
		Name: "emails",
		Redis: rdb,
	} )

	if err!=nil{
		t.Fatalf("queue error %v", err)
		return
	}

	if _, err:= emails.Add(ctx, xeyqor.Job{
		Description: "email:send",
		IdempotencyKey: "345",
	});err!=nil{
		t.Fatalf("enqueue error %v", err)
		return
	}

	if _, err:= emails.Add(ctx, xeyqor.Job{
		Description: "email:send",
		IdempotencyKey: "345",
	});err!=nil{
		t.Fatalf("enqueue error %v", err)
		return
	}

	jobList:=rdb.LRange(ctx, "xeyqor:{emails}:pending", 0, -1)

	if err:=jobList.Err(); err!=nil{
		t.Fatalf("failed to retrieve job list %v", err)
		return
	}

	fmt.Print(jobList.Val())
	t.Cleanup(func() {_ =rdb.Close()})
}