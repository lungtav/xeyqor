package xeyqor_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/lungtav/xeyqor"
	"github.com/redis/go-redis/v9"
)

func TestNewWorker(t *testing.T) {
	ctx:= t.Context()
	mr:= miniredis.RunT(t)

	rdb:= redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	_, err:=xeyqor.NewWorker(ctx, "email-queue", rdb)

	if err!=nil{
		t.Fatalf("worker error %d", err)
		return
	}
}