package store

import (
	"context"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	r := NewRedisStore(rdb)
	ctx := context.Background()
	err := r.Set(ctx, "test", "test", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	var v string
	exist, err := r.Get(ctx, "test", &v)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v %v", exist, v)

	exist, err = r.Get(ctx, "test2", &v)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v %v", exist, v)
}
