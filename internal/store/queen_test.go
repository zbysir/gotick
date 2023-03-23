package store

import (
	"context"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func TestNewRedisAsyncQueen(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	q := NewRedisAsyncQueen(rdb, "1")
	q.Start(context.Background())

	q.Subscribe(func(data string) error {
		t.Logf("data: %v", data)
		return nil
	})

	e, _ := q.Exist([]string{"test1", "test2"})
	t.Logf("exist: %v", e)

	//err := q.Publish("test1", 3*time.Second)
	//if err != nil {
	//	t.Fatal(err)
	//}

	time.Sleep(2 * time.Second)
}
