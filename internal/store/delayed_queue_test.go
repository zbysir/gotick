package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

func TestStdRedisDelayedQueue(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	q := NewStdRedisDelayedQueue(rdb)

	go func() {
		err := q.Start(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	q.Subscribe("t1", func(ctx context.Context, data string) error {
		t.Logf("t1 receive: %v", data)
		return nil
	})

	err := q.Publish(context.Background(), "t1", "test1", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	q.Subscribe("t2", func(ctx context.Context, data string) error {
		t.Logf("t2 receive: %v", data)
		return nil
	})

	err = q.Publish(context.Background(), "t2", "test2", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)
}

func TestAsynqQueue(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	q := NewAsynq(rdb)

	go func() {
		t.Logf("staring")
		err := q.Start(context.Background())
		t.Logf("ended")
		if err != nil {
			t.Fatal(err)
		}
	}()

	q.Subscribe("t2", func(ctx context.Context, data string) error {
		t.Logf("t2 receive: %v at %v", data, time.Now())
		return nil
	})

	q.Subscribe("t3", func(ctx context.Context, data string) error {
		t.Logf("t3 receive : %v at %v", data, time.Now())
		return nil
	})

	err := q.Publish(context.Background(), "t2", "test1", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = q.Publish(context.Background(), "t3", "test2", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)
}
