package example

import (
	"github.com/go-redis/redis/v8"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"sync"
	"testing"
	"time"
)

func TestCloseOrder(t *testing.T) {
	url := "redis://localhost:6379/0"
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)

	tick := gotick.NewTickServer(gotick.Options{RedisURL: url, DelayedQueue: store.NewStdRedisDelayedQueue(redisClient)})
	ctx, c := signal.NewContext()
	var currentCallId string

	tick.Flow("demo/close-order", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)
		startAt, _ := gotick.UseStatus(ctx, "start_at", time.Now())
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			log.Printf("start at %v", time.Now())
			return nil
		})
		gotick.Sleep(ctx, "wait-email", 1*time.Second)

		gotick.Task(ctx, "send-email", func(ctx *gotick.TaskContext) error {
			log.Printf("send email at %v", time.Now().Sub(startAt))
			return nil
		})

		gotick.Sleep(ctx, "wait-close", 3*time.Second)

		gotick.Task(ctx, "close-order", func(ctx *gotick.TaskContext) error {
			log.Printf("close-order at %v", time.Now().Sub(startAt))
			return nil
		})

		if ctx.CallId == currentCallId {
			c()
		}
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tick.StartServer(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}()

	callId, err := tick.Trigger(ctx, "demo/close-order", map[string]string{"name": "bysir"})
	if err != nil {
		t.Fatal(err)
	}
	currentCallId = callId

	t.Logf("Triggered callid: %v", callId)

	wg.Wait()

}
