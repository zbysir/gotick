package main

import (
	"log"
	"sync"
	"time"

	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/store"
)

func main() {
	// 这个例子用内存实现，无需 Redis 即可运行。
	// 生产环境请改用:
	//   tick, err := gotick.NewServerFromConfig(gotick.Config{RedisURL: "redis://localhost:6379/0"})
	tick := gotick.NewServer(gotick.NewServerParams{
		DelayedQueue: store.NewMockRedisDelayedQueue(),
		KVStore:      store.NewMockKvStore(),
	})
	ctx, c := signal.NewContext()
	var currentCallId string

	tick.Flow("demo/close-order", func(ctx *gotick.Context) {
		startAt := gotick.Memo(ctx, "start_at", func() (time.Time, error) {
			return time.Now(), nil
		})
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			log.Printf("start at %v", time.Now())
			return nil
		})
		gotick.Sleep(ctx, "wait-email", 1*time.Second)

		gotick.Task(ctx, "send-email", func(ctx *gotick.TaskContext) error {
			log.Printf("send email at %v {%v}", time.Now().Sub(startAt), ctx.MetaDataAll())
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
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tick.StartServer(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	callId, err := tick.Trigger(ctx, "demo/close-order", map[string]string{"name": "bysir"})
	if err != nil {
		log.Fatal(err)
	}
	currentCallId = callId

	log.Printf("Triggered callid: %v", callId)

	wg.Wait()
}
