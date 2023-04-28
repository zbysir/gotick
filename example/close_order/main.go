package main

import (
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"sync"
	"time"
)

func main() {
	tick := gotick.NewTickServer(gotick.Options{KvStore: store.NewMockNodeStatusStore(), DelayedQueue: store.NewMockRedisDelayedQueue()})
	ctx, c := signal.NewContext()
	var currentCallId string

	tick.Flow("demo/close-order", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)
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
		return nil
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
