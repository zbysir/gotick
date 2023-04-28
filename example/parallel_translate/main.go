package main

import (
	"fmt"
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

	tick.Flow("demo/translate", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)

		src, _ := ctx.MetaData("src")

		//log.Printf("-- to_en")
		toEnF := gotick.Async(ctx, "to_en", func(ctx *gotick.TaskContext) (string, error) {
			//if ctx.Retry < 2 {
			//	log.Printf("exec to_en, retry:%v", ctx.Retry)
			//	return "", fmt.Errorf("retry")
			//}
			time.Sleep(2 * time.Second)
			return fmt.Sprintf("en(%s)", src), nil
		})

		//log.Printf("-- to_jp")
		lenF := gotick.Async(ctx, "token_len", func(ctx *gotick.TaskContext) (int, error) {
			//if ctx.Retry < 2 {
			//	log.Printf("exec to_jp, retry:%v", ctx.Retry)
			//	return 0, fmt.Errorf("retry")
			//}
			time.Sleep(1 * time.Second)
			return len(src), nil
		})

		log.Printf("-- wait")
		gotick.Wait(ctx, toEnF, lenF)

		gotick.Task(ctx, "save", func(ctx *gotick.TaskContext) error {
			log.Printf("save result: en: %v, len:%v", toEnF.Value(), lenF.Value())
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

	callId, err := tick.Trigger(ctx, "demo/translate", map[string]string{"src": "bysir"})
	if err != nil {
		log.Fatal(err)
	}
	currentCallId = callId

	log.Printf("Triggered callid: %v", callId)

	wg.Wait()
}
