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

	var start time.Time
	tick.Flow("demo/translate", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)

		src, _ := ctx.MetaData("src")

		//log.Printf("-- to_en")
		toEnF := gotick.Async(ctx, "to_en", func(ctx *gotick.TaskContext) (string, error) {
			log.Printf("[%s] execing to_en", time.Since(start))
			time.Sleep(2 * time.Second)

			if ctx.Retry < 5 {
				log.Printf("[%s] exec to_en, retry:%v", time.Since(start), ctx.Retry)
				return "", fmt.Errorf("retry")
			} else {
				log.Printf("[%s] exec to_en success", time.Since(start))
			}

			return fmt.Sprintf("en(%s)", src), nil
		})

		//log.Printf("-- to_jp")
		lenF := gotick.Async(ctx, "token_len", func(ctx *gotick.TaskContext) (int, error) {
			log.Printf("[%s] execing token_len", time.Since(start))

			time.Sleep(2 * time.Second)
			if ctx.Retry < 5 {
				log.Printf("[%s] exec token_len, retry:%v", time.Since(start), ctx.Retry)
				return 0, fmt.Errorf("retry")
			} else {
				log.Printf("[%s] exec token_len success", time.Since(start))
			}
			return len(src), nil
		})

		gotick.Wait(ctx, 2, toEnF, lenF)

		gotick.Task(ctx, "save", func(ctx *gotick.TaskContext) error {
			log.Printf("[%s] save result: en: %v, len:%v", time.Since(start), toEnF.Value(), lenF.Value())
			return nil
		})

		if ctx.CallId == currentCallId {
			c()
		}
		return nil
	}).Fail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		log.Printf("[%s] fail %+v", time.Since(start), ts.Errs)
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

	start = time.Now()
	callId, err := tick.Trigger(ctx, "demo/translate", map[string]string{"src": "bysir"})
	if err != nil {
		log.Fatal(err)
	}
	currentCallId = callId

	log.Printf("Triggered callid: %v", callId)

	wg.Wait()
}
