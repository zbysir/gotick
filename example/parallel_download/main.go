package main

import (
	"fmt"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"strings"
	"sync"
	"time"
)

func main() {
	tick := gotick.NewTickServer(gotick.Options{KvStore: store.NewMockNodeStatusStore(), DelayedQueue: store.NewMockRedisDelayedQueue()})
	ctx, c := signal.NewContext()
	var currentCallId string

	var start time.Time
	tick.Flow("demo/download", func(ctx *gotick.Context) error {
		src, _ := ctx.MetaData("src")

		tasks := gotick.Array(ctx, "split", func() ([]string, error) {
			return strings.Split(src, ""), nil
		})

		fs := gotick.AsyncArray(ctx, "download", tasks, func(ctx *gotick.TaskContext, v string) (string, error) {
			log.Printf("[%s] execing download(%v)", time.Since(start), v)
			time.Sleep(2 * time.Second)
			return fmt.Sprintf("download(%s)", v), nil
		})

		gotick.Wait(ctx, 4, fs...)

		gotick.Task(ctx, "save", func(ctx *gotick.TaskContext) error {
			for index, f := range fs {
				v := f.(*gotick.FutureT[string]).Value()
				log.Printf("[%s] save result: %v %v", time.Since(start), index, v)
			}
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
	callId, err := tick.Trigger(ctx, "demo/download", map[string]string{"src": "bysir"})
	if err != nil {
		log.Fatal(err)
	}
	currentCallId = callId

	log.Printf("Triggered callid: %v", callId)

	wg.Wait()
}
