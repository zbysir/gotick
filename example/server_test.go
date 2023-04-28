package example

import (
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"sync"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	tick := gotick.NewTickServer(gotick.Options{
		KvStore:      store.NewMockNodeStatusStore(),
		DelayedQueue: store.NewMockRedisDelayedQueue(),
		ListenAddr:   ":8080",
	})
	ctx, c := signal.NewContext()

	defer c()

	tick.Flow("demo/close-order", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)
		startAt := gotick.Memo(ctx, "start_at", func() (time.Time, error) {
			return time.Now(), nil
		})
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			log.Printf("start at %v", time.Now())
			return nil
		})

		// 生成 100 个任务
		seq := gotick.Sequence(ctx, "gen-tasks", 10)

		for seq.Next() {
			gotick.Task(ctx, seq.TaskKey("send-email"), func(ctx *gotick.TaskContext) error {
				time.Sleep(1 * time.Second / 2)
				log.Printf("send email to '%v' at %v", seq.Current, time.Now().Sub(startAt))
				return nil
			})
		}

		gotick.Task(ctx, "done", func(ctx *gotick.TaskContext) error {
			log.Printf("done at %v", time.Now().Sub(startAt))
			return nil
		})

		log.Printf("meta %+v", ctx.MetaDataAll())
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

	wg.Wait()
}