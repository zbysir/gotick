package example

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
)

var mockTick = gotick.NewTickServer(gotick.Config{
	KvStore:      store.NewMockNodeStatusStore(),
	DelayedQueue: store.NewMockRedisDelayedQueue(),
	ListenAddr:   "",
})

func TestServer(t *testing.T) {
	tick := mockTick
	ctx, c := signal.NewContext()

	defer c()

	tick.Flow("demo/close-order", func(ctx *gotick.Context) {
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
		return
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

func TestFail(t *testing.T) {
	tick := mockTick
	ctx, c := signal.NewContext()

	defer c()

	tick.Flow("demo/close-order", func(ctx *gotick.Context) {
		t.Logf("running: %v, %v", ctx.MetaDataAll(), time.Now())
		gotick.Task(ctx, "text", func(ctx *gotick.TaskContext) error {
			return fmt.Errorf("test error")
		})
		return
	}).OnError(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Logf("OnError: %v", ts)
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Logf("OnFail: %v", ts)
		c()
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

	id, err := tick.Trigger(ctx, "demo/close-order", map[string]string{"name": "bysir"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v", id)

	wg.Wait()
}

func TestSuccess(t *testing.T) {
	tick := mockTick
	ctx, c := signal.NewContext()

	defer c()

	tick.Flow("demo/close-order", func(ctx *gotick.Context) {
		gotick.Task(ctx, "t1", func(ctx *gotick.TaskContext) error {
			t.Logf("running t1: %v, %v", ctx.MetaDataAll(), time.Now())
			return nil
		})
		gotick.Task(ctx, "t2", func(ctx *gotick.TaskContext) error {
			t.Logf("running t2: %v, %v", ctx.MetaDataAll(), time.Now())
			return nil
		})
		return
	}).OnError(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Logf("OnError: %v", ts)
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Logf("OnFail: %v", ts)
		c()
		return nil
	}).OnSuccess(func(ctx *gotick.Context) error {
		t.Logf("OnSuccess")
		c()
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

	id, err := tick.Trigger(ctx, "demo/close-order", map[string]string{"name": "bysir"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v", id)

	wg.Wait()
}
