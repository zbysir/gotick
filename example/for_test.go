package example

import (
	"errors"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"sync"
	"testing"
	"time"
)

// - UseStatus: 和 React Hooks 一样，它用于保存状态。
// - Memo: 缓存数据，当数据不存在时会执行 build 方法。
// - Array: 和 Memo 类似用于缓存数据，不过如果想构建一个循环流程的话应该使用 Array，它提供更合适的 API。

func TestFor(t *testing.T) {
	tick := gotick.NewTickServer(gotick.Options{KvStore: store.NewMockNodeStatusStore(), DelayedQueue: store.NewMockRedisDelayedQueue()})
	ctx, c := signal.NewContext()
	var currentCallId string

	tick.Flow("demo/close-order", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)
		startAt := gotick.Memo(ctx, "start_at", func() (time.Time, error) {
			return time.Now(), nil
		})
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			log.Printf("[%v] start at %v", ctx.CallId, time.Now())
			return nil
		})

		// 生成 10 个任务
		tasks := gotick.Array(ctx, "gen-tasks", func() ([]string, error) {
			log.Printf("call gen-tasks at %v", time.Now().Sub(startAt))
			return []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, nil
		})

		for _, task := range tasks {
			//log.Printf("key: %+v", key)
			gotick.Task(ctx, task.Key("send-email"), func(ctx *gotick.TaskContext) error {
				if task.Value() == "f" {
					// 模拟错误 5 次后成功
					log.Printf("[%v] send email to '%v' error, retrying %v", ctx.CallId, task.Value(), ctx.Retry)
					if ctx.Retry < 5 {
						return errors.New("err")
					}
				}
				log.Printf("[%v] send email to '%v' success at %v", ctx.CallId, task.Value(), time.Now().Sub(startAt))
				return nil
			})
		}

		gotick.Task(ctx, "done", func(ctx *gotick.TaskContext) error {
			log.Printf("done at %v", time.Now().Sub(startAt))
			return nil
		})

		if ctx.CallId == currentCallId {
			c()
		}
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		if ctx.CallId == currentCallId {
			c()
		}
		log.Printf("BreakFail: %+v", ts)
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

func TestSequence(t *testing.T) {
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
