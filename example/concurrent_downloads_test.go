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
// - UseMemo: 缓存数据，当数据不存在时会执行 build 方法。
// - UseArray: 和 UseMemo 类似用于缓存数据，不过如果想构建一个循环流程的话应该使用 UseArray，它提供更合适的 API。

func TestFor(t *testing.T) {
	tick := gotick.NewTickServer(gotick.Options{KvStore: store.NewMockNodeStatusStore(), DelayedQueue: store.NewMockRedisDelayedQueue()})
	ctx, c := signal.NewContext()
	var currentCallId string

	tick.Flow("demo/close-order", func(ctx *gotick.Context) error {
		//log.Printf("schedule callId: %v", ctx.CallId)
		startAt := gotick.UseMemo(ctx, "start_at", func() (time.Time, error) {
			return time.Now(), nil
		})
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			log.Printf("[%v] start at %v", ctx.CallId, time.Now())
			return nil
		})

		// 生成 10 个任务
		tasks := gotick.UseArray(ctx, "gen-tasks", func() ([]string, error) {
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
	}).Fail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		if ctx.CallId == currentCallId {
			c()
		}
		log.Printf("Fail: %+v", ts)
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
		startAt := gotick.UseMemo(ctx, "start_at", func() (time.Time, error) {
			return time.Now(), nil
		})
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			log.Printf("start at %v", time.Now())
			return nil
		})

		// 生成 100 个任务
		seq := gotick.UseSequence(ctx, "gen-tasks", 10)

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

//func TestName(t *testing.T) {
//	// 链式, 每个方法都只会调用一次
//	// task("", func).task("", func)
//
//	// 循环，可能会调用多次，你需要再 func 中控制循环结束、循环进度。
//	// loop("", func)
//
//	// 并发，方法会并发的调用多次，可以指定并发数量
//	// 在 beforeFunc 中需要生成全部的并发任务，beforeFunc 只会调用一次
//	// concurrent("", beforeFunc, jobFunc, 2)
//
//	// 动态生成
//	// product("down-load", productFunc, jobFunc, 2)
//
//	var beforeFunc = func() []string {
//		// 生成 10 个并发任务，会依次执行。
//		return []string{`{"id": 1}`, `{"id": 2}`, `{"id": 3}`, `{"id": 4}`, `{"id": 5}`, `{"id": 6}`}
//	}
//
//	var jobFunc = func(ctx context.Context) error {
//		concurrent := gotick.GetCallId(ctx)
//
//		// do something
//	}
//}

func TestName2(t *testing.T) {
	//os.Setenv("GOTICK_DEBUG", "debug")

}

//func TestName1(t *testing.T) {
//
//	// 使用 golang 控制流将更灵活。
//	var h = func(ctx context.Context) error {
//		err = gotick.Then(ctx, "2", func() {
//			// do something
//		})
//		// 只有当任务执行过（而不是已经执行），才需要 break
//		// 如果不 break 也行，程序将运行到下一个 break point
//		// 如果整个 flow 都没有 bread point，则这个 flow 将只会在这一个机器上运行，而不会调度到其他机器上。
//		// 如果你想让 task 并发执行，则必须 break 让 gotick 并行调度。
//		// 可以使用 panic 来 break
//		if err != nil {
//			// break
//			return
//		}
//
//		gotick.Sleep(30 * time.Minute)
//
//		gotick.Task(ctx, "1", func() {
//			// do something
//
//		})
//
//		// break
//		// 当 task 在调度中则不会再重复返回，因为支持并发
//		// 当 task 执行失败，则会再次返回
//		product := gotick.Array(ctx, "product", func() []string {
//			// 生成 10 个并发任务，会依次执行。
//			return []string{`{"id": 1}`, `{"id": 2}`, `{"id": 3}`, `{"id": 4}`, `{"id": 5}`, `{"id": 6}`}
//		})
//
//		for _, item := range product {
//			gotick.Task(ctx, item, func() {
//				// 如果一个 task 有人在运行了，则会跳过这个 task（不重复调度）。
//				// 如果一个 task 执行失败了，则需要重试，重试逻辑是指定运行具名 task，跳过名字不匹配的 task
//
//				// do something
//
//			})
//			// break
//		}
//
//		gotick.Task(ctx, "end", func() {
//			// do something
//		})
//
//		// 死循环，实际上就是生成一个递增序列
//		x := gotick.Loop(ctx, "loop")
//
//		v, setV := UseStatus(ctx, "index", 1)
//		for x.Next() {
//			setV(v + 1)
//		}
//
//	}
//}
