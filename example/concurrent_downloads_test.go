package example

import (
	"testing"
)

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
