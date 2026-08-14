// Command uidemo 触发几个形态各异的 flow，用来填充数据看 UI 的效果。
//
//	docker run -d -p 6379:6379 redis:7-alpine
//	go run ./example/uidemo -redis redis://localhost:6379/0
//	go run ./cmd/gotick ui -redis redis://localhost:6379/0
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zbysir/gotick"
)

func main() {
	redisURL := flag.String("redis", "redis://localhost:6379/0", "Redis connection URL")
	flag.Parse()

	tick, err := gotick.NewServerFromConfig(gotick.Config{
		RedisURL:          *redisURL,
		Concurrency:       10,
		TaskCheckInterval: 50 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	registerFlows(tick)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := tick.StartServer(ctx); err != nil {
			log.Printf("server stopped: %v", err)
		}
	}()

	// 等调度器起来
	time.Sleep(300 * time.Millisecond)

	trigger := func(flow string, meta gotick.MetaData) {
		callId, err := tick.Trigger(ctx, flow, meta)
		if err != nil {
			log.Printf("trigger %s: %v", flow, err)
			return
		}
		log.Printf("triggered %-22s %s", flow, callId)
	}

	trigger("demo/checkout", gotick.MetaData{"order_id": "ORD-88231", "user": "bysir"})
	trigger("demo/checkout", gotick.MetaData{"order_id": "ORD-88232", "user": "alice"})
	trigger("demo/flaky-email", gotick.MetaData{"to": "someone@example.com"})
	trigger("demo/broken-payment", gotick.MetaData{"order_id": "ORD-99001"})
	trigger("demo/parallel-fetch", gotick.MetaData{"batch": "2026-08-14"})
	trigger("demo/slow-drip", gotick.MetaData{"note": "这个会跑一分钟，用来看 running 状态"})

	fmt.Fprintln(os.Stderr, "\n触发完毕。现在另开一个终端运行：")
	fmt.Fprintf(os.Stderr, "  go run ./cmd/gotick ui -redis %s\n\n", *redisURL)
	fmt.Fprintln(os.Stderr, "Ctrl-C 退出。")

	<-ctx.Done()
	// 给正在跑的任务一点时间收尾
	time.Sleep(500 * time.Millisecond)
}

func registerFlows(tick *gotick.Server) {
	// 一个正常走完的流程，带 sleep 和缓存结果。
	tick.Flow("demo/checkout", func(ctx *gotick.Context) {
		orderId, _ := ctx.MetaData("order_id")

		total := gotick.Memo(ctx, "calc-total", func() (int, error) {
			time.Sleep(120 * time.Millisecond)
			return 4299, nil
		})

		gotick.Task(ctx, "reserve-stock", func(ctx *gotick.TaskContext) error {
			time.Sleep(200 * time.Millisecond)
			log.Printf("[%s] reserved stock", orderId)
			return nil
		})

		gotick.Sleep(ctx, "wait-for-payment", 2*time.Second)

		gotick.Task(ctx, "capture-payment", func(ctx *gotick.TaskContext) error {
			time.Sleep(300 * time.Millisecond)
			log.Printf("[%s] captured %d cents", orderId, total)
			return nil
		})

		gotick.Task(ctx, "send-receipt", func(ctx *gotick.TaskContext) error {
			time.Sleep(150 * time.Millisecond)
			return nil
		})
	})

	// 前几次失败、最后成功，用来看重试计数和错误信息。
	tick.Flow("demo/flaky-email", func(ctx *gotick.Context) {
		gotick.Task(ctx, "render-template", func(ctx *gotick.TaskContext) error {
			time.Sleep(80 * time.Millisecond)
			return nil
		})

		gotick.Task(ctx, "smtp-send", func(ctx *gotick.TaskContext) error {
			if ctx.Retry < 3 {
				return fmt.Errorf("smtp: connection reset (attempt %d)", ctx.Retry+1)
			}
			log.Printf("email sent after %d retries", ctx.Retry)
			return nil
		}, gotick.WithMaxRetry(5))
	})

	// 一定会失败的流程，用来看 failed 状态和「死在哪一步」。
	tick.Flow("demo/broken-payment", func(ctx *gotick.Context) {
		gotick.Task(ctx, "validate", func(ctx *gotick.TaskContext) error {
			time.Sleep(60 * time.Millisecond)
			return nil
		})

		gotick.Task(ctx, "charge-card", func(ctx *gotick.TaskContext) error {
			return errors.New("card declined: insufficient funds")
		}, gotick.WithMaxRetry(2))

		gotick.Task(ctx, "never-reached", func(ctx *gotick.TaskContext) error {
			log.Print("BUG: 这一步不该被执行")
			return nil
		})
	})

	// 并行扇出，用来看多个任务的耗时对比。
	tick.Flow("demo/parallel-fetch", func(ctx *gotick.Context) {
		sources := gotick.Array(ctx, "list-sources", func(ctx *gotick.TaskContext) ([]string, error) {
			return []string{"orders", "users", "inventory", "shipping", "returns"}, nil
		})

		fs := gotick.AsyncArray(ctx, "fetch", sources,
			func(ctx *gotick.TaskContext, name string, index int) (int, error) {
				// 故意让耗时差别明显，好在 UI 上看出长短
				time.Sleep(time.Duration(150+index*220) * time.Millisecond)
				return (index + 1) * 137, nil
			})

		gotick.Wait(ctx, 3, fs...)

		gotick.Task(ctx, "merge", func(ctx *gotick.TaskContext) error {
			sum := 0
			for _, f := range fs {
				sum += f.(*gotick.FutureT[int]).Value()
			}
			log.Printf("merged total: %d", sum)
			return nil
		})
	})

	// 慢慢跑一分钟，用来观察 running 状态和自动刷新。
	tick.Flow("demo/slow-drip", func(ctx *gotick.Context) {
		seq := gotick.Sequence(ctx, "drip", 6)
		for seq.Next() {
			step := seq.Current
			gotick.Task(ctx, seq.TaskKey("drip"), func(ctx *gotick.TaskContext) error {
				log.Printf("drip step %d", step)
				return nil
			})
			gotick.Sleep(ctx, fmt.Sprintf("pause:%d", step), 10*time.Second)
		}
	})
}
