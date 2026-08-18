<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/logo-wordmark-dark.svg">
  <img src="./assets/logo-wordmark.svg" alt="gotick" width="260">
</picture>

[![CI](https://github.com/zbysir/gotick/actions/workflows/ci.yml/badge.svg)](https://github.com/zbysir/gotick/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zbysir/gotick.svg)](https://pkg.go.dev/github.com/zbysir/gotick)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

[English](./readme.md) · **简体中文** · [文档](https://gotick.bysir.top/docs)

用你已经有的那个 Redis，把跨越时间的流程跑稳。

三十分钟后关掉未支付的订单，注册三天后给用户发一封邮件——这些流程活得比启动它
的进程更久。gotick 让你把它们照着念的顺序写下来，而重启、崩溃、发版都不会让它
从头再来。

不需要部署任何服务，只依赖你已经有的 Redis。

## 安装

```bash
go get github.com/zbysir/gotick
```

## 先看代码

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/zbysir/gotick"
)

func main() {
	tick, err := gotick.NewServerFromConfig(gotick.Config{
		RedisURL: "redis://localhost:6379/0",
	})
	if err != nil {
		log.Fatal(err)
	}

	// flow 就是一个普通的 Go 函数。你不调用它，你触发它。
	tick.Flow("order/close", func(ctx *gotick.Context) {
		gotick.Task(ctx, "create-order", func(c *gotick.TaskContext) error {
			log.Println("订单已创建")
			return nil
		})

		// 这不是 time.Sleep。等待期间不占任何进程，
		// 在这里把进程重启掉，流程照样会按时醒来。
		gotick.Sleep(ctx, "wait-payment", 30*time.Second)

		gotick.Task(ctx, "close-order", func(c *gotick.TaskContext) error {
			log.Println("订单已关闭")
			return nil
		})
	})

	ctx := context.Background()
	if _, err := tick.Trigger(ctx, "order/close", nil); err != nil {
		log.Fatal(err)
	}

	log.Fatal(tick.StartServer(ctx))
}
```

## 只有一件事必须先弄明白

gotick 没有把流程编译成状态机，而是**每次调度都从头重新执行你的函数**，已经做完
的步骤靠存在 Redis 里的状态被跳过。一个四步的流程，函数体至少会被执行五次。

所以 `Task` 里面的代码只跑一次，`Task` 外面的代码要跑很多次——而且每次都得走到
同一个判断上：

```go
tick.Flow("demo", func(ctx *gotick.Context) {
	// ✗ 每次拿到的时间都不一样，两次执行可能走进不同分支
	if time.Now().Hour() > 12 { ... }

	// ✗ 每次都会真的查一次库，而且结果可能变了
	user, _ := db.GetUser(id)

	// ✓ Memo 只查一次，后面每次都读回同一个结果
	user := gotick.Memo(ctx, "user", func() (User, error) {
		return db.GetUser(id)
	})
})
```

一句话：**有副作用的、结果可能变的，都放进 `Task` / `Memo` / `Array`。** 函数体
本身只负责决定顺序。

## 全部 API

| 在流程里 | |
| --- | --- |
| `Task` | 执行一步，失败会重试。 |
| `Memo` | 执行一步并记住结果。 |
| `Sleep` | 等一会儿——几秒或者几天，期间不占进程。 |
| `WaitForSignal` | 等外面发来一个信号。 |
| `Async` + `Wait` | 并行执行，可以限并发。 |
| `Array` | 记住一个列表，然后按它展开。 |

| 从外面 | |
| --- | --- |
| `Trigger` | 触发一个流程。 |
| `SendSignal` | 把它等的那个东西递进去。 |
| `Cancel` | 不管流程停在哪一步，让它停下。 |

流程走向就用普通的 `if` 和 `for` 控制，没有任何东西需要注册。

### 等外面的事情发生

「三十分钟内没付就关单」是一场事件和超时的赛跑。`WaitForSignal` 把这场赛跑收进
一次原子判定，所以 29:59.9 到的那笔钱不会既算收到、又算超时：

```go
paid, ok := gotick.WaitForSignal[Payment](ctx, "paid",
	gotick.WithSignalTimeout(30*time.Minute))
if !ok {
	gotick.Task(ctx, "close-order", closeOrder)
	return
}
gotick.Task(ctx, "ship", shipOrder)
```

支付回调那边调一句 `tick.SendSignal(ctx, callId, "paid", payment)` 就行。

### 用你自己的 ID，不用 callId

触发时把你本来就有的标识传进去，之后就用它寻址——不必为了 gotick 在订单表上多存
一列：

```go
tick.Trigger(ctx, "order/close", meta, gotick.WithKey(orderId))

tick.SendSignalByKey(ctx, "order/close", orderId, "paid", payment)
tick.CancelByKey(ctx, "order/close", orderId, "用户撤单")
```

同一个 key 再触发一次，前一个还没结束的调用会自动被取消，只有最后一次继续跑。

## 看它现在跑到哪了

内置一个检查界面：哪些流程在跑、每一个停在哪一步、花了多久、哪里失败了、睡着的
那个还剩多久。列表里带着触发时传的 metadata，可以按 flow、状态、call id 或者
`WithKey` 绑的业务 key 筛——「ORD-88231 那单怎么了」是查一次，不是翻一屏。
它就是一个普通的 `http.Handler`，直接读 Redis，不需要连上正在跑的 worker：

```go
h, _ := ui.NewHandler(ui.Options{
	RootPath: "/_gotick",
	Store:    store.NewRedisStore(rdb),
	Auth:     ui.BasicAuth("admin", "s3cret"), // 它能看到所有 metadata
})
mux.Handle("/_gotick/", h)
```

或者一行代码都不改，直接看一眼：

```bash
go run github.com/zbysir/gotick/cmd/gotick@latest ui -redis redis://localhost:6379/0
```

界面在独立子包里，不 import 它的人二进制不会增加一个字节。

## 文档

剩下的部分——重放引擎到底怎么工作、并行、循环、重试与超时、队列命名空间、存了
什么存多久、以及完整的例子——都在
**[gotick.bysir.top/docs](https://gotick.bysir.top/docs)**。

可以直接跑的例子在 [`example/`](./example)。

## 现状

gotick 还年轻：只支持 Go，还没有流程版本管理，也没有在 Temporal 那种规模上被
验证过。[该说清楚的短板](https://gotick.bysir.top/#rivals)都写下来了，拿它扛钱之前
请先读一遍。

MIT 协议。
