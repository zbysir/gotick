<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/logo-wordmark-dark.svg">
  <img src="./assets/logo-wordmark.svg" alt="gotick" width="260">
</picture>

[![CI](https://github.com/zbysir/gotick/actions/workflows/ci.yml/badge.svg)](https://github.com/zbysir/gotick/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zbysir/gotick.svg)](https://pkg.go.dev/github.com/zbysir/gotick)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

**English** · [简体中文](./readme.zh-CN.md) · [Documentation](https://gotick.bysir.top/en/docs)

Durable workflows for Go, on the Redis you already run.

Close the order in thirty minutes. Email the user three days after signup. These
flows live longer than the process that started them. gotick lets you write them
down as they read — and a restart, a crash or a deploy won't send them back to
the beginning.

There is no server to deploy. It uses the Redis you already have.

## Install

```bash
go get github.com/zbysir/gotick
```

## Quick start

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

	// A flow is an ordinary Go function. You don't call it — you trigger it.
	tick.Flow("order/close", func(ctx *gotick.Context) {
		gotick.Task(ctx, "create-order", func(c *gotick.TaskContext) error {
			log.Println("order created")
			return nil
		})

		// Not time.Sleep. Nothing is running while it waits, and you can
		// restart the process here — the flow still wakes up on time.
		gotick.Sleep(ctx, "wait-payment", 30*time.Second)

		gotick.Task(ctx, "close-order", func(c *gotick.TaskContext) error {
			log.Println("order closed")
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

## The one thing you have to know

gotick does not compile your flow into a state machine. **Every scheduling round
runs your function again from the top**, and the steps that already finished are
skipped using state in Redis. A flow with four steps runs its function at least
five times.

So the code inside `Task` runs once, but the code *around* it runs many times —
and it has to reach the same decisions every time:

```go
tick.Flow("demo", func(ctx *gotick.Context) {
	// ✗ each run sees a different time, so runs can take different branches
	if time.Now().Hour() > 12 { ... }

	// ✗ hits the database on every run, and the answer may change
	user, _ := db.GetUser(id)

	// ✓ Memo runs it once and replays the stored result
	user := gotick.Memo(ctx, "user", func() (User, error) {
		return db.GetUser(id)
	})
})
```

The rule: **anything with a side effect, or whose answer can change, goes inside
`Task` / `Memo` / `Array`.** The function body around them only decides the order.

## The whole API

| In a flow | |
| --- | --- |
| `Task` | Run a step. Retried on failure. |
| `Memo` | Run a step once and remember its result. |
| `Sleep` | Wait — seconds or days — holding no process. |
| `WaitForSignal` | Wait until something outside sends a signal. |
| `Async` + `Wait` | Run steps in parallel, with a concurrency limit. |
| `Array` | Remember a list, then fan out over it. |

| From outside | |
| --- | --- |
| `Trigger` | Start a flow. |
| `SendSignal` | Hand a waiting flow the thing it's waiting for. |
| `Cancel` | Stop a flow, wherever it currently is. |

Steer the flow with ordinary `if` and `for`. There is nothing to register.

### Waiting for something outside

"Close the order unless it's paid within 30 minutes" is a race between an event
and a timeout. `WaitForSignal` settles that race in one atomic operation, so a
payment landing at 29:59.9 can't be both accepted and timed out:

```go
paid, ok := gotick.WaitForSignal[Payment](ctx, "paid",
	gotick.WithSignalTimeout(30*time.Minute))
if !ok {
	gotick.Task(ctx, "close-order", closeOrder)
	return
}
gotick.Task(ctx, "ship", shipOrder)
```

Your payment webhook calls `tick.SendSignal(ctx, callId, "paid", payment)`.

### Use your own ID, not a callId

Pass an identifier you already have when you trigger, and address the flow by it
afterwards — no extra column on your orders table just for gotick:

```go
tick.Trigger(ctx, "order/close", meta, gotick.WithKey(orderId))

tick.SendSignalByKey(ctx, "order/close", orderId, "paid", payment)
tick.CancelByKey(ctx, "order/close", orderId, "user canceled")
```

Trigger the same key again and the previous unfinished run is canceled
automatically — only the latest one carries on.

## Seeing what's going on

A built-in inspector shows which flows are running, which step each one is on,
how long it took, what failed, and how long a sleeping one has left. It's a plain
`http.Handler` that reads Redis directly, so it never has to reach a running
worker:

```go
h, _ := ui.NewHandler(ui.Options{
	RootPath: "/_gotick",
	Store:    store.NewRedisStore(rdb),
	Auth:     ui.BasicAuth("admin", "s3cret"), // it can see every metadata value
})
mux.Handle("/_gotick/", h)
```

Or look without changing any code:

```bash
go run github.com/zbysir/gotick/cmd/gotick@latest ui -redis redis://localhost:6379/0
```

The inspector lives in its own sub-package, so if you never import it your binary
doesn't grow by a single byte.

## Documentation

The rest — how the replay engine actually works, parallel steps, loops, retries
and timeouts, queue namespaces, what is stored and for how long, and complete
examples — is at **[gotick.bysir.top/en/docs](https://gotick.bysir.top/en/docs)**.

Runnable examples live in [`example/`](./example).

## Status

gotick is young. It is Go-only, it has no workflow versioning yet, and it hasn't
been proven at the scale Temporal has. The
[honest tradeoffs](https://gotick.bysir.top/en#rivals) are written down — read
them before you bet money on it.

MIT licensed.
