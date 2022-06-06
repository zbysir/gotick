# Ticktick

可以服务重启恢复的 Sleep 框架，灵感来自 https://github.com/temporalio/temporal

## 特性

- 实现同步语法写异步逻辑，优雅面对复杂逻辑，当然支持服务重启。
- 只能支持精度到秒级。
- 支持失败重试（基于消息队列自身特性）。

TODO

- UI
- 可视化执行

## 用途

最轻量化的定时、延时任务调度器

不同于 temporal，ticktick 不是一个全栈框架，temporal 有很多部署与维护成本（和开发成本），
而 ticktick 只是一个 SDK，集成到项目即可，你不用担心 ticktick 的部署与维护成本。

## 依赖

- mysql：持久化状态
- nsq：使用消息队列来分发事件来实现负载均衡

## Example

```go
package ticktick

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestTick(t *testing.T) {
	ctx := context.Background()

	var c = Workflow{
		workflows: map[string]func(ctx *WorkflowContext) error{},
	}

	c.Handle("test", func(ctx *WorkflowContext) error {
		// 发放 vip
		var rst string
		err := ctx.Wrap("create", func() (any, error) {
			log.Printf("create exec")
			return "success", nil
		}, &rst, nil)
		if err != nil {
			return err
		}
		log.Printf("create result: %v", rst)
		if rst != "success" {
			return nil
		}

		// 2s 之后删除 vip
		err = ctx.Sleep("wait-for-delete", time.Second*2)
		if err != nil {
			return err
		}
		err = ctx.Wrap("delete", func() (interface{}, error) {
			log.Printf("delete exec")
			return "success", nil
		}, &rst, nil)
		if err != nil {
			return err
		}
		log.Printf("delete result: %v", rst)
		return nil
	})

	// 触发 workflow
	status, err := c.Touch("test", ctx)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Logf("exit status: %+v", status)

	time.Sleep(5 * time.Second)
}

```

