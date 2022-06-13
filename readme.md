# Ticktick

可以服务重启恢复的 Sleep 框架，灵感来自 https://github.com/temporalio/temporal

## 特性

- 实现同步语法写异步逻辑，优雅面对复杂逻辑，不必为了延时任务大改你的代码。
- 只能支持精度到秒级。
- 支持失败重试（基于消息队列自身特性）。
- 可扩缩容
- 自身足够简单可信耐，依赖 nsq 和 redis 实现复杂特性，自己只实现了一个定时器与保存历史与 UI。

TODO

- UI
- 可视化执行

## 架构
- SDK：用于项目里，和 nsq handler 类似，只需要 handler 回调，处理完成之后还是通过 nsq 发送结果给 独立服务
- 独立服务：运行独立服务用于定时触发 timer，UI，存储数据，可扩缩容。

## 用途

最轻量化的定时、延时任务调度器

不同于 temporal，ticktick 不是一个全栈框架，temporal 有很多部署与维护成本（和开发成本），
而 ticktick 足够简单，你不用担心 ticktick 的部署与维护成本。

## 依赖

- mysql：持久化 workflow 状态。
- redis：定时消息（容易实现分布式一致性）。
- nsq：使用消息队列来分发事件来实现负载均衡。

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

