# Tick

支持重启恢复的同步方式写异步代码的 go 框架，灵感来自 [temporal](https://github.com/temporalio/temporal)

## 特性

- 实现同步语法写异步逻辑，优雅面对复杂逻辑，不必为了异步、延时任务大改你的代码。
- 只能支持精度到秒级。
- 支持失败重试（基于消息队列自身特性）。
- 自身足够简单可信耐，依赖 mq 和 redis 实现复杂特性。

TODO

- UI
  - 可视化流程，可视化任务状态，统计次数
  - 查看执行结果

## 用途

最轻量化也可靠的定时、延时任务调度器

不同于 temporal，ticktick 不是一个全栈框架，temporal 有很多部署与维护成本（和开发成本），
而 ticktick 足够简单，你不用担心 ticktick 的部署与维护成本。

## 使用方式
1. 只有一个回调，适用于动态配置流程
2. 如果数据流是静态的，可以使用.then 这样的链式调用方式创建流程。

## 依赖

- mysql（可选）：持久化 workflow 状态。
- redis：定时任务。
- nsq（可选）：使用消息队列来分发事件来实现负载均衡与重试。

## Example

```go
func TestTick(t *testing.T) {
	b := MockBridge{}
	tick := NewTickServer(&b)

	wg := sync.WaitGroup{}
	wg.Add(1)

	tick.Flow("demo").
		Then("first", func(ctx context.Context) error {
			log.Printf("first exec at %v", time.Now())
			v := "first value"
			Store(ctx, "first", v)
			log.Printf("set first value: %v", v)

			return nil
		}).
		Sleep("wait-for-second", time.Second*2).
		Then("third", func(ctx context.Context) error {
			log.Printf("third exec at %v", time.Now())
			return nil
		}).
		Then("end", func(ctx context.Context) error {
			log.Printf("end at %v", time.Now())
			data := GetMetaData(ctx)
			log.Printf("meta data: %v", data)
			wg.Done()
			return nil
		})

	err := b.Publish(Scheler{FlowId: "demo"})
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}
```
