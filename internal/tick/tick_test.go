package tick

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"
)

// 只依赖 redis：redis 作为一个通用的基础设施，更方便部署；redis 默认存储短期的已完成的日志，如果要长期存储需要设置 mysql/pgsql.

// 两种使用方式：
// 1. 单机，依赖 redis。
// 2. 服务、客户端模式，其中通过 MQ 通讯，通过封装 sdk 的方式，达到和单机模式一样的使用体验。
// 优先实现 1

// 定义流程和触发还是需要分开写代码，因为每次服务启动都需要注册流程，和触发的时机是不同的。

type MockBridge struct {
	f func(s Scheler) error
}

func (m *MockBridge) Publish(s Scheler) error {
	return m.f(s)
}

func (m *MockBridge) OnCallback(f func(s Scheler) error) {
	m.f = f
}

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
func TestMeta(t *testing.T) {
	ctx := context.Background()
	meta := map[string]string{"1": "1"}
	ctx = WithMetaData(ctx, meta)

	t.Logf("%+v", GetMetaData(ctx))
}
