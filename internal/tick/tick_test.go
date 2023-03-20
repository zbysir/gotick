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

	var start time.Time = time.Now()

	tick.Flow("demo").
		Then("first", func(ctx context.Context) (NextStatus, error) {
			log.Printf("[%v] first exec at %v", GetCallId(ctx), time.Now().Sub(start))
			v := GetCallId(ctx)
			Store(ctx, "first", v)
			log.Printf("[%v] set first value: %v", GetCallId(ctx), v)
			return NextStatus{}, nil
		}).
		Then("wait-for-second", func(ctx context.Context) (NextStatus, error) {
			return NextStatus{status: "sleep", runAt: time.Now().Add(2 * time.Second)}, nil
		}).
		Then("third", func(ctx context.Context) (NextStatus, error) {
			log.Printf("[%v] third exec at %v", GetCallId(ctx), time.Now().Sub(start))
			return NextStatus{}, nil
		}).
		Then("wait-for-second2", func(ctx context.Context) (NextStatus, error) {
			return NextStatus{status: "sleep", runAt: time.Now().Add(2 * time.Second)}, nil
		}).
		Then("end", func(ctx context.Context) (NextStatus, error) {
			log.Printf("[%v] end at %v", GetCallId(ctx), time.Now().Sub(start))
			data := GetMetaData(ctx)
			log.Printf("[%v] meta data: %v", GetCallId(ctx), data)
			wg.Done()
			return NextStatus{}, nil
		})

	wg.Add(1)
	err := b.Publish(Scheler{FlowId: "demo", MetaData: map[string]string{"id": "1"}})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	wg.Add(1)
	err = b.Publish(Scheler{FlowId: "demo", MetaData: map[string]string{"id": "2"}})
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
