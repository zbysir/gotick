package example

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// TestScatteredFlowSets 覆盖最坏的部署形态：
// N 个 worker 各自只注册一个 flow，全都消费同一个命名空间。
//
// 这是文档里不建议的用法（应该用不同的命名空间隔离），但它必须"慢"而不是"坏"。
// 第一版实现靠消息队列的重试把事件弹到正确的 worker，命中率 1/8 时
// 重试预算会被耗尽、事件被归档、流程永久卡死——这条用例就是那次的回归测试。
func TestScatteredFlowSets(t *testing.T) {
	mr := miniredis.RunT(t)

	const workers = 8

	fins := make([]*signaler, workers)
	servers := make([]*gotick.Server, workers)

	for i := 0; i < workers; i++ {
		id := fmt.Sprintf("demo/scatter-%02d", i)
		fins[i] = newSignaler()
		fin := fins[i]

		tick := newRedisServer(t, mr.Addr())
		tick.Flow(id, func(ctx *gotick.Context) {
			gotick.Task(ctx, "a", func(*gotick.TaskContext) error { return nil })
			gotick.Task(ctx, "b", func(*gotick.TaskContext) error { return nil })
		}).OnSuccess(func(ctx *gotick.Context) error { fin.fire(); return nil })
		servers[i] = tick
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range servers {
		s := s
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.StartServer(ctx) }()
	}

	start := time.Now()
	for i := 0; i < workers; i++ {
		_, err := servers[0].Trigger(ctx, fmt.Sprintf("demo/scatter-%02d", i), nil)
		require.NoError(t, err)
	}

	for i, fin := range fins {
		select {
		case <-fin.ch:
		case <-time.After(75 * time.Second):
			t.Fatalf("flow %d 没跑完", i)
		}
	}

	t.Logf("%d 个 worker 各注册 1 个 flow（1/%d 命中率），全部跑完耗时 %v",
		workers, workers, time.Since(start).Round(10*time.Millisecond))

	cancel()
	wg.Wait()
}
