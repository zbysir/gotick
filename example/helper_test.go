package example

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/store"
)

// newTestServer 为每个用例创建独立的 server。
// 不能共用：Subscribe 是往 topic 上追加回调，多个用例注册同名 flow 会互相触发。
func newTestServer() *gotick.Server {
	return gotick.NewServer(gotick.NewServerParams{
		DelayedQueue: store.NewMockRedisDelayedQueue(),
		KVStore:      store.NewMockKvStore(),
	})
}

// signaler 用于从 flow 回调里通知用例结束。
// flow 会被重放多次，回调可能被调用多次，所以必须保证只关闭一次。
type signaler struct {
	once sync.Once
	ch   chan struct{}
}

func newSignaler() *signaler {
	return &signaler{ch: make(chan struct{})}
}

func (s *signaler) fire() {
	s.once.Do(func() { close(s.ch) })
}

// runFlow 启动 server、触发一次 flow，等待 fin 被触发或超时。
// 超时会让用例失败而不是永久挂起——测试永远不应该 hang。
func runFlow(t *testing.T, tick *gotick.Server, flowId string, meta gotick.MetaData, fin *signaler, timeout time.Duration) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tick.StartServer(ctx); err != nil {
			t.Errorf("StartServer: %v", err)
		}
	}()

	callId, err := tick.Trigger(ctx, flowId, meta)
	if err != nil {
		t.Fatalf("Trigger %q: %v", flowId, err)
	}

	select {
	case <-fin.ch:
	case <-time.After(timeout):
		t.Fatalf("flow %q (callId %s) did not finish within %v", flowId, callId, timeout)
	}

	cancel()
	wg.Wait()

	return callId
}

// counter 记录每个 key 被执行的次数，用于断言「恰好执行一次」这类性质。
type counter struct {
	mu sync.Mutex
	m  map[string]int
}

func newCounter() *counter {
	return &counter{m: map[string]int{}}
}

func (c *counter) inc(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key]++
}

func (c *counter) get(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[key]
}

func (c *counter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}
