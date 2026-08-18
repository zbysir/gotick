package example

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// 用业务自己的 key 取消，不必存 callId。
func TestCancelByKey(t *testing.T) {
	tick := newTestServer()
	waiting := newSignaler()

	tick.Flow("key/cancel", func(ctx *gotick.Context) {
		gotick.Task(ctx, "step", func(c *gotick.TaskContext) error {
			waiting.fire()
			return nil
		})
		gotick.Sleep(ctx, "long", time.Hour)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "key/cancel", nil, gotick.WithKey("ORD-1"))
	require.NoError(t, err)

	select {
	case <-waiting.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没跑起来")
	}
	time.Sleep(500 * time.Millisecond)

	// 全程没用过 callId
	require.NoError(t, tick.CancelByKey(ctx, "key/cancel", "ORD-1", "用户撤单"))

	var info gotick.RunInfo
	require.Eventually(t, func() bool {
		var exist bool
		var e error
		info, exist, e = tick.RunIndex().GetRun(callId)
		return e == nil && exist && info.Finished()
	}, 15*time.Second, 100*time.Millisecond, "按 key 取消没生效")
	assert.Equal(t, gotick.RunStatusCanceled, info.Status)
}

// 用业务 key 发信号。
func TestSendSignalByKey(t *testing.T) {
	tick := newTestServer()
	waiting := newSignaler()
	fin := newSignaler()
	var got atomic.Value

	tick.Flow("key/signal", func(ctx *gotick.Context) {
		gotick.Task(ctx, "step", func(c *gotick.TaskContext) error {
			waiting.fire()
			return nil
		})
		v, _ := gotick.WaitForSignal[string](ctx, "approve")
		got.Store(v)
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	_, err := tick.Trigger(ctx, "key/signal", nil, gotick.WithKey("USER-7"))
	require.NoError(t, err)

	select {
	case <-waiting.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没跑起来")
	}
	time.Sleep(500 * time.Millisecond)

	ok, err := tick.SendSignalByKey(ctx, "key/signal", "USER-7", "approve", "同意")
	require.NoError(t, err)
	assert.True(t, ok)

	select {
	case <-fin.ch:
	case <-time.After(15 * time.Second):
		t.Fatal("按 key 发信号之后流程没继续")
	}
	assert.Equal(t, "同意", got.Load())
}

// 默认顶替：同一个 (flow, key) 再次触发，前一个被取消，只有最后一个跑完。
func TestKeySupersedes(t *testing.T) {
	tick := newTestServer()

	first := newSignaler()
	var finished atomic.Int32

	tick.Flow("key/supersede", func(ctx *gotick.Context) {
		gotick.Task(ctx, "step", func(c *gotick.TaskContext) error {
			first.fire()
			return nil
		})
		gotick.Sleep(ctx, "work", 2*time.Second)
		gotick.Task(ctx, "commit", func(c *gotick.TaskContext) error {
			finished.Add(1)
			return nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	oldId, err := tick.Trigger(ctx, "key/supersede", nil, gotick.WithKey("ORD-9"))
	require.NoError(t, err)

	select {
	case <-first.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("第一个流程没跑起来")
	}
	time.Sleep(300 * time.Millisecond)

	// 同一个 key 再触发一次
	newId, err := tick.Trigger(ctx, "key/supersede", nil, gotick.WithKey("ORD-9"))
	require.NoError(t, err)
	require.NotEqual(t, oldId, newId)

	// 旧的应当被取消，而不是凭空消失
	var oldInfo gotick.RunInfo
	require.Eventually(t, func() bool {
		var exist bool
		var e error
		oldInfo, exist, e = tick.RunIndex().GetRun(oldId)
		return e == nil && exist && oldInfo.Finished()
	}, 15*time.Second, 100*time.Millisecond, "旧流程没有被顶替掉")
	assert.Equal(t, gotick.RunStatusCanceled, oldInfo.Status)
	assert.Contains(t, oldInfo.Error, "superseded", "取消原因应当说明是被顶替的")

	// 新的应当正常跑完
	require.Eventually(t, func() bool {
		info, exist, e := tick.RunIndex().GetRun(newId)
		return e == nil && exist && info.Status == gotick.RunStatusDone
	}, 20*time.Second, 100*time.Millisecond, "新流程没跑完")

	assert.Equal(t, int32(1), finished.Load(), "只有最后一次该走到 commit")
}

// 流程结束后绑定要自动解除，否则一个订单一辈子只能跑一次。
func TestKeyUnbindsWhenFinished(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()

	tick.Flow("key/unbind", func(ctx *gotick.Context) {
		gotick.Task(ctx, "only", func(c *gotick.TaskContext) error { return nil })
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	firstId, err := tick.Trigger(ctx, "key/unbind", nil, gotick.WithKey("ORD-2"))
	require.NoError(t, err)

	select {
	case <-fin.ch:
	case <-time.After(15 * time.Second):
		t.Fatal("流程没跑完")
	}
	// 解绑发生在终态那一刻，给它一点时间落盘
	time.Sleep(1 * time.Second)

	// 结束之后 key 应当查不到了
	err = tick.CancelByKey(ctx, "key/unbind", "ORD-2", "试试")
	assert.ErrorIs(t, err, gotick.ErrRunNotFound, "结束之后绑定应当已经解除")

	// 同一个 key 可以再跑一次，而且不会去顶替那个已经结束的
	secondId, err := tick.Trigger(ctx, "key/unbind", nil, gotick.WithKey("ORD-2"))
	require.NoError(t, err)
	require.NotEqual(t, firstId, secondId)

	require.Eventually(t, func() bool {
		info, exist, e := tick.RunIndex().GetRun(secondId)
		return e == nil && exist && info.Status == gotick.RunStatusDone
	}, 20*time.Second, 100*time.Millisecond, "同一个 key 的第二次调用没跑完")

	// 第一次的结局不该被第二次连累
	info, exist, err := tick.RunIndex().GetRun(firstId)
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, gotick.RunStatusDone, info.Status, "已经完成的调用不该被后来的顶替改写")
}

// 不同 flow 用同一个 key 互不干扰——作用域是 (flowId, key)。
func TestKeyScopedPerFlow(t *testing.T) {
	tick := newTestServer()
	aStarted, bStarted := newSignaler(), newSignaler()

	for _, f := range []struct {
		id  string
		sig *signaler
	}{{"key/scope-a", aStarted}, {"key/scope-b", bStarted}} {
		sig := f.sig
		tick.Flow(f.id, func(ctx *gotick.Context) {
			gotick.Task(ctx, "step", func(c *gotick.TaskContext) error {
				sig.fire()
				return nil
			})
			gotick.Sleep(ctx, "hold", time.Hour)
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	aId, err := tick.Trigger(ctx, "key/scope-a", nil, gotick.WithKey("SAME"))
	require.NoError(t, err)
	bId, err := tick.Trigger(ctx, "key/scope-b", nil, gotick.WithKey("SAME"))
	require.NoError(t, err)

	for _, s := range []*signaler{aStarted, bStarted} {
		select {
		case <-s.ch:
		case <-time.After(10 * time.Second):
			t.Fatal("流程没跑起来")
		}
	}
	time.Sleep(500 * time.Millisecond)

	// 取消 a，b 不该受影响
	require.NoError(t, tick.CancelByKey(ctx, "key/scope-a", "SAME", "只停 a"))

	require.Eventually(t, func() bool {
		info, exist, e := tick.RunIndex().GetRun(aId)
		return e == nil && exist && info.Status == gotick.RunStatusCanceled
	}, 15*time.Second, 100*time.Millisecond, "a 没有被取消")

	info, exist, err := tick.RunIndex().GetRun(bId)
	require.NoError(t, err)
	require.True(t, exist)
	assert.False(t, info.Finished(), "b 用了同名 key，但不该被 a 的取消波及")
}
