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

type approval struct {
	By string `json:"by"`
	OK bool   `json:"ok"`
}

// 最基本的一条：流程停在 WaitForSignal，人点了同意之后接着往下走。
func TestSignalResumesFlow(t *testing.T) {
	tick := newTestServer()

	waiting := newSignaler()
	fin := newSignaler()
	var got atomic.Value

	tick.Flow("signal/approve", func(ctx *gotick.Context) {
		gotick.Task(ctx, "submit", func(c *gotick.TaskContext) error {
			waiting.fire()
			return nil
		})
		a, ok := gotick.WaitForSignal[approval](ctx, "approval")
		gotick.Task(ctx, "after", func(c *gotick.TaskContext) error {
			got.Store(struct {
				A  approval
				OK bool
			}{a, ok})
			return nil
		})
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "signal/approve", nil)
	require.NoError(t, err)

	select {
	case <-waiting.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没走到等待点")
	}
	time.Sleep(500 * time.Millisecond) // 等它确实停泊下来

	accepted, err := tick.SendSignal(ctx, callId, "approval", approval{By: "alice", OK: true})
	require.NoError(t, err)
	assert.True(t, accepted)

	select {
	case <-fin.ch:
	case <-time.After(15 * time.Second):
		t.Fatal("收到信号之后流程没有继续")
	}

	v := got.Load().(struct {
		A  approval
		OK bool
	})
	assert.True(t, v.OK, "应当报告收到了信号")
	assert.Equal(t, approval{By: "alice", OK: true}, v.A)
}

// 信号比等待先到——回调比流程跑得快是常态，不该丢事件。
func TestSignalArrivingBeforeWait(t *testing.T) {
	tick := newTestServer()

	slow := newSignaler()
	fin := newSignaler()
	var got atomic.Bool

	tick.Flow("signal/early", func(ctx *gotick.Context) {
		// 第一步故意慢，好让信号在流程走到等待点之前就送到
		gotick.Task(ctx, "slow-step", func(c *gotick.TaskContext) error {
			slow.fire()
			time.Sleep(2 * time.Second)
			return nil
		})
		_, ok := gotick.WaitForSignal[string](ctx, "early")
		got.Store(ok)
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "signal/early", nil)
	require.NoError(t, err)

	select {
	case <-slow.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没开始跑")
	}

	// 此刻流程还卡在 slow-step 里，等待点还没到
	accepted, err := tick.SendSignal(ctx, callId, "early", "先到了")
	require.NoError(t, err)
	assert.True(t, accepted)

	select {
	case <-fin.ch:
	case <-time.After(20 * time.Second):
		t.Fatal("先到的信号没有被取到，流程卡住了")
	}
	assert.True(t, got.Load(), "先到的信号必须能被后来的等待取到")
}

// 超时：等不到就得往下走，而且不能每遍重放都把截止时间往后推。
func TestSignalTimeout(t *testing.T) {
	tick := newTestServer()

	fin := newSignaler()
	var ok atomic.Bool
	ok.Store(true) // 故意先设 true，超时必须把它改成 false

	tick.Flow("signal/timeout", func(ctx *gotick.Context) {
		_, received := gotick.WaitForSignal[string](ctx, "never",
			gotick.WithSignalTimeout(2*time.Second))
		ok.Store(received)
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	start := time.Now()
	_, err := tick.Trigger(ctx, "signal/timeout", nil)
	require.NoError(t, err)

	select {
	case <-fin.ch:
	case <-time.After(20 * time.Second):
		t.Fatal("超时之后流程没有继续——很可能截止时间每次重放都被重算了")
	}

	assert.False(t, ok.Load(), "超时应当报告 false")
	assert.WithinDuration(t, start.Add(2*time.Second), time.Now(), 8*time.Second,
		"应当在设定的超时附近结束，而不是立刻或很久之后")
}

// 同一个 key 的第二个信号必须被丢弃：重放要读到同一个值。
func TestSignalFirstWins(t *testing.T) {
	tick := newTestServer()

	waiting := newSignaler()
	fin := newSignaler()
	var seen atomic.Value

	tick.Flow("signal/once", func(ctx *gotick.Context) {
		gotick.Task(ctx, "step", func(c *gotick.TaskContext) error {
			waiting.fire()
			return nil
		})
		v, _ := gotick.WaitForSignal[string](ctx, "pick")
		seen.Store(v)
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "signal/once", nil)
	require.NoError(t, err)

	select {
	case <-waiting.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没走到等待点")
	}
	time.Sleep(500 * time.Millisecond)

	first, err := tick.SendSignal(ctx, callId, "pick", "第一个")
	require.NoError(t, err)
	assert.True(t, first)

	second, err := tick.SendSignal(ctx, callId, "pick", "第二个")
	require.NoError(t, err)
	assert.False(t, second, "同一个 key 的第二个信号应当被丢弃")

	select {
	case <-fin.ch:
	case <-time.After(15 * time.Second):
		t.Fatal("流程没有继续")
	}
	assert.Equal(t, "第一个", seen.Load(), "流程读到的必须是第一个信号的值")
}

// 无限等的流程可以被取消——否则它会永远停在存储里。
func TestSignalWaitCanBeCanceled(t *testing.T) {
	tick := newTestServer()
	waiting := newSignaler()

	tick.Flow("signal/cancelable", func(ctx *gotick.Context) {
		gotick.Task(ctx, "step", func(c *gotick.TaskContext) error {
			waiting.fire()
			return nil
		})
		gotick.WaitForSignal[string](ctx, "never-comes") // 不设超时
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "signal/cancelable", nil)
	require.NoError(t, err)

	select {
	case <-waiting.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没走到等待点")
	}
	time.Sleep(500 * time.Millisecond)

	require.NoError(t, tick.Cancel(ctx, callId, "不等了"))

	var info gotick.RunInfo
	require.Eventually(t, func() bool {
		var exist bool
		var e error
		info, exist, e = tick.RunIndex().GetRun(callId)
		return e == nil && exist && info.Finished()
	}, 15*time.Second, 100*time.Millisecond, "无限等的流程没能被取消")
	assert.Equal(t, gotick.RunStatusCanceled, info.Status)
}

// 超时之后信号才姗姗来迟：答案必须钉死在 false，不能因为值到了就翻供。
//
// 这是重放模型里最阴的一类 bug：第一遍重放判定超时走了 A 分支，
// 后来的重放读到迟到的值走了 B 分支，同一次调用做了两件相反的事。
func TestSignalLateArrivalDoesNotFlipTimeout(t *testing.T) {
	tick := newTestServer()

	timedOut := newSignaler()
	fin := newSignaler()
	var answers atomic.Int32 // 统计「收到了」的次数，必须一直是 0

	tick.Flow("signal/late", func(ctx *gotick.Context) {
		_, received := gotick.WaitForSignal[string](ctx, "slow",
			gotick.WithSignalTimeout(1*time.Second))
		if received {
			answers.Add(1)
		}
		timedOut.fire()
		// 超时之后还有一步，保证后面还会发生若干次重放，
		// 每一遍都会重新执行上面那个 WaitForSignal
		gotick.Sleep(ctx, "after", 2*time.Second)
		gotick.Task(ctx, "tail", func(c *gotick.TaskContext) error { return nil })
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "signal/late", nil)
	require.NoError(t, err)

	select {
	case <-timedOut.ch:
	case <-time.After(15 * time.Second):
		t.Fatal("没等到超时")
	}

	// 超时已经判定完了，现在信号才到——必须被拒
	accepted, err := tick.SendSignal(ctx, callId, "slow", "我迟到了")
	require.NoError(t, err)
	assert.False(t, accepted, "超时已经用哨兵占住信号位，迟到的信号应当被拒")

	select {
	case <-fin.ch:
	case <-time.After(25 * time.Second):
		t.Fatal("流程没跑完")
	}
	assert.Equal(t, int32(0), answers.Load(),
		"超时判定之后的任何一遍重放都不该翻供成「收到了」")
}
