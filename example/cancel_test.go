package example

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// 取消一个正在睡眠的流程：它应当被立刻唤醒并停下，
// 而不是睡满整个 Sleep 之后才发现自己已经被取消了。
func TestCancelWakesSleepingFlow(t *testing.T) {
	tick := newTestServer()

	var afterSleep atomic.Int32
	sleeping := newSignaler()

	tick.Flow("cancel/sleeping", func(ctx *gotick.Context) {
		gotick.Task(ctx, "before", func(c *gotick.TaskContext) error {
			sleeping.fire() // 通知用例：已经走到 Sleep 之前了
			return nil
		})
		// 睡很久。取消必须不等它醒
		gotick.Sleep(ctx, "long-nap", time.Hour)
		gotick.Task(ctx, "after", func(c *gotick.TaskContext) error {
			afterSleep.Add(1)
			return nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "cancel/sleeping", nil)
	require.NoError(t, err)

	select {
	case <-sleeping.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("流程没能走到 Sleep")
	}
	// 等它确实进入睡眠（Sleep 落盘之后才算）
	time.Sleep(500 * time.Millisecond)

	require.NoError(t, tick.Cancel(ctx, callId, "用例主动取消"))

	// 取消后它应该很快变成终态，而不是一小时后
	var info gotick.RunInfo
	require.Eventually(t, func() bool {
		var exist bool
		var err error
		info, exist, err = tick.RunIndex().GetRun(callId)
		return err == nil && exist && info.Finished()
	}, 10*time.Second, 100*time.Millisecond, "取消之后流程没有进入终态")

	assert.Equal(t, gotick.RunStatusCanceled, info.Status)
	assert.Equal(t, "用例主动取消", info.Error, "取消原因应当被记下来")
	assert.Equal(t, int32(0), afterSleep.Load(), "Sleep 之后的 task 不该再执行")
}

// 取消正在执行的 task：它拿到的 ctx 应当被 cancel，
// 让业务代码有机会提前返回，而不是干到自然结束。
func TestCancelInterruptsRunningTask(t *testing.T) {
	tick := newTestServer()

	entered := newSignaler()
	var sawDone atomic.Bool
	var ranLong atomic.Int32

	tick.Flow("cancel/running", func(ctx *gotick.Context) {
		gotick.Task(ctx, "long", func(c *gotick.TaskContext) error {
			ranLong.Add(1)
			entered.fire()
			// 模拟一个跑很久的调用。ctx 被取消时应当提前回来。
			select {
			case <-c.Done():
				sawDone.Store(true)
				return c.Err()
			case <-time.After(60 * time.Second):
				return errors.New("等到超时都没被取消")
			}
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()

	callId, err := tick.Trigger(ctx, "cancel/running", nil)
	require.NoError(t, err)

	select {
	case <-entered.ch:
	case <-time.After(10 * time.Second):
		t.Fatal("task 没有开始执行")
	}

	require.NoError(t, tick.Cancel(ctx, callId, "掐断执行中的 task"))

	// cancelWatchInterval 是 3s，留够余量
	require.Eventually(t, func() bool { return sawDone.Load() },
		15*time.Second, 100*time.Millisecond, "执行中的 task 没有收到 ctx 取消")
}

// 已经结束的调用不能被取消，且不该被悄悄改状态。
func TestCancelRejectsFinishedRun(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()

	tick.Flow("cancel/done", func(ctx *gotick.Context) {
		gotick.Task(ctx, "only", func(c *gotick.TaskContext) error { return nil })
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	callId := runFlow(t, tick, "cancel/done", nil, fin, 15*time.Second)

	err := tick.Cancel(context.Background(), callId, "太晚了")
	require.Error(t, err)
	assert.ErrorIs(t, err, gotick.ErrRunNotCancelable)

	info, exist, err := tick.RunIndex().GetRun(callId)
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, gotick.RunStatusDone, info.Status, "已完成的流程状态不该被取消改写")
}

// 索引里没有的 callId 应当明确报错，而不是静默写一个没人读的标志。
func TestCancelUnknownRun(t *testing.T) {
	tick := newTestServer()
	err := tick.Cancel(context.Background(), "no-such-call", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, gotick.ErrRunNotFound)
}
