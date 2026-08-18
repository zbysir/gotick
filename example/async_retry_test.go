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

// WithMaxRetry 以前对 Async 完全无效（重试上限写死在 runFuture 里的常量上）。
// 这一组用例钉住「现在它生效了，而且不传时的默认值没被改动」。

// 传了 WithMaxRetry 就该按它来：1 次重试 = 总共执行 2 遍。
func TestAsyncHonorsWithMaxRetry(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	var runs atomic.Int32

	tick.Flow("async/retry-1", func(ctx *gotick.Context) {
		f := gotick.Async(ctx, "always-fails", func(c *gotick.TaskContext) (int, error) {
			runs.Add(1)
			return 0, errors.New("boom")
		}, gotick.WithMaxRetry(1))
		gotick.Wait(ctx, 1, f)
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		fin.fire()
		return nil
	})

	runFlow(t, tick, "async/retry-1", nil, fin, 30*time.Second)

	// 重试次数不含第一次，所以 1 = 执行两遍
	assert.Equal(t, int32(2), runs.Load(),
		"WithMaxRetry(1) 应当总共执行 2 遍；如果是 6 遍说明又走回写死的 defaultAsyncMaxRetry 了")
}

// 不传选项时，默认仍是 defaultAsyncMaxRetry(5) —— 总共 6 遍。
// 这一条是防止「顺手把 Async 的默认值改成 Task 的 3」这类回归。
func TestAsyncDefaultRetryUnchanged(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	var runs atomic.Int32

	tick.Flow("async/retry-default", func(ctx *gotick.Context) {
		f := gotick.Async(ctx, "always-fails", func(c *gotick.TaskContext) (int, error) {
			runs.Add(1)
			return 0, errors.New("boom")
		})
		gotick.Wait(ctx, 1, f)
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		fin.fire()
		return nil
	})

	runFlow(t, tick, "async/retry-default", nil, fin, 40*time.Second)

	assert.Equal(t, int32(6), runs.Load(),
		"不传 WithMaxRetry 时默认应当还是 5 次重试（共 6 遍），别被 Task 的 3 带跑")
}

// WithMaxRetry(0) 表示失败即失败，一遍就结束。
func TestAsyncMaxRetryZero(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	var runs atomic.Int32

	tick.Flow("async/retry-0", func(ctx *gotick.Context) {
		f := gotick.Async(ctx, "always-fails", func(c *gotick.TaskContext) (int, error) {
			runs.Add(1)
			return 0, errors.New("boom")
		}, gotick.WithMaxRetry(0))
		gotick.Wait(ctx, 1, f)
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		fin.fire()
		return nil
	})

	runFlow(t, tick, "async/retry-0", nil, fin, 20*time.Second)
	assert.Equal(t, int32(1), runs.Load(), "WithMaxRetry(0) 应当只执行一遍")
}

// AsyncArray 要把选项透传给它生成的每一个并行任务。
func TestAsyncArrayPropagatesOptions(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	var runs atomic.Int32

	tick.Flow("async/array-opts", func(ctx *gotick.Context) {
		items := gotick.Array(ctx, "items", func(c *gotick.TaskContext) ([]string, error) {
			return []string{"a", "b"}, nil
		})
		fs := gotick.AsyncArray(ctx, "work", items,
			func(c *gotick.TaskContext, v string, i int) (int, error) {
				runs.Add(1)
				return 0, errors.New("boom")
			}, gotick.WithMaxRetry(0))
		gotick.Wait(ctx, 2, fs...)
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		fin.fire()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	go func() { _ = tick.StartServer(ctx) }()
	_, err := tick.Trigger(ctx, "async/array-opts", nil)
	require.NoError(t, err)

	select {
	case <-fin.ch:
	case <-time.After(20 * time.Second):
		t.Fatal("流程没有失败结束")
	}

	// 两个元素、每个只准跑一遍。如果选项没透传，每个会跑 6 遍。
	assert.LessOrEqual(t, runs.Load(), int32(2),
		"WithMaxRetry(0) 应当透传到每个并行任务；跑到 12 遍说明没传下去")
}
