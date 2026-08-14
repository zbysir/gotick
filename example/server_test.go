package example

import (
	"fmt"
	"testing"
	"time"

	"github.com/zbysir/gotick"
)

// TestSuccess 验证一个全部成功的 flow：两个 task 各执行一次，OnSuccess 被调用。
func TestSuccess(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	ran := newCounter()

	tick.Flow("demo/success", func(ctx *gotick.Context) {
		gotick.Task(ctx, "t1", func(ctx *gotick.TaskContext) error {
			ran.inc("t1")
			t.Logf("running t1, meta=%v", ctx.MetaDataAll())
			return nil
		})
		gotick.Task(ctx, "t2", func(ctx *gotick.TaskContext) error {
			ran.inc("t2")
			t.Logf("running t2, meta=%v", ctx.MetaDataAll())
			return nil
		})
	}).OnSuccess(func(ctx *gotick.Context) error {
		ran.inc("onSuccess")
		fin.fire()
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Errorf("unexpected OnFail on task %q: %v", ts.Key, ts.Errs)
		fin.fire()
		return nil
	})

	runFlow(t, tick, "demo/success", gotick.MetaData{"name": "bysir"}, fin, 30*time.Second)

	for _, k := range []string{"t1", "t2"} {
		if got := ran.get(k); got != 1 {
			t.Errorf("task %q ran %d times, expected exactly 1", k, got)
		}
	}
	if ran.get("onSuccess") == 0 {
		t.Error("OnSuccess was never called")
	}
}

// TestFail 验证一个始终失败的 task：重试到上限后 OnFail 被调用，且错误被记录下来。
func TestFail(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	ran := newCounter()

	const maxRetry = 2
	var failedStatus gotick.TaskStatus

	tick.Flow("demo/fail", func(ctx *gotick.Context) {
		gotick.Task(ctx, "always-fails", func(ctx *gotick.TaskContext) error {
			ran.inc("always-fails")
			return fmt.Errorf("boom (retry %d)", ctx.Retry)
		}, gotick.WithMaxRetry(maxRetry))

		gotick.Task(ctx, "never-reached", func(ctx *gotick.TaskContext) error {
			ran.inc("never-reached")
			return nil
		})
	}).OnError(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		ran.inc("onError")
		t.Logf("OnError: task=%q retry=%d", ts.Key, ts.RetryCount)
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		failedStatus = ts
		fin.fire()
		return nil
	}).OnSuccess(func(ctx *gotick.Context) error {
		t.Error("OnSuccess called on a flow that should have failed")
		fin.fire()
		return nil
	})

	runFlow(t, tick, "demo/fail", gotick.MetaData{"name": "bysir"}, fin, 30*time.Second)

	if failedStatus.Key != "always-fails" {
		t.Errorf("OnFail reported task %q, expected %q", failedStatus.Key, "always-fails")
	}
	if failedStatus.Status != "fail" {
		t.Errorf("OnFail reported status %q, expected %q", failedStatus.Status, "fail")
	}
	if len(failedStatus.Errs) == 0 {
		t.Error("OnFail reported no errors, expected at least one")
	}
	if ran.get("onError") == 0 {
		t.Error("OnError was never called")
	}

	// 上游失败后，后续 task 绝不该执行。
	// 目前 Task 对 fail 状态直接放行，所以这条断言可能失败——那正是它存在的意义。
	if got := ran.get("never-reached"); got != 0 {
		t.Errorf("task after a failed task ran %d times, expected 0 "+
			"(上游失败、下游照跑)", got)
	}
}
