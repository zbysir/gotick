package example

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/zbysir/gotick"
)

// TestFor 用 Array 生成一组任务并逐个执行，其中一个任务前 5 次失败。
// 断言：每个任务最终都成功执行，失败的那个确实重试了。
func TestFor(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	succeeded := newCounter()
	attempts := newCounter()

	const flakyItem = "f"
	const flakyRetries = 5

	items := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	tick.Flow("demo/for", func(ctx *gotick.Context) {
		gotick.Task(ctx, "start", func(ctx *gotick.TaskContext) error {
			t.Logf("[%v] start", ctx.CallId)
			return nil
		})

		tasks := gotick.Array(ctx, "gen-tasks", func(ctx *gotick.TaskContext) ([]string, error) {
			t.Logf("gen-tasks called")
			return items, nil
		})

		for _, task := range tasks {
			task := task
			gotick.Task(ctx, task.Key("send-email"), func(ctx *gotick.TaskContext) error {
				v := task.Value()
				attempts.inc(v)

				// 模拟一个不稳定的任务：前 5 次失败，第 6 次成功。
				if v == flakyItem && ctx.Retry < flakyRetries {
					t.Logf("send-email to %q failed, retry=%v", v, ctx.Retry)
					return errors.New("transient failure")
				}

				succeeded.inc(v)
				t.Logf("send-email to %q ok (retry=%v)", v, ctx.Retry)
				return nil
			}, gotick.WithMaxRetry(flakyRetries+1))
		}

		gotick.Task(ctx, "done", func(ctx *gotick.TaskContext) error {
			t.Logf("all done")
			return nil
		})
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Errorf("flow failed on task %q: %v", ts.Key, ts.Errs)
		fin.fire()
		return nil
	})

	runFlow(t, tick, "demo/for", gotick.MetaData{"name": "bysir"}, fin, 60*time.Second)

	for _, v := range items {
		if got := succeeded.get(v); got != 1 {
			t.Errorf("item %q: expected exactly 1 successful execution, got %d", v, got)
		}
	}

	// 那个不稳定的任务应该被尝试了 flakyRetries+1 次，其余各一次。
	if got := attempts.get(flakyItem); got != flakyRetries+1 {
		t.Errorf("item %q: expected %d attempts, got %d", flakyItem, flakyRetries+1, got)
	}
	for _, v := range items {
		if v == flakyItem {
			continue
		}
		if got := attempts.get(v); got != 1 {
			t.Errorf("item %q: expected 1 attempt, got %d", v, got)
		}
	}
}

// TestSequence 用 Sequence 驱动一个固定长度的循环。
// 断言：循环体恰好执行 seqLen 次，且每次的序号互不重复。
func TestSequence(t *testing.T) {
	tick := newTestServer()
	fin := newSignaler()
	ran := newCounter()

	const seqLen = 10

	tick.Flow("demo/sequence", func(ctx *gotick.Context) {
		seq := gotick.Sequence(ctx, "gen-tasks", seqLen)

		for seq.Next() {
			current := seq.Current
			gotick.Task(ctx, seq.TaskKey("send-email"), func(ctx *gotick.TaskContext) error {
				ran.inc(fmt.Sprintf("%d", current))
				t.Logf("send-email #%d", current)
				return nil
			})
		}

		gotick.Task(ctx, "done", func(ctx *gotick.TaskContext) error {
			t.Logf("all done, meta=%v", ctx.MetaDataAll())
			return nil
		})
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Errorf("flow failed on task %q: %v", ts.Key, ts.Errs)
		fin.fire()
		return nil
	})

	runFlow(t, tick, "demo/sequence", gotick.MetaData{"name": "bysir"}, fin, 60*time.Second)

	got := ran.snapshot()
	if len(got) != seqLen {
		t.Errorf("expected %d distinct iterations, got %d: %v", seqLen, len(got), got)
	}
	for k, n := range got {
		if n != 1 {
			t.Errorf("iteration %s ran %d times, expected exactly 1", k, n)
		}
	}
}
