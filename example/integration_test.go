package example

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// 这些用例跑在 miniredis 上，因此覆盖到真实的 Redis 编码语义和整套 asynq 调度，
// 而不是像 MockKvStore 那样绕过它们。
//
// 这一点不是可有可无的细节：MockKvStore 实现了 HSet 的 TTL，而 RedisStore 没有
// （store/kvstore.go 的 HSet 收下 expiration 参数就丢掉了）。只用 mock 测试，
// 心跳与死亡检测这类机制会「测试全绿、生产全坏」。

// newRedisServer 起一台连到给定 Redis 的 gotick server。
func newRedisServer(t *testing.T, addr string) *gotick.Server {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })

	tick, err := gotick.NewServerFromConfig(gotick.Config{
		RedisClient:       rdb,
		Concurrency:       4,
		TaskCheckInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServerFromConfig: %v", err)
	}
	return tick
}

// TestExactlyOnceAcrossWorkers 是这个仓库最重要的一条测试。
//
// 四个独立的 worker 抢同一个 flow 的调度，断言每个 task 的函数体恰好执行一次。
// 这正是 README 承诺「保证任务至少执行一次」时应该被验证的性质，
// 也是任何一次调度层改动最容易破坏的性质。
func TestExactlyOnceAcrossWorkers(t *testing.T) {
	mr := miniredis.RunT(t)

	const (
		workers  = 4
		numTasks = 12
	)

	ran := newCounter()
	fin := newSignaler()

	flow := func(ctx *gotick.Context) {
		for i := 0; i < numTasks; i++ {
			key := fmt.Sprintf("step-%02d", i)
			gotick.Task(ctx, key, func(ctx *gotick.TaskContext) error {
				ran.inc(key)
				return nil
			})
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		tick := newRedisServer(t, mr.Addr())
		tick.Flow("demo/exactly-once", flow).
			OnSuccess(func(ctx *gotick.Context) error {
				fin.fire()
				return nil
			}).
			OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
				t.Errorf("flow failed on task %q: %v", ts.Key, ts.Errs)
				fin.fire()
				return nil
			})

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tick.StartServer(ctx)
		}()

		if i == 0 {
			// 只有第一台负责触发，其余的都是来抢活的
			if _, err := tick.Trigger(ctx, "demo/exactly-once", nil); err != nil {
				t.Fatalf("Trigger: %v", err)
			}
		}
	}

	select {
	case <-fin.ch:
	case <-time.After(60 * time.Second):
		t.Fatalf("flow did not finish within 60s; counters so far: %v", ran.snapshot())
	}

	// 给可能重复的调度一点时间暴露出来——如果有 task 会被跑第二次，
	// 通常就发生在 flow 完成前后的这一小段时间里。
	time.Sleep(500 * time.Millisecond)

	cancel()
	wg.Wait()

	got := ran.snapshot()
	if len(got) != numTasks {
		t.Errorf("expected %d distinct tasks to run, got %d: %v", numTasks, len(got), got)
	}
	for i := 0; i < numTasks; i++ {
		key := fmt.Sprintf("step-%02d", i)
		switch n := got[key]; {
		case n == 0:
			t.Errorf("task %q never ran (任务丢失)", key)
		case n > 1:
			t.Errorf("task %q ran %d times, expected exactly 1 (任务重复执行)", key, n)
		}
	}
}

// TestParallelTasksAcrossWorkers 覆盖 Async + Wait 这条路径，
// 它比顺序执行的 Task 复杂得多：会起后台 goroutine、靠心跳续期、多次重放收敛。
func TestParallelTasksAcrossWorkers(t *testing.T) {
	mr := miniredis.RunT(t)

	const (
		workers = 3
		numJobs = 6
	)

	ran := newCounter()
	fin := newSignaler()

	flow := func(ctx *gotick.Context) {
		fs := make([]gotick.Future, 0, numJobs)
		for i := 0; i < numJobs; i++ {
			key := fmt.Sprintf("job-%02d", i)
			fs = append(fs, gotick.Async(ctx, key, func(ctx *gotick.TaskContext) (string, error) {
				ran.inc(key)
				return key + "-result", nil
			}))
		}

		gotick.Wait(ctx, 3, fs...)

		gotick.Task(ctx, "collect", func(ctx *gotick.TaskContext) error {
			for _, f := range fs {
				if v := f.(*gotick.FutureT[string]).Value(); v == "" {
					return fmt.Errorf("a parallel job produced an empty result")
				}
			}
			return nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		tick := newRedisServer(t, mr.Addr())
		tick.Flow("demo/parallel", flow).
			OnSuccess(func(ctx *gotick.Context) error {
				fin.fire()
				return nil
			}).
			OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
				t.Errorf("flow failed on task %q: %v", ts.Key, ts.Errs)
				fin.fire()
				return nil
			})

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tick.StartServer(ctx)
		}()

		if i == 0 {
			if _, err := tick.Trigger(ctx, "demo/parallel", nil); err != nil {
				t.Fatalf("Trigger: %v", err)
			}
		}
	}

	select {
	case <-fin.ch:
	case <-time.After(60 * time.Second):
		t.Fatalf("parallel flow did not finish within 60s; counters so far: %v", ran.snapshot())
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()

	got := ran.snapshot()
	for i := 0; i < numJobs; i++ {
		key := fmt.Sprintf("job-%02d", i)
		switch n := got[key]; {
		case n == 0:
			t.Errorf("parallel job %q never ran", key)
		case n > 1:
			t.Errorf("parallel job %q ran %d times, expected exactly 1", key, n)
		}
	}
}

// TestMetaDataRoundTrip 断言同一个 key，MetaData 和 MetaDataAll 读出来的值一致。
func TestMetaDataRoundTrip(t *testing.T) {
	mr := miniredis.RunT(t)

	tick := newRedisServer(t, mr.Addr())
	fin := newSignaler()

	var single string
	var all gotick.MetaData

	tick.Flow("demo/meta", func(ctx *gotick.Context) {
		gotick.Task(ctx, "read", func(ctx *gotick.TaskContext) error {
			single, _ = ctx.MetaData("name")
			all = ctx.MetaDataAll()
			return nil
		})
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	runFlow(t, tick, "demo/meta", gotick.MetaData{"name": "bysir"}, fin, 30*time.Second)

	if single != "bysir" {
		t.Errorf(`MetaData("name") = %q, want %q`, single, "bysir")
	}
	if all["name"] != single {
		t.Errorf("MetaDataAll()[%q] = %q but MetaData(%q) = %q — 同一个 key 两个方法结果不一致",
			"name", all["name"], "name", single)
	}
}

// TestRunIndexTracksRealFlow 验证跑一个真实的 flow 之后，索引里能查到它。
// 这是 UI 列表页的数据来源——没有它，状态都在 Redis 里但没有入口找到。
func TestRunIndexTracksRealFlow(t *testing.T) {
	mr := miniredis.RunT(t)
	tick := newRedisServer(t, mr.Addr())
	fin := newSignaler()

	tick.Flow("demo/indexed", func(ctx *gotick.Context) {
		gotick.Task(ctx, "step-1", func(*gotick.TaskContext) error { return nil })
		gotick.Task(ctx, "step-2", func(*gotick.TaskContext) error { return nil })
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	})

	callId := runFlow(t, tick, "demo/indexed", gotick.MetaData{"name": "bysir"}, fin, 30*time.Second)

	idx := tick.RunIndex()

	flows, err := idx.ListFlows()
	require.NoError(t, err)
	assert.Contains(t, flows, "demo/indexed")

	run, exist, err := idx.GetRun(callId)
	require.NoError(t, err)
	require.True(t, exist, "a completed run must be findable in the index")
	assert.Equal(t, "demo/indexed", run.FlowId)
	assert.Equal(t, gotick.RunStatusDone, run.Status)
	assert.True(t, run.Finished())
	assert.Greater(t, run.Replays, 1, "a two-task flow replays more than once")

	runs, err := idx.ListRuns("demo/indexed", 0, 10)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, callId, runs[0].CallId)
}

// TestRunIndexRecordsFailure 确认失败的实例在列表里就能看出死在哪个 task。
func TestRunIndexRecordsFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	tick := newRedisServer(t, mr.Addr())
	fin := newSignaler()

	tick.Flow("demo/failing", func(ctx *gotick.Context) {
		gotick.Task(ctx, "explodes", func(*gotick.TaskContext) error {
			return fmt.Errorf("disk on fire")
		}, gotick.WithMaxRetry(0))
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		fin.fire()
		return nil
	})

	callId := runFlow(t, tick, "demo/failing", nil, fin, 30*time.Second)

	run, exist, err := tick.RunIndex().GetRun(callId)
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, gotick.RunStatusFailed, run.Status)
	assert.Equal(t, "explodes", run.FailedTask)
	assert.Contains(t, run.Error, "disk on fire")
}

// TestQueueCountDoesNotGrowWithFlows 是这次改动的核心断言。
//
// 早期实现给每个 flow 建一对队列，而消息队列的轮询要遍历所有队列——
// 50 个 flow 就是 100 个队列，完全空闲时也在持续刷 Redis。
// 现在所有 flow 共用一对队列，靠事件里的 flowId 分发。
func TestQueueCountDoesNotGrowWithFlows(t *testing.T) {
	mr := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	tick := newRedisServer(t, mr.Addr())

	const flows = 8
	fin := newSignaler()
	done := newCounter()

	for i := 0; i < flows; i++ {
		id := fmt.Sprintf("demo/flow-%02d", i)
		tick.Flow(id, func(ctx *gotick.Context) {
			gotick.Task(ctx, "step", func(*gotick.TaskContext) error {
				done.inc(id)
				return nil
			})
		}).OnSuccess(func(ctx *gotick.Context) error {
			if len(done.snapshot()) == flows {
				fin.fire()
			}
			return nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = tick.StartServer(ctx)
	}()

	for i := 0; i < flows; i++ {
		_, err := tick.Trigger(ctx, fmt.Sprintf("demo/flow-%02d", i), nil)
		require.NoError(t, err)
	}

	select {
	case <-fin.ch:
	case <-time.After(60 * time.Second):
		t.Fatalf("not all flows finished: %v", done.snapshot())
	}

	queues, err := rdb.SMembers(ctx, "asynq:queues").Result()
	require.NoError(t, err)
	assert.Len(t, queues, 2,
		"%d 个 flow 应该只用 2 个队列（普通 + critical），实际是 %v", flows, queues)
	assert.ElementsMatch(t, []string{"gotick", "gotick_critical"}, queues)

	cancel()
	wg.Wait()
}

// TestSharedQueueRoutesToTheRightWorker 覆盖共用队列带来的新风险：
// 一个 worker 会收到它没有注册的 flow 的事件。
//
// 它绝不能默默 ack——那等于把别人的流程吞掉，那个流程再也不会往下走。
func TestSharedQueueRoutesToTheRightWorker(t *testing.T) {
	mr := miniredis.RunT(t)

	ran := newCounter()
	finA := newSignaler()
	finB := newSignaler()

	// 两台 worker 各自只注册一个 flow，但消费的是同一对队列
	makeWorker := func(flowId string, fin *signaler) *gotick.Server {
		tick := newRedisServer(t, mr.Addr())
		tick.Flow(flowId, func(ctx *gotick.Context) {
			gotick.Task(ctx, "step", func(*gotick.TaskContext) error {
				ran.inc(flowId)
				return nil
			})
		}).OnSuccess(func(ctx *gotick.Context) error {
			fin.fire()
			return nil
		})
		return tick
	}

	workerA := makeWorker("demo/only-on-a", finA)
	workerB := makeWorker("demo/only-on-b", finB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, w := range []*gotick.Server{workerA, workerB} {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.StartServer(ctx)
		}()
	}

	// 从 A 触发两个 flow——其中一个只有 B 认识
	for _, id := range []string{"demo/only-on-a", "demo/only-on-b"} {
		_, err := workerA.Trigger(ctx, id, nil)
		require.NoError(t, err)
	}

	for name, fin := range map[string]*signaler{"only-on-a": finA, "only-on-b": finB} {
		select {
		case <-fin.ch:
		case <-time.After(90 * time.Second):
			t.Fatalf("flow %s 没有跑完——它可能被没注册它的 worker 吞掉了；计数：%v",
				name, ran.snapshot())
		}
	}

	cancel()
	wg.Wait()

	assert.Equal(t, 1, ran.get("demo/only-on-a"))
	assert.Equal(t, 1, ran.get("demo/only-on-b"))
}
