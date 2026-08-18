package gotick

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick/store"
)

func newTestStatusStore() NodeStatusStore {
	return NewKvStoreProduct(store.NewMockKvStore()).New("call-test")
}

func TestTaskStatusRunnable(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		status TaskStatus
		exist  bool
		want   bool
	}{
		{"没有状态的任务可以执行", TaskStatus{}, false, true},
		{"done 不能再执行", TaskStatus{Status: TaskStatusDone}, true, false},
		{"fail 不能再执行", TaskStatus{Status: TaskStatusFail}, true, false},
		{"abort 不能再执行", TaskStatus{Status: TaskStatusAbort}, true, false},
		{
			"心跳还新鲜的 running 不能抢",
			TaskStatus{Status: TaskStatusRunning, Heartbeat: now.Add(-heartbeatInterval)},
			true, false,
		},
		{
			"心跳刚好在超时边界内不能抢",
			TaskStatus{Status: TaskStatusRunning, Heartbeat: now.Add(-runningTimeout)},
			true, false,
		},
		{
			"心跳超时的 running 可以接管",
			TaskStatus{Status: TaskStatusRunning, Heartbeat: now.Add(-runningTimeout - time.Second)},
			true, true,
		},
		{
			"running 但从来没有心跳，可以接管",
			TaskStatus{Status: TaskStatusRunning},
			true, true,
		},
		{
			"retry 到点了可以执行",
			TaskStatus{Status: TaskStatusRetry, RunAt: now.Add(-time.Second)},
			true, true,
		},
		{
			"retry 没到点不能执行",
			TaskStatus{Status: TaskStatusRetry, RunAt: now.Add(time.Second)},
			true, false,
		},
		{
			"sleep 没到点不能执行",
			TaskStatus{Status: TaskStatusSleep, RunAt: now.Add(time.Minute)},
			true, false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, c.status.Runnable(now, c.exist))
		})
	}
}

func TestClaimTaskIsExclusive(t *testing.T) {
	st := newTestStatusStore()
	now := time.Now()

	first, ok, err := claimTask(st, "job", now)
	require.NoError(t, err)
	require.True(t, ok, "the first claim should succeed")
	assert.Equal(t, int64(1), first.Epoch)
	assert.Equal(t, TaskStatusRunning, first.Status)

	// 第二个节点在心跳还活着时不能抢走
	_, ok, err = claimTask(st, "job", now)
	require.NoError(t, err)
	assert.False(t, ok, "a live task must not be claimable by a second worker")
}

func TestClaimTaskTakesOverDeadWorker(t *testing.T) {
	st := newTestStatusStore()
	start := time.Now()

	first, ok, err := claimTask(st, "job", start)
	require.NoError(t, err)
	require.True(t, ok)

	// 执行它的节点失联了：心跳停在 start，时间往前走过了超时
	later := start.Add(runningTimeout + time.Second)

	second, ok, err := claimTask(st, "job", later)
	require.NoError(t, err)
	require.True(t, ok, "a task whose worker stopped heartbeating must be claimable")
	assert.Equal(t, first.Epoch+1, second.Epoch, "taking over must advance the epoch")
}

// TestZombieHeartbeatCannotResurrectDoneTask 是这次重写最重要的一条回归测试。
//
// 场景：节点 A 抢到任务后卡住被判死，节点 B 接管并完成了它，
// 然后 A 的心跳 goroutine 醒过来继续续期。
// 如果心跳不校验 epoch，它会把 B 写好的 done 覆盖回 running——
// 任务实际已经成功，flow 却会永远等下去。
func TestZombieHeartbeatCannotResurrectDoneTask(t *testing.T) {
	st := newTestStatusStore()
	start := time.Now()

	// A 抢到执行权
	a, ok, err := claimTask(st, "job", start)
	require.NoError(t, err)
	require.True(t, ok)

	// A 失联，B 接管
	later := start.Add(runningTimeout + time.Second)
	b, ok, err := claimTask(st, "job", later)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, a.Epoch, b.Epoch)

	// B 完成了任务
	applied, err := settleTask(st, "job", b.Epoch, func(s TaskStatus) TaskStatus {
		return s.MakeDone()
	})
	require.NoError(t, err)
	require.True(t, applied)

	// A 的心跳醒过来了，带着已经作废的 epoch
	held, err := renewHeartbeat(st, "job", a.Epoch, later.Add(time.Second))
	require.NoError(t, err)
	assert.False(t, held, "a heartbeat from a superseded epoch must not be accepted")

	final, exist, err := st.GetNodeStatus("job")
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, TaskStatusDone, final.Status,
		"a stale heartbeat must not flip a finished task back to running")
}

// TestSupersededWorkerCannotWriteResult 同理：被接管的节点即使跑完了也不能写结果，
// 否则两次执行的结果会互相覆盖。
func TestSupersededWorkerCannotWriteResult(t *testing.T) {
	st := newTestStatusStore()
	start := time.Now()

	a, ok, _ := claimTask(st, "job", start)
	require.True(t, ok)

	later := start.Add(runningTimeout + time.Second)
	b, ok, _ := claimTask(st, "job", later)
	require.True(t, ok)

	applied, err := settleTask(st, "job", a.Epoch, func(s TaskStatus) TaskStatus {
		return s.MakeDone()
	})
	require.NoError(t, err)
	assert.False(t, applied, "a superseded worker must not be able to settle the task")

	applied, err = settleTask(st, "job", b.Epoch, func(s TaskStatus) TaskStatus {
		return s.MakeDone()
	})
	require.NoError(t, err)
	assert.True(t, applied, "the current owner must be able to settle the task")
}

// TestUpdateNodeStatusIsAtomic 让一群 goroutine 同时给同一个任务累加重试次数。
//
// 旧实现是「读 → 改 → 写」，并发下更新会互相丢失，重试计数永远到不了上限。
func TestUpdateNodeStatusIsAtomic(t *testing.T) {
	st := newTestStatusStore()

	const writers = 40

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := st.UpdateNodeStatus("counter", func(s TaskStatus, isNew bool) (TaskStatus, bool) {
				s.RetryCount++
				return s, true
			})
			assert.NoError(t, err)
		}()
	}
	close(start)
	wg.Wait()

	got, exist, err := st.GetNodeStatus("counter")
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, writers, got.RetryCount, "concurrent updates must not be lost")
}

// TestUpdateNodeStatusRespectsAbort 确认 commit=false 时不会写入。
func TestUpdateNodeStatusRespectsAbort(t *testing.T) {
	st := newTestStatusStore()

	require.NoError(t, st.SetNodeStatus("job", TaskStatus{Status: TaskStatusDone}))

	_, err := st.UpdateNodeStatus("job", func(s TaskStatus, isNew bool) (TaskStatus, bool) {
		return s.MakeRunning(time.Now()), false
	})
	require.NoError(t, err)

	got, _, err := st.GetNodeStatus("job")
	require.NoError(t, err)
	assert.Equal(t, TaskStatusDone, got.Status, "an aborted update must leave the value untouched")
}

func newTestContext() *Context {
	return &Context{
		Context: context.Background(),
		CallId:  "call-test",
		store:   newTestStatusStore(),
	}
}

// catchBreak 捕获 fn 抛出的断点。fn 没有中断时返回 nil。
func catchBreak(t *testing.T, fn func()) (bp Breakpoint) {
	t.Helper()

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		b, ok := r.(Breakpoint)
		if !ok {
			t.Fatalf("expected a Breakpoint panic, got %#v", r)
		}
		bp = b
	}()

	fn()
	return nil
}

// TestMemoDoesNotCacheFailure 是 Memo 那条 Critical 的回归测试。
//
// 旧实现是 `t, _ := build()`：build 失败时把零值当成功结果永久缓存，
// 之后每次重放都命中缓存返回零值，错误被静默吞掉、永不重试。
func TestMemoDoesNotCacheFailure(t *testing.T) {
	ctx := newTestContext()

	bp := catchBreak(t, func() {
		Memo(ctx, "cfg", func() (string, error) {
			return "", errors.New("network down")
		})
	})

	require.NotNil(t, bp, "a failing Memo must break, not return a zero value")
	assert.IsType(t, &breakRetry{}, bp, "a failing Memo should schedule a retry")

	_, exist, err := GetFromStore[string](ctx.store, "cfg")
	require.NoError(t, err)
	assert.False(t, exist, "a failed Memo must not leave anything in the cache")
}

func TestMemoFailsAfterMaxRetry(t *testing.T) {
	ctx := newTestContext()
	require.NoError(t, ctx.store.SetNodeStatus("cfg", TaskStatus{
		Status:     TaskStatusRetry,
		RetryCount: 2,
	}))

	bp := catchBreak(t, func() {
		Memo(ctx, "cfg", func() (string, error) {
			return "", errors.New("still down")
		}, WithMaxRetry(2))
	})

	require.NotNil(t, bp)
	assert.IsType(t, &breakFail{}, bp, "Memo must give up once RetryCount reaches MaxRetry")
}

func TestMemoCachesSuccess(t *testing.T) {
	ctx := newTestContext()

	calls := 0
	got := Memo(ctx, "cfg", func() (string, error) {
		calls++
		return "value", nil
	})
	assert.Equal(t, "value", got)

	// 换一个 Context 模拟下一次重放
	next := &Context{Context: context.Background(), CallId: ctx.CallId, store: ctx.store}
	again := Memo(next, "cfg", func() (string, error) {
		calls++
		return "different", nil
	})

	assert.Equal(t, "value", again, "a replay must see the cached value, not rebuild it")
	assert.Equal(t, 1, calls, "build must run exactly once")
}

// TestTaskStopsAfterFailure 是「上游失败、下游照跑」那条 High 的回归测试。
//
// 旧实现只处理 done 和 retry，遇到 fail 直接落到函数末尾正常返回，
// 于是任何一次重放都会跳过失败的 task，继续执行它后面的 task。
func TestTaskStopsAfterFailure(t *testing.T) {
	ctx := newTestContext()
	require.NoError(t, ctx.store.SetNodeStatus("upstream", TaskStatus{
		Status: TaskStatusFail,
		Errs:   []string{"boom"},
	}))

	ran := false
	bp := catchBreak(t, func() {
		Task(ctx, "upstream", func(*TaskContext) error {
			ran = true
			return nil
		})
	})

	require.NotNil(t, bp, "a failed task must stop the replay, not fall through")
	assert.IsType(t, &breakFail{}, bp)
	assert.False(t, ran, "a failed task must not be executed again")
}

func TestTaskStopsAfterAbort(t *testing.T) {
	ctx := newTestContext()
	require.NoError(t, ctx.store.SetNodeStatus("t", TaskStatus{Status: TaskStatusAbort}))

	bp := catchBreak(t, func() {
		Task(ctx, "t", func(*TaskContext) error { return nil })
	})

	require.NotNil(t, bp)
	assert.IsType(t, &breakAbort{}, bp)
}

func TestTaskWaitsForRetryBackoff(t *testing.T) {
	ctx := newTestContext()
	require.NoError(t, ctx.store.SetNodeStatus("t", TaskStatus{
		Status: TaskStatusRetry,
		RunAt:  time.Now().Add(30 * time.Second),
	}))

	ran := false
	bp := catchBreak(t, func() {
		Task(ctx, "t", func(*TaskContext) error {
			ran = true
			return nil
		})
	})

	require.NotNil(t, bp)
	assert.IsType(t, &breakWait{}, bp, "a task in backoff must wait rather than run early")
	assert.False(t, ran)
}

// TestDuplicateKeyPanics 覆盖 key 重名检测。
//
// 两个 API 用同一个 key 会共用同一份状态并互相覆盖，而且完全没有报错。
// 这是确定性的编码错误，应该在第一次重放就响亮地失败。
func TestDuplicateKeyPanics(t *testing.T) {
	ctx := newTestContext()

	ctx.markKeyUsed("Task", "shared")

	assert.PanicsWithValue(t,
		`gotick: duplicate key "shared" (used by Task and Memo). `+
			"每个 key 在一个 flow 里必须唯一，否则它们会共用同一份状态并互相覆盖。"+
			"在循环里请用 Array/Sequence 生成的 key，不要写死。",
		func() { ctx.markKeyUsed("Memo", "shared") })

	// 不同的 key 不受影响
	assert.NotPanics(t, func() { ctx.markKeyUsed("Memo", "other") })
}

// TestSequenceStateRoundTrips 覆盖 Sequence 的持久化。
//
// 旧实现写用 SetKV(name)、读用 GetFromStore(name)，后者会再加一层 "__" 前缀，
// 于是状态永远读不回来；而且序列化的是整个 SequenceWrap，
// 恢复出来的对象 ctx 是 nil，一旦读回来就会空指针。
func TestSequenceStateRoundTrips(t *testing.T) {
	st := newTestStatusStore()

	first := Sequence(&Context{Context: context.Background(), store: st}, "seq", 5)
	require.Equal(t, -1, first.Current)

	require.True(t, first.Next())
	require.Equal(t, 0, first.Current)
	require.True(t, first.Next())
	require.Equal(t, 1, first.Current)

	// 模拟一次重放：新的 Context，从存储里恢复。
	//
	// Next() 存的是自增之前的序号，所以循环体在跑第 k 轮时，存储里是 k-1。
	// 恢复后会重跑第 k 轮——因为我们无法知道它上次跑完没有。
	// 这是对的：循环体里的 Task 靠自己的状态保证不会重复执行。
	restored := Sequence(&Context{Context: context.Background(), store: st}, "seq", 5)
	assert.Equal(t, 0, restored.Current, "the sequence must resume from the last checkpoint")
	assert.Equal(t, 5, restored.max, "the bound must survive the round trip")
	assert.NotNil(t, restored.ctx, "the restored wrapper must have a usable Context")

	// 恢复出来的对象必须能继续用（旧实现反序列化后 ctx 为 nil，这里会空指针）
	assert.NotPanics(t, func() { restored.Next() })
	assert.Equal(t, 1, restored.Current, "the in-flight iteration is replayed")
}

func TestNewClientAcceptsRedisClientWithoutURL(t *testing.T) {
	// 文档写明「RedisURL 未设置时使用 RedisClient」，
	// 旧实现无条件先 ParseURL 并在失败时 panic，这条路径 100% 崩溃。
	c, err := NewClient(NewClientConfig{
		RedisClient: redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"}),
	})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestNewClientRejectsBadURL(t *testing.T) {
	_, err := NewClient(NewClientConfig{RedisURL: "not-a-redis-url"})
	require.Error(t, err, "an invalid URL must be reported, not panicked")
}

// TestNewClientFromInjectedQueue 覆盖「没有 Redis 也能构造 Client」。
//
// 之前只能从 RedisURL / RedisClient 构造，于是在没有 Redis 的环境里
// （测试、本地开发、降级启动）根本拿不到 Client，只能把它置为 nil。
func TestNewClientFromInjectedQueue(t *testing.T) {
	q := store.NewMockRedisDelayedQueue()
	c := NewClientFrom(NewClientParams{DelayedQueue: q})
	require.NotNil(t, c)

	callId, err := c.Trigger(context.Background(), "demo/flow", MetaData{"k": "v"}, 0)
	require.NoError(t, err)
	assert.NotEmpty(t, callId, "Trigger must work without any Redis")
}

// TestReplayLeaseIsExclusive 覆盖「同一个 callId 同时只能有一个节点重放」。
//
// 消息队列是 at-least-once 的，任何一次重复投递都会让两个节点同时重放同一个 callId，
// 而 Task / Memo / Array 只看状态、没有抢占保护，于是会被并发执行两次。
func TestReplayLeaseIsExclusive(t *testing.T) {
	st := newTestStatusStore()

	ok, err := st.AcquireLease("worker-a", replayLeaseTTL)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = st.AcquireLease("worker-b", replayLeaseTTL)
	require.NoError(t, err)
	assert.False(t, ok, "a second worker must not be able to replay the same call")

	// 别人的 token 不能续期，也不能释放
	held, err := st.RenewLease("worker-b", replayLeaseTTL)
	require.NoError(t, err)
	assert.False(t, held)

	require.NoError(t, st.ReleaseLease("worker-b"))
	ok, err = st.AcquireLease("worker-b", replayLeaseTTL)
	require.NoError(t, err)
	assert.False(t, ok, "releasing with the wrong token must not free the lease")

	// 持有者释放后别人才能接手
	require.NoError(t, st.ReleaseLease("worker-a"))
	ok, err = st.AcquireLease("worker-b", replayLeaseTTL)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestLeaseRenewalStopsWhenLost(t *testing.T) {
	st := newTestStatusStore()

	ok, err := st.AcquireLease("worker-a", replayLeaseTTL)
	require.NoError(t, err)
	require.True(t, ok)

	stop := startLeaseRenewal(st, "worker-a")

	// stop 必须等续期 goroutine 真正退出，调用后不会再有写入
	stop()
	stop() // 幂等

	require.NoError(t, st.ReleaseLease("worker-a"))
}

// TestExpireAllKeepsDataReadable 确认结束的 flow 是「设置过期」而不是「立即删除」。
//
// 直接删除的话，一条迟到的重复事件会看不到任何状态，把整个 flow 从头再跑一遍。
func TestExpireAllKeepsDataReadable(t *testing.T) {
	st := newTestStatusStore()

	require.NoError(t, st.SetNodeStatus("t1", TaskStatus{Status: TaskStatusDone}))
	require.NoError(t, st.SetKV("name", "bysir"))

	require.NoError(t, st.ExpireAll(time.Hour))

	got, exist, err := st.GetNodeStatus("t1")
	require.NoError(t, err)
	require.True(t, exist, "data must stay readable during the retention window")
	assert.Equal(t, TaskStatusDone, got.Status)

	v, ok, err := st.GetKV("name")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "bysir", v)
}

func TestWaitDelay(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

	assert.Equal(t, waitRecheckDelay, waitDelay(time.Time{}, now),
		"with no scheduled retry, fall back to the recheck interval")

	assert.Equal(t, 5*time.Second, waitDelay(now.Add(5*time.Second), now),
		"wait exactly until the next retry is due")

	assert.Equal(t, time.Duration(0), waitDelay(now.Add(-time.Second), now),
		"an overdue retry should be picked up immediately")
}

// TestMakeSleepKeepsItsStartingPoint 覆盖「重放不能重置睡眠起点」。
//
// Sleep 没到期时每次重放都会用剩余时长重新 BreakSleep，调度器于是会再调一次
// MakeSleep。如果那里无条件写 StartedAt，界面上的睡眠进度会一直往回跳，
// 而且「已经睡了多久」这个信息会永远拿不到。
func TestMakeSleepKeepsItsStartingPoint(t *testing.T) {
	wake := time.Now().Add(time.Minute)

	first := TaskStatus{}.MakeSleep(wake)
	require.Equal(t, TaskStatusSleep, first.Status)
	require.False(t, first.StartedAt.IsZero(), "第一次进入 sleep 要记下起点")
	require.True(t, first.RunAt.Equal(wake))

	time.Sleep(5 * time.Millisecond)

	// 模拟一次重放：还在睡，用剩余时长重新 BreakSleep
	second := first.MakeSleep(wake)
	assert.True(t, second.StartedAt.Equal(first.StartedAt),
		"重放不能把睡眠起点往后推，否则进度会一直重置")

	// 但从别的状态进入 sleep 时应该重新计时
	afterRetry := TaskStatus{Status: TaskStatusRetry, StartedAt: time.Now().Add(-time.Hour)}.MakeSleep(wake)
	assert.True(t, afterRetry.StartedAt.After(first.StartedAt),
		"从其他状态进入 sleep 应该重新开始计时")
}

// TestSleepSpinsThroughTinyRemainders 覆盖「快到点了就地等，不要再绕一圈队列」。
//
// 唤醒事件常常比预定时刻早到几十到几百毫秒。早期实现遇到这种情况会用剩余时长
// 重新 BreakSleep，而重新入队要等消息队列把任务从「已排期」搬回「待执行」，
// 实测是 5 秒级的开销——于是每个 Sleep 都白白多花 5 秒、多一次重放。
func TestSleepSpinsThroughTinyRemainders(t *testing.T) {
	ctx := newTestContext()

	// 只差一点点就到点：应该就地等完，把 task 标记为 done，不再中断
	almost := time.Now().Add(60 * time.Millisecond)
	require.NoError(t, ctx.store.SetNodeStatus("nap", TaskStatus{
		Status: TaskStatusSleep,
		RunAt:  almost,
	}))

	start := time.Now()
	bp := catchBreak(t, func() { Sleep(ctx, "nap", time.Hour) })
	elapsed := time.Since(start)

	assert.Nil(t, bp, "只差几十毫秒时不该再中断一次，绕队列比就地等贵得多")
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "但也必须真的等满")

	got, _, err := ctx.store.GetNodeStatus("nap")
	require.NoError(t, err)
	assert.Equal(t, TaskStatusDone, got.Status)
}

func TestSleepYieldsForLongRemainders(t *testing.T) {
	ctx := newTestContext()

	require.NoError(t, ctx.store.SetNodeStatus("nap", TaskStatus{
		Status: TaskStatusSleep,
		RunAt:  time.Now().Add(time.Hour),
	}))

	start := time.Now()
	bp := catchBreak(t, func() { Sleep(ctx, "nap", time.Hour) })

	require.NotNil(t, bp, "还早得很就必须让出 worker，不能占着它干等")
	assert.IsType(t, &breakSleep{}, bp)
	assert.Less(t, time.Since(start), shortWaitTolerance, "让出应该是立刻的")
}

// 负数不代表「无限重试」。判定是 retryCount >= maxRetry，而首次执行时
// retryCount 就是 0，所以传负数的实际效果和 0 一样：第一次失败就放弃。
// 这一点太容易误会，用测试钉住「夹到 0」这个行为。
func TestWithMaxRetryClampsNegative(t *testing.T) {
	for _, n := range []int{-1, -5, -1 << 30} {
		got := TaskOptions{WithMaxRetry(n)}.build()
		assert.Equal(t, 0, got.MaxRetry, "WithMaxRetry(%d) 应当被夹到 0", n)
	}

	// 非负值原样保留，默认值也别被这次改动带跑
	assert.Equal(t, 0, TaskOptions{WithMaxRetry(0)}.build().MaxRetry)
	assert.Equal(t, 7, TaskOptions{WithMaxRetry(7)}.build().MaxRetry)
	assert.Equal(t, defaultMaxRetry, TaskOptions{}.build().MaxRetry)
}
