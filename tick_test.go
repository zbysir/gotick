package gotick

import (
	"sync"
	"testing"
	"time"

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

func TestWaitDelay(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

	assert.Equal(t, waitRecheckDelay, waitDelay(time.Time{}, now),
		"with no scheduled retry, fall back to the recheck interval")

	assert.Equal(t, 5*time.Second, waitDelay(now.Add(5*time.Second), now),
		"wait exactly until the next retry is due")

	assert.Equal(t, time.Duration(0), waitDelay(now.Add(-time.Second), now),
		"an overdue retry should be picked up immediately")
}
