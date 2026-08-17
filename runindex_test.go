package gotick

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick/store"
)

func newTestRunIndex() *KvRunIndex {
	return NewKvRunIndex(store.NewMockKvStore(), time.Hour)
}

func TestRunIndexRecordsLifecycle(t *testing.T) {
	idx := newTestRunIndex()
	start := time.Now().Truncate(time.Millisecond)

	require.NoError(t, idx.BeginRun("demo/flow", "call-1", start))

	got, exist, err := idx.GetRun("call-1")
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, "demo/flow", got.FlowId)
	assert.Equal(t, RunStatusRunning, got.Status)
	assert.Equal(t, 1, got.Replays)
	assert.False(t, got.Finished())

	// 每次重放累加计数，但开始时间不变
	require.NoError(t, idx.BeginRun("demo/flow", "call-1", start.Add(time.Second)))
	require.NoError(t, idx.BeginRun("demo/flow", "call-1", start.Add(2*time.Second)))

	got, _, err = idx.GetRun("call-1")
	require.NoError(t, err)
	assert.Equal(t, 3, got.Replays, "each replay must be counted")
	assert.True(t, got.StartedAt.Equal(start), "the start time must not move")

	end := start.Add(5 * time.Second)
	require.NoError(t, idx.FinishRun("call-1", RunStatusFailed, "send-email", "smtp timeout", end))

	got, _, err = idx.GetRun("call-1")
	require.NoError(t, err)
	assert.Equal(t, RunStatusFailed, got.Status)
	assert.Equal(t, "send-email", got.FailedTask)
	assert.Equal(t, "smtp timeout", got.Error)
	assert.True(t, got.Finished())
	assert.Equal(t, 5*time.Second, got.Duration())
}

func TestRunIndexListsNewestFirst(t *testing.T) {
	idx := newTestRunIndex()
	base := time.Now().Truncate(time.Millisecond)

	require.NoError(t, idx.BeginRun("a", "old", base))
	require.NoError(t, idx.BeginRun("a", "mid", base.Add(time.Minute)))
	require.NoError(t, idx.BeginRun("b", "new", base.Add(2*time.Minute)))

	all, err := idx.ListRuns("", 0, 10)
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, []string{"new", "mid", "old"},
		[]string{all[0].CallId, all[1].CallId, all[2].CallId},
		"runs must come back newest first")

	// 按 flow 过滤
	onlyA, err := idx.ListRuns("a", 0, 10)
	require.NoError(t, err)
	require.Len(t, onlyA, 2)
	assert.Equal(t, []string{"mid", "old"}, []string{onlyA[0].CallId, onlyA[1].CallId})

	n, err := idx.CountRuns("a")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	n, err = idx.CountRuns("")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)

	// 分页
	page, err := idx.ListRuns("", 1, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, "mid", page[0].CallId)
}

func TestRunIndexTracksFlows(t *testing.T) {
	idx := newTestRunIndex()

	require.NoError(t, idx.RegisterFlow("demo/one"))
	require.NoError(t, idx.RegisterFlow("demo/two"))
	require.NoError(t, idx.RegisterFlow("demo/one")) // 重复注册不该产生重复条目

	flows, err := idx.ListFlows()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"demo/one", "demo/two"}, flows)
}

func TestRunIndexTrim(t *testing.T) {
	idx := newTestRunIndex()
	base := time.Now().Truncate(time.Millisecond)

	require.NoError(t, idx.BeginRun("a", "old", base))
	require.NoError(t, idx.BeginRun("a", "new", base.Add(time.Hour)))

	removed, err := idx.TrimBefore("a", base.Add(30*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	left, err := idx.ListRuns("a", 0, 10)
	require.NoError(t, err)
	require.Len(t, left, 1)
	assert.Equal(t, "new", left[0].CallId)
}

func TestRunIndexSurvivesExpiredRecord(t *testing.T) {
	// 索引成员还在但详情已经过期时，列表不该出现空洞或报错。
	kv := store.NewMockKvStore()
	idx := NewKvRunIndex(kv, time.Hour)

	require.NoError(t, idx.BeginRun("a", "gone", time.Now()))
	require.NoError(t, kv.Delete(nil, runInfoKey("gone")))

	runs, err := idx.ListRuns("a", 0, 10)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "gone", runs[0].CallId)
	assert.Equal(t, "expired", runs[0].Status)
}

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{500 * time.Microsecond, "500µs"},
		{250 * time.Millisecond, "250ms"},
		{1500 * time.Millisecond, "1.5s"},
		{90 * time.Second, "1m30s"},
		{3*time.Hour + 15*time.Minute, "3h15m"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, FormatDuration(c.d))
	}
}

func TestTaskStatusElapsed(t *testing.T) {
	start := time.Now()

	var s TaskStatus
	assert.Zero(t, s.Elapsed(), "a task that never ran has no elapsed time")

	s = s.WithStartedAt(start)
	assert.Zero(t, s.Elapsed(), "a task still running has no elapsed time yet")

	s = s.MakeDone()
	assert.Greater(t, s.Elapsed(), time.Duration(0))
	assert.False(t, s.EndedAt.IsZero())

	// WithStartedAt 不该覆盖已有的开始时间
	kept := s.WithStartedAt(time.Time{})
	assert.True(t, kept.StartedAt.Equal(start))
}

// FinishRun 应当顺带把过期的索引条目裁掉。
//
// 这一条是为了防回归：TrimBefore 早就写好了，但很长一段时间里没有任何
// 生产代码调用它，索引因此只增不减。
func TestRunIndexTrimsOnFinish(t *testing.T) {
	retain := time.Hour
	idx := NewKvRunIndex(store.NewMockKvStore(), retain)
	now := time.Now().Truncate(time.Millisecond)

	// 一条早就该过期的，和一条还在保留期内的
	stale := now.Add(-3 * retain)
	require.NoError(t, idx.BeginRun("a", "stale", stale))
	require.NoError(t, idx.BeginRun("a", "fresh", now))

	n, err := idx.CountRuns("")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	require.NoError(t, idx.FinishRun("fresh", RunStatusDone, "", "", now))

	// 全局索引和该 flow 的索引都该只剩下没过期的那条
	for _, flow := range []string{"", "a"} {
		n, err := idx.CountRuns(flow)
		require.NoError(t, err)
		assert.Equal(t, int64(1), n, "flow=%q", flow)
	}
	left, err := idx.ListRuns("", 0, 10)
	require.NoError(t, err)
	require.Len(t, left, 1)
	assert.Equal(t, "fresh", left[0].CallId)
}

// 两次 FinishRun 挨着发生时，第二次不该再打一遍 Redis。
func TestRunIndexTrimIsRateLimited(t *testing.T) {
	idx := NewKvRunIndex(store.NewMockKvStore(), time.Hour)
	now := time.Now().Truncate(time.Millisecond)

	require.NoError(t, idx.BeginRun("a", "one", now))
	require.NoError(t, idx.FinishRun("one", RunStatusDone, "", "", now))
	first := idx.lastTrim
	require.False(t, first.IsZero(), "第一次结束就该裁一次")

	require.NoError(t, idx.BeginRun("a", "two", now))
	require.NoError(t, idx.FinishRun("two", RunStatusDone, "", "", now.Add(time.Second)))
	assert.Equal(t, first, idx.lastTrim, "间隔没到 trimInterval，不该再裁")
}

// retain <= 0 表示永久保留，这时一条都不该被裁掉。
func TestRunIndexNoTrimWhenRetainDisabled(t *testing.T) {
	idx := NewKvRunIndex(store.NewMockKvStore(), 0)
	now := time.Now().Truncate(time.Millisecond)

	require.NoError(t, idx.BeginRun("a", "ancient", now.Add(-10000*time.Hour)))
	require.NoError(t, idx.BeginRun("a", "fresh", now))
	require.NoError(t, idx.FinishRun("fresh", RunStatusDone, "", "", now))

	n, err := idx.CountRuns("")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n, "永久保留时不该裁剪")
}
