package gotick

import (
	"fmt"
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

func TestRunIndexStatusIndex(t *testing.T) {
	idx := newTestRunIndex()
	require.True(t, idx.StatusFilterSupported(), "MockKvStore 支持 ZRem")

	base := time.Now().Truncate(time.Millisecond)
	require.NoError(t, idx.BeginRun("a", "r1", base))
	require.NoError(t, idx.BeginRun("a", "r2", base.Add(time.Second)))
	require.NoError(t, idx.FinishRun("r2", RunStatusFailed, "task-x", "boom", base.Add(2*time.Second)))

	running, err := idx.ListRunsByStatus("", RunStatusRunning, 0, 10)
	require.NoError(t, err)
	require.Len(t, running, 1)
	assert.Equal(t, "r1", running[0].CallId, "结束的那条必须从 running 集合里搬走")

	failed, err := idx.ListRunsByStatus("", RunStatusFailed, 0, 10)
	require.NoError(t, err)
	require.Len(t, failed, 1)
	assert.Equal(t, "r2", failed[0].CallId)

	n, err := idx.CountRunsByStatus("", RunStatusFailed)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "总数是这个状态下的，不是全部")

	// 按 flow 再筛一次也要走对键
	failed, err = idx.ListRunsByStatus("a", RunStatusFailed, 0, 10)
	require.NoError(t, err)
	require.Len(t, failed, 1)

	// 重复 FinishRun 不该把记录搬丢
	require.NoError(t, idx.FinishRun("r2", RunStatusFailed, "task-x", "boom", base.Add(3*time.Second)))
	failed, err = idx.ListRunsByStatus("", RunStatusFailed, 0, 10)
	require.NoError(t, err)
	assert.Len(t, failed, 1)
}

func TestRunIndexKeyIndex(t *testing.T) {
	kv := store.NewMockKvStore()
	idx := NewKvRunIndex(kv, time.Hour)
	base := time.Now().Truncate(time.Millisecond)

	seed := func(flowId, callId, key string, at time.Time) {
		t.Helper()
		require.NoError(t, NewKvStoreProduct(kv).New(callId).SetKV(RunKeyField, key))
		require.NoError(t, idx.BeginRun(flowId, callId, at))
	}

	seed("shop", "r1", "order-1", base)
	seed("shop", "r2", "order-2", base.Add(time.Second))
	seed("shop", "r3", "order-1", base.Add(2*time.Second)) // 同一个 key 又跑了一遍
	seed("other", "r4", "order-1", base.Add(3*time.Second))

	// 不限 flow：三次调用都绑在 order-1 上，倒序
	runs, err := idx.ListRunsByKey("", "order-1", 0, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"r4", "r3", "r1"}, ids(runs))

	n, err := idx.CountRunsByKey("", "order-1")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)

	// 限定 flow：作用域是 (flowId, key)，不同 flow 用同一个订单号不该混在一起
	runs, err = idx.ListRunsByKey("shop", "order-1", 0, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"r3", "r1"}, ids(runs))

	n, err = idx.CountRunsByKey("shop", "order-1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	// 分页
	runs, err = idx.ListRunsByKey("", "order-1", 1, 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"r3"}, ids(runs))

	// 结束之后仍然查得到：这套索引只写不删，正是它比查绑定表强的地方
	require.NoError(t, idx.FinishRun("r1", RunStatusDone, "", "", base.Add(time.Minute)))
	runs, err = idx.ListRunsByKey("shop", "order-1", 0, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"r3", "r1"}, ids(runs))

	// 没有 key 的调用不该进任何 key 的索引
	require.NoError(t, idx.BeginRun("shop", "no-key", base.Add(4*time.Second)))
	runs, err = idx.ListRunsByKey("", "", 0, 10)
	require.NoError(t, err)
	assert.Len(t, runs, 5, "key 为空等于不筛")
}

func TestRunIndexKeyIndexStaysBounded(t *testing.T) {
	kv := store.NewMockKvStore()
	retain := time.Hour
	idx := NewKvRunIndex(kv, retain)
	now := time.Now().Truncate(time.Millisecond)

	// 同一个 key 反复被复用（定时任务那种），其中一半早于保留期。
	// 键名里含 key 本身，枚举不出来，TrimBefore 碰不到它——
	// 所以必须靠写入时顺手裁剪把它收住。
	for i := 0; i < 10; i++ {
		callId := fmt.Sprintf("c%d", i)
		at := now.Add(-3 * time.Hour).Add(time.Duration(i) * 30 * time.Minute)
		require.NoError(t, NewKvStoreProduct(kv).New(callId).SetKV(RunKeyField, "cron"))
		require.NoError(t, idx.BeginRun("job", callId, at))
	}

	n, err := idx.CountRunsByKey("job", "cron")
	require.NoError(t, err)
	assert.LessOrEqual(t, n, int64(4),
		"保留期内只有最后几次，早于保留期的应该在写入时被裁掉")

	runs, err := idx.ListRunsByKey("job", "cron", 0, 100)
	require.NoError(t, err)
	for _, r := range runs {
		assert.False(t, r.StartedAt.Before(now.Add(-retain)),
			"留下来的都该在保留期内，实际有一条 %s", r.StartedAt)
	}
}

func ids(runs []RunInfo) []string {
	out := make([]string, 0, len(runs))
	for _, r := range runs {
		out = append(out, r.CallId)
	}
	return out
}

func TestRunIndexBackfillsLegacyRecords(t *testing.T) {
	kv := store.NewMockKvStore()
	idx := NewKvRunIndex(kv, time.Hour)
	at := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	// 造一条二级索引上线之前的记录：主索引里有，两套二级索引里都没有。
	// 这就是升级那一刻线上 301 条记录的样子。
	require.NoError(t, NewKvStoreProduct(kv).New("legacy").SetKV(RunKeyField, "ORD-1"))
	require.NoError(t, kv.Set(nil, runInfoKey("legacy"), RunInfo{
		CallId: "legacy", FlowId: "shop", Status: RunStatusDone, StartedAt: at,
	}, time.Hour))
	require.NoError(t, kv.ZAdd(nil, allRunsKey, "legacy", float64(at.UnixMilli())))
	require.NoError(t, kv.ZAdd(nil, flowRunsKey("shop"), "legacy", float64(at.UnixMilli())))

	// 补录之前：主列表看得见，但按状态、按 key 都筛不出来——线上就是这个症状
	done, err := idx.ListRunsByStatus("", RunStatusDone, 0, 10)
	require.NoError(t, err)
	require.Empty(t, done, "补录之前状态索引里没有它")

	// 列出来一次就补上
	runs, err := idx.ListRuns("", 0, 10)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "ORD-1", runs[0].Key, "业务 key 要从 meta 里读出来填好")
	assert.True(t, runs[0].Indexed)

	done, err = idx.ListRunsByStatus("", RunStatusDone, 0, 10)
	require.NoError(t, err)
	require.Len(t, done, 1, "补录之后「只看 done」必须能筛出存量记录")

	n, err := idx.CountRunsByStatus("", RunStatusDone)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "总数也要跟上，否则分页是错的")

	byKey, err := idx.ListRunsByKey("shop", "ORD-1", 0, 10)
	require.NoError(t, err)
	require.Len(t, byKey, 1, "列表显示了业务 key，就必须搜得到，不然界面自相矛盾")

	// 而且要落库，不能每次列表都重补一遍
	stored, exist, err := idx.GetRun("legacy")
	require.NoError(t, err)
	require.True(t, exist)
	assert.True(t, stored.Indexed)
	assert.Equal(t, "ORD-1", stored.Key)
}

func TestRunIndexMarksNewRecordsIndexed(t *testing.T) {
	idx := newTestRunIndex()

	// 新记录在 BeginRun 时就建好了索引，不该再被当成存量记录反复补
	require.NoError(t, idx.BeginRun("shop", "fresh", time.Now()))

	info, _, err := idx.GetRun("fresh")
	require.NoError(t, err)
	assert.True(t, info.Indexed,
		"没打标记的话，每次列表都会为这一行重复写一遍二级索引")
}

func TestRunIndexTrimCoversStatusIndex(t *testing.T) {
	idx := newTestRunIndex()
	base := time.Now().Truncate(time.Millisecond)

	require.NoError(t, idx.BeginRun("a", "old", base))
	require.NoError(t, idx.FinishRun("old", RunStatusDone, "", "", base.Add(time.Second)))
	require.NoError(t, idx.BeginRun("a", "new", base.Add(time.Hour)))

	_, err := idx.TrimBefore("", base.Add(30*time.Minute))
	require.NoError(t, err)
	_, err = idx.TrimBefore("a", base.Add(30*time.Minute))
	require.NoError(t, err)

	// 二级索引不跟着裁的话，它只增不减——过期的成员会一直堆在 done 集合里
	done, err := idx.ListRunsByStatus("", RunStatusDone, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, done)

	n, err := idx.CountRunsByStatus("", RunStatusDone)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// noZRem 内嵌 KVStore 接口，于是只有 KVStore 的方法——ZRem 不在其中。
type noZRem struct{ store.KVStore }

func TestRunIndexWithoutZRemKeepsNoStatusIndex(t *testing.T) {
	idx := NewKvRunIndex(noZRem{KVStore: store.NewMockKvStore()}, time.Hour)
	require.False(t, idx.StatusFilterSupported())

	require.NoError(t, idx.BeginRun("a", "r1", time.Now()))

	// 关键：没有能力时索引压根不写，而不是写一个只增不减的半残索引——
	// 那会让「只看在跑的」把所有跑过的都算进去。
	running, err := idx.ListRunsByStatus("", RunStatusRunning, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, running)
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
