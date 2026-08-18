package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/store"
)

// newFixture 造一份有内容的存储：一个成功的实例和一个失败的实例。
func newFixture(t *testing.T) (store.KVStore, gotick.RunIndex) {
	t.Helper()

	kv := store.NewMockKvStore()
	idx := gotick.NewKvRunIndex(kv, time.Hour)

	require.NoError(t, idx.RegisterFlow("demo/ok"))
	require.NoError(t, idx.RegisterFlow("demo/bad"))

	base := time.Now().Add(-time.Minute).Truncate(time.Second)

	require.NoError(t, idx.BeginRun("demo/ok", "call-ok", base))
	require.NoError(t, idx.BeginRun("demo/ok", "call-ok", base.Add(time.Second)))
	require.NoError(t, idx.FinishRun("call-ok", gotick.RunStatusDone, "", "", base.Add(2*time.Second)))

	require.NoError(t, idx.BeginRun("demo/bad", "call-bad", base.Add(10*time.Second)))
	require.NoError(t, idx.FinishRun("call-bad", gotick.RunStatusFailed,
		"charge-card", "card declined", base.Add(12*time.Second)))

	okStore := gotick.NewKvStoreProduct(kv).New("call-ok")
	require.NoError(t, okStore.SetNodeStatus("step-1",
		gotick.TaskStatus{}.WithStartedAt(base).MakeDone()))
	require.NoError(t, okStore.SetKV("user", "bysir"))
	require.NoError(t, gotick.SetToStore(okStore, "cached-thing", "some value"))

	badStore := gotick.NewKvStoreProduct(kv).New("call-bad")
	require.NoError(t, badStore.SetNodeStatus("charge-card",
		gotick.TaskStatus{Errs: []string{"card declined"}, RetryCount: 2}.MakeFail(nil)))

	return kv, idx
}

func get(t *testing.T, h http.Handler, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body map[string]any
	if strings.Contains(rec.Header().Get("Content-Type"), "json") {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "response was: %s", rec.Body.String())
	}
	return rec, body
}

func TestNewHandlerRequiresStore(t *testing.T) {
	_, err := NewHandler(Options{})
	require.Error(t, err, "a handler without a store cannot serve anything")
}

func TestServesIndexPage(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	rec, _ := get(t, h, "/")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, rec.Body.String(), "workflow inspector")

	// 版本号在服务端填好，不留占位符出去
	assert.NotContains(t, rec.Body.String(), versionPlaceholder)
	assert.Contains(t, rec.Body.String(),
		`<span class="ver">`+gotick.Version()+`</span>`)
}

func TestListFlows(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	rec, body := get(t, h, "/api/flows")
	require.Equal(t, http.StatusOK, rec.Code)

	flows := body["flows"].([]any)
	require.Len(t, flows, 2)

	byId := map[string]float64{}
	for _, f := range flows {
		m := f.(map[string]any)
		byId[m["id"].(string)] = m["runs"].(float64)
	}
	assert.Equal(t, float64(1), byId["demo/ok"])
	assert.Equal(t, float64(1), byId["demo/bad"])
	assert.Equal(t, float64(2), body["total_runs"])
}

func TestListRuns(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs")
	runs := body["runs"].([]any)
	require.Len(t, runs, 2)

	// 最新的排在前面
	first := runs[0].(map[string]any)
	assert.Equal(t, "call-bad", first["call_id"])
	assert.Equal(t, "failed", first["status"])
	assert.Equal(t, "charge-card", first["failed_task"])
	assert.Equal(t, "card declined", first["error"])
	assert.Equal(t, false, first["running"])
	assert.NotEmpty(t, first["duration_text"], "the UI shows a formatted duration, not raw nanoseconds")

	// 按 flow 过滤
	_, body = get(t, h, "/api/runs?flow=demo/ok")
	runs = body["runs"].([]any)
	require.Len(t, runs, 1)
	assert.Equal(t, "call-ok", runs[0].(map[string]any)["call_id"])
	assert.Equal(t, float64(2), runs[0].(map[string]any)["replays"])

	// 按状态过滤
	_, body = get(t, h, "/api/runs?status=failed")
	runs = body["runs"].([]any)
	require.Len(t, runs, 1)
	assert.Equal(t, "call-bad", runs[0].(map[string]any)["call_id"])
}

// seedRun 按真实顺序造一条运行记录：metadata 和业务 key 先写进 meta，
// 再 BeginRun。索引的快照就是在那一刻拍的，顺序错了就测不出真实行为。
func seedRun(t *testing.T, kv store.KVStore, idx gotick.RunIndex,
	flowId, callId, key string, meta map[string]string, at time.Time) {
	t.Helper()

	st := gotick.NewKvStoreProduct(kv).New(callId)
	for k, v := range meta {
		require.NoError(t, st.SetKV(k, v))
	}
	if key != "" {
		require.NoError(t, st.SetKV("__gotick_key", key))
	}
	require.NoError(t, idx.BeginRun(flowId, callId, at))
}

func TestRunsStatusFilterIsServerSide(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs?status=failed")
	runs := body["runs"].([]any)
	require.Len(t, runs, 1)
	assert.Equal(t, "call-bad", runs[0].(map[string]any)["call_id"])

	// 关键差别：total 是「失败的有多少条」，不是「一共有多少条」。
	// 走本页过滤的老路径这里会是 2，翻页也就无从谈起。
	assert.Equal(t, float64(1), body["total"])
	assert.Equal(t, "index", body["status_scope"])

	// running 集合在 FinishRun 时被搬空了，否则「只看在跑的」等于「看全部」
	_, body = get(t, h, "/api/runs?status=running")
	assert.Empty(t, body["runs"])
	assert.Equal(t, float64(0), body["total"])
}

// noZRemStore 是个删不掉 ZSET 成员的 store：内嵌 KVStore 接口，
// 于是它只有 KVStore 的方法，ZRem 不在其中。
type noZRemStore struct{ store.KVStore }

func TestRunsStatusFilterFallsBackWhenStoreCannotZRem(t *testing.T) {
	kv, _ := newFixture(t)

	limited := noZRemStore{KVStore: kv}
	require.False(t, store.ZRemSupported(limited), "这个 store 不该被认为支持 ZRem")

	h, err := NewHandler(Options{Store: limited, Index: gotick.NewKvRunIndex(limited, time.Hour)})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs?status=failed")
	// 仍然能筛出来，只是范围退到了当前这一页——界面靠 status_scope 照实说明。
	require.Len(t, body["runs"].([]any), 1)
	assert.Equal(t, "page", body["status_scope"])
}

func TestRunsPaging(t *testing.T) {
	kv := store.NewMockKvStore()
	idx := gotick.NewKvRunIndex(kv, time.Hour)
	base := time.Now().Add(-time.Hour).Truncate(time.Second)

	for i := 0; i < 5; i++ {
		seedRun(t, kv, idx, "demo", fmt.Sprintf("call-%d", i), "", nil, base.Add(time.Duration(i)*time.Second))
	}

	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs?limit=2")
	assert.Equal(t, float64(5), body["total"])
	assert.Equal(t, float64(0), body["offset"])
	assert.Equal(t, float64(2), body["limit"])
	assert.Equal(t, []string{"call-4", "call-3"}, callIds(body))

	// 第二页接着往下，不重不漏
	_, body = get(t, h, "/api/runs?limit=2&offset=2")
	assert.Equal(t, []string{"call-2", "call-1"}, callIds(body))

	// 最后一页只剩一条
	_, body = get(t, h, "/api/runs?limit=2&offset=4")
	assert.Equal(t, []string{"call-0"}, callIds(body))

	// 越界不报错，给一页空的
	_, body = get(t, h, "/api/runs?limit=2&offset=99")
	assert.Empty(t, callIds(body))
}

func TestRunsLookupByCallId(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs?call_id=call-bad")
	assert.Equal(t, []string{"call-bad"}, callIds(body))

	// 精确匹配：前缀不算命中
	_, body = get(t, h, "/api/runs?call_id=call-")
	assert.Empty(t, callIds(body))

	// 和其他条件组合起来要成立
	_, body = get(t, h, "/api/runs?call_id=call-bad&status=done")
	assert.Empty(t, callIds(body), "call-bad 是 failed，不该出现在 done 的结果里")

	// 两个框都填了的时候 key 也要生效，不能只按 callId 认人
	_, body = get(t, h, "/api/runs?call_id=call-bad&key=not-its-key")
	assert.Empty(t, callIds(body))
}

func TestRunsFilterByKey(t *testing.T) {
	kv := store.NewMockKvStore()
	idx := gotick.NewKvRunIndex(kv, time.Hour)
	base := time.Now().Add(-time.Hour).Truncate(time.Second)

	require.NoError(t, idx.RegisterFlow("shop/close-order"))
	seedRun(t, kv, idx, "shop/close-order", "call-live", "order-1", nil, base)
	seedRun(t, kv, idx, "shop/close-order", "call-other", "order-2", nil, base.Add(time.Second))

	// 同一个 key 之前跑过一次并且已经结束。绑定表在终态会解绑，所以这条只能靠
	// 按 key 的二级索引找到——它只写不删，结束与否都在里面。
	seedRun(t, kv, idx, "shop/close-order", "call-old", "order-1", nil, base.Add(-time.Hour))
	require.NoError(t, idx.FinishRun("call-old", gotick.RunStatusDone, "", "", base.Add(-time.Hour+time.Second)))

	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs?key=order-1")
	assert.Equal(t, []string{"call-live", "call-old"}, callIds(body),
		"在跑的和已结束的都要找到，且按开始时间倒序")
	assert.Equal(t, float64(2), body["total"], "总数来自索引，是精确的")

	_, body = get(t, h, "/api/runs?key=order-2")
	assert.Equal(t, []string{"call-other"}, callIds(body))

	_, body = get(t, h, "/api/runs?key=nobody-has-this")
	assert.Empty(t, callIds(body))
	assert.Equal(t, float64(0), body["total"])

	// 按 key 也是正经分页，不是「一次全给」
	_, body = get(t, h, "/api/runs?key=order-1&limit=1")
	assert.Equal(t, []string{"call-live"}, callIds(body))
	assert.Equal(t, float64(2), body["total"])
	_, body = get(t, h, "/api/runs?key=order-1&limit=1&offset=1")
	assert.Equal(t, []string{"call-old"}, callIds(body))

	// key + flow 走 flow 维度的那份索引
	_, body = get(t, h, "/api/runs?key=order-1&flow=shop/close-order")
	assert.Equal(t, []string{"call-live", "call-old"}, callIds(body))
	_, body = get(t, h, "/api/runs?key=order-1&flow=other/flow")
	assert.Empty(t, callIds(body))

	// key + status：状态在这一页里筛，scope 要照实标出来
	_, body = get(t, h, "/api/runs?key=order-1&status=done")
	assert.Equal(t, []string{"call-old"}, callIds(body))
	assert.Equal(t, "page", body["status_scope"])
}

// keylessIndex 是个不支持按 key 筛选的 RunIndex：内嵌接口，
// 于是它只有 RunIndex 的方法，ListRunsByKey 不在其中。
type keylessIndex struct{ gotick.RunIndex }

func TestRunsKeyFilterUnsupportedIsAnError(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: keylessIndex{RunIndex: idx}})
	require.NoError(t, err)

	// 关键：不能返回一页空的——那看起来就是「这个 key 不存在」，是个错误答案。
	rec, body := get(t, h, "/api/runs?key=order-1")
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
	assert.Contains(t, body["error"], "cannot filter by key")

	// 不带 key 的查询照常工作
	rec, _ = get(t, h, "/api/runs")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRunsListCarriesMetadata(t *testing.T) {
	kv := store.NewMockKvStore()
	idx := gotick.NewKvRunIndex(kv, time.Hour)

	seedRun(t, kv, idx, "shop/close-order", "call-1", "order-77",
		map[string]string{"user": "bysir", "amount": "42.00"}, time.Now())

	// 框架自己缓存的任务结果不该出现在列表里
	require.NoError(t, gotick.SetToStore(gotick.NewKvStoreProduct(kv).New("call-1"), "step-result", "big blob"))

	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs")
	run := body["runs"].([]any)[0].(map[string]any)

	assert.Equal(t, "order-77", run["key"], "业务 key 要能直接从列表看到")
	assert.Equal(t, map[string]any{"user": "bysir", "amount": "42.00"}, run["meta"])
	assert.Equal(t, float64(2), run["meta_total"])

	// 列表不带重放记录：那是详情页的东西，一页 50 行会是上万条
	assert.Nil(t, run["marks"])

	// 列表读的是 metadata 本身，不是某个时刻的快照——所以流程中途写进去的
	// 也看得到，列表和详情页不会各说一套。（快照那版做不到这一点。）
	require.NoError(t, gotick.NewKvStoreProduct(kv).New("call-1").SetKV("shipped_at", "later"))

	_, body = get(t, h, "/api/runs")
	run = body["runs"].([]any)[0].(map[string]any)
	assert.Equal(t, float64(3), run["meta_total"])
	assert.Contains(t, run["meta"], "shipped_at")
}

func TestRunsListPreviewIsBounded(t *testing.T) {
	kv := store.NewMockKvStore()
	idx := gotick.NewKvRunIndex(kv, time.Hour)

	meta := map[string]string{}
	for i := 0; i < metaPreviewKeys+6; i++ {
		meta[fmt.Sprintf("k%02d", i)] = strings.Repeat("x", metaPreviewValue+40)
	}
	seedRun(t, kv, idx, "shop", "call-1", "", meta, time.Now())

	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	_, body := get(t, h, "/api/runs")
	run := body["runs"].([]any)[0].(map[string]any)

	preview := run["meta"].(map[string]any)
	assert.Len(t, preview, metaPreviewKeys,
		"列表响应不能被一个塞了几百条 metadata 的调用撑大")
	assert.Equal(t, float64(metaPreviewKeys+6), run["meta_total"],
		"总数要照实报，界面才知道还有更多可以点开")

	// 截断后取的是排序靠前的那几条，每次刷新露出来的是同样的键
	assert.Contains(t, preview, "k00")
	assert.NotContains(t, preview, fmt.Sprintf("k%02d", metaPreviewKeys))

	for name, v := range preview {
		assert.Equal(t, metaPreviewValue+1, len([]rune(v.(string))),
			"%s：截到上限再加一个省略号", name)
	}
}

func callIds(body map[string]any) []string {
	runs, _ := body["runs"].([]any)
	out := make([]string, 0, len(runs))
	for _, r := range runs {
		out = append(out, r.(map[string]any)["call_id"].(string))
	}
	return out
}

func TestRunDetail(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	rec, body := get(t, h, "/api/runs/call-ok")
	require.Equal(t, http.StatusOK, rec.Code)

	run := body["run"].(map[string]any)
	assert.Equal(t, "demo/ok", run["flow_id"])
	assert.Equal(t, "done", run["status"])

	tasks := body["tasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	assert.Equal(t, "step-1", task["key"])
	assert.Equal(t, "done", task["status"])

	// metadata 要把用户数据和框架缓存分开，否则界面上全是 __ 前缀的噪音
	meta := body["metadata"].(map[string]any)
	user := meta["user"].(map[string]any)
	cached := meta["cached"].(map[string]any)
	assert.Equal(t, "bysir", user["user"])
	assert.NotContains(t, user, "__cached-thing")
	assert.Contains(t, cached, "cached-thing")
}

func TestRunDetailNotFound(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	rec, body := get(t, h, "/api/runs/does-not-exist")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, body["error"], "does-not-exist")
}

// TestMountedUnderPrefix 覆盖「挂到用户已有的 mux 上」这条最常见的用法。
func TestMountedUnderPrefix(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{RootPath: "/_gotick", Store: kv, Index: idx})
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle("/_gotick/", h)

	rec, body := get(t, mux, "/_gotick/api/runs")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, body["runs"], 2)

	rec, _ = get(t, mux, "/_gotick/")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "workflow inspector")

	// 前缀之外的路径不该被这个 handler 接管
	rec, _ = get(t, mux, "/api/runs")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAuthWrapsEverything 确认鉴权中间件盖住了所有路径，包括 API。
func TestAuthWrapsEverything(t *testing.T) {
	kv, idx := newFixture(t)

	h, err := NewHandler(Options{
		Store: kv,
		Index: idx,
		Auth: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Token") != "secret" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		},
	})
	require.NoError(t, err)

	for _, path := range []string{"/", "/api/flows", "/api/runs", "/api/runs/call-ok"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusUnauthorized, rec.Code, "unauthenticated %s must be rejected", path)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/flows", nil)
	req.Header.Set("X-Token", "secret")
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUnknownPathIs404(t *testing.T) {
	kv, idx := newFixture(t)
	h, err := NewHandler(Options{Store: kv, Index: idx})
	require.NoError(t, err)

	rec, _ := get(t, h, "/nope")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDeriveActivity(t *testing.T) {
	now := time.Now()

	running := gotick.RunInfo{Status: gotick.RunStatusRunning}

	t.Run("已结束的实例直接报终态", func(t *testing.T) {
		got := deriveActivity(gotick.RunInfo{Status: gotick.RunStatusFailed}, []taskView{
			{Key: "a", Status: gotick.TaskStatusSleep, RunAt: now.Add(time.Hour)},
		})
		assert.Equal(t, gotick.RunStatusFailed, got.State,
			"结束了就不该再报告它在睡觉")
	})

	t.Run("正在执行的任务优先", func(t *testing.T) {
		got := deriveActivity(running, []taskView{
			{Key: "sleeper", Status: gotick.TaskStatusSleep, RunAt: now.Add(time.Minute)},
			{Key: "worker", Status: gotick.TaskStatusRunning, StartedAt: now.Add(-time.Second)},
		})
		assert.Equal(t, "running", got.State)
		assert.Equal(t, "worker", got.Task,
			"此刻真正在消耗时间的是正在跑的那个，不是在等的那个")
	})

	t.Run("睡眠中报告最早醒来的那个", func(t *testing.T) {
		got := deriveActivity(running, []taskView{
			{Key: "late", Status: gotick.TaskStatusSleep, StartedAt: now, RunAt: now.Add(time.Hour)},
			{Key: "soon", Status: gotick.TaskStatusSleep, StartedAt: now, RunAt: now.Add(time.Minute)},
		})
		assert.Equal(t, "sleeping", got.State)
		assert.Equal(t, "soon", got.Task, "最早醒来的那个决定了流程什么时候能往下走")
		assert.True(t, got.Until.Equal(now.Add(time.Minute)))
		assert.False(t, got.Since.IsZero(), "没有起点就画不出进度条")
	})

	t.Run("重试等待和睡眠区分开", func(t *testing.T) {
		got := deriveActivity(running, []taskView{
			{Key: "flaky", Status: gotick.TaskStatusRetry, StartedAt: now, RunAt: now.Add(3 * time.Second)},
		})
		assert.Equal(t, "retrying", got.State)
		assert.Equal(t, "flaky", got.Task)
	})

	t.Run("已经到期的等待不算在等", func(t *testing.T) {
		got := deriveActivity(running, []taskView{
			{Key: "overdue", Status: gotick.TaskStatusSleep, RunAt: now.Add(-time.Second)},
		})
		assert.Equal(t, "scheduling", got.State,
			"到点了还没被唤醒，说明它在等调度而不是在睡")
	})

	t.Run("两步之间是调度中", func(t *testing.T) {
		got := deriveActivity(running, []taskView{
			{Key: "a", Status: gotick.TaskStatusDone},
		})
		assert.Equal(t, "scheduling", got.State)
	})
}

func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8088": true,
		"localhost:8088": true,
		"LocalHost:80":   true,
		"[::1]:8088":     true,
		"127.0.0.5:1":    true,

		"0.0.0.0:8088":     false,
		":8088":            false, // 所有网卡
		"192.168.1.10:80":  false,
		"[::]:8088":        false,
		"example.com:8088": false, // 判断不了的一律当作对外暴露
		"":                 false,
	}

	for addr, want := range cases {
		assert.Equal(t, want, IsLoopbackAddr(addr), "addr = %q", addr)
	}
}

// TestListenAndServeRefusesPublicWithoutAuth 是这道拦截的意义所在：
// 「把地址改成 0.0.0.0 好让我从别的机器上看一眼」是个太容易做出的动作，
// 而界面会暴露所有 flow 的 metadata。
func TestListenAndServeRefusesPublicWithoutAuth(t *testing.T) {
	kv, _ := newFixture(t)

	err := ListenAndServe("0.0.0.0:0", Options{Store: kv})
	require.Error(t, err, "没有凭据就不该允许绑定对外地址")
	assert.Contains(t, err.Error(), "refusing to listen")
	assert.Contains(t, err.Error(), "metadata", "错误信息要说清楚为什么危险")
}

func TestBasicAuth(t *testing.T) {
	kv, idx := newFixture(t)

	h, err := NewHandler(Options{
		Store: kv,
		Index: idx,
		Auth:  BasicAuth("admin", "s3cret"),
	})
	require.NoError(t, err)

	call := func(user, pass string, withCreds bool) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/flows", nil)
		if withCreds {
			req.SetBasicAuth(user, pass)
		}
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	assert.Equal(t, http.StatusOK, call("admin", "s3cret", true))
	assert.Equal(t, http.StatusUnauthorized, call("admin", "wrong", true), "密码错")
	assert.Equal(t, http.StatusUnauthorized, call("nobody", "s3cret", true), "用户名错")
	assert.Equal(t, http.StatusUnauthorized, call("", "", false), "没带凭据")

	// 浏览器要靠这个头弹出输入框
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Contains(t, rec.Header().Get("WWW-Authenticate"), "Basic")
}
