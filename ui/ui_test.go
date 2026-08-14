package ui

import (
	"encoding/json"
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
