// Package ui 提供一个内嵌的 Web 界面，用于查看 gotick 的流程执行情况。
//
// 它是一个普通的 http.Handler，有三种用法，都不需要新部署任何服务：
//
//	// 一、挂到你已有的 mux 上（最常见）
//	h, _ := ui.NewHandler(ui.Options{RootPath: "/_gotick", Store: kv})
//	mux.Handle("/_gotick/", h)
//
//	// 二、进程里没有 HTTP 服务时，让它自己起一个端口
//	go ui.ListenAndServe("127.0.0.1:6060", ui.Options{Store: kv})
//
//	// 三、完全独立运行，只连 Redis（见 cmd/gotick ui）
//
// 三种模式共用同一个 handler，因为数据源是 Redis 而不是进程：
// UI 不需要和正在跑的 worker 待在一起。
//
// 这个包被单独拆出来是有意的：前端资源通过 go:embed 打进二进制，
// 不 import 它的人二进制不会变大一个字节。
package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/store"
)

//go:embed assets
var assets embed.FS

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

// Options 配置 UI handler。
type Options struct {
	// RootPath 是 UI 挂载的路径前缀，例如 "/_gotick"。留空表示挂在根路径。
	RootPath string

	// Store 是 gotick 使用的状态存储，UI 从这里读取所有数据。
	Store store.KVStore

	// Index 是运行索引。留空时会基于 Store 自动创建一个。
	Index gotick.RunIndex

	// ReadOnly 为 false 时才允许写操作。默认只读。
	//
	// UI 能看到所有 metadata，里面很可能有业务敏感数据，
	// 所以鉴权应该由调用方通过 Auth 提供，这里不做任何假设。
	ReadOnly bool

	// Auth 包装整个 handler，用于接入调用方自己的鉴权中间件。
	Auth func(http.Handler) http.Handler
}

type handler struct {
	opt   Options
	index gotick.RunIndex
	mux   *http.ServeMux
}

// NewHandler 创建 UI 的 http.Handler。
func NewHandler(opt Options) (http.Handler, error) {
	if opt.Store == nil {
		return nil, errors.New("ui: Options.Store is required")
	}

	index := opt.Index
	if index == nil {
		// 保留期只影响写入，这里只读，传多少都不影响结果
		index = gotick.NewKvRunIndex(opt.Store, 7*24*time.Hour)
	}

	h := &handler{opt: opt, index: index, mux: http.NewServeMux()}
	h.routes()

	var out http.Handler = h
	if prefix := strings.TrimSuffix(opt.RootPath, "/"); prefix != "" {
		out = http.StripPrefix(prefix, out)
	}
	if opt.Auth != nil {
		out = opt.Auth(out)
	}
	return out, nil
}

// ListenAndServe 在指定地址上单独起一个 HTTP 服务跑 UI。
//
// 适合纯 worker 进程——它们本身没有 HTTP 服务。
// 默认只绑定回环地址，不要暴露到公网。
func ListenAndServe(addr string, opt Options) error {
	h, err := NewHandler(opt)
	if err != nil {
		return err
	}
	return http.ListenAndServe(addr, h)
}

func (h *handler) routes() {
	h.mux.HandleFunc("/api/flows", h.handleFlows)
	h.mux.HandleFunc("/api/runs", h.handleRuns)
	h.mux.HandleFunc("/api/runs/", h.handleRunDetail)
	h.mux.HandleFunc("/", h.handleIndex)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}

	page, err := assets.ReadFile("assets/index.html")
	if err != nil {
		http.Error(w, "ui assets missing", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(page)
}

func (h *handler) handleFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := h.index.ListFlows()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	sort.Strings(flows)

	type flowRow struct {
		Id    string `json:"id"`
		Runs  int64  `json:"runs"`
		Error string `json:"error,omitempty"`
	}

	rows := make([]flowRow, 0, len(flows))
	for _, id := range flows {
		n, err := h.index.CountRuns(id)
		if err != nil {
			rows = append(rows, flowRow{Id: id, Error: err.Error()})
			continue
		}
		rows = append(rows, flowRow{Id: id, Runs: n})
	}

	total, err := h.index.CountRuns("")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, map[string]any{"flows": rows, "total_runs": total})
}

func (h *handler) handleRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	flow := q.Get("flow")
	offset := intParam(q.Get("offset"), 0)
	limit := intParam(q.Get("limit"), defaultPageSize)
	if limit <= 0 || limit > maxPageSize {
		limit = defaultPageSize
	}

	runs, err := h.index.ListRuns(flow, int64(offset), int64(limit))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 状态过滤在这一页之内做。真正的按状态索引要等事件流那一层，
	// 现在先让「只看失败的」这个最常用的诉求可用。
	if want := q.Get("status"); want != "" {
		filtered := runs[:0]
		for _, run := range runs {
			if run.Status == want {
				filtered = append(filtered, run)
			}
		}
		runs = filtered
	}

	total, err := h.index.CountRuns(flow)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, map[string]any{
		"runs":   decorateRuns(runs),
		"total":  total,
		"offset": offset,
		"limit":  limit,
	})
}

func (h *handler) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	callId := strings.TrimPrefix(r.URL.Path, "/api/runs/")
	callId = strings.Trim(callId, "/")
	if callId == "" {
		writeError(w, http.StatusBadRequest, errors.New("missing callId"))
		return
	}

	run, exist, err := h.index.GetRun(callId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !exist {
		// 索引里没有不代表数据没有：老的实例可能是在索引上线之前跑的。
		run = gotick.RunInfo{CallId: callId, Status: "unknown"}
	}

	st := gotick.NewKvStoreProduct(h.opt.Store).New(callId)

	statuses, err := st.GetAllNodeStatus()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	meta, err := st.GetKVAll()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if !exist && len(statuses) == 0 && len(meta) == 0 {
		writeError(w, http.StatusNotFound, fmt.Errorf("no state found for call %q", callId))
		return
	}

	tasks := decorateTasks(statuses)

	writeJSON(w, map[string]any{
		"run":      decorateRun(run),
		"tasks":    tasks,
		"activity": deriveActivity(run, tasks),
		"metadata": splitMeta(meta),
	})
}

// activity 回答「这次调用此刻在干什么」。
//
// 光有一张任务表看不出这个：一个正在 sleep 的流程，表里只是某一行的状态是 sleep
// 加一个孤零零的时间戳，得自己去比对当前时间才知道它还要睡多久。
type activity struct {
	// State: running / sleeping / retrying / scheduling / 或者 run 的终态
	State string `json:"state"`
	Task  string `json:"task,omitempty"`
	// Since 这个状态是什么时候开始的，Until 是预计什么时候结束。
	Since time.Time `json:"since,omitempty"`
	Until time.Time `json:"until,omitempty"`
}

func deriveActivity(run gotick.RunInfo, tasks []taskView) activity {
	if run.Finished() {
		return activity{State: run.Status}
	}

	now := time.Now()

	// 正在跑的任务优先——那是此刻真正在消耗时间的东西
	for _, t := range tasks {
		if t.Status == gotick.TaskStatusRunning {
			return activity{State: "running", Task: t.Key, Since: t.StartedAt}
		}
	}

	// 其次是等待中的：取最早醒来的那个，它决定了整个流程什么时候能往下走
	var next *taskView
	for i := range tasks {
		t := &tasks[i]
		switch t.Status {
		case gotick.TaskStatusSleep, gotick.TaskStatusRetry:
		default:
			continue
		}
		if t.RunAt.IsZero() || !t.RunAt.After(now) {
			continue
		}
		if next == nil || t.RunAt.Before(next.RunAt) {
			next = t
		}
	}
	if next != nil {
		state := "sleeping"
		if next.Status == gotick.TaskStatusRetry {
			state = "retrying"
		}
		return activity{State: state, Task: next.Key, Since: next.StartedAt, Until: next.RunAt}
	}

	// 没有任何任务在跑也没有在等，说明正处在两步之间的调度间隙
	return activity{State: "scheduling"}
}

// runView 是 RunInfo 加上给前端算好的派生字段。
type runView struct {
	gotick.RunInfo
	DurationMs   int64  `json:"duration_ms"`
	DurationText string `json:"duration_text"`
	Running      bool   `json:"running"`
}

func decorateRun(r gotick.RunInfo) runView {
	d := r.Duration()
	return runView{
		RunInfo:      r,
		DurationMs:   d.Milliseconds(),
		DurationText: gotick.FormatDuration(d),
		Running:      !r.Finished(),
	}
}

func decorateRuns(runs []gotick.RunInfo) []runView {
	out := make([]runView, 0, len(runs))
	for _, r := range runs {
		out = append(out, decorateRun(r))
	}
	return out
}

type taskView struct {
	Key          string    `json:"key"`
	Status       string    `json:"status"`
	RetryCount   int       `json:"retry_count"`
	Errs         []string  `json:"errs,omitempty"`
	RunAt        time.Time `json:"run_at,omitempty"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	EndedAt      time.Time `json:"ended_at,omitempty"`
	Heartbeat    time.Time `json:"heartbeat,omitempty"`
	Epoch        int64     `json:"epoch,omitempty"`
	DurationMs   int64     `json:"duration_ms"`
	DurationText string    `json:"duration_text"`
}

func decorateTasks(statuses map[string]gotick.TaskStatus) []taskView {
	out := make([]taskView, 0, len(statuses))
	for key, s := range statuses {
		d := s.Elapsed()
		out = append(out, taskView{
			Key:          key,
			Status:       s.Status,
			RetryCount:   s.RetryCount,
			Errs:         s.Errs,
			RunAt:        s.RunAt,
			StartedAt:    s.StartedAt,
			EndedAt:      s.EndedAt,
			Heartbeat:    s.Heartbeat,
			Epoch:        s.Epoch,
			DurationMs:   d.Milliseconds(),
			DurationText: gotick.FormatDuration(d),
		})
	}

	// 按开始时间排序，没有时间的排在后面并按 key 排，保证顺序稳定。
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.StartedAt.IsZero() && b.StartedAt.IsZero():
			return a.Key < b.Key
		case a.StartedAt.IsZero():
			return false
		case b.StartedAt.IsZero():
			return true
		case !a.StartedAt.Equal(b.StartedAt):
			return a.StartedAt.Before(b.StartedAt)
		default:
			return a.Key < b.Key
		}
	})

	return out
}

// splitMeta 把用户的 metadata 和框架缓存的任务结果分开展示，
// 否则界面上全是 "__xxx" 的噪音。
func splitMeta(meta map[string]string) map[string]any {
	user := map[string]string{}
	cached := map[string]string{}

	for k, v := range meta {
		if strings.HasPrefix(k, "__") {
			cached[strings.TrimPrefix(k, "__")] = v
		} else {
			user[k] = v
		}
	}

	return map[string]any{"user": user, "cached": cached}
}

func intParam(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return def
	}
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// 头已经发出去了，只能记一笔
		return
	}
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
