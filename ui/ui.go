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
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net"
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

// versionPlaceholder 是首页里被替换成真实版本号的那个标记。
const versionPlaceholder = "{{version}}"

// Options 配置 UI handler。
type Options struct {
	// RootPath 是 UI 挂载的路径前缀，例如 "/_gotick"。留空表示挂在根路径。
	RootPath string

	// Store 是 gotick 使用的状态存储，UI 从这里读取所有数据。
	Store store.KVStore

	// Index 是运行索引。留空时会基于 Store 自动创建一个。
	Index gotick.RunIndex

	// Auth 包装整个 handler（包括 API），用于接入鉴权。
	//
	// 界面能看到所有 metadata，里面很可能有业务敏感数据。
	// 挂到已有 mux 上时通常直接复用应用自己的鉴权中间件；
	// 只想要一个口令的话用 BasicAuth。
	//
	// 留空时 ListenAndServe 只允许绑定回环地址——见那里的说明。
	Auth func(http.Handler) http.Handler
}

// BasicAuth 返回一个 HTTP Basic 认证中间件，用于 Options.Auth。
//
// 故意只做到这一步：没有账号体系、没有会话、没有登录页。
// 那些属于挂载这个界面的应用，不属于一个工作流库。
//
// Basic 认证的凭据是明文编码的，所以它适合回环地址、内网、或者反向代理背后的 TLS，
// 不适合直接裸奔在公网上。
func BasicAuth(username, password string) func(http.Handler) http.Handler {
	wantUser := []byte(username)
	wantPass := []byte(password)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			// 用常数时间比较，避免从响应耗时里逐字节猜出口令。
			// 两个都要比，不能因为用户名不对就提前返回。
			userOK := subtle.ConstantTimeCompare([]byte(user), wantUser) == 1
			passOK := subtle.ConstantTimeCompare([]byte(pass), wantPass) == 1

			if !ok || !userOK || !passOK {
				w.Header().Set("WWW-Authenticate", `Basic realm="gotick", charset="UTF-8"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type handler struct {
	opt   Options
	index gotick.RunIndex
	mux   *http.ServeMux

	// page 是把版本号填好之后的首页，构造时算一次。
	//
	// 版本在服务端填而不是让前端再要一次接口：省一个请求，任何一条路由进来
	// 都带着它，而且 curl 一下就能看出对面那个界面是哪一版。
	page []byte
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

	page, err := assets.ReadFile("assets/index.html")
	if err != nil {
		return nil, fmt.Errorf("ui: assets missing: %w", err)
	}

	h := &handler{
		opt:   opt,
		index: index,
		mux:   http.NewServeMux(),
		// 用 ReplaceAll 而不是只替换第一处：页面里多写出一个同名标记时，
		// 只替换一处会把替换让给先出现的那个，真正该显示版本号的地方留着占位符
		// 发出去。（这一版就是这么错过一次的。）
		//
		// 版本号来自构建信息、不是用户输入，但还是转义一遍：
		// 往 HTML 里拼字符串这件事不该有例外。
		page: []byte(strings.ReplaceAll(string(page), versionPlaceholder,
			html.EscapeString(gotick.Version()))),
	}
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
//
// 没有配置 Auth 时，它只允许绑定回环地址。这不是给谨慎用户的提示，
// 而是一道拦截：界面会暴露所有 flow 的 metadata，
// 而「把 -addr 改成 0.0.0.0 好让我从别的机器上看一眼」是个太容易做出的动作。
// 要对外提供访问，就得先明确给出凭据。
func ListenAndServe(addr string, opt Options) error {
	if opt.Auth == nil && !IsLoopbackAddr(addr) {
		return fmt.Errorf("ui: refusing to listen on %q without Options.Auth — "+
			"the inspector exposes every flow's metadata; "+
			"use ui.BasicAuth(user, pass), plug in your own middleware, or bind to 127.0.0.1", addr)
	}

	h, err := NewHandler(opt)
	if err != nil {
		return err
	}
	return http.ListenAndServe(addr, h)
}

// IsLoopbackAddr 判断一个监听地址是否只对本机可见。
// 导出是为了让调用方在启动前就知道自己会不会被拦下来。
//
// 判断不了的一律当作对外暴露：这里宁可错误地拦住，也不能错误地放行。
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// 没有端口分隔符时按整体当主机名处理
		host = addr
	}

	switch strings.ToLower(strings.Trim(host, "[]")) {
	case "localhost":
		return true
	case "", "*":
		// ":8088" 表示所有网卡
		return false
	}

	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(h.page)
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

// 状态筛选的作用范围，界面要照实说清楚。
const (
	statusScopeIndex = "index" // 在全部记录里筛，分页和总数都准
	statusScopePage  = "page"  // 只在当前这一页里筛——store 建不了状态索引时的退路
)

func (h *handler) handleRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	flow := q.Get("flow")
	status := q.Get("status")
	callId := strings.TrimSpace(q.Get("call_id"))
	key := strings.TrimSpace(q.Get("key"))

	offset := intParam(q.Get("offset"), 0)
	limit := intParam(q.Get("limit"), defaultPageSize)
	if limit <= 0 || limit > maxPageSize {
		limit = defaultPageSize
	}

	// callId 是索引的主键：一次读就到，翻页对它毫无意义。
	if callId != "" {
		h.lookupByCallId(w, lookup{flow: flow, status: status, callId: callId, key: key})
		return
	}

	var (
		runs  []gotick.RunInfo
		total int64
		scope string
		err   error
	)

	si, hasStatusIndex := h.index.(gotick.StatusFilterIndex)
	ki, hasKeyIndex := h.index.(gotick.KeyFilterIndex)

	switch {
	case key != "" && hasKeyIndex:
		// 业务 key 是最有选择性的条件，用它的索引来翻页。
		//
		// 同时还选了状态时，状态只在这一页里过滤：一个 key 通常只对应一两次调用，
		// 一页装得下，所以这个退让在实践中不会造成翻页问题。scope 照实标出来。
		if status != "" {
			scope = statusScopePage
		}
		runs, err = ki.ListRunsByKey(flow, key, int64(offset), int64(limit))
		if err == nil {
			if status != "" {
				runs = filterByStatus(runs, status)
			}
			total, err = ki.CountRunsByKey(flow, key)
		}

	case key != "":
		// 自定义的 RunIndex 实现没有这个能力。明确报错，而不是返回一页空的——
		// 后者看起来就是「这个 key 不存在」，是个会误导人的答案。
		writeError(w, http.StatusNotImplemented,
			errors.New("this run index cannot filter by key"))
		return

	case status == "":
		runs, err = h.index.ListRuns(flow, int64(offset), int64(limit))
		if err == nil {
			total, err = h.index.CountRuns(flow)
		}

	case hasStatusIndex && si.StatusFilterSupported():
		// 在服务端筛：翻页和总数都是全量范围里的，和不筛的时候完全同构。
		scope = statusScopeIndex
		runs, err = si.ListRunsByStatus(flow, status, int64(offset), int64(limit))
		if err == nil {
			total, err = si.CountRunsByStatus(flow, status)
		}

	default:
		// 退路：store 删不掉 ZSET 成员，建不了状态索引（见 store.ZRemmer）。
		// 只能在当前这一页里过滤，所以 total 仍然是不筛的总数——
		// 界面靠 status_scope 知道这件事，不会把它说成全量筛选的结果。
		scope = statusScopePage
		runs, err = h.index.ListRuns(flow, int64(offset), int64(limit))
		if err == nil {
			runs = filterByStatus(runs, status)
			total, err = h.index.CountRuns(flow)
		}
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, map[string]any{
		"runs":         decorateRuns(runs),
		"total":        total,
		"offset":       offset,
		"limit":        limit,
		"status_scope": scope,
	})
}

// lookup 是一次按 callId 的精确查询的条件。
type lookup struct{ flow, status, callId, key string }

// lookupByCallId 按 callId 精确查找，最多命中一条。
func (h *handler) lookupByCallId(w http.ResponseWriter, l lookup) {
	run, exist, err := h.index.GetRun(l.callId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 其余条件在这一条上过一遍，让组合筛选成立：填了 callId 又填了 key 的时候，
	// 不能只按 callId 认人、返回一个 key 根本不匹配的调用。
	ok := exist &&
		(l.flow == "" || run.FlowId == l.flow) &&
		(l.status == "" || run.Status == l.status) &&
		(l.key == "" || run.Key == l.key)

	runs := []gotick.RunInfo{}
	if ok {
		runs = append(runs, run)
	}

	writeJSON(w, map[string]any{
		"runs":   decorateRuns(runs),
		"total":  len(runs),
		"offset": 0,
		"limit":  len(runs),
	})
}

func filterByStatus(runs []gotick.RunInfo, want string) []gotick.RunInfo {
	kept := runs[:0]
	for _, run := range runs {
		if run.Status == want {
			kept = append(kept, run)
		}
	}
	return kept
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

	// 其次是有明确醒来时刻的：取最早的那个，它决定了整个流程什么时候能往下走
	var next *taskView
	// 无限等信号的单独记着：它没有醒来时刻，排不进上面的比较，
	// 但它恰恰是最需要被报出来的——不报的话界面会说「马上会被 worker 接走」，
	// 而事实是没有 SendSignal 它永远不会动。
	var forever *taskView
	for i := range tasks {
		t := &tasks[i]
		switch t.Status {
		case gotick.TaskStatusSleep, gotick.TaskStatusRetry, gotick.TaskStatusSignal:
		default:
			continue
		}
		if t.RunAt.IsZero() {
			if t.Status == gotick.TaskStatusSignal && forever == nil {
				forever = t
			}
			continue
		}
		if !t.RunAt.After(now) {
			continue
		}
		if next == nil || t.RunAt.Before(next.RunAt) {
			next = t
		}
	}
	if next != nil {
		var state string
		switch next.Status {
		case gotick.TaskStatusRetry:
			state = "retrying"
		case gotick.TaskStatusSignal:
			state = "awaiting_signal" // 带超时的等待，Until 就是超时时刻
		default:
			state = "sleeping"
		}
		return activity{State: state, Task: next.Key, Since: next.StartedAt, Until: next.RunAt}
	}
	if forever != nil {
		// 不给 Until：没有截止时间，界面不该显示一个假的倒计时
		return activity{State: "awaiting_signal", Task: forever.Key, Since: forever.StartedAt}
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

// decorateRuns 是给列表用的。
//
// 它会丢掉 Marks：一次调用最多留 200 条重放记录，一页 50 行就是上万条，
// 而列表页一条都用不上——只有详情页的执行记录面板要。
func decorateRuns(runs []gotick.RunInfo) []runView {
	out := make([]runView, 0, len(runs))
	for _, r := range runs {
		r.Marks = nil
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
		switch {
		case k == gotick.RunKeyField:
			// 业务 key 不是缓存结果。它已经有自己的位置（RunInfo.Key，
			// 界面上是独立一列和概览里的一行），再挂到「Memo / Array / Async
			// 的缓存结果」那个面板里只会让人以为它是某一步的返回值。
		case strings.HasPrefix(k, "__"):
			cached[strings.TrimPrefix(k, "__")] = v
		default:
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
