package gotick

import (
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/zbysir/gotick/internal/pkg/flow"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MetaData map[string]string

type Context struct {
	context.Context
	CallId  string
	store   NodeStatusStore
	collect func(typ string, key string) bool // 预运行来生成 flow 图
	s       AsyncQueue

	lock sync.Mutex
}

func (c *Context) MetaDataAll() MetaData {
	if c.store == nil {
		return nil
	}
	m, err := c.store.GetKVAll()
	if err != nil {
		panic(err)
	}

	meta := MetaData{}
	for k, v := range m {
		if strings.HasPrefix(k, "__") {
			continue
		}
		meta[k] = v
	}

	return meta
}

func (c *Context) SetMetaData(k, v string) {
	err := c.store.SetKV(k, v)
	if err != nil {
		panic(err)
	}
}

func (c *Context) MetaData(k string) (string, bool) {
	v, ok, err := c.store.GetKV(k)
	if err != nil {
		panic(err)
	}

	return v, ok
}

type SequenceWrap struct {
	Current int
	max     int
	name    string
	ctx     *Context
}

func (s *SequenceWrap) TaskKey(prefix string) string {
	return fmt.Sprintf("%s:%v", prefix, s.Current)
}

func (s *SequenceWrap) Next() bool {
	if s.ctx.collect != nil {
		end := s.ctx.collect("sequence", s.name)
		if end {
			s.Current += 1
			return s.Current <= 0
		}
	}
	// 存储当前的序列号，而不是下一个
	bs, _ := json.Marshal(s)
	_ = s.ctx.store.SetKV(s.name, string(bs))

	s.Current += 1
	if s.max == -1 {
		return true
	}
	return s.Current < s.max
}

func GetFromStore[T interface{}](s NodeStatusStore, key string) (T, bool, error) {
	var t T
	v, ok, err := s.GetKV("__" + key)
	if err != nil {
		return t, false, err
	}
	if !ok {
		return t, false, nil
	}
	err = json.Unmarshal([]byte(v), &t)
	if err != nil {
		return t, false, err
	}
	return t, true, nil
}

func SetToStore[T interface{}](s NodeStatusStore, key string, t T) error {
	bs, _ := json.Marshal(t)
	err := s.SetKV("__"+key, string(bs))
	if err != nil {
		return err
	}
	return nil
}

func Sequence(ctx *Context, key string, maxLen int) SequenceWrap {
	if ctx.collect != nil {
		end := ctx.collect("sequence", key)
		if end {
			return SequenceWrap{
				Current: -1,
				max:     0,
				name:    "",
				ctx:     ctx,
			}
		}
	}

	key = fmt.Sprintf("__%v", key)
	s, ok, _ := GetFromStore[SequenceWrap](ctx.store, key)
	if !ok {
		return SequenceWrap{
			Current: -1, // skip first next()
			max:     maxLen,
			name:    key,
			ctx:     ctx,
		}
	}

	return s
}

type FutureT[T interface{}] struct {
	Val T
	k   string
	fun func() (T, error)
}

func (f *FutureT[T]) Value() T {
	return f.Val
}

func (f *FutureT[T]) exec() (interface{}, error) {
	t, err := f.fun()
	if err != nil {
		return t, err
	}

	f.Val = t

	return t, nil
}

func (f *FutureT[T]) key() string {
	return f.k
}

type Future interface {
	exec() (interface{}, error)
	key() string
}

type AsyncTask struct {
}

func (a *AsyncTask) Done() bool {
	return true
}

func (a *AsyncTask) Exec() bool {
	return true
}
func AsyncArray[T interface{}, A interface{}](ctx *Context, key string, arr []ArrayWrap[A], f func(ctx *TaskContext, a A) (T, error)) []Future {
	var fs []Future

	for _, t := range arr {
		// 注意闭包问题
		t := t
		fs = append(fs, Async(ctx, t.Key(key), func(ctx *TaskContext) (T, error) {
			return f(ctx, t.Val)
		}))
	}

	return fs
}

func Async[T interface{}](ctx *Context, key string, f func(ctx *TaskContext) (T, error)) *FutureT[T] {
	s, exist, _ := ctx.store.GetNodeStatus(key)
	if exist {
		// 有任务正在运行中，跳过执行
		t, _, _ := GetFromStore[T](ctx.store, key)
		return &FutureT[T]{
			Val: t,
			k:   key,
			fun: func() (T, error) {
				return f(newTaskContext(ctx, s))
			},
		}
	}

	future := FutureT[T]{
		k: key,
		fun: func() (T, error) {
			return f(newTaskContext(ctx, s))
		},
	}

	return &future
}

// Wait will wait all future done, if Parallel is not 0, then no limit for parallel.
func Wait(ctx *Context, parallel int, fs ...Future) {
	allDone := true
	runCount := 0
	for _, f := range fs {
		s, exist, _ := ctx.store.GetNodeStatus(f.key())

		if exist {
			// 有任务正在运行中，跳过执行
			done := false
			switch s.Status {
			case "done":
				done = true
			case "fail":
				// 如果有一个任务失败了，则算整个失败。
				panic(BreakFail(f.key(), fmt.Errorf("task %v, error %v: ", s.Key, strings.Join(s.Errs, ";"))))
			}

			if done {
				continue
			} else {
				allDone = false
			}

			if s.Status == "retry" {
				// 如果一个任务是 retry 状态，则需要重新执行
			} else {
				// 任务正在执行，跳过而执行后面的任务
				runCount++
				continue
			}
		}

		if (s.RunAt.IsZero() || s.RunAt.Before(time.Now())) && (parallel <= 0 || runCount < parallel) {
			// 如果任务重试状态，或没有状态，就需要执行
			// 如果是 retry，也改为执行状态，让下次调度跳过这次任务
			_ = ctx.store.SetNodeStatus(f.key(), s.MakeRunning(), 6*time.Second)

			// 如果没到执行时间，则不执行

			//log.Printf("step Run")
			go func(future Future, s TaskStatus) {
				errorc := make(chan error)
				datac := make(chan interface{})
				go func() {
					// 启动心跳
					for {
						select {
						case e := <-errorc:
							// 不重新调度，而是等待 BreakWait 自循环。
							// 重新调度将面临并发问题：
							// 多次任务同时执行成功，将并发调用，可能会导致 task 并发调用出错（状态检查），最好不要并发调度，否则需要加锁导致逻辑复杂。
							if s.RetryCount > 5 {
								_ = ctx.store.SetNodeStatus(future.key(), s.MakeFail(e))
							} else {
								_ = ctx.store.SetNodeStatus(future.key(), s.MakeRetry(e))

								//log.Printf("step MakeRetry")
							}
							return
						case data := <-datac:
							_ = SetToStore(ctx.store, future.key(), data)
							_ = ctx.store.SetNodeStatus(future.key(), s.MakeDone())
							return
						case <-time.After(3 * time.Second):
							// 心跳续期
							_ = ctx.store.SetNodeStatus(future.key(), s.MakeRunning(), 6*time.Second)
						}
					}
				}()

				t, err := future.exec()
				if err != nil {
					errorc <- err
				} else {
					datac <- t
				}
			}(f, s)

			// 并行
			// log.Printf("step BreakWait %s ", nextCall)
			panic(BreakWait(0))
		}
	}

	if !allDone {
		//log.Printf("step BreakWait 10")
		// 如果还有任务没完成，则等待任务完成
		// 循环 1 s 调度一次，检查任务状态，同时检查任务心跳，如果任务没有心跳则重启任务。
		panic(BreakWait(time.Second / 1))
	}
}

func Memo[T interface{}](ctx *Context, key string, build func() (T, error)) T {
	if ctx.collect != nil {
		end := ctx.collect("memo", key)
		if end {
			var t T
			return t
		}
	}

	key = fmt.Sprintf("__%v", key)
	v, exist, _ := GetFromStore[T](ctx.store, key)
	if exist {
		return v
	}

	t, _ := build()
	_ = SetToStore(ctx.store, key, t)
	return t
}

type ArrayWrap[T interface{}] struct {
	ProductKey string `json:"product_key"`
	Val        T      `json:"val"`
	Index      int    `json:"index"`
}

func (a ArrayWrap[T]) Value() (t T) {
	return a.Val
}

func (a ArrayWrap[T]) Key(prefix string) string {
	// /@/ 表示子集
	return fmt.Sprintf("%v/@/%v:%v", a.ProductKey, prefix, a.Index)
}

func Array[T interface{}](ctx *Context, key string, build func() ([]T, error)) []ArrayWrap[T] {
	if ctx.collect != nil {
		end := ctx.collect("array", key)
		if end {
			var t T
			return []ArrayWrap[T]{
				{
					ProductKey: key,
					Val:        t,
					Index:      0,
				},
			}
		}
	}

	v, exist, _ := GetFromStore[[]ArrayWrap[T]](ctx.store, key)
	// todo panic error
	if exist {
		return v
	}

	t, _ := build()
	a := make([]ArrayWrap[T], len(t))
	for i, v := range t {
		a[i] = ArrayWrap[T]{
			ProductKey: key,
			Val:        v,
			Index:      i,
		}
	}
	_ = SetToStore(ctx.store, key, a)
	return a
}

//
//func UseStatus[T interface{}](ctx *Context, key string, def T) (T, func(T)) {
//	// 从上下文中获取变量
//	// 如果不存在则创建
//	// 如果存在则返回
//	// 返回一个函数，用于设置变量
//	m, ok, _ := ctx.store.GetKV()
//	if ok {
//		if v, ok := m[key]; ok {
//			var t T
//			_ = json.Unmarshal([]byte(v), &t)
//			return t, func(t T) {
//				m, ok, _ := ctx.store.GetKV()
//				if !ok {
//					m = make(map[string]string)
//				}
//				bs, _ := json.Marshal(t)
//				m[key] = string(bs)
//				_ = ctx.store.SetKV(m)
//			}
//		}
//	}
//
//	setV := func(t T) {
//		m, ok, _ := ctx.store.GetKV()
//		if !ok {
//			m = make(map[string]string)
//		}
//		bs, _ := json.Marshal(t)
//		m[key] = string(bs)
//		_ = ctx.store.SetKV(m)
//	}
//	setV(def)
//
//	return def, setV
//}

type TaskContext struct {
	*Context
	Retry int
}

type TaskFun func(ctx *TaskContext) error

func newTaskContext(c *Context, taskStatus TaskStatus) *TaskContext {
	return &TaskContext{
		Context: c,
		Retry:   taskStatus.RetryCount,
	}
}

func (t *Context) Lock() func() {
	t.lock.Lock()

	return func() {
		t.lock.Unlock()
	}
}

// Task 同名的 task 在同一时间只能执行一次
func Task(c *Context, key string, fun TaskFun, opts ...TaskOption) {
	if c.collect != nil {
		if c.collect("task", key) {
			return
		}
	}

	s, exist, _ := c.store.GetNodeStatus(key)
	if s.Status == "done" {
		return
	}

	o := TaskOptions(opts).build()

	taskContext := newTaskContext(c, s)
	if !exist || s.Status == "retry" {
		err := fun(taskContext)
		if err != nil {
			if errors.Is(err, AbortError) {
				panic(BreakAbort(key, err))
			}
			if s.RetryCount > o.MaxRetry {
				panic(BreakFail(key, err))
			}
			panic(BreakRetry(key, err))
		}

		// 执行成功也需要断点，因为需要依靠断点来存储状态。
		panic(BreakDone(key))
	}
}

func Sleep(c *Context, key string, duration time.Duration) {
	if c.collect != nil {
		if c.collect("sleep", key) {
			return
		}
	}
	s, exist, _ := c.store.GetNodeStatus(key)
	// todo panic error，这个错误应该直接交给 MQ 重试兜底

	if s.Status == "done" {
		return
	}

	if !exist {
		panic(BreakSleep(key, duration))
	}

	if s.Status == "sleep" {
		d := s.RunAt.Sub(time.Now())
		if d > 0 {
			panic(BreakSleep(key, d))
		}

		_ = c.store.SetNodeStatus(key, s.MakeDone())
		// todo panic error，这个错误应该直接交给 MQ 重试兜底
	}
}

type taskOption struct {
	MaxRetry int // 这个 Task 最大重试次数，默认为 5
}

type TaskOptions []TaskOption

func (os TaskOptions) build() taskOption {
	option := taskOption{
		MaxRetry: 1,
	}
	for _, o := range os {
		o.apply(&option)
	}
	return option
}

type TaskOption interface {
	apply(*taskOption)
}

type maxRetryOption struct {
	maxRetry int
}

func (m *maxRetryOption) apply(option *taskOption) {
	option.MaxRetry = m.maxRetry
	return
}

func WithMaxRetry(maxRetry int) TaskOption {
	return &maxRetryOption{maxRetry: maxRetry}
}

type Set interface {
	Push(i interface{})
}

type TaskStatus struct {
	Key string
	// fail, 超过重试次数就算失败
	// abort, 手动终止流程
	// sleep, 等待中
	// retry, 重试中
	// done, 完成
	// running, 异步任务正在运行
	Status     string    `json:"status"`
	RunAt      time.Time `json:"run_at"` // sleep 到的时间
	Errs       []string  `json:"errs"`   // 每次重试都有错误
	RetryCount int       `json:"retry_count"`
}

func (t TaskStatus) MakeDone() TaskStatus {
	t.Status = "done"
	return t
}

func (t TaskStatus) MakeFail(err error) TaskStatus {
	t.Status = "fail"
	t.RetryCount += 1
	if err != nil {
		t.Errs = append(t.Errs, err.Error())
	}
	return t
}

func (t TaskStatus) MakeAbort() TaskStatus {
	t.Status = "abort"
	return t
}

func (t TaskStatus) MakeRunning() TaskStatus {
	t.Status = "running"
	return t
}

func (t TaskStatus) MakeSleep(runAt time.Time) TaskStatus {
	t.Status = "sleep"
	t.RunAt = runAt
	return t
}

func (t TaskStatus) MakeRetry(err error) TaskStatus {
	t.Status = "retry"
	t.RetryCount += 1
	t.Errs = append(t.Errs, err.Error())
	t.RunAt = time.Now().Add(time.Second * time.Duration(t.RetryCount))
	return t
}

type NodeStatusStore interface {
	GetNodeStatus(key string) (TaskStatus, bool, error) // 获取每个 task 的运行状态
	SetNodeStatus(key string, value TaskStatus, ttl ...time.Duration) error
	UpdateNodeStatus(key string, fu func(status TaskStatus, isNew bool) TaskStatus) (TaskStatus, error)
	GetKVAll() (map[string]string, error)
	SetKV(k string, v string) error
	GetKV(k string) (string, bool, error)
	Clear() error // 删除所有数据
}

var _ NodeStatusStore = (*KvNodeStatusStore)(nil)

type StoreFactory interface {
	New(key string) NodeStatusStore
}

type AsyncQueueFactory interface {
	New(key string) AsyncQueue
	Start(ctx context.Context) error
}

type TickServer struct {
	scheduler  *Scheduler
	httpServer *HttpServer
	measure    Measure
}

type Measure interface {
	OnExec(flow, key string)
	GetCount(flow string) map[string]int64
}

type MockMeasure struct {
	m map[string]map[string]int64
}

func NewMockMeasure() *MockMeasure {
	return &MockMeasure{
		m: map[string]map[string]int64{},
	}
}

func (m *MockMeasure) OnExec(flow, key string) {
	if _, ok := m.m[flow]; !ok {
		m.m[flow] = map[string]int64{}
	}
	m.m[flow][key] += 1
}

func (m *MockMeasure) GetCount(flow string) map[string]int64 {
	return m.m[flow]
}

var _ Measure = (*MockMeasure)(nil)

type RedisMeasure struct {
	redis *redis.Client
}

func NewRedisMeasure(redis *redis.Client) *RedisMeasure {
	return &RedisMeasure{redis: redis}
}

func (r *RedisMeasure) OnExec(flow, key string) {
	r.redis.HIncrBy(context.Background(), "measure:"+flow, key, 1)
}

func (r *RedisMeasure) GetCount(flow string) map[string]int64 {
	x, _ := r.redis.HGetAll(context.Background(), "measure:"+flow).Result()
	rsp := map[string]int64{}
	for k, v := range x {
		rsp[k], _ = strconv.ParseInt(v, 10, 64)
	}
	return rsp
}

var _ Measure = (*RedisMeasure)(nil)

type HttpServer struct {
	flows         map[string]*Flow // to get flow info
	scheduler     *Scheduler       // to trigger flow
	measure       Measure
	listenAddress string
}

func NewHttpServer(scheduler *Scheduler, measure Measure, listenAddress string) *HttpServer {
	return &HttpServer{scheduler: scheduler, measure: measure, flows: map[string]*Flow{}, listenAddress: listenAddress}
}

func (s *HttpServer) Start(ctx context.Context) error {
	r := gin.Default()
	api := r.Group("/api")
	api.GET("/flow_list", func(c *gin.Context) {
		r := map[string]flow.DAG{}
		for name, f := range s.flows {
			dag, err := f.DAG()
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{
					"message": err.Error(),
				})
				return
			}

			r[name] = dag
		}

		c.JSON(http.StatusOK, r)
	})
	go r.Run()

	return nil
}

func (s *HttpServer) register(f *Flow) {
	s.flows[f.Id] = f
}

type TickClient struct {
	scheduler *Scheduler
}

type Flow struct {
	Id        string
	fun       func(ctx *Context)
	onFail    func(ctx *Context, ts TaskStatus) error
	onError   func(ctx *Context, ts TaskStatus) error
	onSuccess func(ctx *Context) error
	opt       flowOpt
}

// DAG 生成一个数据流图
// 可以使用 reactflow 绘制。
func (f *Flow) DAG() (flow.DAG, error) {
	dag := flow.DAG{}

	f.fun(&Context{
		Context: nil,
		CallId:  "dag",
		store:   nil,
		collect: func(typ string, key string) bool {
			ks := strings.Split(key, "/@/")

			var parent string
			if len(ks) > 1 {
				parent = ks[len(ks)-2]
				key = ks[len(ks)-1]
			}

			var node flow.Node
			switch typ {
			case "task":
				node = flow.Node{
					Id: key,
					Data: flow.NodeData{
						Label: fmt.Sprintf("[task] %s", key),
						Data: map[string]interface{}{
							"type": typ,
						},
					},
					ParentNode: parent,
				}
			case "sleep":
				node = flow.Node{
					Id: key,
					Data: flow.NodeData{
						Label: fmt.Sprintf("[sleep] %s", key),
						Data: map[string]interface{}{
							"type": typ,
						},
					},
				}
			case "array":
				node = flow.Node{
					Id: key,
					Data: flow.NodeData{
						Label: fmt.Sprintf("[%v] %s", typ, key),
						Data: map[string]interface{}{
							"type": typ,
						},
					},
				}
			default:
				node = flow.Node{
					Id: key,
					Data: flow.NodeData{
						Label: fmt.Sprintf("[%v] %s", typ, key),
						Data: map[string]interface{}{
							"type": typ,
						},
					},
				}
			}

			dag.AppendNode(node, parent)

			// 连接上一个节点
			if len(dag.Nodes) > 1 {
				//l:=len(dag.Nodes)
				//sourceId := dag.GetNodeByIndex(l-2).Id
				//targetId := nodes[len(nodes)-1].Id
				//edge = append(edge, flow.Edge{
				//	Id:        fmt.Sprintf("%s--%s", sourceId, targetId),
				//	Source:    sourceId,
				//	Target:    targetId,
				//	MarkerEnd: flow.Marker{Type: "arrow"},
				//	Animated:  false,
				//	Label:     "",
				//	Data:      nil,
				//	Style:     nil,
				//})
			}

			return true
		},
	})

	return dag, nil
}

func (f *Flow) OnSuccess(fun func(ctx *Context) error) *Flow {
	f.onSuccess = fun
	return f
}

func (f *Flow) OnFail(fun func(ctx *Context, ts TaskStatus) error) *Flow {
	f.onFail = fun
	return f
}

// OnError 添加一个错误回调，和 task 一样，错误回调也支持重试。
func (f *Flow) OnError(fun func(ctx *Context, ts TaskStatus) error) *Flow {
	f.onError = fun
	return f
}

type Event struct {
	CallId       string
	Critical     bool
	InitMetaData MetaData // 只有当第一次调度时有效
}

type AsyncQueue interface {
	// Publish 当 uniqueKey 不为空时，后面 Publish 的数据会覆盖前面的数据
	// uniqueKey 通常为 callId
	Publish(ctx context.Context, data Event, delay time.Duration) error
	Subscribe(h func(ctx context.Context, data Event) error)
}

type BreakStatus struct {
	Type  string    // abort, sleep, retry, done, fail
	RunAt time.Time // 当 sleep 时，表示下次调度的时间
	Task  string    // 表示触发的是哪一个 task 内部断点
	Err   error
}

func WithCallId(ctx context.Context, callId string) context.Context {
	return context.WithValue(ctx, "callId", callId)
}

func GetCallId(ctx context.Context) string {
	value := ctx.Value("callId")
	if value == nil {
		return ""
	}
	return value.(string)
}

var AbortError = errors.New("abort")

type FlowOption func(f *flowOpt)

type flowOpt struct {
	timeout time.Duration
}

// WithTimeout 控制执行整个 flow 的超时时间，超时后将会中断任务并调用 onFail.
func WithTimeout(t time.Duration) FlowOption {
	return func(f *flowOpt) {
		f.timeout = t
	}
}

// Flow Define a flow
func (t *TickServer) Flow(id string, fun func(ctx *Context), opts ...FlowOption) *Flow {
	f := &Flow{
		Id:        id,
		fun:       fun,
		onFail:    nil,
		onSuccess: nil,
	}

	for _, o := range opts {
		o(&f.opt)
	}

	// 注册调度
	t.scheduler.register(f)

	if t.httpServer != nil {
		t.httpServer.register(f)
	}

	return f
}

type Scheduler struct {
	asyncScheduler AsyncQueueFactory
	statusFactory  StoreFactory
	debug          bool
}

func NewScheduler(asyncScheduler AsyncQueueFactory, statusStore StoreFactory) *Scheduler {
	return &Scheduler{asyncScheduler: asyncScheduler, statusFactory: statusStore}
}
func (s *Scheduler) Start(ctx context.Context) error {
	return s.asyncScheduler.Start(ctx)
}

func (s *Scheduler) register(f *Flow) {
	aw := s.asyncScheduler.New(f.Id)
	aw.Subscribe(func(ctx context.Context, event Event) error {
		//log.Printf("-----------------------------")

		callId := event.CallId
		ctx = WithCallId(ctx, callId)

		statusStore := s.statusFactory.New(callId)

		if event.InitMetaData != nil {
			for k, v := range event.InitMetaData {
				_ = statusStore.SetKV(k, v)
			}
		}
		// 从缓存中拿出上次的运行状态
		//m, _, := statusStore.GetKVAll()
		//if m != nil {
		//	ctx = WithMetaData(ctx, m)
		//}

		err := func() (err error) {
			ctx := &Context{
				Context: ctx,
				CallId:  callId,
				store:   statusStore,
				s:       aw,
			}

			defer func() {
				r := recover()
				if r == nil {
					return
				}

				ns, ok := r.(Breakpoint)
				if !ok {
					panic(r)
				}

				switch breakpoint := ns.(type) {
				case *breakContinue:
					// 立即调度，实现并行
					err = aw.Publish(ctx, Event{
						CallId:   callId,
						Critical: true,
					}, 0)
					if err != nil {
						log.Printf("scheduler event error: %v", err)
						return
					}
				case *breakWait:
					err = aw.Publish(ctx, Event{
						CallId:   callId,
						Critical: true,
					}, breakpoint.RunAt.Sub(time.Now()))
					if err != nil {
						log.Printf("scheduler event error: %v", err)
						return
					}
				case *breakRetry:
					// 存储重试次数
					var newStatus TaskStatus
					newStatus, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) TaskStatus {
						return status.MakeRetry(breakpoint.Err)
					})

					if f.onError != nil {
						err = f.onError(ctx, newStatus)
						if err != nil {
							// TODO retry when onError error
							log.Printf("[gotick error] onFail error: %v", err)
						}
					}
					// 进入下次调度
					err = aw.Publish(ctx, Event{
						CallId:   callId,
						Critical: true,
					}, time.Duration(newStatus.RetryCount)*time.Second) // TODO 支持指定算法计算回退时间
					if err != nil {
						log.Printf("[gotick error] Publish error: %v", err)
					}
				case *breakAbort:
					var newStatus TaskStatus
					newStatus, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) TaskStatus {
						return status.MakeAbort()
					})
					if err != nil {
						return
					}
					if f.onFail != nil {
						err = f.onFail(ctx, newStatus)
						if err != nil {
							log.Printf("[gotick error] onFail error: %v", err)
						}
					}
				case *breakFail:
					var newStatus TaskStatus
					newStatus, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) TaskStatus {
						return status.MakeFail(breakpoint.Err)
					})
					if err != nil {
						return
					}
					if f.onFail != nil {
						err = f.onFail(ctx, newStatus)
						if err != nil {
							log.Printf("[gotick error] onFail error: %v", err)
						}
					}
				case *breakSleep:
					// 进入下次调度
					// TODO 考虑先入队，然后更改状态
					_, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) TaskStatus {
						return status.MakeSleep(breakpoint.RunAt)
					})
					if err != nil {
						return
					}

					now := time.Now()
					err = aw.Publish(ctx, Event{
						CallId:   callId,
						Critical: true,
					}, breakpoint.RunAt.Sub(now))
					if err != nil {
						log.Printf("[gotick error] Publish error: %v", err)
						return
					}
				case *breakDone:
					_, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) TaskStatus {
						return status.MakeDone()
					})
					if err != nil {
						return
					}

					err = aw.Publish(ctx, Event{
						CallId:   callId,
						Critical: true,
					}, 0)
					if err != nil {
						log.Printf("[gotick error] Publish error: %v", err)
						return
					}
				}
			}()

			// 正确情况下不应该返回 error，因为这个 error 会直接交给 asyncq 处理，脱离了框架控制。
			// 都应该在 gotick.Task 中返回 error
			f.fun(ctx)

			if err != nil {
				return nil
			}

			// 全部执行完成，触发 onSuccess
			if f.onSuccess != nil {
				err := f.onSuccess(ctx)
				if err != nil {
					return err
				}
			}

			return err
		}()
		if err != nil {
			// 如果返回错误，则会进入到消息队列的默认重试机制。
			// 通常是调用消息队列新增任务等无法正常进入下一步流程等致命错误，程序逻辑已经无法处理，只能交由消息队列处理。
			return err
		}
		//
		//if f.onSuccess != nil {
		//	// 如果什么都没做，默认就是完成状态
		//	taskStatus := TaskStatus{
		//		Type: "done",
		//	}
		//	err := f.onSuccess(ctx, taskStatus)
		//	if err != nil {
		//		return err
		//	}
		//}

		return nil
	})
}

// Trigger 触发一次流程运行
func (s *Scheduler) Trigger(ctx context.Context, flowId string, initData MetaData) (string, error) {
	callId := randomStr()
	event := Event{
		CallId:       callId,
		InitMetaData: initData,
	}
	err := s.asyncScheduler.New(flowId).Publish(ctx, event, 0)
	if err != nil {
		return "", err
	}

	return callId, nil
}

func randomStr() string {
	b := make([]byte, 16)
	_, _ = rand2.Read(b)
	return fmt.Sprintf("%x", b)
}

// Trigger 触发一次流程运行，在服务端和客户端都可以调用。
func (t *TickServer) Trigger(ctx context.Context, flowId string, data MetaData) (string, error) {
	return t.scheduler.Trigger(ctx, flowId, data)
}

// Trigger 触发一次流程运行，在服务端和客户端都可以调用。
func (t *TickClient) Trigger(ctx context.Context, flowId string, data MetaData) (string, error) {
	return t.scheduler.Trigger(ctx, flowId, data)
}

// StartServer 启动服务，在服务端应该调用此方法开始执行异步任务。
// 当 ctx 被关闭时，服务也会关闭。
func (t *TickServer) StartServer(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.scheduler.Start(ctx)
		if err != nil {
			log.Printf("async queue start error: %v", err)
		}
	}()

	if t.httpServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := t.httpServer.Start(ctx)
			if err != nil {
				log.Printf("async queue start error: %v", err)
			}
		}()
	}

	wg.Wait()

	return nil
}

type KvStoreProduct struct {
	store store.KVStore
}

func NewKvStoreProduct(store store.KVStore) *KvStoreProduct {
	return &KvStoreProduct{store: store}
}

func (s KvStoreProduct) New(key string) NodeStatusStore {
	return NewKvNodeStatusStore(s.store, key)
}

type KvNodeStatusStore struct {
	store store.KVStore
	key   string
}

func (n *KvNodeStatusStore) metaKey() string {
	return n.key + "_meta"
}

func (n *KvNodeStatusStore) statusKey() string {
	return n.key + "_status"
}

func (n *KvNodeStatusStore) Clear() error {
	err := n.store.Delete(context.Background(), n.metaKey())
	if err != nil {
		return err
	}

	err = n.store.Delete(context.Background(), n.statusKey())
	if err != nil {
		return err
	}

	return nil
}

func NewKvNodeStatusStore(store store.KVStore, key string) *KvNodeStatusStore {
	return &KvNodeStatusStore{store: store, key: key}
}

func (n *KvNodeStatusStore) UpdateNodeStatus(key string, fu func(status TaskStatus, isNew bool) TaskStatus) (TaskStatus, error) {
	old, ok, err := n.GetNodeStatus(key)
	if err != nil {
		return old, err
	}

	nw := fu(old, !ok)
	err = n.SetNodeStatus(key, nw)
	if err != nil {
		return old, err
	}
	return nw, nil
}

func (n *KvNodeStatusStore) GetNodeStatus(key string) (TaskStatus, bool, error) {
	status := TaskStatus{Key: key}
	exist, err := n.store.HGet(context.Background(), n.statusKey(), key, &status)
	if err != nil {
		return status, false, err
	}
	if !exist {
		return status, false, nil
	}

	return status, true, nil
}

func (n *KvNodeStatusStore) SetNodeStatus(key string, value TaskStatus, ttl ...time.Duration) error {
	var t time.Duration
	if len(ttl) >= 1 {
		t = ttl[0]
	}
	return n.store.HSet(context.Background(), n.statusKey(), key, value, t)
}

func (n *KvNodeStatusStore) GetKVAll() (map[string]string, error) {
	v, exist, err := n.store.HGetAll(context.Background(), n.metaKey())
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}

	return v, nil
}

func (n *KvNodeStatusStore) GetKV(k string) (string, bool, error) {
	var v string
	exist, err := n.store.HGet(context.Background(), n.metaKey(), k, &v)
	if err != nil {
		return "", false, err
	}
	if !exist {
		return "", false, nil
	}
	return v, true, nil
}

func (n *KvNodeStatusStore) SetKV(k, v string) error {
	return n.store.HSet(context.Background(), n.metaKey(), k, v, 0)
}

type Options struct {
	RedisURL     string // "redis://<user>:<pass>@localhost:6379/<db>"
	DelayedQueue store.DelayedQueue
	KvStore      store.KVStore
	ListenAddr   string // ":8080"
}

func NewTickServer(p Options) *TickServer {
	if p.DelayedQueue == nil {
		opt, err := redis.ParseURL(p.RedisURL)
		if err != nil {
			panic(err)
		}

		redisClient := redis.NewClient(opt)
		p.DelayedQueue = store.NewAsynq(redisClient)
	}
	if p.KvStore == nil {
		opt, err := redis.ParseURL(p.RedisURL)
		if err != nil {
			panic(err)
		}

		redisClient := redis.NewClient(opt)
		p.KvStore = store.NewRedisStore(redisClient)
	}

	ap := NewAsyncQueueFactory(p.DelayedQueue)
	st := NewKvStoreProduct(p.KvStore)
	_, debug := os.LookupEnv("GOTICK_DEBUG")

	scheduler := NewScheduler(ap, st)
	var server *HttpServer
	if p.ListenAddr != "" {
		server = NewHttpServer(scheduler, nil, p.ListenAddr)
	}
	t := &TickServer{
		scheduler:  scheduler,
		httpServer: server,
	}
	t.scheduler.debug = debug

	return t
}

func NewTickClient(p Options) *TickClient {
	opt, err := redis.ParseURL(p.RedisURL)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)
	st := NewKvStoreProduct(store.NewRedisStore(redisClient))
	if p.DelayedQueue == nil {
		p.DelayedQueue = store.NewAsynq(redisClient)
	}
	ap := NewAsyncQueueFactory(p.DelayedQueue)

	t := &TickClient{
		scheduler: NewScheduler(ap, st),
	}

	return t
}

type DelayedAsyncQueueProduct struct {
	queue     store.DelayedQueue
	wg        sync.WaitGroup
	closeChan chan bool
}

func (a *DelayedAsyncQueueProduct) Start(ctx context.Context) (err error) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = a.queue.Start(ctx)
	}()

	wg.Wait()

	close(a.closeChan)
	// wait for all queue down
	a.wg.Wait()

	return
}

func NewAsyncQueueFactory(redis store.DelayedQueue) *DelayedAsyncQueueProduct {
	return &DelayedAsyncQueueProduct{queue: redis, closeChan: make(chan bool)}
}

func (a *DelayedAsyncQueueProduct) New(key string) AsyncQueue {
	x := NewDelayedAsyncQueue(a.queue, key, &a.wg, a.closeChan)
	return x
}

type DelayedAsyncQueue struct {
	delayedQueue store.DelayedQueue
	key          string
	wg           *sync.WaitGroup // wait for all callback down
	closeChan    chan bool
}

func (a *DelayedAsyncQueue) Publish(ctx context.Context, data Event, delay time.Duration) error {
	bs, _ := json.Marshal(data)
	return a.delayedQueue.Publish(ctx, a.key, bs, delay, store.Option{
		Critical: data.Critical,
	})
}

func (a *DelayedAsyncQueue) Subscribe(h func(ctx context.Context, data Event) error) {
	a.delayedQueue.Subscribe(a.key, func(ctx context.Context, data []byte) error {
		a.wg.Add(1)
		defer a.wg.Done()

		// 如果已经关闭，则返回错误重试
		//select {
		//case <-a.closeChan:
		//	// TODO 考虑是否可以重新入队
		//	return errors.New("queue closed")
		//default:
		//}

		var ev Event
		_ = json.Unmarshal(data, &ev)

		return h(ctx, ev)
	})
}

func NewDelayedAsyncQueue(redis store.DelayedQueue, key string, wg *sync.WaitGroup, closeChan chan bool) *DelayedAsyncQueue {
	return &DelayedAsyncQueue{
		delayedQueue: redis,
		key:          key,
		wg:           wg,
		closeChan:    closeChan,
	}
}
