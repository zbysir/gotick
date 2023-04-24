package gotick

import (
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"os"
	"sync"
	"time"
)

func Store(ctx context.Context, k string, v string) {
	_ = WithMetaData(ctx, MetaData{
		k: v,
	})
}

type MetaData map[string]string

func WithMetaData(ctx context.Context, meta MetaData) context.Context {
	o := GetMetaData(ctx)
	if o == nil {
		return context.WithValue(ctx, "meta", meta)
	}

	for k, v := range meta {
		o[k] = v
	}
	return ctx
}

func GetMetaData(ctx context.Context) MetaData {
	value := ctx.Value("meta")
	if value == nil {
		return nil
	}
	return value.(MetaData)
}

type Context struct {
	context.Context
	CallId string
	s      NodeStatusStore
}

type Sequence struct {
	Current int
	max     int
	name    string
	ctx     *Context `json:"-"`
}

func (s *Sequence) TaskKey(prefix string) string {
	return fmt.Sprintf("%s:%v", prefix, s.Current)
}

func (s *Sequence) Next() bool {
	// 存储当前的序列号，而不是下一个
	md, _, _ := s.ctx.s.GetMetaData()
	bs, _ := json.Marshal(s)
	md[s.name] = string(bs)
	_ = s.ctx.s.SetMetaData(md)

	s.Current += 1
	if s.max == -1 {
		return true
	}
	return s.Current < s.max
}

func UseSequence(ctx *Context, key string, maxLen int) Sequence {
	md, ok, _ := ctx.s.GetMetaData()
	s, ok := md[key]
	if !ok {
		return Sequence{
			Current: 0,
			max:     maxLen,
			name:    key,
			ctx:     ctx,
		}
	}

	seq := Sequence{
		Current: 0,
		max:     maxLen,
		name:    key,
		ctx:     ctx,
	}
	_ = json.Unmarshal([]byte(s), &seq)
	return seq
}

func UseMemo[T interface{}](ctx *Context, key string, build func() (T, error)) T {
	m, exist, _ := ctx.s.GetMetaData()
	// todo panic error
	if exist {
		if v, ok := m[key]; ok {
			var t T
			_ = json.Unmarshal([]byte(v), &t)
			return t
		}
	}

	t, _ := build()
	// todo panic error
	bs, _ := json.Marshal(t)
	m[key] = string(bs)
	_ = ctx.s.SetMetaData(m)
	return t
}

type ArrayWrap[T interface{}] struct {
	Val T `json:"val"`
}

func (a ArrayWrap[T]) Value() (t T) {
	return a.Val
}

func (a ArrayWrap[T]) Key(prefix string) string {
	return fmt.Sprintf("%v:%v", prefix, a.Val)
}

func UseArray[T interface{}](ctx *Context, key string, build func() ([]T, error)) []ArrayWrap[T] {
	m, exist, _ := ctx.s.GetMetaData()
	// todo panic error
	if exist {
		if v, ok := m[key]; ok {
			var t []ArrayWrap[T]
			err := json.Unmarshal([]byte(v), &t)
			if err != nil {
				log.Printf("err: %v", err)
			}
			return t
		}
	}

	t, _ := build()
	a := make([]ArrayWrap[T], len(t))
	for i, v := range t {
		a[i] = ArrayWrap[T]{
			Val: v,
		}
	}
	// todo panic error
	bs, _ := json.Marshal(a)
	m[key] = string(bs)
	_ = ctx.s.SetMetaData(m)
	return a
}
func UseStatus[T interface{}](ctx *Context, key string, def T) (T, func(T)) {
	// 从上下文中获取变量
	// 如果不存在则创建
	// 如果存在则返回
	// 返回一个函数，用于设置变量
	m, ok, _ := ctx.s.GetMetaData()
	if ok {
		if v, ok := m[key]; ok {
			var t T
			_ = json.Unmarshal([]byte(v), &t)
			return t, func(t T) {
				m, ok, _ := ctx.s.GetMetaData()
				if !ok {
					m = make(map[string]string)
				}
				bs, _ := json.Marshal(t)
				m[key] = string(bs)
				_ = ctx.s.SetMetaData(m)
			}
		}
	}

	setV := func(t T) {
		m, ok, _ := ctx.s.GetMetaData()
		if !ok {
			m = make(map[string]string)
		}
		bs, _ := json.Marshal(t)
		m[key] = string(bs)
		_ = ctx.s.SetMetaData(m)
	}
	setV(def)

	return def, setV
}

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

func Task(c *Context, key string, fun TaskFun, opts ...TaskOption) {
	s, exist, _ := c.s.GetNodeStatus(key)
	// todo panic error

	o := TaskOptions(opts).build()

	taskContext := newTaskContext(c, s)
	if !exist {
		err := fun(taskContext)
		if err != nil {
			if s.RetryCount > o.MaxRetry {
				panic(Fail(key, err))
			}
			panic(Retry(key, err))
		}
		panic(Done(key))
	}
	if s.Status == "retry" {
		err := fun(taskContext)
		if err != nil {
			if s.RetryCount > o.MaxRetry {
				panic(Fail(key, err))
			}
			panic(Retry(key, err))
		}
		panic(Done(key))
	}
}

func AB(c *Context, key string, fun func() error) {
	s, exist, _ := c.s.GetNodeStatus(key)
	// todo panic error

	if !exist {
		_ = fun()
		// TODO retry
		panic(Done(key))
	}
	if s.Status == "retry" {
		_ = fun()
		panic(Done(key))
	}
}

func Sleep(c *Context, key string, duration time.Duration) {
	s, exist, _ := c.s.GetNodeStatus(key)
	// todo panic error

	if !exist {
		panic(NewSleep(key, duration))
	}

	if s.Status == "sleep" {
		d := s.RunAt.Sub(time.Now())
		if d > 0 {
			panic(NewSleep(key, d))
		}

		_ = c.s.SetNodeStatus(key, s.MakeDone())
		// todo panic error
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
	// fail, 超过重试次数就算失败
	// abort, 手动终止流程
	// sleep, 等待中
	// retry, 重试中
	// done, 完成
	Status     string    `json:"status"`
	RunAt      time.Time `json:"run_at"`
	Errs       []string  `json:"errs"`
	RetryCount int       `json:"retry_count"`
}

func (t TaskStatus) MakeDone() TaskStatus {
	t.Status = "done"
	return t
}

func (t TaskStatus) MakeFail(err error) TaskStatus {
	t.Status = "fail"
	if err != nil {
		t.Errs = append(t.Errs, err.Error())
	}
	return t
}

func (t TaskStatus) MakeAbort() TaskStatus {
	t.Status = "abort"
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
	return t
}

type NodeStatusStore interface {
	GetNodeStatus(key string) (TaskStatus, bool, error)
	SetNodeStatus(key string, value TaskStatus) error
	GetMetaData() (MetaData, bool, error)
	SetMetaData(MetaData) error
	Clear() error // 删除所有数据
}

type StoreFactory interface {
	New(key string) NodeStatusStore
}

type AsyncQueueFactory interface {
	New(key string) AsyncQueue
	Start(ctx context.Context) error
}

type SimpleAsyncQueueFactory struct {
}

func (s SimpleAsyncQueueFactory) Start(ctx context.Context) error {
	return nil
}

func (s SimpleAsyncQueueFactory) New(key string) AsyncQueue {
	return &SimpleAsyncQueue{}
}

type TickServer struct {
	flows      map[string]*Flow
	asyncQueue AsyncQueueFactory
	closeChan  chan bool
	closeOnce  sync.Once
	wg         sync.WaitGroup

	scheduler *Scheduler
}

type TickClient struct {
	scheduler *Scheduler
}

type Flow struct {
	Id        string
	fun       func(ctx *Context) error
	onFail    func(ctx *Context, ts TaskStatus) error
	onSuccess func(ctx *Context, ts TaskStatus) error
}

func (f *Flow) DAG() {

}
func (f *Flow) Success(fun func(ctx *Context, ts TaskStatus) error) FailAble {
	f.onSuccess = fun
	return f
}

func (f *Flow) Fail(fun func(ctx *Context, ts TaskStatus) error) SuccessAble {
	f.onFail = fun
	return f
}

type Event struct {
	CallId       string
	InitMetaData MetaData // 只有当第一次调度时有效
}

type AsyncQueue interface {
	// Publish 当 uniqueKey 不为空时，后面 Publish 的数据会覆盖前面的数据
	// uniqueKey 通常为 callId
	Publish(ctx context.Context, data Event, delay time.Duration) error
	Subscribe(h func(ctx context.Context, data Event) error)
}

type NextStatus struct {
	Status string // abort, sleep, retry, done, fail
	RunAt  time.Time
	Task   string
	Err    error
}

type ThenAble interface {
	SuccessAble
	FailAble
	Then(key string, c NodeCaller, opts ...TaskOption) ThenAble
}

type SuccessAble interface {
	// Success When task exec success, will call this function
	Success(f func(ctx *Context, ts TaskStatus) error) FailAble
}

type FailAble interface {
	// Fail When task exec fail, will call this function
	Fail(f func(ctx *Context, ts TaskStatus) error) SuccessAble
}

type NodeCaller func(ctx context.Context) (NextStatus, error)

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

func (t *TickServer) Flow(id string, fun func(ctx *Context) error) *Flow {
	f := &Flow{
		Id:        id,
		fun:       fun,
		onFail:    nil,
		onSuccess: nil,
	}

	// 注册调度
	t.scheduler.register(f)

	t.flows[id] = f
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
		callId := event.CallId
		meta := MetaData{}
		ctx = WithMetaData(ctx, meta)

		// 如果是第一次调度，则需要生成 callId 用于存储调用状态
		if callId == "" {
			// 拼装上额外的 data
			if event.InitMetaData != nil {
				ctx = WithMetaData(ctx, event.InitMetaData)
			}

			callId = randomStr()
		}

		ctx = WithCallId(ctx, callId)

		statusStore := s.statusFactory.New(callId)
		// 从缓存中拿出上次的运行状态
		m, _, _ := statusStore.GetMetaData()
		if m != nil {
			ctx = WithMetaData(ctx, m)
		}

		err := func() error {
			ctx := &Context{
				Context: ctx,
				CallId:  callId,
				s:       statusStore,
			}

			defer func() {
				r := recover()
				if r == nil {
					return
				}

				ns, ok := r.(NextStatus)
				if !ok {
					panic(r)
				}

				if s.debug {
					log.Printf("[gotick] %v", ns)
				}

				switch ns.Status {
				case "retry":
					ts, _, _ := statusStore.GetNodeStatus(ns.Task)
					_ = statusStore.SetNodeStatus(ns.Task, ts.MakeRetry(ns.Err))
					// 进入下次调度
					err := aw.Publish(ctx, Event{
						CallId: callId,
					}, time.Duration(ts.RetryCount)*time.Second)
					if err != nil {
						log.Printf("scheduler event error: %v", err)
					}
				case "abort":
					ts, _, _ := statusStore.GetNodeStatus(ns.Task)
					if f.onFail != nil {
						err := f.onFail(ctx, TaskStatus{
							Status:     "abort",
							RunAt:      time.Now(),
							Errs:       nil,
							RetryCount: ts.RetryCount,
						})
						if err != nil {
							panic(err)
						}
					}
				case "fail":
					ts, _, _ := statusStore.GetNodeStatus(ns.Task)
					if f.onFail != nil {
						err := f.onFail(ctx, TaskStatus{
							Status:     "fail",
							RunAt:      time.Now(),
							Errs:       ts.Errs,
							RetryCount: ts.RetryCount,
						})
						if err != nil {
							panic(err)
						}
					}
				case "sleep":
					// 进入下次调度
					now := time.Now()
					_ = statusStore.SetNodeStatus(ns.Task, TaskStatus{
						Status:     "sleep",
						RunAt:      ns.RunAt,
						Errs:       nil,
						RetryCount: 0,
					})
					err := aw.Publish(ctx, Event{
						CallId: callId,
					}, ns.RunAt.Sub(now))
					if err != nil {
						log.Printf("scheduler event error: %v", err)
					}
				case "done":
					fallthrough
				default:
					_ = statusStore.SetNodeStatus(ns.Task, TaskStatus{
						Status:     "done",
						RunAt:      time.Now(),
						Errs:       nil,
						RetryCount: 0,
					})
					// 进入下次调度
					err := aw.Publish(ctx, Event{
						CallId: callId,
					}, 0)
					if err != nil {
						log.Printf("scheduler event error: %v", err)
					}
				}
			}()

			return f.fun(ctx)
		}()
		if err != nil {
			return err
		}
		//
		//if f.onSuccess != nil {
		//	// 如果什么都没做，默认就是完成状态
		//	taskStatus := TaskStatus{
		//		Status: "done",
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
	var schedulerWg sync.WaitGroup
	schedulerWg.Add(1)
	go func() {
		defer schedulerWg.Done()
		err := t.scheduler.Start(ctx)
		if err != nil {
			log.Printf("async queue start error: %v", err)
		}
	}()

	schedulerWg.Wait()
	// 先等待 asyncQueue（消费者）关闭之后才应该关闭 tick。
	// 因为 tick 会在关闭期间如果收到任务会重新入队，如果消费者没有提前关闭则可能又收到重新入队的任务。

	select {
	case <-ctx.Done():
		t.closeOnce.Do(func() {
			close(t.closeChan)
		})
	}

	t.wg.Wait()
	return nil
}

// SimpleAsyncQueue 使用 Goroutine 实现的异步队列
// 它在重启之后会丢数据，只应该在测试使用。
// - 使用它可以达到最大定时的精度（毫秒级）
// - 缺点是可能会占用更多的内容并且重启恢复需要时间。
// - 所有任务都会在同一个机器上执行，可能导致热点问题。
type SimpleAsyncQueue struct {
	callback []func(ctx context.Context, s Event) error
}

func (a *SimpleAsyncQueue) Publish(ctx context.Context, data Event, delay time.Duration) error {
	go func() {
		time.Sleep(delay)
		for _, c := range a.callback {
			c(ctx, data)
		}
	}()

	return nil
}

func (a *SimpleAsyncQueue) Subscribe(f func(ctx context.Context, s Event) error) {
	a.callback = append(a.callback, f)
}

func (a *SimpleAsyncQueue) Exist(uniqueKey []string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

type MockNodeStatusStoreProduct struct {
	m map[string]NodeStatusStore
}

func (n *MockNodeStatusStoreProduct) New(key string) NodeStatusStore {
	if n.m == nil {
		n.m = make(map[string]NodeStatusStore)
	}
	if n.m[key] == nil {
		n.m[key] = &MockNodeStatusStore{m: make(map[string]interface{})}
	}
	return n.m[key]
}

type MockNodeStatusStore struct {
	m map[string]interface{}
}

func (m *MockNodeStatusStore) Clear() error {
	m.m = make(map[string]interface{})
	return nil
}

func (m *MockNodeStatusStore) GetNodeStatus(key string) (TaskStatus, bool, error) {
	i, ok := m.m[key]
	if !ok {
		return TaskStatus{}, false, nil
	}
	return i.(TaskStatus), true, nil
}

func (m *MockNodeStatusStore) SetNodeStatus(key string, value TaskStatus) error {
	m.m[key] = value
	return nil
}

func (m *MockNodeStatusStore) GetMetaData() (MetaData, bool, error) {
	i, ok := m.m["meta"]
	if !ok {
		return nil, false, nil
	}
	return i.(MetaData), true, nil
}

func (m *MockNodeStatusStore) SetMetaData(data MetaData) error {
	m.m["meta"] = data
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
	table string
}

func (n *KvNodeStatusStore) Clear() error {
	return n.store.HClear(context.Background(), n.table)
}

func NewKvNodeStatusStore(store store.KVStore, table string) *KvNodeStatusStore {
	return &KvNodeStatusStore{store: store, table: table}
}

func (n *KvNodeStatusStore) GetNodeStatus(key string) (TaskStatus, bool, error) {
	status := TaskStatus{}
	exist, err := n.store.HGet(context.Background(), n.table, key, &status)
	if err != nil {
		return status, false, err
	}
	if !exist {
		return status, false, nil
	}

	return status, true, nil
}

func (n *KvNodeStatusStore) SetNodeStatus(key string, value TaskStatus) error {
	return n.store.HSet(context.Background(), n.table, key, value, 0)
}

func (n *KvNodeStatusStore) GetMetaData() (MetaData, bool, error) {
	md := MetaData{}
	exist, err := n.store.HGet(context.Background(), n.table, "__meta", &md)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		return nil, false, nil
	}
	return md, true, nil
}

func (n *KvNodeStatusStore) SetMetaData(data MetaData) error {
	return n.store.HSet(context.Background(), n.table, "__meta", data, 0)
}

type Options struct {
	RedisURL     string // "redis://<user>:<pass>@localhost:6379/<db>"
	DelayedQueue store.DelayedQueue
}

func NewTickServer(p Options) *TickServer {
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

	_, debug := os.LookupEnv("GOTICK_DEBUG")

	t := &TickServer{
		flows:      map[string]*Flow{},
		asyncQueue: ap,
		closeChan:  make(chan bool),
		closeOnce:  sync.Once{},
		wg:         sync.WaitGroup{},
		scheduler:  NewScheduler(ap, st),
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
	redis     store.DelayedQueue
	key       string
	wg        *sync.WaitGroup
	closeChan chan bool
}

func (a *DelayedAsyncQueue) Publish(ctx context.Context, data Event, delay time.Duration) error {
	select {
	case <-a.closeChan:
		return errors.New("queue closed")
	default:
	}

	return a.redis.Publish(ctx, a.key, data.CallId, delay)
}

func (a *DelayedAsyncQueue) Subscribe(h func(ctx context.Context, data Event) error) {
	a.redis.Subscribe(a.key, func(ctx context.Context, data string) error {
		a.wg.Add(1)
		defer a.wg.Done()

		// 如果已经关闭，则不再处理
		select {
		case <-a.closeChan:
			return errors.New("queue closed")
		default:
		}

		return h(ctx, Event{CallId: data})
	})
}

func NewDelayedAsyncQueue(redis store.DelayedQueue, key string, wg *sync.WaitGroup, closeChan chan bool) *DelayedAsyncQueue {
	return &DelayedAsyncQueue{
		redis:     redis,
		key:       key,
		wg:        wg,
		closeChan: closeChan,
	}
}

// Sleep means the task is exec success and sleep for d.
func NewSleep(task string, d time.Duration) NextStatus {
	return NextStatus{Task: task, Status: "sleep", RunAt: time.Now().Add(d)}
}

// Done means the task is exec success
func Done(task string) NextStatus {
	return NextStatus{Status: "done", Task: task}
}

// Done means the task is exec success
func DoFunc(fun func() error) NextStatus {
	return NextStatus{Status: "done"}
}

// Abort means abort the flow
func Abort() NextStatus {
	return NextStatus{Status: "abort"}
}

// Abort means abort the flow
func Retry(task string, err error) NextStatus {
	return NextStatus{Status: "retry", Task: task, Err: err}
}

// Abort means abort the flow
func Fail(task string, err error) NextStatus {
	return NextStatus{Status: "fail", Task: task, Err: err}
}
