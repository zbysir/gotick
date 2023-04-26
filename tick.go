package gotick

import (
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/zbysir/gotick/internal/pkg/flow"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"os"
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
}

func (c *Context) MetaDataAll() MetaData {
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

func UseSequence(ctx *Context, key string, maxLen int) Sequence {
	key = fmt.Sprintf("__%v", key)
	s, ok, _ := GetFromStore[Sequence](ctx.store, key)
	if !ok {
		return Sequence{
			Current: -1, // skip first next()
			max:     maxLen,
			name:    key,
			ctx:     ctx,
		}
	}

	return s
}

func UseMemo[T interface{}](ctx *Context, key string, build func() (T, error)) T {
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
	return fmt.Sprintf("%v/@/%v:%v", a.ProductKey, prefix, a.Val)
}

func UseArray[T interface{}](ctx *Context, key string, build func() ([]T, error)) []ArrayWrap[T] {
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

func Task(c *Context, key string, fun TaskFun, opts ...TaskOption) {
	if c.collect != nil {
		if c.collect("task", key) {
			return
		}
	}

	s, exist, _ := c.store.GetNodeStatus(key)
	// todo panic error

	o := TaskOptions(opts).build()

	taskContext := newTaskContext(c, s)
	if !exist {
		err := fun(taskContext)
		if err != nil {
			if s.RetryCount > o.MaxRetry {
				panic(Fail(s, key, err))
			}
			panic(Retry(s, key, err))
		}
		panic(Done(s, key))
	}
	if s.Status == "retry" {
		err := fun(taskContext)
		if err != nil {
			if s.RetryCount > o.MaxRetry {
				panic(Fail(s, key, err))
			}
			panic(Retry(s, key, err))
		}
		panic(Done(s, key))
	}
}

func Sleep(c *Context, key string, duration time.Duration) {
	if c.collect != nil {
		if c.collect("sleep", key) {
			return
		}
	}
	s, exist, _ := c.store.GetNodeStatus(key)
	// todo panic error

	if !exist {
		panic(NewSleep(s, key, duration))
	}

	if s.Status == "sleep" {
		d := s.RunAt.Sub(time.Now())
		if d > 0 {
			panic(NewSleep(s, key, d))
		}

		_ = c.store.SetNodeStatus(key, s.MakeDone())
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
	Key        string
	Status     string    `json:"status"`
	RunAt      time.Time `json:"run_at"` // sleep 到的时间
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
	GetNodeStatus(key string) (TaskStatus, bool, error) // 获取每个 task 的运行状态
	SetNodeStatus(key string, value TaskStatus) error
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

// DAG 生成一个数据流图
// 可以使用 reactflow 绘制。
func (f *Flow) DAG() (flow.DAG, error) {
	dag := flow.DAG{}

	err := f.fun(&Context{
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
						Label: fmt.Sprintf("[array] %s", key),
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

	return dag, err
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

type BreakStatus struct {
	Status     string // abort, sleep, retry, done, fail
	RunAt      time.Time
	Task       string
	Err        error
	TaskStatus TaskStatus
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

type NodeCaller func(ctx context.Context) (BreakStatus, error)

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

		err := func() error {
			ctx := &Context{
				Context: ctx,
				CallId:  callId,
				store:   statusStore,
			}

			defer func() {
				r := recover()
				if r == nil {
					return
				}

				ns, ok := r.(BreakStatus)
				if !ok {
					panic(r)
				}

				if s.debug {
					log.Printf("[gotick] %v", ns)
				}

				switch ns.Status {
				case "retry":
					// 存储重试次数
					_ = statusStore.SetNodeStatus(ns.Task, ns.TaskStatus.MakeRetry(ns.Err))
					// 进入下次调度
					err := aw.Publish(ctx, Event{
						CallId: callId,
					}, time.Duration(ns.TaskStatus.RetryCount)*time.Second)
					if err != nil {
						log.Printf("scheduler event error: %v", err)
					}
				case "abort":
					if f.onFail != nil {
						err := f.onFail(ctx, ns.TaskStatus.MakeAbort())
						if err != nil {
							panic(err)
						}
					}
				case "fail":
					if f.onFail != nil {
						err := f.onFail(ctx, ns.TaskStatus.MakeFail(ns.Err))
						if err != nil {
							panic(err)
						}
					}
				case "sleep":
					// 进入下次调度
					now := time.Now()
					_ = statusStore.SetNodeStatus(ns.Task, ns.TaskStatus.MakeSleep(ns.RunAt))
					err := aw.Publish(ctx, Event{
						CallId: callId,
					}, ns.RunAt.Sub(now))
					if err != nil {
						log.Printf("scheduler event error: %v", err)
					}
				case "done":
					fallthrough
				default:
					_ = statusStore.SetNodeStatus(ns.Task, ns.TaskStatus.MakeDone())
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

func (n *KvNodeStatusStore) SetNodeStatus(key string, value TaskStatus) error {
	return n.store.HSet(context.Background(), n.statusKey(), key, value, 0)
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

	bs, _ := json.Marshal(data)

	return a.redis.Publish(ctx, a.key, string(bs), delay)
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

		var ev Event
		json.Unmarshal([]byte(data), &ev)

		return h(ctx, ev)
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
func NewSleep(s TaskStatus, task string, d time.Duration) BreakStatus {
	return BreakStatus{Task: task, Status: "sleep", RunAt: time.Now().Add(d), TaskStatus: s}
}

// Done means the task is exec success
func Done(s TaskStatus, task string) BreakStatus {
	return BreakStatus{Status: "done", Task: task, TaskStatus: s}
}

// Done means the task is exec success
func DoFunc(fun func() error) BreakStatus {
	return BreakStatus{Status: "done"}
}

// Abort means abort the flow
func Abort() BreakStatus {
	return BreakStatus{Status: "abort"}
}

// Abort means abort the flow
func Retry(s TaskStatus, task string, err error) BreakStatus {
	return BreakStatus{Status: "retry", Task: task, Err: err, TaskStatus: s}
}

// Fail means abort the flow
func Fail(s TaskStatus, task string, err error) BreakStatus {
	return BreakStatus{Status: "fail", Task: task, Err: err, TaskStatus: s}
}
