package gotick

import (
	"context"
	rand2 "crypto/rand"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/zbysir/gotick/internal/store"
	"log"
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

type Task struct {
	Key  string
	Func NodeCaller
	//Options TaskOptions
	options taskOption
	//SleepDuration time.Duration
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

type AsyncQueenFactory interface {
	New(key string) AsyncQueen
	Start(ctx context.Context) error
}

type SimpleAsyncQueenProduct struct {
}

func (s SimpleAsyncQueenProduct) Start(ctx context.Context) error {
	return nil
}

func (s SimpleAsyncQueenProduct) New(key string) AsyncQueen {
	return &SimpleAsyncQueen{}
}

type TickServer struct {
	flows      map[string]*Flow
	asyncQueen AsyncQueenFactory
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
	tasks     []Task
	onFail    func(ctx context.Context, ts TaskStatus) error
	onSuccess func(ctx context.Context, ts TaskStatus) error
}

func (f *Flow) Success(fun func(ctx context.Context, ts TaskStatus) error) FailAble {
	f.onSuccess = fun
	return f
}

func (f *Flow) Fail(fun func(ctx context.Context, ts TaskStatus) error) SuccessAble {
	f.onFail = fun
	return f
}

type Event struct {
	CallId       string
	InitMetaData MetaData // 只有当第一次调度时有效
}

type AsyncQueen interface {
	// Publish 当 uniqueKey 不为空时，后面 Publish 的数据会覆盖前面的数据
	// uniqueKey 通常为 callId
	Publish(ctx context.Context, data Event, delay time.Duration) error
	Subscribe(h func(ctx context.Context, data Event) error)
}

type NextStatus struct {
	Status string // abort, sleep
	RunAt  time.Time
}

type ThenAble interface {
	SuccessAble
	FailAble
	Then(key string, c NodeCaller, opts ...TaskOption) ThenAble
}

type SuccessAble interface {
	// Success When task exec success, will call this function
	Success(f func(ctx context.Context, ts TaskStatus) error) FailAble
}

type FailAble interface {
	// Fail When task exec fail, will call this function
	Fail(f func(ctx context.Context, ts TaskStatus) error) SuccessAble
}

type NodeCaller func(ctx context.Context) (NextStatus, error)

func (f *Flow) Then(key string, c NodeCaller, opts ...TaskOption) ThenAble {
	f.tasks = append(f.tasks, Task{Key: key, Func: c, options: TaskOptions(opts).build()})
	return f
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

func (t *TickServer) Flow(id string) *Flow {
	f := &Flow{
		Id:    id,
		tasks: nil,
	}

	// 注册调度
	t.scheduler.register(f)

	t.flows[id] = f
	return f
}

type Scheduler struct {
	asyncScheduler AsyncQueenFactory
	statusStore    StoreFactory
}

func NewScheduler(asyncScheduler AsyncQueenFactory, statusStore StoreFactory) *Scheduler {
	return &Scheduler{asyncScheduler: asyncScheduler, statusStore: statusStore}
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

		statusStore := s.statusStore.New(callId)
		// 从缓存中拿出上次的运行状态
		m, _, _ := statusStore.GetMetaData()
		if m != nil {
			ctx = WithMetaData(ctx, m)
		}

		for _, node := range f.tasks {
			ns, ok, _ := statusStore.GetNodeStatus(node.Key)
			if ok {
				if ns.Status == "done" {
					// 跳过已经执行过的节点
					continue
				}
				if ns.Status == "abort" {
					// 终止流程
					status := ns.MakeAbort()
					err := f.onSuccess(ctx, status)
					if err != nil {
						return err
					}
					break
				}

			}

			// 异步任务
			if ok && ns.Status == "sleep" {
				// 等待一定时间，node.SleepDuration
				d := ns.RunAt.Sub(time.Now())
				// 还没到时间，重入调度
				if d > 0 {
					taskStatus := ns.MakeSleep(time.Now().Add(d))

					err := aw.Publish(ctx, Event{
						CallId: callId,
					}, d)
					if err != nil {
						return fmt.Errorf("scheduler event error: %w", err)
					}

					err = statusStore.SetNodeStatus(node.Key, taskStatus)
					if err != nil {
						return fmt.Errorf("save node status error: %w", err)
					}

					// 终止流程，等待下次调度
					return nil
				} else {
					// 已经过时间了，当成执行成功，进入下一个节点
					taskStatus := ns.MakeDone()
					err := statusStore.SetNodeStatus(node.Key, taskStatus)
					if err != nil {
						return err
					}

					continue
				}
			}

			// 执行节点
			nextStatus, funcError := node.Func(ctx)
			if funcError != nil {
				// 默认需要重试，如果不需要重试，应该返回 AboutError
				if errors.Is(funcError, AbortError) {
					if f.onFail != nil {
						status := ns.MakeAbort()
						err := f.onFail(ctx, status)
						if err != nil {
							return err
						}
						err = statusStore.SetNodeStatus(node.Key, status)
						if err != nil {
							return fmt.Errorf("save node status error: %w", err)
						}
					}

					return nil
				} else {
					// 重试
					retryd := ns.RetryCount
					if retryd >= node.options.MaxRetry {
						// 超过次数，不再重试
						status := ns.MakeFail(nil)
						err := f.onFail(ctx, status)
						if err != nil {
							return err
						}
						err = statusStore.SetNodeStatus(node.Key, status)
						if err != nil {
							return fmt.Errorf("save node status error: %w", err)
						}

						return nil
					} else {
						// 重试 下一次调度
						err := aw.Publish(ctx, Event{
							CallId: callId,
						}, time.Duration(retryd+1)*time.Second)
						if err != nil {
							return fmt.Errorf("scheduler event error: %w", err)
						}

						status := ns.MakeRetry(funcError)
						log.Printf("--- retry --- %v %v %v", retryd, callId, ns)
						err = statusStore.SetNodeStatus(node.Key, status)
						if err != nil {
							return fmt.Errorf("save node status error: %w", err)
						}

						return nil
					}
				}
			}

			funcError = statusStore.SetMetaData(meta)
			if funcError != nil {
				return fmt.Errorf("save meta data error: %w", funcError)
			}

			switch nextStatus.Status {
			case "abort":
				taskStatus := ns.MakeAbort()
				funcError = statusStore.SetNodeStatus(node.Key, taskStatus)
				if funcError != nil {
					return fmt.Errorf("save node status error: %w", funcError)
				}

				if f.onSuccess != nil {
					err := f.onSuccess(ctx, taskStatus)
					if err != nil {
						return err
					}
				}

				// 终止整个流程
				return nil
			case "sleep":
				d := nextStatus.RunAt.Sub(time.Now())
				if d > 0 {
					taskStatus := ns.MakeSleep(nextStatus.RunAt)
					funcError = aw.Publish(ctx, Event{
						CallId: callId,
					}, d)
					if funcError != nil {
						return fmt.Errorf("scheduler event error: %w", funcError)
					}

					funcError = statusStore.SetNodeStatus(node.Key, taskStatus)
					if funcError != nil {
						return fmt.Errorf("save node status error: %w", funcError)
					}
				} else {
					taskStatus := TaskStatus{
						Status: "done",
						RunAt:  time.Time{},
					}
					// 已经过时间了，当成执行成功，进入下一个节点
					err := statusStore.SetNodeStatus(node.Key, taskStatus)
					if err != nil {
						return fmt.Errorf("save node status error: %w", err)
					}

					continue
				}

			default:
				// 默认为 done
				taskStatus := ns.MakeDone()

				// 默认情况下进入调度循环
				err := aw.Publish(ctx, Event{
					CallId: callId,
				}, 0)
				if err != nil {
					return fmt.Errorf("scheduler event error: %w", err)
				}
				err = statusStore.SetNodeStatus(node.Key, taskStatus)
				if err != nil {
					return fmt.Errorf("save node status error: %w", err)
				}
			}

			return nil
		}

		if f.onSuccess != nil {
			// 如果什么都没做，默认就是完成状态
			taskStatus := TaskStatus{
				Status: "done",
			}
			err := f.onSuccess(ctx, taskStatus)
			if err != nil {
				return err
			}
		}

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
			log.Printf("async queen start error: %v", err)
		}
	}()

	schedulerWg.Wait()
	// 先等待 asyncQueen（消费者）关闭之后才应该关闭 tick。
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

// SimpleAsyncQueen 使用 Goroutine 实现的异步队列
// 它在重启之后会丢数据，只应该在测试使用。
// - 使用它可以达到最大定时的精度（毫秒级）
// - 缺点是可能会占用更多的内容并且重启恢复需要时间。
// - 所有任务都会在同一个机器上执行，可能导致热点问题。
type SimpleAsyncQueen struct {
	callback []func(ctx context.Context, s Event) error
}

func (a *SimpleAsyncQueen) Publish(ctx context.Context, data Event, delay time.Duration) error {
	go func() {
		time.Sleep(delay)
		for _, c := range a.callback {
			c(ctx, data)
		}
	}()

	return nil
}

func (a *SimpleAsyncQueen) Subscribe(f func(ctx context.Context, s Event) error) {
	a.callback = append(a.callback, f)
}

func (a *SimpleAsyncQueen) Exist(uniqueKey []string) (map[string]bool, error) {
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
	ap := NewAsyncQueenProduct(p.DelayedQueue)

	t := &TickServer{
		flows:      map[string]*Flow{},
		asyncQueen: ap,
		closeChan:  make(chan bool),
		closeOnce:  sync.Once{},
		wg:         sync.WaitGroup{},
		scheduler:  NewScheduler(ap, st),
	}

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
	ap := NewAsyncQueenProduct(p.DelayedQueue)

	t := &TickClient{
		scheduler: NewScheduler(ap, st),
	}

	return t
}

type DelayedAsyncQueenProduct struct {
	queen     store.DelayedQueue
	wg        sync.WaitGroup
	closeChan chan bool
}

func (a *DelayedAsyncQueenProduct) Start(ctx context.Context) (err error) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = a.queen.Start(ctx)
	}()

	wg.Wait()

	close(a.closeChan)
	// wait for all queen down
	a.wg.Wait()

	return
}

func NewAsyncQueenProduct(redis store.DelayedQueue) *DelayedAsyncQueenProduct {
	return &DelayedAsyncQueenProduct{queen: redis, closeChan: make(chan bool)}
}

func (a *DelayedAsyncQueenProduct) New(key string) AsyncQueen {
	x := NewDelayedAsyncQueen(a.queen, key, &a.wg, a.closeChan)
	return x
}

type DelayedAsyncQueen struct {
	redis     store.DelayedQueue
	key       string
	wg        *sync.WaitGroup
	closeChan chan bool
}

func (a *DelayedAsyncQueen) Publish(ctx context.Context, data Event, delay time.Duration) error {
	select {
	case <-a.closeChan:
		return errors.New("queen closed")
	default:
	}

	return a.redis.Publish(ctx, a.key, data.CallId, delay)
}

func (a *DelayedAsyncQueen) Subscribe(h func(ctx context.Context, data Event) error) {
	a.redis.Subscribe(a.key, func(ctx context.Context, data string) error {
		a.wg.Add(1)
		defer a.wg.Done()

		return h(ctx, Event{CallId: data})
	})
}

func NewDelayedAsyncQueen(redis store.DelayedQueue, key string, wg *sync.WaitGroup, closeChan chan bool) *DelayedAsyncQueen {
	return &DelayedAsyncQueen{
		redis:     redis,
		key:       key,
		wg:        wg,
		closeChan: closeChan,
	}
}

// Sleep means the task is exec success and sleep for d.
func Sleep(d time.Duration) NextStatus {
	return NextStatus{Status: "sleep", RunAt: time.Now().Add(d)}
}

// Done means the task is exec success
func Done() NextStatus {
	return NextStatus{Status: "done"}
}

// Abort means abort the flow
func Abort() NextStatus {
	return NextStatus{Status: "abort"}
}
