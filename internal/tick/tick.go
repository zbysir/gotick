package tick

import (
	"context"
	rand2 "crypto/rand"
	"errors"
	"fmt"
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
	//SleepDuration time.Duration
}

type Set interface {
	Push(i interface{})
}

type NodeStatus struct {
	Status string
	RunAt  time.Time
}

type NodeStatusStore interface {
	GetNodeStatus(key string) (*NodeStatus, bool, error)
	SetNodeStatus(key string, value *NodeStatus) error
	GetMetaData() (MetaData, bool, error)
	SetMetaData(MetaData) error
	Clear() error // 删除所有数据
}

type StoreProduct interface {
	New(key string) NodeStatusStore
}
type AsyncQueenProduct interface {
	New(key string) AsyncQueen
	Start(ctx context.Context)
}

type SimpleAsyncQueenProduct struct {
}

func (s SimpleAsyncQueenProduct) Start(ctx context.Context) {

}

func (s SimpleAsyncQueenProduct) New(key string) AsyncQueen {
	return &SimpleAsyncQueen{}
}

type Tick struct {
	Flows       map[string]*Flow
	statusStore StoreProduct
	asyncQueen  AsyncQueenProduct
	close       bool
	wg          sync.WaitGroup
}

type Flow struct {
	Id             string
	tasks          []Task
	asyncScheduler AsyncQueen
	statusStore    StoreProduct
	t              *Tick
}

type Event struct {
	CallId       string
	InitMetaData MetaData // 只有当第一次调度时有效
}

type AsyncQueen interface {
	// Publish 当 uniqueKey 不为空时，后面 Publish 的数据会覆盖前面的数据
	// uniqueKey 通常为 callId
	Publish(data Event, delay time.Duration) error
	Exist(uniqueKey []string) (map[string]bool, error)
	Subscribe(h func(data Event) error)
}

type NextStatus struct {
	Status string // abort, sleep
	RunAt  time.Time
}

type NodeCaller func(ctx context.Context) (NextStatus, error)

func (f *Flow) Then(key string, c NodeCaller) *Flow {
	f.tasks = append(f.tasks, Task{Key: key, Func: c})
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

// Trigger 触发一次流程运行
func (f *Flow) Trigger(params MetaData) (callId string, err error) {
	callId = randomStr()
	event := Event{
		CallId:       callId,
		InitMetaData: params,
	}
	err = f.scheduler(event)
	return
}

// scheduler 调度一次
func (f *Flow) scheduler(event Event) error {
	if f.t.close {
		return errors.New("tick is closing")
	}

	f.t.wg.Add(1)
	defer f.t.wg.Done()

	callId := event.CallId
	ctx := context.Background()
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

	statusStore := f.statusStore.New(callId)
	// 从缓存中拿出上次的运行状态
	m, _, _ := statusStore.GetMetaData()
	if m != nil {
		ctx = WithMetaData(ctx, m)
	}

	for _, node := range f.tasks {
		ns, ok, _ := statusStore.GetNodeStatus(node.Key)
		if ok && ns.Status == "done" {
			// 跳过已经执行过的节点
			continue
		}

		// 异步任务
		if ok && ns.Status == "sleep" {
			// 等待一定时间，node.SleepDuration
			d := ns.RunAt.Sub(time.Now())
			// 还没到时间，重入调度
			if d > 0 {
				err := statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "sleep",
					RunAt:  time.Now().Add(d),
				})
				if err != nil {
					return err
				}

				f.asyncScheduler.Publish(Event{
					CallId: callId,
				}, d)

				// 终止流程，等待下次调度
				return nil
			} else {
				// 已经过时间了，当成执行成功，进入下一个节点
				err := statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "done",
					RunAt:  time.Time{},
				})
				if err != nil {
					return err
				}

				continue
			}
		}

		// 执行节点
		nextStatus, err := node.Func(ctx)
		if err != nil {
			log.Printf("node %v exec error: %v", node.Key, err)
			return err
		}

		err = statusStore.SetMetaData(meta)
		if err != nil {
			return err
		}

		switch nextStatus.Status {
		case "abort":
			err = statusStore.SetNodeStatus(node.Key, &NodeStatus{
				Status: "done",
				RunAt:  time.Time{},
			})
			if err != nil {
				return err
			}

			// 终止流程
			goto finish
		case "sleep":
			d := nextStatus.RunAt.Sub(time.Now())
			if d > 0 {
				err = statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "sleep",
					RunAt:  nextStatus.RunAt,
				})

				f.asyncScheduler.Publish(Event{
					CallId: callId,
				}, d)
			} else {
				// 已经过时间了，当成执行成功
				err := statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "done",
					RunAt:  time.Time{},
				})
				if err != nil {
					return err
				}
			}

		default:
			err = statusStore.SetNodeStatus(node.Key, &NodeStatus{
				Status: "done",
				RunAt:  time.Time{},
			})
			if err != nil {
				return err
			}

			// 默认情况下进入调度循环
			f.asyncScheduler.Publish(Event{
				CallId: callId,
			}, 0)
		}

		return nil
	}

finish:

	// 执行到这里就是任务完成了
	log.Printf("---- end ----")

	return nil
}

func (t *Tick) Flow(id string) *Flow {
	aw := t.asyncQueen.New(id)
	f := &Flow{
		Id:             id,
		tasks:          nil,
		asyncScheduler: aw,
		statusStore:    t.statusStore,
		t:              t,
	}

	// 订阅异步队列
	aw.Subscribe(f.scheduler)

	t.Flows[id] = f
	return f
}

func randomStr() string {
	b := make([]byte, 16)
	_, _ = rand2.Read(b)
	return fmt.Sprintf("%x", b)
}

func (t *Tick) Trigger(flowId string, data MetaData) (string, error) {
	f := t.Flows[flowId]
	if f == nil {
		return "", errors.New("flow not found")
	}

	return f.Trigger(data)
}

func (t *Tick) Start(ctx context.Context) {
	t.asyncQueen.Start(ctx)

	select {
	case <-ctx.Done():
		t.close = true
	}

	// 启动异步队列

	t.wg.Wait()
	return
}

// SimpleAsyncQueen 使用 Goroutine 实现的异步队列
// 不过不用担心它在重启之后不会触发，因为 tick 会记录所有 call 的状态，在重启之后会从缓存中重新调度 call（未完成的）。
// - 使用它可以达到最大定时的精度（毫秒级）
// - 缺点是可能会占用更多的内容并且重启恢复需要时间。
// - 所有任务都会在同一个机器上执行，可能导致热点问题。
type SimpleAsyncQueen struct {
	callback []func(s Event) error
}

func (a *SimpleAsyncQueen) Publish(data Event, delay time.Duration) error {
	go func() {
		time.Sleep(delay)
		for _, c := range a.callback {
			c(data)
		}
	}()

	return nil
}

func (a *SimpleAsyncQueen) Subscribe(f func(s Event) error) {
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

func (m *MockNodeStatusStore) GetNodeStatus(key string) (*NodeStatus, bool, error) {
	i, ok := m.m[key]
	if !ok {
		return nil, false, nil
	}
	return i.(*NodeStatus), true, nil
}

func (m *MockNodeStatusStore) SetNodeStatus(key string, value *NodeStatus) error {
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

func NewTickServer(st StoreProduct, ap AsyncQueenProduct) *Tick {
	t := &Tick{
		Flows:       map[string]*Flow{},
		statusStore: st,
		asyncQueen:  ap,
		close:       false,
		wg:          sync.WaitGroup{},
	}

	return t
}
