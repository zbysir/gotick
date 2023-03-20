package tick

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
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

type Node struct {
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
}

type StoreProduct interface {
	New(key string) NodeStatusStore
}

type Tick struct {
	Flows       map[string]*Flow
	statusStore StoreProduct
	asyncJob    AsyncJobProduct
}

type Flow struct {
	Id       string
	nodes    []Node
	asyncJob AsyncJobProduct
}

type Scheler struct {
	FlowId string
	//Node   string
	CallId   string
	MetaData MetaData
}

type AsyncJobProduct interface {
	Push(data Scheler, delay time.Duration)
}

type NextStatus struct {
	status string // abort, sleep
	runAt  time.Time
}

type NodeCaller func(ctx context.Context) (NextStatus, error)

func (t *Flow) Then(key string, f NodeCaller) *Flow {
	t.nodes = append(t.nodes, Node{Key: key, Func: f})
	return t
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

// 调度 scheduler
func (t *Flow) Scheduler(statusStore NodeStatusStore, callId string, data MetaData) error {
	ctx := context.Background()
	meta := MetaData{}
	ctx = WithMetaData(ctx, meta)
	ctx = WithCallId(ctx, callId)
	// 从缓存中拿出上次的运行状态
	m, _, _ := statusStore.GetMetaData()
	if m != nil {
		ctx = WithMetaData(ctx, m)
	}

	// 拼装上额外的 data
	if data != nil {
		ctx = WithMetaData(ctx, data)
	}

	for _, node := range t.nodes {
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

				t.asyncJob.Push(Scheler{
					FlowId: t.Id,
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

		switch nextStatus.status {
		case "abort":
			err = statusStore.SetNodeStatus(node.Key, &NodeStatus{
				Status: "done",
				RunAt:  time.Time{},
			})
			if err != nil {
				return err
			}

			// 终止流程
			return nil
		case "sleep":
			d := nextStatus.runAt.Sub(time.Now())
			if d > 0 {
				err = statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "sleep",
					RunAt:  nextStatus.runAt,
				})

				t.asyncJob.Push(Scheler{
					FlowId: t.Id,
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

			// 下此次调度
			t.asyncJob.Push(Scheler{
				FlowId: t.Id,
				CallId: callId,
			}, 0)
		}

		return nil
	}

	return nil
}

func (t *Tick) Flow(id string) *Flow {
	f := &Flow{
		Id:       id,
		nodes:    nil,
		asyncJob: t.asyncJob,
	}
	t.Flows[id] = f
	return f
}

func randomStr() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func (t *Tick) Touch(s Scheler) error {
	callId := s.CallId
	if callId == "" {
		callId = randomStr()
	}
	f := t.Flows[s.FlowId]
	if f == nil {
		return errors.New("flow not found")
	}

	st := t.statusStore.New(callId)

	return f.Scheduler(st, callId, s.MetaData)
}

type Bridge interface {
	Publish(s Scheler) error
	OnCallback(f func(s Scheler) error)
}

type MockAsyncWorker struct {
	callback func(s Scheler) error
}

func (a *MockAsyncWorker) Push(data Scheler, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		a.callback(data)
	}()
}

func (a *MockAsyncWorker) OnCallback(f func(s Scheler) error) {
	a.callback = f
}

type NodeStatusStoreProduct struct {
	m map[string]NodeStatusStore
}

func (n *NodeStatusStoreProduct) New(key string) NodeStatusStore {
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

func NewTickServer(b Bridge) *Tick {
	aw := MockAsyncWorker{}

	st := NodeStatusStoreProduct{}
	t := &Tick{
		Flows:       map[string]*Flow{},
		statusStore: &st,
		asyncJob:    &aw,
	}

	b.OnCallback(func(s Scheler) error {
		return t.Touch(s)
	})
	aw.OnCallback(func(s Scheler) error {
		return b.Publish(s)
	})
	return t
}
