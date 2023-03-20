package tick

import (
	"context"
	"errors"
	"log"
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
	Key           string
	Func          func(ctx context.Context) error
	SleepDuration time.Duration
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

type Tick struct {
	Flows       map[string]*Flow
	statusStore NodeStatusStore
	asyncJob    AsyncJobProduct
}

type Flow struct {
	Id          string
	nodes       []Node
	statusStore NodeStatusStore
	asyncJob    AsyncJobProduct
}

type Scheler struct {
	FlowId string
	//Node   string
}

type AsyncJobProduct interface {
	Push(data Scheler, delay time.Duration)
}

func (t *Flow) Then(key string, f func(ctx context.Context) error) *Flow {
	t.nodes = append(t.nodes, Node{Key: key, Func: f})
	return t
}

func (t *Flow) Sleep(key string, d time.Duration) *Flow {
	t.nodes = append(t.nodes, Node{Key: key, SleepDuration: d})
	return t
}

// 调度 scheduler
func (t *Flow) Scheduler(callId string, data MetaData) error {
	ctx := context.Background()
	meta := MetaData{}
	ctx = WithMetaData(ctx, meta)

	// 从缓存中拿出上次的运行状态
	m, _, _ := t.statusStore.GetMetaData()
	if m != nil {
		ctx = WithMetaData(ctx, m)
	}

	for _, node := range t.nodes {
		ns, ok, _ := t.statusStore.GetNodeStatus(node.Key)
		if ok && ns.Status == "done" {
			// 跳过已经执行过的节点
			continue
		}

		// 异步任务
		if ok && ns.Status == "sleep" {
			// 等待一定时间，node.SleepDuration
			d := ns.RunAt.Sub(time.Now())
			if d > 0 {
				err := t.statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "sleep",
					RunAt:  time.Now().Add(d),
				})
				if err != nil {
					return err
				}

				t.asyncJob.Push(Scheler{
					FlowId: t.Id,
				}, d)

				// 终止流程，等待下次调度
				return nil
			} else {
				// 已经过时间了，当成执行成功
				err := t.statusStore.SetNodeStatus(node.Key, &NodeStatus{
					Status: "done",
					RunAt:  time.Time{},
				})
				if err != nil {
					return err
				}

				continue
			}
		}

		if node.Func == nil {
			err := t.statusStore.SetNodeStatus(node.Key, &NodeStatus{
				Status: "sleep",
				RunAt:  time.Now().Add(node.SleepDuration),
			})
			if err != nil {
				return err
			}

			t.asyncJob.Push(Scheler{
				FlowId: t.Id,
			}, node.SleepDuration)

			// 终止流程，等待下次调度
			return nil
		}

		// 执行节点
		err := node.Func(ctx)
		if err != nil {
			log.Printf("node %v exec error: %v", node.Key, err)
			return err
		}

		err = t.statusStore.SetNodeStatus(node.Key, &NodeStatus{
			Status: "done",
			RunAt:  time.Time{},
		})
		if err != nil {
			return err
		}

		err = t.statusStore.SetMetaData(meta)
		if err != nil {
			return err
		}

	}

	return nil
}

func (t *Tick) Flow(id string) *Flow {
	f := &Flow{
		Id:          id,
		nodes:       nil,
		statusStore: t.statusStore,
		asyncJob:    t.asyncJob,
	}
	t.Flows[id] = f
	return f
}

func (t *Tick) Touch(flow string, data MetaData) error {
	// touch 可以通过 mq 实现
	callId := "1"
	f := t.Flows[flow]
	if f == nil {
		return errors.New("flow not found")
	}
	return f.Scheduler(callId, data)
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

	st := MockNodeStatusStore{m: map[string]interface{}{}}
	t := &Tick{
		Flows:       map[string]*Flow{},
		statusStore: &st,
		asyncJob:    &aw,
	}

	b.OnCallback(func(s Scheler) error {
		return t.Touch(s.FlowId, MetaData{})
	})
	aw.OnCallback(func(s Scheler) error {
		return b.Publish(s)
	})
	return t
}
