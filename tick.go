package gotick

import (
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zbysir/gotick/store"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
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
func AsyncArray[T interface{}, A interface{}](ctx *Context, key string, arr []ArrayWrap[A], f func(ctx *TaskContext, a A, index int) (T, error)) []Future {
	var fs []Future

	for index, t := range arr {
		// 注意闭包问题
		t := t
		fs = append(fs, Async(ctx, t.Key(key), func(ctx *TaskContext) (T, error) {
			return f(ctx, t.Val, index)
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

type ParallelOption struct {
	Parallel           int // 如果不设置，则不并发。
	BatchSizePerRunner int // 如果不设置，则会在这个 runner 上运行完所有 Future
}

// WaitFast 等待所有任务完成。
// 如果是不是快速模式，每个 task 都会被重新调度到任何一台 runner 上，并且会有一些延迟（任务队列的最低延迟）。
// 如果是快速模式，那么在一个 runner 上会执行多次，直到达到 BatchSizePerRunner 才会让出任务。
// 例如 BatchSizePerRunner = 10，那么每完成 10 个任务就会重新调度一次（可能会调度到其他 runner 上）。
func WaitFast(ctx *Context, opt ParallelOption, fs ...Future) {
	// log.Printf("[gotick] WaitFast start, opt: %v", opt)
	runCount := int64(0)
	wg := sync.WaitGroup{}

	var maxParallelChan chan struct{}
	if opt.Parallel > 0 {
		maxParallelChan = make(chan struct{}, opt.Parallel)
	}

	doneCount := int64(0)

	for _, f := range fs {
		s, exist, _ := ctx.store.GetNodeStatus(f.key())
		// 这里的 for 循环会导致过多的判断，也就是每次调度，都会再次判断之前所有的任务状态，也许可以优化？

		if exist {
			// 有任务正在运行中，跳过执行
			switch s.Status {
			case "done":
				// 任务正在其他节点上（包括自己）执行，跳过而执行后面的任务
				atomic.AddInt64(&doneCount, 1)
				continue
			case "fail":
				// 如果有一个任务失败了，则算整个失败。
				panic(BreakFail(f.key(), fmt.Errorf("task %v, error %v: ", s.Key, strings.Join(s.Errs, ";"))))
			}

			if s.Status == "retry" {
				// 如果一个任务是 retry 状态，则需要重新执行
			} else {
				continue
			}
		}

		// 如果任务没有状态，或则到了应该执行的时间，就需要执行
		if !(s.RunAt.IsZero() || s.RunAt.Before(time.Now())) {
			continue
		}

		if maxParallelChan != nil {
			// 并发满了，则阻塞直到其他任务做完
			maxParallelChan <- struct{}{}
		}

		wg.Add(1)
		go func(future Future, s TaskStatus) {
			defer func() {
				wg.Done()
				atomic.AddInt64(&doneCount, 1)

				if maxParallelChan != nil {
					<-maxParallelChan
				}
			}()
			errorc := make(chan error)
			datac := make(chan interface{})

			responseChan := make(chan struct{})

			// 启动心跳
			go func() {
				defer close(responseChan)
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

						// log.Printf("[gotick] task done: %v", future.key())
						return
					case <-time.After(3 * time.Second):
						// 心跳续期
						_ = ctx.store.SetNodeStatus(future.key(), s.MakeRunning(), 6*time.Second)
					}
				}

			}()

			// log.Printf("[gotick] task running: %v, runCount: %v", future.key(), runCount)

			t, err := future.exec()
			if err != nil {
				errorc <- err
			} else {
				datac <- t
			}

			<-responseChan // 等待后台的心跳任务完成

		}(f, s)

		// 如果达到了最大运行数，则跳出循环
		if opt.BatchSizePerRunner > 0 {
			if atomic.AddInt64(&runCount, 1) >= int64(opt.BatchSizePerRunner) {
				break
			}
		}
	}

	wg.Wait()

	// log.Printf("[gotick] WaitFast done, doneCount: %v", doneCount)

	if atomic.LoadInt64(&doneCount) != int64(len(fs)) {
		//log.Printf("step BreakWait 10")
		// 如果还有任务没完成，则等待一会进行下一个调度
		// 在下一个调度中，会再次检查任务状态，同时检查任务心跳，如果任务没有心跳则也会重启任务。
		// log.Printf("[gotick] task next")

		panic(BreakWait(0))
	}

}

// Wait will wait all future done, if Parallel is not 0, then no limit for parallel.
// Wait 在大量任务时，会出现明显的延迟，因为每执行完一次
func Wait(ctx *Context, parallel int, fs ...Future) {
	allDone := true
	runCount := 0

	// wg := sync.WaitGroup{}
	// 获取并并行 parallel 个任务，等这些任务都完成了，才会开始下一个循环
	// 可能出现短板，运行时间取决于 n 个任务中最长耗时的任务

	for _, f := range fs {
		s, exist, _ := ctx.store.GetNodeStatus(f.key())
		// 这里的 for 循环会导致过多的判断，也就是每次调度，都会再次判断之前所有的任务状态，也许可以优化？

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
				// 任务正在其他节点上（包括自己）执行，跳过而执行后面的任务
				runCount++
				continue
			}
		} else {
			allDone = false
		}

		// 如果任务是重试状态，或没有状态，就需要执行
		if (s.RunAt.IsZero() || s.RunAt.Before(time.Now())) && (parallel <= 0 || runCount < parallel) {
			// 如果是 retry，也改为执行状态，让下次调度跳过这次任务
			_ = ctx.store.SetNodeStatus(f.key(), s.MakeRunning(), 6*time.Second)

			// 如果没到执行时间，则不执行

			// wg.Add(1)

			// 如果 fastMode 为 true
			//

			//log.Printf("step Run")
			go func(future Future, s TaskStatus) {
				// defer wg.Done()

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

							// log.Printf("[gotick] task done: %v", future.key())
							return
						case <-time.After(3 * time.Second):
							// 心跳续期
							_ = ctx.store.SetNodeStatus(future.key(), s.MakeRunning(), 6*time.Second)
						}
					}
				}()

				log.Printf("[gotick] task running: %v, runCount: %v", future.key(), runCount)

				t, err := future.exec()
				if err != nil {
					errorc <- err
				} else {
					datac <- t
				}
			}(f, s)

			// 并行
			log.Printf("step BreakWait nextCall: %s ", f.key())
			panic(BreakWait(0))
		}
	}

	// wg.Wait()

	if !allDone {
		//log.Printf("step BreakWait 10")
		// 如果还有任务没完成，则等待任务完成
		// 循环 1 s 调度一次，检查任务状态，同时检查任务心跳，如果任务没有心跳则也会重启任务。
		log.Printf("[gotick] task next")

		// 为什么要等待 1s?
		// 如果所有任务都在后台执行，那么 Wait 方法会快速的执行完成，导致很平凡的调度。
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

func Array[T interface{}](ctx *Context, key string, build func(ctx *TaskContext) ([]T, error), opts ...TaskOption) []ArrayWrap[T] {
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

	// 如果没有数据，无论任务状态是什么都始终执行
	v, exist, _ := GetFromStore[[]ArrayWrap[T]](ctx.store, key)
	// todo panic error
	if exist {
		return v
	}

	s, exist, _ := ctx.store.GetNodeStatus(key)
	opt := TaskOptions(opts).build()

	taskContext := newTaskContext(ctx, s)

	t, err := build(taskContext)
	if err != nil {
		if errors.Is(err, AbortError) {
			panic(BreakAbort(key, err))
		}
		if s.RetryCount > opt.MaxRetry {
			panic(BreakFail(key, err))
		}
		panic(BreakRetry(key, err))
	}

	a := make([]ArrayWrap[T], len(t))
	for i, v := range t {
		a[i] = ArrayWrap[T]{
			ProductKey: key,
			Val:        v,
			Index:      i,
		}
	}
	_ = SetToStore(ctx.store, key, a)

	// 执行成功也需要断点，因为需要依靠断点来存储任务状态。
	panic(BreakDone(key))
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

	opt := TaskOptions(opts).build()

	taskContext := newTaskContext(c, s)
	if !exist || s.Status == "retry" {
		err := fun(taskContext)
		if err != nil {
			if errors.Is(err, AbortError) {
				panic(BreakAbort(key, err))
			}
			if s.RetryCount > opt.MaxRetry {
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

// maxRetry 最大重试次数, 如果为 -1，则不重试。
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

type Server struct {
	scheduler *Scheduler
	measure   Measure
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

// Client 用于触发调度，和 TickServer 的区别是，Client 不会启动调度器。
type Client struct {
	trigger *Trigger
}

type Flow struct {
	Id        string
	fun       func(ctx *Context)
	onFail    func(ctx *Context, ts TaskStatus) error
	onError   func(ctx *Context, ts TaskStatus) error
	onSuccess func(ctx *Context) error
	opt       flowOpt
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
func (t *Server) Flow(id string, fun func(ctx *Context), opts ...FlowOption) *Flow {
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

	return f
}

type Scheduler struct {
	asyncScheduler AsyncQueueFactory
	statusFactory  StoreFactory
	trigger        *Trigger
	debug          bool
}

func NewScheduler(asyncScheduler AsyncQueueFactory, statusStore StoreFactory) *Scheduler {
	return &Scheduler{asyncScheduler: asyncScheduler, statusFactory: statusStore, trigger: NewTrigger(asyncScheduler)}
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
func (s *Scheduler) Trigger(ctx context.Context, flowId string, initData MetaData, delay time.Duration) (string, error) {
	return s.trigger.Trigger(ctx, flowId, initData, delay)
}

type Trigger struct {
	asyncScheduler AsyncQueueFactory
}

func NewTrigger(asyncScheduler AsyncQueueFactory) *Trigger {
	return &Trigger{asyncScheduler: asyncScheduler}
}

// Trigger 触发一次流程运行
func (t *Trigger) Trigger(ctx context.Context, flowId string, initData MetaData, delay time.Duration) (string, error) {
	callId := randomStr()
	event := Event{
		CallId:       callId,
		InitMetaData: initData,
	}
	err := t.asyncScheduler.New(flowId).Publish(ctx, event, delay)
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
func (t *Server) Trigger(ctx context.Context, flowId string, data MetaData) (string, error) {
	return t.scheduler.Trigger(ctx, flowId, data, 0)
}

// Trigger 触发一次流程运行，在服务端和客户端都可以调用。
func (t *Client) Trigger(ctx context.Context, flowId string, data MetaData, delay time.Duration) (string, error) {
	return t.trigger.Trigger(ctx, flowId, data, delay)
}

// StartServer 启动服务，在服务端应该调用此方法开始执行异步任务。
// 当 ctx 被关闭时，服务也会关闭。
func (t *Server) StartServer(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.scheduler.Start(ctx)
		if err != nil {
			log.Printf("async queue start error: %v", err)
		}
	}()

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

type Config struct {
	RedisURL          string                // "redis://<user>:<pass>@localhost:6379/<db>"
	RedisClient       redis.UniversalClient // if RedisURL is not set, use this client
	Concurrency       int                   // default 10
	TaskCheckInterval time.Duration         // default 100ms
}

func newScheduler(delayedQueue store.DelayedQueue, kvStore store.KVStore) *Scheduler {
	ap := NewAsyncQueueFactory(delayedQueue)
	st := NewKvStoreProduct(kvStore)
	_, debug := os.LookupEnv("GOTICK_DEBUG")

	scheduler := NewScheduler(ap, st)
	scheduler.debug = debug

	return scheduler
}

func newSchedulerFromConfig(p Config) *Scheduler {
	opt, err := redis.ParseURL(p.RedisURL)
	if err != nil {
		panic(err)
	}
	var redisClient redis.UniversalClient
	if p.RedisClient != nil {
		redisClient = p.RedisClient
	} else {
		redisClient = redis.NewClient(opt)
	}

	delayedQueue := store.NewAsynq(redisClient, asynq.Config{
		Concurrency:       p.Concurrency,
		TaskCheckInterval: p.TaskCheckInterval,
	})
	kvStore := store.NewRedisStore(redisClient)

	return newScheduler(delayedQueue, kvStore)
}

func NewServerFromConfig(p Config) *Server {
	scheduler := newSchedulerFromConfig(p)
	t := &Server{
		scheduler: scheduler,
	}

	return t
}

type NewServerParams struct {
	DelayedQueue store.DelayedQueue
	KVStore      store.KVStore
}

func NewServer(p NewServerParams) *Server {
	scheduler := newScheduler(p.DelayedQueue, p.KVStore)
	t := &Server{
		scheduler: scheduler,
	}

	return t
}

type NewClientConfig struct {
	RedisURL    string                // "redis://<user>:<pass>@localhost:6379/<db>"
	RedisClient redis.UniversalClient // if RedisURL is not set, use this client
}

func NewClient(p NewClientConfig) *Client {
	opt, err := redis.ParseURL(p.RedisURL)
	if err != nil {
		panic(err)
	}
	var redisClient redis.UniversalClient
	if p.RedisClient != nil {
		redisClient = p.RedisClient
	} else {
		redisClient = redis.NewClient(opt)
	}

	delayedQueue := store.NewAsynq(redisClient, asynq.Config{
		Concurrency: 0, // Client not need run scheduler, concurrency is unused
	})

	t := &Client{
		trigger: NewTrigger(NewAsyncQueueFactory(delayedQueue)),
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
