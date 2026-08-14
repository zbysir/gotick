package gotick

import (
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	CallId string
	store  NodeStatusStore
	s      AsyncQueue

	// collect 不为 nil 时表示这是一次「预运行」：不真正执行任何任务，
	// 只把遇到的 key 和类型报告出去，用来生成 flow 的静态结构图。
	//
	// 目前没有任何地方给它赋值——生成 DAG 的那部分代码还没有接回来。
	// 接的时候注意两个缺口：Async 和 waitAll 还没有 collect 分支，
	// 预运行会走到真实的存储读取上。
	collect func(typ string, key string) bool

	keyMu    sync.Mutex
	usedKeys map[string]string // key -> 使用它的 API，用于检测重名

	// inCallback 表示正在执行 OnSuccess / OnFail / OnError 回调。
	//
	// 回调里再读一次 Memo 的缓存值是合法用法——比如用 start_at 算总耗时——
	// 那不是重复声明，所以不做重名检查。
	inCallback bool

	// attemptStart 是当前同步任务（Task / Memo / Array）开始执行的时间。
	// 它们只在断点处写状态，没有别的地方能记下开始时间。
	// 同步执行保证了同一时刻只有一个值有效。
	attemptStart time.Time
}

// markKeyUsed 记录一个 key 在本次重放中被哪个 API 使用了。
//
// 同一个 key 被用两次（无论是同一个 API 还是不同 API）会共用同一份任务状态和缓存，
// 结果是它们互相覆盖、任务被跳过或重复执行，而且不会有任何报错。
// 这里直接崩掉：这是确定性的编码错误，第一次重放就会暴露，
// 让它在开发阶段响亮地失败，远好过在生产上悄悄算错。
func (c *Context) markKeyUsed(api, key string) {
	c.keyMu.Lock()
	defer c.keyMu.Unlock()

	// 回调不是 flow 体的一部分，在那里读缓存不算重复声明
	if c.inCallback {
		return
	}

	if c.usedKeys == nil {
		c.usedKeys = map[string]string{}
	}
	if prev, ok := c.usedKeys[key]; ok {
		panic(fmt.Sprintf(
			"gotick: duplicate key %q (used by %s and %s). "+
				"每个 key 在一个 flow 里必须唯一，否则它们会共用同一份状态并互相覆盖。"+
				"在循环里请用 Array/Sequence 生成的 key，不要写死。",
			key, prev, api))
	}
	c.usedKeys[key] = api
}

// storeUnavailable 在存储读写失败时中断本次重放，稍后重来。
//
// 绝不能把「读失败」当成「数据不存在」：那会让已完成的任务被重新执行、
// 让 Memo 的缓存被重新构建出一个不同的值。
// 也绝不能抛裸 error：调度器的 recover 只认识 Breakpoint，
// 其他 panic 会被重新抛出，把整个 asynq worker 打崩。
func storeUnavailable() {
	panic(BreakWait(waitRecheckDelay))
}

func (c *Context) MetaDataAll() MetaData {
	if c.store == nil {
		return nil
	}
	m, err := c.store.GetKVAll()
	if err != nil {
		storeUnavailable()
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
	if err := c.store.SetKV(k, v); err != nil {
		storeUnavailable()
	}
}

func (c *Context) MetaData(k string) (string, bool) {
	v, ok, err := c.store.GetKV(k)
	if err != nil {
		storeUnavailable()
	}

	return v, ok
}

// mustGetNodeStatus 读取任务状态，读失败时中断重放而不是当作「不存在」。
func mustGetNodeStatus(store NodeStatusStore, key string) (TaskStatus, bool) {
	s, exist, err := store.GetNodeStatus(key)
	if err != nil {
		storeUnavailable()
	}
	return s, exist
}

// breakOnTaskError 按重试策略把任务错误转成对应的断点。
func breakOnTaskError(key string, err error, retryCount, maxRetry int) {
	if errors.Is(err, AbortError) {
		panic(BreakAbort(key, err))
	}
	if retryCount >= maxRetry {
		panic(BreakFail(key, err))
	}
	panic(BreakRetry(key, err))
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

// sequenceState 是 Sequence 需要持久化的部分。
//
// SequenceWrap 里的 ctx / max / name 都是运行期的东西，不能序列化：
// 旧实现直接 json.Marshal 整个 SequenceWrap，恢复出来的对象 ctx 是 nil，
// 一调用 Next() 就会空指针。
type sequenceState struct {
	Current int `json:"current"`
	Max     int `json:"max"`
}

func (s *SequenceWrap) Next() bool {
	if s.ctx.collect != nil {
		end := s.ctx.collect("sequence", s.name)
		if end {
			s.Current += 1
			return s.Current <= 0
		}
	}

	// 存储当前的序列号，而不是下一个：
	// 如果在循环体里中断，恢复后要重跑当前这一轮。
	if err := SetToStore(s.ctx.store, s.name, sequenceState{Current: s.Current, Max: s.max}); err != nil {
		storeUnavailable()
	}

	s.Current += 1
	if s.max < 0 {
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

	ctx.markKeyUsed("Sequence", key)

	// 读写必须走同一条路径。旧实现写用 SetKV(name)、读用 GetFromStore(name)，
	// 而 GetFromStore 会再加一层 "__" 前缀，于是状态永远读不回来，
	// 每次重放都从头再数一遍。
	st, ok, err := GetFromStore[sequenceState](ctx.store, key)
	if err != nil {
		storeUnavailable()
	}
	if !ok {
		return SequenceWrap{
			Current: -1, // 让第一次 Next() 把它推到 0
			max:     maxLen,
			name:    key,
			ctx:     ctx,
		}
	}

	return SequenceWrap{
		Current: st.Current,
		max:     st.Max,
		name:    key,
		ctx:     ctx,
	}
}

type FutureT[T interface{}] struct {
	Val T
	k   string
	fun func(retry int) (T, error)
}

func (f *FutureT[T]) Value() T {
	return f.Val
}

func (f *FutureT[T]) exec(retry int) (interface{}, error) {
	t, err := f.fun(retry)
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
	exec(retry int) (interface{}, error)
	key() string
}

func AsyncArray[T interface{}, A interface{}](ctx *Context, key string, arr []ArrayWrap[A], f func(ctx *TaskContext, a A, index int) (T, error)) []Future {
	var fs []Future

	for index, t := range arr {
		// 注意闭包问题
		t := t
		index := index
		fs = append(fs, Async(ctx, t.Key(key), func(ctx *TaskContext) (T, error) {
			return f(ctx, t.Val, index)
		}))
	}

	return fs
}

// Async 声明一个异步任务，需要配合 Wait 才会真正执行。
//
// 这里只读取已经缓存的结果，不查任务状态：任务是否已完成、能不能执行，
// 由 Wait 一次性读全量状态来判断，避免每个 future 各查一次 Redis。
func Async[T interface{}](ctx *Context, key string, f func(ctx *TaskContext) (T, error)) *FutureT[T] {
	ctx.markKeyUsed("Async", key)

	val, _, err := GetFromStore[T](ctx.store, key)
	if err != nil {
		// 读失败绝不能当作「没有缓存」，否则已完成的任务会被重新执行一遍。
		storeUnavailable()
	}

	return &FutureT[T]{
		Val: val,
		k:   key,
		fun: func(retry int) (T, error) {
			return f(&TaskContext{Context: ctx, Retry: retry})
		},
	}
}

type ParallelOption struct {
	// Parallel 本次调度里最多同时执行多少个任务，<=0 表示不限制。
	Parallel int
	// BatchSizePerRunner 本次调度最多认领多少个任务，达到后把剩下的留给其他节点。
	// <=0 表示把当前所有可执行的任务都认领下来。
	BatchSizePerRunner int
}

// defaultAsyncMaxRetry 是 Async 任务的重试上限。
//
// TODO: 让 Async / AsyncArray 也接受 TaskOption，和 Task / Array 用同一套配置。
// 现在用户对 Async 任务设置 WithMaxRetry 是完全无效的。
const defaultAsyncMaxRetry = 5

// waitRecheckDelay 是没有明确重试时间时，下一次回来查看的间隔。
const waitRecheckDelay = time.Second

// Wait 执行并等待所有 future 完成。
// parallel 限制同时执行的任务数，<=0 表示不限制。
func Wait(ctx *Context, parallel int, fs ...Future) {
	waitAll(ctx, ParallelOption{Parallel: parallel}, fs)
}

// WaitFast 和 Wait 的区别只在于可以用 BatchSizePerRunner 限制单个节点一次认领多少任务。
//
// Deprecated: 直接用 Wait，或者用 WaitWithOption 表达同样的意图。
func WaitFast(ctx *Context, opt ParallelOption, fs ...Future) {
	waitAll(ctx, opt, fs)
}

// WaitWithOption 是 Wait 的完整版本。
func WaitWithOption(ctx *Context, opt ParallelOption, fs ...Future) {
	waitAll(ctx, opt, fs)
}

// waitAll 是 Wait / WaitFast 的共同实现。
//
// 每次重放都会：一次性读回所有任务状态 → 判断谁完成了、谁失败了、谁现在可以跑 →
// 原子地认领可跑的任务并执行 → 如果还有没完成的，就中断等待下一次调度。
func waitAll(ctx *Context, opt ParallelOption, fs []Future) {
	if len(fs) == 0 {
		return
	}

	// 一次 HGETALL 拿回全部状态。
	// 旧实现是每个 future 各查一次，N 个任务的 flow 调度 N 次就是 N² 次 Redis 读。
	all, err := ctx.store.GetAllNodeStatus()
	if err != nil {
		// 读不到状态时不能假设任务不存在，否则会重复执行已完成的任务。
		panic(BreakWait(waitRecheckDelay))
	}

	now := time.Now()

	var (
		pending  []Future  // 还没有结论的
		runnable []Future  // 现在就可以执行的
		nextRun  time.Time // 最早一个可以重试的时间
	)

	for _, f := range fs {
		s, exist := all[f.key()]

		if exist {
			switch s.Status {
			case TaskStatusDone:
				continue
			case TaskStatusFail:
				panic(BreakFail(f.key(), fmt.Errorf("task %v failed after %d attempts: %v",
					f.key(), s.RetryCount, strings.Join(s.Errs, "; "))))
			case TaskStatusAbort:
				panic(BreakAbort(f.key(), fmt.Errorf("task %v aborted", f.key())))
			}
		}

		pending = append(pending, f)

		if s.Runnable(now, exist) {
			runnable = append(runnable, f)
			continue
		}
		// 还没到重试时间的，记下来好算等待多久
		if exist && !s.RunAt.IsZero() && s.RunAt.After(now) {
			if nextRun.IsZero() || s.RunAt.Before(nextRun) {
				nextRun = s.RunAt
			}
		}
	}

	if len(pending) == 0 {
		return
	}

	if opt.BatchSizePerRunner > 0 && len(runnable) > opt.BatchSizePerRunner {
		runnable = runnable[:opt.BatchSizePerRunner]
	}

	succeeded := runFutures(ctx, opt.Parallel, runnable)

	// 本次就把剩下的都做完了，不用再调度一轮，直接往下走。
	if succeeded == len(pending) {
		return
	}

	panic(BreakWait(waitDelay(nextRun, time.Now())))
}

// waitDelay 计算下次回来查看的间隔。
//
// 旧实现在重试退避期间用 BreakWait(0) 空转，以任务队列的最小间隔疯狂重放并狂刷 Redis。
func waitDelay(nextRun time.Time, now time.Time) time.Duration {
	if nextRun.IsZero() {
		return waitRecheckDelay
	}
	if d := nextRun.Sub(now); d > 0 {
		return d
	}
	return 0
}

// runFutures 认领并执行给定的任务，返回真正成功完成的数量。
func runFutures(ctx *Context, parallel int, fs []Future) int {
	if len(fs) == 0 {
		return 0
	}

	var (
		wg        sync.WaitGroup
		succeeded int64
		sem       chan struct{}
	)
	if parallel > 0 {
		sem = make(chan struct{}, parallel)
	}

	for _, f := range fs {
		f := f

		// 原子地抢执行权。抢不到说明别的节点正在跑它，或者它刚刚有了结论。
		//
		// 旧实现是「读状态 → 写 running（忽略错误）→ 起 goroutine」，
		// 写失败或两个节点同时读到可执行状态时，同一个任务会在两处同时执行。
		status, claimed, err := claimTask(ctx.store, f.key(), time.Now())
		if err != nil || !claimed {
			continue
		}

		if sem != nil {
			sem <- struct{}{}
		}
		wg.Add(1)
		go func(epoch int64, retry int) {
			defer wg.Done()
			if sem != nil {
				defer func() { <-sem }()
			}
			if runFuture(ctx, f, epoch, retry) {
				atomic.AddInt64(&succeeded, 1)
			}
		}(status.Epoch, status.RetryCount)
	}

	wg.Wait()

	return int(atomic.LoadInt64(&succeeded))
}

// runFuture 执行单个任务并写回结果，期间持续心跳。返回是否成功完成。
func runFuture(ctx *Context, f Future, epoch int64, retry int) bool {
	stopHeartbeat := startHeartbeat(ctx, f.key(), epoch)

	val, execErr := f.exec(retry)

	// 先停心跳再写结果，避免续期把刚写好的终态又改回 running。
	stopHeartbeat()

	if execErr != nil {
		_, _ = settleTask(ctx.store, f.key(), epoch, func(s TaskStatus) TaskStatus {
			if s.RetryCount >= defaultAsyncMaxRetry {
				return s.MakeFail(execErr)
			}
			return s.MakeRetry(execErr)
		})
		return false
	}

	// 结果存不下来就不能标记成功，否则下游会读到零值还以为任务成功了。
	if err := SetToStore(ctx.store, f.key(), val); err != nil {
		_, _ = settleTask(ctx.store, f.key(), epoch, func(s TaskStatus) TaskStatus {
			return s.MakeRetry(err)
		})
		return false
	}

	applied, err := settleTask(ctx.store, f.key(), epoch, func(s TaskStatus) TaskStatus {
		return s.MakeDone()
	})
	if err != nil {
		return false
	}

	// applied 为 false 说明执行权已经被别人抢走了，这次的结果不算数。
	return applied
}

// startHeartbeat 起一个后台 goroutine 为任务续期，返回停止它的函数。
// 停止函数会等待 goroutine 真正退出，所以调用后不会再有心跳写入。
func startHeartbeat(ctx *Context, key string, epoch int64) (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()

		for {
			select {
			case <-done:
				return
			case <-t.C:
				held, err := renewHeartbeat(ctx.store, key, epoch, time.Now())
				if err == nil && !held {
					// 执行权已经不在自己手上，再续期只会破坏别人的状态
					return
				}
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}
}

// Memo 执行 build 并缓存它的结果，重放时直接返回缓存。
//
// 如果 build 返回错误，会按重试策略重试；超过上限则整个 flow 失败。
func Memo[T interface{}](ctx *Context, key string, build func() (T, error), opts ...TaskOption) T {
	if ctx.collect != nil {
		end := ctx.collect("memo", key)
		if end {
			var t T
			return t
		}
	}

	ctx.markKeyUsed("Memo", key)

	v, exist, err := GetFromStore[T](ctx.store, key)
	if err != nil {
		storeUnavailable()
	}
	if exist {
		return v
	}

	s, _ := mustGetNodeStatus(ctx.store, key)
	opt := TaskOptions(opts).build()

	ctx.attemptStart = time.Now()
	t, err := build()
	if err != nil {
		// 旧实现是 `t, _ := build()`：构建失败时把零值当成功结果永久缓存下来，
		// 之后所有重放都命中缓存返回零值，错误被静默吞掉且永不重试。
		// 现在和 Task / Array 用同一套重试语义。
		breakOnTaskError(key, err, s.RetryCount, opt.MaxRetry)
	}

	if err := SetToStore(ctx.store, key, t); err != nil {
		// 存不下来就不能返回，否则下一次重放会重新执行 build，
		// 拿到一个和这次不同的值，破坏下游的确定性。
		panic(BreakRetry(key, err))
	}

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

	ctx.markKeyUsed("Array", key)

	// 只要缓存里有数据就直接用，不管任务状态是什么
	v, exist, err := GetFromStore[[]ArrayWrap[T]](ctx.store, key)
	if err != nil {
		storeUnavailable()
	}
	if exist {
		return v
	}

	s, _ := mustGetNodeStatus(ctx.store, key)
	opt := TaskOptions(opts).build()

	ctx.attemptStart = time.Now()
	t, err := build(newTaskContext(ctx, s))
	if err != nil {
		breakOnTaskError(key, err, s.RetryCount, opt.MaxRetry)
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

// Task 同名的 task 在同一时间只能执行一次
func Task(c *Context, key string, fun TaskFun, opts ...TaskOption) {
	if c.collect != nil {
		if c.collect("task", key) {
			return
		}
	}

	c.markKeyUsed("Task", key)

	s, exist := mustGetNodeStatus(c.store, key)

	switch s.Status {
	case TaskStatusDone:
		return
	case TaskStatusFail:
		// 旧实现在这里直接放行，于是只要发生任何一次重放，
		// 失败的 task 会被静默跳过，后面的 task 照常执行——上游失败、下游照跑。
		panic(BreakFail(key, fmt.Errorf("task %v已经失败: %v", key, strings.Join(s.Errs, "; "))))
	case TaskStatusAbort:
		panic(BreakAbort(key, fmt.Errorf("task %v 已经被终止", key)))
	}

	if !exist || s.Status == TaskStatusRetry {
		// 还没到重试时间就先等着，别把退避时间空转掉
		if exist && s.RunAt.After(time.Now()) {
			panic(BreakWait(time.Until(s.RunAt)))
		}

		opt := TaskOptions(opts).build()

		c.attemptStart = time.Now()
		if err := fun(newTaskContext(c, s)); err != nil {
			breakOnTaskError(key, err, s.RetryCount, opt.MaxRetry)
		}

		// 执行成功也需要断点，因为需要依靠断点来存储状态。
		panic(BreakDone(key))
	}

	// running / sleep 不该出现在 Task 上：这个 key 一定是被别的 API 也用了。
	panic(fmt.Sprintf("gotick: task %q found in unexpected state %q; "+
		"这个 key 很可能同时被 Sleep 或 Async 用了", key, s.Status))
}

func Sleep(c *Context, key string, duration time.Duration) {
	if c.collect != nil {
		if c.collect("sleep", key) {
			return
		}
	}
	c.markKeyUsed("Sleep", key)

	s, exist := mustGetNodeStatus(c.store, key)

	if s.Status == TaskStatusDone {
		return
	}

	if !exist {
		panic(BreakSleep(key, duration))
	}

	if s.Status == TaskStatusSleep {
		if d := time.Until(s.RunAt); d > 0 {
			panic(BreakSleep(key, d))
		}

		_, err := c.store.UpdateNodeStatus(key, func(st TaskStatus, isNew bool) (TaskStatus, bool) {
			if st.Status == TaskStatusDone {
				return st, false
			}
			return st.MakeDone(), true
		})
		if err != nil {
			storeUnavailable()
		}
		return
	}

	panic(fmt.Sprintf("gotick: sleep %q found in unexpected state %q; "+
		"这个 key 很可能同时被 Task 或 Async 用了", key, s.Status))
}

// defaultMaxRetry 是 Task / Memo / Array 默认的重试次数上限（不含第一次执行）。
const defaultMaxRetry = 3

type taskOption struct {
	// MaxRetry 最大重试次数，不包含第一次执行。
	// MaxRetry = 0 表示失败即失败，不重试。
	MaxRetry int
}

type TaskOptions []TaskOption

func (os TaskOptions) build() taskOption {
	option := taskOption{
		MaxRetry: defaultMaxRetry,
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

// WithMaxRetry 设置最大重试次数（不含第一次执行）。0 表示失败即失败。
//
// 注意：目前对 Async / AsyncArray 无效，它们用的是 defaultAsyncMaxRetry。
func WithMaxRetry(maxRetry int) TaskOption {
	return &maxRetryOption{maxRetry: maxRetry}
}

// Task 的状态取值。
const (
	TaskStatusRunning = "running" // 正在某个节点上执行
	TaskStatusDone    = "done"    // 已完成
	TaskStatusFail    = "fail"    // 超过重试次数，整个 flow 失败
	TaskStatusAbort   = "abort"   // 被手动终止
	TaskStatusSleep   = "sleep"   // 等待到 RunAt
	TaskStatusRetry   = "retry"   // 失败后等待重试
)

const (
	// heartbeatInterval 执行中的任务多久向存储续期一次。
	heartbeatInterval = 3 * time.Second

	// runningTimeout 超过这么久没有心跳，就认为执行它的节点已经死了，任务可以被别人接管。
	//
	// 容忍连续丢失 4 次心跳。旧实现是 6s 超时配 3s 心跳，只要有一次超过 3s 的
	// GC 停顿或 Redis 抖动就会误判死亡，让同一个任务在两个节点上同时执行。
	runningTimeout = heartbeatInterval * 5
)

type TaskStatus struct {
	// Key 由存储层的 field 名提供，不重复序列化。
	Key string `json:"-"`

	Status     string    `json:"status"`
	RunAt      time.Time `json:"run_at"` // sleep 到的时间，或下次可以重试的时间
	Errs       []string  `json:"errs"`   // 每次失败都会追加一条
	RetryCount int       `json:"retry_count"`

	// Heartbeat 是 running 状态下最近一次续期的时间。
	//
	// 判断执行节点是否还活着靠的是它，而不是存储层的 TTL：Redis 的 hash field
	// 在 7.4 之前根本没有 TTL，旧实现依赖的那个过期从未生效过，
	// 于是节点崩溃后任务会永远停在 running，flow 永久卡死。
	Heartbeat time.Time `json:"heartbeat,omitempty"`

	// StartedAt / EndedAt 是最近一次尝试的起止时间，用于时间轴和耗时统计。
	StartedAt time.Time `json:"started_at,omitempty"`
	EndedAt   time.Time `json:"ended_at,omitempty"`

	// Epoch 是执行权的代号，每被抢占一次 +1。
	//
	// 心跳和写结果时都要带上自己抢到的 epoch，不匹配就说明任务已经被别人接管，
	// 这次写入必须放弃——否则残留的心跳会把别人写好的 done 覆盖回 running。
	Epoch int64 `json:"epoch,omitempty"`
}

// Alive 报告一个 running 状态的任务是否还有活着的执行者。
func (t TaskStatus) Alive(now time.Time) bool {
	return t.Status == TaskStatusRunning && !now.After(t.Heartbeat.Add(runningTimeout))
}

// Settled 报告这个任务是否已经有了最终结论，不该再被执行。
func (t TaskStatus) Settled() bool {
	switch t.Status {
	case TaskStatusDone, TaskStatusFail, TaskStatusAbort:
		return true
	}
	return false
}

// Runnable 报告现在是否可以（重新）执行这个任务。exist 为 false 表示这个任务还没有任何状态。
func (t TaskStatus) Runnable(now time.Time, exist bool) bool {
	if !exist {
		return true
	}
	if t.Settled() {
		return false
	}
	if t.Status == TaskStatusRunning {
		// 只有执行者失联了才能接管
		return !t.Alive(now)
	}
	// retry / sleep 要等到 RunAt
	return t.RunAt.IsZero() || !t.RunAt.After(now)
}

func (t TaskStatus) MakeDone() TaskStatus {
	t.Status = TaskStatusDone
	t.Heartbeat = time.Time{}
	t.EndedAt = time.Now()
	return t
}

// WithStartedAt 记录这次尝试的开始时间。
//
// Task / Memo / Array 是同步执行的，只在断点处写状态，
// 所以开始时间要由调用方带过来，否则时间轴上只有终点没有起点。
func (t TaskStatus) WithStartedAt(at time.Time) TaskStatus {
	if !at.IsZero() {
		t.StartedAt = at
	}
	return t
}

// Elapsed 返回最近一次尝试的耗时。
func (t TaskStatus) Elapsed() time.Duration {
	if t.StartedAt.IsZero() || t.EndedAt.IsZero() || t.EndedAt.Before(t.StartedAt) {
		return 0
	}
	return t.EndedAt.Sub(t.StartedAt)
}

func (t TaskStatus) MakeFail(err error) TaskStatus {
	t.Status = TaskStatusFail
	t.Heartbeat = time.Time{}
	t.EndedAt = time.Now()
	t.RetryCount += 1
	if err != nil {
		t.Errs = append(t.Errs, err.Error())
	}
	return t
}

func (t TaskStatus) MakeAbort() TaskStatus {
	t.Status = TaskStatusAbort
	t.Heartbeat = time.Time{}
	t.EndedAt = time.Now()
	return t
}

// MakeRunning 续期一个已经属于自己的执行中任务。抢占执行权请用 MakeClaimed。
func (t TaskStatus) MakeRunning(now time.Time) TaskStatus {
	t.Status = TaskStatusRunning
	t.Heartbeat = now
	return t
}

// MakeClaimed 抢占执行权：进入 running 并推进 epoch，让之前那次执行的心跳失效。
func (t TaskStatus) MakeClaimed(now time.Time) TaskStatus {
	t.Status = TaskStatusRunning
	t.Heartbeat = now
	t.StartedAt = now
	t.EndedAt = time.Time{}
	t.Epoch += 1
	return t
}

func (t TaskStatus) MakeSleep(runAt time.Time) TaskStatus {
	// 记录进入 sleep 的时刻，这样界面上才能显示「睡了多久 / 还剩多久」，
	// 而不是只有一个孤零零的唤醒时间戳。
	//
	// 重放会再次走到这里（Sleep 未到期时会用剩余时长重新 BreakSleep），
	// 所以已经在 sleep 中就不能重置起点，否则进度会一直往回跳。
	if t.Status != TaskStatusSleep || t.StartedAt.IsZero() {
		t.StartedAt = time.Now()
	}
	t.Status = TaskStatusSleep
	t.RunAt = runAt
	t.EndedAt = time.Time{}
	return t
}

func (t TaskStatus) MakeRetry(err error) TaskStatus {
	t.Status = TaskStatusRetry
	t.Heartbeat = time.Time{}
	t.EndedAt = time.Now()
	t.RetryCount += 1
	if err != nil {
		t.Errs = append(t.Errs, err.Error())
	}
	t.RunAt = time.Now().Add(time.Second * time.Duration(t.RetryCount))
	return t
}

type NodeStatusStore interface {
	GetNodeStatus(key string) (TaskStatus, bool, error) // 获取单个 task 的运行状态
	GetAllNodeStatus() (map[string]TaskStatus, error)   // 获取这次调用中所有 task 的状态，用于调度、inspect 和 UI
	SetNodeStatus(key string, value TaskStatus) error

	// UpdateNodeStatus 原子地更新一个 task 的状态。
	//
	// fu 返回 commit=false 表示放弃本次更新。如果期间有别的节点改动了这个 task，
	// fu 会带着最新状态被重新调用，所以它必须是纯函数、可以被安全地重复执行。
	UpdateNodeStatus(key string, fu func(status TaskStatus, isNew bool) (next TaskStatus, commit bool)) (TaskStatus, error)

	GetKVAll() (map[string]string, error)
	SetKV(k string, v string) error
	GetKV(k string) (string, bool, error)
	Clear() error // 删除这次调用的所有数据

	// AcquireLease 尝试获取这次调用的重放租约，保证同一时刻只有一个节点在重放它。
	// token 是本次持有的凭证，只有它能续期和释放。
	AcquireLease(token string, ttl time.Duration) (bool, error)
	RenewLease(token string, ttl time.Duration) (bool, error)
	ReleaseLease(token string) error

	// ExpireAll 给这次调用的所有数据设置过期时间，用于回收已经结束的 flow。
	ExpireAll(ttl time.Duration) error
}

// claimTask 原子地抢占一个 task 的执行权。
//
// 抢到时返回带着新 epoch 的状态。抢不到说明它已经有结论了，
// 或者正在别的节点上执行且心跳还活着。
func claimTask(store NodeStatusStore, key string, now time.Time) (TaskStatus, bool, error) {
	claimed := false
	s, err := store.UpdateNodeStatus(key, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
		if !status.Runnable(now, !isNew) {
			claimed = false
			return status, false
		}
		claimed = true
		return status.MakeClaimed(now), true
	})
	if err != nil {
		return s, false, err
	}
	return s, claimed, nil
}

// renewHeartbeat 为执行中的 task 续期。
//
// 只有当这个 task 仍然由 epoch 这次执行持有时才会写入，
// 否则一个已经被判死、任务已被别人完成的节点，其残留心跳会把 done 打回 running。
func renewHeartbeat(store NodeStatusStore, key string, epoch int64, now time.Time) (held bool, err error) {
	_, err = store.UpdateNodeStatus(key, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
		if isNew || status.Epoch != epoch || status.Status != TaskStatusRunning {
			held = false
			return status, false
		}
		held = true
		return status.MakeRunning(now), true
	})
	return held, err
}

// settleTask 写入 task 的最终结果，同样要求执行权还在自己手上。
func settleTask(store NodeStatusStore, key string, epoch int64, apply func(TaskStatus) TaskStatus) (applied bool, err error) {
	_, err = store.UpdateNodeStatus(key, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
		if isNew || status.Epoch != epoch {
			applied = false
			return status, false
		}
		applied = true
		return apply(status), true
	})
	return applied, err
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
}

// Client 用于触发调度，和 Server 的区别是 Client 不会启动调度器。
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

	// retainCompleted 已结束的 flow 数据保留多久，<=0 表示永久保留。
	retainCompleted time.Duration

	// runIndex 记录 flow 与运行实例的索引，供 inspect 和 UI 使用。
	// 默认是 no-op，不产生任何额外写入。
	runIndex RunIndex
}

func NewScheduler(asyncScheduler AsyncQueueFactory, statusStore StoreFactory) *Scheduler {
	return &Scheduler{
		asyncScheduler:  asyncScheduler,
		statusFactory:   statusStore,
		trigger:         NewTrigger(asyncScheduler),
		retainCompleted: defaultRetainCompleted,
		runIndex:        noopRunIndex{},
	}
}
func (s *Scheduler) Start(ctx context.Context) error {
	return s.asyncScheduler.Start(ctx)
}

const (
	// replayLeaseTTL 一次重放持有租约的时长。
	// 进程被 kill 时租约靠它自动释放，别的节点才能接手。
	replayLeaseTTL = 30 * time.Second
	// replayLeaseRenew 续期间隔，必须显著小于 replayLeaseTTL。
	replayLeaseRenew = 10 * time.Second
	// defaultRetainCompleted 已结束的 flow 数据默认保留多久。
	defaultRetainCompleted = 7 * 24 * time.Hour
)

// ErrReplayInFlight 表示同一个 callId 正在别的节点上重放。
//
// 它会被返回给消息队列，让这个事件稍后重投，而不是被丢掉。
var ErrReplayInFlight = errors.New("gotick: another worker is replaying this call")

// nextEvent 描述本次重放结束后要发出的下一个调度事件。
type nextEvent struct {
	delay time.Duration
}

// startLeaseRenewal 起一个后台 goroutine 持续续期重放租约，返回停止它的函数。
func startLeaseRenewal(store NodeStatusStore, token string) (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		t := time.NewTicker(replayLeaseRenew)
		defer t.Stop()

		for {
			select {
			case <-done:
				return
			case <-t.C:
				held, err := store.RenewLease(token, replayLeaseTTL)
				if err == nil && !held {
					// 租约已经不在自己手上了，继续续期只会把别人的租约续走
					return
				}
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}
}

func (s *Scheduler) register(f *Flow) {
	aw := s.asyncScheduler.New(f.Id)

	if err := s.runIndex.RegisterFlow(f.Id); err != nil {
		log.Printf("[gotick] register flow %s in the run index failed: %v", f.Id, err)
	}

	aw.Subscribe(func(ctx context.Context, event Event) error {
		callId := event.CallId
		ctx = WithCallId(ctx, callId)

		statusStore := s.statusFactory.New(callId)

		// 同一个 callId 同一时刻只允许一个节点重放。
		//
		// 消息队列是 at-least-once 的：worker 卡住、租约到期、任务被重投，
		// 都会让两个节点同时重放同一个 callId。Task / Memo / Array 只看状态、
		// 没有抢占保护，于是它们会被并发执行两次。
		token := randomStr()
		acquired, err := statusStore.AcquireLease(token, replayLeaseTTL)
		if err != nil {
			return err
		}
		if !acquired {
			return ErrReplayInFlight
		}

		stopRenewal := startLeaseRenewal(statusStore, token)
		leaseHeld := true
		releaseLease := func() {
			if !leaseHeld {
				return
			}
			leaseHeld = false
			stopRenewal()
			if err := statusStore.ReleaseLease(token); err != nil {
				log.Printf("[gotick] release replay lease of %s failed: %v", callId, err)
			}
		}
		defer releaseLease()

		if event.InitMetaData != nil {
			for k, v := range event.InitMetaData {
				if err := statusStore.SetKV(k, v); err != nil {
					return err
				}
			}
		}

		// 索引写失败不该让流程停下来——可观测性坏了比业务停摆好。
		if err := s.runIndex.BeginRun(f.Id, callId, time.Now()); err != nil {
			log.Printf("[gotick] index run %s failed: %v", callId, err)
		}

		var (
			next       *nextEvent // 本次重放之后要发的事件
			terminal   bool       // flow 是否已经有了最终结论
			runStatus  = RunStatusDone
			failedTask string
			failedErr  string
		)

		err = func() (err error) {
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
				case *breakWait:
					next = &nextEvent{delay: time.Until(breakpoint.RunAt)}

				case *breakRetry:
					var newStatus TaskStatus
					newStatus, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
						return status.WithStartedAt(ctx.attemptStart).MakeRetry(breakpoint.Err), true
					})
					if err != nil {
						// 重试次数落不了盘就不能继续调度：RetryCount 永远涨不上去，
						// 就永远到不了 MaxRetry，会无限重试下去。
						// 把错误交给消息队列，让它重投这个事件。
						return
					}

					if f.onError != nil {
						ctx.inCallback = true
						if cbErr := f.onError(ctx, newStatus); cbErr != nil {
							// TODO 支持 onError 回调失败后的重试
							log.Printf("[gotick] onError callback failed: %v", cbErr)
						}
					}

					// 退避时间由 MakeRetry 写在 RunAt 里。
					// TODO 支持自定义退避算法
					next = &nextEvent{delay: time.Until(newStatus.RunAt)}

				case *breakAbort:
					var newStatus TaskStatus
					newStatus, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
						if status.Status == TaskStatusAbort {
							return status, false
						}
						return status.WithStartedAt(ctx.attemptStart).MakeAbort(), true
					})
					if err != nil {
						return
					}
					terminal = true
					runStatus = RunStatusAborted
					failedTask = breakpoint.Task
					if breakpoint.Error != nil {
						failedErr = breakpoint.Error.Error()
					}
					if f.onFail != nil {
						ctx.inCallback = true
						if cbErr := f.onFail(ctx, newStatus); cbErr != nil {
							log.Printf("[gotick] onFail callback failed: %v", cbErr)
						}
					}

				case *breakFail:
					var newStatus TaskStatus
					newStatus, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
						// Wait 里的任务可能已经把自己标成 fail 了，
						// 再标一次会重复累加 RetryCount 和 Errs。
						if status.Status == TaskStatusFail {
							return status, false
						}
						return status.WithStartedAt(ctx.attemptStart).MakeFail(breakpoint.Err), true
					})
					if err != nil {
						return
					}
					terminal = true
					runStatus = RunStatusFailed
					failedTask = breakpoint.Task
					if breakpoint.Err != nil {
						failedErr = breakpoint.Err.Error()
					}
					if f.onFail != nil {
						ctx.inCallback = true
						if cbErr := f.onFail(ctx, newStatus); cbErr != nil {
							log.Printf("[gotick] onFail callback failed: %v", cbErr)
						}
					}

				case *breakSleep:
					// TODO 考虑先入队，然后更改状态
					_, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
						return status.MakeSleep(breakpoint.RunAt), true
					})
					if err != nil {
						return
					}
					next = &nextEvent{delay: time.Until(breakpoint.RunAt)}

				case *breakDone:
					_, err = statusStore.UpdateNodeStatus(breakpoint.Task, func(status TaskStatus, isNew bool) (TaskStatus, bool) {
						return status.WithStartedAt(ctx.attemptStart).MakeDone(), true
					})
					if err != nil {
						return
					}
					next = &nextEvent{delay: 0}
				}
			}()

			// flow 函数不应该返回错误：错误会直接交给消息队列，脱离框架的控制。
			// 所有错误都应该从 gotick.Task 里返回。
			f.fun(ctx)

			// 没有中断，说明整个 flow 跑完了
			terminal = true
			if f.onSuccess != nil {
				ctx.inCallback = true
				if err := f.onSuccess(ctx); err != nil {
					return err
				}
			}

			return nil
		}()
		if err != nil {
			// 返回错误会进入消息队列的重试机制。
			// 通常是入队失败这种框架已经无法处理的致命错误。
			return err
		}

		// 先释放租约再发下一个事件。
		// 反过来的话，下一个事件的处理方会抢不到租约、被迫走消息队列的退避重试，
		// 给流程的每一步都加上秒级延迟。
		releaseLease()

		if terminal {
			if err := s.runIndex.FinishRun(callId, runStatus, failedTask, failedErr, time.Now()); err != nil {
				log.Printf("[gotick] index finished run %s failed: %v", callId, err)
			}
		}

		if terminal && s.retainCompleted > 0 {
			// 已经结束的 flow 不再需要长期保留。
			// 用过期而不是直接删除：迟到的重复事件仍然能看到「已完成」，
			// 否则它会把整个 flow 从头再跑一遍。
			if err := statusStore.ExpireAll(s.retainCompleted); err != nil {
				log.Printf("[gotick] set retention on finished call %s failed: %v", callId, err)
			}
		}

		if next != nil {
			if err := aw.Publish(ctx, Event{
				CallId:   callId,
				Critical: true,
			}, next.delay); err != nil {
				return err
			}
		}

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

// RunIndex 返回运行索引，UI 和运维工具用它查询有哪些实例在跑。
func (t *Server) RunIndex() RunIndex {
	return t.scheduler.runIndex
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

func (n *KvNodeStatusStore) leaseKey() string {
	return n.key + "_lease"
}

func (n *KvNodeStatusStore) AcquireLease(token string, ttl time.Duration) (bool, error) {
	return n.store.SetNX(context.Background(), n.leaseKey(), token, ttl)
}

func (n *KvNodeStatusStore) RenewLease(token string, ttl time.Duration) (bool, error) {
	return n.store.ExpireIf(context.Background(), n.leaseKey(), token, ttl)
}

func (n *KvNodeStatusStore) ReleaseLease(token string) error {
	_, err := n.store.DeleteIf(context.Background(), n.leaseKey(), token)
	return err
}

func (n *KvNodeStatusStore) ExpireAll(ttl time.Duration) error {
	ctx := context.Background()
	if err := n.store.Expire(ctx, n.metaKey(), ttl); err != nil {
		return err
	}
	return n.store.Expire(ctx, n.statusKey(), ttl)
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

// casMaxAttempts 单次 UpdateNodeStatus 最多重试多少轮 CAS。
// 同一个 task 的并发写入方最多是「若干个重放 + 一个心跳」，正常情况下一两轮就成功。
const casMaxAttempts = 20

func (n *KvNodeStatusStore) UpdateNodeStatus(key string, fu func(status TaskStatus, isNew bool) (TaskStatus, bool)) (TaskStatus, error) {
	ctx := context.Background()

	for attempt := 0; attempt < casMaxAttempts; attempt++ {
		raw, exist, err := n.getRawNodeStatus(key)
		if err != nil {
			return TaskStatus{Key: key}, err
		}

		old := TaskStatus{Key: key}
		if exist {
			if err := json.Unmarshal([]byte(raw), &old); err != nil {
				return old, fmt.Errorf("decode status of task %q: %w", key, err)
			}
			old.Key = key
		}

		next, commit := fu(old, !exist)
		if !commit {
			return old, nil
		}

		// expect 为 nil 表示「要求这个 field 当前不存在」，
		// 这样两个节点同时首次写入同一个 task 时只有一个能赢。
		var expect *string
		if exist {
			expect = &raw
		}

		ok, err := n.store.HSetCAS(ctx, n.statusKey(), key, expect, next)
		if err != nil {
			return old, err
		}
		if ok {
			next.Key = key
			return next, nil
		}
		// 有人抢先改了，带着最新状态重来一轮
	}

	return TaskStatus{Key: key}, fmt.Errorf("update status of task %q: gave up after %d contended attempts", key, casMaxAttempts)
}

// getRawNodeStatus 取出存储里原始的 JSON 字节，CAS 需要拿它做比较。
func (n *KvNodeStatusStore) getRawNodeStatus(key string) (string, bool, error) {
	var raw json.RawMessage
	exist, err := n.store.HGet(context.Background(), n.statusKey(), key, &raw)
	if err != nil {
		return "", false, err
	}
	if !exist {
		return "", false, nil
	}
	return string(raw), true, nil
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

func (n *KvNodeStatusStore) GetAllNodeStatus() (map[string]TaskStatus, error) {
	raw, exist, err := n.store.HGetAll(context.Background(), n.statusKey())
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}

	all := make(map[string]TaskStatus, len(raw))
	for k, v := range raw {
		status := TaskStatus{Key: k}
		if err := json.Unmarshal([]byte(v), &status); err != nil {
			return nil, fmt.Errorf("decode status of task %q: %w", k, err)
		}
		// Key 不在序列化字段里，反序列化后要补回来
		status.Key = k
		all[k] = status
	}

	return all, nil
}

func (n *KvNodeStatusStore) SetNodeStatus(key string, value TaskStatus) error {
	return n.store.HSet(context.Background(), n.statusKey(), key, value)
}

func (n *KvNodeStatusStore) GetKVAll() (map[string]string, error) {
	raw, exist, err := n.store.HGetAll(context.Background(), n.metaKey())
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}

	// HGetAll 返回的是存进去时 json.Marshal 的结果，而 GetKV 会 Unmarshal 一次。
	// 不在这里解码的话，同一个 key 用 GetKV 读到 `bysir`、用 GetKVAll 读到 `"bysir"`。
	//
	// 这个 hash 里混着两类数据：用户的 metadata（一定是 JSON 字符串）和
	// Memo / Array / Async 缓存的结果（任意 JSON）。所以只在能解成字符串时解码，
	// 其余保持原始 JSON —— 这也正是 inspect 这类工具想看到的形式。
	// TODO: 把两类数据拆到不同的 hash，这个特判就可以去掉了。
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		if err := json.Unmarshal([]byte(v), &s); err == nil {
			out[k] = s
		} else {
			out[k] = v
		}
	}

	return out, nil
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
	return n.store.HSet(context.Background(), n.metaKey(), k, v)
}

type Config struct {
	RedisURL          string                // "redis://<user>:<pass>@localhost:6379/<db>"
	RedisClient       redis.UniversalClient // if RedisURL is not set, use this client
	Concurrency       int                   // default 10
	TaskCheckInterval time.Duration         // default 100ms
}

func newScheduler(delayedQueue store.DelayedQueue, kvStore store.KVStore) *Scheduler {
	s := NewScheduler(NewAsyncQueueFactory(delayedQueue), NewKvStoreProduct(kvStore))
	// 索引和状态存在同一个 KVStore 上，所以只要有存储就能有可观测性，
	// 不需要额外配置任何东西。
	s.runIndex = NewKvRunIndex(kvStore, s.retainCompleted)
	return s
}

func newSchedulerFromConfig(p Config) (*Scheduler, error) {
	var (
		redisClient redis.UniversalClient
		ownsClient  bool
	)
	if p.RedisClient != nil {
		redisClient = p.RedisClient
	} else {
		opt, err := redis.ParseURL(p.RedisURL)
		if err != nil {
			return nil, err
		}

		redisClient = redis.NewClient(opt)
		ownsClient = true
	}

	delayedQueue := store.NewAsynq(redisClient, asynq.Config{
		Concurrency:       p.Concurrency,
		TaskCheckInterval: p.TaskCheckInterval,
	}, ownsClient)
	kvStore := store.NewRedisStore(redisClient)

	return newScheduler(delayedQueue, kvStore), nil
}

func NewServerFromConfig(p Config) (*Server, error) {
	scheduler, err := newSchedulerFromConfig(p)
	if err != nil {
		return nil, err
	}

	return &Server{scheduler: scheduler}, nil
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
	RedisClient redis.UniversalClient // RedisURL 未设置时使用这个 client
}

// NewClientParams 用于直接注入队列实现，和 NewServerParams 对称。
//
// 测试、或者在没有 Redis 的环境里构造 client 时用它，
// 不必为了拿到一个 Client 而先有一个能连上的 Redis。
type NewClientParams struct {
	DelayedQueue store.DelayedQueue
}

// NewClientFrom 用给定的队列实现创建 Client。
func NewClientFrom(p NewClientParams) *Client {
	return &Client{
		trigger: NewTrigger(NewAsyncQueueFactory(p.DelayedQueue)),
	}
}

// NewClient 创建一个只能触发 flow、不启动调度器的客户端。
func NewClient(p NewClientConfig) (*Client, error) {
	// 先看 RedisClient 再解析 URL。
	// 旧实现无条件先 ParseURL 并在失败时 panic，
	// 于是文档里写明支持的「只传 RedisClient」这条路径 100% 崩溃。
	var (
		redisClient redis.UniversalClient
		ownsClient  bool
	)
	if p.RedisClient != nil {
		redisClient = p.RedisClient
	} else {
		opt, err := redis.ParseURL(p.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("parse RedisURL: %w", err)
		}
		redisClient = redis.NewClient(opt)
		ownsClient = true
	}

	delayedQueue := store.NewAsynq(redisClient, asynq.Config{
		Concurrency: 0, // Client 不跑调度器，并发度用不上
	}, ownsClient)

	return &Client{
		trigger: NewTrigger(NewAsyncQueueFactory(delayedQueue)),
	}, nil
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
