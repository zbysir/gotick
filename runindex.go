package gotick

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zbysir/gotick/store"
)

// 一次 flow 调用的整体状态。
const (
	RunStatusRunning  = "running"
	RunStatusDone     = "done"
	RunStatusFailed   = "failed"
	RunStatusAborted  = "aborted"
	RunStatusCanceled = "canceled"
)

// RunInfo 是一次 flow 调用的概览，用于列表展示。
//
// 每个 task 的细节在 {callId}_status 里，这里只放「一眼能看懂」的东西。
type RunInfo struct {
	CallId string `json:"call_id"`
	FlowId string `json:"flow_id"`

	// Key 是 Trigger 时用 WithKey 绑定的业务 key（订单号、用户 ID 之类）。
	//
	// 冗余在这里是有意的：它同时也在这次调用的 meta 里（RunKeyField），但那一份
	// 在流程结束时会被解绑，而且要多一次 HGETALL 才读得到。列表页每行都要显示它、
	// 还要能按它筛，所以必须能从索引记录里直接拿到——否则一页 50 行就是 50 次额外往返。
	Key string `json:"key,omitempty"`

	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`

	// Replays 这次调用被重放了多少次。
	// 它是判断调度是否失控的关键指标——一个只有 3 个 task 的 flow
	// 如果重放了 400 次，说明某处在空转。
	Replays int `json:"replays"`

	// FailedTask / Error 只在失败时有值，让列表页不用点进去就知道死在哪。
	FailedTask string `json:"failed_task,omitempty"`
	Error      string `json:"error,omitempty"`

	// Indexed 表示这条记录的二级索引条目（按状态、按业务 key）已经写好了。
	//
	// 二级索引是后加的，升级那一刻已经存在的记录一条都不在里面。没有这个标记就
	// 分不清「补过了」和「还没补」，于是要么存量记录永远筛不出来，要么每次列表
	// 都为每一行重复写一遍索引。
	//
	// 注意这里没有 metadata 的快照：那份数据一直在 <callId>_meta 里，界面读它就行。
	// 之前冗余过一份，理由是「一页 50 行就是 50 次 HGETALL、还会拖回 Memo/Async
	// 的缓存结果」——实测真实负载下那个 hash 只有几百字节，为此付快照、截断、
	// 补录三笔复杂度不值得，而且快照还会和详情页显示得不一样。
	Indexed bool `json:"indexed,omitempty"`

	// Marks 记录每次重放的开始时刻和它以什么方式结束。
	//
	// 只有计数是不够的：一次「本该立刻继续、却等了 5 秒才被调度」的重放，
	// 在计数里和正常重放长得一模一样，证据全在时间间隔里。
	Marks []ReplayMark `json:"marks,omitempty"`
}

// ReplayMark 是一次重放的记录。
type ReplayMark struct {
	At time.Time `json:"at"`
	// Kind 这次重放以什么方式结束：done / sleep / retry / wait / fail / abort / finish。
	Kind string `json:"kind,omitempty"`
	Task string `json:"task,omitempty"`
}

// maxReplayMarks 最多保留多少条重放记录。
// 超过之后丢掉最早的——排查问题时关心的总是最近发生了什么。
const maxReplayMarks = 200

// Duration 返回这次调用的耗时；还在运行中的返回到现在为止的时长。
func (r RunInfo) Duration() time.Duration {
	if r.StartedAt.IsZero() {
		return 0
	}
	if r.EndedAt.IsZero() {
		return time.Since(r.StartedAt)
	}
	return r.EndedAt.Sub(r.StartedAt)
}

// Finished 报告这次调用是否已经结束。
func (r RunInfo) Finished() bool {
	return r.Status == RunStatusDone || r.Status == RunStatusFailed ||
		r.Status == RunStatusAborted || r.Status == RunStatusCanceled
}

// RunIndex 维护 flow 和运行实例的全局索引。
//
// 没有它就回答不了「现在有哪些实例在跑」——这是运维盲区，
// 也是做 UI 时第一个会撞上的墙：状态数据都在，但没有入口找到它们。
type RunIndex interface {
	// RegisterFlow 记录一个已注册的 flow，让 UI 知道有哪些 flow 存在。
	RegisterFlow(flowId string) error
	ListFlows() ([]string, error)

	// BeginRun 在每次重放开始时调用。首次会创建记录，之后只累加重放次数。
	BeginRun(flowId, callId string, at time.Time) error
	// FinishRun 在 flow 有了最终结论时调用。
	FinishRun(callId, status, failedTask, errMsg string, at time.Time) error

	// RecordReplay 记下一次重放的开始时刻和结束方式。
	RecordReplay(callId string, mark ReplayMark) error

	GetRun(callId string) (RunInfo, bool, error)
	// ListRuns 按开始时间倒序返回实例。flowId 为空表示不限 flow。
	ListRuns(flowId string, offset, limit int64) ([]RunInfo, error)
	CountRuns(flowId string) (int64, error)
}

// StatusFilterIndex 是 RunIndex 的一个可选能力：在服务端按状态筛选。
//
// 没有并进 RunIndex，理由和 store.ZRemmer 一样——那是个公开接口，加方法会让
// 自己实现过它的调用方升级即编译不过。调用方用类型断言问一次，拿不到就退回
// 「只在当前这一页里过滤」，而不是假装筛过了。
type StatusFilterIndex interface {
	// StatusFilterSupported 报告底层设施到底支不支持。实现了这个接口不等于能用：
	// KvRunIndex 永远实现它，但在一个删不了 ZSET 成员的 store 上会返回 false。
	StatusFilterSupported() bool
	ListRunsByStatus(flowId, status string, offset, limit int64) ([]RunInfo, error)
	CountRunsByStatus(flowId, status string) (int64, error)
}

// KeyFilterIndex 是 RunIndex 的另一个可选能力：按业务 key 筛选。
// 不并进 RunIndex 的理由同 StatusFilterIndex。
//
// 这里没有对应的 Supported 方法：按 key 的索引只写不删，不依赖任何可选的
// store 能力。它唯一的边界是时间——索引上线之前跑的调用没有被记进来，
// 那些记录会随保留期自己消失。
type KeyFilterIndex interface {
	ListRunsByKey(flowId, key string, offset, limit int64) ([]RunInfo, error)
	CountRunsByKey(flowId, key string) (int64, error)
}

const (
	flowsKey   = "gotick:flows"
	allRunsKey = "gotick:runs"
)

func flowRunsKey(flowId string) string { return "gotick:flow:" + flowId + ":runs" }
func runInfoKey(callId string) string  { return "gotick:run:" + callId }

// 按状态的二级索引。和主索引同构（成员是 callId，score 是开始时间），
// 所以「按状态筛」和「不筛」走的是完全一样的分页和排序逻辑。
func statusRunsKey(status string) string { return "gotick:runs:status:" + status }
func flowStatusRunsKey(flowId, status string) string {
	return "gotick:flow:" + flowId + ":runs:status:" + status
}

// 按业务 key 的二级索引。形状和按状态那套完全一样（成员是 callId，score 是开始时间），
// 所以「按 key 筛」也是一次 ZREVRANGE 加一个精确总数，和不筛时走同一套分页。
//
// 和状态索引有一个关键区别：一次调用的 key 从头到尾不会变，所以这里只写不删，
// 不需要 store.ZRemmer，也不会有滞留成员。
func keyRunsKey(key string) string { return "gotick:runs:key:" + key }
func flowKeyRunsKey(flowId, key string) string {
	return "gotick:flow:" + flowId + ":runs:key:" + key
}

// indexedStatuses 是会进二级索引的状态。
//
// "expired" 不在里面：那是 ListRuns 在记录已经过了 TTL、但 ZSET 成员还在时
// 凭空补出来的占位状态，从来没有被写入过，也就没有索引可言。
var indexedStatuses = []string{
	RunStatusRunning, RunStatusDone, RunStatusFailed, RunStatusAborted, RunStatusCanceled,
}

// KvRunIndex 把索引建在 KVStore 上，因此 Redis 和内存实现都能用。
type KvRunIndex struct {
	store store.KVStore

	// zrem 是可选能力，nil 表示这个 store 删不了有序集合的指定成员。
	//
	// 有它才维护按状态的二级索引：一次调用结束时要从 running 集合搬到终态集合，
	// 搬不走的话 running 集合会把所有跑过的调用都留在里面，
	// 「只看在跑的」于是变成「看全部」——一个会给出错误答案的索引比没有索引更糟。
	zrem store.ZRemmer

	// retain 索引里的记录保留多久，和 flow 数据的保留期保持一致。
	retain time.Duration

	// trimMu 保护 lastTrim：索引会被多个 goroutine 并发写。
	trimMu   sync.Mutex
	lastTrim time.Time
}

// trimInterval 两次裁剪之间至少隔这么久。
//
// 裁剪挂在 FinishRun 上，没有单独起后台 goroutine：索引本来就是被动写入的，
// 多一个后台循环就多一份生命周期要管（还要考虑优雅退出）。代价是完全不跑 flow
// 的进程不会裁剪——但那种进程也不会让索引变长。
const trimInterval = 10 * time.Minute

var _ RunIndex = (*KvRunIndex)(nil)

func NewKvRunIndex(s store.KVStore, retain time.Duration) *KvRunIndex {
	k := &KvRunIndex{store: s, retain: retain}
	if store.ZRemSupported(s) {
		k.zrem, _ = s.(store.ZRemmer)
	}
	return k
}

// StatusFilterSupported 报告这个索引能不能在服务端按状态筛选。
//
// 导出是因为界面要据此决定说什么：能筛的时候「只看失败的」是在全部记录里筛，
// 不能筛的时候它只在当前这一页里生效——两者差别很大，不该让人自己猜。
func (k *KvRunIndex) StatusFilterSupported() bool { return k.zrem != nil }

func (k *KvRunIndex) RegisterFlow(flowId string) error {
	return k.store.ZAdd(context.Background(), flowsKey, flowId, float64(time.Now().UnixMilli()))
}

func (k *KvRunIndex) ListFlows() ([]string, error) {
	members, err := k.store.ZRevRange(context.Background(), flowsKey, 0, 1000)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(members))
	for _, m := range members {
		out = append(out, m.Member)
	}
	return out, nil
}

func (k *KvRunIndex) BeginRun(flowId, callId string, at time.Time) error {
	ctx := context.Background()

	info, exist, err := k.GetRun(callId)
	if err != nil {
		return err
	}

	if !exist {
		info = RunInfo{
			CallId:    callId,
			FlowId:    flowId,
			Status:    RunStatusRunning,
			StartedAt: at,
		}

		// 只在这里拍快照——这个分支每个 callId 只走一次，不是每次重放。
		// 时序是成立的：触发时传的 InitMetaData 和 WithKey 绑的 key
		// 都在调度器调用 BeginRun 之前就已经写进 meta 了。
		// 只取业务 key：按 key 的索引需要它，而 metadata 界面自己去读。
		key, keyOK := k.readRunKey(callId)
		info.Key = key

		score := float64(at.UnixMilli())
		if err := k.store.ZAdd(ctx, allRunsKey, callId, score); err != nil {
			return err
		}
		if err := k.store.ZAdd(ctx, flowRunsKey(flowId), callId, score); err != nil {
			return err
		}
		if err := k.addToStatusIndex(flowId, callId, RunStatusRunning, score); err != nil {
			return err
		}
		if err := k.addToKeyIndex(flowId, callId, info.Key, score, at); err != nil {
			return err
		}
		// 读 key 失败时不打标记，第一次进列表会重试；否则这次调用会永久缺在
		// 按 key 的索引里。
		info.Indexed = keyOK
	}

	// 读-改-写在这里是安全的：同一个 callId 的重放已经被租约串行化了。
	info.Replays++
	info.UpdatedAt = at

	return k.saveRun(info)
}

// readRunKey 读出这次调用绑定的业务 key。
//
// 只读 key，不读 metadata：metadata 一直在 <callId>_meta 里，界面直接读那份就行。
// key 不一样——按 key 的二级索引要用它当键名，那是写入时就必须定下来的东西。
//
// ok 为 false 表示读失败（不是「没有 key」）。调用方靠它决定要不要留着重试：
// 读失败时如果把记录标成已建索引，这次调用就永久缺在按 key 的索引里了。
func (k *KvRunIndex) readRunKey(callId string) (key string, ok bool) {
	v, exist, err := NewKvStoreProduct(k.store).New(callId).GetKV(RunKeyField)
	if err != nil {
		log.Printf("[gotick] read business key of run %s failed: %v", callId, err)
		return "", false
	}
	if !exist {
		return "", true // 没用 WithKey 触发，正常情况
	}
	return v, true
}

// addToStatusIndex 把一次调用记进某个状态的集合。
// 没有 ZRem 能力时整个二级索引都不维护——见 KvRunIndex.zrem。
func (k *KvRunIndex) addToStatusIndex(flowId, callId, status string, score float64) error {
	if k.zrem == nil {
		return nil
	}

	ctx := context.Background()
	if err := k.store.ZAdd(ctx, statusRunsKey(status), callId, score); err != nil {
		return err
	}
	if flowId == "" {
		return nil
	}
	return k.store.ZAdd(ctx, flowStatusRunsKey(flowId, status), callId, score)
}

// backfill 给二级索引上线之前写的记录补上索引条目，并把补好的写回去。
//
// 为什么必须补：两套二级索引都是在 BeginRun 的首次分支里写的，所以升级那一刻
// 已经存在的记录一条都不在里面。后果是在一个全是 done 的列表上选「只看 done」
// 会返回空，按业务 key 搜也搜不到历史。
//
// 这件事读穿解决不了：读穿只能拿到某一条记录的内容，而「按状态取一页并给出精确
// 总数」要求那个集合里本来就有成员。补一次，之后和新记录一样。
//
// 只补索引，不碰 metadata——那份数据一直都在，界面自己读。
func (k *KvRunIndex) backfill(info RunInfo) RunInfo {
	key, ok := k.readRunKey(info.CallId)
	info.Key = key

	// 二级索引用主索引同一个 score（开始时间），两边排序才一致。
	score := float64(info.StartedAt.UnixMilli())
	if err := k.addToStatusIndex(info.FlowId, info.CallId, info.Status, score); err != nil {
		log.Printf("[gotick] backfill status index of run %s failed: %v", info.CallId, err)
		ok = false
	}
	// 裁剪窗口按现在算：这次补录发生在当下，不是在这次调用开始的时候。
	if err := k.addToKeyIndex(info.FlowId, info.CallId, info.Key, score, time.Now()); err != nil {
		log.Printf("[gotick] backfill key index of run %s failed: %v", info.CallId, err)
		ok = false
	}

	// 只有全部补齐才打标记。打上就不会再进来，所以补了一半的记录必须留着
	// 下次列表再试，否则它会永久缺在某个二级索引里。
	info.Indexed = ok

	if err := k.saveRun(info); err != nil {
		log.Printf("[gotick] persist backfill of run %s failed: %v", info.CallId, err)
	}
	return info
}

// addToKeyIndex 把一次调用记进它的业务 key 索引。
//
// 这套索引的键名里含用户数据（key 本身），所以键的数量是无界的：一个 flow
// 跑过多少个不同的订单号，就有多少个 ZSET。清理靠三件事，各管一种情况：
//
//  1. 每次写都刷一遍 TTL。于是「这个 key 再也不会出现」的那些 ZSET，在最后一次
//     被使用之后 retain 时间就自己消失了。这是它们唯一的出路——键名枚举不出来，
//     TrimBefore 够不到。
//  2. 每次写顺手裁掉 now-retain 之前开始的成员。反复复用同一个 key（定时任务那种）
//     不会让它越堆越大。注意 now 是这次调用的开始时间而不是墙上时间，和 trimIfDue
//     的口径一致：生产里调用方传的就是 time.Now()，而拿历史时间回填时窗口会偏早，
//     方向是少裁，不会误删窗口内的记录。
//  3. 上面那一刀把成员裁光时，Redis 会连整个键一起删掉，于是空壳也不留。
//
// 一个已知的边界：TTL 只在这里刷，而这里每个 callId 只走一次（BeginRun 的首次分支），
// 所以活得比 retain 还久的调用会在中途从这套索引里消失。主索引是一样的——TrimBefore
// 按开始时间裁，长命的调用同样会被裁掉。两边一致，都是「保留期以开始时间为锚」
// 这个既有设定的结果，不是这套索引单独的问题。
func (k *KvRunIndex) addToKeyIndex(flowId, callId, key string, score float64, now time.Time) error {
	if key == "" {
		return nil
	}

	ctx := context.Background()
	names := []string{keyRunsKey(key)}
	if flowId != "" {
		names = append(names, flowKeyRunsKey(flowId, key))
	}

	for _, name := range names {
		if err := k.store.ZAdd(ctx, name, callId, score); err != nil {
			return err
		}
		if k.retain <= 0 {
			continue // <=0 表示永久保留，既不设 TTL 也不裁
		}
		if err := k.store.Expire(ctx, name, k.retain); err != nil {
			return err
		}
		if _, err := k.store.ZRemBelow(ctx, name, float64(now.Add(-k.retain).UnixMilli())); err != nil {
			return err
		}
	}
	return nil
}

// moveStatusIndex 把一次调用从旧状态的集合搬到新状态的集合。
func (k *KvRunIndex) moveStatusIndex(flowId, callId, from, to string, score float64) error {
	if k.zrem == nil || from == to {
		return nil
	}

	ctx := context.Background()
	if err := k.zrem.ZRem(ctx, statusRunsKey(from), callId); err != nil {
		return err
	}
	if flowId != "" {
		if err := k.zrem.ZRem(ctx, flowStatusRunsKey(flowId, from), callId); err != nil {
			return err
		}
	}
	return k.addToStatusIndex(flowId, callId, to, score)
}

func (k *KvRunIndex) RecordReplay(callId string, mark ReplayMark) error {
	info, exist, err := k.GetRun(callId)
	if err != nil || !exist {
		return err
	}

	info.Marks = append(info.Marks, mark)
	if len(info.Marks) > maxReplayMarks {
		info.Marks = info.Marks[len(info.Marks)-maxReplayMarks:]
	}

	return k.saveRun(info)
}

func (k *KvRunIndex) FinishRun(callId, status, failedTask, errMsg string, at time.Time) error {
	info, exist, err := k.GetRun(callId)
	if err != nil {
		return err
	}
	if !exist {
		// 没有 BeginRun 过的调用不该出现在这里，忽略而不是凭空造一条记录
		return nil
	}

	prevStatus := info.Status

	info.Status = status
	info.EndedAt = at
	info.UpdatedAt = at
	info.FailedTask = failedTask
	info.Error = errMsg

	if err := k.saveRun(info); err != nil {
		return err
	}

	// 二级索引用主索引同一个 score（开始时间），两边排序才一致。
	// from == to 时是空操作，所以重复 FinishRun 不会把记录搬丢。
	if err := k.moveStatusIndex(info.FlowId, callId, prevStatus, status,
		float64(info.StartedAt.UnixMilli())); err != nil {
		// 主记录已经写成功了，状态是对的，只是二级索引落后了一点。
		// 为此让流程报错不值得——下一次裁剪会把过期成员清掉，
		// 而按状态筛选顶多多显示一条已经结束的调用。
		log.Printf("[gotick] move run %s to status index %s failed: %v", callId, status, err)
	}

	k.trimIfDue(info.FlowId, at)
	return nil
}

// trimIfDue 按保留期裁掉过旧的索引条目。
//
// 详情键 gotick:run:<callId> 自带 TTL，到期自己就没了；但 ZSET 成员不会跟着消失，
// 于是列表里会越堆越多「点进去什么都没有」的空壳，翻很多页才看到有效记录。
// 补的就是这一刀。
//
// 只裁 gotick:runs 和当前 flow 的 gotick:flow:<id>:runs——每个 flow 结束时会裁到
// 自己那条，不需要枚举全部 flow。gotick:flows 不裁：它的大小由 flow 数量决定
// （几十条），而且按时间裁会把仍在服役、只是很久没跑过的 flow 从下拉框里抹掉。
func (k *KvRunIndex) trimIfDue(flowId string, now time.Time) {
	if k.retain <= 0 {
		return // <=0 表示永久保留，不裁
	}

	k.trimMu.Lock()
	if !k.lastTrim.IsZero() && now.Sub(k.lastTrim) < trimInterval {
		k.trimMu.Unlock()
		return
	}
	k.lastTrim = now
	k.trimMu.Unlock()

	// 裁剪失败不该影响流程：索引是观测设施，不是流程状态的一部分。
	cutoff := now.Add(-k.retain)
	if _, err := k.TrimBefore("", cutoff); err != nil {
		log.Printf("[gotick] trim run index failed: %v", err)
	}
	if flowId != "" {
		if _, err := k.TrimBefore(flowId, cutoff); err != nil {
			log.Printf("[gotick] trim run index of flow %s failed: %v", flowId, err)
		}
	}
}

func (k *KvRunIndex) saveRun(info RunInfo) error {
	// 索引记录和 flow 数据用同一个保留期，避免列表里出现点进去什么都没有的条目。
	return k.store.Set(context.Background(), runInfoKey(info.CallId), info, k.retain)
}

func (k *KvRunIndex) GetRun(callId string) (RunInfo, bool, error) {
	var info RunInfo
	exist, err := k.store.Get(context.Background(), runInfoKey(callId), &info)
	if err != nil {
		return info, false, err
	}
	return info, exist, nil
}

func (k *KvRunIndex) ListRuns(flowId string, offset, limit int64) ([]RunInfo, error) {
	return k.listFrom(runsKeyOf(flowId), flowId, offset, limit, nil)
}

// ListRunsByStatus 只返回某个状态的实例，分页和总数都是全量范围的。
//
// 需要 store 支持 ZRem，先用 StatusFilterSupported 问一次；不支持时二级索引
// 从来没被写过，这里会返回空——而不是悄悄退化成「全部」。
func (k *KvRunIndex) ListRunsByStatus(flowId, status string, offset, limit int64) ([]RunInfo, error) {
	if status == "" {
		return k.ListRuns(flowId, offset, limit)
	}
	// 核对真实状态：二级索引的搬迁不和主记录一起原子完成，搬迁失败会留下滞留成员。
	return k.listFrom(statusKeyOf(flowId, status), flowId, offset, limit,
		func(info RunInfo) bool { return info.Status == status })
}

// ListRunsByKey 返回绑定了某个业务 key 的实例，按开始时间倒序。
//
// 不核对记录里的 Key：一次调用的 key 不会变，这套索引只写不删，
// 不存在状态索引那种滞留成员。
func (k *KvRunIndex) ListRunsByKey(flowId, key string, offset, limit int64) ([]RunInfo, error) {
	if key == "" {
		return k.ListRuns(flowId, offset, limit)
	}
	return k.listFrom(keyKeyOf(flowId, key), flowId, offset, limit, nil)
}

// CountRunsByKey 是绑定了这个业务 key 的实例总数。
func (k *KvRunIndex) CountRunsByKey(flowId, key string) (int64, error) {
	if key == "" {
		return k.CountRuns(flowId)
	}
	return k.store.ZCard(context.Background(), keyKeyOf(flowId, key))
}

func (k *KvRunIndex) CountRuns(flowId string) (int64, error) {
	return k.store.ZCard(context.Background(), runsKeyOf(flowId))
}

// CountRunsByStatus 是某个状态下的实例总数。
//
// 它可能比实际能列出来的多一点：二级索引搬迁失败时会有成员滞留在旧状态里，
// 列表会在读到记录时把它们剔掉，而这个计数是直接问 ZSET 的。
// 宁可让计数偏大也不去逐条核对——那等于把一次 ZCARD 换成一次全量扫描。
func (k *KvRunIndex) CountRunsByStatus(flowId, status string) (int64, error) {
	if status == "" {
		return k.CountRuns(flowId)
	}
	return k.store.ZCard(context.Background(), statusKeyOf(flowId, status))
}

func runsKeyOf(flowId string) string {
	if flowId != "" {
		return flowRunsKey(flowId)
	}
	return allRunsKey
}

func statusKeyOf(flowId, status string) string {
	if flowId != "" {
		return flowStatusRunsKey(flowId, status)
	}
	return statusRunsKey(status)
}

func keyKeyOf(flowId, key string) string {
	if flowId != "" {
		return flowKeyRunsKey(flowId, key)
	}
	return keyRunsKey(key)
}

// listFrom 从某个索引集合里读一页并把记录取回来。
//
// keep 非 nil 时用来核对读到的记录还符不符合这个索引的前提。核对是免费的——
// 记录本来就要读——但只有会漂移的索引需要它（见 ListRunsByStatus）。
func (k *KvRunIndex) listFrom(key, flowId string, offset, limit int64,
	keep func(RunInfo) bool) ([]RunInfo, error) {
	members, err := k.store.ZRevRange(context.Background(), key, offset, limit)
	if err != nil {
		return nil, err
	}

	out := make([]RunInfo, 0, len(members))
	for _, m := range members {
		info, exist, err := k.GetRun(m.Member)
		if err != nil {
			return nil, err
		}
		if !exist {
			// 记录已经过期但索引还在，补一条占位的，免得列表出现空洞
			info = RunInfo{
				CallId:    m.Member,
				FlowId:    flowId,
				Status:    "expired",
				StartedAt: time.UnixMilli(int64(m.Score)),
			}
		}
		// exist 才补：不存在时上面那条占位记录是凭空造的，
		// 它的 meta hash 早就跟着 TTL 一起没了，补也补不出东西。
		if exist && !info.Indexed {
			info = k.backfill(info)
		}
		if keep != nil && !keep(info) {
			continue
		}
		out = append(out, info)
	}
	return out, nil
}

// TrimBefore 删掉开始时间早于 cutoff 的索引条目，返回主索引里删掉的数量。
// 记录本身有 TTL 会自己消失，但 ZSET 成员不会，需要定期裁剪。
//
// 按状态的二级索引也一起裁：它们和主索引同构，不裁的话就只增不减了。
// 返回值只算主索引——同一次调用在两处各有一份，加起来会得到一个双倍的数字。
//
// 按 key 的索引裁不到，因为键名里含 key 本身、枚举不出来。它靠自己的 TTL
// 和写入时的顺手裁剪收口，见 addToKeyIndex。
func (k *KvRunIndex) TrimBefore(flowId string, cutoff time.Time) (int64, error) {
	ctx := context.Background()
	max := float64(cutoff.UnixMilli())

	n, err := k.store.ZRemBelow(ctx, runsKeyOf(flowId), max)
	if err != nil {
		return n, err
	}

	if k.zrem == nil {
		return n, nil // 二级索引从来没被写过，没什么可裁的
	}
	for _, status := range indexedStatuses {
		if _, err := k.store.ZRemBelow(ctx, statusKeyOf(flowId, status), max); err != nil {
			return n, err
		}
	}
	return n, nil
}

// noopRunIndex 在没有配置索引时使用，所有操作都是空操作。
type noopRunIndex struct{}

var _ RunIndex = (*noopRunIndex)(nil)

func (noopRunIndex) RegisterFlow(string) error                                 { return nil }
func (noopRunIndex) ListFlows() ([]string, error)                              { return nil, nil }
func (noopRunIndex) BeginRun(string, string, time.Time) error                  { return nil }
func (noopRunIndex) FinishRun(string, string, string, string, time.Time) error { return nil }
func (noopRunIndex) RecordReplay(string, ReplayMark) error                     { return nil }
func (noopRunIndex) GetRun(string) (RunInfo, bool, error)                      { return RunInfo{}, false, nil }
func (noopRunIndex) ListRuns(string, int64, int64) ([]RunInfo, error)          { return nil, nil }
func (noopRunIndex) CountRuns(string) (int64, error)                           { return 0, nil }

// FormatDuration 把时长格式化成人读的形式，UI 和 CLI 共用。
func FormatDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
