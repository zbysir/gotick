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
	RunStatusRunning = "running"
	RunStatusDone    = "done"
	RunStatusFailed  = "failed"
	RunStatusAborted = "aborted"
)

// RunInfo 是一次 flow 调用的概览，用于列表展示。
//
// 每个 task 的细节在 {callId}_status 里，这里只放「一眼能看懂」的东西。
type RunInfo struct {
	CallId    string    `json:"call_id"`
	FlowId    string    `json:"flow_id"`
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
	return r.Status == RunStatusDone || r.Status == RunStatusFailed || r.Status == RunStatusAborted
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

const (
	flowsKey   = "gotick:flows"
	allRunsKey = "gotick:runs"
)

func flowRunsKey(flowId string) string { return "gotick:flow:" + flowId + ":runs" }
func runInfoKey(callId string) string  { return "gotick:run:" + callId }

// KvRunIndex 把索引建在 KVStore 上，因此 Redis 和内存实现都能用。
type KvRunIndex struct {
	store store.KVStore

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
	return &KvRunIndex{store: s, retain: retain}
}

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

		score := float64(at.UnixMilli())
		if err := k.store.ZAdd(ctx, allRunsKey, callId, score); err != nil {
			return err
		}
		if err := k.store.ZAdd(ctx, flowRunsKey(flowId), callId, score); err != nil {
			return err
		}
	}

	// 读-改-写在这里是安全的：同一个 callId 的重放已经被租约串行化了。
	info.Replays++
	info.UpdatedAt = at

	return k.saveRun(info)
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

	info.Status = status
	info.EndedAt = at
	info.UpdatedAt = at
	info.FailedTask = failedTask
	info.Error = errMsg

	if err := k.saveRun(info); err != nil {
		return err
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
	key := allRunsKey
	if flowId != "" {
		key = flowRunsKey(flowId)
	}

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
		out = append(out, info)
	}
	return out, nil
}

func (k *KvRunIndex) CountRuns(flowId string) (int64, error) {
	key := allRunsKey
	if flowId != "" {
		key = flowRunsKey(flowId)
	}
	return k.store.ZCard(context.Background(), key)
}

// TrimBefore 删掉开始时间早于 cutoff 的索引条目。
// 记录本身有 TTL 会自己消失，但 ZSET 成员不会，需要定期裁剪。
func (k *KvRunIndex) TrimBefore(flowId string, cutoff time.Time) (int64, error) {
	key := allRunsKey
	if flowId != "" {
		key = flowRunsKey(flowId)
	}
	return k.store.ZRemBelow(context.Background(), key, float64(cutoff.UnixMilli()))
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
