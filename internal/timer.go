package internal

// TimerModel 还未运行的定时器保存在 redis 中，已经运行的定时器保存在 mysql，提供核对功能，可定义一定的过期策略，如最多保存一个月。
type TimerModel struct {
	Id        int64
	CreatedAt int64       `json:"created_at"` // 创建时间戳
	RunAt     int64       `json:"run_at"`     // 运行时间戳
	Payload   []byte      `json:"body"`
	Status    TimerStatus `json:"status"`
}

type TimerStatus = string

const (
	Pending TimerStatus = "pending"
	Fired   TimerStatus = "fired"
)

type TimerHistoryStatus = int

type TimerHistoryModel struct {
	Id      int64
	TimerId int64
	Status  TimerHistoryStatus
}

type Timer interface {
	Create(runAt int64, payload []byte) (id int64, err error) // 新建一个 timer
	GetLatestPending(max int) (ts []TimerModel, err error)    // 获取应该触发的 timer
	Fired(id int64) (ok bool, err error)                      // 将 timer 更新为已触发
}

type TestTimer struct {
	ts []TimerModel
}

func (t *TestTimer) Create(runAt int64, payload []byte) (id int64, err error) {
	return 0, nil
}

func (t *TestTimer) GetLatestPending(max int) (ts []TimerModel, err error) {
	// 获取最新的 Timer，保证只有一个节点能获取
	return t.ts, nil
}

func (t *TestTimer) Fired(id int64) (ok bool, err error) {
	return false, nil
}
