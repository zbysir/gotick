package gotick

import "time"

type breakSleep struct {
	Task  string
	RunAt time.Time
}

func (b *breakSleep) GetTask() string {
	return b.Task
}

// BreakSleep means the task is exec success and sleep for d.
func BreakSleep(task string, d time.Duration) Breakpoint {
	return &breakSleep{Task: task, RunAt: time.Now().Add(d)}
}

type breakDone struct {
	Task string
}

func (b *breakDone) GetTask() string {
	return b.Task
}

// BreakDone means the task is exec success
func BreakDone(task string) Breakpoint {
	return &breakDone{Task: task}
}

type breakAbort struct {
	Task  string
	Error error // 可以为空
}

func (b *breakAbort) GetTask() string {
	return b.Task
}

// BreakAbort 终止整个 flow，err 可以为空
func BreakAbort(task string, err error) Breakpoint {
	return &breakAbort{Task: task, Error: err}
}

type breakStatus struct {
	Task string
}

func (b *breakStatus) GetTask() string {
	return b.Task
}

type breakRetry struct {
	Task string
	Err  error
}

// BreakRetry Abort means abort the flow
func BreakRetry(task string, err error) Breakpoint {
	return &breakRetry{Task: task, Err: err}
}

func (b *breakRetry) GetTask() string {
	return b.Task
}

type Breakpoint interface {
	GetTask() string
}

type breakFail struct {
	Task string
	Err  error
}

func (b *breakFail) GetTask() string {
	return b.Task
}

func BreakFail(task string, err error) Breakpoint {
	return &breakFail{Task: task, Err: err}
}

type breakWait struct {
	RunAt time.Time
	Task  string
}

func (b *breakWait) GetTask() string {
	return b.Task
}

// BreakWait 和 BreakSleep 的区别是 BreakSleep 只会在执行指定的 task 时才会 sleep，而 Wait 始终会 sleep。
func BreakWait(t time.Duration) Breakpoint {
	return &breakWait{Task: "*", RunAt: time.Now().Add(t)} // * 表示不是针对某个 task 的断点，而是系统断点。
}

type breakContinue struct {
	Task string
}

func (b *breakContinue) GetTask() string {
	return b.Task
}

func BreakContinue(t time.Duration) Breakpoint {
	return &breakContinue{Task: "*"} // * 表示不是针对某个 task 的断点，而是系统断点。
}
