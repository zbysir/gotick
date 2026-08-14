package example

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// TestReplayTrace 把「一个 flow 到底被重放了几次、每次停在哪」打印出来。
//
// 重放次数不等于 task 数量，这一点很容易让人困惑：
// 每个会写状态的断点都要占用一次调度，而 flow 函数正常返回本身也要占一次。
func TestReplayTrace(t *testing.T) {
	mr := miniredis.RunT(t)
	tick := newRedisServer(t, mr.Addr())
	fin := newSignaler()

	var (
		mu     sync.Mutex
		replay int
		trace  []string
	)
	note := func(format string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		trace = append(trace, fmt.Sprintf(format, a...))
	}

	// 和 example/uidemo 里的 demo/scheduled-report 同样的形状：
	// Task → Sleep → Task
	tick.Flow("demo/report", func(ctx *gotick.Context) {
		mu.Lock()
		replay++
		n := replay
		mu.Unlock()

		note("重放 #%d 开始", n)

		gotick.Task(ctx, "collect-data", func(*gotick.TaskContext) error {
			note("  #%d 执行 collect-data", n)
			return nil
		})
		note("  #%d 走过 collect-data", n)

		gotick.Sleep(ctx, "wait-for-quiet-hours", 2*time.Second)
		note("  #%d 走过 sleep", n)

		gotick.Task(ctx, "render-and-send", func(*gotick.TaskContext) error {
			note("  #%d 执行 render-and-send", n)
			return nil
		})
		note("  #%d 走过 render-and-send（flow 函数即将正常返回）", n)
	}).OnSuccess(func(ctx *gotick.Context) error {
		note("OnSuccess")
		fin.fire()
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Errorf("不该失败：%v", ts.Errs)
		fin.fire()
		return nil
	})

	callId := runFlow(t, tick, "demo/report", nil, fin, 60*time.Second)

	mu.Lock()
	for _, line := range trace {
		t.Log(line)
	}
	total := replay
	mu.Unlock()

	run, exist, err := tick.RunIndex().GetRun(callId)
	require.NoError(t, err)
	require.True(t, exist)

	t.Logf("索引记录的重放次数 = %d，实际进入 flow 函数 %d 次", run.Replays, total)

	assert.Equal(t, total, run.Replays,
		"索引里的重放次数应该等于 flow 函数被真正执行的次数")

	// 三个断点（两个 Task 各一次、Sleep 一次）加上最后一次「什么都不做直接跑完」，
	// 至少要四次调度。
	assert.GreaterOrEqual(t, run.Replays, 4)
}
