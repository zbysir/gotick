package example

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// 这个文件演示并验证「LLM agent 循环跑到一半服务重启，不丢状态也不重复付费」。
//
// agent 的执行形态和传统后台任务很不一样：单步几秒到几十秒、每步都要花钱、
// 失败是常态、循环轮数由模型输出决定。进程在执行期间被重启不是小概率事件，
// 而如果重启意味着从第一轮重新开始，那已经烧掉的 token 就白费了。

// fakeLLM 记录每个 step 被真正调用了几次。
//
// 断言的核心就是这个计数：重放不该让任何一步被重新调用。
type fakeLLM struct {
	mu    sync.Mutex
	calls map[string]int
}

func newFakeLLM() *fakeLLM { return &fakeLLM{calls: map[string]int{}} }

type llmReply struct {
	Text     string `json:"text"`
	ToolName string `json:"tool_name,omitempty"` // 为空表示模型给出了最终答案
	ToolArg  string `json:"tool_arg,omitempty"`
}

// generate 模拟前两轮要求调工具、第三轮给出最终答案。
func (l *fakeLLM) generate(step int, history []string) llmReply {
	l.mu.Lock()
	l.calls[fmt.Sprintf("step-%d", step)]++
	l.mu.Unlock()

	// 真实场景这里是几秒到几十秒的网络调用
	time.Sleep(20 * time.Millisecond)

	switch step {
	case 0:
		return llmReply{Text: "我需要先查一下站点信息", ToolName: "get_site", ToolArg: "42"}
	case 1:
		return llmReply{Text: "还需要看看内容列表", ToolName: "list_content", ToolArg: "42"}
	default:
		return llmReply{Text: fmt.Sprintf("基于 %d 条上下文，结论是：一切正常", len(history))}
	}
}

func (l *fakeLLM) callsOf(step int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[fmt.Sprintf("step-%d", step)]
}

func (l *fakeLLM) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.calls {
		n += c
	}
	return n
}

// fakeTools 记录工具被执行了几次。工具通常是有副作用的，重复执行会出问题。
type fakeTools struct {
	mu    sync.Mutex
	calls map[string]int
}

func newFakeTools() *fakeTools { return &fakeTools{calls: map[string]int{}} }

func (f *fakeTools) run(name, arg string) string {
	f.mu.Lock()
	f.calls[name]++
	f.mu.Unlock()
	return fmt.Sprintf("%s(%s) => ok", name, arg)
}

func (f *fakeTools) callsOf(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[name]
}

// registerAgentFlow 把一个 agent 循环写成 gotick flow。
//
// 关键设计：
//
//   - 每次 LLM 调用包在 Memo 里。Memo 成功后结果立刻落盘，
//     重放时直接读缓存，不会再花一次钱。
//
//   - 每次工具调用同样包在 Memo 里，重放不会重复产生副作用。
//
//   - 循环用普通的 for，不用 Sequence。Sequence 会跳过已完成的轮次，
//     而 history 是靠逐轮 append 累积起来的——跳过就丢了上下文。
//     普通 for 每次重放都从第 0 轮走一遍，但每轮都命中缓存，
//     代价只是几次 Redis 读，换来的是 history 每次都被完整重建。
//
//   - 循环的终止条件依赖 LLM 的输出，而输出已经被 Memo 固化，
//     所以「什么时候停」这个判断在每次重放里都是一样的。
//     这是让非确定性的模型输出能安全参与重放的关键。
//
//   - 每轮后面跟一个 Task。它有两个作用：把这一轮的消息落库，
//     以及制造一个断点让调度器让出——纯 Memo 的循环会在一次重放里跑完，
//     中间没有让出点，UI 上也看不到进度。
func registerAgentFlow(tick *gotick.Server, llm *fakeLLM, tools *fakeTools,
	saved *counter, fin *signaler) {

	const maxSteps = 8

	tick.Flow("agent/chat", func(ctx *gotick.Context) {
		question, _ := ctx.MetaData("question")

		history := []string{"user: " + question}

		for step := 0; step < maxSteps; step++ {
			step := step
			snapshot := append([]string(nil), history...)

			// LLM 调用：贵、慢、可能失败。落盘之后重放不再重复调用。
			reply := gotick.Memo(ctx, fmt.Sprintf("llm:%d", step), func() (llmReply, error) {
				return llm.generate(step, snapshot), nil
			}, gotick.WithMaxRetry(2))

			history = append(history, "assistant: "+reply.Text)

			// 把这一轮落库，同时制造一个断点让出调度
			gotick.Task(ctx, fmt.Sprintf("save:%d", step), func(ctx *gotick.TaskContext) error {
				saved.inc(fmt.Sprintf("step-%d", step))
				return nil
			})

			if reply.ToolName == "" {
				// 模型给出了最终答案。这个判断依赖的是已经 Memo 过的值，
				// 所以每次重放都会在同一轮停下。
				break
			}

			result := gotick.Memo(ctx, fmt.Sprintf("tool:%d", step), func() (string, error) {
				return tools.run(reply.ToolName, reply.ToolArg), nil
			}, gotick.WithMaxRetry(2))

			history = append(history, "tool: "+result)
		}

		gotick.Task(ctx, "finalize", func(ctx *gotick.TaskContext) error {
			saved.inc("finalize")
			saved.inc(fmt.Sprintf("history-len:%d", len(history)))
			return nil
		})
	}).OnSuccess(func(ctx *gotick.Context) error {
		fin.fire()
		return nil
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		fin.fire()
		return nil
	})
}

// TestAgentLoopSurvivesRestart 是这个文件的重点。
//
// 流程跑到一半把 server 停掉，再用一台新的 server 接上同一个 Redis，
// 断言：流程能跑完，而且已经完成的 LLM 调用和工具调用一次都没有重复。
func TestAgentLoopSurvivesRestart(t *testing.T) {
	mr := miniredis.RunT(t)

	llm := newFakeLLM()
	tools := newFakeTools()
	saved := newCounter()

	// ---- 第一台 server：跑一部分就被停掉 ----
	firstDone := newSignaler()
	first := newRedisServer(t, mr.Addr())
	registerAgentFlow(first, llm, tools, saved, firstDone)

	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		_ = first.StartServer(ctx1)
	}()

	callId, err := first.Trigger(ctx1, "agent/chat", gotick.MetaData{"question": "帮我看看这个站点"})
	require.NoError(t, err)

	// 等到至少完成第一轮（LLM 调过、工具调过），再模拟重启
	require.Eventually(t, func() bool {
		return llm.callsOf(0) >= 1 && tools.callsOf("get_site") >= 1
	}, 20*time.Second, 20*time.Millisecond, "第一轮应该先跑起来")

	cancel1()
	wg1.Wait()

	callsBefore := llm.total()
	t.Logf("重启前：LLM 调用 %d 次，get_site %d 次", callsBefore, tools.callsOf("get_site"))
	require.False(t, isFired(firstDone), "第一台 server 不该已经把流程跑完，否则这条用例没有意义")

	// ---- 第二台 server：全新进程，接上同一个 Redis ----
	secondDone := newSignaler()
	second := newRedisServer(t, mr.Addr())
	registerAgentFlow(second, llm, tools, saved, secondDone)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		_ = second.StartServer(ctx2)
	}()

	select {
	case <-secondDone.ch:
	case <-time.After(60 * time.Second):
		t.Fatalf("重启后流程没有跑完；LLM 调用情况：%v", llm.calls)
	}

	time.Sleep(300 * time.Millisecond) // 给重复执行一点暴露的机会
	cancel2()
	wg2.Wait()

	// ---- 断言 ----

	// 三轮 LLM：两轮要求调工具，第三轮给出最终答案。
	//
	// 这是整条用例的核心：重启之后没有任何一步被重新调用。
	// 如果没有 checkpoint，重启就意味着从第 0 轮重头再来，
	// 已经花掉的钱全部作废。
	for step := 0; step < 3; step++ {
		assert.Equal(t, 1, llm.callsOf(step),
			"step %d 的 LLM 调用应该恰好一次——重放必须命中缓存而不是重新付费", step)
	}
	assert.Equal(t, 3, llm.total(), "总共只该有三轮 LLM 调用")

	// 工具有副作用，更不能重复执行
	assert.Equal(t, 1, tools.callsOf("get_site"))
	assert.Equal(t, 1, tools.callsOf("list_content"))

	// 每一轮的落库动作也只做一次
	for step := 0; step < 3; step++ {
		assert.Equal(t, 1, saved.get(fmt.Sprintf("step-%d", step)),
			"step %d 的落库只该发生一次", step)
	}
	assert.Equal(t, 1, saved.get("finalize"))

	// history 必须被完整重建：1 条 user + 3 条 assistant + 2 条 tool = 6
	assert.Equal(t, 1, saved.get("history-len:6"),
		"重放必须把完整的对话历史重建出来，实际计数：%v", saved.snapshot())

	// 索引里能查到这次调用，UI 上看得见
	run, exist, err := second.RunIndex().GetRun(callId)
	require.NoError(t, err)
	require.True(t, exist)
	assert.Equal(t, gotick.RunStatusDone, run.Status)
	t.Logf("重启后完成：%s 共重放 %d 次，耗时 %s", callId, run.Replays, gotick.FormatDuration(run.Duration()))
}

// TestAgentLoopStopsWhereTheModelSaidSo 验证「循环轮数由模型输出决定」
// 这件事在重放下依然是确定的。
func TestAgentLoopStopsWhereTheModelSaidSo(t *testing.T) {
	mr := miniredis.RunT(t)

	llm := newFakeLLM()
	tools := newFakeTools()
	saved := newCounter()
	fin := newSignaler()

	tick := newRedisServer(t, mr.Addr())
	registerAgentFlow(tick, llm, tools, saved, fin)

	runFlow(t, tick, "agent/chat", gotick.MetaData{"question": "hi"}, fin, 60*time.Second)

	// fakeLLM 在第三轮才给出最终答案，所以循环必须正好停在第三轮。
	// 第 3 轮及以后不该有任何调用。
	assert.Equal(t, 3, llm.total())
	assert.Equal(t, 0, llm.callsOf(3), "模型给出最终答案之后就不该再调了")
	assert.Equal(t, 0, saved.get("step-3"))
	assert.Equal(t, 1, saved.get("history-len:6"))
}

// isFired 报告 signaler 是否已经被触发。
func isFired(s *signaler) bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}
