package example

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
)

// 这个文件里的用例照搬了 weave 生产环境里真实的 flow 结构
// （internal/task/ 下的 content_import、send_email_sms 和 gen_preview_ppt），
// 只是把数据库、邮件网关和截图服务换成了内存里的假实现。
//
// 它们存在的意义是：gotick 的调度层改动很大，而 weave 依赖的正是
// 「Memo 的值在重放之间不变」「ts.Errs 的条数准确」「WithMaxRetry 的语义」
// 「回调里能再读一次 Memo」这几件事——它们恰好是最容易在重构中被破坏、
// 又最难在生产上发现的性质。gen_preview 那条就抓到了一个真实的回归。

// importTaskRecord 模拟 weave 的 contentImportRepo：记录 OnFail / OnSuccess 写回的结果。
type importTaskRecord struct {
	mu         sync.Mutex
	status     string
	errorMsg   string
	finishedAt time.Time
	writes     int
}

func (r *importTaskRecord) markFailed(errs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = "failed"
	// 和 weave 一样把所有错误拼起来写进数据库
	r.errorMsg = "Error: " + strings.Join(errs, "\n")
	r.finishedAt = time.Now()
	r.writes++
}

func (r *importTaskRecord) markSucceeded() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = "completed"
	r.finishedAt = time.Now()
	r.writes++
}

func (r *importTaskRecord) snapshot() (status, errorMsg string, writes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, r.errorMsg, r.writes
}

// TestWeaveContentImportFlow 覆盖 weave 的 content_import：
// Memo(start_at) + 单个带重试的 Task，从 MetaData 取 task_id，
// OnFail 把 ts.Errs 拼成错误信息写回数据库。
func TestWeaveContentImportFlow(t *testing.T) {
	t.Run("导入成功", func(t *testing.T) {
		mr := miniredis.RunT(t)
		tick := newRedisServer(t, mr.Addr())
		fin := newSignaler()
		rec := &importTaskRecord{}

		var (
			mu           sync.Mutex
			seenTaskIds  []string
			seenStartAts []time.Time
		)

		tick.Flow("content_import", func(ctx *gotick.Context) {
			startAt := gotick.Memo(ctx, "start_at", func() (time.Time, error) {
				return time.Now(), nil
			})

			// 每次重放都记一次，用来验证 Memo 的值在重放之间是稳定的
			mu.Lock()
			seenStartAts = append(seenStartAts, startAt)
			mu.Unlock()

			gotick.Task(ctx, "import_content", func(ctx *gotick.TaskContext) error {
				taskId, ok := ctx.MetaData("task_id")
				if !ok {
					return fmt.Errorf("缺少任务ID")
				}

				mu.Lock()
				seenTaskIds = append(seenTaskIds, taskId)
				mu.Unlock()

				time.Sleep(30 * time.Millisecond)
				return nil
			}, gotick.WithMaxRetry(3))
		}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
			rec.markFailed(ts.Errs)
			fin.fire()
			return nil
		}).OnSuccess(func(ctx *gotick.Context) error {
			rec.markSucceeded()
			fin.fire()
			return nil
		})

		runFlow(t, tick, "content_import", gotick.MetaData{"task_id": "10086"}, fin, 30*time.Second)

		status, errMsg, writes := rec.snapshot()
		assert.Equal(t, "completed", status)
		assert.Empty(t, errMsg)
		assert.Equal(t, 1, writes, "终态回调只应该写一次数据库")

		mu.Lock()
		defer mu.Unlock()

		require.Len(t, seenTaskIds, 1, "任务体必须恰好执行一次")
		assert.Equal(t, "10086", seenTaskIds[0], "MetaData 必须能在 task 内部读到")

		// Memo 的核心契约：flow 函数被重放多次，但缓存值必须每次都一样。
		// weave 把 start_at 写进日志，如果它每次重放都变，日志和耗时统计就全是错的。
		require.Greater(t, len(seenStartAts), 1, "带 Task 的 flow 一定会被重放不止一次")
		for i, got := range seenStartAts {
			assert.True(t, got.Equal(seenStartAts[0]),
				"replay #%d 拿到的 start_at 是 %v，第一次是 %v —— Memo 在重放之间必须稳定",
				i, got, seenStartAts[0])
		}
	})

	t.Run("导入始终失败", func(t *testing.T) {
		mr := miniredis.RunT(t)
		tick := newRedisServer(t, mr.Addr())
		fin := newSignaler()
		rec := &importTaskRecord{}

		attempts := newCounter()
		const maxRetry = 3

		tick.Flow("content_import", func(ctx *gotick.Context) {
			gotick.Memo(ctx, "start_at", func() (time.Time, error) {
				return time.Now(), nil
			})

			gotick.Task(ctx, "import_content", func(ctx *gotick.TaskContext) error {
				attempts.inc("import")
				return fmt.Errorf("获取导入任务失败: connection refused")
			}, gotick.WithMaxRetry(maxRetry))
		}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
			rec.markFailed(ts.Errs)
			fin.fire()
			return nil
		}).OnSuccess(func(ctx *gotick.Context) error {
			t.Error("一个始终失败的 flow 不该触发 OnSuccess")
			fin.fire()
			return nil
		})

		runFlow(t, tick, "content_import", gotick.MetaData{"task_id": "10086"}, fin, 60*time.Second)

		status, errMsg, writes := rec.snapshot()
		assert.Equal(t, "failed", status)
		assert.Equal(t, 1, writes, "OnFail 只应该写一次数据库")

		// WithMaxRetry(n) 是「重试 n 次」，不含第一次执行，所以总共 n+1 次尝试。
		want := maxRetry + 1
		assert.Equal(t, want, attempts.get("import"),
			"WithMaxRetry(%d) 应该总共尝试 %d 次", maxRetry, want)

		// weave 把 ts.Errs 拼起来写进数据库给用户看，条数必须和真实尝试次数一致，
		// 不能因为终态被重复标记而多出几条。
		lines := strings.Split(strings.TrimPrefix(errMsg, "Error: "), "\n")
		assert.Len(t, lines, want,
			"错误列表的条数必须等于尝试次数，实际写入的是：%q", errMsg)
		for _, line := range lines {
			assert.Contains(t, line, "connection refused")
		}
	})

	t.Run("缺少 task_id 时按错误处理", func(t *testing.T) {
		mr := miniredis.RunT(t)
		tick := newRedisServer(t, mr.Addr())
		fin := newSignaler()
		rec := &importTaskRecord{}

		tick.Flow("content_import", func(ctx *gotick.Context) {
			gotick.Task(ctx, "import_content", func(ctx *gotick.TaskContext) error {
				if _, ok := ctx.MetaData("task_id"); !ok {
					return fmt.Errorf("缺少任务ID")
				}
				return nil
			}, gotick.WithMaxRetry(0)) // 缺参数重试也没用，一次就失败
		}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
			rec.markFailed(ts.Errs)
			fin.fire()
			return nil
		})

		runFlow(t, tick, "content_import", nil, fin, 30*time.Second)

		status, errMsg, _ := rec.snapshot()
		assert.Equal(t, "failed", status)
		assert.Contains(t, errMsg, "缺少任务ID")
		assert.Equal(t, 1, len(strings.Split(strings.TrimPrefix(errMsg, "Error: "), "\n")),
			"WithMaxRetry(0) 意味着不重试，只应该有一条错误")
	})
}

// sentMail 模拟 weave 的 email 网关和 notificationRepo。
type sentMail struct {
	mu       sync.Mutex
	sends    int
	statuses []string
	lastInfo string
}

func (s *sentMail) send() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
}

func (s *sentMail) setStatus(status, info string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, status)
	s.lastInfo = info
}

func (s *sentMail) snapshot() (sends int, statuses []string, info string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sends, append([]string(nil), s.statuses...), s.lastInfo
}

// TestWeaveSendEmailSmsFlow 覆盖 weave 的 send_email_sms。
//
// 它和 content_import 的关键差别是 WithMaxRetry(-1)：发信不能重试，
// 因为重试意味着用户可能收到两封一模一样的邮件。
func TestWeaveSendEmailSmsFlow(t *testing.T) {
	t.Run("发送成功", func(t *testing.T) {
		mr := miniredis.RunT(t)
		tick := newRedisServer(t, mr.Addr())
		fin := newSignaler()
		mail := &sentMail{}

		tick.Flow("send_email_sms", func(ctx *gotick.Context) {
			gotick.Memo(ctx, "start_at", func() (time.Time, error) {
				return time.Now(), nil
			})

			gotick.Task(ctx, "send", func(ctx *gotick.TaskContext) error {
				params, ok := ctx.MetaData("params")
				if !ok {
					// weave 在这里返回 nil：没有参数就当作什么都不用做
					return nil
				}
				if params == "" {
					return fmt.Errorf("title or content is required")
				}

				mail.setStatus("started", "")
				mail.send()
				mail.setStatus("success", "")
				return nil
			}, gotick.WithMaxRetry(-1))
		}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
			mail.setStatus("failed", strings.Join(ts.Errs, "\n"))
			fin.fire()
			return nil
		}).OnSuccess(func(ctx *gotick.Context) error {
			fin.fire()
			return nil
		})

		runFlow(t, tick, "send_email_sms",
			gotick.MetaData{"params": `{"type":"email","to":["a@example.com"]}`}, fin, 30*time.Second)

		sends, statuses, _ := mail.snapshot()
		assert.Equal(t, 1, sends, "邮件必须恰好发一次")
		assert.Equal(t, []string{"started", "success"}, statuses)
	})

	t.Run("发送失败时绝不重试", func(t *testing.T) {
		mr := miniredis.RunT(t)
		tick := newRedisServer(t, mr.Addr())
		fin := newSignaler()
		mail := &sentMail{}

		attempts := newCounter()

		tick.Flow("send_email_sms", func(ctx *gotick.Context) {
			gotick.Memo(ctx, "start_at", func() (time.Time, error) {
				return time.Now(), nil
			})

			gotick.Task(ctx, "send", func(ctx *gotick.TaskContext) error {
				attempts.inc("send")
				mail.setStatus("started", "")
				return fmt.Errorf("smtp: 550 mailbox unavailable")
			}, gotick.WithMaxRetry(-1))
		}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
			mail.setStatus("failed", strings.Join(ts.Errs, "\n"))
			fin.fire()
			return nil
		}).OnSuccess(func(ctx *gotick.Context) error {
			t.Error("发送失败的 flow 不该触发 OnSuccess")
			fin.fire()
			return nil
		})

		runFlow(t, tick, "send_email_sms",
			gotick.MetaData{"params": `{"type":"email","to":["a@example.com"]}`}, fin, 30*time.Second)

		// 这是这条用例真正要守住的性质：
		// WithMaxRetry(-1) 表示一次都不重试，否则用户会收到重复的邮件。
		assert.Equal(t, 1, attempts.get("send"),
			"WithMaxRetry(-1) 必须只执行一次——重试会让用户收到重复邮件")

		sends, statuses, info := mail.snapshot()
		assert.Equal(t, 0, sends)
		assert.Equal(t, []string{"started", "failed"}, statuses)
		assert.Equal(t, "smtp: 550 mailbox unavailable", info,
			"只尝试了一次，错误列表里就只该有一条")
	})

	t.Run("没有参数时视为成功且不发信", func(t *testing.T) {
		mr := miniredis.RunT(t)
		tick := newRedisServer(t, mr.Addr())
		fin := newSignaler()
		mail := &sentMail{}

		tick.Flow("send_email_sms", func(ctx *gotick.Context) {
			gotick.Task(ctx, "send", func(ctx *gotick.TaskContext) error {
				if _, ok := ctx.MetaData("params"); !ok {
					return nil
				}
				mail.send()
				return nil
			}, gotick.WithMaxRetry(-1))
		}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
			t.Errorf("不该失败：%v", ts.Errs)
			fin.fire()
			return nil
		}).OnSuccess(func(ctx *gotick.Context) error {
			fin.fire()
			return nil
		})

		runFlow(t, tick, "send_email_sms", nil, fin, 30*time.Second)

		sends, _, _ := mail.snapshot()
		assert.Equal(t, 0, sends, "没有参数时不该发出任何邮件")
	})
}

// TestWeaveGenPreviewFlow 覆盖 weave 的 gen_preview_ppt。
//
// 它的特别之处在于 OnSuccess 回调里又读了一次 Memo("start_at") 来算总耗时。
// 这是合法用法——回调里读缓存值不是重复声明——所以重名检测不能把它拦下来。
func TestWeaveGenPreviewFlow(t *testing.T) {
	mr := miniredis.RunT(t)
	tick := newRedisServer(t, mr.Addr())
	fin := newSignaler()

	var (
		mu            sync.Mutex
		bodyStartAt   time.Time
		successStart  time.Time
		successCalled bool
	)

	tick.Flow("gen_preview_ppt", func(ctx *gotick.Context) {
		startAt := gotick.Memo(ctx, "start_at", func() (time.Time, error) {
			return time.Now(), nil
		})

		mu.Lock()
		bodyStartAt = startAt
		mu.Unlock()

		gotick.Task(ctx, "screenshot", func(ctx *gotick.TaskContext) error {
			id, ok := ctx.MetaData("id")
			if !ok {
				return fmt.Errorf("缺少 id")
			}
			ctx.SetMetaData("filename", "preview-"+id+".png")
			return nil
		}, gotick.WithMaxRetry(-1))
	}).OnFail(func(ctx *gotick.Context, ts gotick.TaskStatus) error {
		t.Errorf("不该失败：%v", ts.Errs)
		fin.fire()
		return nil
	}).OnSuccess(func(ctx *gotick.Context) error {
		filename, _ := ctx.MetaData("filename")
		assert.Equal(t, "preview-42.png", filename,
			"task 里 SetMetaData 写的值必须能在 OnSuccess 里读到")

		// weave 就是这么算耗时的：在回调里再读一次 Memo。
		startAt := gotick.Memo(ctx, "start_at", func() (time.Time, error) {
			t.Error("OnSuccess 里的 Memo 应该命中缓存，不该重新构建")
			return time.Now(), nil
		})

		mu.Lock()
		successStart = startAt
		successCalled = true
		mu.Unlock()

		fin.fire()
		return nil
	})

	runFlow(t, tick, "gen_preview_ppt", gotick.MetaData{"id": "42"}, fin, 30*time.Second)

	mu.Lock()
	defer mu.Unlock()

	require.True(t, successCalled, "OnSuccess 必须被调用")
	assert.True(t, successStart.Equal(bodyStartAt),
		"OnSuccess 里读到的 start_at (%v) 必须和 flow 体里的 (%v) 一致",
		successStart, bodyStartAt)
}
