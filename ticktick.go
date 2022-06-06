package ticktick

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"
)

type WorkflowContext struct {
	Params json.Marshaler

	jod   []string
	state map[string]State
	timer []*Timer
	w     *Workflow
}

// 记录每一个步骤的执行状态，每一个 workflow 有有限个状态，可落盘
type State struct {
	response interface{} // 每一个结果
	status   Status
	pauseAt  time.Time
}

type Status = string

const (
	Padding   Status = "padding"    // 还未运行
	Running   Status = "running"    // 运行中
	Paused    Status = "paused"     // 时停，只有 sleep job 才会有这个状态
	WaitRetry Status = "wait_retry" // 重试等待中
	Ran       Status = "ran"        // 已经运行过，会存储 Response
)

var NeedRetryError = errors.New("need retry error")

type Timer struct {
	On time.Time
	// params
	wfName  string
	jobName string
}

type Workflow struct {
	jod []string

	data      map[string]State
	hook      map[string]interface{}
	workflows map[string]func(ctx *WorkflowContext) error
	timer     []*Timer
}

type UniquKeyer interface {
	UniquKey() string
}

func (c *WorkflowContext) pushTimer(jobName string, wfName string, on time.Time, ctx context.Context) {
	c.timer = append(c.timer, &Timer{
		On:      on,
		wfName:  wfName,
		jobName: jobName,
	})

	time.AfterFunc(on.Sub(time.Now()), func() {
		log.Printf("timer fight")
		status, err := c.w.Touch(wfName, WorkflowContextWithCtx(ctx, c))
		if err != nil {
			log.Printf("error: %v", err)
		} else {
			log.Printf("status: %v", status)
		}
	})
}

// Wrap 运行一个方法 f，返回一个结果，如果已经运行过了则会取已经缓存的结果 而不会再次运行方法，同样的 depend 始终只会运行成功一次（方法 f 依然需要保证幂等）。
func (c *WorkflowContext) Wrap(jobName string, f func() (any, error), rstPrt interface{}, dependencies []fmt.Stringer) (err error) {
	if x, ok := c.state[jobName]; ok {
		reflect.ValueOf(rstPrt).Elem().Set(reflect.ValueOf(x.response))
		return
	}

	r, err := f()
	if err != nil {
		return err
	}
	reflect.ValueOf(rstPrt).Elem().Set(reflect.ValueOf(r))

	c.state[jobName] = State{
		response: r,
		status:   Running,
	}
	return nil
}

func (c *Workflow) Hook(jobName string, rstPrt interface{}) (exist bool, err error) {
	c.jod = append(c.jod, jobName)

	r, ok := c.data[jobName]
	if !ok {
		return false, nil
	}

	if ok {
		reflect.ValueOf(rstPrt).Elem().Set(reflect.ValueOf(r.response))
		return true, nil
	}

	return
}

func (c *Workflow) SendHook(jobName string, s interface{}) (exist bool, err error) {
	c.jod = append(c.jod, jobName)

	r, ok := c.data[jobName]
	if ok {
		r.response = s
		c.data[jobName] = r
		return
	} else {
		c.data[jobName] = State{response: r}
	}

	return
}

func (c *Workflow) Handle(wfName string, f func(ctx *WorkflowContext) error) {
	c.workflows[wfName] = f
	return
}

var ctxWorkflowName struct{}

func WorkflowNameFromCtx(ctx context.Context) string {
	x := ctx.Value(ctxWorkflowName)
	if x != nil {
		return x.(string)
	}

	return ""
}

func WorkflowNameWithCtx(ctx context.Context, wfName string) context.Context {
	return context.WithValue(ctx, ctxWorkflowName, wfName)
}

var ctxWorkflowContext struct {
	A string
}

func WorkflowContextFromCtx(ctx context.Context) *WorkflowContext {
	x := ctx.Value(ctxWorkflowContext)
	if x != nil {
		return x.(*WorkflowContext)
	}

	return nil
}

func WorkflowContextWithCtx(ctx context.Context, c *WorkflowContext) context.Context {
	return context.WithValue(ctx, ctxWorkflowContext, c)
}

type SleepStatus struct {
	On time.Time
}

func (s SleepStatus) Error() string {
	return fmt.Sprintf("sleep unit %s", s.On)
}

type ExitStatus struct {
	IsFinish bool
	IsSleep  *SleepStatus
	IsError  error
}

func (e ExitStatus) String() string {
	if e.IsFinish {
		return "finished"
	}
	if e.IsSleep != nil {
		return fmt.Sprintf("sleep unit %v", e.IsSleep.On)
	}
	if e.IsError != nil {
		return fmt.Sprintf("error :%v", e.IsError)
	}
	panic(e)
}

func (c *Workflow) Touch(wfName string, ctx context.Context) (ExitStatus, error) {
	// todo 做成发布模式
	ctx = WorkflowNameWithCtx(ctx, wfName)

	// todo WorkflowContext 需要从持久化数据库获取
	wctx := WorkflowContextFromCtx(ctx)
	if wctx == nil {
		wctx = &WorkflowContext{
			Params: nil,
			jod:    nil,
			state:  map[string]State{},
			timer:  nil,
			w:      c,
		}
	}

	defer func() {
		// 将 wctx 存档
	}()

	err := c.workflows[wfName](wctx)
	if err != nil {
		var s SleepStatus
		if errors.As(err, &s) {
			// 存储 timer
			wctx.pushTimer("", wfName, s.On, ctx)
			return ExitStatus{
				IsSleep: &s,
			}, nil
		}
		return ExitStatus{IsError: err}, err
	}
	return ExitStatus{IsFinish: true}, nil
}

var SleepError = fmt.Errorf("sleep error")

// Sleep 将会停止整个 Workflow，创建一个 Timer 等到时间到了会再次执行整个 Workflow
func (c *WorkflowContext) Sleep(jobName string, duration time.Duration) (err error) {
	c.jod = append(c.jod, jobName)

	if x, ok := c.state[jobName]; ok {
		if x.status == "pause" {
			if x.pauseAt.Add(duration).Before(time.Now()) {
				x.status = "running"
				c.state[jobName] = x
				return nil
			} else {
				return SleepStatus{
					On: x.pauseAt.Add(duration),
				}
			}
		}
		return nil
	}

	c.state[jobName] = State{
		response: "",
		status:   "pause",
		pauseAt:  time.Now(),
	}
	return SleepStatus{
		On: time.Now().Add(duration),
	}
}
