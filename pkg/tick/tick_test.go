package tick

import (
	"context"
	"github.com/zbysir/ticktick/internal/pkg/signal"
	"github.com/zbysir/ticktick/internal/tick"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var start = time.Now()

func newTick(name string) *tick.Tick {
	t := NewTick(Options{RedisURL: "redis://localhost:6379/0"})
	flow := t.Flow("demo")

	l := func(format string, args ...interface{}) {
		log.Printf("["+name+"] "+format, args...)
	}

	flow.
		Then("first", func(ctx context.Context) (tick.NextStatus, error) {
			l("[%v] first exec at %v", tick.GetCallId(ctx), time.Now().Sub(start))
			v := tick.GetCallId(ctx)
			tick.Store(ctx, "first", v)
			l("[%v] set first value: %v", tick.GetCallId(ctx), v)
			return tick.NextStatus{}, nil
		}).
		Then("wait-for-second", func(ctx context.Context) (tick.NextStatus, error) {
			l("[%v] wait exec at %v", tick.GetCallId(ctx), time.Now().Sub(start))
			return tick.NextStatus{Status: "sleep", RunAt: time.Now().Add(2 * time.Second)}, nil
		}).
		Then("end", func(ctx context.Context) (tick.NextStatus, error) {
			l("[%v] end exec at %v", tick.GetCallId(ctx), time.Now().Sub(start))
			l("[%v] data: %v", tick.GetCallId(ctx), tick.GetMetaData(ctx))
			return tick.NextStatus{}, nil
		})

	return t
}

func TestTickCreate(t *testing.T) {
	ctx, c := signal.NewContext()

	ti := newTick("default")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ti.Start(ctx)

		t.Logf("tick end")
	}()

	t.Logf("start")

	// asynq 启动与调度有一点延时
	time.Sleep(3 * time.Second)

	start = time.Now()
	callid, err := ti.Trigger("demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("callid: %+v", callid)

	go func() {
		time.Sleep(4 * time.Second)
		c()
	}()

	wg.Wait()
}

func TestTickRestore(t *testing.T) {
	ctx, c := signal.NewContext()
	go func() {
		time.Sleep(2 * time.Second)
		c()
	}()
	ti := newTick("default")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ti.Start(ctx)

		t.Logf("tick end")
	}()

	//callid, err := ti.Trigger("demo", nil)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//t.Logf("callid: %+v", callid)

	wg.Wait()
}

func TestMultiService(t *testing.T) {
	ctx, c := signal.NewContext()
	go func() {
		time.Sleep(5 * time.Second)
		c()
	}()

	var wg sync.WaitGroup

	// 初始化多个tick，他们都使用同一个redis，所以可以互相调度
	ts := []*tick.Tick{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ti := newTick("tick-" + strconv.Itoa(i))
			ts = append(ts, ti)
			ti.Start(ctx)
		}(i)
	}

	time.Sleep(time.Second / 10)

	// 随机选取一个tick触发
	ti := ts[rand.Intn(len(ts))-1]
	callid, err := ti.Trigger("demo", nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("callid: %+v", callid)

	wg.Wait()
}
