package test

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/internal/pkg/signal"
	"github.com/zbysir/gotick/internal/store"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var start = time.Now()

func newTick(name string, whenEnd func()) *gotick.TickServer {
	url := "redis://localhost:6379/0"
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)

	t := gotick.NewTickServer(gotick.Options{RedisURL: url, DelayedQueue: store.NewAsynq(redisClient)})
	flow := t.Flow(name)

	l := func(format string, args ...interface{}) {
		log.Printf("["+name+"] "+format, args...)
	}

	flow.
		Then("first", func(ctx context.Context) (gotick.NextStatus, error) {
			l("[%v] first exec at %v", gotick.GetCallId(ctx), time.Now().Sub(start))
			v := gotick.GetCallId(ctx)
			gotick.Store(ctx, "first", v)
			return gotick.Done(), nil
		}).
		Then("wait-for-second", func(ctx context.Context) (gotick.NextStatus, error) {
			l("[%v] wait exec at %v", gotick.GetCallId(ctx), time.Now().Sub(start))
			return gotick.Sleep(2 * time.Second), nil
		}).
		Then("end", func(ctx context.Context) (gotick.NextStatus, error) {
			l("[%v] end exec at %v, data: %v", gotick.GetCallId(ctx), time.Now().Sub(start), gotick.GetMetaData(ctx))
			return gotick.Done(), nil
		}).
		Success(func(ctx context.Context, t gotick.TaskStatus) error {
			l("[%v] success exec at %v, task: %v", gotick.GetCallId(ctx), time.Now().Sub(start), t)
			whenEnd()
			return nil
		}).
		Fail(func(ctx context.Context, t gotick.TaskStatus) error {
			l("[%v] fail exec at %v, task: %v", gotick.GetCallId(ctx), time.Now().Sub(start), t)
			return nil
		})

	return t
}
func newFailTick(name string, whenEnd func()) *gotick.TickServer {
	url := "redis://localhost:6379/0"
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)

	t := gotick.NewTickServer(gotick.Options{RedisURL: url, DelayedQueue: store.NewAsynq(redisClient)})
	flow := t.Flow(name)

	l := func(format string, args ...interface{}) {
		log.Printf("["+name+"] "+format, args...)
	}

	flow.
		Then("first", func(ctx context.Context) (gotick.NextStatus, error) {
			l("[%v] first exec at %v", gotick.GetCallId(ctx), time.Now().Sub(start))
			v := gotick.GetCallId(ctx)
			gotick.Store(ctx, "first", v)
			return gotick.Done(), nil
		}).
		Then("wait-for-second", func(ctx context.Context) (gotick.NextStatus, error) {
			l("[%v] wait exec at %v", gotick.GetCallId(ctx), time.Now().Sub(start))
			return gotick.Sleep(2 * time.Second), nil
		}).
		Then("end", func(ctx context.Context) (gotick.NextStatus, error) {
			return gotick.Done(), errors.New("mock end fail")
		}).
		Success(func(ctx context.Context, t gotick.TaskStatus) error {
			l("[%v] success exec at %v, task: %v", gotick.GetCallId(ctx), time.Now().Sub(start), t)
			whenEnd()
			return nil
		}).
		Fail(func(ctx context.Context, t gotick.TaskStatus) error {
			l("[%v] fail exec at %v, task: %v", gotick.GetCallId(ctx), time.Now().Sub(start), t)
			whenEnd()
			return nil
		})

	return t
}

func TestTickSuccess(t *testing.T) {
	ctx, c := signal.NewContext()

	ti := newTick("default", c)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ti.StartServer(ctx)

		t.Logf("tick end")
	}()

	t.Logf("start")

	start = time.Now()
	callid, err := ti.Trigger(context.Background(), "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("callid: %+v", callid)

	wg.Wait()
}

func TestTickFail(t *testing.T) {
	ctx, c := signal.NewContext()

	ti := newFailTick("fail", c)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ti.StartServer(ctx)

		t.Logf("tick end")
	}()

	t.Logf("start")

	start = time.Now()
	callid, err := ti.Trigger(context.Background(), "fail", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("callid: %+v", callid)

	wg.Wait()
}

func TestTickRestore(t *testing.T) {
	ctx, c := signal.NewContext()
	ctx, c = context.WithTimeout(ctx, 10*time.Second)
	ti := newTick("default", c)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ti.StartServer(ctx)

		t.Logf("tick end")
	}()

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
	ts := []*gotick.TickServer{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ti := newTick("tick-"+strconv.Itoa(i), func() {

			})
			ts = append(ts, ti)
			ti.StartServer(ctx)
		}(i)
	}

	time.Sleep(time.Second / 10)

	// 随机选取一个tick触发
	ti := ts[rand.Intn(len(ts))-1]
	callid, err := ti.Trigger(context.Background(), "demo", nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("callid: %+v", callid)

	wg.Wait()
}

func TestNewStoreProduct(t *testing.T) {
	opt, err := redis.ParseURL("redis://localhost:6379/0")
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)
	st := gotick.NewKvStoreProduct(store.NewRedisStore(redisClient))
	s := st.New("12312312312312312312342342342343242335623523r12312")
	err = s.SetNodeStatus("test", gotick.TaskStatus{
		Status:     "fail",
		RunAt:      time.Time{},
		Errs:       nil,
		RetryCount: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	s = st.New("123")

	n, exist, err := s.GetNodeStatus("test")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, true, exist)
	assert.Equal(t, "fail", n.Status)

	n, exist, err = s.GetNodeStatus("test3")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, false, exist)
	assert.Equal(t, "", n.Status)
}

func TestTick(t *testing.T) {
	tick := gotick.NewTickServer(gotick.Options{RedisURL: "redis://localhost:6379/0"})

	// Define a flow named "demo"
	flow := tick.Flow("demo")
	var start = time.Now()

	ctx, c := context.WithCancel(context.Background())

	flow.
		// Use Then to add a task
		Then("first", func(ctx context.Context) (gotick.NextStatus, error) {
			t.Logf("first exec at %v", time.Now().Sub(start))
			v := gotick.GetCallId(ctx)
			gotick.Store(ctx, "first", v)
			return gotick.Done(), nil
		}).
		Then("wait-for-second", func(ctx context.Context) (gotick.NextStatus, error) {
			t.Logf("wait-for-second exec at %v", time.Now().Sub(start))
			return gotick.Sleep(2 * time.Second), nil
		}).
		Then("end", func(ctx context.Context) (gotick.NextStatus, error) {
			t.Logf("end exec at %v", time.Now().Sub(start))
			return gotick.Done(), nil
		}).
		// Use Success or Fail to add a callback
		Success(func(ctx context.Context, s gotick.TaskStatus) error {
			t.Logf("success exec at %v, task: %v", time.Now().Sub(start), s)
			// Call the cancel function to stop the tick server
			c()
			return nil
		}).
		Fail(func(ctx context.Context, s gotick.TaskStatus) error {
			t.Logf("fail exec at %v, task: %v", time.Now().Sub(start), s)
			c()
			return nil
		})

	// Trigger one flow
	callId, err := tick.Trigger(context.Background(), "demo", nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("callId: %+v", callId)

	// Start the tick server
	// The tick server will run until the context is canceled
	e := tick.StartServer(ctx)
	if e != nil {
		t.Fatal(e)
	}
}
