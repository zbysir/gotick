package tick

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/zbysir/ticktick/internal/pkg/signal"
	"github.com/zbysir/ticktick/internal/store"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var start = time.Now()

func newTick(name string, whenEnd func()) *Tick {
	url := "redis://localhost:6379/0"
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)

	t := NewTick(Options{RedisURL: url, DelayedQueue: store.NewAsynq(redisClient)})
	flow := t.Flow("demo")

	l := func(format string, args ...interface{}) {
		log.Printf("["+name+"] "+format, args...)
	}

	flow.
		Then("first", func(ctx context.Context) (NextStatus, error) {
			l("[%v] first exec at %v", GetCallId(ctx), time.Now().Sub(start))
			v := GetCallId(ctx)
			Store(ctx, "first", v)
			return Done(), nil
		}).
		Then("wait-for-second", func(ctx context.Context) (NextStatus, error) {
			l("[%v] wait exec at %v", GetCallId(ctx), time.Now().Sub(start))
			return Sleep(2 * time.Second), nil
		}).
		Then("end", func(ctx context.Context) (NextStatus, error) {
			l("[%v] end exec at %v, data: %v", GetCallId(ctx), time.Now().Sub(start), GetMetaData(ctx))
			return Done(), nil
		}).
		Success(func(ctx context.Context, t TaskStatus) error {
			l("[%v] success exec at %v, task: %v", GetCallId(ctx), time.Now().Sub(start), t)
			whenEnd()
			return nil
		}).
		Fail(func(ctx context.Context, t TaskStatus) error {
			l("[%v] fail exec at %v, task: %v", GetCallId(ctx), time.Now().Sub(start), t)
			return nil
		})

	return t
}
func newFailTick(name string, whenEnd func()) *Tick {
	url := "redis://localhost:6379/0"
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)

	t := NewTick(Options{RedisURL: url, DelayedQueue: store.NewAsynq(redisClient)})
	flow := t.Flow(name)

	l := func(format string, args ...interface{}) {
		log.Printf("["+name+"] "+format, args...)
	}

	flow.
		Then("first", func(ctx context.Context) (NextStatus, error) {
			l("[%v] first exec at %v", GetCallId(ctx), time.Now().Sub(start))
			v := GetCallId(ctx)
			Store(ctx, "first", v)
			return Done(), nil
		}).
		Then("wait-for-second", func(ctx context.Context) (NextStatus, error) {
			l("[%v] wait exec at %v", GetCallId(ctx), time.Now().Sub(start))
			return Sleep(2 * time.Second), nil
		}).
		Then("end", func(ctx context.Context) (NextStatus, error) {
			return Done(), errors.New("mock end fail")
		}).
		Success(func(ctx context.Context, t TaskStatus) error {
			l("[%v] success exec at %v, task: %v", GetCallId(ctx), time.Now().Sub(start), t)
			whenEnd()
			return nil
		}).
		Fail(func(ctx context.Context, t TaskStatus) error {
			l("[%v] fail exec at %v, task: %v", GetCallId(ctx), time.Now().Sub(start), t)
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
		ti.Start(ctx)

		t.Logf("tick end")
	}()

	t.Logf("start")

	start = time.Now()
	callid, err := ti.Trigger(context.Background(), "demo", nil)
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
		ti.Start(ctx)

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
	ts := []*Tick{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ti := newTick("tick-"+strconv.Itoa(i), func() {

			})
			ts = append(ts, ti)
			ti.Start(ctx)
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
	st := NewKvStoreProduct(store.NewRedisStore(redisClient))
	s := st.New("12312312312312312312342342342343242335623523r12312")
	err = s.SetNodeStatus("test", TaskStatus{
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
