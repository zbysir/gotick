package store

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type Asynq struct {
	opt asynq.Config
	cli *asynq.Client

	redisCli redis.UniversalClient

	// topic => callback
	callback map[string][]func(ctx context.Context, task *asynq.Task) error
}

func (a *Asynq) Start(ctx context.Context) error {
	queues := map[string]int{}

	// 只监听注册了的 topic
	for k := range a.callback {
		queues[k] = 1
		queues[k+"_critical"] = 9
	}

	a.opt.Queues = queues

	if a.opt.Concurrency == 0 {
		a.opt.Concurrency = 10
	}
	if a.opt.TaskCheckInterval == 0 {
		a.opt.TaskCheckInterval = time.Millisecond * 100
	}

	srv := asynq.NewServer(
		&RawRedisClient{c: a.redisCli},
		a.opt,
	)

	err := srv.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		// log.Printf("[gotick] ------ call: queue: %v ------", task.Type())
		for _, c := range a.callback[task.Type()] {
			err := c(ctx, task)
			if err != nil {
				return err
			}
		}

		return nil
	}))
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
	}

	srv.Shutdown()
	_ = a.cli.Close()

	return nil
}

type Option struct {
	Critical bool // If true, it is scheduled first.
}

func (a *Asynq) Publish(ctx context.Context, topic string, data []byte, delay time.Duration, opt Option) error {
	queueName := topic
	if opt.Critical {
		queueName = queueName + "_critical"
	}
	// log.Printf("[gotick] ------ publish: %v, queue: %v, runat: %v ------", topic, queueName, time.Now().Add(delay))
	_, err := a.cli.EnqueueContext(ctx, asynq.NewTask(topic, data),
		asynq.ProcessAt(time.Now().Add(delay)),
		asynq.Queue(queueName),
	)
	if err != nil {
		return err
	}

	return nil
}

func (a *Asynq) Subscribe(topic string, h func(ctx context.Context, data []byte) error) {
	a.callback[topic] = append(a.callback[topic], func(ctx context.Context, task *asynq.Task) error {
		return h(context.TODO(), task.Payload())
	})
}

var _ DelayedQueue = (*Asynq)(nil)

type RawRedisClient struct {
	c redis.UniversalClient
}

func (r *RawRedisClient) MakeRedisClient() interface{} {
	return r.c
}

func NewAsynq(redisCli redis.UniversalClient, opt asynq.Config) *Asynq {
	client := asynq.NewClient(&RawRedisClient{c: redisCli})

	return &Asynq{
		opt:      opt,
		cli:      client,
		redisCli: redisCli,
		callback: map[string][]func(ctx context.Context, task *asynq.Task) error{},
	}
}
