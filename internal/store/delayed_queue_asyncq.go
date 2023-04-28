package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	"time"
)

type Asynq struct {
	//srv *asynq.Server
	cli *asynq.Client

	redisCli redis.UniversalClient

	// topic => callback
	callback map[string][]func(ctx context.Context, task *asynq.Task) error
}

func (a *Asynq) Start(ctx context.Context) error {
	queues := map[string]int{}
	for k := range a.callback {
		queues[k] = 1
	}

	cli := &RawRedisClient{a.redisCli}
	srv := asynq.NewServer(
		cli,
		asynq.Config{
			Concurrency:              10,
			DelayedTaskCheckInterval: time.Millisecond * 10,
			Queues:                   queues,
		},
	)

	err := srv.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
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

func (a *Asynq) Publish(ctx context.Context, topic string, data []byte, delay time.Duration) error {
	// log.Printf("Asynq Publish, taks: %v at %s", data, time.Now().Add(delay))

	_, err := a.cli.Enqueue(asynq.NewTask(topic, data), asynq.ProcessAt(time.Now().Add(delay)), asynq.Queue(topic))
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

func NewAsynq(redisCli redis.UniversalClient) *Asynq {
	cli := &RawRedisClient{redisCli}

	client := asynq.NewClient(cli)

	return &Asynq{
		//srv:      srv,
		cli:      client,
		redisCli: redisCli,
		callback: map[string][]func(ctx context.Context, task *asynq.Task) error{},
	}
}
