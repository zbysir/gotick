package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	"time"
)

type Asynq struct {
	srv *asynq.Server
	cli *asynq.Client

	// topic => callback
	callback map[string][]func(ctx context.Context, task *asynq.Task) error
}

func (a *Asynq) Start(ctx context.Context) error {
	err := a.srv.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
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

	a.srv.Shutdown()
	_ = a.cli.Close()

	return nil
}

func (a *Asynq) Publish(ctx context.Context, topic string, data string, delay time.Duration) error {
	//log.Printf("Asynq Publish, taks: %v at %s", data, time.Now().Add(delay))

	_, err := a.cli.Enqueue(asynq.NewTask(topic, []byte(data)), asynq.ProcessAt(time.Now().Add(delay)))
	if err != nil {
		return err
	}

	return nil
}

func (a *Asynq) Subscribe(topic string, h func(ctx context.Context, data string) error) {
	a.callback[topic] = append(a.callback[topic], func(ctx context.Context, task *asynq.Task) error {
		return h(context.TODO(), string(task.Payload()))
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
	srv := asynq.NewServer(
		cli,
		asynq.Config{
			Concurrency:              10,
			DelayedTaskCheckInterval: time.Millisecond * 10,
		},
	)

	client := asynq.NewClient(cli)

	return &Asynq{
		srv:      srv,
		cli:      client,
		callback: map[string][]func(ctx context.Context, task *asynq.Task) error{},
	}
}
