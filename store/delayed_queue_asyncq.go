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

	// ownsClient 表示这个 redis 连接是我们自己创建的，关闭时应该由我们负责。
	// 调用方传进来的共享连接绝不能关——那是别人的连接，
	// 关掉它会让用户的整个应用失去 Redis。
	ownsClient bool

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
		&RawRedisClient{c: a.redisCli, keepOpen: !a.ownsClient},
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

	if a.ownsClient {
		_ = a.cli.Close()
	}

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
	// keepOpen 为 true 时交给 asynq 一个屏蔽了 Close 的包装。
	keepOpen bool
}

func (r *RawRedisClient) MakeRedisClient() interface{} {
	if r.keepOpen {
		return nonClosingClient{r.c}
	}
	return r.c
}

// nonClosingClient 屏蔽 Close()。
//
// asynq 的 Client.Close 和 Server.Shutdown 都会关闭它拿到的 redis 连接。
// 当这个连接是调用方共享进来的（Config.RedisClient），
// 关停 gotick 就会顺手关掉用户整个应用的 Redis 连接。
type nonClosingClient struct {
	redis.UniversalClient
}

func (nonClosingClient) Close() error { return nil }

// NewAsynq 创建延时队列。
//
// ownsClient 表示 redisCli 是调用方专门为这个队列创建的，
// 关停时可以连同关闭。传入应用共享的连接时必须为 false。
func NewAsynq(redisCli redis.UniversalClient, opt asynq.Config, ownsClient bool) *Asynq {
	return &Asynq{
		opt:        opt,
		cli:        asynq.NewClient(&RawRedisClient{c: redisCli, keepOpen: !ownsClient}),
		redisCli:   redisCli,
		ownsClient: ownsClient,
		callback:   map[string][]func(ctx context.Context, task *asynq.Task) error{},
	}
}
