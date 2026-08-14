package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// DefaultQueue 是所有 flow 共用的队列命名空间。
const DefaultQueue = "gotick"

// ErrUnknownFlow 表示任务落到了没有注册这个 flow 的 worker 上。
//
// 它不是真的失败：事件是好的，只是来错了地方，需要尽快换一个 worker 重投。
var ErrUnknownFlow = errors.New("gotick: no flow registered for this task on this worker")

// unknownFlowRetryDelay 是「来错 worker」的重投间隔。
//
// 不能用默认的指数退避：那是分钟级的，而正确的 worker 可能就在旁边，
// 流程会毫无理由地卡上几分钟。
const unknownFlowRetryDelay = 200 * time.Millisecond

// criticalSuffix 标记优先调度的队列。
//
// 已经开始的流程比新触发的流程优先，这样在积压时它们能尽快跑完让出资源，
// 而不是所有流程一起龟速前进。
const criticalSuffix = "_critical"

// AsynqOptions 是 gotick 自己的队列配置，和 asynq.Config 分开放。
type AsynqOptions struct {
	// Queue 队列命名空间，默认 DefaultQueue。
	//
	// 所有 flow 共用 {Queue} 和 {Queue}_critical 两个队列，靠任务自带的 flowId 分发。
	// 早期实现是一个 flow 一对队列，于是队列数随 flow 数线性增长，
	// 而消息队列的轮询要遍历所有队列——50 个 flow 就是 100 个队列，
	// 完全空闲时也在持续刷 Redis。
	//
	// 需要把某个重负载的 flow 和其他流程隔开时，给它单独起一个 Server 并换个命名空间。
	Queue string

	// OwnsClient 表示 redis 连接是调用方专门为这个队列创建的，关停时可以连同关闭。
	// 传入应用共享的连接时必须为 false。
	OwnsClient bool

	// ConsumeLegacyPerFlowQueues 让 worker 额外消费旧的「一个 flow 一个队列」。
	//
	// 从旧版本升级时打开它，等旧队列里的任务排空之后再关掉。
	// 不打开的话，升级前还留在旧队列里的任务不会有任何人处理。
	ConsumeLegacyPerFlowQueues bool
}

type Asynq struct {
	cfg asynq.Config
	opt AsynqOptions
	cli *asynq.Client

	redisCli redis.UniversalClient

	mu sync.RWMutex
	// callback 按 flowId 分发。注册发生在 Start 之前，但读发生在 handler 里，
	// 所以要加锁。
	callback map[string][]func(ctx context.Context, task *asynq.Task) error
	started  bool
	// warned 记录已经报过警的 flow，避免刷屏。
	warned map[string]bool

	// onUnknownTopic 收到没有注册的 topic 时调用。
	// 返回 nil 表示调用方已经接管（本条会被 ack），返回错误则走正常重试。
	onUnknownTopic func(ctx context.Context, topic string, data []byte) error
}

// SetUnknownTopicHandler 设置「收到没注册的 flow」时的处理方式。
//
// 默认行为是返回错误让消息队列重投，但那会消耗重试预算：
// 命中率低的时候（比如两个不相干的服务共用了命名空间）事件会重试耗尽后被归档，
// 流程就永久卡住了。上层用这个钩子改成「换个信封重新投递」，不吃重试预算。
func (a *Asynq) SetUnknownTopicHandler(h func(ctx context.Context, topic string, data []byte) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onUnknownTopic = h
}

func (a *Asynq) queue() string {
	if a.opt.Queue != "" {
		return a.opt.Queue
	}
	return DefaultQueue
}

func (a *Asynq) criticalQueue() string { return a.queue() + criticalSuffix }

func (a *Asynq) Start(ctx context.Context) error {
	// 所有 flow 共用这两个队列，不再随 flow 数量增长。
	queues := map[string]int{
		a.queue():         1,
		a.criticalQueue(): 9,
	}

	a.mu.Lock()
	a.started = true
	if a.opt.ConsumeLegacyPerFlowQueues {
		for topic := range a.callback {
			if topic == a.queue() {
				continue
			}
			queues[topic] = 1
			queues[topic+criticalSuffix] = 9
		}
	}
	a.mu.Unlock()

	a.cfg.Queues = queues

	if a.cfg.Concurrency == 0 {
		a.cfg.Concurrency = 10
	}
	if a.cfg.TaskCheckInterval == 0 {
		a.cfg.TaskCheckInterval = time.Millisecond * 100
	}
	if a.cfg.DelayedTaskCheckInterval == 0 {
		// asynq 这一项的默认值是 5 秒，而它决定了「已排期」的任务多久才被搬到
		// 「待执行」——也就是每一个 Sleep、每一次重试退避的实际精度。
		// 用默认值的话，睡 2 秒实际要等 4 秒多，而且这个延迟对用户完全不可见。
		//
		// 注意它和 TaskCheckInterval 不是一回事：后者只管队列空时多久查一次新任务。
		a.cfg.DelayedTaskCheckInterval = 500 * time.Millisecond
	}

	// 「来错 worker」要快速重投，其余错误保持调用方或 asynq 的退避策略。
	userRetryDelay := a.cfg.RetryDelayFunc
	a.cfg.RetryDelayFunc = func(n int, err error, task *asynq.Task) time.Duration {
		if errors.Is(err, ErrUnknownFlow) {
			return unknownFlowRetryDelay
		}
		if userRetryDelay != nil {
			return userRetryDelay(n, err, task)
		}
		return asynq.DefaultRetryDelayFunc(n, err, task)
	}

	srv := asynq.NewServer(
		&RawRedisClient{c: a.redisCli, keepOpen: !a.opt.OwnsClient},
		a.cfg,
	)

	err := srv.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		a.mu.RLock()
		handlers := a.callback[task.Type()]
		onUnknown := a.onUnknownTopic
		a.mu.RUnlock()

		if len(handlers) == 0 {
			a.warnUnknownFlow(task.Type())

			// 交给上层重新投递。这样不消耗重试预算——
			// 靠 asynq 的重试来「弹」到正确的 worker，在命中率低时
			// 会重试耗尽然后归档，把事件彻底丢掉。
			if onUnknown != nil {
				return onUnknown(ctx, task.Type(), task.Payload())
			}

			// 共用队列意味着这个 worker 会收到它没有注册的 flow 的事件。
			//
			// 必须返回错误而不是默默 ack：ack 等于把别人的事件吞掉，
			// 那个流程就再也不会往下走了。返回错误会让它被快速重投，
			// 落到真正注册了这个 flow 的 worker 上。
			//
			// 没有钩子时退回到「报错重投」。
			return fmt.Errorf("%w: %q", ErrUnknownFlow, task.Type())
		}

		for _, c := range handlers {
			if err := c(ctx, task); err != nil {
				return err
			}
		}

		return nil
	}))
	if err != nil {
		return err
	}

	<-ctx.Done()

	srv.Shutdown()

	if a.opt.OwnsClient {
		_ = a.cli.Close()
	}

	return nil
}

type Option struct {
	Critical bool // If true, it is scheduled first.
}

func (a *Asynq) Publish(ctx context.Context, topic string, data []byte, delay time.Duration, opt Option) error {
	queueName := a.queue()
	if opt.Critical {
		queueName = a.criticalQueue()
	}

	// 任务的 type 就是 flowId，消费端靠它分发——所以队列不需要按 flow 分。
	// 这也让 Client 不必知道任何 flow 的信息就能触发流程。
	_, err := a.cli.EnqueueContext(ctx, asynq.NewTask(topic, data),
		asynq.ProcessAt(time.Now().Add(delay)),
		asynq.Queue(queueName),
	)
	return err
}

func (a *Asynq) Subscribe(topic string, h func(ctx context.Context, data []byte) error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 启动之后再注册是允许的：队列集合是固定的，handler 在分发时才读这张表。
	// 早期实现按 flow 建队列，队列集合在 Start 时就定死了，
	// 于是启动后注册的 flow 永远收不到任何事件——共用队列顺带消除了这个问题。
	//
	// 唯一的例外是旧队列兼容模式，那批队列名确实要在 Start 时就知道。
	if a.started && a.opt.ConsumeLegacyPerFlowQueues {
		log.Printf("[gotick] flow %q registered after start; its legacy per-flow queue will not be consumed", topic)
	}

	a.callback[topic] = append(a.callback[topic], func(ctx context.Context, task *asynq.Task) error {
		return h(ctx, task.Payload())
	})
}

// warnUnknownFlow 每个 flow 只报一次，让配置错误在日志里立刻可见，又不至于刷屏。
func (a *Asynq) warnUnknownFlow(topic string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.warned[topic] {
		return
	}
	a.warned[topic] = true
	log.Printf("[gotick] flow %q is not registered on this worker; its events are being handed to "+
		"another worker. If this keeps happening, check that unrelated services are not sharing "+
		"the same queue namespace.", topic)
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
func NewAsynq(redisCli redis.UniversalClient, cfg asynq.Config, opt AsynqOptions) *Asynq {
	return &Asynq{
		cfg:      cfg,
		opt:      opt,
		cli:      asynq.NewClient(&RawRedisClient{c: redisCli, keepOpen: !opt.OwnsClient}),
		redisCli: redisCli,
		callback: map[string][]func(ctx context.Context, task *asynq.Task) error{},
		warned:   map[string]bool{},
	}
}
