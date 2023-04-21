package store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"strconv"
	"sync"
	"time"
)

// ZRANGEBYSCORE
// https://cloud.tencent.com/developer/article/1358266
// https://cloud.tencent.com/developer/article/1650069?from=article.detail.1358266
// https://cloud.tencent.com/developer/article/1650054?from=article.detail.1358266

// https://github.com/hibiken/asynq/blob/master/internal/rdb/rdb.go
// https://blog.bysir.top/blogs/boom_asyncq/

type StdRedisDelayedQueue struct {
	redis     *redis.Client
	callbacks map[string][]func(ctx context.Context, data string) error
	runOnce   sync.Once
}

var _ DelayedQueue = (*StdRedisDelayedQueue)(nil)

func NewStdRedisDelayedQueue(redis *redis.Client) *StdRedisDelayedQueue {
	return &StdRedisDelayedQueue{redis: redis, callbacks: make(map[string][]func(ctx context.Context, data string) error)}
}

func (r *StdRedisDelayedQueue) Publish(ctx context.Context, topic string, data string, delay time.Duration) error {
	return r.redis.ZAdd(context.Background(), topic, &redis.Z{
		Score:  float64(time.Now().Add(delay).Unix()),
		Member: data,
	}).Err()
}

func (r *StdRedisDelayedQueue) Exist(uniqueKey []string) (map[string]bool, error) {
	var exist = make(map[string]bool)
	for _, c := range uniqueKey {
		x, err := r.redis.ZScore(context.Background(), "xx", c).Result()
		if err != nil {
			return exist, err
		}
		if x != 0 {
			exist[c] = true
		}
	}

	return exist, nil
}

func (r *StdRedisDelayedQueue) Subscribe(topic string, h func(ctx context.Context, data string) error) {
	r.callbacks[topic] = append(r.callbacks[topic], h)
}

func (r *StdRedisDelayedQueue) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		for topic, callback := range r.callbacks {
			members, err := r.redis.ZRangeByScore(context.Background(), topic, &redis.ZRangeBy{
				Min:    "",
				Max:    strconv.FormatInt(time.Now().Unix(), 10),
				Offset: 0,
				Count:  100,
			}).Result()
			if err != nil {
				time.Sleep(1 * time.Second)
				log.Printf("ERROR: %v", err)
				continue
			}

			for _, v := range members {
				// 删除成功才能消费
				i, err := r.redis.ZRem(context.Background(), topic, v).Result()
				if err != nil {
					log.Printf("ERROR: %v", err)
					continue
				}
				if i == 0 {
					continue
				}
				for _, c := range callback {
					if err := c(context.Background(), v); err != nil {
						log.Printf("callbacks ERROR: %v", err)
						// 如果执行失败，重新放入队列
						// TODO 这个操作不是原子性的，可能会导致数据丢失
						e := r.Publish(context.Background(), topic, v, 0)
						if e != nil {
							log.Printf("requeue ERROR: %v", err)
						}
					}
				}
			}
		}

		time.Sleep(time.Millisecond * 100)
	}

	return nil
}
