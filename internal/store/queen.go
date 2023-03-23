package store

import (
	"context"
	"github.com/redis/go-redis/v9"
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
// - 使用有序集合存放 pendding 的任务
// - 定时将 pendding 的任务放入 ready 集合，(func ForwardIfReady)
//  - 	ZRANGEBYSCORE,ZREM
// - 从 ready 集合中取出任务

type AsyncQueen interface {
	// Publish 当 data 相同时后面 Publish 的数据会覆盖前面的数据
	Publish(data string, delay time.Duration) error
	Exist(data []string) (map[string]bool, error)
	Subscribe(h func(data string) error)
	Start(ctx context.Context)
}

type RedisAsyncQueen struct {
	redis     *redis.Client
	callbacks []func(data string) error
	queenKey  string
	runOnce   sync.Once
}

var _ AsyncQueen = (*RedisAsyncQueen)(nil)

func NewRedisAsyncQueen(redis *redis.Client, queenKey string) *RedisAsyncQueen {
	return &RedisAsyncQueen{redis: redis, queenKey: queenKey}
}

func (r *RedisAsyncQueen) Publish(data string, delay time.Duration) error {
	return r.redis.ZAdd(context.Background(), r.queenKey, redis.Z{
		Score:  float64(time.Now().Add(delay).Unix()),
		Member: data,
	}).Err()
}

func (r *RedisAsyncQueen) Exist(uniqueKey []string) (map[string]bool, error) {
	var exist = make(map[string]bool)
	for _, c := range uniqueKey {
		x, err := r.redis.ZScore(context.Background(), r.queenKey, c).Result()
		if err != nil {
			return exist, err
		}
		if x != 0 {
			exist[c] = true
		}
	}

	return exist, nil
}

func (r *RedisAsyncQueen) Subscribe(h func(data string) error) {
	r.callbacks = append(r.callbacks, h)

}

func (r *RedisAsyncQueen) Start(ctx context.Context) {
	r.runOnce.Do(func() {
		go func() {
			for {
				members, err := r.redis.ZRangeByScore(context.Background(), r.queenKey, &redis.ZRangeBy{
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
					i, err := r.redis.ZRem(context.Background(), r.queenKey, v).Result()
					if err != nil {
						log.Printf("ERROR: %v", err)
						continue
					}
					if i == 0 {
						continue
					}
					for _, c := range r.callbacks {
						if err := c(v); err != nil {
							log.Printf("callbacks ERROR: %v", err)
							// 如果执行失败，重新放入队列
							// TODO 这个操作不是原子性的，可能会导致数据丢失
							e := r.Publish(v, 0)
							if e != nil {
								log.Printf("requeen ERROR: %v", err)
							}
						}
					}
				}
				time.Sleep(time.Millisecond * 100)
			}
		}()
	})
}
