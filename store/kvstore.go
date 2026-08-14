package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type KVStore interface {
	Get(ctx context.Context, key string, r interface{}) (bool, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	HGet(ctx context.Context, table string, key string, r interface{}) (bool, error)
	HGetAll(ctx context.Context, table string) (map[string]string, bool, error)
	HSet(ctx context.Context, table string, key string, value interface{}) error
	// HSetCAS 只有当字段的当前原始值等于 expect 时才写入 value，返回是否写入成功。
	// expect 为 nil 表示要求该字段当前不存在。
	//
	// 这是并发安全地更新任务状态的唯一手段：读-改-写不是原子的，
	// 多个节点同时调度同一个 callId 时会互相覆盖重试计数和状态。
	HSetCAS(ctx context.Context, table string, key string, expect *string, value interface{}) (bool, error)
	Delete(ctx context.Context, key string) error

	// 下面三个用于实现带自动过期的租约（同一个 callId 同时只允许一个节点重放）。
	// 值是一个随机 token，只有持有者才能续期和释放，
	// 否则一个已经超时的持有者会误删别人刚拿到的租约。

	// SetNX 仅当 key 不存在时写入，返回是否写入成功。
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	// ExpireIf 仅当 key 的当前值等于 expect 时刷新过期时间。
	ExpireIf(ctx context.Context, key string, expect string, ttl time.Duration) (bool, error)
	// DeleteIf 仅当 key 的当前值等于 expect 时删除。
	DeleteIf(ctx context.Context, key string, expect string) (bool, error)
}

var _ KVStore = (*WithPrefix)(nil)

type WithPrefix struct {
	prefix string
	store  KVStore
}

func (w *WithPrefix) HGetAll(ctx context.Context, table string) (map[string]string, bool, error) {
	return w.store.HGetAll(ctx, w.prefix+table)
}

func (w *WithPrefix) HGet(ctx context.Context, table string, key string, r interface{}) (bool, error) {
	return w.store.HGet(ctx, w.prefix+table, key, r)
}

func (w *WithPrefix) HSet(ctx context.Context, table string, key string, value interface{}) error {
	return w.store.HSet(ctx, w.prefix+table, key, value)
}

func (w *WithPrefix) HSetCAS(ctx context.Context, table string, key string, expect *string, value interface{}) (bool, error) {
	return w.store.HSetCAS(ctx, w.prefix+table, key, expect, value)
}

func (w *WithPrefix) Delete(ctx context.Context, table string) error {
	return w.store.Delete(ctx, w.prefix+table)
}

func (w *WithPrefix) Get(ctx context.Context, key string, r interface{}) (bool, error) {
	return w.store.Get(ctx, w.prefix+key, r)
}

func (w *WithPrefix) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return w.store.Set(ctx, w.prefix+key, value, expiration)
}

func (w *WithPrefix) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	return w.store.SetNX(ctx, w.prefix+key, value, ttl)
}

func (w *WithPrefix) ExpireIf(ctx context.Context, key string, expect string, ttl time.Duration) (bool, error) {
	return w.store.ExpireIf(ctx, w.prefix+key, expect, ttl)
}

func (w *WithPrefix) DeleteIf(ctx context.Context, key string, expect string) (bool, error) {
	return w.store.DeleteIf(ctx, w.prefix+key, expect)
}

func NewWithPrefix(prefix string, store KVStore) *WithPrefix {
	return &WithPrefix{
		prefix: prefix,
		store:  store,
	}
}

type RedisStore struct {
	redis redis.UniversalClient
}

var _ KVStore = (*RedisStore)(nil)

// casScript 在服务端原子地完成「比较后写入」。
// ARGV[1]=field, ARGV[2]='0' 表示要求字段不存在 / '1' 表示要求等于 ARGV[3], ARGV[4]=新值
var casScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if ARGV[2] == '0' then
  if cur == false then
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
    return 1
  end
  return 0
end
if cur ~= false and cur == ARGV[3] then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
  return 1
end
return 0
`)

func (r *RedisStore) HGet(ctx context.Context, table string, key string, value interface{}) (bool, error) {
	val, err := r.redis.HGet(ctx, table, key).Bytes()
	if err != nil {
		if redis.Nil == err {
			return false, nil
		}
		return false, err
	}
	err = json.Unmarshal(val, value)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RedisStore) HGetAll(ctx context.Context, table string) (map[string]string, bool, error) {
	val, err := r.redis.HGetAll(ctx, table).Result()
	if err != nil {
		if redis.Nil == err {
			return nil, false, nil
		}
		return nil, false, err
	}

	return val, true, nil
}

func (r *RedisStore) HSet(ctx context.Context, table string, key string, value interface{}) error {
	bs, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.redis.HSet(ctx, table, key, bs).Err()
}

func (r *RedisStore) HSetCAS(ctx context.Context, table string, key string, expect *string, value interface{}) (bool, error) {
	bs, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	mustNotExist := "0"
	expectVal := ""
	if expect != nil {
		mustNotExist = "1"
		expectVal = *expect
	}

	res, err := casScript.Run(ctx, r.redis, []string{table}, key, mustNotExist, expectVal, string(bs)).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (r *RedisStore) Delete(ctx context.Context, table string) error {
	return r.redis.Del(ctx, table).Err()
}

func NewRedisStore(redis redis.UniversalClient) *RedisStore {
	return &RedisStore{
		redis: redis,
	}
}

func (r *RedisStore) Get(ctx context.Context, key string, value interface{}) (bool, error) {
	val, err := r.redis.Get(ctx, key).Bytes()
	if err != nil {
		if redis.Nil == err {
			return false, nil
		}
		return false, err
	}
	err = json.Unmarshal(val, value)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *RedisStore) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	bs, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.redis.Set(ctx, key, bs, expiration).Err()
}

// expireIfScript 只在值匹配时续期，避免一个已经超时的持有者把别人的租约续走。
var expireIfScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

// deleteIfScript 只在值匹配时删除，避免释放掉别人刚拿到的租约。
var deleteIfScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

func (r *RedisStore) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	return r.redis.SetNX(ctx, key, value, ttl).Result()
}

func (r *RedisStore) ExpireIf(ctx context.Context, key string, expect string, ttl time.Duration) (bool, error) {
	res, err := expireIfScript.Run(ctx, r.redis, []string{key}, expect, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (r *RedisStore) DeleteIf(ctx context.Context, key string, expect string) (bool, error) {
	res, err := deleteIfScript.Run(ctx, r.redis, []string{key}, expect).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
