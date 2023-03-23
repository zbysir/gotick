package store

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"time"
)

type KVStore interface {
	Get(ctx context.Context, key string, r interface{}) (bool, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	HGet(ctx context.Context, table string, key string, r interface{}) (bool, error)
	HSet(ctx context.Context, table string, key string, value interface{}, expiration time.Duration) error
	HClear(ctx context.Context, table string) error
}

type WithPrefix struct {
	prefix string
	store  KVStore
}

func (w *WithPrefix) HGet(ctx context.Context, table string, key string, r interface{}) (bool, error) {
	return w.store.HGet(ctx, w.prefix+table, key, r)
}

func (w *WithPrefix) HSet(ctx context.Context, table string, key string, value interface{}, expiration time.Duration) error {
	return w.store.HSet(ctx, w.prefix+table, key, value, expiration)
}

func (w *WithPrefix) HClear(ctx context.Context, table string) error {
	return w.store.HClear(ctx, w.prefix+table)
}

func (w *WithPrefix) Get(ctx context.Context, key string, r interface{}) (bool, error) {
	return w.store.Get(ctx, w.prefix+key, r)
}

func (w *WithPrefix) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return w.store.Set(ctx, w.prefix+key, value, expiration)
}

func NewWithPrefix(prefix string, store KVStore) *WithPrefix {
	return &WithPrefix{
		prefix: prefix,
		store:  store,
	}
}

type RedisStore struct {
	redis *redis.Client
}

var _ KVStore = (*RedisStore)(nil)

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

func (r *RedisStore) HSet(ctx context.Context, table string, key string, value interface{}, expiration time.Duration) error {
	bs, _ := json.Marshal(value)
	err := r.redis.HSet(ctx, table, key, bs).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) HClear(ctx context.Context, table string) error {
	err := r.redis.Del(ctx, table).Err()
	if err != nil {
		return err
	}
	return nil
}

func NewRedisStore(redis *redis.Client) *RedisStore {
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
	bs, _ := json.Marshal(value)
	err := r.redis.Set(ctx, key, bs, expiration).Err()
	if err != nil {
		return err
	}
	return nil
}
