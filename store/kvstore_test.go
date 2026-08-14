package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 这里用 miniredis 而不是 localhost:6379。
// 连本机 Redis 的测试在开发机上一直是绿的、在 CI 上一直是红的，
// 结果就是没人跑它——这个文件之前就是这样。
func newTestRedisStore(t *testing.T) *RedisStore {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	return NewRedisStore(rdb)
}

func TestRedisStoreGetSet(t *testing.T) {
	r := newTestRedisStore(t)
	ctx := context.Background()

	require.NoError(t, r.Set(ctx, "test", "test", 1*time.Hour))

	var v string
	exist, err := r.Get(ctx, "test", &v)
	require.NoError(t, err)
	assert.True(t, exist)
	assert.Equal(t, "test", v)

	exist, err = r.Get(ctx, "missing", &v)
	require.NoError(t, err)
	assert.False(t, exist, "reading a missing key must report not-exist, not an error")
}

func TestRedisStoreHash(t *testing.T) {
	r := newTestRedisStore(t)
	ctx := context.Background()

	require.NoError(t, r.HSet(ctx, "tbl", "a", "value-a"))
	require.NoError(t, r.HSet(ctx, "tbl", "b", map[string]int{"n": 1}))

	var s string
	exist, err := r.HGet(ctx, "tbl", "a", &s)
	require.NoError(t, err)
	assert.True(t, exist)
	assert.Equal(t, "value-a", s)

	exist, err = r.HGet(ctx, "tbl", "missing", &s)
	require.NoError(t, err)
	assert.False(t, exist)

	all, exist, err := r.HGetAll(ctx, "tbl")
	require.NoError(t, err)
	assert.True(t, exist)
	assert.Len(t, all, 2)

	require.NoError(t, r.Delete(ctx, "tbl"))
	all, _, err = r.HGetAll(ctx, "tbl")
	require.NoError(t, err)
	assert.Empty(t, all)
}

// TestRedisStoreSetHonorsTTL 确认 Set 的过期时间是真的生效的。
//
// HSet 现在没有 expiration 参数了：Redis 的 hash field 在 7.4 之前不支持 TTL，
// 而旧接口收下这个参数后直接丢掉，让依赖它的心跳机制从未真正工作过。
// 任务存活判断已经改为把心跳时间戳写进状态，不再依赖存储层的过期。
func TestRedisStoreSetHonorsTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	r := NewRedisStore(rdb)
	ctx := context.Background()

	require.NoError(t, r.Set(ctx, "ephemeral", "v", 100*time.Millisecond))

	var v string
	exist, err := r.Get(ctx, "ephemeral", &v)
	require.NoError(t, err)
	require.True(t, exist, "value should exist before the TTL elapses")

	mr.FastForward(200 * time.Millisecond)

	exist, err = r.Get(ctx, "ephemeral", &v)
	require.NoError(t, err)
	assert.False(t, exist, "value should be gone after the TTL elapses")
}

// TestMockKvStoreMatchesRedis 把 mock 和真实实现放在一起比对。
//
// MockKvStore 曾经实现了 HSet 的 TTL 而 RedisStore 没有，导致所有依赖心跳过期的
// 逻辑「测试全绿、生产全坏」。两个实现的行为必须一致，否则 mock 就是在骗人。
func TestMockKvStoreMatchesRedis(t *testing.T) {
	ctx := context.Background()

	impls := map[string]KVStore{
		"redis": newTestRedisStore(t),
		"mock":  NewMockKvStore(),
	}

	for name, s := range impls {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, s.Set(ctx, "k", "v", 0))
			var v string
			exist, err := s.Get(ctx, "k", &v)
			require.NoError(t, err)
			assert.True(t, exist)
			assert.Equal(t, "v", v)

			require.NoError(t, s.HSet(ctx, "t", "f", "hv"))
			exist, err = s.HGet(ctx, "t", "f", &v)
			require.NoError(t, err)
			assert.True(t, exist)
			assert.Equal(t, "hv", v)

			all, _, err := s.HGetAll(ctx, "t")
			require.NoError(t, err)
			require.Len(t, all, 1)
			// 两个实现都必须返回存储层里的原始 JSON 编码，上层才好统一解码。
			assert.Equal(t, `"hv"`, all["f"],
				"HGetAll must return the stored JSON encoding in both implementations")

			exist, err = s.HGet(ctx, "t", "nope", &v)
			require.NoError(t, err)
			assert.False(t, exist)
		})
	}
}

// TestHSetCAS 是并发安全的核心原语：没有它，多个节点同时更新同一个 task
// 会互相覆盖状态和重试计数。两个实现必须给出完全一样的答案。
func TestHSetCAS(t *testing.T) {
	ctx := context.Background()

	impls := map[string]KVStore{
		"redis": newTestRedisStore(t),
		"mock":  NewMockKvStore(),
	}

	for name, s := range impls {
		t.Run(name, func(t *testing.T) {
			// expect == nil 表示要求字段不存在：第一次成功，第二次必须失败。
			ok, err := s.HSetCAS(ctx, "tbl", "k", nil, "first")
			require.NoError(t, err)
			assert.True(t, ok, "CAS on a missing field should succeed when expecting absence")

			ok, err = s.HSetCAS(ctx, "tbl", "k", nil, "second")
			require.NoError(t, err)
			assert.False(t, ok, "CAS expecting absence must fail once the field exists")

			var got string
			_, err = s.HGet(ctx, "tbl", "k", &got)
			require.NoError(t, err)
			assert.Equal(t, "first", got, "a failed CAS must not modify the value")

			// 拿当前的原始值做 expect，应该成功。
			all, _, err := s.HGetAll(ctx, "tbl")
			require.NoError(t, err)
			cur := all["k"]

			ok, err = s.HSetCAS(ctx, "tbl", "k", &cur, "updated")
			require.NoError(t, err)
			assert.True(t, ok, "CAS with the current raw value should succeed")

			// 用已经过期的 expect，必须失败。
			stale := cur
			ok, err = s.HSetCAS(ctx, "tbl", "k", &stale, "clobbered")
			require.NoError(t, err)
			assert.False(t, ok, "CAS with a stale value must fail")

			_, err = s.HGet(ctx, "tbl", "k", &got)
			require.NoError(t, err)
			assert.Equal(t, "updated", got)

			// 对不存在的字段带 expect，必须失败。
			other := `"whatever"`
			ok, err = s.HSetCAS(ctx, "tbl", "missing", &other, "x")
			require.NoError(t, err)
			assert.False(t, ok, "CAS expecting a value must fail when the field does not exist")
		})
	}
}

// TestHSetCASIsAtomic 让一群 goroutine 用 CAS 抢同一个字段，
// 断言恰好只有一个赢家——这正是「抢占任务执行权」依赖的性质。
func TestHSetCASIsAtomic(t *testing.T) {
	ctx := context.Background()

	impls := map[string]KVStore{
		"redis": newTestRedisStore(t),
		"mock":  NewMockKvStore(),
	}

	for name, s := range impls {
		t.Run(name, func(t *testing.T) {
			const racers = 32

			var (
				wg    sync.WaitGroup
				mu    sync.Mutex
				winrs []int
			)

			start := make(chan struct{})
			for i := 0; i < racers; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					ok, err := s.HSetCAS(ctx, "race", "field", nil, i)
					if err == nil && ok {
						mu.Lock()
						winrs = append(winrs, i)
						mu.Unlock()
					}
				}()
			}

			close(start)
			wg.Wait()

			require.Len(t, winrs, 1, "exactly one racer must win the claim, got %v", winrs)

			var stored int
			exist, err := s.HGet(ctx, "race", "field", &stored)
			require.NoError(t, err)
			require.True(t, exist)
			assert.Equal(t, winrs[0], stored, "the stored value must belong to the winner")
		})
	}
}
