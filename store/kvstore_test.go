package store

import (
	"context"
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

	require.NoError(t, r.HSet(ctx, "tbl", "a", "value-a", 0))
	require.NoError(t, r.HSet(ctx, "tbl", "b", map[string]int{"n": 1}, 0))

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
// 这条测试存在的理由：同一个文件里的 HSet 收下 expiration 参数后直接丢掉，
// 而调度器的心跳与死亡检测全押在那个 TTL 上。见 TestKVStoreTTLContract。
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

			require.NoError(t, s.HSet(ctx, "t", "f", "hv", 0))
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
