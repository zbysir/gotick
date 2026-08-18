package store

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// MockKvStore 是 KVStore 的内存实现。
//
// 它刻意存储 JSON 编码后的字节，而不是 Go 值——因为 Redis 就是这么存的。
// 上一版用 reflect 直接塞 Go 值，导致 mock 和 RedisStore 的行为并不一致
// （比如 HGetAll 返回的编码形式不同），测试因此测不到真实语义。
// 一个说谎的 mock 比没有 mock 更糟。
type MockKvStore struct {
	lock  sync.Mutex
	m     map[string]lifeValue
	hm    map[string]map[string][]byte
	hLive map[string]time.Time // hash 表的过期时间，零值表示不过期
	zs    map[string]map[string]float64
}

type lifeValue struct {
	live  time.Time // 零值表示永不过期
	value []byte
}

func (l lifeValue) expired(now time.Time) bool {
	return !l.live.IsZero() && l.live.Before(now)
}

func NewMockKvStore() *MockKvStore {
	return &MockKvStore{
		m:     map[string]lifeValue{},
		hm:    map[string]map[string][]byte{},
		hLive: map[string]time.Time{},
		zs:    map[string]map[string]float64{},
	}
}

var _ KVStore = (*MockKvStore)(nil)

func (m *MockKvStore) Get(ctx context.Context, key string, r interface{}) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	v, ok := m.m[key]
	if !ok || v.expired(time.Now()) {
		return false, nil
	}

	return true, json.Unmarshal(v.value, r)
}

func (m *MockKvStore) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	bs, err := json.Marshal(value)
	if err != nil {
		return err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	var live time.Time
	if expiration > 0 {
		live = time.Now().Add(expiration)
	}
	m.m[key] = lifeValue{live: live, value: bs}
	return nil
}

func (m *MockKvStore) HGet(ctx context.Context, table string, key string, r interface{}) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.hashExpired(table) {
		return false, nil
	}

	v, ok := m.hm[table][key]
	if !ok {
		return false, nil
	}

	return true, json.Unmarshal(v, r)
}

func (m *MockKvStore) HGetAll(ctx context.Context, table string) (map[string]string, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	tbl, ok := m.hm[table]
	if !ok || m.hashExpired(table) {
		// Redis 对不存在的 key 返回空 map 而不是 nil，这里保持一致
		return map[string]string{}, true, nil
	}

	out := make(map[string]string, len(tbl))
	for k, v := range tbl {
		out[k] = string(v)
	}
	return out, true, nil
}

func (m *MockKvStore) HSet(ctx context.Context, table string, key string, value interface{}) error {
	bs, err := json.Marshal(value)
	if err != nil {
		return err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.hm[table] == nil {
		m.hm[table] = map[string][]byte{}
	}
	m.hm[table][key] = bs
	return nil
}

func (m *MockKvStore) HDelIf(ctx context.Context, table string, key string, expect string) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	cur, exists := m.hm[table][key]
	if !exists || string(cur) != expect {
		return false, nil
	}
	delete(m.hm[table], key)
	return true, nil
}

func (m *MockKvStore) HSetCAS(ctx context.Context, table string, key string, expect *string, value interface{}) (bool, error) {
	bs, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	cur, exists := m.hm[table][key]

	if expect == nil {
		if exists {
			return false, nil
		}
	} else {
		if !exists || string(cur) != *expect {
			return false, nil
		}
	}

	if m.hm[table] == nil {
		m.hm[table] = map[string][]byte{}
	}
	m.hm[table][key] = bs
	return true, nil
}

func (m *MockKvStore) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if v, ok := m.m[key]; ok && !v.expired(time.Now()) {
		return false, nil
	}

	var live time.Time
	if ttl > 0 {
		live = time.Now().Add(ttl)
	}
	m.m[key] = lifeValue{live: live, value: []byte(value)}
	return true, nil
}

func (m *MockKvStore) ExpireIf(ctx context.Context, key string, expect string, ttl time.Duration) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	v, ok := m.m[key]
	if !ok || v.expired(time.Now()) || string(v.value) != expect {
		return false, nil
	}

	var live time.Time
	if ttl > 0 {
		live = time.Now().Add(ttl)
	}
	m.m[key] = lifeValue{live: live, value: v.value}
	return true, nil
}

func (m *MockKvStore) DeleteIf(ctx context.Context, key string, expect string) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	v, ok := m.m[key]
	if !ok || v.expired(time.Now()) || string(v.value) != expect {
		return false, nil
	}

	delete(m.m, key)
	return true, nil
}

// hashLife 记录 hash 表的过期时间。Redis 的过期是整个 key 级别的，这里保持一致。
func (m *MockKvStore) Expire(ctx context.Context, key string, ttl time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	var live time.Time
	if ttl > 0 {
		live = time.Now().Add(ttl)
	}

	if v, ok := m.m[key]; ok {
		m.m[key] = lifeValue{live: live, value: v.value}
	}
	if _, ok := m.hm[key]; ok {
		m.hLive[key] = live
	}
	return nil
}

func (m *MockKvStore) Delete(ctx context.Context, key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.m, key)
	delete(m.hm, key)
	delete(m.hLive, key)
	delete(m.zs, key)
	return nil
}

func (m *MockKvStore) ZAdd(ctx context.Context, key string, member string, score float64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.zs[key] == nil {
		m.zs[key] = map[string]float64{}
	}
	m.zs[key][member] = score
	return nil
}

func (m *MockKvStore) ZRevRange(ctx context.Context, key string, offset, count int64) ([]ZMember, error) {
	if count <= 0 {
		return nil, nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	all := make([]ZMember, 0, len(m.zs[key]))
	for member, score := range m.zs[key] {
		all = append(all, ZMember{Member: member, Score: score})
	}

	// score 从大到小；score 相同时按成员名排序，保证结果稳定
	sort.Slice(all, func(i, j int) bool {
		if all[i].Score != all[j].Score {
			return all[i].Score > all[j].Score
		}
		return all[i].Member < all[j].Member
	})

	if offset >= int64(len(all)) {
		return nil, nil
	}
	end := offset + count
	if end > int64(len(all)) {
		end = int64(len(all))
	}
	return all[offset:end], nil
}

func (m *MockKvStore) ZCard(ctx context.Context, key string) (int64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	return int64(len(m.zs[key])), nil
}

func (m *MockKvStore) ZRemBelow(ctx context.Context, key string, max float64) (int64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	var removed int64
	for member, score := range m.zs[key] {
		if score < max {
			delete(m.zs[key], member)
			removed++
		}
	}
	return removed, nil
}

// hashExpired 调用方必须已经持有锁。
func (m *MockKvStore) hashExpired(table string) bool {
	live, ok := m.hLive[table]
	return ok && !live.IsZero() && live.Before(time.Now())
}
