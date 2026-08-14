package store

import (
	"context"
	"encoding/json"
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
	lock sync.Mutex
	m    map[string]lifeValue
	hm   map[string]map[string][]byte
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
		m:  map[string]lifeValue{},
		hm: map[string]map[string][]byte{},
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
	if !ok {
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

func (m *MockKvStore) Delete(ctx context.Context, key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.m, key)
	delete(m.hm, key)
	return nil
}
