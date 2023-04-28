package store

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"
)

type MockNodeStatusStore struct {
	m    map[string]interface{}
	lock sync.Mutex
}

func NewMockNodeStatusStore() *MockNodeStatusStore {
	return &MockNodeStatusStore{m: map[string]interface{}{}}
}

func (m *MockNodeStatusStore) Get(ctx context.Context, key string, r interface{}) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	v, ok := m.m[key]
	if !ok {
		return false, nil
	}
	rv := reflect.ValueOf(r)
	rv.Elem().Set(reflect.ValueOf(v))
	return true, nil
}

func (m *MockNodeStatusStore) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.m[key] = value
	return nil
}

func (m *MockNodeStatusStore) HGet(ctx context.Context, table string, key string, r interface{}) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mp, ok := m.m[table+"_"]
	if !ok {
		return false, nil
	}

	v, ok := mp.(map[string]interface{})[key]
	if !ok {
		return false, nil
	}
	rv := reflect.ValueOf(r)
	rv.Elem().Set(reflect.ValueOf(v))

	return true, nil
}

func (m *MockNodeStatusStore) HGetAll(ctx context.Context, table string) (map[string]string, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mp, ok := m.m[table+"_"]
	if !ok {
		return nil, false, nil
	}

	r := make(map[string]string)
	for k, v := range mp.(map[string]interface{}) {
		bs, _ := json.Marshal(v)
		r[k] = string(bs)
	}

	return r, true, nil
}

func (m *MockNodeStatusStore) HSet(ctx context.Context, table string, key string, value interface{}, expiration time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	mp, ok := m.m[table+"_"]
	if !ok {
		m.m[table+"_"] = map[string]interface{}{
			key: value,
		}
		return nil
	}
	mp.(map[string]interface{})[key] = value

	return nil
}

func (m *MockNodeStatusStore) Delete(ctx context.Context, key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.m, key)
	delete(m.m, key+"_")
	return nil
}
