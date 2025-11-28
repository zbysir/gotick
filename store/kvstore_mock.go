package store

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"
)

type LifeValue struct {
	live  time.Time
	value interface{}
}

type MockKvStore struct {
	m    map[string]LifeValue
	hm   map[string]map[string]LifeValue
	lock sync.Mutex
}

func NewMockKvStore() *MockKvStore {
	return &MockKvStore{m: map[string]LifeValue{}, hm: map[string]map[string]LifeValue{}}
}

func (m *MockKvStore) Get(ctx context.Context, key string, r interface{}) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	v, ok := m.m[key]
	if !ok {
		return false, nil
	}
	if v.live.Before(time.Now()) {
		return false, nil
	}

	rv := reflect.ValueOf(r)
	rv.Elem().Set(reflect.ValueOf(v.value))
	return true, nil
}

func (m *MockKvStore) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	live := time.Now()
	if expiration <= 0 {
		live = time.Now().AddDate(1, 0, 0)
	} else {
		live = live.Add(expiration)
	}
	m.m[key] = LifeValue{
		live:  live,
		value: value,
	}
	return nil
}

func (m *MockKvStore) HGet(ctx context.Context, table string, key string, r interface{}) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mp, ok := m.hm[table]
	if !ok {
		return false, nil
	}

	v, ok := mp[key]
	if !ok {
		return false, nil
	}

	if v.live.Before(time.Now()) {
		return false, nil
	}

	rv := reflect.ValueOf(r)
	rv.Elem().Set(reflect.ValueOf(v.value))

	return true, nil
}

func (m *MockKvStore) HGetAll(ctx context.Context, table string) (map[string]string, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mp, ok := m.hm[table]
	if !ok {
		return nil, false, nil
	}

	r := make(map[string]string)
	for k, v := range mp {
		if v.live.Before(time.Now()) {
			continue
		}

		bs, _ := json.Marshal(v.value)
		r[k] = string(bs)
	}

	return r, true, nil
}

func (m *MockKvStore) HSet(ctx context.Context, table string, key string, value interface{}, expiration time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	mp, ok := m.hm[table]

	live := time.Now()
	if expiration <= 0 {
		live = time.Now().AddDate(1, 0, 0)
	} else {
		live = live.Add(expiration)
	}

	va := LifeValue{
		live:  live,
		value: value,
	}
	if !ok {
		m.hm[table] = map[string]LifeValue{
			key: va,
		}
		return nil
	}
	mp[key] = va

	return nil
}

func (m *MockKvStore) Delete(ctx context.Context, key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.m, key)
	delete(m.hm, key)
	return nil
}
