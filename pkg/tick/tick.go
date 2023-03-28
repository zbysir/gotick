package tick

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/zbysir/ticktick/internal/store"
	"github.com/zbysir/ticktick/internal/tick"
	"time"
)

type StoreProduct struct {
	store store.KVStore
}

func NewStoreProduct(store store.KVStore) *StoreProduct {
	return &StoreProduct{store: store}
}

func (s StoreProduct) New(key string) tick.NodeStatusStore {
	return NewNodeStatusStore(s.store, key)
}

type NodeStatusStore struct {
	store store.KVStore
	table string
}

func (n *NodeStatusStore) Clear() error {
	return n.store.HClear(context.Background(), n.table)
}

func NewNodeStatusStore(store store.KVStore, table string) *NodeStatusStore {
	return &NodeStatusStore{store: store, table: table}
}

func (n *NodeStatusStore) GetNodeStatus(key string) (*tick.NodeStatus, bool, error) {
	status := tick.NodeStatus{}
	exist, err := n.store.HGet(context.Background(), n.table, key, &status)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		return nil, false, nil
	}

	return &status, true, nil
}

func (n *NodeStatusStore) SetNodeStatus(key string, value *tick.NodeStatus) error {
	return n.store.HSet(context.Background(), n.table, key, value, 0)
}

func (n *NodeStatusStore) GetMetaData() (tick.MetaData, bool, error) {
	md := tick.MetaData{}
	exist, err := n.store.HGet(context.Background(), n.table, "__meta", &md)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		return nil, false, nil
	}
	return md, true, nil
}

func (n *NodeStatusStore) SetMetaData(data tick.MetaData) error {
	return n.store.HSet(context.Background(), n.table, "__meta", data, 0)
}

type Options struct {
	RedisURL string // "redis://<user>:<pass>@localhost:6379/<db>"
}

func NewTick(p Options) *tick.Tick {
	opt, err := redis.ParseURL(p.RedisURL)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)
	st := NewStoreProduct(store.NewRedisStore(redisClient))
	queen := store.NewAsynq(redisClient)
	ap := NewAsyncQueenProduct(queen)
	t := tick.NewTickServer(st, ap)
	return t
}

type AsyncQueenProduct struct {
	queen store.DelayedQueue
}

func (a *AsyncQueenProduct) Start(ctx context.Context) error {
	return a.queen.Start(ctx)
}

func NewAsyncQueenProduct(redis store.DelayedQueue) *AsyncQueenProduct {
	return &AsyncQueenProduct{queen: redis}
}

func (a *AsyncQueenProduct) New(key string) tick.AsyncQueen {
	x := NewAsyncQueen(a.queen, key)
	return x
}

type AsyncQueen struct {
	redis store.DelayedQueue
	key   string
}

func (a *AsyncQueen) Publish(data tick.Event, delay time.Duration) error {
	return a.redis.Publish(a.key, data.CallId, delay)
}

func (a *AsyncQueen) Exist(uniqueKey []string) (map[string]bool, error) {
	// return a.redis.Exist(uniqueKey)
	return nil, nil
}

func (a *AsyncQueen) Subscribe(h func(data tick.Event) error) {
	a.redis.Subscribe(a.key, func(data string) error {
		return h(tick.Event{CallId: data})
	})
}

func NewAsyncQueen(redis store.DelayedQueue, key string) *AsyncQueen {
	return &AsyncQueen{redis: redis, key: key}
}
