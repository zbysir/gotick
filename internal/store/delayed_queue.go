package store

import (
	"context"
	"time"
)

type DelayedQueue interface {
	// Publish 当 data 相同时后面 Publish 的数据会覆盖前面的数据
	Publish(ctx context.Context, topic string, data []byte, delay time.Duration, option Option) error
	//Exist(data []string) (map[string]bool, error)
	Subscribe(topic string, h func(ctx context.Context, data []byte) error)
	Start(ctx context.Context) error // 这是一个阻塞方法，用于优雅关闭
}
