package store

import (
	"context"
	"time"
)

type MockDelayedQueue struct {
	callbacks map[string][]func(ctx context.Context, data string) error
}

var _ DelayedQueue = (*MockDelayedQueue)(nil)

func NewMockRedisDelayedQueue() *MockDelayedQueue {
	return &MockDelayedQueue{callbacks: map[string][]func(ctx context.Context, data string) error{}}
}

func (r *MockDelayedQueue) Publish(ctx context.Context, topic string, data string, delay time.Duration) error {
	go func() {
		time.Sleep(delay)
		for _, h := range r.callbacks[topic] {
			h(ctx, data)
		}
	}()

	return nil
}

func (r *MockDelayedQueue) Subscribe(topic string, h func(ctx context.Context, data string) error) {
	r.callbacks[topic] = append(r.callbacks[topic], h)
}

func (r *MockDelayedQueue) Start(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	}
}
