package signal

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

type Context struct {
	Context context.Context
	Cancel  func()
}

// NewContext 等待信号量
func NewContext(sig ...os.Signal) (ctx context.Context, cancel func()) {
	ctx, cancel = context.WithCancel(context.Background())

	if len(sig) == 0 {
		sig = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}
	go func() {
		// signal.Notify 不会阻塞发送，channel 必须有缓冲，否则信号会被丢弃。
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, sig...)
		<-ch

		cancel()
	}()
	return
}

// 等待Term和Int信号量关闭
func NewTermContext() (ctx context.Context, cancel func()) {
	return NewContext(syscall.SIGINT, syscall.SIGTERM)
}
