package gotick

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 关停之后不该再接新活。
func TestInflightRejectsAfterClose(t *testing.T) {
	var inf inflight

	require.True(t, inf.enter(), "关停前应当能进")
	inf.leave()

	inf.closeAndWait()
	assert.False(t, inf.enter(), "关停后不该再接新活")
}

// closeAndWait 必须等在飞的回调走光才返回。
func TestInflightWaitsForOutstanding(t *testing.T) {
	var inf inflight
	require.True(t, inf.enter())

	var released atomic.Bool
	go func() {
		time.Sleep(200 * time.Millisecond)
		released.Store(true)
		inf.leave()
	}()

	inf.closeAndWait()
	assert.True(t, released.Load(), "closeAndWait 在回调还没走完时就返回了")
}

// 关停和投递并发：这正是 CI 上炸出来的时序。
//
// 旧实现里 Subscribe 直接 wg.Add(1)、Start 直接 wg.Wait()，
// 而 sync.WaitGroup 要求「计数为 0 时的 Add 必须发生在 Wait 之前」——
// 队列是 at-least-once 的，关停和投递本来就会重叠，于是 -race 报警，
// 极端情况下还会 panic "WaitGroup misuse"。
func TestInflightCloseRacesWithEnter(t *testing.T) {
	for round := 0; round < 200; round++ {
		var inf inflight
		var wg sync.WaitGroup

		// 一群「投递」和一次「关停」同时发生
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if inf.enter() {
					inf.leave()
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			inf.closeAndWait()
		}()

		wg.Wait()
		assert.False(t, inf.enter(), "关停之后仍然放行了")
	}
}
