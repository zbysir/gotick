package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/store"
)

func TestInspectReportsTaskState(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	const callId = "call-abc123"
	st := gotick.NewKvStoreProduct(store.NewRedisStore(rdb)).New(callId)

	require.NoError(t, st.SetNodeStatus("start", gotick.TaskStatus{Key: "start"}.MakeDone()))
	require.NoError(t, st.SetNodeStatus("send-email", gotick.TaskStatus{Key: "send-email"}.
		MakeRetry(assertableError("smtp timeout"))))
	require.NoError(t, st.SetNodeStatus("wait", gotick.TaskStatus{Key: "wait"}.
		MakeSleep(time.Date(2026, 8, 14, 9, 12, 3, 0, time.UTC))))
	require.NoError(t, st.SetKV("name", "bysir"))

	var out bytes.Buffer
	err := inspectTo(&out, []string{"-redis", "redis://" + mr.Addr(), callId})
	require.NoError(t, err)

	got := out.String()
	t.Logf("\n%s", got)

	assert.Contains(t, got, callId)
	assert.Contains(t, got, "tasks   3")

	// 每个 task 各自的状态都要出现
	assert.Contains(t, got, "start")
	assert.Contains(t, got, "done")
	assert.Contains(t, got, "send-email")
	assert.Contains(t, got, "retry")
	assert.Contains(t, got, "smtp timeout")
	assert.Contains(t, got, "sleep")
	assert.Contains(t, got, "2026-08-14T09:12:03Z")

	// metadata 要按用户可读的形式展示，而不是 JSON 编码后的 `"bysir"`
	assert.Contains(t, got, "name = bysir")
	assert.NotContains(t, got, `name = "bysir"`)
}

func TestInspectUnknownCallId(t *testing.T) {
	mr := miniredis.RunT(t)

	var out bytes.Buffer
	err := inspectTo(&out, []string{"-redis", "redis://" + mr.Addr(), "does-not-exist"})

	require.Error(t, err, "inspecting an unknown callId should be an error, not empty output")
	assert.Contains(t, err.Error(), "does-not-exist")
}

func TestInspectRejectsBadArgs(t *testing.T) {
	var out bytes.Buffer

	err := inspectTo(&out, []string{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one callId")

	err = inspectTo(&out, []string{"a", "b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one callId")

	err = inspectTo(&out, []string{"-redis", "not-a-url", "call-1"})
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "redis")
}

type assertableError string

func (e assertableError) Error() string { return string(e) }
