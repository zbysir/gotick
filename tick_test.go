package gotick

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDag(t *testing.T) {
	f := Flow{
		Id: "",
		fun: func(ctx *Context) error {
			Task(ctx, "start", func(ctx *TaskContext) error {
				return nil
			})

			Sleep(ctx, "sleep", 10*time.Second)

			tasks := Array(ctx, "gen-tasks", func() ([]string, error) {
				return []string{"a", "b", "c"}, nil
			})

			for _, ta := range tasks {
				Task(ctx, ta.Key("send-email"), func(ctx *TaskContext) error {
					return nil
				})
			}

			Task(ctx, "end", nil)

			return nil
		},
		onFail:    nil,
		onSuccess: nil,
	}

	d, err := f.DAG()
	if err != nil {
		t.Fatal(err)
		return
	}

	bs, _ := json.MarshalIndent(d.Nodes, "", "  ")
	t.Logf("node: %s", bs)

	bs, _ = json.MarshalIndent(d.Edges, "", "  ")
	t.Logf("edges: %s", bs)
}
