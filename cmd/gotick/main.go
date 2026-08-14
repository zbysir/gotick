// Command gotick 是 gotick 的运维小工具。
//
// 目前只有一个子命令：
//
//	gotick inspect [-redis URL] <callId>
//
// 它直接读 Redis，不需要连上正在运行的 worker，也不需要部署任何东西。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/store"
)

const defaultRedisURL = "redis://localhost:6379/0"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "gotick: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("missing command")
	}

	switch args[0] {
	case "inspect":
		return inspect(args[1:])
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `gotick — inspect gotick workflow state in Redis

Usage:
  gotick inspect [-redis URL] <callId>

Flags:
  -redis URL   Redis connection URL (default `+defaultRedisURL+`)
               Also read from the REDIS_URL environment variable.
`)
}

func inspect(args []string) error {
	return inspectTo(os.Stdout, args)
}

func inspectTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	redisURL := fs.String("redis", envOr("REDIS_URL", defaultRedisURL), "Redis connection URL")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() != 1 {
		return fmt.Errorf("inspect takes exactly one callId, got %d arguments", fs.NArg())
	}
	callId := fs.Arg(0)

	opt, err := redis.ParseURL(*redisURL)
	if err != nil {
		return fmt.Errorf("parse -redis %q: %w", *redisURL, err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	st := gotick.NewKvStoreProduct(store.NewRedisStore(rdb)).New(callId)

	statuses, err := st.GetAllNodeStatus()
	if err != nil {
		return fmt.Errorf("read task status: %w", err)
	}
	meta, err := st.GetKVAll()
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}

	if len(statuses) == 0 && len(meta) == 0 {
		return fmt.Errorf("no state found for callId %q — wrong id, wrong Redis db, or already cleared", callId)
	}

	printReport(w, callId, statuses, meta)
	return nil
}

func printReport(w io.Writer, callId string, statuses map[string]gotick.TaskStatus, meta map[string]string) {
	fmt.Fprintf(w, "callId  %s\n", callId)
	fmt.Fprintf(w, "tasks   %d\n\n", len(statuses))

	keys := make([]string, 0, len(statuses))
	for k := range statuses {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TASK\tSTATUS\tRETRY\tRUN AT\tLAST ERROR")
	for _, k := range keys {
		s := statuses[k]

		runAt := "-"
		if !s.RunAt.IsZero() {
			runAt = s.RunAt.Format(time.RFC3339)
		}

		lastErr := "-"
		if n := len(s.Errs); n > 0 {
			lastErr = truncate(strings.ReplaceAll(s.Errs[n-1], "\n", " "), 60)
		}

		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", k, orDash(s.Status), s.RetryCount, runAt, lastErr)
	}
	tw.Flush()

	// 内部状态用 "__" 前缀存在同一个 hash 里，展示时分开，否则用户看到的全是噪音。
	var userKeys, internalKeys []string
	for k := range meta {
		if strings.HasPrefix(k, "__") {
			internalKeys = append(internalKeys, k)
		} else {
			userKeys = append(userKeys, k)
		}
	}
	sort.Strings(userKeys)
	sort.Strings(internalKeys)

	if len(userKeys) > 0 {
		fmt.Fprintf(w, "\nmetadata\n")
		for _, k := range userKeys {
			fmt.Fprintf(w, "  %s = %s\n", k, truncate(meta[k], 100))
		}
	}
	if len(internalKeys) > 0 {
		fmt.Fprintf(w, "\ncached results (Memo / Array / Async)\n")
		for _, k := range internalKeys {
			fmt.Fprintf(w, "  %s = %s\n", strings.TrimPrefix(k, "__"), truncate(meta[k], 100))
		}
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
