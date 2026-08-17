// 下单后 N 分钟未支付自动关单——一个「事件和超时赛跑」的例子。
//
// 这是 WaitForSignal 真正不可替代的地方，不只是写起来短。
//
// 不用信号的话有两条路，都不好走：
//
//	轮询：Sleep(30s) 然后查一次数据库，没付就再睡。用户第 3 分钟付的钱，
//	      订单要到第 3 分半才发货，而且你为「什么都没发生」查了 60 次库。
//
//	拆成两个 flow：一个「30 分钟后关单」的定时 flow，一个「收到支付」的 flow。
//	      问题是它俩必须互相取消，而这是个分布式竞态：
//	        - 用户 29:59 付款，关单 flow 30:00 醒来，谁赢？
//	        - 关单 flow 已经把订单改成 closed，支付回调这时才到 → 钱收了单关了
//	      最难受的是它静默出错：不报错，只是偶尔有个订单收了钱没发货，
//	      只能靠对账发现。
//
// 信号把这场赛跑收进了一次原子判定：超时时用哨兵去抢信号位，抢到就是超时，
// 抢不到说明支付在同一瞬间落了地——胜负由 Redis 一次操作定死，
// 不存在两边都以为自己赢了。定死之后所有重放读到同一个答案。
//
// 跑它：go run ./example/payment_race
package main

import (
	"context"
	"log"
	"time"

	"github.com/zbysir/gotick"
	"github.com/zbysir/gotick/store"
)

// Payment 支付回调带进来的信息。
type Payment struct {
	TradeNo string `json:"trade_no"`
	Amount  int64  `json:"amount"` // 分
}

// 为了让例子几秒钟就能跑完，把真实的 30 分钟压缩成 3 秒。
const payWindow = 3 * time.Second

func main() {
	// 内存实现，不需要 Redis。生产环境用：
	//   tick, err := gotick.NewServerFromConfig(gotick.Config{RedisURL: "redis://..."})
	tick := gotick.NewServer(gotick.NewServerParams{
		DelayedQueue: store.NewMockRedisDelayedQueue(),
		KVStore:      store.NewMockKvStore(),
	})

	done := make(chan string, 2)

	tick.Flow("order/close", func(ctx *gotick.Context) {
		orderId, _ := ctx.MetaData("order_id")

		gotick.Task(ctx, "create-order", func(c *gotick.TaskContext) error {
			log.Printf("[%s] 下单，等支付，最多 %v", orderId, payWindow)
			return nil
		})

		// 整个例子的重点就是这一行：等支付，但最多等 payWindow。
		// 流程停在这里时不占任何进程，重启也不受影响。
		paid, ok := gotick.WaitForSignal[Payment](ctx, "paid",
			gotick.WithSignalTimeout(payWindow))

		if !ok {
			gotick.Task(ctx, "close-order", func(c *gotick.TaskContext) error {
				log.Printf("[%s] 超时未支付，关单", orderId)
				done <- orderId + ": closed"
				return nil
			})
			return
		}

		// 走到这儿说明支付赢了这场赛跑。paid 是回调带进来的值，
		// 后面每一遍重放读到的都是同一个值。
		gotick.Task(ctx, "ship", func(c *gotick.TaskContext) error {
			log.Printf("[%s] 已支付 %d 分（%s），发货", orderId, paid.Amount, paid.TradeNo)
			done <- orderId + ": shipped"
			return nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		if err := tick.StartServer(ctx); err != nil {
			log.Printf("server stopped: %v", err)
		}
	}()
	time.Sleep(300 * time.Millisecond) // 等调度器起来

	// ---- 结局一：用户及时付款 ----
	paidOrder, err := tick.Trigger(ctx, "order/close", gotick.MetaData{"order_id": "ORD-PAID"})
	if err != nil {
		log.Fatal(err)
	}
	// 模拟支付回调：在窗口内到达。真实代码里这一行就在你的 webhook handler 里。
	time.Sleep(1 * time.Second)
	accepted, err := tick.SendSignal(ctx, paidOrder, "paid", Payment{TradeNo: "2026081700001", Amount: 4299})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[ORD-PAID] 支付回调已投递，被采纳=%v", accepted)

	// ---- 结局二：一直没人付 ----
	if _, err := tick.Trigger(ctx, "order/close", gotick.MetaData{"order_id": "ORD-TIMEOUT"}); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		select {
		case r := <-done:
			log.Printf("结局：%s", r)
		case <-ctx.Done():
			log.Fatal("超时：流程没跑完")
		}
	}

	// 迟到的支付会被拒——超时那一刻哨兵已经占住了信号位。
	// 这一点是确定性的关键：否则后来的重放会读到迟到的值、翻供去发货，
	// 而订单早就关了。
	late, err := tick.SendSignal(ctx, paidOrder, "paid", Payment{TradeNo: "dup", Amount: 1})
	if err == nil {
		log.Printf("重复投递同一个 key，被采纳=%v（应当是 false：只认第一个）", late)
	}
}
