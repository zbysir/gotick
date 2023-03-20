package internal

// ZRANGEBYSCORE
// https://cloud.tencent.com/developer/article/1358266
// https://cloud.tencent.com/developer/article/1650069?from=article.detail.1358266
// https://cloud.tencent.com/developer/article/1650054?from=article.detail.1358266

// https://github.com/hibiken/asynq/blob/master/internal/rdb/rdb.go
// - 使用有序集合存放 pendding 的任务
// - 定时将 pendding 的任务放入 ready 集合，(func ForwardIfReady)
//  - 	ZRANGEBYSCORE,ZREM
// - 从 ready 集合中取出任务
