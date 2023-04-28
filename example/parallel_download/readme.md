## 并行下载

需求：创建 n 个并行任务。

你可以在 Wait 函数参数中指定并行数量，0 表示不限制。

1. tasks = Array("split")
2. fs = AsyncArray("download", task)
3. Wait(0, fs...)
4. Save(fs)
