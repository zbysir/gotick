## 并行翻译

需求：我写了一篇中文文章，并同时调用 API 翻译它为英文并且计算长度。

这个需求中我们需要让 "翻译为英文" 和 "计算长度" 两个任务并行执行来提高效率。

1. en = Async("translate_en")
2. le = Async("token_len")
3. Wait(0, en, le)
4. SaveArticle(en.Val, le.Val)
