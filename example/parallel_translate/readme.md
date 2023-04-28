## 并行执行

需求：我写了一篇中文文章，并同时调用 API 翻译它为英文和日文。

这个需求中我们需要让 "翻译为英文" 和 "翻译为日文" 两个任务并行执行来提高效率。

2. en = Async("translate_en")
3. jp = Async("translate_jp")
4. Wait(en, jp)
3. SaveArticle(en.Val, jp.Val)
