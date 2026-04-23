# find-repos

## 1. 目标

`find-repos` 的目标是为 scope 内的论文寻找最可信的 GitHub 仓库链接，并给出一个稳定状态。

它解决的问题是：

- 对每篇论文给出“是否找到 repo”的当前结论
- 保留多来源证据，而不是只保留一个最终 URL
- 为后续 `refresh-metadata` 提供 repo URL 集合

它不负责：

- 同步新的论文
- 补全 GitHub 仓库详情

## 2. 输入与候选论文集合

### 2.1 必填输入

- `categories`

### 2.2 支持的范围

- `day`
- `month`
- `from` + `to`

它不会像 `sync-arxiv` 一样支持“无时间窗口抓最新一页”的业务模式。它的输入前提是：系统里已经有论文。

### 2.3 候选论文来源

`find-repos` 不直接向 arXiv 查“哪些论文需要处理”，而是：

1. 先根据 scope 从数据库里选出论文
2. 再对这些论文判断是否“到期需要重新找链接”

## 3. 到期规则

只有满足下列条件的论文才会真正进入查找流程：

- 还没有 `PaperRepoState`
- 当前状态是 `unknown`
- 已经过了 `refresh_after`
- 或者用户显式设置了 `force=true`

因此，已经得到稳定结论且仍在有效期内的论文会被跳过。

当前实现中，repo 链接结论的 TTL 固定为 7 天，还没有暴露成配置项。

这个固定 TTL 适用于已经形成稳定答案的三种状态：

- `found`
- `not_found`
- `ambiguous`

## 4. 来源优先级与查找顺序

系统按“先便宜、先直接、先更接近论文原文”的顺序寻找仓库：

1. arXiv comment
2. arXiv abs 页面
3. Hugging Face paper API
4. Hugging Face paper HTML
5. AlphaXiv paper API
6. AlphaXiv paper HTML

其中有两个短路规则：

- 只要 comment 或 abs 已经得出 repo URL，就不再进入 Hugging Face / AlphaXiv
- 只有在前面阶段还没得到最终 URL 时，才继续向后探测

另外：

- `HUGGINGFACE_ENABLED=false` 时，整条 Hugging Face 路径关闭
- `ALPHAXIV_ENABLED=false` 时，整条 AlphaXiv 路径关闭

## 5. 观察结果与稳定状态

### 5.1 单次观察的状态

每个来源、每个页面都会留下结构化观察，主要有三种结果：

- `found`
- `checked_no_match`
- `fetch_failed`

### 5.2 最终 URL 集合

只有 `found` 且能规范化成 GitHub URL 的结果，才进入最终 URL 集合。

如果多个来源都找到了同一个 URL，它会因为证据更强而排在更前面。

### 5.3 稳定状态判定

最终稳定状态遵循以下规则：

- 恰好 1 个 URL：`found`
- 超过 1 个 URL：`ambiguous`
- 0 个 URL 且本次查找完整完成：`not_found`
- 0 个 URL 且本次查找不完整：保留上一次稳定答案；如果从未有过稳定答案，则为 `unknown`

这条规则的核心意图是：

- 不把一次网络波动误判成“没有 repo”
- 不让系统因为部分失败而丢掉之前已经确认的结论

## 6. 落库内容

一次成功处理某篇论文后，系统会更新：

- `RepoObservation`
- `RawFetch`
- `PaperRepoState`

`PaperRepoState` 中最关键的业务字段有：

- `stable_status`
- `primary_repo_url`
- `repo_urls_json`
- `stable_decided_at`
- `refresh_after`
- `last_attempt_at`
- `last_attempt_complete`
- `last_attempt_error`

## 7. 并发与锁

虽然整个系统的任务队列是串行的，但 `find-repos` 在单个任务内部会并发处理多篇论文。

为避免重复写同一篇论文，系统会对每篇论文加资源锁：

- 锁拿到后才真正落库
- 锁拿不到则跳过，并记入统计

这意味着该任务追求的是“尽量推进整体进度”，而不是“为一篇论文阻塞全部处理”。

## 8. 批量拆分规则

当 scope 跨越多个月份窗口时，任务会先被拆成批量根任务，再按月份优先生成子任务。

整月窗口保留为 `month` 子任务；不满整月的窗口保留为 `from/to` 子任务。

这样设计的目的是：

- 让修复只针对局部月份
- 减少一次失败影响的范围

## 9. 关键配置项

- `HUGGINGFACE_ENABLED`
- `ALPHAXIV_ENABLED`
- `HUGGINGFACE_TOKEN`
- `ALPHAXIV_TOKEN`
- `ARXIV_API_MIN_INTERVAL`
- `HUGGINGFACE_MIN_INTERVAL`
- `ALPHAXIV_MIN_INTERVAL`
- `FIND_REPOS_WORKER_CONCURRENCY`
- `FIND_REPOS_ARXIV_MAX_CONCURRENT`
- `FIND_REPOS_HUGGINGFACE_MAX_CONCURRENT`
- `FIND_REPOS_HUGGINGFACE_HTML_MAX_CONCURRENT`
- `FIND_REPOS_ALPHAXIV_MAX_CONCURRENT`

它们分别影响：

- 哪些外部来源启用
- 是否带认证访问第三方服务
- 各来源的请求节流速度
- 任务内部的并发度
