# Find Repos 设计

本文档描述当前 `find-repos` 后端实现。事实来源是当前代码，不继承旧 design snapshot。

## 1. 产品目标

`find-repos` 负责为本地 `papers` 中的 arXiv paper 查找对应 GitHub repository。

它读取：

- `papers`
- `paper_repo_state`

它写入：

- `repo_observations`
- `paper_repo_state`
- `raw_fetches`

## 2. 输入与候选集

输入必须包含：

- `categories`
- `day`、`month`、`from/to` 之一

候选 papers 来自 `scoped_papers()`：

- category 条件：`Paper.categories_json` 包含任意请求 category。
- 时间条件：使用 `Paper.published_at`。
- 排序：`published_at desc nullslast`，然后 `arxiv_id desc`。

当前不会使用 `SyncPapersArxivArchiveAppearance` 作为 find-repos 的 scope 依据。

## 3. Batch

`find-repos` 的 batch 展开使用 month-priority 模型：

| Scope | 展开方式 |
| --- | --- |
| 单 day | 不创建 batch。 |
| 单完整 month | 不创建 batch。 |
| 单非完整 month 内的 `from/to` | 不创建 batch。 |
| 跨多个月的 `from/to` | 创建 `find_repos_batch`；边缘部分月保留 `from/to`，完整中间月转为 `month`。 |

Child scope 内 categories 保持在一起，不按 category 拆分。

## 4. Due / TTL

`find-repos` 有 link TTL，默认 `FIND_REPOS_LINK_TTL_DAYS=7`。

`_link_lookup_due(state, force)` 当前规则：

| 条件 | 是否 due |
| --- | --- |
| `force=true` | 是 |
| 没有 `PaperRepoState` | 是 |
| `stable_status=unknown` | 是 |
| `refresh_after is None` | 是 |
| `refresh_after <= now` | 是 |
| stable 状态且 `refresh_after > now` | 否 |

稳定状态包括：

- `found`
- `not_found`
- `ambiguous`

如果 provider 请求不完整，并且之前已经有稳定状态，当前实现会保留之前的稳定状态和 `refresh_after`，同时更新 `last_attempt_complete=false` 与错误信息。

## 5. 查找来源顺序

当前 provider sequence：

1. `Paper.comment`
2. `Paper.abstract`
3. AlphaXiv paper API
4. AlphaXiv paper HTML
5. Hugging Face paper API

当前代码事实：

- 不抓 arXiv abs HTML 页面。
- 不抓 Hugging Face paper HTML。
- comment 和 abstract 都来自 `sync-papers` 已入库字段。
- 每一步如果已经得到 final repo URL，就短路跳过后续来源。
- AlphaXiv API 返回 404 时，不再请求 AlphaXiv HTML。
- AlphaXiv API 非 404 且未找到 URL 时，才会继续请求 AlphaXiv HTML。
- Hugging Face API 只有在前面都没找到 URL 且 enabled 时才会请求。

## 6. Provider 行为

### 6.1 Paper comment / abstract

直接从本地 DB 字段提取 GitHub URL：

- `Paper.comment`
- `Paper.abstract`

提取规则：

- 使用 GitHub URL regex。
- 只接受 `github.com` / `www.github.com`。
- 规范化为 `https://github.com/{owner}/{repo}`。
- 排除 GitHub 保留路径，例如 `search`、`topics`、`users` 等。

### 6.2 AlphaXiv

请求：

- API：`https://api.alphaxiv.org/papers/v3/{arxiv_id}`
- HTML：`https://www.alphaxiv.org/abs/{arxiv_id}`

API 提取字段：

- `paper.implementation`
- `paper.marimo_implementation`
- `paper.paper_group.resources`
- `paper.resources`
- payload 内任意可递归提取的 GitHub URL

HTML 提取：

- 解析当前代码中支持的 embedded resource / implementation 字段 pattern。

配置：

- `FIND_REPOS_ALPHAXIV_ENABLED=true`
- `FIND_REPOS_ALPHAXIV_MIN_INTERVAL=0.2`
- `FIND_REPOS_ALPHAXIV_MAX_CONCURRENT=4`
- `ALPHAXIV_TOKEN` 可选，存在时作为 Bearer token。

### 6.3 Hugging Face

请求：

- API：`https://huggingface.co/api/papers/{arxiv_id}`

提取字段：

- JSON `githubRepo`

配置：

- `FIND_REPOS_HUGGINGFACE_ENABLED=true`
- `FIND_REPOS_HUGGINGFACE_MIN_INTERVAL=0.2`
- `FIND_REPOS_HUGGINGFACE_MAX_CONCURRENT=4`
- `HUGGINGFACE_TOKEN` 可选，存在时作为 Bearer token。

## 7. 并发模型

`find-repos` 内部会并发处理 due papers：

- worker 数量：`min(FIND_REPOS_WORKER_CONCURRENCY, due_papers数量)`。
- 默认 `FIND_REPOS_WORKER_CONCURRENCY=24`。
- provider 自己还有 semaphore：
  - Hugging Face 默认 `4`。
  - AlphaXiv 默认 `4`。
- provider 自己还有 RateLimiter：
  - Hugging Face 默认 `0.2` 秒。
  - AlphaXiv 默认 `0.2` 秒。

关系：

- worker concurrency 控制“同时有多少篇 paper 在跑查找流程”。
- provider max concurrent 控制“同一 provider 同时有多少请求在飞”。
- provider min interval 控制“同一 provider 两次发请求之间的最小间隔”。

## 8. Observation 与稳定结果

每篇 paper 会生成 observations：

| Observation status | 含义 |
| --- | --- |
| `found` | 当前 provider/surface 找到 repo URL。 |
| `checked_no_match` | provider/surface 成功检查但没有 repo URL。 |
| `fetch_failed` | provider/surface 请求失败。 |

持久化时：

- 先删除该 paper 旧 `RepoObservation`。
- 再写入本次 observations。
- provider 原始响应会写入 `RawFetch`，observation 通过 `raw_fetch_id` 关联。

最终 repo URL 排序：

1. provider 数量多的 URL 优先。
2. surface 数量多的 URL 优先。
3. URL 字典序。

最终 `PaperRepoState`：

| 结果 | `stable_status` | 其他字段 |
| --- | --- | --- |
| 找到 1 个 URL | `found` | `primary_repo_url` 为该 URL，`repo_urls_json` 含该 URL。 |
| 找到多个 URL | `ambiguous` | 排序第一个作为 `primary_repo_url`，全部写入 `repo_urls_json`。 |
| 没找到且所有 provider 完整检查 | `not_found` | URL 字段清空。 |
| 没找到且检查不完整，之前无稳定状态 | `unknown` | URL 字段清空。 |
| 没找到且检查不完整，之前有稳定状态 | 保留之前 stable 状态 | 保留之前 URL 和 `refresh_after`。 |

稳定结果会设置：

- `stable_decided_at=now`
- `refresh_after=now + FIND_REPOS_LINK_TTL_DAYS`
- `last_attempt_at=now`
- `last_attempt_complete`
- `last_attempt_error`

## 9. 锁与持久化

每篇 paper 持久化前会尝试 advisory lock：

- lock key：`paper:{arxiv_id}`

如果拿不到 lock：

- 跳过该 paper 的持久化。
- `stats_json.skipped_locked` 增加。

当前 provider 请求发生在持久化 lock 之前，因此 lock 只保护 DB 写入，不阻止多个进程重复请求 provider。由于全局队列当前是 serial，这不是常规路径问题。

## 10. RawFetch surfaces

当前会写入：

| Provider | Surface |
| --- | --- |
| `alphaxiv` | `paper_api` |
| `alphaxiv` | `paper_html` |
| `huggingface` | `paper_api` |

Paper comment / abstract 是本地字段，不写 `RawFetch`。

## 11. Stats

主要 stats：

| 字段 | 含义 |
| --- | --- |
| `papers_considered` | scope 内 papers 总数。 |
| `papers_processed` | 实际完成并持久化状态的 papers 数。 |
| `papers_skipped_fresh` | 因 TTL 未过期跳过的 papers。 |
| `papers_skipped_no_longer_due` | 并发期间重新检查发现已经不 due 的 papers。 |
| `resume_items_reused` | repair run 复用并跳过的 paper 数。 |
| `resume_items_completed` | 本次成功记录 item-level resume progress 的 paper 数。 |
| `found` | 本次处理后 stable_status 为 found 的数量。 |
| `not_found` | 本次处理后 stable_status 为 not_found 的数量。 |
| `ambiguous` | 本次处理后 stable_status 为 ambiguous 的数量。 |
| `unknown` | 本次处理后 stable_status 为 unknown 的数量。 |
| `skipped_locked` | paper advisory lock 冲突数量。 |
| `provider_counts.arxiv.comment_matches` | comment 中找到的 URL 数。 |
| `provider_counts.arxiv.abstract_matches` | abstract 中找到的 URL 数。 |
| `provider_counts.alphaxiv.api_requests` | AlphaXiv API 请求数。 |
| `provider_counts.alphaxiv.api_failures` | AlphaXiv API 失败数。 |
| `provider_counts.alphaxiv.html_requests` | AlphaXiv HTML 请求数。 |
| `provider_counts.alphaxiv.html_failures` | AlphaXiv HTML 失败数。 |
| `provider_counts.huggingface.api_requests` | Hugging Face API 请求数。 |
| `provider_counts.huggingface.api_failures` | Hugging Face API 失败数。 |
| `stage_seconds.*` | provider 与 persist 阶段耗时。 |
| `elapsed_seconds` | job 已运行秒数。 |
| `papers_per_minute` | 运行吞吐估算。 |

## 12. Item-level resume

`find-repos` 使用 item-level resume，粒度是 paper。

当前语义：

- 每篇 paper 完整 lookup result 成功持久化后，会记录 `JobItemResumeProgress(item_kind=paper, item_key=arxiv_id, status=completed)`。
- repair run 会先读取同一 `attempt_series_key` 下已经 completed 的 paper，并跳过这些 paper；剩余 paper 再按 `PaperRepoState.refresh_after` 判断是否 due。
- `force=true` 不复用 item-level resume，仍然重查 scoped papers。
- 非 force run 仍然先遵守 `PaperRepoState.refresh_after` TTL。

当前限制：

- item-level resume 不复用 AlphaXiv / Hugging Face 原始响应。
- 如果 provider 返回不完整 lookup result，或 job 在 provider 请求阶段失败且还没持久化完整 paper result，该 paper 不会被标记 completed。
- `sync-papers` 的 request-level checkpoint 不适用于 `find-repos`。
