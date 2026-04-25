# Refresh Metadata 设计

本文档描述当前 `refresh-metadata` 后端实现。事实来源是当前代码，不继承旧 design snapshot。

## 1. 产品目标

`refresh-metadata` 负责刷新已经解析出的 GitHub repository metadata，例如 stars、description、topics、license、archived、pushed_at。

它读取：

- `papers`
- `paper_repo_state`
- `github_repos`

它写入：

- `github_repos`
- `raw_fetches`

## 2. 输入与候选集

输入必须包含：

- `categories`
- `day`、`month`、`from/to` 之一

候选 papers 来自 `scoped_papers()`：

- category 条件：`Paper.categories_json` 包含任意请求 category。
- 时间条件：使用 `Paper.published_at`。
- 当前不会使用 `SyncPapersArxivArchiveAppearance` 作为 scope 依据。

候选 repos：

- 遍历 scope 内 papers。
- 读取 `paper.repo_state.repo_urls_json`。
- 用 set 去重。
- 只处理已经由 `find-repos` 写入的 repo URL。

当前 `refresh-metadata` 没有 TTL；每次运行都会处理 scope 内所有 repo URL。

## 3. Batch

`refresh-metadata` 的 batch 展开使用 month-priority 模型：

| Scope | 展开方式 |
| --- | --- |
| 单 day | 不创建 batch。 |
| 单完整 month | 不创建 batch。 |
| 单非完整 month 内的 `from/to` | 不创建 batch。 |
| 跨多个月的 `from/to` | 创建 `refresh_metadata_batch`；边缘部分月保留 `from/to`，完整中间月转为 `month`。 |

Child scope 内 categories 保持在一起，不按 category 拆分。

## 4. GitHub 请求策略

当前根据 `GITHUB_TOKEN` 选择不同路径。

### 4.1 有 GitHub token

如果 `GITHUB_TOKEN` 非空：

1. 优先使用 GitHub GraphQL batch。
2. 每批 repo 数量由 `REFRESH_METADATA_GITHUB_GRAPHQL_BATCH_SIZE` 控制，默认 `50`。
3. GraphQL unresolved、error alias、invalid owner/repo 或 batch 错误会进入 REST fallback。
4. REST fallback 使用 GitHub REST repo API。

GraphQL 当前抓取字段：

| 字段 | 来源 |
| --- | --- |
| GitHub database id | `databaseId` |
| owner | `owner.login` |
| repo name | `name` |
| stars | `stargazerCount` |
| created_at | `createdAt` |
| description | `description` |
| homepage | `homepageUrl` |
| archived | `isArchived` |
| pushed_at | `pushedAt` |
| license | `licenseInfo.spdxId` 或 `licenseInfo.name` |
| topics | `repositoryTopics(first: 20).nodes.topic.name` |

### 4.2 无 GitHub token

如果 `GITHUB_TOKEN` 为空：

- 跳过 GraphQL。
- 所有 repo 都走 REST。
- REST 最小请求间隔为 `max(REFRESH_METADATA_GITHUB_MIN_INTERVAL, 60.0)`。

这是当前匿名 GitHub REST 保护策略。

## 5. REST 行为

REST 请求：

- `https://api.github.com/repos/{owner}/{repo}`

请求 headers：

- `Accept: application/vnd.github+json`
- `User-Agent: papertorepo`
- 有 token 时加 `Authorization: Bearer ...`
- 如果已有 repo metadata 且存在 `etag`，加 `If-None-Match`
- 如果已有 repo metadata 且存在 `last_modified`，加 `If-Modified-Since`

允许状态码：

| 状态 | 当前行为 |
| --- | --- |
| `200` | 解析 payload，upsert `github_repos`，计入 `updated`。 |
| `304` | 如果已有 repo，只更新 `checked_at`，计入 `not_modified`。 |
| `404` | 如果已有 repo，只更新 `checked_at`，计入 `missing`；如果没有已有 repo，不创建空 repo。 |

REST payload 当前字段：

| 字段 | 来源 |
| --- | --- |
| GitHub id | `id` |
| owner | `owner.login` |
| repo name | `name` |
| stars | `stargazers_count` |
| created_at | `created_at` |
| description | `description` |
| homepage | `homepage` |
| topics | `topics` |
| license | `license.spdx_id` 或 `license.name` |
| archived | `archived` |
| pushed_at | `pushed_at` |

## 6. GitHubRepo upsert 语义

`github_repos` 主键是 `normalized_github_url`。

创建时写入：

- `normalized_github_url`
- `owner`
- `repo`
- `first_seen_at`

刷新时：

- `stars` 每次更新。
- `description` 每次更新。
- `homepage` 每次更新。
- `topics_json` 每次更新。
- `license` 每次更新。
- `archived` 每次更新。
- `pushed_at` 每次更新。
- `checked_at` 每次更新。
- REST 返回 headers 时更新 `etag` / `last_modified`。

保留语义：

- `github_id` 只在原来为空时写入。
- `created_at` 只在原来为空时写入。
- `first_seen_at` 只在 create 时写入。

## 7. 限速与并发

当前配置和常量：

| 名称 | 当前值 | 说明 |
| --- | --- | --- |
| `REFRESH_METADATA_GITHUB_MIN_INTERVAL` | `0.2` | 有 token 时 GraphQL/REST limiter 间隔。 |
| `REFRESH_METADATA_GITHUB_GRAPHQL_BATCH_SIZE` | `50` | GraphQL 每批 repo 数量。 |
| `REFRESH_METADATA_GITHUB_REST_FALLBACK_MAX_CONCURRENT` | `2` | REST fallback semaphore。 |
| `REFRESH_METADATA_GITHUB_GRAPHQL_MAX_CONCURRENT` | `1` | GraphQL semaphore。 |
| `REFRESH_METADATA_GITHUB_GRAPHQL_TOPICS_FIRST` | `20` | GraphQL topics 数量。 |
| `REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS` | `60.0` | 无 token 时 REST 最小间隔下限。 |

注意：

- GraphQL 当前是 batch 顺序执行，semaphore 为 1。
- REST fallback semaphore 配置存在，但当前主循环按 fallback URL 顺序逐个 await，因此实际 REST 请求是串行发起；semaphore 只是在代码结构上限制上限。
- RateLimiter 仍然会约束请求发出间隔。

## 8. Advisory lock

每个 repo 写入前会尝试 advisory lock：

- lock key：`repo:{normalized_url}`

如果拿不到 lock：

- 跳过该 repo。
- `stats_json.skipped_locked` 增加。

SQLite 测试环境中 advisory lock 永远返回成功。

## 9. RawFetch surfaces

当前会写入：

| Provider | Surface | 说明 |
| --- | --- | --- |
| `github` | `graphql_batch` | GraphQL batch 原始响应。 |
| `github` | `repo_api` | REST repo API 原始响应，包括 200/304/404。 |

## 10. Stats

主要 stats：

| 字段 | 含义 |
| --- | --- |
| `repos_considered` | scope 内去重后的 repo URL 数。 |
| `repos_completed` | 完成处理的 repo URL 数。 |
| `resume_items_reused` | repair run 复用并跳过的 repo URL 数。 |
| `resume_items_completed` | 本次成功记录 item-level resume progress 的 repo URL 数。 |
| `updated` | 200 或 GraphQL 成功更新数量。 |
| `not_modified` | REST 304 数量。 |
| `missing` | REST 404 数量。 |
| `skipped_locked` | repo advisory lock 冲突数量。 |
| `provider_counts.github.graphql_batches` | GraphQL batch 请求数。 |
| `provider_counts.github.graphql_batch_failures` | GraphQL batch 失败数。 |
| `provider_counts.github.graphql_repos` | 进入 GraphQL batch 的 repo 数。 |
| `provider_counts.github.graphql_fallbacks` | GraphQL 后进入 REST fallback 的 repo 数。 |
| `provider_counts.github.rest_requests` | REST 请求数。 |
| `provider_counts.github.rest_failures` | REST 失败数。 |
| `stage_seconds.github_graphql` | GraphQL 阶段耗时。 |
| `stage_seconds.github_rest` | REST 阶段耗时。 |
| `stage_seconds.persist` | DB 持久化耗时。 |
| `elapsed_seconds` | job 已运行秒数。 |
| `repos_per_minute` | 运行吞吐估算。 |

## 11. Item-level resume

`refresh-metadata` 使用 item-level resume，粒度是 repo URL。

当前语义：

- 每个 repo URL 成功处理后，会记录 `JobItemResumeProgress(item_kind=repo, item_key=normalized_github_url, status=completed)`。
- repair run 会读取同一 `attempt_series_key` 下已经 completed 的 repo URL，并跳过这些 repo。
- `force=true` 不复用 item-level resume，仍然重查 scoped repos。
- REST 仍会使用已有 `etag` / `last_modified` 做 conditional request，但这属于请求成本优化，不是 resume progress。

当前限制：

- item-level resume 不复用 GitHub GraphQL / REST 原始响应。
- 没有 metadata TTL。
- 如果 job 在 GitHub 请求阶段失败，且还没持久化 repo result，该 repo 不会被标记 completed。
