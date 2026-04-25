# 后端公共设计

本文档是 `2026-04-25-15-16-27` 时点的后端设计快照。旧版 design snapshot 不作为事实来源；本文只描述当前代码已经实现的行为。

## 1. 范围

本文档记录 `sync-papers`、`find-repos`、`refresh-metadata` 三条后端链路的公共设计。三条链路共享：

- 同一套 `ScopePayload` 输入模型。
- 同一套 Job / Batch / Attempt 生命周期。
- 同一套串行队列与 Stop / Re-run API。
- 同一套原始请求落盘模型 `RawFetch`。
- 同一套前端可见的 Job 状态、统计字段和健康检查元数据。

`export` 也复用 Job 队列，但不是本次 backend design 的主体。

## 2. 术语

| 术语 | 当前含义 |
| --- | --- |
| Queue | 全局执行队列。当前设计是 serial queue，一次只允许一个 fresh running job 占用执行权。 |
| Batch folder | 多窗口任务的父 Job，例如 `sync_papers_batch`、`find_repos_batch`、`refresh_metadata_batch`。它本身负责展开 child jobs，不直接做业务抓取。 |
| Child job | Batch folder 展开的真实业务 Job，例如 `sync_papers`、`find_repos`、`refresh_metadata`。 |
| Scope | 用户选择的 categories + 时间窗口 + force 等输入。Batch folder 内部每个 child job 都有自己的 scope。 |
| Attempt series | 同一个 Job 被 re-run 后形成的修复链，由 `attempt_series_key` 关联。 |
| Fresh run | 新启动的独立运行，`attempt_mode=fresh`。即使 scope 相同，也会创建独立 attempt series。 |
| Repair run | 对某个失败、取消或待执行 scope 的重新执行，`attempt_mode=repair`。 |

## 3. Scope 模型

三条链路都要求：

- `categories` 必填，支持逗号分隔字符串或数组，格式必须类似 `cs.CV`、`cs.LG`。
- 时间窗口必填，且只能选择 `day`、`month`、`from/to` 三者之一。
- `from` 和 `to` 必须同时提供，并且 `from <= to`。

Scope 会被 canonicalize：

| 输入 | 规范化结果 |
| --- | --- |
| `from == to` | 转成 `day` |
| 完整自然月 `from/to` | 转成 `month` |
| 非完整区间 | 保持 `from/to` |
| categories | 去空白、去重、排序 |

`force` 当前对 `sync-papers` 和 `find-repos` 有业务重查语义；对 `refresh-metadata` 没有 metadata TTL 语义，但 repair run 中 `force=true` 仍表示不复用 item-level resume。

## 4. Job 模型

当前 Job types：

| Job type | 含义 |
| --- | --- |
| `sync_papers` | 单个 paper sync 执行单元。 |
| `sync_papers_batch` | paper sync batch folder。 |
| `find_repos` | 单个 repo lookup 执行单元。 |
| `find_repos_batch` | repo lookup batch folder。 |
| `refresh_metadata` | 单个 GitHub metadata refresh 执行单元。 |
| `refresh_metadata_batch` | metadata refresh batch folder。 |
| `export` | CSV export。 |

当前 Job statuses：

| Status | 含义 |
| --- | --- |
| `pending` | 等待队列执行。 |
| `running` | 已被 worker claim 并执行中。 |
| `succeeded` | 执行成功。 |
| `failed` | 执行抛错失败。 |
| `cancelled` | 用户 stop 或已停止。 |

Job 关键字段：

| 字段 | 作用 |
| --- | --- |
| `parent_job_id` | child job 指向 batch folder。 |
| `attempt_mode` | `fresh` 或 `repair`。 |
| `attempt_series_key` | 同一修复链的关联 key。 |
| `scope_json` | 规范化后的执行 scope。 |
| `dedupe_key` | `job_type + scope_json` 的 hash，用于相同 active job 去重。 |
| `stats_json` | 运行中和运行后的可观测统计。 |
| `stop_requested_at` / `stop_reason` | stop 请求标记。 |
| `locked_by` / `locked_at` | worker claim 与心跳时间。 |
| `attempts` | worker 实际执行次数。 |

## 5. 串行队列

当前队列是串行模型：

- worker 每次通过 `claim_next_job()` claim 一个 job。
- 如果存在 fresh running job，且 `locked_at` 没有超过 `JOB_QUEUE_RUNNING_TIMEOUT_SECONDS`，新 job 不会被 claim。
- 默认 stale running timeout 是 `1800` 秒。
- worker poll 间隔默认 `1.0` 秒。
- 如果 running job 超过 stale timeout，队列允许重新 claim。

执行顺序与展示顺序不同：

| 场景 | 排序 |
| --- | --- |
| Queue 执行顺序 | `created_at asc`，然后 scope window start/end asc，然后 `id asc`。 |
| Job 列表展示顺序 | `created_at desc`，然后 scope window start/end desc，然后 `id desc`。 |
| Batch child 展示顺序 | 按计划 scope 的时间倒序展示，新月份在上，旧月份在下。re-run 后仍留在原 scope 位置。 |

## 6. Batch folder

Batch folder 的创建规则：

| 链路 | 创建 batch folder 的条件 | Child scope 展开方式 |
| --- | --- | --- |
| `sync-papers` | scope 跨多个 archive month | 按 category × archive month 展开，每个 child 是单 category + 单 month。 |
| `find-repos` | month-priority 展开后 child 数量大于 1 | 按自然月切分；完整月用 `month`，边缘非完整月用 `from/to`，categories 保持在同一个 child 内。 |
| `refresh-metadata` | 同 `find-repos` | 同 `find-repos`。 |

Batch state 是 child-centric 的聚合状态，不完全等同 parent job 自身 status：

| 优先级 | 条件 | `batch_state` |
| --- | --- | --- |
| 1 | parent running 且 stop requested，或任意 child stopping | `stopping` |
| 2 | 任意最新 child running | `running` |
| 3 | 任意最新 child pending，或缺失计划 child 且 parent 未取消 | `queued` |
| 4 | 任意最新 child failed | `failed` |
| 5 | 任意最新 child cancelled，或缺失计划 child 且 parent 已取消 | `cancelled` |
| 6 | 最新 child 全部 succeeded | `succeeded` |

## 7. Re-run 与 Stop

Re-run 当前规则：

- pending/running job 不能 re-run。
- 只有 attempt series 里的最新 attempt 可以 re-run。
- 普通 `sync_papers` / `find_repos` / `refresh_metadata` re-run 会创建新的 `repair` job。
- Batch folder re-run 是 in-place 行为：不会创建新的 batch folder，只会为 failed、cancelled、pending 或缺失的 child scope 创建 repair child。
- Batch folder re-run 会跳过已经 succeeded 的 child scope。
- Child job 可以单独 re-run，但要求 parent batch folder 不是 `stopping`。
- Batch child re-run 后仍属于同一个 parent batch folder，并在 child 列表里保留原 scope 位置。

Stop 当前规则：

- pending job stop 会立即变成 `cancelled`。
- running job stop 只写入 `stop_requested_at`，由执行中的链路在 stop checkpoint 处抛出 `JobStopRequested` 后变成 `cancelled`。
- Batch folder stop 会给 parent 写 stop request；pending child 立即 cancelled；running child 写 stop request。
- parent batch 已经稳定为 cancelled 后，单独 re-run 某个 child 是允许的；该 child pending/running 时，batch state 会临时回到 queued/running，child 结束后 batch state 再按所有最新 child 的最差稳定状态聚合。

## 8. Resume Model

三条后端链路统一使用同一套 resume 用户心智：

- TTL 是 freshness policy：判断已完成数据什么时候需要重新检查。
- Resume 是 retry progress policy：判断同一个 attempt series 的 repair run 是否可以跳过已经成功完成的工作。
- `force=true` 表示用户要求重新处理当前 scope，不复用 item-level resume。

当前采用两层实现：

| 层级 | 使用链路 | 粒度 | 说明 |
| --- | --- | --- | --- |
| Item-level resume | `find-repos`、`refresh-metadata` | paper / repo | repair run 跳过同一 attempt series 已完成 item。 |
| Request-level checkpoint | `sync-papers` | arXiv request | 复用已成功获取的 arXiv response body，避免重复触发 arXiv 限流。 |

`sync-papers` 不迁移到 item-level resume，因为它真正昂贵且容易失败的是远程 arXiv request；`find-repos` 和 `refresh-metadata` 不引入通用 raw response checkpoint，因为它们的自然恢复粒度是业务 item。

## 9. 公共数据模型

| 表 / 模型 | 用途 |
| --- | --- |
| `papers` / `Paper` | arXiv paper 元数据，由 `sync-papers` 写入，后续链路读取。 |
| `paper_repo_state` / `PaperRepoState` | 每篇 paper 的稳定 repo 解析结果和 find-repos TTL。 |
| `repo_observations` / `RepoObservation` | 每个 provider/surface 对某篇 paper 的观测结果。 |
| `github_repos` / `GitHubRepo` | GitHub repo metadata，由 `refresh-metadata` 写入。 |
| `raw_fetches` / `RawFetch` | provider 原始响应元数据和 body 文件路径。 |
| `sync_papers_arxiv_days` / `SyncPapersArxivDay` | `sync-papers` 的 category/day TTL 完成记录。 |
| `sync_papers_arxiv_archive_appearances` / `SyncPapersArxivArchiveAppearance` | 记录 paper 在 arXiv listing 的 category/archive_month 出现关系。 |
| `sync_papers_arxiv_request_checkpoints` / `SyncPapersArxivRequestCheckpoint` | `sync-papers` repair resume 的请求级 checkpoint。 |
| `job_item_resume_progress` / `JobItemResumeProgress` | `find-repos` 和 `refresh-metadata` repair resume 的 item-level progress。 |

## 10. RawFetch 与 HTTP

所有外部请求成功拿到 body 后会尽量写入 `RawFetch`：

- DB 里按 `provider + surface + request_key` 唯一。
- body 文件写入 `DATA_DIR/raw/<provider>/<surface>/`。
- 文件名包含 request hash 与 content hash。
- `Content-Type` 决定扩展名：json/xml/html/txt。
- 会保存 status code、headers、ETag、Last-Modified、content hash、fetched_at。

HTTP 默认行为：

| 常量 | 当前值 |
| --- | --- |
| `HTTP_TOTAL_TIMEOUT` | 20 秒 |
| `HTTP_CONNECT_TIMEOUT` | 10 秒 |
| `HTTP_MAX_RETRIES` | 2 |
| 可重试状态码 | 429、500、502、503、504 |
| retry base delay | 0.2 秒 |
| retry max delay | 3.0 秒 |
| jitter ratio | 0.1 |

如果响应包含 `Retry-After`，会优先按 `Retry-After` 延迟。

## 11. API 与健康检查

公共 API：

| API | 用途 |
| --- | --- |
| `GET /api/v1/health` | 返回运行环境、queue mode、GitHub auth、provider sequence。 |
| `GET /api/v1/dashboard` | 返回统计、queue summary、recent jobs。 |
| `GET /api/v1/papers` | 按 scope 返回 paper summary。 |
| `GET /api/v1/papers/{arxiv_id}` | 返回 paper detail。 |
| `GET /api/v1/repos` | 返回 scope 内的 GitHub repo metadata。 |
| `GET /api/v1/jobs` | 返回 jobs，可按 parent/filter latest/all。 |
| `GET /api/v1/jobs/{id}` | 返回单个 job。 |
| `GET /api/v1/jobs/{id}/attempts` | 返回同 attempt series 的历史。 |
| `POST /api/v1/jobs/sync-papers` | 启动 sync-papers。 |
| `POST /api/v1/jobs/find-repos` | 启动 find-repos。 |
| `POST /api/v1/jobs/refresh-metadata` | 启动 refresh-metadata。 |
| `POST /api/v1/jobs/{id}/rerun` | re-run job。 |
| `POST /api/v1/jobs/{id}/stop` | stop job。 |

`/health` 当前暴露的 provider sequence：

| Step | Provider sequence |
| --- | --- |
| `sync_papers` | `arxiv_listing`、`arxiv_catchup`、`arxiv_submitted_day`、`arxiv_id_list` |
| `find_repos` | `paper_comment`、`paper_abstract`、`alphaxiv_api`、`alphaxiv_html`、`huggingface_api` |
| `refresh_metadata` | `github_api` |

## 12. 配置项

当前通过 `.env` 暴露的配置：

| Env | 默认值 | 影响范围 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+psycopg://papertorepo:papertorepo@db:5432/papertorepo` | DB 连接。 |
| `DATA_DIR` | `data` | raw fetch 与 export 文件根目录。 |
| `DEFAULT_CATEGORIES` | `cs.CV` | 前端和 health 默认 categories。 |
| `GITHUB_TOKEN` | 空 | GitHub GraphQL/REST 鉴权；存在时 refresh-metadata 优先 GraphQL。 |
| `HUGGINGFACE_TOKEN` | 空 | Hugging Face paper API Bearer token。 |
| `ALPHAXIV_TOKEN` | 空 | AlphaXiv API/HTML Bearer token。 |
| `SYNC_PAPERS_ARXIV_MIN_INTERVAL` | `3.0` | sync-papers 所有 arXiv 请求的最小间隔。 |
| `SYNC_PAPERS_ARXIV_TTL_DAYS` | `30` | sync-papers 已完成 closed day 的重抓 TTL。 |
| `SYNC_PAPERS_ARXIV_ID_BATCH_SIZE` | `100` | id_list feed 每批 arXiv IDs 数量。 |
| `SYNC_PAPERS_ARXIV_LIST_PAGE_SIZE` | `2000` | listing/submitted day 分页大小。 |
| `FIND_REPOS_LINK_TTL_DAYS` | `7` | find-repos 稳定结果下次可重查时间。 |
| `FIND_REPOS_HUGGINGFACE_ENABLED` | `true` | 是否启用 Hugging Face paper API。 |
| `FIND_REPOS_ALPHAXIV_ENABLED` | `true` | 是否启用 AlphaXiv API/HTML。 |
| `FIND_REPOS_HUGGINGFACE_MIN_INTERVAL` | `0.2` | Hugging Face 请求最小间隔。 |
| `FIND_REPOS_ALPHAXIV_MIN_INTERVAL` | `0.2` | AlphaXiv 请求最小间隔。 |
| `FIND_REPOS_WORKER_CONCURRENCY` | `24` | find-repos paper lookup worker 数量上限。 |
| `FIND_REPOS_HUGGINGFACE_MAX_CONCURRENT` | `4` | Hugging Face provider semaphore。 |
| `FIND_REPOS_ALPHAXIV_MAX_CONCURRENT` | `4` | AlphaXiv provider semaphore。 |
| `REFRESH_METADATA_GITHUB_MIN_INTERVAL` | `0.2` | GitHub 有 token 时的最小请求间隔；无 token 时会被提升到至少 60 秒。 |
| `REFRESH_METADATA_GITHUB_GRAPHQL_BATCH_SIZE` | `50` | GitHub GraphQL 每批 repo 数量。 |
| `REFRESH_METADATA_GITHUB_REST_FALLBACK_MAX_CONCURRENT` | `2` | GitHub REST fallback semaphore。 |
| `JOB_QUEUE_WORKER_POLL_SECONDS` | `1.0` | worker 空转轮询间隔。 |
| `JOB_QUEUE_RUNNING_TIMEOUT_SECONDS` | `1800` | running job stale timeout。 |
| `PUBLIC_EXPORT_DOWNLOADS` | `true` | 是否允许公开下载 export。 |
| `CORS_ORIGINS` | `["*"]` | CORS origins。 |

当前没有通过 `.env` 暴露的内部常量：

| 常量 | 当前值 | 影响范围 |
| --- | --- | --- |
| `SYNC_PAPERS_ARXIV_CATCHUP_MAX_AGE_DAYS` | `90` | sync-papers day mode 中 recent day 使用 catchup 的最大天数。 |
| `SYNC_PAPERS_ARXIV_MAX_CONCURRENT` | `1` | sync-papers arXiv client semaphore。 |
| `REFRESH_METADATA_GITHUB_GRAPHQL_MAX_CONCURRENT` | `1` | GitHub GraphQL semaphore。 |
| `REFRESH_METADATA_GITHUB_GRAPHQL_TOPICS_FIRST` | `20` | GraphQL 拉取 repository topics 数量上限。 |
| `REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS` | `60.0` | 无 GitHub token 时 REST 最小间隔下限。 |
| `JOB_QUEUE_INIT_DATABASE_LOCK_ID` | `649183502117041921` | PostgreSQL 初始化迁移 advisory lock。 |

## 13. 当前限制

- 当前系统设计是单 worker / serial queue。代码可检测 stale running job，但不是多 worker 并行调度设计。
- `sync-papers` 有 request-level checkpoint resume；`find-repos` 和 `refresh-metadata` 有 item-level resume。
- `find-repos` 和 `refresh-metadata` 不复用 provider raw response。
- Batch folder 是展示和 scope 聚合概念；真实业务执行发生在 child job。
- `refresh-metadata` 当前没有 TTL，每次运行都会处理 scope 内所有已解析 repo URL。
