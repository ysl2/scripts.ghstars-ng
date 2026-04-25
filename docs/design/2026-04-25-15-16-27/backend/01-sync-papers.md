# Sync Papers 设计

本文档描述当前 `sync-papers` 后端实现。事实来源是当前代码，不继承旧 design snapshot。

## 1. 产品目标

`sync-papers` 负责把 arXiv paper 元数据同步进本地数据库，为后续 `find-repos` 和 `refresh-metadata` 提供基础数据。

它写入：

- `papers`
- `raw_fetches`
- `sync_papers_arxiv_days`
- `sync_papers_arxiv_archive_appearances`
- `sync_papers_arxiv_request_checkpoints`

## 2. 输入与 batch

输入必须包含：

- `categories`
- `day`、`month`、`from/to` 之一

`force` 有效：

- `force=false`：遵守 `SYNC_PAPERS_ARXIV_TTL_DAYS`。
- `force=true`：绕过 TTL，强制请求 arXiv。

Batch 展开规则：

| Scope | 是否 batch | Child scope |
| --- | --- | --- |
| 单 day | 否 | 直接执行。 |
| 单 month | 否 | 直接执行。 |
| `from/to` 在同一个 archive month 内 | 否 | 直接执行，但抓取时仍按对应 archive month 请求 listing。 |
| `from/to` 跨多个 archive month | 是 | 按 category × archive month 展开 child，每个 child 是单 category + 单 month。 |

## 3. arXiv 请求规划

当前有四类 arXiv surface：

| Surface | 触发条件 | 请求目标 | 作用 |
| --- | --- | --- | --- |
| `listing_html` | `month` 或 `from/to` | `https://arxiv.org/list/{category}/{YYYY-MM}?skip=...&show=...` | 从 listing HTML 提取 arXiv IDs。 |
| `catchup_html` | `day` 且目标日期在过去 90 天内 | `https://arxiv.org/catchup/{category}/{YYYY-MM-DD}` | 从 catchup HTML 提取 arXiv IDs。 |
| `submitted_day_feed` | `day` 且目标日期早于过去 90 天 | `https://export.arxiv.org/api/query?search_query=cat:{category} AND submittedDate:[...]` | 从 Atom feed 提取 arXiv IDs。 |
| `id_list_feed` | 上述三类请求拿到 IDs 后 | `https://export.arxiv.org/api/query?id_list=...` | 获取完整 paper metadata 并入库。 |

当前代码事实：

- `listing_html` 和 `catchup_html` 只解析 `/abs/...` 链接得到 arXiv ID。
- `submitted_day_feed` 虽然是 Atom feed，但当前只调用 `parse_arxiv_ids_from_feed()` 提取 arXiv ID。
- 完整 paper metadata 一律来自第二步 `id_list_feed` hydration。
- 当前不会抓取 arXiv abs HTML 页面。

## 4. 数据流

执行流程：

1. 根据 scope 规划 `SyncPapersArxivUnit`。
2. 对每个 unit 获取 advisory lock。
3. 判断 TTL 是否允许跳过。
4. 请求 listing/catchup/submitted day 获取 arXiv IDs。
5. 按 `SYNC_PAPERS_ARXIV_ID_BATCH_SIZE` 把 IDs 分批请求 `id_list_feed`。
6. 解析 Atom entry，upsert `papers`。
7. 记录 archive appearance。
8. 记录 closed days completion，用于 TTL。
9. 持续更新 `stats_json`。

`from/to` 的重要当前行为：

- 请求层面按 archive month 拉取 listing。
- 当前不会在 hydration 后按用户请求的 `from/to` 再过滤 paper。
- 因此一个 partial month sync 可能额外 upsert 同 archive month 但 published_at 不在用户请求窗口内的 paper。
- 后续 dashboard、papers、find-repos、refresh-metadata 的 scope 过滤使用 `Paper.published_at`，不是 archive appearance。

## 5. Paper 入库字段

`id_list_feed` 解析出的字段会映射到 `papers`：

| Paper 字段 | 来源 |
| --- | --- |
| `arxiv_id` | Atom entry id 解析出的规范 arXiv ID。 |
| `entry_id` | Atom `entry.id`。 |
| `abs_url` | 由 arXiv ID 生成的 canonical abs URL。 |
| `title` | Atom `entry.title`，清理 HTML/tag/空白。 |
| `abstract` | Atom `entry.summary`，清理 HTML/tag/空白。 |
| `published_at` | Atom `entry.published`。 |
| `updated_at` | Atom `entry.updated`。 |
| `authors_json` | Atom `entry.author.name`。 |
| `author_details_json` | 作者名与 arXiv affiliation。 |
| `categories_json` | Atom `entry.category.term`。 |
| `category_details_json` | Atom `entry.category.{term,scheme,label}`。 |
| `links_json` | Atom `entry.link.{href,rel,type,title}`。 |
| `comment` | `arxiv:comment`。 |
| `journal_ref` | `arxiv:journal_ref`。 |
| `doi` | `arxiv:doi`。 |
| `primary_category` | `arxiv:primary_category.term`，缺失时 fallback 到第一个 category。 |
| `primary_category_scheme` | `arxiv:primary_category.scheme` 或 category detail scheme。 |
| `source_first_seen_at` | 首次入库时间，仅 create 时设置。 |
| `source_last_seen_at` | 每次 upsert 更新。 |

## 6. TTL

TTL 表是 `sync_papers_arxiv_days`，主键是 `(category, sync_day)`。

当前语义：

- TTL 只用于 `sync-papers`。
- TTL 以自然 day 为粒度，不是以 month 为粒度。
- 只有 closed days 会被记录完成。今天或未来日期不会被 TTL 视为稳定。
- 如果请求窗口内任意一天缺失记录或过期，则该 unit 需要请求 arXiv。
- 如果请求窗口内所有 closed days 都未过期，则跳过该 unit。
- `force=true` 会绕过 TTL。
- 默认 TTL 是 `SYNC_PAPERS_ARXIV_TTL_DAYS=30`。

TTL 的产品含义：

- 它不是断点续跑机制。
- 它用于避免重复抓取近期已经完整同步过的 closed day。
- 它允许过去的 paper 元数据在 TTL 过期后重新从 arXiv 获取后续更新。

## 7. Checkpoint / 断点续跑

`sync-papers` 当前有真正的请求级 checkpoint resume。

Checkpoint 表：`sync_papers_arxiv_request_checkpoints`。

唯一键：

- `attempt_series_key`
- `surface`
- `request_key`

写入条件：

- arXiv 请求成功返回 status/body。
- 对应 raw fetch 已写入 `raw_fetches`。
- 当前 job 有 `attempt_series_key`。

复用条件：

- 当前 job 是 `attempt_mode=repair`。
- 同一 `attempt_series_key` 下存在相同 `surface + request_key` checkpoint。
- checkpoint 指向的 body file 仍然存在。

Fresh run 行为：

- `attempt_mode=fresh` 永远不复用旧 checkpoint。
- 即使 scope 完全相同，也会重新请求。

Repair run 行为：

- 已成功请求过的 listing/catchup/submitted/id_list 会直接复用 checkpoint body。
- 失败前未完成的请求继续真实请求 arXiv。
- 前端会通过 `repair_resume_json` 展示可复用 checkpoint 数量、上次 attempt 状态、上次 stats。

## 8. 限速、并发和分页

当前配置和常量：

| 名称 | 当前值 | 说明 |
| --- | --- | --- |
| `SYNC_PAPERS_ARXIV_MIN_INTERVAL` | `3.0` | 所有 sync-papers arXiv 请求的最小间隔。 |
| `SYNC_PAPERS_ARXIV_MAX_CONCURRENT` | `1` | arXiv client semaphore。 |
| `SYNC_PAPERS_ARXIV_ID_BATCH_SIZE` | `100` | id_list hydration 每批 IDs。 |
| `SYNC_PAPERS_ARXIV_LIST_PAGE_SIZE` | `2000` | listing/submitted day 每页大小。 |
| `SYNC_PAPERS_ARXIV_CATCHUP_MAX_AGE_DAYS` | `90` | day mode 使用 catchup 的最大过去天数。 |

Rate limiter 当前是 event-loop scoped：

- `_SYNC_PAPERS_ARXIV_RATE_LIMITERS` 按 event loop 存储。
- 同一 event loop 内按 interval 复用同一个 limiter。
- 这样一个 worker 内连续 child jobs 会共享 arXiv 请求间隔。

## 9. Advisory lock

每个 unit 会尝试获取 PostgreSQL advisory lock：

- day：`arxiv:{category}:{transport}:{day}`
- month：`arxiv:{category}:{transport}:{YYYY-MM}`

如果拿不到 lock：

- 当前 unit 跳过。
- `stats_json.categories_skipped_locked` 增加。

SQLite 测试环境中 advisory lock 永远返回成功。

## 10. Stats

`sync-papers` 主要 stats：

| 字段 | 含义 |
| --- | --- |
| `categories` | categories 数量。 |
| `papers_upserted` | upsert paper 数量。 |
| `pages_fetched` | 实际远程请求页数，不含 checkpoint reuse。 |
| `search_pages_fetched` | submitted day feed 页数。 |
| `listing_pages_fetched` | listing HTML 页数。 |
| `catchup_pages_fetched` | catchup HTML 页数。 |
| `metadata_batches_fetched` | id_list feed 批次数。 |
| `checkpoint_reused` | repair 中复用 checkpoint 总数。 |
| `checkpoint_pages_reused` | 复用 listing/catchup/submitted 页数。 |
| `checkpoint_metadata_batches_reused` | 复用 id_list 批次数。 |
| `categories_skipped_locked` | advisory lock 冲突跳过次数。 |
| `windows_skipped_ttl` | TTL 跳过 unit 次数。 |
| `provider_counts.arxiv.*` | arXiv surface 请求计数。 |
| `stage_seconds.*` | 各阶段耗时。 |
| `elapsed_seconds` | job 已运行秒数。 |
| `papers_per_minute` | 运行吞吐估算。 |

## 11. 当前限制

- `submitted_day_feed` 当前没有直接入库完整 Atom metadata，而是只取 ID 后继续走 `id_list_feed`。
- `from/to` partial month 会按 archive month 获取，可能 upsert 请求窗口外的同 archive month papers。
- `sync-papers` 的 checkpoint resume 是请求级，不是 paper 级。
- 如果 checkpoint body 文件被删除，对应 checkpoint 不会复用。
- arXiv 限流依然可能发生；checkpoint resume 的目标是避免 repair 重复请求已经成功的页面。
