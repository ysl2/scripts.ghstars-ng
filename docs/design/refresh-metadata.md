# refresh-metadata

## 1. 目标

`refresh-metadata` 的目标是刷新 scope 内已发现 GitHub 仓库的元数据，让系统里保留的是“最近一次可验证的仓库信息”。

当前代码命令名是 `enrich`，design 中统一称为 `refresh-metadata`，因为它更直接表达业务含义。

它解决的问题是：

- 把论文级 repo URL 变成仓库级结构化信息
- 定期刷新 stars、topics、description 等动态字段
- 为导出、排序、筛选和展示提供 repo 维度的数据

它不负责：

- 重新寻找 repo URL
- 从 GitHub 下载代码内容

## 2. 输入与候选 repo 集合

### 2.1 必填输入

- `categories`

### 2.2 支持的范围

- `day`
- `month`
- `from` + `to`

### 2.3 候选 repo 的来源

系统先按 scope 选出论文，再从这些论文的 `PaperRepoState.repo_urls_json` 汇总出去重后的 GitHub URL 集合。

因此，`refresh-metadata` 的输入前提是：

- 论文已经存在
- 且 `find-repos` 至少为部分论文找到了 repo URL

## 3. 刷新策略

### 3.1 有 GitHub Token 时

如果配置了 `GITHUB_TOKEN`，系统采用两级策略：

1. 先用 GraphQL 批量刷新
2. 对 GraphQL 无法解析、报错或未命中的 repo，再回退到 REST 单仓库请求

这条路径优先追求吞吐和配额效率。

### 3.2 没有 GitHub Token 时

如果没有 `GITHUB_TOKEN`，系统直接走 REST 单仓库刷新。

为了降低匿名访问被限流的风险，REST 请求节流会被提升到一个更保守的水平。

## 4. REST 与 GraphQL 的结果语义

### 4.1 GraphQL

GraphQL 批量请求的目标是尽可能一次取回：

- 基本仓库标识
- stars
- description
- homepage
- topics
- license
- archived
- pushed_at

GraphQL 某一项失败，不会直接判定整个 repo 无法刷新，而是把该 repo 放入 REST fallback 队列。

### 4.2 REST

REST 刷新支持三种核心结果：

- `ok`
- `not_modified`，对应 304
- `missing`，对应 404

它们的业务含义分别是：

- `ok`：拿到新的 repo 元数据并更新库内记录
- `not_modified`：远端内容未变化，只刷新本地检查时间
- `missing`：仓库不存在或不可见，记录一次缺失结果

## 5. 落库规则

### 5.1 更新哪些字段

每次成功拿到 metadata 时，系统会刷新动态字段，例如：

- `stars`
- `description`
- `homepage`
- `topics`
- `license`
- `archived`
- `pushed_at`

### 5.2 哪些字段尽量保留初始化值

以下字段更偏“首次建档信息”，不会在每次刷新时被重置：

- `first_seen_at`
- `github_id`，一旦已有值就沿用
- `created_at`，一旦已有值就沿用

### 5.3 `missing` 的处理

当 REST 返回 `missing` 时：

- 如果 repo 记录已存在，只更新 `checked_at`
- 如果 repo 记录还不存在，不会凭空创建一条空 repo 记录

这条规则的目标是避免把“远端不存在”误建模成“本地有一个内容为空的仓库对象”。

## 6. 原始抓取与可追溯性

无论走 GraphQL 还是 REST，系统都会保留原始抓取快照。

这样做的价值是：

- 追查配额、权限、限流问题
- 验证字段解析是否正确
- 复盘某次 metadata 为什么被判成 `missing` 或 `not_modified`

## 7. 并发与锁

`refresh-metadata` 会逐个 repo 刷新，但每个 repo 在落库前都会先尝试资源锁。

规则是：

- 锁拿到才更新该 repo
- 锁拿不到则跳过，并记入统计

这和 `find-repos` 一样，优先保证全局可推进，而不是让一个资源阻塞整个任务。

## 8. 批量拆分规则

当 scope 覆盖多个月份窗口时，任务会先拆成批量根任务，再按月份优先生成子任务。

拆分目标不是为了并行，而是为了：

- 控制失败影响面
- 让修复更细粒度
- 让任务历史更容易解释

## 9. 关键配置项

- `GITHUB_TOKEN`
- `GITHUB_MIN_INTERVAL`
- `GITHUB_GRAPHQL_BATCH_SIZE`
- `GITHUB_REST_FALLBACK_MAX_CONCURRENT`

这些配置分别影响：

- 是否启用 GraphQL 批量路径
- GitHub 请求的基础节流速度
- GraphQL 每批刷新的 repo 数量
- REST fallback 的并发度

当前还有一个隐含规则：

- 无 Token 时，REST 的有效最小间隔不会低于 60 秒

这是为了降低匿名访问被平台限流的概率。

## 10. 当前写死但需要让用户知道的常量

### 10.1 GraphQL 批刷新并发固定为 `1`

当前实现里，GitHub GraphQL 批量请求在单个任务内部是串行发出的，一次只会有 `1` 个 batch 在飞。

这意味着：

- 即使 `GITHUB_GRAPHQL_BATCH_SIZE` 调大，GraphQL 侧也不是多批并发
- 这是一条当前写死的吞吐上限

### 10.2 GraphQL topics 只取前 `20` 个

GraphQL 查询当前固定只读取仓库 topic 的前 `20` 项。

这意味着：

- topic 非常多的仓库，库内保存的 topic 可能是截断结果
- 这个上限当前不是配置项

### 10.3 匿名 REST 访问的最小间隔下限固定为 `60` 秒

如果没有 `GITHUB_TOKEN`，系统会把 REST 刷新的有效最小间隔强制抬到至少 `60` 秒。

这意味着：

- 即使用户把 `GITHUB_MIN_INTERVAL` 配得更小，匿名模式也不会按更小值运行
- 这个保护下限是写死的，不是配置项
