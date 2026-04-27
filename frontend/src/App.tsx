import {
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import type { CustomCellRendererProps } from 'ag-grid-react'
import AgGridSheet, { type SheetColumn } from './components/AgGridSheet'
import {
  compactDateColumnFilter,
  compactDateFilterParams,
  compactNumberColumnFilter,
  compactNumberFilterParams,
  compactValueColumnFilter,
  createCompactSetFilterParams,
} from './components/CompactColumnFilter'
import HoverTooltip from './components/HoverTooltip'
import usePointerDownOutside from './hooks/usePointerDownOutside'
import './App.css'

type JobStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled'
type BatchState = 'queued' | 'running' | 'stopping' | 'succeeded' | 'failed' | 'cancelled'
type JobAttemptMode = 'fresh' | 'repair'
type LinkStatus = 'found' | 'not_found' | 'ambiguous' | 'unknown'
type PreviewTab = 'papers' | 'jobs' | 'exports'
type TimeMode = 'day' | 'month' | 'range'
type StepJob = 'sync-papers' | 'find-repos' | 'refresh-metadata' | 'export'
type ExportMode = 'all_papers' | 'papers_view'

type Health = {
  app_name: string
  api_prefix: string
  default_categories: string[]
  database_dialect: string
  queue_mode: 'serial'
  github_auth_configured: boolean
  effective_github_min_interval_seconds: number
  step_providers: Record<string, string[]>
}

type Job = {
  id: string
  parent_job_id: string | null
  job_type: string
  status: JobStatus
  attempt_mode: JobAttemptMode
  attempt_series_key: string
  scope_json: Record<string, unknown>
  dedupe_key: string
  stats_json: Record<string, unknown>
  repair_resume_json: Record<string, unknown> | null
  error_text: string | null
  stop_requested_at: string | null
  stop_reason: string | null
  created_at: string
  started_at: string | null
  finished_at: string | null
  attempts: number
  locked_by: string | null
  locked_at: string | null
  batch_state: BatchState | null
  child_summary: ChildSummary | null
  attempt_count: number
  attempt_rank: number
}

type JobLaunch = {
  disposition: 'created'
  job: Job
}

type LaunchFeedback = {
  stepJob: StepJob
  job: Job | null
}

type ChildSummary = {
  total: number
  pending: number
  running: number
  stopping: number
  succeeded: number
  failed: number
  cancelled: number
}

type JobQueueSummaryState = 'idle' | 'waiting' | 'active'

type JobQueueSummary = {
  state: JobQueueSummaryState
  running: number
  pending: number
  stopping: number
  current_job: Job | null
  next_job: Job | null
}

type Dashboard = {
  papers: number
  found: number
  not_found: number
  ambiguous: number
  unknown: number
  repos: number
  exports: number
  pending_jobs: number
  running_jobs: number
  stopping_jobs: number
  job_queue_summary: JobQueueSummary
  recent_jobs: Job[]
}

type PaperSummary = {
  arxiv_id: string
  abs_url: string
  title: string
  published_at: string | null
  updated_at: string | null
  authors_json: string[]
  categories_json: string[]
  primary_category: string | null
  comment: string | null
  journal_ref: string | null
  link_status: LinkStatus
  primary_github_url: string | null
  primary_github_stargazers_count: number | null
  primary_github_language: string | null
  primary_github_size_kb: number | null
  primary_github_created_at: string | null
  primary_github_pushed_at: string | null
  primary_github_description: string | null
  stable_decided_at: string | null
  refresh_after: string | null
  last_attempt_at: string | null
  last_attempt_complete: boolean
  last_attempt_error: string | null
}

type PaperDetail = PaperSummary & {
  abstract: string
  doi: string | null
  github_urls: string[]
}

type Repo = {
  github_url: string
  github_id: number | null
  node_id: string | null
  name_with_owner: string | null
  description: string | null
  homepage: string | null
  stargazers_count: number | null
  forks_count: number | null
  size_kb: number | null
  primary_language: string | null
  topic: string | null
  license_spdx_id: string | null
  license_name: string | null
  default_branch: string | null
  is_private: boolean | null
  visibility: string | null
  is_fork: boolean | null
  is_archived: boolean | null
  is_template: boolean | null
  is_disabled: boolean | null
  has_issues: boolean | null
  has_projects: boolean | null
  has_wiki: boolean | null
  has_discussions: boolean | null
  allow_forking: boolean | null
  web_commit_signoff_required: boolean | null
  parent_github_url: string | null
  created_at: string | null
  updated_at: string | null
  pushed_at: string | null
}

type ExportRow = {
  id: string
  file_name: string
  file_path: string
  scope_json: Record<string, unknown>
  created_at: string
}

type ScopeState = {
  categories: string
  timeMode: TimeMode
  day: string
  month: string
  from: string
  to: string
}

type PersistedScopeState = {
  categories: string
  timeMode: TimeMode
  day: string | null
  month: string | null
  from: string | null
  to: string | null
}

type ResolvedScope = {
  categories: string
  day: string | null
  month: string | null
  from: string | null
  to: string | null
}

type ScopeResolution = {
  payload: ResolvedScope
  error: string | null
}

type JobRowKind = 'root' | 'child' | 'history'

type JobGridRow = Record<string, unknown> & {
  id?: unknown
  row_kind?: unknown
  row_depth?: unknown
  history_depth?: unknown
  children_toggleable?: unknown
  children_expanded?: unknown
  children_loading?: unknown
  can_rerun?: unknown
  rerun_busy?: unknown
  history_toggleable?: unknown
  history_expanded?: unknown
  history_loading?: unknown
  attempt_count?: unknown
  attempt_rank?: unknown
  attempt_relation_label?: unknown
  can_stop?: unknown
  stop_busy?: unknown
}

type JobRerunCellRendererProps = CustomCellRendererProps<JobGridRow> & {
  onRerun?: (jobId: string) => void
}

type JobStopCellRendererProps = CustomCellRendererProps<JobGridRow> & {
  onStop?: (jobId: string) => void
}

type JobChildrenChevronCellRendererProps = CustomCellRendererProps<JobGridRow> & {
  onToggleChildren?: (jobId: string) => void
}

type JobHistoryChevronCellRendererProps = CustomCellRendererProps<JobGridRow> & {
  onToggleHistory?: (jobId: string) => void
}

type JobAttemptCellRendererProps = CustomCellRendererProps<JobGridRow>

const PAPER_BATCH_SIZE = 1000
const REPO_PREVIEW_LIMIT = 10000
const JOB_PREVIEW_LIMIT = 500
const ACTIVE_DASHBOARD_POLL_MS = 1000
const IDLE_DASHBOARD_POLL_MS = 8000
const ACTIVE_JOBS_POLL_MS = 1000
const PASSIVE_JOBS_POLL_MS = 5000
const LEGACY_SCOPE_STORAGE_KEY = 'papertorepo:scope:v3'
const OLDER_SCOPE_STORAGE_KEY = 'papertorepo:scope:v4'
const PREVIOUS_SCOPE_STORAGE_KEY = 'papertorepo:scope:v5'
const SCOPE_STORAGE_KEY = 'papertorepo:scope:v6'
const MONTH_START_YEAR = 1991
const MONTH_PATTERN = /^\d{4}-(0[1-9]|1[0-2])$/
const DATE_PATTERN = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/
const ARXIV_CATEGORY_PATTERN = /^[a-z]+(?:-[a-z]+)*(?:\.[A-Za-z-]+)?$/
const CATEGORIES_HINT = 'cs.CV, cs.LG'
const RANGE_ORDER_HINT = 'From ≤ To'

function toDateInputValue(value: Date) {
  const year = value.getFullYear()
  const month = String(value.getMonth() + 1).padStart(2, '0')
  const day = String(value.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function toMonthInputValue(value: Date) {
  return toDateInputValue(value).slice(0, 7)
}

function addDays(value: Date, delta: number) {
  const next = new Date(value)
  next.setDate(next.getDate() + delta)
  return next
}

function hasTypedValue(value: string | null | undefined) {
  return typeof value === 'string' && value.trim().length > 0
}

function categoriesValidationMessage(value: string | null | undefined) {
  const normalized = typeof value === 'string' ? value.trim() : ''
  if (!normalized) return `Enter categories as comma-separated arXiv fields, e.g. ${CATEGORIES_HINT}.`
  const rawTokens = normalized.split(',')
  if (rawTokens.some((token) => token.trim().length === 0)) {
    return `Enter categories as comma-separated arXiv fields, e.g. ${CATEGORIES_HINT}.`
  }
  const tokens = rawTokens.map((token) => token.trim())
  if (tokens.some((token) => !ARXIV_CATEGORY_PATTERN.test(token))) {
    return `Enter categories as comma-separated arXiv fields, e.g. ${CATEGORIES_HINT}.`
  }
  return null
}

function isRealDateString(value: string) {
  if (!DATE_PATTERN.test(value)) return false
  const [yearString, monthString, dayString] = value.split('-')
  const year = Number(yearString)
  const month = Number(monthString)
  const day = Number(dayString)
  const candidate = new Date(Date.UTC(year, month - 1, day))
  return candidate.getUTCFullYear() === year && candidate.getUTCMonth() === month - 1 && candidate.getUTCDate() === day
}

function dateValidationMessage(value: string | null | undefined) {
  if (typeof value !== 'string' || !isRealDateString(value)) return 'Enter date as YYYY-MM-DD.'
  return null
}

function monthValidationMessage(value: string | null | undefined) {
  if (typeof value !== 'string' || !MONTH_PATTERN.test(value)) return 'Enter month as YYYY-MM.'
  const year = Number(value.slice(0, 4))
  const currentYear = new Date().getFullYear()
  if (year < MONTH_START_YEAR || year > currentYear + 1) return 'Enter month as YYYY-MM.'
  return null
}

function normalizeTimeMode(value: unknown): TimeMode {
  return value === 'month' || value === 'range' ? value : 'day'
}

function joinLegacyMonthValue(monthYear: unknown, monthNumber: unknown) {
  const year = typeof monthYear === 'string' ? monthYear.trim() : ''
  const month = typeof monthNumber === 'string' ? monthNumber.trim() : ''
  if (!year && !month) return ''
  return `${year}-${month}`
}

function restoreValidDate(value: unknown, fallback: string) {
  return typeof value === 'string' && dateValidationMessage(value) === null ? value : fallback
}

function restoreValidMonth(value: unknown, fallback: string) {
  return typeof value === 'string' && monthValidationMessage(value) === null ? value : fallback
}

function restoreValidRange(values: Pick<PersistedScopeState, 'from' | 'to'>, fallback: Pick<ScopeState, 'from' | 'to'>) {
  if (
    typeof values.from === 'string' &&
    typeof values.to === 'string' &&
    dateValidationMessage(values.from) === null &&
    dateValidationMessage(values.to) === null &&
    values.from <= values.to
  ) {
    return {
      from: values.from,
      to: values.to,
    }
  }

  return {
    from: fallback.from,
    to: fallback.to,
  }
}

function restoreValidCategories(value: unknown, fallback: string) {
  return typeof value === 'string' && categoriesValidationMessage(value) === null ? value.trim() : fallback
}

function createPersistedScopeSnapshot(scope: ScopeState): PersistedScopeState {
  return {
    categories: scope.categories,
    timeMode: scope.timeMode,
    day: scope.day,
    month: scope.month,
    from: scope.from,
    to: scope.to,
  }
}

function createPersistableScope(scope: ScopeState, previous: PersistedScopeState): PersistedScopeState {
  const rangeValid =
    dateValidationMessage(scope.from) === null &&
    dateValidationMessage(scope.to) === null &&
    scope.from <= scope.to

  return {
    categories: categoriesValidationMessage(scope.categories) === null ? scope.categories.trim() : previous.categories,
    timeMode: scope.timeMode,
    day: dateValidationMessage(scope.day) === null ? scope.day : null,
    month: monthValidationMessage(scope.month) === null ? scope.month : null,
    from: rangeValid ? scope.from : null,
    to: rangeValid ? scope.to : null,
  }
}

function samePersistedScopeState(left: PersistedScopeState, right: PersistedScopeState) {
  return (
    left.categories === right.categories &&
    left.timeMode === right.timeMode &&
    left.day === right.day &&
    left.month === right.month &&
    left.from === right.from &&
    left.to === right.to
  )
}

function defaultScopeState(): ScopeState {
  const today = new Date()
  return {
    categories: '',
    timeMode: 'day',
    day: toDateInputValue(today),
    month: toMonthInputValue(today),
    from: toDateInputValue(addDays(today, -1)),
    to: toDateInputValue(today),
  }
}

function loadSavedScope(): ScopeState {
  const defaults = defaultScopeState()
  if (typeof window === 'undefined') return defaults
  try {
    const raw =
      window.localStorage.getItem(SCOPE_STORAGE_KEY) ??
      window.localStorage.getItem(PREVIOUS_SCOPE_STORAGE_KEY) ??
      window.localStorage.getItem(OLDER_SCOPE_STORAGE_KEY) ??
      window.localStorage.getItem(LEGACY_SCOPE_STORAGE_KEY)
    if (!raw) return defaults
    const parsed = JSON.parse(raw) as Partial<PersistedScopeState> & { monthYear?: unknown; monthNumber?: unknown; month?: unknown }
    const monthCandidate =
      typeof parsed.month === 'string' || parsed.month === null ? parsed.month : joinLegacyMonthValue(parsed.monthYear, parsed.monthNumber)
    const restoredRange = restoreValidRange(
      {
        from: parsed.from ?? null,
        to: parsed.to ?? null,
      },
      defaults,
    )
    return {
      categories: restoreValidCategories(parsed.categories, defaults.categories),
      timeMode: normalizeTimeMode(parsed.timeMode),
      day: restoreValidDate(parsed.day, defaults.day),
      month: restoreValidMonth(monthCandidate, defaults.month),
      from: restoredRange.from,
      to: restoredRange.to,
    }
  } catch {
    return defaults
  }
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
    ...init,
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || `Request failed: ${response.status}`)
  }
  return response.json() as Promise<T>
}

function isAbortError(error: unknown) {
  return (
    (error instanceof DOMException && error.name === 'AbortError') ||
    (error instanceof Error && error.name === 'AbortError')
  )
}

function resolveScope(scope: ScopeState): ScopeResolution {
  const payload: ResolvedScope = {
    categories: scope.categories.trim(),
    day: null,
    month: null,
    from: null,
    to: null,
  }

  const categoriesError = categoriesValidationMessage(scope.categories)
  if (categoriesError) {
    return {
      payload,
      error: categoriesError,
    }
  }

  if (scope.timeMode === 'day') {
    const dayError = dateValidationMessage(scope.day)
    if (dayError) {
      return {
        payload,
        error: dayError,
      }
    }
    payload.day = scope.day
    return { payload, error: null }
  }

  if (scope.timeMode === 'month') {
    const monthError = monthValidationMessage(scope.month)
    if (monthError) {
      return {
        payload,
        error: monthError,
      }
    }
    payload.month = scope.month
    return { payload, error: null }
  }

  const fromError = dateValidationMessage(scope.from)
  const toError = dateValidationMessage(scope.to)
  if (fromError || toError) {
    return {
      payload,
      error: 'Enter both dates as YYYY-MM-DD.',
    }
  }
  if (scope.from > scope.to) {
    return {
      payload,
      error: 'From must be earlier than or equal to To.',
    }
  }
  payload.from = scope.from
  payload.to = scope.to
  return { payload, error: null }
}

function formatTime(value: string | null) {
  if (!value) return '—'
  try {
    return new Intl.DateTimeFormat('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    }).format(new Date(value))
  } catch {
    return value
  }
}

function formatDate(value: string | null) {
  if (!value) return '—'
  try {
    return new Intl.DateTimeFormat('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date(value))
  } catch {
    return value
  }
}

function formatClock(value: string | null) {
  if (!value) return '—'
  try {
    return new Intl.DateTimeFormat('zh-CN', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    }).format(new Date(value))
  } catch {
    return value
  }
}

function formatSeconds(value: number | null | undefined) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—'
  const rounded = Number.isInteger(value) ? String(value) : value.toFixed(1).replace(/\.0$/, '')
  return `${rounded}s`
}

function formatInteger(value: number) {
  return value.toLocaleString('en-US')
}

function pluralize(value: number, singular: string, plural = `${singular}s`) {
  return `${formatInteger(value)} ${value === 1 ? singular : plural}`
}

function numericStat(stats: Record<string, unknown>, key: string) {
  const value = stats[key]
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function summarizeStats(stats: Record<string, unknown>, emptyLabel = 'no stats yet') {
  const items = Object.entries(stats)
    .filter(([, value]) => value !== null && value !== '' && value !== 0)
    .slice(0, 5)
  if (items.length === 0) return emptyLabel
  return items.map(([key, value]) => `${key}: ${String(value)}`).join(' · ')
}

function isBatchRootType(jobType: string) {
  return jobType === 'sync_papers_batch' || jobType === 'find_repos_batch' || jobType === 'refresh_metadata_batch'
}

function isReusedChildStats(stats: Record<string, unknown>) {
  return stats.reused === true && typeof stats.reused_from_job_id === 'string'
}

function reusedChildSummary(stats: Record<string, unknown>) {
  if (!isReusedChildStats(stats)) return null
  return `Reused from previous success · ${shortId(String(stats.reused_from_job_id))}`
}

function syncPapersStatsSummary(job: Job, emptyLabel: string) {
  const stats = job.stats_json
  const parts: string[] = []
  const categories = numericStat(stats, 'categories')
  const papersSaved = numericStat(stats, 'papers_upserted')
  const pagesFetched = numericStat(stats, 'pages_fetched')
  const listingPages = numericStat(stats, 'listing_pages_fetched')
  const metadataBatches = numericStat(stats, 'metadata_batches_fetched')
  const checkpointsReused = numericStat(stats, 'checkpoint_reused')
  const checkpointPages = numericStat(stats, 'checkpoint_pages_reused')
  const checkpointMetadataBatches = numericStat(stats, 'checkpoint_metadata_batches_reused')
  const ttlSkips = numericStat(stats, 'windows_skipped_ttl')

  if (categories !== null && categories > 0) parts.push(pluralize(categories, 'category', 'categories'))
  if (papersSaved !== null && papersSaved > 0) parts.push(`${formatInteger(papersSaved)} papers saved`)
  if (pagesFetched !== null && pagesFetched > 0) parts.push(pluralize(pagesFetched, 'remote request'))
  if (listingPages !== null && listingPages > 0) parts.push(pluralize(listingPages, 'listing page'))
  if (metadataBatches !== null && metadataBatches > 0) parts.push(pluralize(metadataBatches, 'metadata batch', 'metadata batches'))
  if (checkpointsReused !== null && checkpointsReused > 0) {
    const checkpointParts: string[] = []
    if (checkpointPages !== null && checkpointPages > 0) checkpointParts.push(pluralize(checkpointPages, 'page'))
    if (checkpointMetadataBatches !== null && checkpointMetadataBatches > 0) {
      checkpointParts.push(pluralize(checkpointMetadataBatches, 'metadata batch', 'metadata batches'))
    }
    parts.push(`${pluralize(checkpointsReused, 'checkpoint reused', 'checkpoints reused')}${checkpointParts.length > 0 ? ` (${checkpointParts.join(', ')})` : ''}`)
  }
  if (ttlSkips !== null && ttlSkips > 0) parts.push(pluralize(ttlSkips, 'TTL skip'))

  if (parts.length > 0) return parts.join(' · ')
  return repairResumeSummary(job) || emptyLabel
}

function repairResumeSummary(job: Job) {
  const resume = job.repair_resume_json
  if (!resume) return null
  const previousStats = resume.previous_stats_json
  const previousStatsJson = previousStats && typeof previousStats === 'object' && !Array.isArray(previousStats) ? (previousStats as Record<string, unknown>) : {}
  const checkpoints = resume.checkpoints
  const checkpointJson = checkpoints && typeof checkpoints === 'object' && !Array.isArray(checkpoints) ? (checkpoints as Record<string, unknown>) : {}
  const bySurface = checkpointJson.by_surface
  const bySurfaceJson = bySurface && typeof bySurface === 'object' && !Array.isArray(bySurface) ? (bySurface as Record<string, unknown>) : {}
  const resumeItems = resume.resume_items
  const resumeItemsJson = resumeItems && typeof resumeItems === 'object' && !Array.isArray(resumeItems) ? (resumeItems as Record<string, unknown>) : {}

  const parts: string[] = []
  const totalCheckpoints = numericStat(checkpointJson, 'total')
  const totalResumeItems = numericStat(resumeItemsJson, 'total')
  const itemKind = typeof resumeItemsJson.item_kind === 'string' && resumeItemsJson.item_kind ? resumeItemsJson.item_kind : 'item'
  const listingCheckpoints = numericStat(bySurfaceJson, 'listing_html')
  const submittedDayCheckpoints = numericStat(bySurfaceJson, 'submitted_day_feed')
  const catchupCheckpoints = numericStat(bySurfaceJson, 'catchup_html')
  const metadataCheckpoints = numericStat(bySurfaceJson, 'id_list_feed')
  const previousPages = numericStat(previousStatsJson, 'pages_fetched')
  const previousListingPages = numericStat(previousStatsJson, 'listing_pages_fetched')
  const previousMetadataBatches = numericStat(previousStatsJson, 'metadata_batches_fetched')
  const previousPapers = numericStat(previousStatsJson, 'papers_upserted')

  if (totalCheckpoints !== null && totalCheckpoints > 0) {
    const checkpointParts: string[] = []
    if (listingCheckpoints !== null && listingCheckpoints > 0) checkpointParts.push(pluralize(listingCheckpoints, 'listing checkpoint'))
    if (submittedDayCheckpoints !== null && submittedDayCheckpoints > 0) checkpointParts.push(pluralize(submittedDayCheckpoints, 'submitted-day checkpoint'))
    if (catchupCheckpoints !== null && catchupCheckpoints > 0) checkpointParts.push(pluralize(catchupCheckpoints, 'catchup checkpoint'))
    if (metadataCheckpoints !== null && metadataCheckpoints > 0) checkpointParts.push(pluralize(metadataCheckpoints, 'metadata checkpoint'))
    parts.push(`${formatInteger(totalCheckpoints)} reusable checkpoints${checkpointParts.length > 0 ? ` (${checkpointParts.join(', ')})` : ''}`)
  }
  if (totalResumeItems !== null && totalResumeItems > 0) {
    parts.push(pluralize(totalResumeItems, `completed ${itemKind}`))
  }
  if (previousPages !== null && previousPages > 0) parts.push(`${formatInteger(previousPages)} requests completed previously`)
  if (previousListingPages !== null && previousListingPages > 0) parts.push(pluralize(previousListingPages, 'listing page'))
  if (previousMetadataBatches !== null && previousMetadataBatches > 0) parts.push(pluralize(previousMetadataBatches, 'metadata batch', 'metadata batches'))
  if (previousPapers !== null && previousPapers > 0) parts.push(`${formatInteger(previousPapers)} papers saved previously`)

  const previousStatus = typeof resume.previous_status === 'string' ? resume.previous_status : null
  const previousJobId = typeof resume.previous_job_id === 'string' ? resume.previous_job_id : null
  if (parts.length === 0 && previousStatus) parts.push(`Previous attempt ${previousStatus}`)
  if (previousJobId) parts.push(`from ${shortId(previousJobId)}`)
  return parts.length > 0 ? `Repair resume · ${parts.join(' · ')}` : null
}

function shortId(value: string) {
  return value.slice(0, 8)
}

function attemptModeLabel(attemptMode: JobAttemptMode) {
  return attemptMode === 'repair' ? 'Repair rerun' : 'Fresh run'
}

function attemptGroupKey(job: Pick<Job, 'attempt_series_key'>) {
  return job.attempt_series_key
}

function rerunCountLabel(attemptCount: number) {
  const reruns = Math.max(0, attemptCount - 1)
  if (reruns <= 0) return 'Fresh run'
  return reruns === 1 ? '1 rerun' : `${formatInteger(reruns)} reruns`
}

function isLatestAttempt(job: Pick<Job, 'attempt_rank'>) {
  return job.attempt_rank === 1
}

function jobTypeLabel(jobType: string) {
  switch (jobType) {
    case 'sync_papers':
      return 'Sync papers'
    case 'sync_papers_batch':
      return 'Paper sync batch'
    case 'find_repos':
      return 'Find repos'
    case 'find_repos_batch':
      return 'Repo lookup batch'
    case 'refresh_metadata':
      return 'Refresh metadata'
    case 'refresh_metadata_batch':
      return 'Metadata batch'
    case 'export':
      return 'Export'
    default:
      return jobType
  }
}

function batchFolderLabel(jobType: string) {
  switch (jobType) {
    case 'sync_papers_batch':
      return 'Paper sync batch folder'
    case 'find_repos_batch':
      return 'Repo lookup batch folder'
    case 'refresh_metadata_batch':
      return 'Metadata batch folder'
    default:
      return 'Batch folder'
  }
}

function stepJobLabel(stepJob: StepJob) {
  switch (stepJob) {
    case 'sync-papers':
      return 'Sync papers'
    case 'find-repos':
      return 'Find repos'
    case 'refresh-metadata':
      return 'Refresh metadata'
    case 'export':
      return 'Export'
  }
}

function jobDisplayStatus(job: Job) {
  if (job.status === 'running' && job.stop_requested_at) return 'stopping'
  return job.batch_state || job.status
}

function childSummaryLabel(summary: ChildSummary | null) {
  if (!summary) return '—'
  const parts = [`${summary.succeeded}/${summary.total} succeeded`]
  if (summary.stopping > 0) parts.push(`${summary.stopping} stopping`)
  if (summary.running > 0) parts.push(`${summary.running} running`)
  if (summary.pending > 0) parts.push(`${summary.pending} queued`)
  if (summary.cancelled > 0) parts.push(`${summary.cancelled} cancelled`)
  if (summary.failed > 0) parts.push(`${summary.failed} failed`)
  return parts.join(' · ')
}

function batchAttemptSummary(job: Job) {
  const reused = numericStat(job.stats_json, 'children_reused_success') ?? 0
  const enqueued = numericStat(job.stats_json, 'children_enqueued') ?? 0
  const existing = numericStat(job.stats_json, 'children_existing') ?? 0
  const parts: string[] = []
  if (reused > 0) parts.push(`${formatInteger(reused)} reused`)
  if (enqueued > 0) parts.push(`${formatInteger(enqueued)} queued`)
  if (existing > 0) parts.push(`${formatInteger(existing)} existing`)
  return parts.join(' · ')
}

function jobSummary(job: Job) {
  const displayStatus = jobDisplayStatus(job)
  if (job.error_text && displayStatus !== 'stopping') return job.error_text
  const reusedSummary = reusedChildSummary(job.stats_json)
  if (reusedSummary) return reusedSummary
  const resumeSummary = repairResumeSummary(job)
  if (isBatchRootType(job.job_type)) {
    const batchAttempt = batchAttemptSummary(job)
    const childSummary = childSummaryLabel(job.child_summary)
    const combinedSummary = [batchAttempt, childSummary].filter((item) => item && item !== '—').join(' · ') || '—'
    if (job.batch_state === 'queued') return `Queued · ${combinedSummary}`
    if (job.batch_state === 'stopping') return `Stopping · ${combinedSummary}`
    if (job.batch_state === 'cancelled') return job.error_text || `Stopped · ${combinedSummary}`
    return combinedSummary
  }
  if (displayStatus === 'pending') return resumeSummary ? `Queued · ${resumeSummary}` : 'Queued'
  if (displayStatus === 'stopping') {
    const statsSummary = summarizeStats(job.stats_json, 'Stop requested…')
    return statsSummary === 'Stop requested…' ? statsSummary : `Stopping · ${statsSummary}`
  }
  if (displayStatus === 'running') {
    const statsSummary = summarizeStats(job.stats_json, 'Starting…')
    return statsSummary === 'Starting…' ? statsSummary : `Running · ${statsSummary}`
  }
  if (displayStatus === 'cancelled') return job.error_text || 'Stopped by user.'
  return summarizeStats(job.stats_json)
}

function jobStatsDetailSummary(job: Job) {
  const reusedSummary = reusedChildSummary(job.stats_json)
  if (reusedSummary) return reusedSummary

  const emptyLabel =
    jobDisplayStatus(job) === 'stopping' ? 'Stop requested…' : job.status === 'running' ? 'Starting…' : 'no stats yet'
  if (job.job_type === 'sync_papers') return syncPapersStatsSummary(job, emptyLabel)
  const statsSummary = summarizeStats(job.stats_json, emptyLabel)
  const resumeSummary = repairResumeSummary(job)
  if (!resumeSummary) return statsSummary
  if (statsSummary === emptyLabel) return resumeSummary
  return `${statsSummary} · ${resumeSummary}`
}

function isFinishedDisplayState(value: string) {
  return value === 'succeeded' || value === 'failed' || value === 'cancelled'
}

function canRerunBatchRoot(job: Job) {
  if (!job.child_summary) return false
  return job.child_summary.failed > 0 || job.child_summary.cancelled > 0 || job.child_summary.pending > 0
}

function canRerunJob(job: Job) {
  if (isBatchRootType(job.job_type)) {
    if (!canRerunBatchRoot(job)) return false
    if (!isFinishedDisplayState(jobDisplayStatus(job))) return false
    return isLatestAttempt(job)
  }
  if (job.job_type !== 'sync_papers' && job.job_type !== 'find_repos' && job.job_type !== 'refresh_metadata') return false
  if (!isFinishedDisplayState(jobDisplayStatus(job))) return false
  return isLatestAttempt(job)
}

function parentBatchBlocksChildRerun(parentJob: Job | null | undefined) {
  if (!parentJob) return false
  return jobDisplayStatus(parentJob) === 'stopping'
}

function canRerunJobInContext(job: Job, parentJob?: Job | null) {
  if (!canRerunJob(job)) return false
  if (job.parent_job_id && parentBatchBlocksChildRerun(parentJob)) return false
  return true
}

function canStopJob(job: Job) {
  const displayStatus = jobDisplayStatus(job)
  if (isBatchRootType(job.job_type) && job.parent_job_id === null) {
    return displayStatus === 'queued' || displayStatus === 'running'
  }
  if (job.stop_requested_at) return false
  return job.status === 'pending' || job.status === 'running'
}

function queueCountSummary(summary: Pick<JobQueueSummary, 'running' | 'pending' | 'stopping'>) {
  const parts: string[] = []
  if (summary.running > 0) parts.push(`${formatInteger(summary.running)} running`)
  if (summary.stopping > 0) parts.push(`${formatInteger(summary.stopping)} stopping`)
  if (summary.pending > 0) parts.push(`${formatInteger(summary.pending)} queued`)
  if (parts.length === 0) parts.push('0 running', '0 queued')
  return parts
}

function queueJobProgressLabel(job: Job) {
  const displayStatus = jobDisplayStatus(job)
  if (job.error_text && displayStatus !== 'stopping') return job.error_text

  switch (job.job_type) {
    case 'sync_papers_batch':
    case 'find_repos_batch':
    case 'refresh_metadata_batch':
      return [batchAttemptSummary(job), childSummaryLabel(job.child_summary)]
        .filter((item) => item && item !== '—')
        .join(' · ') || 'Batch prepared'
    case 'sync_papers': {
      const papersSaved = numericStat(job.stats_json, 'papers_upserted')
      const listingPages = numericStat(job.stats_json, 'listing_pages_fetched')
      const metadataBatches = numericStat(job.stats_json, 'metadata_batches_fetched')
      const ttlSkips = numericStat(job.stats_json, 'windows_skipped_ttl')
      const parts: string[] = []
      if (papersSaved && papersSaved > 0) parts.push(`${formatInteger(papersSaved)} papers saved`)
      if (listingPages && listingPages > 0) parts.push(pluralize(listingPages, 'listing page'))
      if (metadataBatches && metadataBatches > 0) parts.push(pluralize(metadataBatches, 'metadata batch', 'metadata batches'))
      const checkpointsReused = numericStat(job.stats_json, 'checkpoint_reused')
      if (checkpointsReused && checkpointsReused > 0) parts.push(pluralize(checkpointsReused, 'checkpoint reused', 'checkpoints reused'))
      if (ttlSkips && ttlSkips > 0) parts.push(pluralize(ttlSkips, 'TTL skip'))
      return parts.join(' · ') || repairResumeSummary(job) || (displayStatus === 'stopping' ? 'Stop requested…' : 'Starting paper sync…')
    }
    case 'find_repos': {
      const considered = numericStat(job.stats_json, 'papers_considered')
      const processed = numericStat(job.stats_json, 'papers_processed')
      const found = numericStat(job.stats_json, 'found')
      const notFound = numericStat(job.stats_json, 'not_found')
      const ambiguous = numericStat(job.stats_json, 'ambiguous')
      const skippedFresh = numericStat(job.stats_json, 'papers_skipped_fresh')
      const parts: string[] = []
      if (considered !== null && considered > 0) {
        parts.push(`${formatInteger(processed ?? 0)} / ${formatInteger(considered)} papers checked`)
      } else if (processed !== null && processed > 0) {
        parts.push(`${formatInteger(processed)} papers checked`)
      }
      if (found && found > 0) parts.push(`${formatInteger(found)} found`)
      if (notFound && notFound > 0) parts.push(`${formatInteger(notFound)} not found`)
      if (ambiguous && ambiguous > 0) parts.push(`${formatInteger(ambiguous)} ambiguous`)
      if (skippedFresh && skippedFresh > 0) parts.push(`${formatInteger(skippedFresh)} fresh skipped`)
      return parts.join(' · ') || (displayStatus === 'stopping' ? 'Stop requested…' : 'Starting repo lookup…')
    }
    case 'refresh_metadata': {
      const considered = numericStat(job.stats_json, 'repos_considered')
      const updated = numericStat(job.stats_json, 'updated') ?? 0
      const missing = numericStat(job.stats_json, 'missing') ?? 0
      const checked = updated + missing
      const parts: string[] = []
      if (considered !== null && considered > 0) {
        parts.push(`${formatInteger(checked)} / ${formatInteger(considered)} repos checked`)
      } else if (checked > 0) {
        parts.push(`${formatInteger(checked)} repos checked`)
      }
      if (updated > 0) parts.push(`${formatInteger(updated)} updated`)
      if (missing > 0) parts.push(`${formatInteger(missing)} missing`)
      return parts.join(' · ') || (displayStatus === 'stopping' ? 'Stop requested…' : 'Starting metadata refresh…')
    }
    case 'export': {
      const rows = numericStat(job.stats_json, 'rows')
      const fileName = typeof job.stats_json.file_name === 'string' && job.stats_json.file_name ? job.stats_json.file_name : null
      const parts: string[] = []
      if (rows && rows > 0) parts.push(`${formatInteger(rows)} rows prepared`)
      if (fileName) parts.push(fileName)
      return parts.join(' · ') || 'Preparing export…'
    }
    default:
      return jobSummary(job)
  }
}

function queueNextJobLabel(job: Job) {
  return `${jobTypeLabel(job.job_type)} · ${scopeJsonLabel(job.scope_json)}`
}

function isBatchFolderJob(job: Pick<Job, 'job_type' | 'parent_job_id'>) {
  return isBatchRootType(job.job_type) && job.parent_job_id === null
}

function joinList(values: string[], maxItems = 6) {
  if (values.length === 0) return '—'
  if (values.length <= maxItems) return values.join(', ')
  return `${values.slice(0, maxItems).join(', ')} +${values.length - maxItems}`
}

function scopeLabel(scope: ResolvedScope) {
  const parts: string[] = []
  parts.push(scope.categories || 'categories required')
  if (scope.day) parts.push(`day ${scope.day}`)
  else if (scope.month) parts.push(`month ${scope.month}`)
  else if (scope.from && scope.to) parts.push(`${scope.from} → ${scope.to}`)
  else parts.push('time required')
  return parts.join(' · ')
}

function scopeJsonLabel(scope: Record<string, unknown>) {
  const parts: string[] = []
  if (scope.export_mode === 'all_papers') {
    parts.push('all papers export')
  } else if (scope.export_mode === 'papers_view') {
    const paperCount = Array.isArray(scope.paper_ids) ? scope.paper_ids.length : 0
    parts.push(paperCount > 0 ? `filtered papers · ${paperCount} rows` : 'filtered papers')
  } else if (Array.isArray(scope.categories)) {
    const categories = scope.categories.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    if (categories.length > 0) parts.push(categories.join(', '))
  }
  if (typeof scope.day === 'string' && scope.day) parts.push(`day ${scope.day}`)
  else if (typeof scope.month === 'string' && scope.month) parts.push(`month ${scope.month}`)
  else if (typeof scope.from === 'string' && scope.from && typeof scope.to === 'string' && scope.to) parts.push(`${scope.from} → ${scope.to}`)
  if (scope.force === true) parts.push('force refresh')
  if (typeof scope.output_name === 'string' && scope.output_name) parts.push(scope.output_name)
  return parts.join(' · ') || 'default scope'
}

function normalizeExportBaseName(value: string) {
  return value.replace(/\.csv\s*$/i, '')
}

function columnWidth(header: string, preferred: number) {
  const headerMinimum = Math.min(240, Math.ceil(header.length * 7.4 + 86))
  return Math.max(preferred, headerMinimum)
}

function repoLabel(url: string) {
  try {
    return new URL(url).pathname.replace(/^\/+/, '')
  } catch {
    return url
  }
}

function formatAuthorLabel(authors: string[]) {
  const visibleAuthors = authors.slice(0, 3).join(', ')
  if (authors.length <= 3) return visibleAuthors
  return `${visibleAuthors} et al.`
}

function formatRepoSize(sizeKb: number | null | undefined) {
  if (typeof sizeKb !== 'number' || !Number.isFinite(sizeKb)) return '—'
  const units = ['KB', 'MB', 'GB']
  let value = sizeKb
  let unitIndex = 0
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }
  const formatted = Number.isInteger(value) ? String(value) : value.toFixed(2).replace(/\.?0+$/, '')
  return `${formatted} ${units[unitIndex]}`
}

function statusCellClass(value: unknown) {
  return value ? `status-cell status-${String(value)}` : 'status-cell'
}

function StatusTag({ value }: { value: string }) {
  return <span className={`status-tag ${value}`}>{value}</span>
}

function ChevronRightIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true">
      <path d="m6 3.75 4.5 4.25L6 12.25" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
    </svg>
  )
}

function suppressInteractiveCellMouseHandling() {
  return true
}

function DetailBlock({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="detail-block">
      <span className="detail-label">{label}</span>
      <div className="detail-value">{value}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{detail}</p>
    </div>
  )
}

function InputGhostHint({
  text,
  visible,
  wide = false,
}: {
  text: string
  visible: boolean
  wide?: boolean
}) {
  return <span className={wide ? (visible ? 'input-ghost-hint wide visible' : 'input-ghost-hint wide') : visible ? 'input-ghost-hint visible' : 'input-ghost-hint'}>{text}</span>
}

function MaskedDateField({
  label,
  value,
  invalid,
  showGhostHint = false,
  ghostHintText = 'YYYY-MM-DD',
  onChange,
}: {
  label: string
  value: string
  invalid?: boolean
  showGhostHint?: boolean
  ghostHintText?: string
  onChange: (value: string) => void
}) {
  return (
    <label className="form-field">
      <span className="field-label">{label}</span>
      <div className={showGhostHint ? 'input-ghost-shell hint-visible' : 'input-ghost-shell'}>
        <input
          type="text"
          inputMode="text"
          autoComplete="off"
          spellCheck={false}
          maxLength={10}
          placeholder="YYYY-MM-DD"
          value={value}
          onChange={(event) => onChange(event.target.value)}
          aria-invalid={invalid ? true : undefined}
          className={invalid ? 'date-mask-input input-invalid' : 'date-mask-input'}
        />
        <InputGhostHint text={ghostHintText} visible={showGhostHint} />
      </div>
    </label>
  )
}

function MaskedMonthField({
  value,
  invalid,
  showGhostHint = false,
  onChange,
}: {
  value: string
  invalid?: boolean
  showGhostHint?: boolean
  onChange: (value: string) => void
}) {
  return (
    <label className="form-field">
      <span className="field-label">Month</span>
      <div className={showGhostHint ? 'input-ghost-shell hint-visible' : 'input-ghost-shell'}>
        <input
          type="text"
          inputMode="text"
          autoComplete="off"
          spellCheck={false}
          maxLength={7}
          placeholder="YYYY-MM"
          value={value}
          onChange={(event) => onChange(event.target.value)}
          aria-invalid={invalid ? true : undefined}
          className={invalid ? 'month-mask-input input-invalid' : 'month-mask-input'}
        />
        <InputGhostHint text="YYYY-MM" visible={showGhostHint} />
      </div>
    </label>
  )
}

function ForceChip({
  checked,
  label,
  onChange,
}: {
  checked: boolean
  label: string
  onChange: (checked: boolean) => void
}) {
  return (
    <label className="force-chip">
      <input
        className="force-chip-input"
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      <span className="force-chip-label">{label}</span>
      <span className="force-chip-indicator" aria-hidden="true">
        <span className="force-chip-thumb" />
      </span>
    </label>
  )
}

function QueueSummaryCard({
  summary,
  launchingJob,
  launchFeedback,
}: {
  summary: JobQueueSummary | null
  launchingJob: StepJob | null
  launchFeedback: LaunchFeedback | null
}) {
  const showSubmittingState = launchingJob !== null && (!summary || summary.state === 'idle')
  const showCreatedHandoffState = !showSubmittingState && launchFeedback !== null && (!summary || summary.state === 'idle')
  const currentJob = showSubmittingState || showCreatedHandoffState ? null : summary?.current_job ?? null
  const nextJob = showSubmittingState || showCreatedHandoffState ? null : summary?.next_job ?? null
  const counts = showSubmittingState || showCreatedHandoffState
    ? { running: 0, stopping: 0, pending: 1 }
    : {
        running: summary?.running ?? 0,
        stopping: summary?.stopping ?? 0,
        pending: summary?.pending ?? 0,
      }

  let toneClassName = 'loading'
  let stateLabel = 'Loading'
  let primary = 'Loading job queue…'
  const segments: string[] = []

  if (showSubmittingState && launchingJob !== null) {
    toneClassName = 'waiting'
    stateLabel = 'Submitting'
    primary = `Queuing ${stepJobLabel(launchingJob)}…`
  } else if (showCreatedHandoffState && launchFeedback !== null) {
    toneClassName = 'waiting'
    stateLabel = 'Queued'
    primary = `Starting ${stepJobLabel(launchFeedback.stepJob)}…`
    segments.push('Refreshing queue status')
  } else if (!summary) {
    toneClassName = 'loading'
    stateLabel = 'Loading'
    segments.push('Checking worker status and queued jobs')
  } else if (currentJob) {
    const displayStatus = jobDisplayStatus(currentJob)
    toneClassName = displayStatus === 'stopping' ? 'stopping' : 'active'
    stateLabel = displayStatus === 'stopping' ? 'Stopping' : 'Running'
    primary = jobTypeLabel(currentJob.job_type)
    segments.push(scopeJsonLabel(currentJob.scope_json))
    segments.push(queueJobProgressLabel(currentJob))
    if (nextJob) segments.push(`Up next: ${queueNextJobLabel(nextJob)}`)
    else if (counts.pending === 0) segments.push('No queued jobs behind the current run')
  } else if (summary.state === 'waiting' && nextJob) {
    toneClassName = 'waiting'
    stateLabel = 'Waiting'
    primary = 'Waiting to start'
    segments.push(`Up next: ${queueNextJobLabel(nextJob)}`)
  } else if (summary.state === 'active') {
    toneClassName = 'active'
    stateLabel = 'Running'
    primary = 'Refreshing queue status…'
    segments.push('Worker activity detected')
  } else if (summary.state === 'waiting') {
    toneClassName = 'waiting'
    stateLabel = 'Waiting'
    primary = 'Refreshing queue status…'
    segments.push('Queued work detected')
  } else {
    toneClassName = 'idle'
    stateLabel = 'Idle'
    primary = 'No jobs are running or waiting'
  }

  const countsLabel = queueCountSummary(counts).join(' · ')

  return (
    <div className={`queue-summary queue-summary-${toneClassName}`}>
      <span className="queue-summary-kicker">Job queue</span>
      <span className={`queue-summary-state queue-summary-state-${toneClassName}`}>{stateLabel}</span>

      <div className="queue-summary-text" title={[primary, ...segments].join(' · ')}>
        <strong className="queue-summary-primary">{primary}</strong>
        {segments.map((segment) => (
          <span key={segment} className="queue-summary-segment">
            {segment}
          </span>
        ))}
      </div>

      <span className="queue-summary-counts" title={countsLabel}>
        {countsLabel}
      </span>
    </div>
  )
}

function StepCard({
  index,
  title,
  detail,
  running,
  disabled,
  disabledReason,
  config,
  onRun,
}: {
  index: number
  title: string
  detail: string
  running: boolean
  disabled: boolean
  disabledReason?: string | null
  config?: ReactNode
  onRun: () => void
}) {
  const runButton = (
    <button type="button" className="primary-button step-card-run" onClick={onRun} disabled={disabled}>
      {running ? 'Running…' : 'Run'}
    </button>
  )

  return (
    <article className="step-card">
      <div className="step-card-top">
        <div className="step-card-title-row">
          <span className="step-index">{index}</span>
          <strong>{title}</strong>
        </div>

        <div className="step-card-actions">
          {config ? <div className="step-card-config">{config}</div> : null}

          {disabled && disabledReason ? (
            <HoverTooltip content={disabledReason} anchorClassName="step-card-run-tooltip">
              <span className="step-card-run-tooltip-target">{runButton}</span>
            </HoverTooltip>
          ) : (
            runButton
          )}
        </div>
      </div>

      <p className="step-card-detail">{detail}</p>
    </article>
  )
}

function JobRerunCellRenderer({ data, onRerun }: JobRerunCellRendererProps) {
  if (!data || data.can_rerun !== true) return null

  const jobId = typeof data.id === 'string' ? data.id : ''
  const busy = data.rerun_busy === true

  return (
    <button
      type="button"
      className="grid-action-button"
      data-grid-action="true"
      disabled={busy}
      onMouseDown={(event) => {
        event.preventDefault()
        event.stopPropagation()
      }}
      onClick={(event) => {
        event.preventDefault()
        event.stopPropagation()
        if (jobId) onRerun?.(jobId)
      }}
    >
      {busy ? 'Queued…' : 'Re-run'}
    </button>
  )
}

function JobStopCellRenderer({ data, onStop }: JobStopCellRendererProps) {
  if (!data || data.can_stop !== true) return null

  const jobId = typeof data.id === 'string' ? data.id : ''
  const busy = data.stop_busy === true

  return (
    <button
      type="button"
      className="grid-action-button danger"
      data-grid-action="true"
      disabled={busy}
      onMouseDown={(event) => {
        event.preventDefault()
        event.stopPropagation()
      }}
      onClick={(event) => {
        event.preventDefault()
        event.stopPropagation()
        if (jobId) onStop?.(jobId)
      }}
    >
      {busy ? 'Stopping…' : 'Stop'}
    </button>
  )
}

function JobChildrenChevronCellRenderer({ data, onToggleChildren }: JobChildrenChevronCellRendererProps) {
  if (!data) return null

  const rowKind = typeof data.row_kind === 'string' ? (data.row_kind as JobRowKind) : 'root'
  const rowDepth = typeof data.row_depth === 'number' ? data.row_depth : 0

  if (rowKind === 'child' || (rowKind === 'history' && rowDepth > 0)) {
    const branchClassName = rowDepth > 1 ? 'job-tree-branch depth-2' : 'job-tree-branch depth-1'
    return <span className={branchClassName} aria-hidden="true" />
  }

  const toggleable = data.children_toggleable === true
  if (!toggleable) {
    return <span className="job-tree-spacer" aria-hidden="true" />
  }

  const jobId = typeof data.id === 'string' ? data.id : ''
  const busy = data.children_loading === true
  const expanded = data.children_expanded === true
  const label = busy ? 'Loading child jobs' : expanded ? 'Collapse child jobs' : 'Expand child jobs'
  const toggleClassName = ['job-tree-toggle', expanded ? 'expanded' : '', busy ? 'loading' : ''].filter(Boolean).join(' ')

  return (
    <button
      type="button"
      className={toggleClassName}
      data-grid-action="true"
      aria-label={label}
      aria-busy={busy}
      aria-expanded={expanded}
      title={label}
      onMouseDown={(event) => {
        event.preventDefault()
        event.stopPropagation()
      }}
      onClick={(event) => {
        event.preventDefault()
        event.stopPropagation()
        if (jobId) onToggleChildren?.(jobId)
      }}
    >
      <span className="job-tree-toggle-icon" aria-hidden="true">
        <ChevronRightIcon />
      </span>
    </button>
  )
}

function JobHistoryChevronCellRenderer({ data, onToggleHistory }: JobHistoryChevronCellRendererProps) {
  if (!data) return null

  const rowKind = typeof data.row_kind === 'string' ? (data.row_kind as JobRowKind) : 'root'
  if (rowKind === 'history') {
    const historyDepth = typeof data.history_depth === 'number' ? data.history_depth : 1
    const branchClassName = historyDepth > 1 ? 'job-history-branch depth-2' : 'job-history-branch depth-1'
    return <span className={branchClassName} aria-hidden="true" />
  }

  const toggleable = data.history_toggleable === true
  if (!toggleable) {
    return <span className="job-history-spacer" aria-hidden="true" />
  }

  const jobId = typeof data.id === 'string' ? data.id : ''
  const busy = data.history_loading === true
  const expanded = data.history_expanded === true
  const attemptCount = typeof data.attempt_count === 'number' ? data.attempt_count : null
  const label = busy ? 'Loading history' : expanded ? `Collapse ${attemptCount ?? ''} attempts`.trim() : `Expand ${attemptCount ?? ''} attempts`.trim()
  const toggleClassName = ['job-history-toggle', expanded ? 'expanded' : '', busy ? 'loading' : ''].filter(Boolean).join(' ')

  return (
    <button
      type="button"
      className={toggleClassName}
      data-grid-action="true"
      aria-label={label}
      aria-busy={busy}
      aria-expanded={expanded}
      title={label}
      onMouseDown={(event) => {
        event.preventDefault()
        event.stopPropagation()
      }}
      onClick={(event) => {
        event.preventDefault()
        event.stopPropagation()
        if (jobId) onToggleHistory?.(jobId)
      }}
    >
      <span className="job-history-toggle-icon" aria-hidden="true">
        <ChevronRightIcon />
      </span>
    </button>
  )
}

function JobAttemptCellRenderer({ data }: JobAttemptCellRendererProps) {
  if (!data) return null

  const rowKind = typeof data.row_kind === 'string' ? (data.row_kind as JobRowKind) : 'root'
  const label = typeof data.attempt_relation_label === 'string' ? data.attempt_relation_label : ''
  const attemptCount = typeof data.attempt_count === 'number' ? data.attempt_count : 1
  const attemptRank = typeof data.attempt_rank === 'number' ? data.attempt_rank : 1

  if (rowKind === 'history') {
    return <span className="job-attempt-inline history">{label || `History #${attemptRank}`}</span>
  }

  if (attemptCount > 1) {
    return <span className="job-attempt-chip latest">{label || `${attemptCount} attempts`}</span>
  }

  return <span className="job-attempt-chip latest">{label || 'Latest'}</span>
}

function App() {
  const exportMenuRef = useRef<HTMLDetailsElement | null>(null)
  const drawerPanelRef = useRef<HTMLElement | null>(null)
  const sheetFrameRef = useRef<HTMLDivElement | null>(null)
  const rerunJobRef = useRef<(jobId: string) => void>(() => {})
  const stopJobRef = useRef<(jobId: string) => void>(() => {})
  const [health, setHealth] = useState<Health | null>(null)
  const [dashboard, setDashboard] = useState<Dashboard | null>(null)
  const [papers, setPapers] = useState<PaperSummary[]>([])
  const [paperDetailsById, setPaperDetailsById] = useState<Record<string, PaperDetail>>({})
  const [repos, setRepos] = useState<Repo[]>([])
  const [jobs, setJobs] = useState<Job[]>([])
  const [childJobsByParentId, setChildJobsByParentId] = useState<Record<string, Job[]>>({})
  const [jobAttemptHistories, setJobAttemptHistories] = useState<Record<string, Job[]>>({})
  const [expandedParentJobIds, setExpandedParentJobIds] = useState<string[]>([])
  const [expandedJobGroups, setExpandedJobGroups] = useState<string[]>([])
  const [loadingChildJobParentIds, setLoadingChildJobParentIds] = useState<string[]>([])
  const [loadingJobHistoryGroups, setLoadingJobHistoryGroups] = useState<string[]>([])
  const [selectedJobChildren, setSelectedJobChildren] = useState<Job[]>([])
  const [selectedJobAttempts, setSelectedJobAttempts] = useState<Job[]>([])
  const [selectedJobDetail, setSelectedJobDetail] = useState<Job | null>(null)
  const [exportsData, setExportsData] = useState<ExportRow[]>([])
  const initialScopeRef = useRef<ScopeState | null>(null)
  if (initialScopeRef.current === null) {
    initialScopeRef.current = loadSavedScope()
  }
  const [scope, setScope] = useState<ScopeState>(initialScopeRef.current)
  const persistedScopeRef = useRef<PersistedScopeState>(createPersistedScopeSnapshot(initialScopeRef.current))
  const [syncPapersForce, setSyncPapersForce] = useState(false)
  const [findReposForce, setFindReposForce] = useState(false)
  const [exportOutputName, setExportOutputName] = useState('')
  const [exportMode, setExportMode] = useState<ExportMode>('all_papers')
  const [previewTab, setPreviewTab] = useState<PreviewTab>('papers')
  const [tableSearch, setTableSearch] = useState('')
  const [selectedPaperId, setSelectedPaperId] = useState<string | null>(null)
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)
  const [selectedExportId, setSelectedExportId] = useState<string | null>(null)
  const [visibleKeys, setVisibleKeys] = useState<string[]>([])
  const [launchingJob, setLaunchingJob] = useState<StepJob | null>(null)
  const [launchFeedback, setLaunchFeedback] = useState<LaunchFeedback | null>(null)
  const [rerunningJobId, setRerunningJobId] = useState<string | null>(null)
  const [stoppingJobIds, setStoppingJobIds] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)
  const [summaryRefreshTick, setSummaryRefreshTick] = useState(0)
  const [jobsRefreshTick, setJobsRefreshTick] = useState(0)
  const [tableRefreshTick, setTableRefreshTick] = useState(0)
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null)
  const [selectedPaperLoading, setSelectedPaperLoading] = useState(false)
  const [selectedJobDetailLoading, setSelectedJobDetailLoading] = useState(false)
  const [selectedJobChildrenLoading, setSelectedJobChildrenLoading] = useState(false)
  const [selectedJobAttemptsLoading, setSelectedJobAttemptsLoading] = useState(false)
  const [papersLoading, setPapersLoading] = useState(false)
  const [papersLoadedCount, setPapersLoadedCount] = useState(0)
  const [exportsLoading, setExportsLoading] = useState(false)
  const previousActiveJobsRef = useRef(false)
  const papersRef = useRef<PaperSummary[]>([])
  const childJobsByParentIdRef = useRef<Record<string, Job[]>>({})
  const jobAttemptHistoriesRef = useRef<Record<string, Job[]>>({})
  const expandedParentJobIdsRef = useRef<string[]>([])
  const expandedJobGroupsRef = useRef<string[]>([])
  const validExpandedParentJobIdsRef = useRef<string[]>([])
  const validExpandedJobGroupsRef = useRef<string[]>([])
  const latestJobByGroupRef = useRef<Map<string, Job>>(new Map())
  const selectedJobIdRef = useRef<string | null>(null)
  const rerunSelectionHandoffJobIdRef = useRef<string | null>(null)
  const rerunSelectionHandoffTimeoutRef = useRef<number | null>(null)
  const childJobAbortControllersRef = useRef<Map<string, AbortController>>(new Map())
  const historyAbortControllersRef = useRef<Map<string, AbortController>>(new Map())
  const queueHandoffTimeoutRef = useRef<number | null>(null)
  const selectedJobDetailHydratedJobIdRef = useRef<string | null>(null)
  const selectedJobChildrenHydratedJobIdRef = useRef<string | null>(null)
  const selectedJobAttemptsHydratedJobIdRef = useRef<string | null>(null)
  const [selectedJobRefreshTick, setSelectedJobRefreshTick] = useState(0)

  const exportBaseName = normalizeExportBaseName(exportOutputName).trim()
  const exportNameValid = exportBaseName.length > 0
  const deferredTableSearch = useDeferredValue(tableSearch)
  const categoriesFieldError = useMemo(() => categoriesValidationMessage(scope.categories), [scope.categories])
  const categoriesFieldInvalid = useMemo(
    () => categoriesFieldError !== null && (hasTypedValue(scope.categories) || health !== null),
    [categoriesFieldError, health, scope.categories],
  )
  const categoriesShowGhostHint = useMemo(
    () => categoriesFieldInvalid && hasTypedValue(scope.categories),
    [categoriesFieldInvalid, scope.categories],
  )
  const dayFieldError = useMemo(() => (scope.timeMode === 'day' ? dateValidationMessage(scope.day) : null), [scope.day, scope.timeMode])
  const dayFieldInvalid = useMemo(() => scope.timeMode === 'day' && dateValidationMessage(scope.day) !== null, [scope.day, scope.timeMode])
  const dayShowGhostHint = useMemo(() => dayFieldError !== null && hasTypedValue(scope.day), [dayFieldError, scope.day])
  const monthFieldError = useMemo(() => (scope.timeMode === 'month' ? monthValidationMessage(scope.month) : null), [scope.month, scope.timeMode])
  const monthFieldInvalid = useMemo(() => scope.timeMode === 'month' && monthValidationMessage(scope.month) !== null, [scope.month, scope.timeMode])
  const monthShowGhostHint = useMemo(() => monthFieldError !== null && hasTypedValue(scope.month), [monthFieldError, scope.month])
  const rangeFromFieldError = useMemo(() => (scope.timeMode === 'range' ? dateValidationMessage(scope.from) : null), [scope.from, scope.timeMode])
  const rangeToFieldError = useMemo(() => (scope.timeMode === 'range' ? dateValidationMessage(scope.to) : null), [scope.timeMode, scope.to])
  const rangeOrderInvalid = useMemo(
    () => scope.timeMode === 'range' && rangeFromFieldError === null && rangeToFieldError === null && scope.from > scope.to,
    [rangeFromFieldError, rangeToFieldError, scope.from, scope.timeMode, scope.to],
  )
  const rangeFromFieldInvalid = useMemo(() => scope.timeMode === 'range' && (rangeFromFieldError !== null || rangeOrderInvalid), [rangeFromFieldError, rangeOrderInvalid, scope.timeMode])
  const rangeToFieldInvalid = useMemo(() => scope.timeMode === 'range' && (rangeToFieldError !== null || rangeOrderInvalid), [rangeOrderInvalid, rangeToFieldError, scope.timeMode])
  const rangeGhostHintText = useMemo(() => (rangeOrderInvalid ? RANGE_ORDER_HINT : 'YYYY-MM-DD'), [rangeOrderInvalid])
  const rangeFromShowGhostHint = useMemo(
    () => hasTypedValue(scope.from) && (rangeFromFieldError !== null || rangeOrderInvalid),
    [rangeFromFieldError, rangeOrderInvalid, scope.from],
  )
  const rangeToShowGhostHint = useMemo(
    () => hasTypedValue(scope.to) && (rangeToFieldError !== null || rangeOrderInvalid),
    [rangeOrderInvalid, rangeToFieldError, scope.to],
  )
  const queueModeLabel = health?.queue_mode === 'serial' ? 'serial queue' : 'loading queue mode'
  const githubRuntimeLabel = health
    ? health.github_auth_configured
      ? `GitHub token on · ${formatSeconds(health.effective_github_min_interval_seconds)} min interval`
      : `GitHub token off · ${formatSeconds(health.effective_github_min_interval_seconds)} min interval`
    : 'GitHub API settings loading'

  const liveScope = useMemo(() => resolveScope(scope), [scope])
  const filteredPaperIds = previewTab === 'papers' ? visibleKeys : []
  const filteredPaperExportReady = previewTab === 'papers' && filteredPaperIds.length > 0

  const runDisabledReason = useCallback(
    (jobType: 'sync-papers' | 'find-repos' | 'refresh-metadata', title: string) => {
      if (launchingJob === jobType) {
        return `${title} is already being queued.`
      }
      if (launchingJob !== null) {
        return 'Wait for the current task request to finish before starting another step.'
      }
      if (liveScope.error !== null) {
        return liveScope.error
      }
      return null
    },
    [launchingJob, liveScope.error],
  )

  usePointerDownOutside(exportMenuRef, () => {
    if (exportMenuRef.current?.open) {
      exportMenuRef.current.open = false
    }
  })

  useEffect(() => {
    let cancelled = false

    fetchJson<Health>('/api/v1/health')
      .then((data) => {
        if (cancelled) return
        setHealth(data)
        const categories = data.default_categories.join(', ')
        startTransition(() => {
          setScope((current) => ({ ...current, categories: current.categories || categories }))
        })
      })
      .catch((err: Error) => {
        if (!cancelled) setError(err.message)
      })

    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      const nextPersistedScope = createPersistableScope(scope, persistedScopeRef.current)
      if (samePersistedScopeState(nextPersistedScope, persistedScopeRef.current)) return
      window.localStorage.setItem(SCOPE_STORAGE_KEY, JSON.stringify(nextPersistedScope))
      persistedScopeRef.current = nextPersistedScope
    } catch {
      // Ignore storage failures. The UI can still operate without persistence.
    }
  }, [scope])

  useEffect(() => {
    papersRef.current = papers
  }, [papers])

  useEffect(() => {
    childJobsByParentIdRef.current = childJobsByParentId
  }, [childJobsByParentId])

  useEffect(() => {
    jobAttemptHistoriesRef.current = jobAttemptHistories
  }, [jobAttemptHistories])

  useEffect(() => {
    expandedParentJobIdsRef.current = expandedParentJobIds
  }, [expandedParentJobIds])

  useEffect(() => {
    expandedJobGroupsRef.current = expandedJobGroups
  }, [expandedJobGroups])

  useEffect(() => {
    selectedJobIdRef.current = selectedJobId
  }, [selectedJobId])

  const activeJobsInList = jobs.some((job) => {
    const displayStatus = jobDisplayStatus(job)
    return displayStatus === 'queued' || displayStatus === 'running' || displayStatus === 'stopping'
  })
  const hasActiveJobs =
    launchingJob !== null ||
    activeJobsInList ||
    (dashboard?.pending_jobs ?? 0) > 0 ||
    (dashboard?.running_jobs ?? 0) > 0 ||
    (dashboard?.stopping_jobs ?? 0) > 0

  function clearRerunSelectionHandoff() {
    rerunSelectionHandoffJobIdRef.current = null
    if (rerunSelectionHandoffTimeoutRef.current !== null) {
      window.clearTimeout(rerunSelectionHandoffTimeoutRef.current)
      rerunSelectionHandoffTimeoutRef.current = null
    }
  }

  function protectRerunSelectionHandoff(jobId: string) {
    clearRerunSelectionHandoff()
    rerunSelectionHandoffJobIdRef.current = jobId
    rerunSelectionHandoffTimeoutRef.current = window.setTimeout(() => {
      rerunSelectionHandoffJobIdRef.current = null
      rerunSelectionHandoffTimeoutRef.current = null
    }, 10000)
  }

  useEffect(() => {
    return () => {
      if (queueHandoffTimeoutRef.current !== null) {
        window.clearTimeout(queueHandoffTimeoutRef.current)
        queueHandoffTimeoutRef.current = null
      }
      clearRerunSelectionHandoff()
    }
  }, [])

  useEffect(() => {
    const intervalMs = hasActiveJobs ? ACTIVE_DASHBOARD_POLL_MS : IDLE_DASHBOARD_POLL_MS
    const timer = window.setInterval(() => setSummaryRefreshTick((value) => value + 1), intervalMs)
    return () => window.clearInterval(timer)
  }, [hasActiveJobs])

  useEffect(() => {
    let cancelled = false

    async function load() {
      try {
        const dashboardData = await fetchJson<Dashboard>('/api/v1/dashboard')
        if (cancelled) return
        setDashboard(dashboardData)
        setLastRefreshedAt(new Date().toISOString())
        setError(null)
      } catch (err) {
        if (!cancelled && err instanceof Error) setError(err.message)
      }
    }

    load()
    return () => {
      cancelled = true
    }
  }, [summaryRefreshTick])

  useEffect(() => {
    if (!hasActiveJobs && previewTab !== 'jobs') return
    const intervalMs = hasActiveJobs ? ACTIVE_JOBS_POLL_MS : PASSIVE_JOBS_POLL_MS
    const timer = window.setInterval(() => setJobsRefreshTick((value) => value + 1), intervalMs)
    return () => window.clearInterval(timer)
  }, [hasActiveJobs, previewTab])

  useEffect(() => {
    let cancelled = false

    async function loadJobs() {
      try {
        const jobData = await fetchJson<Job[]>(`/api/v1/jobs?limit=${JOB_PREVIEW_LIMIT}&view=latest&root_only=true`)
        if (cancelled) return
        setJobs(jobData)
        setLastRefreshedAt(new Date().toISOString())
        setError(null)
      } catch (err) {
        if (!cancelled && err instanceof Error) setError(err.message)
      }
    }

    loadJobs()
    return () => {
      cancelled = true
    }
  }, [jobsRefreshTick, previewTab])

  const rootJobById = useMemo(() => {
    const result = new Map<string, Job>()
    for (const job of jobs) result.set(job.id, job)
    return result
  }, [jobs])

  const validExpandedParentJobIds = useMemo(
    () => expandedParentJobIds.filter((jobId) => rootJobById.has(jobId)),
    [expandedParentJobIds, rootJobById],
  )
  const expandedParentJobSet = useMemo(() => new Set(validExpandedParentJobIds), [validExpandedParentJobIds])
  const loadingChildJobParentSet = useMemo(() => new Set(loadingChildJobParentIds), [loadingChildJobParentIds])

  useEffect(() => {
    validExpandedParentJobIdsRef.current = validExpandedParentJobIds
  }, [validExpandedParentJobIds])

  const loadedChildJobs = useMemo(() => Object.values(childJobsByParentId).flat(), [childJobsByParentId])
  const allLatestJobs = useMemo(() => [...jobs, ...loadedChildJobs], [jobs, loadedChildJobs])
  const latestJobById = useMemo(() => {
    const result = new Map<string, Job>()
    for (const job of allLatestJobs) result.set(job.id, job)
    return result
  }, [allLatestJobs])

  const latestJobByGroup = useMemo(() => {
    const result = new Map<string, Job>()
    for (const job of allLatestJobs) result.set(attemptGroupKey(job), job)
    return result
  }, [allLatestJobs])

  const validExpandedJobGroups = useMemo(
    () => expandedJobGroups.filter((groupKey) => latestJobByGroup.has(groupKey)),
    [expandedJobGroups, latestJobByGroup],
  )
  const expandedJobGroupSet = useMemo(() => new Set(validExpandedJobGroups), [validExpandedJobGroups])
  const loadingJobHistoryGroupSet = useMemo(() => new Set(loadingJobHistoryGroups), [loadingJobHistoryGroups])

  useEffect(() => {
    validExpandedJobGroupsRef.current = validExpandedJobGroups
  }, [validExpandedJobGroups])

  useEffect(() => {
    latestJobByGroupRef.current = latestJobByGroup
  }, [latestJobByGroup])

  const setChildJobsLoading = useCallback((parentId: string, active: boolean) => {
    setLoadingChildJobParentIds((current) =>
      active
        ? current.includes(parentId)
          ? current
          : [...current, parentId]
        : current.filter((value) => value !== parentId),
    )
  }, [])

  const setJobHistoryLoading = useCallback((groupKey: string, active: boolean) => {
    setLoadingJobHistoryGroups((current) =>
      active
        ? current.includes(groupKey)
          ? current
          : [...current, groupKey]
        : current.filter((value) => value !== groupKey),
    )
  }, [])

  const cancelChildJobsLoad = useCallback(
    (parentId: string) => {
      const controller = childJobAbortControllersRef.current.get(parentId)
      if (controller) {
        controller.abort()
        childJobAbortControllersRef.current.delete(parentId)
      }
      setChildJobsLoading(parentId, false)
    },
    [setChildJobsLoading],
  )

  const cancelJobHistoryLoad = useCallback(
    (groupKey: string) => {
      const controller = historyAbortControllersRef.current.get(groupKey)
      if (controller) {
        controller.abort()
        historyAbortControllersRef.current.delete(groupKey)
      }
      setJobHistoryLoading(groupKey, false)
    },
    [setJobHistoryLoading],
  )

  const abortAllJobTreeRequests = useCallback(() => {
    for (const controller of childJobAbortControllersRef.current.values()) controller.abort()
    for (const controller of historyAbortControllersRef.current.values()) controller.abort()
    childJobAbortControllersRef.current.clear()
    historyAbortControllersRef.current.clear()
  }, [])

  const cancelAllJobTreeLoads = useCallback(() => {
    abortAllJobTreeRequests()
    setLoadingChildJobParentIds([])
    setLoadingJobHistoryGroups([])
  }, [abortAllJobTreeRequests])

  const loadChildJobs = useCallback(
    async (parentId: string, options: { force?: boolean } = {}) => {
      const { force = false } = options
      const existingController = childJobAbortControllersRef.current.get(parentId)
      const hasCached = Object.prototype.hasOwnProperty.call(childJobsByParentIdRef.current, parentId)

      if (existingController) return
      if (hasCached && !force) return

      const controller = new AbortController()
      childJobAbortControllersRef.current.set(parentId, controller)
      setChildJobsLoading(parentId, true)

      try {
        const children = await fetchJson<Job[]>(
          `/api/v1/jobs?parent_id=${parentId}&limit=${JOB_PREVIEW_LIMIT}&view=latest`,
          { signal: controller.signal },
        )
        if (childJobAbortControllersRef.current.get(parentId) !== controller) return
        setChildJobsByParentId((current) => {
          const next = { ...current, [parentId]: children }
          childJobsByParentIdRef.current = next
          return next
        })
      } catch (err) {
        if (!isAbortError(err) && err instanceof Error) setError(`Jobs: ${err.message}`)
      } finally {
        if (childJobAbortControllersRef.current.get(parentId) === controller) {
          childJobAbortControllersRef.current.delete(parentId)
          setChildJobsLoading(parentId, false)
        }
      }
    },
    [setChildJobsLoading],
  )

  const loadJobHistory = useCallback(
    async (groupKey: string, jobId: string, options: { force?: boolean } = {}) => {
      const { force = false } = options
      const existingController = historyAbortControllersRef.current.get(groupKey)
      const hasCached = Object.prototype.hasOwnProperty.call(jobAttemptHistoriesRef.current, groupKey)

      if (existingController) return
      if (hasCached && !force) return

      const controller = new AbortController()
      historyAbortControllersRef.current.set(groupKey, controller)
      setJobHistoryLoading(groupKey, true)

      try {
        const attempts = await fetchJson<Job[]>(`/api/v1/jobs/${jobId}/attempts?limit=${JOB_PREVIEW_LIMIT}`, {
          signal: controller.signal,
        })
        if (historyAbortControllersRef.current.get(groupKey) !== controller) return
        setJobAttemptHistories((current) => {
          const next = { ...current, [groupKey]: attempts }
          jobAttemptHistoriesRef.current = next
          return next
        })
      } catch (err) {
        if (!isAbortError(err) && err instanceof Error) setError(`Jobs: ${err.message}`)
      } finally {
        if (historyAbortControllersRef.current.get(groupKey) === controller) {
          historyAbortControllersRef.current.delete(groupKey)
          setJobHistoryLoading(groupKey, false)
        }
      }
    },
    [setJobHistoryLoading],
  )

  useEffect(() => {
    if (previewTab !== 'jobs') return
    for (const parentId of validExpandedParentJobIds) {
      void loadChildJobs(parentId)
    }
  }, [loadChildJobs, previewTab, validExpandedParentJobIds])

  useEffect(() => {
    if (previewTab !== 'jobs') return
    for (const groupKey of validExpandedJobGroups) {
      const job = latestJobByGroup.get(groupKey)
      if (!job) continue
      void loadJobHistory(groupKey, job.id)
    }
  }, [latestJobByGroup, loadJobHistory, previewTab, validExpandedJobGroups])

  useEffect(() => {
    if (previewTab !== 'jobs') return
    for (const parentId of validExpandedParentJobIdsRef.current) {
      void loadChildJobs(parentId, { force: true })
    }
    for (const groupKey of validExpandedJobGroupsRef.current) {
      const job = latestJobByGroupRef.current.get(groupKey)
      if (!job) continue
      void loadJobHistory(groupKey, job.id, { force: true })
    }
  }, [jobsRefreshTick, loadChildJobs, loadJobHistory, previewTab])

  useEffect(() => {
    if (!hasActiveJobs) return
    const timer = window.setInterval(() => setTableRefreshTick((value) => value + 1), 20000)
    return () => window.clearInterval(timer)
  }, [hasActiveJobs])

  useEffect(() => {
    const controller = new AbortController()
    const hadVisibleRows = papersRef.current.length > 0

    async function loadPapers() {
      const nextRows: PaperSummary[] = []
      setPapersLoading(true)
      setPapersLoadedCount(0)

      try {
        for (let offset = 0; ; offset += PAPER_BATCH_SIZE) {
          const batch = await fetchJson<PaperSummary[]>(
            `/api/v1/papers?limit=${PAPER_BATCH_SIZE}&offset=${offset}`,
            { signal: controller.signal },
          )
          nextRows.push(...batch)
          setPapersLoadedCount(nextRows.length)

          if (!hadVisibleRows) {
            setPapers([...nextRows])
          }

          if (batch.length < PAPER_BATCH_SIZE) {
            break
          }
        }

        if (controller.signal.aborted) return
        if (hadVisibleRows) {
          setPapers(nextRows)
        }
        setLastRefreshedAt(new Date().toISOString())
        setError(null)
      } catch (err) {
        if (!isAbortError(err) && err instanceof Error) setError(`Papers: ${err.message}`)
      } finally {
        if (!controller.signal.aborted) {
          setPapersLoading(false)
        }
      }
    }

    void loadPapers()

    return () => {
      controller.abort()
    }
  }, [tableRefreshTick])

  useEffect(() => {
    const controller = new AbortController()
    const repoLimit = REPO_PREVIEW_LIMIT

    async function loadRepos() {
      try {
        const data = await fetchJson<Repo[]>(`/api/v1/repos?limit=${repoLimit}`, {
          signal: controller.signal,
        })
        if (controller.signal.aborted) return
        setRepos(data)
        setLastRefreshedAt(new Date().toISOString())
        setError(null)
      } catch (err) {
        if (!isAbortError(err) && err instanceof Error) setError(`Repos: ${err.message}`)
      }
    }

    void loadRepos()

    return () => {
      controller.abort()
    }
  }, [tableRefreshTick])

  useEffect(() => {
    const controller = new AbortController()

    async function loadExports() {
      setExportsLoading(true)
      try {
        const data = await fetchJson<ExportRow[]>('/api/v1/exports', {
          signal: controller.signal,
        })
        if (controller.signal.aborted) return
        setExportsData(data)
        setLastRefreshedAt(new Date().toISOString())
        setError(null)
      } catch (err) {
        if (!isAbortError(err) && err instanceof Error) setError(`Exports: ${err.message}`)
      } finally {
        if (!controller.signal.aborted) {
          setExportsLoading(false)
        }
      }
    }

    void loadExports()

    return () => {
      controller.abort()
    }
  }, [tableRefreshTick])

  useEffect(() => {
    if (previewTab === 'jobs') return
    abortAllJobTreeRequests()
    queueMicrotask(() => {
      setLoadingChildJobParentIds([])
      setLoadingJobHistoryGroups([])
    })
  }, [abortAllJobTreeRequests, previewTab])

  useEffect(() => {
    const childJobAbortControllers = childJobAbortControllersRef.current
    const historyAbortControllers = historyAbortControllersRef.current

    return () => {
      for (const controller of childJobAbortControllers.values()) controller.abort()
      for (const controller of historyAbortControllers.values()) controller.abort()
      childJobAbortControllers.clear()
      historyAbortControllers.clear()
    }
  }, [])

  useEffect(() => {
    if (previousActiveJobsRef.current && !hasActiveJobs) {
      setSummaryRefreshTick((value) => value + 1)
      setJobsRefreshTick((value) => value + 1)
      setTableRefreshTick((value) => value + 1)
    }
    previousActiveJobsRef.current = hasActiveJobs
  }, [hasActiveJobs])

  const drawerOpen = Boolean(selectedPaperId || selectedJobId || selectedExportId)

  usePointerDownOutside(
    drawerPanelRef,
    (event) => {
      const target = event.target
      if (target instanceof HTMLElement && sheetFrameRef.current?.contains(target)) {
        if (target.closest('[data-grid-action="true"]')) return
        if (target.closest('.ag-row')) return
      }
      closeDrawer()
    },
    drawerOpen,
  )

  useEffect(() => {
    if (!drawerOpen) return

    function handleKeydown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        closeDrawer()
      }
    }

    window.addEventListener('keydown', handleKeydown)
    return () => window.removeEventListener('keydown', handleKeydown)
  }, [drawerOpen])

  useEffect(() => {
    if (!drawerOpen) return
    drawerPanelRef.current?.scrollTo({ top: 0, behavior: 'auto' })
  }, [drawerOpen, previewTab, selectedExportId, selectedJobId, selectedPaperId])

  async function launchJob(jobType: StepJob) {
    if (jobType !== 'export' && liveScope.error) {
      setError('Fix the scope error before starting a task.')
      return
    }

    if (jobType === 'export' && !exportNameValid) {
      setError('Enter an export name before queuing the export.')
      return
    }

    const payload: Record<string, unknown> = {}

    if (jobType === 'export') {
      if (exportMode === 'papers_view' && !filteredPaperExportReady) {
        setError('Filtered view export requires the Papers table and at least one visible row.')
        return
      }
      payload.output_name = `${exportBaseName}.csv`
      payload.export_mode = exportMode
      if (exportMode === 'papers_view') {
        payload.paper_ids = filteredPaperIds
      }
    } else {
      payload.categories = liveScope.payload.categories
      payload.day = liveScope.payload.day
      payload.month = liveScope.payload.month
      payload.from = liveScope.payload.from
      payload.to = liveScope.payload.to
    }

    if (jobType === 'sync-papers') {
      payload.force = syncPapersForce
    }

    if (jobType === 'find-repos') {
      payload.force = findReposForce
    }

    try {
      setLaunchingJob(jobType)
      const response =
        jobType === 'export'
          ? {
              disposition: 'created' as const,
              job: await fetchJson<Job>(`/api/v1/jobs/${jobType}`, {
                method: 'POST',
                body: JSON.stringify(payload),
              }),
            }
          : await fetchJson<JobLaunch>(`/api/v1/jobs/${jobType}`, {
              method: 'POST',
              body: JSON.stringify(payload),
            })
      if (queueHandoffTimeoutRef.current !== null) {
        window.clearTimeout(queueHandoffTimeoutRef.current)
      }
      setLaunchFeedback({
        stepJob: jobType,
        job: response.job,
      })
      queueHandoffTimeoutRef.current = window.setTimeout(() => {
        setLaunchFeedback(null)
        queueHandoffTimeoutRef.current = null
      }, 3000)
      setSummaryRefreshTick((value) => value + 1)
      setJobsRefreshTick((value) => value + 1)
      setTableRefreshTick((value) => value + 1)
      setError(null)
      if (jobType === 'export') {
        setExportOutputName('')
        if (exportMenuRef.current) exportMenuRef.current.open = false
      }
    } catch (err) {
      if (queueHandoffTimeoutRef.current !== null) {
        window.clearTimeout(queueHandoffTimeoutRef.current)
        queueHandoffTimeoutRef.current = null
      }
      setLaunchFeedback(null)
      if (err instanceof Error) setError(err.message)
    } finally {
      setLaunchingJob(null)
    }
  }

  async function rerunExistingJob(jobId: string, sourceJob?: Job) {
    const previousJob =
      sourceJob ||
      jobsById.get(jobId) ||
      (selectedJobDetail?.id === jobId ? selectedJobDetail : null)
    const shouldMoveSelection = selectedJobIdRef.current === jobId

    try {
      setRerunningJobId(jobId)
      const rerun = await fetchJson<Job>(`/api/v1/jobs/${jobId}/rerun`, {
        method: 'POST',
      })

      const rerunGroupKey = attemptGroupKey(rerun)

      if (previousJob && isBatchFolderJob(previousJob) && expandedParentJobIdsRef.current.includes(previousJob.id)) {
        setExpandedParentJobIds((current) => (current.includes(rerun.id) ? current : [...current, rerun.id]))
        cancelChildJobsLoad(rerun.id)
        void loadChildJobs(rerun.id, { force: true })
      }

      const previousParentJobId = previousJob?.parent_job_id
      if (previousParentJobId && expandedParentJobIdsRef.current.includes(previousParentJobId)) {
        cancelChildJobsLoad(previousParentJobId)
        setChildJobsByParentId((current) => {
          const next = { ...current }
          delete next[previousParentJobId]
          childJobsByParentIdRef.current = next
          return next
        })
        void loadChildJobs(previousParentJobId, { force: true })
      }

      if (expandedJobGroupsRef.current.includes(rerunGroupKey)) {
        cancelJobHistoryLoad(rerunGroupKey)
        setJobAttemptHistories((current) => {
          const next = { ...current }
          delete next[rerunGroupKey]
          jobAttemptHistoriesRef.current = next
          return next
        })
        void loadJobHistory(rerunGroupKey, rerun.id, { force: true })
      }

      if (shouldMoveSelection && selectedJobIdRef.current === jobId) {
        protectRerunSelectionHandoff(rerun.id)
        selectedJobIdRef.current = rerun.id
        selectedJobDetailHydratedJobIdRef.current = rerun.id
        setSelectedPaperId(null)
        setSelectedExportId(null)
        setSelectedJobId(rerun.id)
        setSelectedJobDetail(rerun)
        setSelectedJobDetailLoading(false)
      }

      setSummaryRefreshTick((value) => value + 1)
      setJobsRefreshTick((value) => value + 1)
      setTableRefreshTick((value) => value + 1)
      setSelectedJobRefreshTick((value) => value + 1)
      setError(null)
    } catch (err) {
      if (err instanceof Error) setError(err.message)
    } finally {
      setRerunningJobId(null)
    }
  }

  rerunJobRef.current = (jobId: string) => {
    void rerunExistingJob(jobId)
  }

  async function stopExistingJob(jobId: string) {
    try {
      setStoppingJobIds((current) => (current.includes(jobId) ? current : [...current, jobId]))
      await fetchJson<Job>(`/api/v1/jobs/${jobId}/stop`, {
        method: 'POST',
      })
      setSummaryRefreshTick((value) => value + 1)
      setJobsRefreshTick((value) => value + 1)
      setTableRefreshTick((value) => value + 1)
      setError(null)
    } catch (err) {
      if (err instanceof Error) setError(err.message)
    } finally {
      setStoppingJobIds((current) => current.filter((value) => value !== jobId))
    }
  }

  stopJobRef.current = (jobId: string) => {
    void stopExistingJob(jobId)
  }

  const repoByUrl = useMemo<Record<string, Repo>>(() => {
    const result: Record<string, Repo> = {}
    for (const repo of repos) result[repo.github_url] = repo
    return result
  }, [repos])

  const paperRows = useMemo(
    () =>
      papers.map((paper) => {
        return {
          id: paper.arxiv_id,
          link_status: paper.link_status,
          arxiv_id: paper.arxiv_id,
          title: paper.title,
          author_label: formatAuthorLabel(paper.authors_json),
          categories: paper.categories_json,
          categories_label: paper.categories_json.join(', ') || paper.primary_category || '',
          published_at: paper.published_at || '',
          comment: paper.comment || '',
          journal_ref: paper.journal_ref || '',
          repo_label: paper.primary_github_url ? repoLabel(paper.primary_github_url) : '',
          repo_stars: paper.primary_github_stargazers_count,
          repo_language: paper.primary_github_language || '',
          repo_size_kb: paper.primary_github_size_kb,
          repo_created_at: paper.primary_github_created_at || '',
          repo_pushed_at: paper.primary_github_pushed_at || '',
          repo_description: paper.primary_github_description || '',
        }
      }),
    [papers],
  )

  const jobRows = useMemo(
    () => {
      const rows: JobGridRow[] = []

      function buildJobRow(
        job: Job,
        {
          rowKind,
          rowDepth,
          historyDepth,
          latestJob,
          parentJob,
        }: {
          rowKind: JobRowKind
          rowDepth: number
          historyDepth: number
          latestJob?: Job
          parentJob?: Job
        },
      ): JobGridRow {
        const groupKey = attemptGroupKey(job)
        const latestAttempt = latestJob ?? latestJobByGroup.get(groupKey) ?? job
        const isHistoryRow = rowKind === 'history'
        const isBatchFolder = !isHistoryRow && rowKind === 'root' && isBatchFolderJob(job)
        const historyLabel = isHistoryRow
          ? 'Earlier run'
          : isBatchFolder
            ? 'Batch folder'
            : job.attempt_count > 1
              ? rerunCountLabel(job.attempt_count)
              : attemptModeLabel(job.attempt_mode)
        const historySummary =
          latestAttempt.id !== job.id
            ? [jobSummary(job), `superseded by ${shortId(latestAttempt.id)}`]
                .filter((item) => item && item !== 'no stats yet')
                .join(' · ') || `Superseded by ${shortId(latestAttempt.id)}`
            : jobSummary(job)

        return {
          id: job.id,
          row_kind: rowKind,
          row_depth: rowDepth,
          history_depth: historyDepth,
          status: jobDisplayStatus(job),
          job_type: isBatchFolder ? batchFolderLabel(job.job_type) : jobTypeLabel(job.job_type),
          scope_label: scopeJsonLabel(job.scope_json),
          attempt_count: job.attempt_count,
          attempt_rank: job.attempt_rank,
          attempt_relation_label: historyLabel,
          created_at: job.created_at,
          child_progress: job.child_summary ? childSummaryLabel(job.child_summary) : '',
          summary: isHistoryRow ? historySummary : jobSummary(job),
          can_rerun: !isHistoryRow && canRerunJobInContext(job, parentJob),
          rerun_busy: rerunningJobId === job.id,
          can_stop: !isHistoryRow && canStopJob(job),
          stop_busy: stoppingJobIds.includes(job.id),
          children_toggleable: isBatchFolder,
          children_expanded: isBatchFolder && expandedParentJobSet.has(job.id),
          children_loading: isBatchFolder && loadingChildJobParentSet.has(job.id),
          history_toggleable: !isHistoryRow && !isBatchFolder && job.attempt_count > 1,
          history_expanded: !isHistoryRow && !isBatchFolder && expandedJobGroupSet.has(groupKey),
          history_loading: !isHistoryRow && !isBatchFolder && loadingJobHistoryGroupSet.has(groupKey),
          search_blob: [
            job.id,
            job.job_type,
            attemptModeLabel(job.attempt_mode),
            isBatchFolder ? 'batch folder' : jobTypeLabel(job.job_type),
            scopeJsonLabel(job.scope_json),
            jobDisplayStatus(job),
            childSummaryLabel(job.child_summary),
            isHistoryRow ? historySummary : jobSummary(job),
            job.error_text || '',
            historyLabel,
            isBatchFolder ? 'batch folder' : rowKind === 'root' ? 'root job' : rowKind === 'child' ? 'child job' : 'history attempt',
          ]
            .join(' ')
            .trim(),
        }
      }

      for (const job of jobs) {
        const groupKey = attemptGroupKey(job)
        const rootHistory = (jobAttemptHistories[groupKey] ?? []).filter((attempt) => attempt.id !== job.id)
        const showRootHistory = !isBatchFolderJob(job)

        rows.push(buildJobRow(job, { rowKind: 'root', rowDepth: 0, historyDepth: 0 }))

        if (showRootHistory && expandedJobGroupSet.has(groupKey)) {
          for (const attempt of rootHistory) {
            rows.push(buildJobRow(attempt, { rowKind: 'history', rowDepth: 0, historyDepth: 1, latestJob: job }))
          }
        }

        if (!expandedParentJobSet.has(job.id)) continue

        const childJobs = childJobsByParentId[job.id] ?? []

        for (const child of childJobs) {
          const childGroupKey = attemptGroupKey(child)
          const childHistory = (jobAttemptHistories[childGroupKey] ?? []).filter((attempt) => attempt.id !== child.id)

          rows.push(buildJobRow(child, { rowKind: 'child', rowDepth: 1, historyDepth: 0, parentJob: job }))

          if (!expandedJobGroupSet.has(childGroupKey)) continue

          for (const attempt of childHistory) {
            rows.push(buildJobRow(attempt, { rowKind: 'history', rowDepth: 1, historyDepth: 2, latestJob: child }))
          }
        }
      }

      return rows
    },
    [
      childJobsByParentId,
      expandedJobGroupSet,
      expandedParentJobSet,
      jobAttemptHistories,
      jobs,
      latestJobByGroup,
      loadingChildJobParentSet,
      loadingJobHistoryGroupSet,
      rerunningJobId,
      stoppingJobIds,
    ],
  )

  const exportRows = useMemo(
    () =>
      exportsData.map((row) => ({
        id: row.id,
        file_name: row.file_name,
        scope_label: scopeJsonLabel(row.scope_json),
        created_at: row.created_at,
        search_blob: [row.id, row.file_name, scopeJsonLabel(row.scope_json)].join(' ').trim(),
      })),
    [exportsData],
  )

  const paperColumns = useMemo<SheetColumn<Record<string, unknown>>[]>(
    () => [
      {
        field: 'link_status',
        headerName: 'Status',
        width: columnWidth('Status', 118),
        filter: compactValueColumnFilter,
        filterParams: createCompactSetFilterParams(),
        cellClass: (params) => statusCellClass(params.value),
      },
      {
        field: 'arxiv_id',
        headerName: 'ID',
        width: columnWidth('ID', 126),
        cellClass: 'mono-cell',
      },
      { field: 'title', headerName: 'Title', width: columnWidth('Title', 460) },
      {
        field: 'categories_label',
        headerName: 'Category',
        width: columnWidth('Category', 220),
        filter: compactValueColumnFilter,
        filterParams: createCompactSetFilterParams({
          extractValues: (row) =>
            Array.isArray(row.categories)
              ? row.categories
                  .map((value) => String(value ?? '').trim())
                  .filter((value) => value.length > 0)
              : [],
          searchPlaceholder: 'Search categories',
        }),
      },
      {
        field: 'published_at',
        headerName: 'Published',
        width: columnWidth('Published', 132),
        filter: compactDateColumnFilter,
        filterParams: compactDateFilterParams,
        valueFormatter: (params) => formatDate(String(params.value || '')),
      },
      { field: 'repo_label', headerName: 'Repo', width: columnWidth('Repo', 220), cellClass: 'mono-cell' },
      {
        field: 'repo_stars',
        headerName: 'Stars',
        width: columnWidth('Stars', 108),
        filter: compactNumberColumnFilter,
        filterParams: compactNumberFilterParams,
        cellClass: 'number-cell',
      },
      {
        field: 'repo_language',
        headerName: 'Language',
        width: columnWidth('Language', 148),
        filter: compactValueColumnFilter,
        filterParams: createCompactSetFilterParams(),
      },
      {
        field: 'repo_size_kb',
        headerName: 'Size',
        width: columnWidth('Size', 118),
        filter: compactNumberColumnFilter,
        filterParams: compactNumberFilterParams,
        valueFormatter: (params) => formatRepoSize(typeof params.value === 'number' ? params.value : null),
        cellClass: 'number-cell',
      },
      {
        field: 'repo_created_at',
        headerName: 'Created',
        width: columnWidth('Created', 146),
        filter: compactDateColumnFilter,
        filterParams: compactDateFilterParams,
        valueFormatter: (params) => formatDate(String(params.value || '')),
      },
      {
        field: 'repo_pushed_at',
        headerName: 'Pushed',
        width: columnWidth('Pushed', 146),
        filter: compactDateColumnFilter,
        filterParams: compactDateFilterParams,
        valueFormatter: (params) => formatDate(String(params.value || '')),
      },
      { field: 'journal_ref', headerName: 'Journal Ref', width: columnWidth('Journal Ref', 190) },
      { field: 'comment', headerName: 'Comment', width: columnWidth('Comment', 280) },
      { field: 'repo_description', headerName: 'Description', width: columnWidth('Description', 360) },
      { field: 'author_label', headerName: 'Author', width: columnWidth('Author', 280) },
    ],
    [],
  )

  const jobColumns = useMemo<SheetColumn<Record<string, unknown>>[]>(
    () => [
      {
        field: 'children_toggle',
        colId: 'children_toggle',
        headerName: '',
        width: 54,
        minWidth: 54,
        maxWidth: 54,
        resizable: false,
        sortable: false,
        filter: false,
        hideable: false,
        suppressMovable: true,
        lockPosition: 'left',
        cellClass: 'job-tree-toggle-cell',
        cellRenderer: JobChildrenChevronCellRenderer,
        cellRendererParams: {
          suppressMouseEventHandling: suppressInteractiveCellMouseHandling,
          onToggleChildren: (jobId: string) => {
            const job = rootJobById.get(jobId)
            if (!job || !isBatchFolderJob(job)) return

            if (expandedParentJobSet.has(jobId)) {
              cancelChildJobsLoad(jobId)
              setExpandedParentJobIds((current) => current.filter((value) => value !== jobId))
              return
            }

            setExpandedParentJobIds((current) => (current.includes(jobId) ? current : [...current, jobId]))
            void loadChildJobs(jobId)
          },
        },
      },
      {
        field: 'history_toggle',
        colId: 'history_toggle',
        headerName: '',
        width: 54,
        minWidth: 54,
        maxWidth: 54,
        resizable: false,
        sortable: false,
        filter: false,
        hideable: false,
        suppressMovable: true,
        lockPosition: 'left',
        cellClass: 'job-history-toggle-cell',
        cellRenderer: JobHistoryChevronCellRenderer,
        cellRendererParams: {
          suppressMouseEventHandling: suppressInteractiveCellMouseHandling,
          onToggleHistory: (jobId: string) => {
            const job = latestJobById.get(jobId)
            if (!job || job.attempt_count <= 1) return

            const groupKey = attemptGroupKey(job)
            if (expandedJobGroupSet.has(groupKey)) {
              cancelJobHistoryLoad(groupKey)
              setExpandedJobGroups((current) => current.filter((value) => value !== groupKey))
              return
            }

            setExpandedJobGroups((current) => (current.includes(groupKey) ? current : [...current, groupKey]))
            void loadJobHistory(groupKey, job.id)
          },
        },
      },
      {
        field: 'status',
        headerName: 'Status',
        width: columnWidth('Status', 132),
        filter: compactValueColumnFilter,
        filterParams: createCompactSetFilterParams(),
        cellClass: (params) => statusCellClass(params.value),
      },
      {
        field: 'job_type',
        headerName: 'Job Type',
        width: columnWidth('Job Type', 180),
        filter: compactValueColumnFilter,
        filterParams: createCompactSetFilterParams(),
      },
      { field: 'scope_label', headerName: 'Scope', width: columnWidth('Scope', 320) },
      {
        field: 'attempt_relation_label',
        headerName: 'Attempt',
        width: columnWidth('Attempt', 200),
        cellClass: (params) => {
          const rowKind = params.data?.row_kind
          return rowKind === 'history' ? 'job-attempt-cell job-attempt-cell-history' : 'job-attempt-cell'
        },
        cellRenderer: JobAttemptCellRenderer,
      },
      { field: 'child_progress', headerName: 'Child Jobs', width: columnWidth('Child Jobs', 260) },
      {
        field: 'created_at',
        headerName: 'Created',
        width: columnWidth('Created', 172),
        filter: compactDateColumnFilter,
        filterParams: compactDateFilterParams,
        valueFormatter: (params) => formatTime(String(params.value || '')),
      },
      { field: 'summary', headerName: 'Summary', width: columnWidth('Summary', 420) },
      {
        field: 'rerun_action',
        headerName: 'Re-run',
        width: columnWidth('Re-run', 118),
        sortable: false,
        filter: false,
        hideable: false,
        cellRenderer: JobRerunCellRenderer,
        cellRendererParams: {
          suppressMouseEventHandling: suppressInteractiveCellMouseHandling,
          onRerun: (jobId: string) => rerunJobRef.current(jobId),
        },
      },
      {
        field: 'stop_action',
        headerName: 'Stop',
        width: columnWidth('Stop', 108),
        sortable: false,
        filter: false,
        hideable: false,
        cellRenderer: JobStopCellRenderer,
        cellRendererParams: {
          suppressMouseEventHandling: suppressInteractiveCellMouseHandling,
          onStop: (jobId: string) => stopJobRef.current(jobId),
        },
      },
      { field: 'id', headerName: 'Job ID', width: columnWidth('Job ID', 280), hide: true, cellClass: 'mono-cell' },
      {
        field: 'search_blob',
        headerName: 'Search Blob',
        hide: true,
        hideable: false,
        sortable: false,
        filter: false,
      },
    ],
    [
      cancelChildJobsLoad,
      cancelJobHistoryLoad,
      expandedJobGroupSet,
      expandedParentJobSet,
      latestJobById,
      loadChildJobs,
      loadJobHistory,
      rootJobById,
    ],
  )

  const exportColumns = useMemo<SheetColumn<Record<string, unknown>>[]>(
    () => [
      { field: 'file_name', headerName: 'File Name', width: columnWidth('File Name', 280) },
      { field: 'scope_label', headerName: 'Scope', width: columnWidth('Scope', 360) },
      {
        field: 'created_at',
        headerName: 'Created',
        width: columnWidth('Created', 180),
        filter: compactDateColumnFilter,
        filterParams: compactDateFilterParams,
        valueFormatter: (params) => formatTime(String(params.value || '')),
      },
      { field: 'id', headerName: 'Export ID', width: columnWidth('Export ID', 280), hide: true, cellClass: 'mono-cell' },
      {
        field: 'search_blob',
        headerName: 'Search Blob',
        hide: true,
        hideable: false,
        sortable: false,
        filter: false,
      },
    ],
    [],
  )

  const jobHistoryList = useMemo(() => Object.values(jobAttemptHistories).flat(), [jobAttemptHistories])
  const jobsById = useMemo(() => {
    const result = new Map<string, Job>()
    for (const job of jobs) result.set(job.id, job)
    for (const job of loadedChildJobs) result.set(job.id, job)
    for (const job of jobHistoryList) result.set(job.id, job)
    return result
  }, [jobHistoryList, jobs, loadedChildJobs])

  const selectedPaperSummary = papers.find((paper) => paper.arxiv_id === selectedPaperId) || null
  const selectedPaperDetail = (selectedPaperId ? paperDetailsById[selectedPaperId] : null) || null
  const selectedPaper = selectedPaperDetail ?? selectedPaperSummary
  const selectedJobSummary = (selectedJobId ? jobsById.get(selectedJobId) : null) || null
  const selectedJob = selectedJobDetail?.id === selectedJobId ? selectedJobDetail : selectedJobSummary
  const selectedExport = exportsData.find((row) => row.id === selectedExportId) || null
  const selectedJobParent = selectedJob?.parent_job_id ? rootJobById.get(selectedJob.parent_job_id) : null
  const selectedJobCanRerun = selectedJob ? canRerunJobInContext(selectedJob, selectedJobParent) : false
  const selectedJobCanStop = selectedJob ? canStopJob(selectedJob) : false
  const paperTotalRows = Math.max(dashboard?.papers ?? 0, papers.length, papersLoadedCount)
  const paperProgressTotal = paperTotalRows > 0 ? paperTotalRows : undefined
  const selectedJobLatestChildren = useMemo(
    () => selectedJobChildren.filter((job) => isLatestAttempt(job)),
    [selectedJobChildren],
  )

  useEffect(() => {
    if (previewTab !== 'jobs' || !selectedJobId) return
    const timer = window.setInterval(() => setSelectedJobRefreshTick((value) => value + 1), ACTIVE_JOBS_POLL_MS)
    return () => window.clearInterval(timer)
  }, [previewTab, selectedJobId])

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    async function loadSelectedJobDetail() {
      if (previewTab !== 'jobs' || !selectedJobId) {
        selectedJobDetailHydratedJobIdRef.current = null
        setSelectedJobDetail(null)
        setSelectedJobDetailLoading(false)
        return
      }

      const needsBlockingLoad = selectedJobDetailHydratedJobIdRef.current !== selectedJobId

      if (needsBlockingLoad) {
        setSelectedJobDetailLoading(true)
      }

      try {
        const data = await fetchJson<Job>(`/api/v1/jobs/${selectedJobId}`, {
          signal: controller.signal,
        })
        if (cancelled) return
        selectedJobDetailHydratedJobIdRef.current = selectedJobId
        setSelectedJobDetail(data)
        setSelectedJobDetailLoading(false)
      } catch (err) {
        if (cancelled || isAbortError(err) || !(err instanceof Error)) return
        setSelectedJobDetailLoading(false)
        setError(err.message)
      }
    }

    void loadSelectedJobDetail()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [previewTab, selectedJobId, selectedJobRefreshTick])

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    async function loadSelectedPaperDetail() {
      if (previewTab !== 'papers' || !selectedPaperId) {
        setSelectedPaperLoading(false)
        return
      }

      setSelectedPaperLoading(true)

      try {
        const data = await fetchJson<PaperDetail>(`/api/v1/papers/${selectedPaperId}`, {
          signal: controller.signal,
        })
        if (cancelled) return
        setPaperDetailsById((current) => ({ ...current, [data.arxiv_id]: data }))
        setSelectedPaperLoading(false)
      } catch (err) {
        if (cancelled || isAbortError(err) || !(err instanceof Error)) return
        setSelectedPaperLoading(false)
        setError(err.message)
      }
    }

    void loadSelectedPaperDetail()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [previewTab, selectedPaperId, tableRefreshTick])

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    async function loadSelectedJobChildren() {
      if (previewTab !== 'jobs' || !selectedJob || !isBatchFolderJob(selectedJob)) {
        selectedJobChildrenHydratedJobIdRef.current = null
        setSelectedJobChildren([])
        setSelectedJobChildrenLoading(false)
        return
      }

      const needsBlockingLoad = selectedJobChildrenHydratedJobIdRef.current !== selectedJob.id

      if (needsBlockingLoad) {
        setSelectedJobChildren([])
        setSelectedJobChildrenLoading(true)
      }

      try {
        const data = await fetchJson<Job[]>(`/api/v1/jobs?parent_id=${selectedJob.id}&limit=${JOB_PREVIEW_LIMIT}&view=all`, {
          signal: controller.signal,
        })
        if (cancelled) return
        selectedJobChildrenHydratedJobIdRef.current = selectedJob.id
        setSelectedJobChildren(data)
        setSelectedJobChildrenLoading(false)
      } catch (err) {
        if (cancelled || isAbortError(err) || !(err instanceof Error)) return
        setSelectedJobChildrenLoading(false)
        setError(err.message)
      }
    }

    void loadSelectedJobChildren()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [previewTab, selectedJob?.id, selectedJob?.job_type, selectedJobRefreshTick])

  const selectedJobLatestAttempt = useMemo(
    () => selectedJobAttempts.find((job) => isLatestAttempt(job)) || selectedJob,
    [selectedJobAttempts, selectedJob],
  )

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    async function loadSelectedJobAttempts() {
      if (previewTab !== 'jobs' || !selectedJob) {
        selectedJobAttemptsHydratedJobIdRef.current = null
        setSelectedJobAttempts([])
        setSelectedJobAttemptsLoading(false)
        return
      }

      if (selectedJob.attempt_count <= 1) {
        selectedJobAttemptsHydratedJobIdRef.current = selectedJob.id
        setSelectedJobAttempts([])
        setSelectedJobAttemptsLoading(false)
        return
      }

      const needsBlockingLoad = selectedJobAttemptsHydratedJobIdRef.current !== selectedJob.id

      if (needsBlockingLoad) {
        setSelectedJobAttempts([])
        setSelectedJobAttemptsLoading(true)
      }

      try {
        const data = await fetchJson<Job[]>(`/api/v1/jobs/${selectedJob.id}/attempts?limit=${JOB_PREVIEW_LIMIT}`, {
          signal: controller.signal,
        })
        if (cancelled) return
        selectedJobAttemptsHydratedJobIdRef.current = selectedJob.id
        setSelectedJobAttempts(data)
        setSelectedJobAttemptsLoading(false)
      } catch (err) {
        if (cancelled || isAbortError(err) || !(err instanceof Error)) return
        setSelectedJobAttemptsLoading(false)
        setError(err.message)
      }
    }

    void loadSelectedJobAttempts()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [previewTab, selectedJob?.attempt_count, selectedJob?.id, selectedJobRefreshTick])

  function closeDrawer() {
    clearRerunSelectionHandoff()
    selectedJobDetailHydratedJobIdRef.current = null
    selectedJobChildrenHydratedJobIdRef.current = null
    selectedJobAttemptsHydratedJobIdRef.current = null
    selectedJobIdRef.current = null
    setSelectedPaperId(null)
    setSelectedJobId(null)
    setSelectedExportId(null)
    setSelectedJobDetail(null)
    setSelectedJobDetailLoading(false)
    setSelectedJobChildren([])
    setSelectedJobAttempts([])
  }

  function selectJob(jobId: string | null) {
    const rerunHandoffJobId = rerunSelectionHandoffJobIdRef.current
    if (jobId === null && rerunHandoffJobId && selectedJobIdRef.current === rerunHandoffJobId) {
      return
    }
    if (jobId !== rerunHandoffJobId) {
      clearRerunSelectionHandoff()
    }
    selectedJobIdRef.current = jobId
    setSelectedJobId(jobId)
  }

  function handlePreviewTabChange(nextTab: PreviewTab) {
    if (nextTab === previewTab) {
      return
    }
    if (previewTab === 'jobs' && nextTab !== 'jobs') {
      cancelAllJobTreeLoads()
    }
    setPreviewTab(nextTab)
    setTableSearch('')
    setVisibleKeys([])
    clearRerunSelectionHandoff()
    selectedJobDetailHydratedJobIdRef.current = null
    selectedJobChildrenHydratedJobIdRef.current = null
    selectedJobAttemptsHydratedJobIdRef.current = null
    selectedJobIdRef.current = null
    setSelectedPaperId(null)
    setSelectedJobId(null)
    setSelectedExportId(null)
    setSelectedJobDetail(null)
    setSelectedJobDetailLoading(false)
    setSelectedJobChildren([])
    setSelectedJobAttempts([])
    setExpandedParentJobIds([])
    setExpandedJobGroups([])
    setChildJobsByParentId({})
    setLoadingChildJobParentIds([])
    setJobAttemptHistories({})
    setLoadingJobHistoryGroups([])
    if (exportMenuRef.current) exportMenuRef.current.open = false
  }

  function handleDisplayedKeysChange(keys: string[]) {
    setVisibleKeys(keys)
    const rerunHandoffJobId = rerunSelectionHandoffJobIdRef.current
    if (rerunHandoffJobId && selectedJobId !== rerunHandoffJobId) {
      clearRerunSelectionHandoff()
    } else if (rerunHandoffJobId && keys.includes(rerunHandoffJobId)) {
      clearRerunSelectionHandoff()
    }

    if (previewTab === 'papers' && selectedPaperId && !keys.includes(selectedPaperId)) {
      setSelectedPaperId(null)
      return
    }
    if (previewTab === 'jobs' && selectedJobId && !keys.includes(selectedJobId)) {
      if (rerunHandoffJobId === selectedJobId) return
      selectedJobIdRef.current = null
      setSelectedJobId(null)
      return
    }
    if (previewTab === 'exports' && selectedExportId && !keys.includes(selectedExportId)) {
      setSelectedExportId(null)
    }
  }

  function renderDrawerContent() {
    if (previewTab === 'papers') {
      if (!selectedPaper) {
        return <EmptyState title="No paper selected" detail="Select a paper to inspect its link state and repository context." />
      }

      const repo = selectedPaper.primary_github_url ? repoByUrl[selectedPaper.primary_github_url] : undefined
      const selectedPaperRepoUrls = selectedPaperDetail?.github_urls ?? []
      return (
        <div className="drawer-content">
          <div className="drawer-header">
            <div>
              <p className="panel-kicker">Paper details</p>
              <h3>{selectedPaper.title}</h3>
            </div>
            <button type="button" className="ghost-button" onClick={closeDrawer}>
              Close
            </button>
          </div>

          <div className="drawer-tags">
            <StatusTag value={selectedPaper.link_status} />
            <span className="meta-chip">{selectedPaper.primary_category || 'uncategorized'}</span>
            <a className="meta-chip meta-chip-link" href={selectedPaper.abs_url} target="_blank" rel="noreferrer">
              arXiv
            </a>
            {selectedPaper.primary_github_url ? (
              <a className="meta-chip meta-chip-link" href={selectedPaper.primary_github_url} target="_blank" rel="noreferrer">
                GitHub
              </a>
            ) : null}
            <span className="meta-chip">{selectedPaper.published_at || 'no published date'}</span>
          </div>

          <DetailBlock
            label="Abstract"
            value={
              selectedPaperDetail ? (
                <p className="long-text">{selectedPaperDetail.abstract}</p>
              ) : selectedPaperLoading ? (
                <p className="long-text">Loading paper details…</p>
              ) : (
                <p className="long-text">Paper details are not available yet.</p>
              )
            }
          />
          <DetailBlock label="Authors" value={joinList(selectedPaper.authors_json, 8)} />
          <DetailBlock label="Categories" value={joinList(selectedPaper.categories_json, 8)} />
          <DetailBlock label="Updated" value={formatTime(selectedPaper.updated_at)} />
          <DetailBlock
            label="ArXiv"
            value={
              <a className="table-link" href={selectedPaper.abs_url} target="_blank" rel="noreferrer">
                {selectedPaper.arxiv_id}
              </a>
            }
          />
          <DetailBlock label="Link refresh after" value={formatTime(selectedPaper.refresh_after)} />
          <DetailBlock label="Last attempt" value={formatTime(selectedPaper.last_attempt_at)} />
          <DetailBlock label="Attempt result" value={selectedPaper.last_attempt_complete ? 'complete' : 'incomplete'} />
          <DetailBlock label="Attempt error" value={selectedPaper.last_attempt_error || '—'} />
          {selectedPaperDetail?.comment ? <DetailBlock label="Comment" value={selectedPaperDetail.comment} /> : null}
          {selectedPaperDetail?.journal_ref ? <DetailBlock label="Journal Ref" value={selectedPaperDetail.journal_ref} /> : null}
          {selectedPaperDetail?.doi ? <DetailBlock label="DOI" value={selectedPaperDetail.doi} /> : null}
          <DetailBlock
            label="Repository"
            value={
              selectedPaper.primary_github_url ? (
                <div className="linked-card">
                  <a className="table-link" href={selectedPaper.primary_github_url} target="_blank" rel="noreferrer">
                    {repoLabel(selectedPaper.primary_github_url)}
                  </a>
                  <p className="row-meta">
                    {repo
                      ? `${repo.stargazers_count ?? 0} stars · ${repo.license_spdx_id || repo.license_name || 'no license'} · updated ${formatTime(repo.updated_at)}`
                      : 'metadata not fetched yet'}
                  </p>
                </div>
              ) : (
                'No linked repository'
              )
            }
          />
          {selectedPaperRepoUrls.length > 1 ? (
            <DetailBlock
              label="Other candidates"
              value={
                <div className="linked-list">
                  {selectedPaperRepoUrls.map((url) => (
                    <a key={url} className="linked-list-item" href={url} target="_blank" rel="noreferrer">
                      {repoLabel(url)}
                    </a>
                  ))}
                </div>
              }
            />
          ) : null}
        </div>
      )
    }

    if (previewTab === 'jobs') {
      if (!selectedJob) {
        return <EmptyState title="No job selected" detail="Select a job to inspect its payload, status and error summary." />
      }

      const selectedJobIsBatchFolder = isBatchFolderJob(selectedJob)
      const selectedJobIsHistorical = !isLatestAttempt(selectedJob)
      const selectedJobAttemptList = selectedJobAttempts.length > 0 ? selectedJobAttempts : [selectedJob]
      const selectedJobRerunCount = Math.max(0, selectedJob.attempt_count - 1)

      return (
        <div className="drawer-content">
          <div className="drawer-header">
            <div>
              <p className="panel-kicker">Job details</p>
              <h3>{jobTypeLabel(selectedJob.job_type)}</h3>
            </div>
            <div className="drawer-header-actions">
              {selectedJobIsHistorical && selectedJobLatestAttempt && selectedJobLatestAttempt.id !== selectedJob.id ? (
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => selectJob(selectedJobLatestAttempt.id)}
                >
                  Jump to latest
                </button>
              ) : null}
              {selectedJobCanRerun ? (
                <button
                  type="button"
                  className="primary-button"
                  onClick={() => void rerunExistingJob(selectedJob.id, selectedJob)}
                  disabled={rerunningJobId === selectedJob.id}
                >
                  {rerunningJobId === selectedJob.id ? 'Queued…' : 'Re-run'}
                </button>
              ) : null}
              {selectedJobCanStop ? (
                <button
                  type="button"
                  className="ghost-button danger-button"
                  onClick={() => void stopExistingJob(selectedJob.id)}
                  disabled={stoppingJobIds.includes(selectedJob.id)}
                >
                  {stoppingJobIds.includes(selectedJob.id) ? 'Stopping…' : 'Stop'}
                </button>
              ) : null}
              <button type="button" className="ghost-button" onClick={closeDrawer}>
                Close
              </button>
            </div>
          </div>

          <div className="drawer-tags">
            <StatusTag value={jobDisplayStatus(selectedJob)} />
            <span className="meta-chip">{selectedJobIsHistorical ? 'Earlier run in chain' : selectedJobIsBatchFolder ? 'Batch folder' : attemptModeLabel(selectedJob.attempt_mode)}</span>
            {selectedJob.attempt_count > 1 ? (
              <span className="meta-chip">
                {selectedJobRerunCount === 1 ? '1 rerun in chain' : `${formatInteger(selectedJobRerunCount)} reruns in chain`}
              </span>
            ) : null}
            <span className="meta-chip">worker attempts {selectedJob.attempts}</span>
            <span className="meta-chip">{formatTime(selectedJob.created_at)}</span>
            {selectedJobDetailLoading ? <span className="meta-chip">Refreshing…</span> : null}
          </div>

          <DetailBlock label="Job id" value={<span className="mono-cell">{selectedJob.id}</span>} />
          {selectedJob.parent_job_id ? <DetailBlock label="Parent job" value={<span className="mono-cell">{selectedJob.parent_job_id}</span>} /> : null}
          <DetailBlock label="Attempt mode" value={attemptModeLabel(selectedJob.attempt_mode)} />
          {selectedJobIsHistorical && selectedJobLatestAttempt && selectedJobLatestAttempt.id !== selectedJob.id ? (
            <DetailBlock
              label="Superseded by"
              value={
                <button type="button" className="drawer-inline-action" onClick={() => selectJob(selectedJobLatestAttempt.id)}>
                  {shortId(selectedJobLatestAttempt.id)} · {formatTime(selectedJobLatestAttempt.created_at)}
                </button>
              }
            />
          ) : null}
          <DetailBlock label="Scope" value={scopeJsonLabel(selectedJob.scope_json)} />
          <DetailBlock label="Started at" value={formatTime(selectedJob.started_at)} />
          <DetailBlock label="Stop requested at" value={formatTime(selectedJob.stop_requested_at)} />
          <DetailBlock label="Last heartbeat" value={formatTime(selectedJob.locked_at)} />
          <DetailBlock label="Finished at" value={formatTime(selectedJob.finished_at)} />
          {selectedJob.attempt_count > 1 ? (
            <DetailBlock
              label={selectedJobIsBatchFolder ? 'Batch run history' : 'Run history'}
              value={
                selectedJobAttemptsLoading ? (
                  <p className="long-text">{selectedJobIsBatchFolder ? 'Loading batch runs…' : 'Loading run history…'}</p>
                ) : (
                  <div className="linked-list">
                    {selectedJobAttemptList.map((attempt) => (
                      <button
                        key={attempt.id}
                        type="button"
                        className={attempt.id === selectedJob.id ? 'attempt-history-item active' : 'attempt-history-item'}
                        onClick={() => selectJob(attempt.id)}
                      >
                        <span className="attempt-history-title">
                          <StatusTag value={jobDisplayStatus(attempt)} />
                          <span className="meta-chip">
                            {attempt.attempt_rank === 1 ? (selectedJobIsBatchFolder ? 'Latest batch run' : 'Latest run') : 'Earlier run'}
                          </span>
                        </span>
                        <span className="row-meta">
                          {shortId(attempt.id)} · {formatTime(attempt.created_at)}
                        </span>
                      </button>
                    ))}
                  </div>
                )
              }
            />
          ) : null}
          {selectedJob.child_summary ? <DetailBlock label="Latest child job summary" value={childSummaryLabel(selectedJob.child_summary)} /> : null}
          <DetailBlock
            label="Stats"
            value={
              <p className="long-text">
                {jobStatsDetailSummary(selectedJob)}
              </p>
            }
          />
          {selectedJob.error_text ? <DetailBlock label="Error" value={<p className="long-text">{selectedJob.error_text}</p>} /> : null}
          {isBatchFolderJob(selectedJob) ? (
            <DetailBlock
              label="Latest child attempt per scope"
              value={
                selectedJobChildrenLoading ? (
                  <p className="long-text">Loading child jobs…</p>
                ) : selectedJobLatestChildren.length === 0 ? (
                  <p className="long-text">No child jobs have been created yet.</p>
                ) : (
                  <div className="linked-list">
                    <p className="row-meta child-job-note">
                      Each card shows the latest attempt for one scope. Re-running a child only queues that scope again.
                    </p>
                    {selectedJobLatestChildren.map((child) => (
                      <div key={child.id} className="child-job-card">
                        <div className="child-job-copy">
                          <div className="drawer-tags">
                            <StatusTag value={jobDisplayStatus(child)} />
                            <span className="meta-chip">{scopeJsonLabel(child.scope_json)}</span>
                          </div>
                          <p className="row-meta">
                            {jobSummary(child)} · created {formatTime(child.created_at)}
                          </p>
                        </div>
                        {canStopJob(child) ? (
                          <button
                            type="button"
                            className="ghost-button child-job-action danger-button"
                            onClick={() => void stopExistingJob(child.id)}
                            disabled={stoppingJobIds.includes(child.id)}
                          >
                            {stoppingJobIds.includes(child.id) ? 'Stopping…' : 'Stop'}
                          </button>
                        ) : canRerunJobInContext(child, selectedJob) ? (
                          <button
                            type="button"
                            className="ghost-button child-job-action"
                            onClick={() => void rerunExistingJob(child.id, child)}
                            disabled={rerunningJobId === child.id}
                          >
                            {rerunningJobId === child.id ? 'Queued…' : 'Re-run'}
                          </button>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )
              }
            />
          ) : null}
        </div>
      )
    }

    if (!selectedExport) {
      return <EmptyState title="No export selected" detail="Select an export row to inspect and download it." />
    }

    return (
      <div className="drawer-content">
        <div className="drawer-header">
          <div>
            <p className="panel-kicker">Export details</p>
            <h3>{selectedExport.file_name}</h3>
          </div>
          <button type="button" className="ghost-button" onClick={closeDrawer}>
            Close
          </button>
        </div>

        <div className="drawer-tags">
          <span className="meta-chip">{formatTime(selectedExport.created_at)}</span>
        </div>

        <DetailBlock label="Export id" value={<span className="mono-cell">{selectedExport.id}</span>} />
        <DetailBlock label="Scope" value={scopeJsonLabel(selectedExport.scope_json)} />
        <DetailBlock label="Created at" value={formatTime(selectedExport.created_at)} />
        <DetailBlock
          label="Download"
          value={
            <a className="download-link" href={`/api/v1/exports/${selectedExport.id}/download`}>
              download CSV
            </a>
          }
        />
      </div>
    )
  }

  const activeLoadedRows = previewTab === 'papers' ? papers.length : previewTab === 'jobs' ? jobRows.length : exportsData.length
  const activeTotalRows = previewTab === 'papers' ? paperTotalRows : activeLoadedRows
  const tableSummaryLabel = `${visibleKeys.length.toLocaleString()} / ${activeTotalRows.toLocaleString()} rows`
  const quickSearchPlaceholder =
    previewTab === 'papers'
      ? 'Search title, abstract, authors, repo...'
      : previewTab === 'jobs'
        ? 'Search job id, scope, summary...'
        : 'Search export name or scope...'

  const sheetToolbarLeading = (
    <div className="tab-strip">
      <button type="button" className={previewTab === 'papers' ? 'tab-button active' : 'tab-button'} onClick={() => handlePreviewTabChange('papers')}>
        Papers
      </button>
      <button type="button" className={previewTab === 'jobs' ? 'tab-button active' : 'tab-button'} onClick={() => handlePreviewTabChange('jobs')}>
        Jobs
      </button>
      <button type="button" className={previewTab === 'exports' ? 'tab-button active' : 'tab-button'} onClick={() => handlePreviewTabChange('exports')}>
        Exports
      </button>
    </div>
  )

  const sheetToolbarSearch = (
    <label className="sheet-search-field">
      <input
        className="floating-placeholder-input"
        value={tableSearch}
        onChange={(event) => setTableSearch(event.target.value)}
        placeholder={quickSearchPlaceholder}
        aria-label="Quick search"
      />
    </label>
  )

  const sheetToolbarSummary = (
    <span className="sheet-inline-summary" title="Rows currently shown / rows in total">
      {tableSummaryLabel}
    </span>
  )

  const sheetToolbarActions = (
    <button
      type="button"
      className="ghost-button refresh-button"
      onClick={() => {
        setSummaryRefreshTick((value) => value + 1)
        setJobsRefreshTick((value) => value + 1)
        setTableRefreshTick((value) => value + 1)
      }}
    >
      Refresh
    </button>
  )

  const sheetToolbarExport =
    previewTab === 'papers' ? (
      <details ref={exportMenuRef} className="export-menu">
        <summary className="primary-button export-menu-trigger">Export CSV</summary>
        <div className="export-menu-panel">
          <div className="form-field">
            <span className="field-label">Export scope</span>
            <div className="segmented-control export-mode-control" role="tablist" aria-label="Export mode">
              <button
                type="button"
                className={exportMode === 'all_papers' ? 'segment-button active' : 'segment-button'}
                onClick={() => setExportMode('all_papers')}
              >
                Full export
              </button>
              <button
                type="button"
                className={exportMode === 'papers_view' ? 'segment-button active' : 'segment-button'}
                onClick={() => setExportMode('papers_view')}
              >
                Current view
              </button>
            </div>
            <span className="inline-note">
              {exportMode === 'all_papers'
                ? `Export all ${dashboard?.papers ?? papers.length} papers from the local database.`
                : `Export ${filteredPaperIds.length} visible papers in the current filter and sort order.`}
            </span>
          </div>

          <label className="form-field">
            <span className="field-label">Export name</span>
            <div className="input-with-suffix">
              <input
                className="floating-placeholder-input suffix-input"
                value={exportOutputName}
                onChange={(event) => setExportOutputName(normalizeExportBaseName(event.target.value))}
                placeholder="cv-weekly-2026-04-19"
              />
              <span className="input-suffix">.csv</span>
            </div>
          </label>

          <div className="export-menu-actions">
            <button
              type="button"
              className="primary-button"
              onClick={() => launchJob('export')}
              disabled={
                launchingJob !== null ||
                !exportNameValid ||
                (exportMode === 'papers_view' && !filteredPaperExportReady)
              }
            >
              {launchingJob === 'export' ? 'Running…' : 'Queue export'}
            </button>
            <button
              type="button"
              className="ghost-button"
              onClick={() => {
                handlePreviewTabChange('exports')
              }}
            >
              View exports
            </button>
          </div>
        </div>
      </details>
    ) : null

  const activeGrid =
    previewTab === 'papers' ? (
      <AgGridSheet
        key="papers"
        columns={paperColumns}
        rows={paperRows}
        rowKey="id"
        selectedKey={selectedPaperId}
        onSelectedKeyChange={setSelectedPaperId}
        onDisplayedKeysChange={handleDisplayedKeysChange}
        quickSearch={deferredTableSearch}
        persistenceId="papertorepo-papers-v2"
        emptyMessage="No papers are stored yet."
        toolbarLeading={sheetToolbarLeading}
        toolbarActions={sheetToolbarActions}
        toolbarSearch={sheetToolbarSearch}
        toolbarSummary={sheetToolbarSummary}
        toolbarAfterSummary={sheetToolbarExport}
        loading={papersLoading}
        loadingLabel={papers.length > 0 ? 'Refreshing papers…' : 'Loading papers…'}
        loadingSummaryMode="labelOnly"
        progressCurrent={papersLoadedCount}
        progressTotal={paperProgressTotal}
      />
    ) : previewTab === 'jobs' ? (
      <AgGridSheet
        key="jobs"
        columns={jobColumns}
        rows={jobRows}
        rowKey="id"
        selectedKey={selectedJobId}
        onSelectedKeyChange={selectJob}
        onDisplayedKeysChange={handleDisplayedKeysChange}
        quickSearch={deferredTableSearch}
        persistenceId="papertorepo-jobs"
        emptyMessage="No jobs yet."
        toolbarLeading={sheetToolbarLeading}
        toolbarActions={sheetToolbarActions}
        toolbarSearch={sheetToolbarSearch}
        toolbarSummary={sheetToolbarSummary}
        getRowClass={(row) => {
          const classes: string[] = []
          if (row.row_kind === 'child') classes.push('sheet-row-child')
          if (row.row_kind === 'history') classes.push('sheet-row-history')
          if (typeof row.row_depth === 'number') classes.push(`sheet-row-depth-${row.row_depth}`)
          return classes.join(' ') || undefined
        }}
      />
    ) : (
      <AgGridSheet
        key="exports"
        columns={exportColumns}
        rows={exportRows}
        rowKey="id"
        selectedKey={selectedExportId}
        onSelectedKeyChange={setSelectedExportId}
        onDisplayedKeysChange={handleDisplayedKeysChange}
        quickSearch={deferredTableSearch}
        persistenceId="papertorepo-exports"
        emptyMessage="No exports yet."
        toolbarLeading={sheetToolbarLeading}
        toolbarActions={sheetToolbarActions}
        toolbarSearch={sheetToolbarSearch}
        toolbarSummary={sheetToolbarSummary}
        loading={exportsLoading}
        loadingLabel={exportsData.length > 0 ? 'Refreshing exports…' : 'Loading exports…'}
      />
    )

  return (
    <main className="app-shell">
      <header className="topbar panel">
        <div className="headline">
          <div>
            <p className="eyebrow">papertorepo</p>
            <h1>Research Workspace</h1>
          </div>
          <p className="lede">Pull arXiv papers, find GitHub repos, refresh repo facts, and export the working set.</p>
        </div>

        <div className="header-meta">
          <span className="meta-chip">job scope: {scopeLabel(liveScope.payload)}</span>
          <span className="meta-chip">queue: {queueModeLabel}</span>
          <span className="meta-chip">database: {health?.database_dialect ?? 'loading'}</span>
          <span className="meta-chip">{githubRuntimeLabel}</span>
          <span className="meta-chip">refreshed: {formatClock(lastRefreshedAt)}</span>
          <span className="meta-chip">{hasActiveJobs ? 'jobs/dashboard 1s · tables 20s' : previewTab === 'jobs' ? 'jobs 5s · dashboard 8s' : 'idle · dashboard 8s'}</span>
        </div>

        <div className="stats-bar header-stats">
          <div className="stat-pill">
            <span>Papers</span>
            <strong>{dashboard?.papers ?? 0}</strong>
          </div>
          <div className="stat-pill">
            <span>Papers with linked repo</span>
            <strong>{dashboard?.found ?? 0}</strong>
          </div>
          <div className="stat-pill">
            <span>Repos with metadata</span>
            <strong>{dashboard?.repos ?? 0}</strong>
          </div>
          <div className="stat-pill">
            <span>Running jobs</span>
            <strong>{dashboard?.running_jobs ?? 0}</strong>
          </div>
          <div className="stat-pill">
            <span>Exports</span>
            <strong>{dashboard?.exports ?? 0}</strong>
          </div>
        </div>
      </header>

      <section className="scope-bar panel">
        <div className="section-copy">
          <p className="panel-kicker">Job scope</p>
          <h2>Set task parameters</h2>
          <p className="scope-hint">These parameters define new task scope. Sync papers uses archive coverage; later steps use each paper&apos;s stored publish date. Narrow the table separately with its own filters.</p>
        </div>

        <div className="scope-controls">
          <label className="form-field categories-field">
            <span className="field-label">Categories</span>
            <div className={categoriesShowGhostHint ? 'input-ghost-shell hint-visible wide' : 'input-ghost-shell wide'}>
              <input
                className={categoriesFieldInvalid ? 'ghost-placeholder-input input-invalid' : 'ghost-placeholder-input'}
                value={scope.categories}
                onChange={(event) => setScope((current) => ({ ...current, categories: event.target.value }))}
                placeholder={CATEGORIES_HINT}
                aria-invalid={categoriesFieldInvalid ? true : undefined}
              />
              <InputGhostHint text={CATEGORIES_HINT} visible={categoriesShowGhostHint} wide />
            </div>
          </label>

          <div className="time-mode-field">
            <span className="field-label">Time mode</span>
            <div className="segmented-control" role="tablist" aria-label="Time mode">
              {(['day', 'month', 'range'] as TimeMode[]).map((mode) => (
                <button
                  key={mode}
                  type="button"
                  className={scope.timeMode === mode ? 'segment-button active' : 'segment-button'}
                  onClick={() => setScope((current) => ({ ...current, timeMode: mode }))}
                >
                  {mode === 'day' ? 'Day' : mode === 'month' ? 'Month' : 'Range'}
                </button>
              ))}
            </div>
          </div>

          <div className={scope.timeMode === 'range' ? 'time-inputs time-inputs-range' : 'time-inputs'}>
            {scope.timeMode === 'day' ? (
              <MaskedDateField
                label="Date"
                value={scope.day}
                invalid={dayFieldInvalid}
                showGhostHint={dayShowGhostHint}
                onChange={(value) => setScope((current) => ({ ...current, day: value }))}
              />
            ) : scope.timeMode === 'month' ? (
              <MaskedMonthField
                value={scope.month}
                invalid={monthFieldInvalid}
                showGhostHint={monthShowGhostHint}
                onChange={(value) => setScope((current) => ({ ...current, month: value }))}
              />
            ) : (
              <div className="range-fields">
                <MaskedDateField
                  label="From"
                  value={scope.from}
                  invalid={rangeFromFieldInvalid}
                  showGhostHint={rangeFromShowGhostHint}
                  ghostHintText={rangeGhostHintText}
                  onChange={(value) => setScope((current) => ({ ...current, from: value }))}
                />

                <MaskedDateField
                  label="To"
                  value={scope.to}
                  invalid={rangeToFieldInvalid}
                  showGhostHint={rangeToShowGhostHint}
                  ghostHintText={rangeGhostHintText}
                  onChange={(value) => setScope((current) => ({ ...current, to: value }))}
                />
              </div>
            )}
          </div>
        </div>
      </section>

      <section className="pipeline-bar panel">
        <div className="section-copy">
          <p className="panel-kicker">Pipeline</p>
          <h2>Run the next step</h2>
        </div>

        <div className="pipeline-grid">
          <StepCard
            index={1}
            title="Sync papers"
            detail={syncPapersForce ? 'Pull arXiv archive results and ignore TTL for this run.' : 'Pull arXiv archive results into the database.'}
            running={launchingJob === 'sync-papers'}
            disabled={liveScope.error !== null || launchingJob !== null}
            disabledReason={runDisabledReason('sync-papers', 'Sync papers')}
            onRun={() => launchJob('sync-papers')}
            config={
              <ForceChip checked={syncPapersForce} label="Force paper refresh" onChange={setSyncPapersForce} />
            }
          />

          <StepCard
            index={2}
            title="Find repos"
            detail={findReposForce ? 'Resolve repos in the selected publish-date scope and ignore TTL for this run.' : 'Resolve GitHub repos in the selected publish-date scope.'}
            running={launchingJob === 'find-repos'}
            disabled={liveScope.error !== null || launchingJob !== null}
            disabledReason={runDisabledReason('find-repos', 'Find repos')}
            onRun={() => launchJob('find-repos')}
            config={
              <ForceChip checked={findReposForce} label="Force link refresh" onChange={setFindReposForce} />
            }
          />

          <StepCard
            index={3}
            title="Refresh metadata"
            detail="Refresh stars and fast-changing repo metadata in the selected publish-date scope."
            running={launchingJob === 'refresh-metadata'}
            disabled={liveScope.error !== null || launchingJob !== null}
            disabledReason={runDisabledReason('refresh-metadata', 'Refresh metadata')}
            onRun={() => launchJob('refresh-metadata')}
          />
        </div>

        <div className="feedback-strip">
          <QueueSummaryCard
            summary={dashboard?.job_queue_summary ?? null}
            launchingJob={launchingJob}
            launchFeedback={launchFeedback}
          />
          {error ? <div className="error-box">{error}</div> : null}
        </div>
      </section>

      <section className="sheet-panel panel">
        <div ref={sheetFrameRef} className="sheet-frame">
          {activeGrid}
        </div>
      </section>

      {drawerOpen ? (
        <aside ref={drawerPanelRef} className="drawer-panel" role="complementary" aria-label={`${previewTab} details`}>
          {renderDrawerContent()}
        </aside>
      ) : null}
    </main>
  )
}

export default App
