/* eslint-disable react-refresh/only-export-components */

import { useMemo, useRef, useState, type HTMLAttributes, type KeyboardEvent as ReactKeyboardEvent, type RefObject } from 'react'
import type {
  ColumnFilter,
  DateFilterModel,
  DoesFilterPassParams,
  ICombinedSimpleModel,
  ISimpleFilterModelType,
  JoinOperator,
  NumberFilterModel,
  SetFilterModel,
  TextFilterModel,
} from 'ag-grid-community'
import type { CustomFilterDisplayProps } from 'ag-grid-react'
import { useGridFilterDisplay } from 'ag-grid-react'

type RowRecord = Record<string, unknown>
type ConditionFilterKind = 'text' | 'number' | 'date'

type ConditionModel = TextFilterModel | NumberFilterModel | DateFilterModel
type CombinedConditionModel =
  | ICombinedSimpleModel<TextFilterModel>
  | ICombinedSimpleModel<NumberFilterModel>
  | ICombinedSimpleModel<DateFilterModel>
type CompactConditionFilterModel = ConditionModel | CombinedConditionModel

type ConditionDraft = {
  type: ISimpleFilterModelType
  filter: string
  filterTo: string
}

type ConditionUiState = {
  operator: JoinOperator
  conditions: [ConditionDraft, ConditionDraft]
}

type CompactConditionFilterParams = {
  filterKind: ConditionFilterKind
}

type CompactConditionFilterProps = CustomFilterDisplayProps<unknown, unknown, CompactConditionFilterModel> &
  CompactConditionFilterParams

export type CompactSetFilterParams = {
  filterKind: 'set'
  extractValues?: (row: RowRecord, fallbackValue: unknown) => string[]
  searchPlaceholder?: string
}

type CompactSetFilterProps = CustomFilterDisplayProps<unknown, unknown, SetFilterModel> & CompactSetFilterParams

type OperatorMeta = {
  value: ISimpleFilterModelType
  label: string
  inputs: 0 | 1 | 2
  firstPlaceholder?: string
  secondPlaceholder?: string
}

const TEXT_OPERATORS: readonly OperatorMeta[] = [
  { value: 'contains', label: 'Contains', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'notContains', label: 'Not contains', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'equals', label: 'Equals', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'notEqual', label: 'Not equal', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'startsWith', label: 'Starts with', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'endsWith', label: 'Ends with', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'blank', label: 'Is blank', inputs: 0 },
  { value: 'notBlank', label: 'Has value', inputs: 0 },
] as const

const NUMBER_OPERATORS: readonly OperatorMeta[] = [
  { value: 'equals', label: 'Equals', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'notEqual', label: 'Not equal', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'lessThan', label: 'Less than', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'lessThanOrEqual', label: '<= Max', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'greaterThan', label: 'Greater than', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'greaterThanOrEqual', label: '>= Min', inputs: 1, firstPlaceholder: 'Value' },
  { value: 'inRange', label: 'Between', inputs: 2, firstPlaceholder: 'From', secondPlaceholder: 'To' },
  { value: 'blank', label: 'Is blank', inputs: 0 },
  { value: 'notBlank', label: 'Has value', inputs: 0 },
] as const

const DATE_OPERATORS: readonly OperatorMeta[] = [
  { value: 'equals', label: 'On', inputs: 1, firstPlaceholder: 'YYYY-MM-DD' },
  { value: 'lessThan', label: 'Before', inputs: 1, firstPlaceholder: 'YYYY-MM-DD' },
  { value: 'greaterThan', label: 'After', inputs: 1, firstPlaceholder: 'YYYY-MM-DD' },
  { value: 'inRange', label: 'Between', inputs: 2, firstPlaceholder: 'From', secondPlaceholder: 'To' },
  { value: 'blank', label: 'Is blank', inputs: 0 },
  { value: 'notBlank', label: 'Has value', inputs: 0 },
] as const

function digitsOnly(value: string | null | undefined) {
  return typeof value === 'string' ? value.replace(/\D/g, '') : ''
}

function formatDateDraft(value: string | null | undefined) {
  const digits = digitsOnly(value).slice(0, 8)
  if (digits.length <= 4) return digits
  if (digits.length <= 6) return `${digits.slice(0, 4)}-${digits.slice(4)}`
  return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6)}`
}

function operatorList(filterKind: ConditionFilterKind) {
  if (filterKind === 'number') return NUMBER_OPERATORS
  if (filterKind === 'date') return DATE_OPERATORS
  return TEXT_OPERATORS
}

function defaultOperator(filterKind: ConditionFilterKind): ISimpleFilterModelType {
  if (filterKind === 'number' || filterKind === 'date') return 'equals'
  return 'contains'
}

function createEmptyCondition(filterKind: ConditionFilterKind): ConditionDraft {
  return {
    type: defaultOperator(filterKind),
    filter: '',
    filterTo: '',
  }
}

function createEmptyUiState(filterKind: ConditionFilterKind): ConditionUiState {
  return {
    operator: 'AND',
    conditions: [createEmptyCondition(filterKind), createEmptyCondition(filterKind)],
  }
}

function isCombinedConditionModel(
  model: CompactConditionFilterModel | null | undefined,
): model is
  | ICombinedSimpleModel<TextFilterModel>
  | ICombinedSimpleModel<NumberFilterModel>
  | ICombinedSimpleModel<DateFilterModel> {
  return Boolean(model && 'conditions' in model && Array.isArray(model.conditions))
}

function isAllowedOperator(filterKind: ConditionFilterKind, value: ISimpleFilterModelType | null | undefined) {
  return operatorList(filterKind).some((operator) => operator.value === value)
}

function getOperatorMeta(filterKind: ConditionFilterKind, value: ISimpleFilterModelType | null | undefined) {
  return operatorList(filterKind).find((operator) => operator.value === value) ?? operatorList(filterKind)[0]
}

function normalizeDateModelDraft(value: string | null | undefined) {
  if (typeof value !== 'string') return ''
  const trimmed = value.trim()
  if (!trimmed) return ''
  const match = trimmed.match(/\d{4}-\d{2}-\d{2}/)
  if (match) return match[0]
  return formatDateDraft(trimmed)
}

function normalizeConditionDraft(
  filterKind: ConditionFilterKind,
  condition: Partial<ConditionDraft> | TextFilterModel | NumberFilterModel | DateFilterModel | null | undefined,
): ConditionDraft {
  const type = isAllowedOperator(filterKind, condition?.type) ? condition?.type : defaultOperator(filterKind)
  if (filterKind === 'date') {
    const dateCondition = condition as
      | (Partial<ConditionDraft> & { dateFrom?: string | null; dateTo?: string | null })
      | null
      | undefined
    return {
      type: type ?? defaultOperator(filterKind),
      filter: normalizeDateModelDraft(dateCondition?.dateFrom ?? dateCondition?.filter),
      filterTo: normalizeDateModelDraft(dateCondition?.dateTo ?? dateCondition?.filterTo),
    }
  }
  const textLikeCondition = condition as Partial<ConditionDraft> | TextFilterModel | NumberFilterModel | null | undefined
  return {
    type: type ?? defaultOperator(filterKind),
    filter:
      filterKind === 'number'
        ? typeof textLikeCondition?.filter === 'number'
          ? String(textLikeCondition.filter)
          : typeof textLikeCondition?.filter === 'string'
            ? textLikeCondition.filter
            : ''
        : typeof textLikeCondition?.filter === 'string'
          ? textLikeCondition.filter
          : '',
    filterTo:
      filterKind === 'number'
        ? typeof textLikeCondition?.filterTo === 'number'
          ? String(textLikeCondition.filterTo)
          : typeof textLikeCondition?.filterTo === 'string'
            ? textLikeCondition.filterTo
            : ''
        : typeof textLikeCondition?.filterTo === 'string'
          ? textLikeCondition.filterTo
          : '',
  }
}

function uiStateFromConditionModel(
  filterKind: ConditionFilterKind,
  model: CompactConditionFilterModel | null | undefined,
): ConditionUiState {
  if (!model) return createEmptyUiState(filterKind)

  if (isCombinedConditionModel(model)) {
    return {
      operator: model.operator === 'OR' ? 'OR' : 'AND',
      conditions: [
        normalizeConditionDraft(filterKind, model.conditions[0]),
        normalizeConditionDraft(filterKind, model.conditions[1]),
      ],
    }
  }

  return {
    operator: 'AND',
    conditions: [normalizeConditionDraft(filterKind, model), createEmptyCondition(filterKind)],
  }
}

function normalizeConditionUiState(
  filterKind: ConditionFilterKind,
  state: ConditionUiState | null | undefined,
): ConditionUiState {
  if (!state) return createEmptyUiState(filterKind)
  return {
    operator: state.operator === 'OR' ? 'OR' : 'AND',
    conditions: [
      normalizeConditionDraft(filterKind, state.conditions?.[0]),
      normalizeConditionDraft(filterKind, state.conditions?.[1]),
    ],
  }
}

function parseNumberDraft(value: string) {
  const trimmed = value.trim()
  if (!trimmed) return null
  const normalized = trimmed.replaceAll(',', '')
  const numeric = Number(normalized)
  return Number.isFinite(numeric) ? numeric : null
}

function isValidDateDraft(value: string) {
  return /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(value)
}

function isConditionActive(filterKind: ConditionFilterKind, draft: ConditionDraft) {
  const operator = getOperatorMeta(filterKind, draft.type)
  if (operator.inputs === 0) return true
  if (filterKind === 'number') {
    if (operator.inputs === 1) return parseNumberDraft(draft.filter) !== null
    return parseNumberDraft(draft.filter) !== null && parseNumberDraft(draft.filterTo) !== null
  }
  if (filterKind === 'date') {
    if (operator.inputs === 1) return isValidDateDraft(draft.filter)
    return isValidDateDraft(draft.filter) && isValidDateDraft(draft.filterTo)
  }
  if (operator.inputs === 1) return draft.filter.length > 0
  return draft.filter.length > 0 && draft.filterTo.length > 0
}

function hasDraftContent(filterKind: ConditionFilterKind, draft: ConditionDraft) {
  const operator = getOperatorMeta(filterKind, draft.type)
  if (operator.inputs === 0) return true
  return draft.filter.length > 0 || draft.filterTo.length > 0
}

function buildConditionModel(filterKind: ConditionFilterKind, draft: ConditionDraft): ConditionModel | null {
  const operator = getOperatorMeta(filterKind, draft.type)
  if (operator.inputs === 0) {
    if (filterKind === 'number') return { filterType: 'number', type: draft.type }
    if (filterKind === 'date') return { filterType: 'date', type: draft.type, dateFrom: null, dateTo: null }
    return { filterType: 'text', type: draft.type }
  }

  if (filterKind === 'number') {
    const filter = parseNumberDraft(draft.filter)
    const filterTo = operator.inputs === 2 ? parseNumberDraft(draft.filterTo) : null
    if (operator.inputs === 1 && filter === null) return null
    if (operator.inputs === 2 && (filter === null || filterTo === null)) return null
    return {
      filterType: 'number',
      type: draft.type,
      filter,
      filterTo: operator.inputs === 2 ? filterTo : null,
    }
  }

  if (filterKind === 'date') {
    const filter = isValidDateDraft(draft.filter) ? draft.filter : null
    const filterTo = operator.inputs === 2 && isValidDateDraft(draft.filterTo) ? draft.filterTo : null
    if (operator.inputs === 1 && filter === null) return null
    if (operator.inputs === 2 && (filter === null || filterTo === null)) return null
    return {
      filterType: 'date',
      type: draft.type,
      dateFrom: filter,
      dateTo: operator.inputs === 2 ? filterTo : null,
    }
  }

  if (operator.inputs === 1 && draft.filter.length === 0) return null
  if (operator.inputs === 2 && (draft.filter.length === 0 || draft.filterTo.length === 0)) return null
  return {
    filterType: 'text',
    type: draft.type,
    filter: draft.filter,
    filterTo: operator.inputs === 2 ? draft.filterTo : null,
  }
}

function buildConditionFilterModel(filterKind: ConditionFilterKind, state: ConditionUiState): CompactConditionFilterModel | null {
  const first = buildConditionModel(filterKind, state.conditions[0])
  const second = isConditionActive(filterKind, state.conditions[0]) ? buildConditionModel(filterKind, state.conditions[1]) : null

  if (first && second) {
    if (filterKind === 'number') {
      return {
        filterType: 'number',
        operator: state.operator,
        conditions: [first as NumberFilterModel, second as NumberFilterModel],
      }
    }
    if (filterKind === 'date') {
      return {
        filterType: 'date',
        operator: state.operator,
        conditions: [first as DateFilterModel, second as DateFilterModel],
      }
    }
    return {
      filterType: 'text',
      operator: state.operator,
      conditions: [first as TextFilterModel, second as TextFilterModel],
    }
  }

  return first ?? null
}

function normalizeTextValue(value: unknown) {
  return typeof value === 'string' ? value.trim().toLowerCase() : value == null ? '' : String(value).trim().toLowerCase()
}

function isBlankValue(value: unknown) {
  if (value == null) return true
  if (typeof value === 'string') return value.trim().length === 0
  if (Array.isArray(value)) return value.length === 0
  return false
}

function evaluateTextCondition(model: TextFilterModel, rawValue: unknown) {
  const operator = model.type
  if (!operator) return true
  if (operator === 'blank') return isBlankValue(rawValue)
  if (operator === 'notBlank') return !isBlankValue(rawValue)

  const cellValue = normalizeTextValue(rawValue)
  const filterValue = normalizeTextValue(model.filter)
  if (!filterValue) return false

  switch (operator) {
    case 'contains':
      return cellValue.includes(filterValue)
    case 'notContains':
      return !cellValue.includes(filterValue)
    case 'equals':
      return cellValue === filterValue
    case 'notEqual':
      return cellValue !== filterValue
    case 'startsWith':
      return cellValue.startsWith(filterValue)
    case 'endsWith':
      return cellValue.endsWith(filterValue)
    default:
      return true
  }
}

function evaluateNumberCondition(model: NumberFilterModel, rawValue: unknown) {
  const operator = model.type
  if (!operator) return true
  if (operator === 'blank') return isBlankValue(rawValue)
  if (operator === 'notBlank') return !isBlankValue(rawValue)

  const cellValue =
    typeof rawValue === 'number'
      ? rawValue
      : typeof rawValue === 'string'
        ? parseNumberDraft(rawValue)
        : null
  if (cellValue === null) return false

  const filter = typeof model.filter === 'number' ? model.filter : null
  const filterTo = typeof model.filterTo === 'number' ? model.filterTo : null
  switch (operator) {
    case 'equals':
      return filter !== null && cellValue === filter
    case 'notEqual':
      return filter !== null && cellValue !== filter
    case 'lessThan':
      return filter !== null && cellValue < filter
    case 'lessThanOrEqual':
      return filter !== null && cellValue <= filter
    case 'greaterThan':
      return filter !== null && cellValue > filter
    case 'greaterThanOrEqual':
      return filter !== null && cellValue >= filter
    case 'inRange':
      return filter !== null && filterTo !== null && cellValue >= filter && cellValue <= filterTo
    default:
      return true
  }
}

function toLocalDateKey(date: Date) {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function normalizeComparableDate(value: unknown): string | null {
  if (value == null) return null
  if (value instanceof Date && !Number.isNaN(value.getTime())) return toLocalDateKey(value)
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return null
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed
    const parsed = new Date(trimmed)
    if (!Number.isNaN(parsed.getTime())) return toLocalDateKey(parsed)
    return null
  }
  return null
}

function evaluateDateCondition(model: DateFilterModel, rawValue: unknown) {
  const operator = model.type
  if (!operator) return true
  if (operator === 'blank') return normalizeComparableDate(rawValue) === null
  if (operator === 'notBlank') return normalizeComparableDate(rawValue) !== null

  const cellValue = normalizeComparableDate(rawValue)
  if (!cellValue) return false

  const dateFrom = normalizeDateModelDraft(model.dateFrom)
  const dateTo = normalizeDateModelDraft(model.dateTo)
  switch (operator) {
    case 'equals':
      return Boolean(dateFrom) && cellValue === dateFrom
    case 'lessThan':
      return Boolean(dateFrom) && cellValue < dateFrom
    case 'greaterThan':
      return Boolean(dateFrom) && cellValue > dateFrom
    case 'inRange':
      return Boolean(dateFrom) && Boolean(dateTo) && cellValue >= dateFrom && cellValue <= dateTo
    default:
      return true
  }
}

function evaluateConditionModel(filterKind: ConditionFilterKind, model: CompactConditionFilterModel | null, rawValue: unknown) {
  if (!model) return true

  const evaluateSingle = (condition: ConditionModel) => {
    if (filterKind === 'number') return evaluateNumberCondition(condition as NumberFilterModel, rawValue)
    if (filterKind === 'date') return evaluateDateCondition(condition as DateFilterModel, rawValue)
    return evaluateTextCondition(condition as TextFilterModel, rawValue)
  }

  if (isCombinedConditionModel(model)) {
    const [first, second] = model.conditions
    if (!first || !second) return true
    return model.operator === 'OR'
      ? evaluateSingle(first as ConditionModel) || evaluateSingle(second as ConditionModel)
      : evaluateSingle(first as ConditionModel) && evaluateSingle(second as ConditionModel)
  }

  return evaluateSingle(model as ConditionModel)
}

function stopGridKeyboardPropagation(event: ReactKeyboardEvent<HTMLElement>) {
  if (event.key === 'Escape') return
  event.stopPropagation()
}

function formatDraftInput(filterKind: ConditionFilterKind, value: string) {
  return filterKind === 'date' ? formatDateDraft(value) : value
}

function FilterValueInput({
  disabled = false,
  invalid = false,
  inputMode,
  placeholder,
  value,
  onChange,
  inputRef,
}: {
  disabled?: boolean
  invalid?: boolean
  inputMode?: HTMLAttributes<HTMLInputElement>['inputMode']
  placeholder: string
  value: string
  onChange: (value: string) => void
  inputRef?: RefObject<HTMLInputElement | null>
}) {
  return (
    <input
      ref={inputRef}
      type="text"
      value={value}
      disabled={disabled}
      inputMode={inputMode}
      spellCheck={false}
      autoComplete="off"
      placeholder={placeholder}
      aria-invalid={invalid ? true : undefined}
      className={invalid ? 'compact-filter-input invalid' : 'compact-filter-input'}
      onKeyDown={stopGridKeyboardPropagation}
      onChange={(event) => onChange(event.target.value)}
    />
  )
}

function FilterValueGroup({
  filterKind,
  draft,
  disabled = false,
  onChange,
  inputRef,
}: {
  filterKind: ConditionFilterKind
  draft: ConditionDraft
  disabled?: boolean
  onChange: (patch: Partial<ConditionDraft>) => void
  inputRef?: RefObject<HTMLInputElement | null>
}) {
  const operator = getOperatorMeta(filterKind, draft.type)
  const firstInvalid =
    filterKind === 'number'
      ? draft.filter.length > 0 && parseNumberDraft(draft.filter) === null
      : filterKind === 'date'
        ? draft.filter.length > 0 && !isValidDateDraft(draft.filter)
        : false
  const secondInvalid =
    filterKind === 'number'
      ? draft.filterTo.length > 0 && parseNumberDraft(draft.filterTo) === null
      : filterKind === 'date'
        ? draft.filterTo.length > 0 && !isValidDateDraft(draft.filterTo)
        : false

  if (operator.inputs === 0) {
    return (
      <div className="compact-filter-zero-input">
        <span>No value needed</span>
      </div>
    )
  }

  if (operator.inputs === 2) {
    return (
      <div className="compact-filter-range">
        <FilterValueInput
          disabled={disabled}
          invalid={firstInvalid}
          inputMode={filterKind === 'number' ? 'decimal' : filterKind === 'date' ? 'numeric' : 'text'}
          placeholder={operator.firstPlaceholder ?? 'From'}
          value={draft.filter}
          inputRef={inputRef}
          onChange={(value) => onChange({ filter: formatDraftInput(filterKind, value) })}
        />
        <FilterValueInput
          disabled={disabled}
          invalid={secondInvalid}
          inputMode={filterKind === 'number' ? 'decimal' : filterKind === 'date' ? 'numeric' : 'text'}
          placeholder={operator.secondPlaceholder ?? 'To'}
          value={draft.filterTo}
          onChange={(value) => onChange({ filterTo: formatDraftInput(filterKind, value) })}
        />
      </div>
    )
  }

  return (
    <FilterValueInput
      disabled={disabled}
      invalid={firstInvalid}
      inputMode={filterKind === 'number' ? 'decimal' : filterKind === 'date' ? 'numeric' : 'text'}
      placeholder={operator.firstPlaceholder ?? 'Value'}
      value={draft.filter}
      inputRef={inputRef}
      onChange={(value) => onChange({ filter: formatDraftInput(filterKind, value) })}
    />
  )
}

function CompactConditionColumnFilter(props: CompactConditionFilterProps) {
  const operatorRef = useRef<HTMLSelectElement | null>(null)
  const firstValueRef = useRef<HTMLInputElement | null>(null)
  const filterKind = props.filterKind
  const appliedModel = props.state.model ?? props.model
  const uiState = props.state.state as ConditionUiState | undefined
  const displayState = useMemo(
    () => (uiState ? normalizeConditionUiState(filterKind, uiState) : uiStateFromConditionModel(filterKind, appliedModel)),
    [appliedModel, filterKind, uiState],
  )
  const fallbackState = useMemo(() => uiStateFromConditionModel(filterKind, appliedModel), [appliedModel, filterKind])
  const firstConditionActive = isConditionActive(filterKind, displayState.conditions[0])
  const nextModel = buildConditionFilterModel(filterKind, displayState)
  const hasAnyDrafts =
    nextModel !== null || displayState.conditions.some((condition) => hasDraftContent(filterKind, condition))

  useGridFilterDisplay({
    afterGuiAttached() {
      if (getOperatorMeta(filterKind, displayState.conditions[0].type).inputs > 0) {
        firstValueRef.current?.focus()
        return
      }
      operatorRef.current?.focus()
    },
    afterGuiDetached() {
      props.onStateChange({
        model: props.model,
        state: fallbackState,
        valid: true,
      })
    },
  })

  function updateState(nextState: ConditionUiState) {
    const normalized = normalizeConditionUiState(filterKind, nextState)
    const model = buildConditionFilterModel(filterKind, normalized)
    props.onStateChange({
      model,
      state: normalized,
      valid: true,
    })
    props.onModelChange(model)
    props.onUiChange()
  }

  function updateCondition(index: 0 | 1, patch: Partial<ConditionDraft>) {
    const nextConditions: [ConditionDraft, ConditionDraft] = [...displayState.conditions] as [ConditionDraft, ConditionDraft]
    nextConditions[index] = {
      ...nextConditions[index],
      ...patch,
    }
    updateState({
      ...displayState,
      conditions: nextConditions,
    })
  }

  function updateOperator(index: 0 | 1, value: string) {
    const nextType = isAllowedOperator(filterKind, value as ISimpleFilterModelType)
      ? (value as ISimpleFilterModelType)
      : defaultOperator(filterKind)
    updateCondition(index, { type: nextType })
  }

  return (
    <div className="ghstars-filter-panel" onKeyDown={stopGridKeyboardPropagation}>
      <div className="ghstars-filter-rows">
        <div className="ghstars-filter-row">
          <select
            ref={operatorRef}
            value={displayState.conditions[0].type}
            className="compact-filter-select"
            onChange={(event) => updateOperator(0, event.target.value)}
          >
            {operatorList(filterKind).map((operator) => (
              <option key={operator.value} value={operator.value}>
                {operator.label}
              </option>
            ))}
          </select>

          <FilterValueGroup
            filterKind={filterKind}
            draft={displayState.conditions[0]}
            onChange={(patch) => updateCondition(0, patch)}
            inputRef={firstValueRef}
          />
        </div>

        <div className={firstConditionActive ? 'ghstars-filter-join-row' : 'ghstars-filter-join-row disabled'}>
          <div className="ghstars-filter-join" role="group" aria-label="Combine conditions">
            <button
              type="button"
              disabled={!firstConditionActive}
              className={displayState.operator === 'AND' ? 'compact-filter-join-button active' : 'compact-filter-join-button'}
              onClick={() => updateState({ ...displayState, operator: 'AND' })}
            >
              AND
            </button>
            <button
              type="button"
              disabled={!firstConditionActive}
              className={displayState.operator === 'OR' ? 'compact-filter-join-button active' : 'compact-filter-join-button'}
              onClick={() => updateState({ ...displayState, operator: 'OR' })}
            >
              OR
            </button>
          </div>
        </div>

        <div className={firstConditionActive ? 'ghstars-filter-row secondary' : 'ghstars-filter-row secondary disabled'}>
          <select
            value={displayState.conditions[1].type}
            disabled={!firstConditionActive}
            className="compact-filter-select"
            onChange={(event) => updateOperator(1, event.target.value)}
          >
            {operatorList(filterKind).map((operator) => (
              <option key={operator.value} value={operator.value}>
                {operator.label}
              </option>
            ))}
          </select>

          <FilterValueGroup
            filterKind={filterKind}
            draft={displayState.conditions[1]}
            disabled={!firstConditionActive}
            onChange={(patch) => updateCondition(1, patch)}
          />
        </div>
      </div>

      <div className="ghstars-filter-footer">
        <button
          type="button"
          className="compact-filter-reset"
          disabled={!hasAnyDrafts}
          onClick={() => {
            const cleared = createEmptyUiState(filterKind)
            props.onStateChange({
              model: null,
              state: cleared,
              valid: true,
            })
            props.onModelChange(null)
            props.onUiChange()
          }}
        >
          Reset
        </button>
      </div>
    </div>
  )
}

function normalizeSetValues(model: SetFilterModel | null | undefined) {
  if (!model) return null
  return Array.from(
    new Set(
      model.values
        .map((value) => (typeof value === 'string' ? value.trim() : ''))
        .filter((value) => value.length > 0),
    ),
  )
}

function extractSetValues(params: CompactSetFilterParams, row: RowRecord | null | undefined, fallbackValue: unknown) {
  if (!row) return []
  const extracted =
    typeof params.extractValues === 'function'
      ? params.extractValues(row, fallbackValue)
      : Array.isArray(fallbackValue)
        ? fallbackValue.map((item) => String(item ?? '').trim()).filter(Boolean)
        : fallbackValue == null
          ? []
          : [String(fallbackValue).trim()].filter(Boolean)
  return Array.from(new Set(extracted.filter((value) => value.length > 0)))
}

function collectSetFilterOptions(props: CompactSetFilterProps, selectedValues: string[]) {
  const values = new Set<string>()
  props.api.forEachLeafNode((node) => {
    if (!node.data) return
    if (!props.doesRowPassOtherFilter(node)) return
    const fallbackValue = props.getValue(node)
    for (const value of extractSetValues(props, node.data as RowRecord, fallbackValue)) {
      values.add(value)
    }
  })
  for (const value of selectedValues) values.add(value)
  return Array.from(values).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }))
}

function CompactSetColumnFilter(props: CompactSetFilterProps) {
  const searchRef = useRef<HTMLInputElement | null>(null)
  const [miniFilter, setMiniFilter] = useState('')
  const selectedValues = useMemo(
    () => normalizeSetValues(props.state.model ?? props.model) ?? [],
    [props.model, props.state.model],
  )
  const selectedValueSet = useMemo(() => new Set(selectedValues), [selectedValues])
  const options = useMemo(() => collectSetFilterOptions(props, selectedValues), [props, selectedValues])
  const visibleOptions = useMemo(() => {
    const query = miniFilter.trim().toLowerCase()
    if (!query) return options
    return options.filter((option) => option.toLowerCase().includes(query))
  }, [miniFilter, options])
  const allSelected = options.length > 0 && options.every((option) => selectedValueSet.has(option))
  const noneSelected = selectedValues.length === 0 && props.model !== null

  useGridFilterDisplay({
    afterGuiAttached() {
      searchRef.current?.focus()
    },
    afterGuiDetached() {
      setMiniFilter('')
    },
  })

  function applyValues(nextValues: string[]) {
    const normalized = Array.from(new Set(nextValues)).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }))
    const nextModel =
      normalized.length === options.length && options.every((option) => normalized.includes(option))
        ? null
        : {
            filterType: 'set' as const,
            values: normalized,
          }
    props.onStateChange({
      model: nextModel,
      valid: true,
    })
    props.onModelChange(nextModel)
    props.onUiChange()
  }

  function toggleOption(option: string) {
    if (selectedValues.length === 0 && props.model === null) {
      const nextValues = options.filter((value) => value !== option)
      applyValues(nextValues)
      return
    }

    const nextSelected = new Set(selectedValueSet)
    if (nextSelected.has(option)) nextSelected.delete(option)
    else nextSelected.add(option)
    applyValues(Array.from(nextSelected))
  }

  return (
    <div className="ghstars-filter-panel" onKeyDown={stopGridKeyboardPropagation}>
      <div className="compact-set-filter-search-shell">
        <input
          ref={searchRef}
          type="text"
          value={miniFilter}
          spellCheck={false}
          autoComplete="off"
          className="compact-filter-input"
          placeholder={props.searchPlaceholder ?? 'Search values'}
          onKeyDown={stopGridKeyboardPropagation}
          onChange={(event) => setMiniFilter(event.target.value)}
        />
      </div>

      <div className="compact-set-filter-meta">
        <span>
          {props.model === null
            ? `${options.length.toLocaleString()} values available`
            : `${selectedValues.length.toLocaleString()} selected`}
        </span>
        <div className="compact-set-filter-actions">
          <button type="button" className="compact-set-filter-action" disabled={allSelected} onClick={() => applyValues(options)}>
            All
          </button>
          <button type="button" className="compact-set-filter-action" disabled={noneSelected} onClick={() => applyValues([])}>
            None
          </button>
        </div>
      </div>

      <div className="compact-set-filter-list" role="listbox" aria-multiselectable="true">
        {visibleOptions.length > 0 ? (
          visibleOptions.map((option) => {
            const checked = props.model === null ? true : selectedValueSet.has(option)
            return (
              <label key={option} className={checked ? 'compact-set-filter-option checked' : 'compact-set-filter-option'}>
                <input type="checkbox" checked={checked} onChange={() => toggleOption(option)} />
                <span title={option}>{option}</span>
              </label>
            )
          })
        ) : (
          <div className="compact-set-filter-empty">No matching values</div>
        )}
      </div>

      <div className="ghstars-filter-footer">
        <button
          type="button"
          className="compact-filter-reset"
          disabled={props.model === null}
          onClick={() => {
            props.onStateChange({ model: null, valid: true })
            props.onModelChange(null)
            props.onUiChange()
          }}
        >
          Reset
        </button>
      </div>
    </div>
  )
}

function createConditionDoesFilterPass(filterKind: ConditionFilterKind) {
  return (params: DoesFilterPassParams<unknown, unknown, CompactConditionFilterModel>) => {
    const rawValue = params.handlerParams.getValue(params.node)
    return evaluateConditionModel(filterKind, params.model ?? null, rawValue)
  }
}

function doesSetFilterPass(params: DoesFilterPassParams<unknown, unknown, SetFilterModel, CompactSetFilterParams>) {
  const selectedValues = normalizeSetValues(params.model)
  if (selectedValues === null) return true
  if (selectedValues.length === 0) return false
  const selectedValueSet = new Set(selectedValues)
  const fallbackValue = params.handlerParams.getValue(params.node)
  const rowValues = extractSetValues(params.handlerParams.filterParams, params.data as RowRecord, fallbackValue)
  return rowValues.some((value) => selectedValueSet.has(value))
}

export function createCompactSetFilterParams(params: Omit<CompactSetFilterParams, 'filterKind'> = {}): CompactSetFilterParams {
  return {
    filterKind: 'set',
    ...params,
  }
}

export const compactTextColumnFilter: ColumnFilter = {
  component: CompactConditionColumnFilter,
  doesFilterPass: createConditionDoesFilterPass('text'),
}

export const compactNumberColumnFilter: ColumnFilter = {
  component: CompactConditionColumnFilter,
  doesFilterPass: createConditionDoesFilterPass('number'),
}

export const compactDateColumnFilter: ColumnFilter = {
  component: CompactConditionColumnFilter,
  doesFilterPass: createConditionDoesFilterPass('date'),
}

export const compactValueColumnFilter: ColumnFilter = {
  component: CompactSetColumnFilter,
  doesFilterPass: doesSetFilterPass,
}

export const compactTextFilterParams = {
  filterKind: 'text' as const,
}

export const compactNumberFilterParams = {
  filterKind: 'number' as const,
}

export const compactDateFilterParams = {
  filterKind: 'date' as const,
}
