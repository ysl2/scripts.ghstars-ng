import { useEffect, useMemo, useRef, useState } from 'react'
import { AgGridReact } from 'ag-grid-react'
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridApi,
  type GridReadyEvent,
  type RowClickedEvent,
  type RowClassParams,
  type GridState,
} from 'ag-grid-community'
import 'ag-grid-community/styles/ag-grid.css'
import 'ag-grid-community/styles/ag-theme-balham.css'
import SheetHeader from './SheetHeader'
import usePointerDownOutside from '../hooks/usePointerDownOutside'

ModuleRegistry.registerModules([AllCommunityModule])

type RowRecord = Record<string, unknown>

export type SheetColumn<TData extends RowRecord = RowRecord> = ColDef<TData> & {
  hideable?: boolean
}

type ColumnToggle = {
  colId: string
  title: string
  visible: boolean
}

type AgGridSheetProps<TData extends RowRecord = RowRecord> = {
  columns: SheetColumn<TData>[]
  rows: TData[]
  rowKey: keyof TData & string
  selectedKey: string | null
  onSelectedKeyChange: (key: string | null) => void
  onDisplayedKeysChange: (keys: string[]) => void
  quickSearch: string
  persistenceId: string
  emptyMessage: string
  toolbarSummary?: string
  loading?: boolean
  loadingLabel?: string
  progressCurrent?: number
  progressTotal?: number
  getRowClass?: (row: TData) => string | undefined
}

function storageKey(id: string) {
  return `ghstars:grid:v2:${id}`
}

function normalizeGridState(state: GridState | undefined): GridState | undefined {
  if (!state) return undefined
  const sortModel = state.sort?.sortModel ?? []
  if (sortModel.length <= 1) return state
  return {
    ...state,
    sort: {
      ...state.sort,
      sortModel: sortModel.slice(0, 1),
    },
  }
}

function loadGridState(id: string): GridState | undefined {
  if (typeof window === 'undefined') return undefined
  try {
    const raw = window.localStorage.getItem(storageKey(id))
    if (!raw) return undefined
    return normalizeGridState(JSON.parse(raw) as GridState)
  } catch {
    return undefined
  }
}

function persistGridState(id: string, state: GridState) {
  if (typeof window === 'undefined') return
  try {
    const normalized = normalizeGridState(state) ?? state
    const pruned = {
      version: normalized.version,
      columnOrder: normalized.columnOrder,
      columnPinning: normalized.columnPinning,
      columnSizing: normalized.columnSizing,
      columnVisibility: normalized.columnVisibility,
      filter: normalized.filter,
      sort: normalized.sort,
    }
    window.localStorage.setItem(storageKey(id), JSON.stringify(pruned))
  } catch {
    // Ignore storage failures. The grid still works without persistence.
  }
}

function clearGridState(id: string) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.removeItem(storageKey(id))
  } catch {
    // Ignore storage failures.
  }
}

function columnIdOf<TData extends RowRecord>(column: SheetColumn<TData>) {
  return column.colId ?? column.field ?? ''
}

function clampFreezeCount(value: string | number, visibleColumnCount: number) {
  const numeric =
    typeof value === 'number'
      ? value
      : value.trim().length > 0
        ? Number.parseInt(value, 10)
        : 0
  if (Number.isNaN(numeric)) return 0
  return Math.max(0, Math.min(visibleColumnCount, Math.trunc(numeric)))
}

export default function AgGridSheet<TData extends RowRecord>({
  columns,
  rows,
  rowKey,
  selectedKey,
  onSelectedKeyChange,
  onDisplayedKeysChange,
  quickSearch,
  persistenceId,
  emptyMessage,
  toolbarSummary,
  loading = false,
  loadingLabel = 'Loading rows…',
  progressCurrent,
  progressTotal,
  getRowClass,
}: AgGridSheetProps<TData>) {
  const apiRef = useRef<GridApi<TData> | null>(null)
  const columnMenuRef = useRef<HTMLDetailsElement | null>(null)
  const freezeMenuRef = useRef<HTMLDetailsElement | null>(null)
  const [columnToggles, setColumnToggles] = useState<ColumnToggle[]>([])
  const [visibleColumnCount, setVisibleColumnCount] = useState(0)
  const [frozenCount, setFrozenCount] = useState(0)
  const [freezeInput, setFreezeInput] = useState('0')

  const initialState = useMemo(() => loadGridState(persistenceId), [persistenceId])
  const columnIds = useMemo(() => new Set(columns.map((column) => columnIdOf(column)).filter(Boolean)), [columns])
  const blockingLoad = loading && rows.length === 0
  const normalizedProgressCurrent = Math.max(0, progressCurrent ?? 0)
  const normalizedProgressTotal =
    typeof progressTotal === 'number' && progressTotal > 0 ? progressTotal : undefined
  const determinateProgress =
    normalizedProgressTotal !== undefined
      ? Math.max(0, Math.min(100, (Math.min(normalizedProgressCurrent, normalizedProgressTotal) / normalizedProgressTotal) * 100))
      : undefined
  const loadingSummary =
    normalizedProgressTotal !== undefined
      ? `${loadingLabel} ${Math.min(normalizedProgressCurrent, normalizedProgressTotal).toLocaleString()} / ${normalizedProgressTotal.toLocaleString()}`
      : normalizedProgressCurrent > 0
        ? `${loadingLabel} ${normalizedProgressCurrent.toLocaleString()} loaded`
        : loadingLabel

  usePointerDownOutside(columnMenuRef, () => {
    if (columnMenuRef.current?.open) {
      columnMenuRef.current.open = false
    }
  })

  usePointerDownOutside(freezeMenuRef, () => {
    if (freezeMenuRef.current?.open) {
      freezeMenuRef.current.open = false
      setFreezeInput(String(frozenCount))
    }
  })

  const defaultColDef = useMemo<ColDef<TData>>(
    () => ({
      sortable: true,
      resizable: true,
      filter: 'agTextColumnFilter',
      filterParams: {
        buttons: ['reset', 'cancel'],
        closeOnApply: true,
      },
      headerComponent: SheetHeader,
      minWidth: 140,
    }),
    [],
  )

  function syncDisplayedKeys(api: GridApi<TData>) {
    const keys: string[] = []
    api.forEachNodeAfterFilterAndSort((node) => {
      if (node.data == null) return
      const value = node.data[rowKey]
      if (value == null) return
      keys.push(String(value))
    })
    onDisplayedKeysChange(keys)
  }

  function syncColumnControls(api: GridApi<TData>) {
    const columnState = api.getColumnState().filter((item) => columnIds.has(item.colId))
    const stateById = new Map(columnState.map((item) => [item.colId, item]))
    setColumnToggles(
      columns
        .filter((column) => column.hideable !== false)
        .map((column) => {
          const colId = columnIdOf(column)
          const state = stateById.get(colId)
          return {
            colId,
            title: column.headerName ?? colId,
            visible: !state?.hide,
          }
        })
        .filter((column) => column.colId),
    )

    const visibleIds = columnState.filter((item) => !item.hide).map((item) => item.colId)
    let nextFrozenCount = 0
    for (const colId of visibleIds) {
      const state = stateById.get(colId)
      if (state?.pinned === 'left' || state?.pinned === true) {
        nextFrozenCount += 1
        continue
      }
      break
    }

    setVisibleColumnCount(visibleIds.length)
    setFrozenCount(nextFrozenCount)
    setFreezeInput(String(nextFrozenCount))
  }

  function handleGridReady(event: GridReadyEvent<TData>) {
    apiRef.current = event.api
    syncColumnControls(event.api)
    syncDisplayedKeys(event.api)
  }

  useEffect(() => {
    const api = apiRef.current
    if (!api) return
    api.setGridOption('quickFilterText', quickSearch.trim())
  }, [quickSearch])

  useEffect(() => {
    const api = apiRef.current
    if (!api) return

    if (!selectedKey) {
      api.deselectAll()
      return
    }

    const node = api.getRowNode(selectedKey)
    if (!node) {
      api.deselectAll()
      return
    }

    if (!node.isSelected()) {
      node.setSelected(true, true)
    }
  }, [rows, selectedKey])

  function toggleColumn(colId: string) {
    const api = apiRef.current
    if (!api) return
    const state = api.getColumnState().find((item) => item.colId === colId)
    api.setColumnsVisible([colId], state?.hide ?? true)
    syncColumnControls(api)
  }

  function applyFreezeCount(rawValue: string | number) {
    const api = apiRef.current
    if (!api) return

    const columnState = api.getColumnState().filter((item) => columnIds.has(item.colId))
    const visibleIds = columnState.filter((item) => !item.hide).map((item) => item.colId)
    const nextFreezeCount = clampFreezeCount(rawValue, visibleIds.length)
    const frozenIds = new Set(visibleIds.slice(0, nextFreezeCount))

    api.applyColumnState({
      state: columnState.map((item) => ({
        colId: item.colId,
        pinned: frozenIds.has(item.colId) ? 'left' : null,
      })),
      defaultState: {
        pinned: null,
      },
    })

    setFreezeInput(String(nextFreezeCount))
    if (freezeMenuRef.current) {
      freezeMenuRef.current.open = false
    }

    syncColumnControls(api)
  }

  function resetView() {
    const api = apiRef.current
    if (!api) return
    clearGridState(persistenceId)
    api.setFilterModel(null)
    api.resetColumnState()
    api.setGridOption('quickFilterText', quickSearch.trim())
    syncColumnControls(api)
    syncDisplayedKeys(api)
  }

  return (
    <div className="sheet-grid-shell">
      <div className="sheet-grid-toolbar">
        <div className="sheet-grid-toolbar-group">
          <details ref={columnMenuRef} className="column-picker">
            <summary>Columns</summary>
            <div className="column-picker-menu">
              {columnToggles.map((column) => (
                <label key={column.colId} className="column-picker-item">
                  <input type="checkbox" checked={column.visible} onChange={() => toggleColumn(column.colId)} />
                  <span>{column.title}</span>
                </label>
              ))}
            </div>
          </details>

          <details
            ref={freezeMenuRef}
            className="column-picker freeze-picker"
            onToggle={(event) => {
              if (!event.currentTarget.open) {
                setFreezeInput(String(frozenCount))
              }
            }}
          >
            <summary>Freeze</summary>
            <div className="column-picker-menu freeze-picker-menu">
              <div className="freeze-picker-copy">
                <strong>{frozenCount > 0 ? `${frozenCount} columns frozen` : 'No frozen columns'}</strong>
                <span>Freeze the first N visible columns in the current left-to-right order.</span>
              </div>

              <form
                className="freeze-picker-form"
                onSubmit={(event) => {
                  event.preventDefault()
                  applyFreezeCount(freezeInput)
                }}
              >
                <label className="form-field freeze-count-field">
                  <span className="field-label">Freeze first</span>
                  <input
                    type="number"
                    min={0}
                    max={visibleColumnCount}
                    step={1}
                    value={freezeInput}
                    onChange={(event) => setFreezeInput(event.target.value)}
                    placeholder="0"
                  />
                </label>

                <button
                  type="button"
                  className="column-picker-action"
                  onClick={() => applyFreezeCount(0)}
                  disabled={frozenCount === 0}
                >
                  Unfreeze
                </button>

                <button type="submit" className="column-picker-action active">
                  Apply
                </button>
              </form>
            </div>
          </details>

          <div className="sheet-grid-status-slot" role="status" aria-live="polite">
            {loading ? (
              <>
                <span className="sheet-grid-status-label">{loadingSummary}</span>
                <div className="sheet-loading-meter toolbar" aria-hidden="true">
                  <span
                    className={determinateProgress === undefined ? 'sheet-loading-meter-bar indeterminate' : 'sheet-loading-meter-bar'}
                    style={determinateProgress === undefined ? undefined : { width: `${determinateProgress}%` }}
                  />
                </div>
              </>
            ) : null}
          </div>
        </div>

        <div className="sheet-grid-toolbar-actions">
          {toolbarSummary ? <span className="sheet-grid-toolbar-summary">{toolbarSummary}</span> : null}

          <button type="button" className="ghost-button" onClick={resetView}>
            Reset view
          </button>
        </div>
      </div>

      <div className={blockingLoad ? 'ag-theme-balham sheet-grid-host loading' : 'ag-theme-balham sheet-grid-host'}>
        {blockingLoad ? (
          <div className="sheet-grid-loading-overlay" role="status" aria-live="polite">
            <strong>{loadingLabel}</strong>
            <span>{loadingSummary}</span>
            <div className="sheet-loading-meter overlay" aria-hidden="true">
              <span
                className={determinateProgress === undefined ? 'sheet-loading-meter-bar indeterminate' : 'sheet-loading-meter-bar'}
                style={determinateProgress === undefined ? undefined : { width: `${determinateProgress}%` }}
              />
            </div>
          </div>
        ) : null}
        <AgGridReact<TData>
          rowData={rows}
          columnDefs={columns}
          defaultColDef={defaultColDef}
          initialState={initialState}
          suppressMultiSort
          quickFilterText={quickSearch.trim()}
          getRowId={(params) => String(params.data[rowKey] ?? '')}
          rowSelection="single"
          rowNumbers={{ width: 56, minWidth: 56, resizable: false }}
          headerHeight={42}
          rowHeight={40}
          animateRows={false}
          enableCellTextSelection
          ensureDomOrder
          overlayNoRowsTemplate={blockingLoad ? '<span class="sheet-empty"></span>' : `<span class="sheet-empty">${emptyMessage}</span>`}
          onGridReady={handleGridReady}
          getRowClass={(params: RowClassParams<TData>) => (params.data ? getRowClass?.(params.data) ?? '' : '')}
          onRowClicked={(event: RowClickedEvent<TData>) => {
            if (event.isEventHandlingSuppressed) return
            const value = event.data?.[rowKey]
            if (value == null) return
            const key = String(value)
            if (selectedKey === key) {
              event.api.deselectAll()
              onSelectedKeyChange(null)
              return
            }
            event.node.setSelected(true, true)
            onSelectedKeyChange(key)
          }}
          onSelectionChanged={(event) => {
            const row = event.api.getSelectedNodes()[0]
            const value = row?.data?.[rowKey]
            onSelectedKeyChange(value == null ? null : String(value))
          }}
          onFirstDataRendered={(event) => {
            syncColumnControls(event.api)
            syncDisplayedKeys(event.api)
          }}
          onModelUpdated={(event) => {
            syncDisplayedKeys(event.api)
          }}
          onStateUpdated={(event) => {
            persistGridState(persistenceId, event.state)
            syncColumnControls(event.api)
          }}
        />
      </div>
    </div>
  )
}
