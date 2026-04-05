# Uncommitted Changes Reimplementation Guide (2026-04-05)

This document captures all current uncommitted changes in this workspace so they can be recreated in a fresh clone.

## Scope

Changed files:

1. `dashboard/routers/stats.py`
2. `dashboard/static/main.js`
3. `dashboard/static/style.css`
4. `dashboard/templates/dashboard.html`
5. `dashboard/templates/index.html`
6. `main.py`

Working tree summary at capture time:

- 6 modified files
- 0 staged files
- 0 untracked files

---

## 1) Backend: `dashboard/routers/stats.py`

### High-level change

The stats router was rewritten from SQL/Mongo connectivity table-level reporting to a pipeline-oriented KPI API that supports the redesigned dashboard stats cards.

### Import and config changes

- Removed direct SQL inspector usage (`sqlalchemy.inspect`, `text`) and Mongo DB-name-based table/collection reporting.
- Added:
  - `json`
  - `datetime`, `timezone`
  - `typing.Any`
  - config symbols: `CHECKPOINT_FILE`, `TRANSACTION_LOG_FILE`

### New module-level constants

- `_DATA_DIR = Path(METADATA_FILE).resolve().parent`
- `_PIPELINE_CHECKPOINT_FILE = _DATA_DIR / "pipeline_checkpoint.json"`

### New helper functions

1. `_read_json(path, default)`
- Safe JSON reader.
- Returns `default` when file is missing, malformed, or not same container type as default.

2. `_safe_float(value, fallback=0.0)`
- Converts to float.
- Guards against invalid values and NaN.

3. `_safe_int(value, fallback=0)`
- Safe integer coercion.

4. `_to_iso_timestamp(raw_timestamp)`
- Accepts `None`, string, int, float.
- If numeric, converts to UTC ISO8601 and replaces `+00:00` with `Z`.
- If non-numeric string, returns original trimmed string.

5. `_field_status(field)`
- `pending` if `is_discovered_buffer` is truthy.
- `discovered` if `user_constraints` is `None`.
- else `defined`.

6. `_storage_type(field)`
- `pending` if buffer field.
- `structured` if `decision == "SQL"`.
- `flexible` if `decision == "MONGO"`.
- else `pending`.

7. `_load_metadata_fields()`
- Loads `METADATA_FILE` and returns sanitized `fields` list (dict-only entries).

8. `_build_active_fields(fields)`
- Computes counters:
  - `defined`
  - `discovered`
  - `pending`
- Builds sorted `details` rows with:
  - `field_name`
  - `status`
  - `frequency` (rounded to 4 decimals)
  - `density` (same value as frequency, rounded)
  - `storage_type`
- Returns:
  - `total` as `defined + discovered` (does not include pending)
  - `defined`, `discovered`, `pending`, `details`

9. `_compute_data_density(fields)`
- Uses only `defined` and `discovered` fields.
- Averages `frequency` values.
- Multiplies by 100 and rounds to 1 decimal.

10. `_compute_transaction_stats()`
- Reads `TRANSACTION_LOG_FILE` list.
- Counts states:
  - `committed`
  - `rolled_back`
  - `failed_needs_recovery` -> exposed as `failed`
- Returns:
  - `total`, `committed`, `rolled_back`, `failed`, `success_rate` (percentage, 1 decimal)

11. `_load_last_fetch()`
- Reads checkpoint in order:
  - `_PIPELINE_CHECKPOINT_FILE`
  - fallback `CHECKPOINT_FILE`
- Returns:
  - `timestamp` as normalized ISO or null
  - `count` as int (default 0)

12. `_get_total_records_from_sql(request)`
- If SQL engine exists and initialized, uses `sql_engine.get_table_count("main_records")`.
- Fallback to `METADATA_FILE["total_records"]`.

13. `_check_external_api_reachable()`
- Uses async `httpx` with timeout 1.5s.
- `GET API_HOST`.
- Returns `response.is_success` on success, else false.

### Endpoint changes

1. `GET /api/status`
- Kept shape, but now normalizes booleans more explicitly:
  - `pipeline_state`
  - `has_schema`
  - `has_metadata`
  - `pipeline_busy`

2. `GET /api/stats` (major contract change)
- Old SQL/Mongo payload removed.
- New response:
  - `status`: `"pipeline_busy"` or `"ok"`
  - `pipeline_busy`: bool
  - `total_records`: int
  - `external_api_reachable`: bool

3. Added `GET /api/pipeline/stats`
- Returns:
  - `total_records`
  - `active_fields` object (counts + details)
  - `data_density`
  - `pipeline_state`
  - `pipeline_busy`
  - `last_fetch` object (`timestamp`, `count`)
  - `transactions` object (`total`, `committed`, `rolled_back`, `failed`, `success_rate`)

---

## 2) Backend pipeline metadata: `main.py`

### `set_checkpoint` enhancement

- Signature changed from:
  - `set_checkpoint(step)`
- To:
  - `set_checkpoint(step, count=None)`

### New behavior

- Writes payload with mandatory fields:
  - `last_step`
  - `timestamp`
- Conditionally includes:
  - `count` (as int) when provided

### Call-site updates

In both `initialise(count=...)` and `fetch(count=...)`, changed:

- `set_checkpoint("sql")` -> `set_checkpoint("sql", count=count)`
- `set_checkpoint("mongo")` -> `set_checkpoint("mongo", count=count)`

Purpose: expose last fetch/init count to dashboard stats (`last_fetch.count`).

---

## 3) Frontend behavior: `dashboard/static/main.js`

### New global UI state for field-details table

Added globals near existing query state:

- `fieldDetailsRows = []`
- `fieldDetailsSortKey = "field_name"`
- `fieldDetailsSortDirection = "asc"`
- `fieldDetailsFilter = "all"`
- `fieldDetailsVisible = false`

### Reset dialog flow improvements

1. `showResetConfirmation()` hardened:
- Detects usable modal with `showModal` support and required elements.
- Falls back to `window.confirm` if modal is unavailable.
- Adds robust cleanup and single-settle protection:
  - `settled` flag
  - `finalize(result)`
  - handles `cancel` and `close` events
- Ensures listeners are removed and dialog closed safely.

2. New helper:
- `getResetEndpointUrl(wipeSchema)`
- Returns `/api/pipeline/reset?wipe_schema=true|false`.

3. Landing reset action (`attachLandingHandlers`):
- Uses `showResetConfirmation()` instead of plain confirm.
- Posts to URL from `getResetEndpointUrl`.
- Feedback text now differs by checkbox choice:
  - schema removed -> `fresh`
  - schema preserved -> `schema ready`

4. Dashboard reset action (`attachDashboardHandlers`):
- Same checkbox-aware reset URL behavior.
- Updated success messages for redirect flow.

### Dashboard control lock updates

`setDashboardControlsDisabled(disabled)` now also disables:

- `btn-toggle-field-details`
- `field-status-filter`

### Stats rendering rewrite (major)

Removed old SQL/Mongo list rendering approach (`renderStatsTables`, old `renderDashboardStats`).

Added helper functions:

- `formatInteger`
- `toPercentValue`
- `formatPercent`
- `titleCase`
- `formatRelativeTime`
- `getSystemStatusPresentation`
- `normalizeFieldDetailsRows`
- `updateFieldSortButtonState`
- `getVisibleFieldDetailsRows`
- `renderFieldDetailsTable`
- `renderSchemaDimensions`
- `renderDashboardStatsBundle`

### New dashboard data flow

`refreshDashboardStats()` now does `Promise.all` for:

1. `/api/status`
2. `/api/stats`
3. `/api/pipeline/stats`

Behavior:

- If `pipeline_state !== "initialized"`:
  - show error feedback
  - redirect to landing after ~1.2s
- If busy:
  - disable controls
  - show busy feedback
- Else:
  - clear feedback
- Render all KPI widgets from new bundle renderer.

### Field-details interactions

In `attachDashboardHandlers()`:

1. Schema details summary action text sync:
- `schema-summary-action` toggles between `Expand` and `Collapse` based on `<details open>`.

2. Field details panel toggle:
- Button `btn-toggle-field-details` shows/hides panel `field-details-panel`.
- Button text switches:
  - hidden: `View Field Details ->`
  - visible: `Hide Field Details`

3. Status filter:
- Select `field-status-filter` updates `fieldDetailsFilter` and rerenders table.

4. Sort buttons:
- All `.field-sort-btn` buttons toggle `asc/desc` if same key, otherwise switch key and reset to `asc`.
- Updates labels to include ` (asc)` or ` (desc)` on active key.

### Dashboard initialization updates

In `initializeDashboard()`:

- Grabs new elements:
  - `field-status-filter`
  - `btn-toggle-field-details`
  - `field-details-panel`
- Resets field-details state values.
- Forces default UI state:
  - filter set to `all`
  - button text `View Field Details ->`
  - panel hidden
  - sort button labels synced

---

## 4) Frontend styles: `dashboard/static/style.css`

### Added button style

- `.btn-secondary`
- `.btn-secondary:hover`

### Added modal styling block

- `.custom-modal`
- `.custom-modal::backdrop`
- `.modal-content`
- `.modal-content h2`
- `.modal-content p`
- `.modal-options`
- `.checkbox-container`
- `.checkbox-container input`
- `.checkmark`
- `.checkbox-container input:checked + .checkmark`
- `.checkbox-container input:checked + .checkmark::after`
- `.modal-actions`

### Added KPI layout and typography

- `.stats-kpi-grid`
- `.kpi-card`
- `.kpi-label`
- `.kpi-value`
- `.kpi-subtitle`
- `.kpi-status-value`
- `.info-tooltip`

### Added status tones

- `.status-dot.neutral`
- `.status-dot.warning`

### Added schema dimensions and field-details styles

- `.schema-dimensions-card`
- `.schema-dimensions-summary`
- `.schema-dimensions-summary::-webkit-details-marker`
- `.schema-summary-title`
- `.schema-summary-action`
- `.schema-dimensions-content`
- `.schema-breakdown-list`
- `.schema-breakdown-item`
- `.schema-breakdown-title`
- `.field-details-actions`
- `.field-details-panel`
- `.field-details-controls`
- `.field-sort-btn`
- `#field-details-table`
- `.field-details-panel .result-table-wrap`

### Added result badge variants

- `.result-badge.good`
- `.result-badge.warning`
- `.result-badge.busy`

### Responsive adjustments in media block

- Included `.stats-kpi-grid` in single-column mobile collapse.
- Left-align field-details action/control rows on small screens.
- Modal width override for mobile.
- Modal actions stack vertically.
- Modal action buttons full width.

---

## 5) Dashboard markup: `dashboard/templates/dashboard.html`

### Stats section redesign

1. Section title changed:
- `Stats Panel` -> `Stats`

2. Removed old logical entity block
- Removed prior list-based store summary using:
  - `system-status-text`
  - `system-main-total`
  - `system-table-list`

3. Added KPI card grid (`stats-kpi-grid`)
- Cards/IDs introduced:
  - `kpi-total-records`
  - `kpi-active-fields`
  - `kpi-active-fields-breakdown`
  - `kpi-data-density`
  - `kpi-system-dot`
  - `kpi-system-status`
  - `kpi-last-fetch`
  - `kpi-transactions`
  - `kpi-transactions-breakdown`

4. Added schema dimensions details card
- Root details: `schema-dimensions-details`
- Summary action label span: `schema-summary-action`
- Breakdown counters:
  - `schema-defined-count`
  - `schema-discovered-count`
  - `schema-pending-count`
- Field details controls:
  - toggle button `btn-toggle-field-details`
  - panel `field-details-panel` (initially hidden)
  - status filter select `field-status-filter`
  - table body `field-details-body`
  - sort buttons with `data-sort-key` for `field_name`, `status`, `frequency`, `density`

### Added reset dialog markup to dashboard page

Appended a custom dialog near end of body:

- `dialog#reset-dialog.custom-modal`
- Checkbox `#wipe-schema-check` with copy:
  - `Also delete the existing initial_schema.json`
- Buttons:
  - `#btn-reset-cancel` (`type="button"`, `btn btn-secondary`)
  - `#btn-reset-confirm` (`type="button"`, `btn btn-danger`)

---

## 6) Landing template tweak: `dashboard/templates/index.html`

Inside existing reset dialog:

1. Checkbox label copy changed:
- `Also delete the initial schema definition`
- to
- `Also delete the existing initial_schema.json`

2. Button type attributes added:
- cancel button now `type="button"`
- confirm button now `type="button"`

---

## Cross-file coupling to preserve

The following IDs/classes/API keys are tightly coupled and must match exactly:

1. Frontend IDs in `dashboard.html` <-> selectors in `main.js`
- `kpi-*`, `schema-*`, `field-*`, `btn-toggle-field-details`, `stats-refreshed-at`

2. API response contracts in `stats.py` <-> rendering in `main.js`
- `/api/stats`: `total_records`, `external_api_reachable`, `status`, `pipeline_busy`
- `/api/pipeline/stats`: `active_fields`, `data_density`, `last_fetch`, `transactions`

3. Reset modal element IDs in templates <-> `showResetConfirmation()`
- `reset-dialog`, `btn-reset-confirm`, `btn-reset-cancel`, `wipe-schema-check`

4. Checkpoint count in `main.py` <-> `stats.py` last fetch KPI
- `set_checkpoint(..., count=...)`
- `_load_last_fetch()` expecting `count` in checkpoint payload

---

## Suggested reimplementation order in fresh clone

1. `main.py` checkpoint payload/count support
2. `dashboard/routers/stats.py` API refactor + new endpoint
3. `dashboard/templates/dashboard.html` new stats markup + dialog
4. `dashboard/templates/index.html` small reset modal text/type tweaks
5. `dashboard/static/style.css` new classes for cards/modal/schema panel
6. `dashboard/static/main.js` state/helpers/rendering/event wiring

---

## Quick verification checklist

1. Landing reset opens custom dialog and passes `wipe_schema` query param.
2. Dashboard reset uses same custom dialog and redirects after completion.
3. Dashboard stats render KPI cards (not old SQL/Mongo table list).
4. Schema dimensions expandable panel shows defined/discovered/pending counts.
5. Field details panel can be shown/hidden, filtered by status, and sorted by each column.
6. Busy pipeline state disables the new controls and shows busy feedback.
7. `last_fetch` KPI shows relative time and count from checkpoint metadata.
