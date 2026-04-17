let busyPollTimer = null;
let dashboardStatsTimer = null;
let lastQueryResult = null;
let lastRequestedColumns = null;  // Store requested columns
let currentQueryPage = 1;
let currentColumnSortMode = "frequency";
let currentRowSortColumn = "";
const PAGE_SIZE = 100;

let fieldDetailsRows = [];
let fieldDetailsSortKey = "field_name";
let fieldDetailsSortDirection = "asc";
let fieldDetailsFilter = "all";
let fieldDetailsVisible = false;

let _progressTimer = null;
let _progressStart = null;
// Simulated fill: creeps toward 90% while running, jumps to 100% on hideProgress
let _progressFill = 0;

function showProgress(label = "Running...") {
  const wrap = document.getElementById("progress-wrap");
  const labelEl = document.getElementById("progress-label");
  const elapsed = document.getElementById("progress-elapsed");
  const bar = document.getElementById("progress-bar-fill");
  if (!wrap) return;
  if (labelEl) labelEl.textContent = label;
  if (elapsed) elapsed.textContent = "0s";
  _progressFill = 0;
  if (bar) {
    bar.style.transition = "none";
    bar.style.width = "0%";
  }
  wrap.classList.remove("hidden");
  _progressStart = Date.now();
  if (_progressTimer) clearInterval(_progressTimer);
  _progressTimer = setInterval(() => {
    const secs = Math.floor((Date.now() - _progressStart) / 1000);
    if (elapsed) elapsed.textContent = `${secs}s`;
    // Ease fill toward 90%: slows down asymptotically
    if (bar) {
      _progressFill = _progressFill + (90 - _progressFill) * 0.06;
      bar.style.transition = "width 0.6s ease";
      bar.style.width = `${Math.min(_progressFill, 90)}%`;
    }
  }, 600);
}

function hideProgress() {
  const wrap = document.getElementById("progress-wrap");
  const bar = document.getElementById("progress-bar-fill");
  if (_progressTimer) {
    clearInterval(_progressTimer);
    _progressTimer = null;
  }
  _progressStart = null;
  if (bar) {
    bar.style.transition = "width 0.3s ease";
    bar.style.width = "100%";
  }
  // Short delay so user sees 100% before hiding
  setTimeout(() => {
    if (wrap) wrap.classList.add("hidden");
    if (bar) {
      bar.style.transition = "none";
      bar.style.width = "0%";
    }
    _progressFill = 0;
  }, 350);
}

const BASIC_ACID_TESTS = [
  "atomicity",
  "consistency",
  "isolation",
  "durability",
];

const ADVANCED_ACID_TESTS = [
  "multi_record_atomicity",
  "cross_db_atomicity",
  "not_null_constraint",
  "schema_validation",
  "dirty_read_prevention",
  "concurrent_read_write_isolation",
  "concurrent_insert_lost_updates",
  "concurrent_update_atomicity",
  "stress_test_concurrent_ops",
  "persistent_connection",
  "index_integrity",
];

const ACID_TEST_DETAILS = {
  atomicity: {
    performed:
      "Triggered a cross-database failure scenario and verified rollback behavior.",
    passCriteria:
      "No partial write should remain and transaction state should indicate rollback.",
  },
  consistency: {
    performed:
      "Attempted a duplicate/invalid write to validate constraint enforcement.",
    passCriteria: "Database constraints must reject invalid state transitions.",
  },
  isolation: {
    performed:
      "Ran concurrent reads and checked whether all readers observed a stable committed state.",
    passCriteria:
      "Concurrent reads should be consistent with no dirty/intermediate state leakage.",
  },
  durability: {
    performed:
      "Committed a record, then re-read it repeatedly from SQL and Mongo.",
    passCriteria: "Committed data must remain visible across repeated checks.",
  },
  multi_record_atomicity: {
    performed:
      "Inserted multiple records in one transaction and validated all-or-nothing behavior.",
    passCriteria: "Expected full commit of all records or complete rollback.",
  },
  cross_db_atomicity: {
    performed:
      "Validated success and forced-failure paths across SQL and Mongo in a single transaction flow.",
    passCriteria:
      "Success path must commit in both stores; failure path must roll back consistently.",
  },
  not_null_constraint: {
    performed: "Attempted inserts violating required-field constraints.",
    passCriteria: "NOT NULL violations must be rejected by the data layer.",
  },
  schema_validation: {
    performed: "Ran writes with schema/type checks enabled.",
    passCriteria:
      "Invalid schema/type payloads should be rejected; valid payloads should pass.",
  },
  dirty_read_prevention: {
    performed:
      "Simulated concurrent operations to detect visibility of uncommitted writes.",
    passCriteria: "Uncommitted data must not be visible to other transactions.",
  },
  concurrent_read_write_isolation: {
    performed:
      "Executed concurrent reads and writes to verify isolation guarantees.",
    passCriteria:
      "No inconsistent intermediate state should be observed by readers.",
  },
  concurrent_insert_lost_updates: {
    performed:
      "Ran competing insert/update operations under concurrency pressure.",
    passCriteria:
      "System should avoid lost updates and preserve intended writes.",
  },
  concurrent_update_atomicity: {
    performed:
      "Applied concurrent updates and inspected final state coherence.",
    passCriteria:
      "Concurrent updates should remain atomic and leave consistent final data.",
  },
  stress_test_concurrent_ops: {
    performed: "Executed high-concurrency mixed operations.",
    passCriteria:
      "No integrity break, crash, or invariant violation under load.",
  },
  persistent_connection: {
    performed: "Exercised repeated operations over persistent DB connections.",
    passCriteria:
      "Connections should remain healthy without breaking transaction correctness.",
  },
  index_integrity: {
    performed: "Validated behavior around indexed reads/writes after updates.",
    passCriteria: "Indexes should remain consistent with stored records.",
  },
};

function humanizeAcidLabel(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatAcidMetricValue(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) =>
        typeof item === "boolean" ? (item ? "true" : "false") : String(item),
      )
      .join(", ");
  }

  if (value && typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

function buildAcidSummary(testName, payload, isError = false) {
  const info = ACID_TEST_DETAILS[testName] || {};
  const testLabel = humanizeAcidLabel(testName);
  const lines = [];

  lines.push(`Test: ${testLabel}`);
  if (info.performed) {
    lines.push(`Performed: ${info.performed}`);
  }
  if (info.passCriteria) {
    lines.push(`Pass Criteria: ${info.passCriteria}`);
  }

  if (isError || payload?.error) {
    const errorText = payload?.error || "Unknown error";
    lines.push("");
    lines.push(`Observed Output: ${errorText}`);
    lines.push("Conclusion: FAIL (test execution returned an error).");
    return lines.join("\n");
  }

  const evidenceEntries = Object.entries(payload || {}).filter(
    ([key]) => !["test", "passed", "error"].includes(key),
  );

  lines.push("");
  if (evidenceEntries.length > 0) {
    lines.push("Observed Evidence:");
    evidenceEntries.slice(0, 10).forEach(([key, value]) => {
      lines.push(
        `- ${humanizeAcidLabel(key)}: ${formatAcidMetricValue(value)}`,
      );
    });
  } else {
    lines.push(
      "Observed Evidence: Validator returned PASS/FAIL without extra metrics.",
    );
  }

  lines.push("");
  if (payload?.passed === true) {
    lines.push(
      "Conclusion: PASS (observed evidence satisfied the pass criteria).",
    );
  } else if (payload?.passed === false) {
    lines.push(
      "Conclusion: FAIL (observed evidence did not satisfy the pass criteria).",
    );
  } else {
    lines.push(
      "Conclusion: DONE (result received, but validator did not report explicit pass/fail).",
    );
  }

  return lines.join("\n");
}

const QUERY_TEMPLATES = {
  READ: {
    operation: "READ",
    entity: "main_records",
    filters: {},
  },
  CREATE: {
    operation: "CREATE",
    entity: "main_records",
    payload: {
      username: "sample_user",
      action: "view",
    },
  },
  UPDATE: {
    operation: "UPDATE",
    entity: "main_records",
    filters: {
      record_id: 1,
    },
    payload: {
      weather: "sunny",
    },
  },
  DELETE: {
    operation: "DELETE",
    entity: "main_records",
    filters: {
      record_id: 1,
    },
  },
};

function isLandingPage() {
  return Boolean(document.getElementById("pipeline-state"));
}

function isSetupPage() {
  return Boolean(document.getElementById("schema-input"));
}

function isDashboardPage() {
  return Boolean(document.getElementById("dashboard-root"));
}

function setFeedback(element, message, level = false) {
  if (!element) return;
  element.textContent = message;
  element.classList.remove("hidden", "error", "ok", "warn");
  if (level === true || level === "error") {
    element.classList.add("error");
    return;
  }
  if (level === "warn") {
    element.classList.add("warn");
    return;
  }
  element.classList.add("ok");
}

function clearFeedback(element) {
  if (!element) return;
  element.classList.add("hidden");
  element.textContent = "";
  element.classList.remove("error", "ok", "warn");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setDotState(dotElement, isOnline, isBusy = false) {
  if (!dotElement) return;
  dotElement.classList.remove("online", "offline", "busy");
  if (isBusy) {
    dotElement.classList.add("busy");
    return;
  }
  dotElement.classList.add(isOnline ? "online" : "offline");
}

function findCountByName(items, nameKey, countKey, targetName) {
  if (!Array.isArray(items)) return 0;
  const entry = items.find((item) => item && item[nameKey] === targetName);
  return Number(entry?.[countKey] || 0);
}

async function apiGet(url) {
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const payload = await response.json();
      if (payload.detail) detail = payload.detail;
    } catch {
      // Ignore parse errors.
    }
    throw new Error(detail);
  }
  return response.json();
}

async function apiPost(url, body = null) {
  const options = {
    method: "POST",
    headers: {},
  };

  if (body !== null) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const payload = await response.json();
      if (payload.detail) detail = payload.detail;
    } catch {
      // Ignore parse errors.
    }
    throw new Error(detail);
  }

  return response.json();
}

function getResetEndpointUrl(wipeSchema) {
  return `/api/pipeline/reset?wipe_schema=${wipeSchema ? 'true' : 'false'}`;
}

function showResetConfirmation() {
  return new Promise((resolve) => {
    const dialog = document.getElementById("reset-dialog");
    const confirmBtn = document.getElementById("btn-reset-confirm");
    const cancelBtn = document.getElementById("btn-reset-cancel");
    const wipeCheck = document.getElementById("wipe-schema-check");

    if (!dialog || typeof dialog.showModal !== "function" || !confirmBtn || !cancelBtn) {
      const confirmed = window.confirm("Reset everything? This clears SQL and Mongo data.");
      resolve({ confirmed, wipeSchema: false });
      return;
    }

    let settled = false;
    const finalize = (result) => {
      if (settled) return;
      settled = true;
      confirmBtn.removeEventListener("click", onConfirm);
      cancelBtn.removeEventListener("click", onCancel);
      dialog.removeEventListener("close", onClose);
      if (dialog.open) dialog.close();
      resolve(result);
    };

    const onConfirm = () => finalize({ confirmed: true, wipeSchema: !!wipeCheck?.checked });
    const onCancel = () => finalize({ confirmed: false, wipeSchema: false });
    const onClose = () => finalize({ confirmed: false, wipeSchema: false });

    confirmBtn.addEventListener("click", onConfirm);
    cancelBtn.addEventListener("click", onCancel);
    dialog.addEventListener("close", onClose);

    if (wipeCheck) wipeCheck.checked = false;
    dialog.showModal();
  });
}

async function fetchLandingState() {
  const [status, stats] = await Promise.all([
    apiGet("/api/status"),
    apiGet("/api/stats"),
  ]);
  return { status, stats };
}

function normalizeState(stateValue) {
  if (stateValue === "schema_ready") return "schema ready";
  return stateValue || "fresh";
}

function setButtonsDisabled(disabled) {
  const controls = [
    document.getElementById("btn-setup"),
    document.getElementById("btn-initialise"),
    document.getElementById("btn-dashboard"),
    document.getElementById("btn-reset"),
    document.getElementById("init-count"),
  ];

  controls.forEach((control) => {
    if (control) control.disabled = disabled;
  });
}

function applyLandingState(status, stats) {
  const stateLabel = document.getElementById("pipeline-state");
  const busyNotice = document.getElementById("pipeline-busy");
  const warning = document.getElementById("external-api-warning");
  const initCountRow = document.getElementById("init-count-row");

  const setupBtn = document.getElementById("btn-setup");
  const initialiseBtn = document.getElementById("btn-initialise");
  const dashboardBtn = document.getElementById("btn-dashboard");

  if (stateLabel)
    stateLabel.textContent = normalizeState(status.pipeline_state);

  const pipelineBusy = Boolean(status.pipeline_busy);
  if (busyNotice) busyNotice.classList.toggle("hidden", !pipelineBusy);
  if (warning) {
    const externalReachable = Boolean(stats.external_api_reachable);
    // While pipeline work is running, suppress external API noise to avoid false alarms.
    warning.classList.toggle("hidden", externalReachable || pipelineBusy);
  }

  if (setupBtn) setupBtn.classList.add("hidden");
  if (initialiseBtn) initialiseBtn.classList.add("hidden");
  if (dashboardBtn) dashboardBtn.classList.add("hidden");
  if (initCountRow) initCountRow.classList.add("hidden");

  if (status.pipeline_state === "initialized") {
    if (dashboardBtn) dashboardBtn.classList.remove("hidden");
  } else if (status.pipeline_state === "schema_ready") {
    if (initialiseBtn) initialiseBtn.classList.remove("hidden");
    if (initCountRow) initCountRow.classList.remove("hidden");
  } else {
    if (setupBtn) setupBtn.classList.remove("hidden");
  }

  setButtonsDisabled(pipelineBusy);
}

async function refreshLanding() {
  const feedback = document.getElementById("landing-message");
  try {
    const { status, stats } = await fetchLandingState();
    applyLandingState(status, stats);
  } catch (error) {
    // During in-flight operations, transient endpoint blips are warnings, not hard errors.
    setFeedback(
      feedback,
      String(error.message || error),
      busyPollTimer ? "warn" : true,
    );
  }
}

function startBusyPolling() {
  if (busyPollTimer) return;
  busyPollTimer = setInterval(async () => {
    try {
      const status = await apiGet("/api/status");
      if (!status.pipeline_busy) {
        clearInterval(busyPollTimer);
        busyPollTimer = null;
        await refreshLanding();
      }
    } catch {
      // Ignore transient poll errors.
    }
  }, 1500);
}

function attachLandingHandlers() {
  const setupBtn = document.getElementById("btn-setup");
  const initialiseBtn = document.getElementById("btn-initialise");
  const dashboardBtn = document.getElementById("btn-dashboard");
  const resetBtn = document.getElementById("btn-reset");
  const initCount = document.getElementById("init-count");
  const feedback = document.getElementById("landing-message");

  if (setupBtn) {
    setupBtn.addEventListener("click", () => {
      window.location.href = "/setup";
    });
  }

  if (dashboardBtn) {
    dashboardBtn.addEventListener("click", () => {
      window.location.href = "/dashboard";
    });
  }

  if (initialiseBtn) {
    initialiseBtn.addEventListener("click", async () => {
      clearFeedback(feedback);
      const count = Math.max(Number(initCount?.value || 1000), 1);
      setButtonsDisabled(true);
      setFeedback(feedback, "Initialise started. Waiting for completion...");

      try {
        await apiPost(`/api/pipeline/initialise?count=${count}`);
        setFeedback(feedback, "Initialise completed.");
        await refreshLanding();
      } catch (error) {
        setFeedback(feedback, String(error.message || error), true);
        await refreshLanding();
      } finally {
        startBusyPolling();
      }
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", async () => {
      clearFeedback(feedback);
      const { confirmed, wipeSchema } = await showResetConfirmation();
      if (!confirmed) return;

      setButtonsDisabled(true);
      setFeedback(feedback, "Reset started...");

      try {
        await apiPost(getResetEndpointUrl(wipeSchema));
        setFeedback(feedback, wipeSchema ? "Reset completed. Pipeline is fresh." : "Reset completed. Pipeline is schema ready.");
        await refreshLanding();
      } catch (error) {
        setFeedback(feedback, String(error.message || error), true);
        await refreshLanding();
      } finally {
        startBusyPolling();
      }
    });
  }
}

function attachSetupHandlers() {
  const schemaInput = document.getElementById("schema-input");
  const saveBtn = document.getElementById("btn-save-schema");
  const initBtn = document.getElementById("btn-setup-initialise");
  const homeBtn = document.getElementById("btn-back-home");
  const initCount = document.getElementById("setup-init-count");
  const message = document.getElementById("setup-message");

  if (homeBtn) {
    homeBtn.addEventListener("click", () => {
      window.location.href = "/";
    });
  }

  if (saveBtn) {
    saveBtn.addEventListener("click", async () => {
      clearFeedback(message);
      const raw = String(schemaInput?.value || "").trim();
      if (!raw) {
        setFeedback(message, "Schema JSON is required.", true);
        return;
      }

      let schema;
      try {
        schema = JSON.parse(raw);
      } catch {
        setFeedback(message, "Schema must be valid JSON.", true);
        return;
      }

      saveBtn.disabled = true;
      setFeedback(message, "Saving schema...");

      try {
        await apiPost("/api/pipeline/schema", { schema });
        setFeedback(message, "Schema saved.");
        if (initBtn) initBtn.classList.remove("hidden");
      } catch (error) {
        setFeedback(message, String(error.message || error), true);
      } finally {
        saveBtn.disabled = false;
      }
    });
  }

  if (initBtn) {
    initBtn.addEventListener("click", async () => {
      clearFeedback(message);
      const count = Math.max(Number(initCount?.value || 1000), 1);
      initBtn.disabled = true;
      setFeedback(
        message,
        "Initialise started. Redirecting to landing on completion...",
      );

      try {
        await apiPost(`/api/pipeline/initialise?count=${count}`);
        window.location.href = "/";
      } catch (error) {
        setFeedback(message, String(error.message || error), true);
        initBtn.disabled = false;
      }
    });
  }
}

function setDashboardControlsDisabled(disabled) {
  const controls = [
    document.getElementById("btn-fetch-more-toggle"),
    document.getElementById("btn-run-fetch"),
    document.getElementById("btn-reset-dashboard"),
    document.getElementById("btn-run-query"),
    document.getElementById("query-operation"),
    document.getElementById("query-column-sort"),
    document.getElementById("query-row-sort"),
    document.getElementById("btn-run-all-acid"),
    document.getElementById("btn-run-all-advanced-acid"),
    document.getElementById("fetch-count"),
    document.getElementById("btn-download-json"),
    document.getElementById("btn-toggle-field-details"),
    document.getElementById("field-status-filter"),
  ];

  controls.forEach((control) => {
    if (control) control.disabled = disabled;
  });

  document.querySelectorAll(".run-acid-btn, .run-advanced-acid-btn").forEach((button) => {
    button.disabled = disabled;
  });
}

function formatInteger(num) {
  return new Intl.NumberFormat().format(num || 0);
}

function toPercentValue(fraction) {
  return Math.round(Number(fraction || 0) * 100);
}

function formatPercent(value) {
  return `${Number(value || 0).toFixed(1)}%`;
}

function titleCase(str) {
  return String(str || "").replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
}

function formatRelativeTime(isoRaw) {
  if (!isoRaw) return "Never";
  try {
    const d = new Date(isoRaw);
    if (isNaN(d.getTime())) return "Invalid date";
    const diff = Math.floor((new Date() - d) / 1000);
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff/60)}m ago`;
    return `${Math.floor(diff/3600)}h ago`;
  } catch {
    return "Unknown";
  }
}

function formatClockTime(dateObj) {
  return dateObj.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function setStatsRefreshedAt(variant = "success") {
  const el = document.getElementById("stats-refreshed-at");
  if (!el) return;

  const now = new Date();
  const label = variant === "error" ? "Refresh failed" : "Last refreshed";
  el.textContent = `${label}: ${formatClockTime(now)}`;
}

function getSystemStatusPresentation(status) {
  if (status.pipeline_state !== "initialized") return { message: "System initializing", variant: "warning" };
  if (status.pipeline_busy) return { message: "Pipeline busy", variant: "busy" };
  return { message: "Online and ready", variant: "online" };
}

function normalizeFieldDetailsRows(rows) {
  return Array.isArray(rows) ? rows : [];
}

function updateFieldSortButtonState() {
  document.querySelectorAll(".field-sort-btn").forEach((btn) => {
    const sortKey = btn.getAttribute("data-sort-key");
    const baseText = titleCase(sortKey);
    if (sortKey === fieldDetailsSortKey) {
      btn.textContent = `${baseText} (${fieldDetailsSortDirection})`;
    } else {
      btn.textContent = baseText;
    }
  });
}

function getVisibleFieldDetailsRows() {
  const filtered = fieldDetailsRows.filter(r => fieldDetailsFilter === "all" || r.status === fieldDetailsFilter);
  return filtered.sort((a, b) => {
    let va = a[fieldDetailsSortKey];
    let vb = b[fieldDetailsSortKey];
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    
    if (va < vb) return fieldDetailsSortDirection === "asc" ? -1 : 1;
    if (va > vb) return fieldDetailsSortDirection === "asc" ? 1 : -1;
    return 0;
  });
}

function renderFieldDetailsTable() {
  const tbody = document.getElementById("field-details-body");
  if (!tbody) return;
  const rows = getVisibleFieldDetailsRows();
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4" class="meta-text" style="text-align: center;">No fields match the current filter.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${escapeHtml(r.field_name)}</td>
      <td>${escapeHtml(r.status)}</td>
      <td>${formatInteger(r.frequency)}</td>
      <td>${formatPercent(r.density)}</td>
    </tr>
  `).join("");
}

function setKpiCardContent(id, value, subtitle, statusValue = null) {
  const card = document.getElementById(id);
  if (!card) return;
  
  let html = `<span class="kpi-label">${escapeHtml(card.getAttribute("data-label") || titleCase(id.replace("kpi-", "")))}</span>`;
  html += `<span class="kpi-value">${escapeHtml(value)}</span>`;
  html += `<span class="kpi-subtitle">${escapeHtml(subtitle)}</span>`;
  
  if (statusValue) {
    html += `<span class="kpi-status-value">${escapeHtml(statusValue)}</span>`;
  }
  
  card.innerHTML = html;
}

function renderDashboardStatsBundle(status, stats, pipelineStats) {
  // Update SQL and NoSQL connection indicators
  const sqlDot = document.getElementById("sql-status-dot");
  if (sqlDot) {
    setDotState(sqlDot, status.sql_connected || false);
  }
  
  const mongoDot = document.getElementById("mongo-status-dot");
  if (mongoDot) {
    setDotState(mongoDot, status.mongo_connected || false);
  }
  
  setKpiCardContent("kpi-total-records", formatInteger(pipelineStats?.total_records), "Total stored records");
  setKpiCardContent("kpi-active-fields", formatInteger(pipelineStats?.active_fields?.total), "Fields actively tracked");
  setKpiCardContent("kpi-data-density", formatPercent(pipelineStats?.data_density), "Average field density");
  
  const fetchedCount = formatInteger(pipelineStats?.last_fetch?.count);
  const fetchedWhen = formatRelativeTime(pipelineStats?.last_fetch?.timestamp);
  setKpiCardContent("kpi-last-fetch", fetchedWhen, `Fetched ${fetchedCount} records`);
  
  const txTotal = formatInteger(pipelineStats?.transactions?.total);
  setKpiCardContent("kpi-transactions", txTotal, "Total operations");
  
  setKpiCardContent("kpi-active-fields-breakdown", formatInteger(pipelineStats?.active_fields?.defined), "Defined fields");
}

async function refreshDashboardStats() {
  const feedback = document.getElementById("dashboard-feedback");
  try {
    const [status, stats, pipelineStats] = await Promise.all([
      apiGet("/api/status"),
      apiGet("/api/stats"),
      apiGet("/api/pipeline/stats"),
    ]);

    const stateLabel = document.getElementById("dashboard-state");
    if (stateLabel) stateLabel.textContent = normalizeState(status.pipeline_state);

    if (status.pipeline_state !== "initialized") {
      setFeedback(feedback, "Pipeline is not initialized anymore. Redirecting to landing...", true);
      setTimeout(() => window.location.href = "/", 1200);
      return;
    }
    
    if (status.pipeline_busy) {
      setDashboardControlsDisabled(true);
      setFeedback(feedback, "Pipeline is busy. Controls are temporarily locked.", "warn");
    } else {
      clearFeedback(feedback);
      setDashboardControlsDisabled(false);
    }
    
    renderDashboardStatsBundle(status, stats, pipelineStats);
    setStatsRefreshedAt("success");
  } catch (error) {
    setFeedback(feedback, `Stats refresh failed: ${String(error.message || error)}`, true);
    setStatsRefreshedAt("error");
  }
}

function applyQueryTemplate(operation) {
  const editor = document.getElementById("query-json");
  if (!editor) return;
  const template = QUERY_TEMPLATES[operation] || QUERY_TEMPLATES.READ;
  editor.value = JSON.stringify(template, null, 2);
}

function normalizeDisplayRecord(record) {
  const hiddenKeys = new Set([
    "record_id",
    "_source",
    "routing",
    "routing_decisions",
    "transaction",
    "transaction_id",
  ]);

  const normalized = {};
  Object.entries(record || {}).forEach(([key, value]) => {
    if (!hiddenKeys.has(key)) {
      normalized[key] = value;
    }
  });
  return normalized;
}

function valueToCell(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return escapeHtml(JSON.stringify(value));
  return escapeHtml(String(value));
}

function getRowSortDisplayValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const numeric = Number(trimmed);
    return Number.isFinite(numeric) ? numeric : trimmed.toLowerCase();
  }
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value instanceof Date) return value.getTime();
  if (Array.isArray(value) || typeof value === "object") {
    return JSON.stringify(value).toLowerCase();
  }
  return String(value).toLowerCase();
}

function sortRecordsByColumn(records, columnName) {
  if (!columnName) return records.slice();

  return records
    .map((record, index) => ({ record, index }))
    .sort((left, right) => {
      const leftValue = getRowSortDisplayValue(left.record?.[columnName]);
      const rightValue = getRowSortDisplayValue(right.record?.[columnName]);

      const leftMissing = leftValue === null || leftValue === undefined;
      const rightMissing = rightValue === null || rightValue === undefined;
      if (leftMissing && rightMissing) return left.index - right.index;
      if (leftMissing) return 1;
      if (rightMissing) return -1;

      if (typeof leftValue === "number" && typeof rightValue === "number") {
        if (leftValue !== rightValue) return leftValue - rightValue;
      } else {
        const compared = String(leftValue).localeCompare(
          String(rightValue),
          undefined,
          {
            numeric: true,
            sensitivity: "base",
          },
        );
        if (compared !== 0) return compared;
      }

      return left.index - right.index;
    })
    .map(({ record }) => record);
}

function populateRowSortOptions(columns) {
  const select = document.getElementById("query-row-sort");
  if (!select) return;

  const availableColumns = Array.isArray(columns) ? columns : [];
  const hasSelection = availableColumns.includes(currentRowSortColumn);
  if (!hasSelection) {
    currentRowSortColumn = "";
  }

  select.innerHTML = [
    '<option value="">No row sort</option>',
    ...availableColumns.map(
      (column) =>
        `<option value="${escapeHtml(column)}">${escapeHtml(column)}</option>`,
    ),
  ].join("");
  select.value = currentRowSortColumn;
  select.disabled = availableColumns.length === 0;
}

function hasMeaningfulValue(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === "string") return value.trim().length > 0;
  return true;
}

function getColumnsBySortMode(records, sortMode) {
  const columnFrequency = new Map();
  const firstSeenIndex = new Map();
  let keyIndex = 0;

  records.forEach((record) => {
    Object.keys(record).forEach((key) => {
      const value = record[key];
      if (hasMeaningfulValue(value)) {
        columnFrequency.set(key, (columnFrequency.get(key) || 0) + 1);
      } else if (!columnFrequency.has(key)) {
        columnFrequency.set(key, 0);
      }
      if (!firstSeenIndex.has(key)) {
        firstSeenIndex.set(key, keyIndex++);
      }
    });
  });

  const entries = Array.from(columnFrequency.entries());

  if (sortMode === "alphabetical") {
    return {
      columns: entries.map(([key]) => key).sort((a, b) => a.localeCompare(b)),
      columnFrequency,
    };
  }

  if (sortMode === "original") {
    return {
      columns: entries
        .map(([key]) => key)
        .sort(
          (a, b) => (firstSeenIndex.get(a) || 0) - (firstSeenIndex.get(b) || 0),
        ),
      columnFrequency,
    };
  }

  return {
    columns: entries
      .sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1];
        return a[0].localeCompare(b[0]);
      })
      .map(([key]) => key),
    columnFrequency,
  };
}

function getSortLabel(sortMode) {
  if (sortMode === "alphabetical") return "alphabetical";
  if (sortMode === "original") return "original order";
  return "field frequency";
}

function renderReadTable(result, page = 1) {
  const records = Object.values(result?.data || {}).map((record) =>
    normalizeDisplayRecord(record),
  );
  if (!records.length) {
    populateRowSortOptions([]);
    return '<p class="meta-text">No records matched this query.</p>';
  }

  const totalPages = Math.ceil(records.length / PAGE_SIZE);
  const startIdx = (page - 1) * PAGE_SIZE;

  const { columns, columnFrequency } = getColumnsBySortMode(
    records,
    currentColumnSortMode,
  );

  // Filter to only requested columns if specified
  let filteredColumns = columns;
  if (lastRequestedColumns && Array.isArray(lastRequestedColumns) && lastRequestedColumns.length > 0) {
    const requestedSet = new Set(lastRequestedColumns);
    filteredColumns = columns.filter(col => requestedSet.has(col) || col === 'record_id');
  }

  populateRowSortOptions(filteredColumns);

  const sortedRecords = sortRecordsByColumn(records, currentRowSortColumn);
  const displayRecords = sortedRecords.slice(startIdx, startIdx + PAGE_SIZE);

  if (!filteredColumns.length) {
    return '<p class="meta-text">Records were returned, but all visible fields are hidden in this view.</p>';
  }

  const from = startIdx + 1;
  const to = Math.min(startIdx + displayRecords.length, records.length);

  const headerHtml = filteredColumns
    .map((col) => {
      const presentCount = columnFrequency.get(col) || 0;
      const title = `${col} (${presentCount}/${records.length} populated records)`;
      return `<th title="${escapeHtml(title)}">${escapeHtml(col)}</th>`;
    })
    .join("");

  const rowsHtml = displayRecords
    .map(
      (record) =>
        `<tr>${filteredColumns
          .map((col) => {
            const raw = record[col];
            const display = valueToCell(raw);
            const tip =
              raw !== null && raw !== undefined
                ? escapeHtml(
                    String(typeof raw === "object" ? JSON.stringify(raw) : raw),
                  )
                : "";
            return `<td title="${tip}">${display}</td>`;
          })
          .join("")}</tr>`,
    )
    .join("");

  let paginationHtml = "";
  if (totalPages > 1) {
    paginationHtml = `
			<div class="pagination-controls">
				<button class="btn btn-sm" onclick="changePage(${page - 1})" ${page === 1 ? "disabled" : ""}>← Prev</button>
				<span class="pagination-info">Page ${page} of ${totalPages}</span>
				<button class="btn btn-sm" onclick="changePage(${page + 1})" ${page === totalPages ? "disabled" : ""}>Next →</button>
			</div>`;
  }

  return `
		<div class="result-table-meta">
			<span>${from}–${to} of ${records.length} records</span>
      <span>${filteredColumns.length} columns · sorted by ${getSortLabel(currentColumnSortMode)}</span>
		</div>
		<div class="result-table-wrap">
			<table class="result-table">
				<thead><tr>${headerHtml}</tr></thead>
				<tbody>${rowsHtml}</tbody>
			</table>
		</div>
		${paginationHtml}`;
}

function refreshCurrentReadResults() {
  if (
    !lastQueryResult ||
    lastQueryResult.operation !== "READ" ||
    !lastQueryResult.data ||
    typeof lastQueryResult.data !== "object"
  ) {
    return;
  }

  const resultsContainer = document.getElementById("query-results");
  if (!resultsContainer) return;

  resultsContainer.innerHTML = renderReadTable(
    lastQueryResult,
    currentQueryPage,
  );

  const wrap = resultsContainer.querySelector(".result-table-wrap");
  if (wrap) wrap.scrollTop = 0;
}

window.changePage = function (newPage) {
  if (!lastQueryResult) return;
  const records = Object.values(lastQueryResult?.data || {});
  const totalPages = Math.ceil(records.length / PAGE_SIZE);
  if (newPage < 1 || newPage > totalPages) return;

  currentQueryPage = newPage;
  const resultsContainer = document.getElementById("query-results");
  resultsContainer.innerHTML = renderReadTable(
    lastQueryResult,
    currentQueryPage,
  );

  // Scroll to top of results
  const wrap = resultsContainer.querySelector(".result-table-wrap");
  if (wrap) wrap.scrollTop = 0;
};

function renderSummaryTable(result) {
  const entries = [];
  entries.push(["operation", result?.operation || "-"]);
  entries.push(["entity", result?.entity || "-"]);
  if (result?.status) entries.push(["status", result.status]);
  if (result?.error) entries.push(["error", result.error]);
  if (result?.details?.error)
    entries.push(["details_error", result.details.error]);

  if (
    result?.details?.updated_count &&
    typeof result.details.updated_count === "object"
  ) {
    const totalUpdated = Object.values(result.details.updated_count).reduce(
      (acc, count) => acc + Number(count || 0),
      0,
    );
    entries.push(["updated_records", totalUpdated]);
  }

  if (
    result?.details?.deleted_count &&
    typeof result.details.deleted_count === "object"
  ) {
    const totalDeleted = Object.values(result.details.deleted_count).reduce(
      (acc, count) => acc + Number(count || 0),
      0,
    );
    entries.push(["deleted_records", totalDeleted]);
  }

  if (!entries.length) {
    return '<p class="meta-text">No summary available for this response.</p>';
  }

  const rowsHtml = entries
    .map(
      ([key, value]) =>
        `<tr><th>${escapeHtml(String(key))}</th><td>${valueToCell(value)}</td></tr>`,
    )
    .join("");

  return `
		<div class="result-table-wrap">
			<table class="kv-table">
				<tbody>
					${rowsHtml}
				</tbody>
			</table>
		</div>
	`;
}

function renderQueryResult(result) {
  const resultsContainer = document.getElementById("query-results");
  const emptyState = document.getElementById("query-empty");
  if (!resultsContainer || !emptyState) return;

  if (!result) {
    emptyState.classList.remove("hidden");
    resultsContainer.classList.add("hidden");
    resultsContainer.innerHTML = "";
    return;
  }

  emptyState.classList.add("hidden");
  resultsContainer.classList.remove("hidden");

  if (
    result.operation === "READ" &&
    result.data &&
    typeof result.data === "object"
  ) {
    currentQueryPage = 1;
    resultsContainer.innerHTML = renderReadTable(result, currentQueryPage);
    return;
  }

  populateRowSortOptions([]);
  currentRowSortColumn = "";

  resultsContainer.innerHTML = renderSummaryTable(result);
}

function setAcidBadge(testName, label, variant) {
  const badge = document.getElementById(`badge-${testName}`);
  if (!badge) return;
  badge.textContent = label;
  badge.classList.remove("neutral", "pass", "fail");
  badge.classList.add(variant);
}

function renderAcidResult(testName, payload, isError = false) {
  const details = document.getElementById(`details-${testName}`);
  const summaryTarget = document.getElementById(`json-${testName}`);

  if (summaryTarget) {
    summaryTarget.textContent = buildAcidSummary(testName, payload, isError);
  }
  if (details) {
    details.classList.remove("hidden");
  }

  if (isError || payload?.error || payload?.passed === false) {
    setAcidBadge(testName, "FAIL", "fail");
    return;
  }

  if (payload?.passed === true) {
    setAcidBadge(testName, "PASS", "pass");
    return;
  }

  setAcidBadge(testName, "DONE", "neutral");
}

async function runSingleAcidTest(testName, isAdvanced = false) {
  const feedback = document.getElementById("acid-feedback");
  const button = document.querySelector(
    `.run-${isAdvanced ? "advanced-" : ""}acid-btn[data-test="${testName}"]`,
  );
  const previousText = button?.textContent || "Run Test";

  if (button) {
    button.disabled = true;
    button.textContent = "Running...";
  }
  clearFeedback(feedback);
  showProgress(`Running ${testName}...`);

  try {
    // Use advanced endpoint if it's an advanced test
    const endpoint = isAdvanced
      ? `/api/acid/advanced/${testName}`
      : `/api/acid/${testName}`;
    console.log(`[ACID] Fetching: ${endpoint}`);
    const result = await apiGet(endpoint);
    console.log(`[ACID] Result:`, result);
    renderAcidResult(testName, result, false);
    setFeedback(feedback, `${testName} test completed.`, false);
  } catch (error) {
    console.error(`[ACID] Error for ${testName}:`, error);
    const payload = { error: String(error.message || error) };
    renderAcidResult(testName, payload, true);
    setFeedback(feedback, payload.error, true);
  } finally {
    hideProgress();
    if (button) {
      button.disabled = false;
      button.textContent = previousText;
    }
  }
}

async function runAllAcidTests() {
  const feedback = document.getElementById("acid-feedback");
  const runAllButton = document.getElementById("btn-run-all-acid");
  if (runAllButton) {
    runAllButton.disabled = true;
    runAllButton.textContent = "Running...";
  }
  document.querySelectorAll(".run-acid-btn").forEach((button) => {
    button.disabled = true;
  });

  clearFeedback(feedback);

  try {
    const results = await apiGet("/api/acid/all");
    BASIC_ACID_TESTS.forEach((testName) => {
      if (results && Object.prototype.hasOwnProperty.call(results, testName)) {
        renderAcidResult(testName, results[testName], false);
      }
    });
    setFeedback(feedback, "All ACID tests completed.", false);
  } catch (error) {
    setFeedback(feedback, String(error.message || error), true);
  } finally {
    if (runAllButton) {
      runAllButton.disabled = false;
      runAllButton.textContent = "Run All";
    }
    document.querySelectorAll(".run-acid-btn").forEach((button) => {
      button.disabled = false;
    });
  }
}

async function runAllAdvancedAcidTests() {
  const feedback = document.getElementById("acid-feedback");
  const runAllAdvButton = document.getElementById("btn-run-all-advanced-acid");

  if (runAllAdvButton) {
    runAllAdvButton.disabled = true;
    runAllAdvButton.textContent = "Running...";
  }
  document.querySelectorAll(".run-advanced-acid-btn").forEach((button) => {
    button.disabled = true;
  });

  clearFeedback(feedback);
  showProgress("Running advanced ACID tests...");

  try {
    // Fetch results for each advanced test sequentially
    const results = {};
    for (const testName of ADVANCED_ACID_TESTS) {
      try {
        console.log(`[Advanced] Running: ${testName}`);
        results[testName] = await apiGet(`/api/acid/advanced/${testName}`);
        console.log(`[Advanced] Completed: ${testName}`, results[testName]);
      } catch (e) {
        console.error(`[Advanced] Failed: ${testName}`, e);
        results[testName] = { passed: false, error: String(e.message || e) };
      }
    }

    // Render all results
    ADVANCED_ACID_TESTS.forEach((testName) => {
      if (results && Object.prototype.hasOwnProperty.call(results, testName)) {
        renderAcidResult(testName, results[testName], false);
      }
    });

    setFeedback(feedback, "All advanced ACID tests completed.", false);
  } catch (error) {
    console.error("[Advanced] Error:", error);
    setFeedback(feedback, String(error.message || error), true);
  } finally {
    hideProgress();
    if (runAllAdvButton) {
      runAllAdvButton.disabled = false;
      runAllAdvButton.textContent = "Run All Advanced";
    }
    document.querySelectorAll(".run-advanced-acid-btn").forEach((button) => {
      button.disabled = false;
    });
  }
}

function downloadQueryJson() {
  if (!lastQueryResult) return;
  const payload = JSON.stringify(lastQueryResult, null, 2);
  const blob = new Blob([payload], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `query-result-${Date.now()}.json`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function attachDashboardHandlers() {
  const fetchToggle = document.getElementById("btn-fetch-more-toggle");
  const fetchForm = document.getElementById("fetch-form-inline");
  const runFetchButton = document.getElementById("btn-run-fetch");
  const fetchCount = document.getElementById("fetch-count");
  const resetButton = document.getElementById("btn-reset-dashboard");
  const feedback = document.getElementById("dashboard-feedback");

  const operationSelect = document.getElementById("query-operation");
  const queryEditor = document.getElementById("query-json");
  const runQueryButton = document.getElementById("btn-run-query");
  const columnSortSelect = document.getElementById("query-column-sort");
  const rowSortSelect = document.getElementById("query-row-sort");
  const queryFeedback = document.getElementById("query-feedback");
  const downloadButton = document.getElementById("btn-download-json");

  if (fetchToggle && fetchForm) {
    fetchToggle.addEventListener("click", () => {
      fetchForm.classList.toggle("hidden");
    });
  }

  if (runFetchButton) {
    runFetchButton.addEventListener("click", async () => {
      clearFeedback(feedback);
      const count = Math.max(Number(fetchCount?.value || 100), 1);
      setDashboardControlsDisabled(true);
      setFeedback(feedback, "Fetch started. Waiting for completion...", false);

      try {
        await apiPost(`/api/pipeline/fetch?count=${count}`);
        setFeedback(feedback, `Fetch completed for ${count} records.`, false);
        await refreshDashboardStats();
      } catch (error) {
        setFeedback(feedback, String(error.message || error), true);
      } finally {
        await refreshDashboardStats();
      }
    });
  }

  if (resetButton) {
    resetButton.addEventListener("click", async () => {
      clearFeedback(feedback);
      const { confirmed, wipeSchema } = await showResetConfirmation();
      if (!confirmed) return;

      setDashboardControlsDisabled(true);
      setFeedback(feedback, "Reset started...", false);

      try {
        await apiPost(getResetEndpointUrl(wipeSchema));
        setFeedback(feedback, wipeSchema ? "Reset completed. Schema wiped. Redirecting..." : "Reset completed. Schema preserved. Redirecting...", false);
        setTimeout(() => window.location.href = "/", 900);
      } catch (error) {
        setFeedback(feedback, String(error.message || error), true);
        await refreshDashboardStats();
      }
    });
  }

  const schemaDetails = document.getElementById("schema-dimensions-details");
  const schemaAction = document.getElementById("schema-summary-action");
  if (schemaDetails && schemaAction) {
    schemaDetails.addEventListener("toggle", () => {
      schemaAction.textContent = schemaDetails.open ? "Collapse" : "Expand";
    });
  }

  const toggleDetailsBtn = document.getElementById("btn-toggle-field-details");
  const detailsPanel = document.getElementById("field-details-panel");
  if (toggleDetailsBtn && detailsPanel) {
    toggleDetailsBtn.addEventListener("click", () => {
      fieldDetailsVisible = !fieldDetailsVisible;
      if (fieldDetailsVisible) {
        detailsPanel.classList.remove("hidden");
        toggleDetailsBtn.textContent = "Hide Field Details";
      } else {
        detailsPanel.classList.add("hidden");
        toggleDetailsBtn.textContent = "View Field Details ->";
      }
    });
  }

  const statusFilter = document.getElementById("field-status-filter");
  if (statusFilter) {
    statusFilter.addEventListener("change", () => {
      fieldDetailsFilter = statusFilter.value;
      renderFieldDetailsTable();
    });
  }

  document.querySelectorAll(".field-sort-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.getAttribute("data-sort-key");
      if (fieldDetailsSortKey === key) {
        fieldDetailsSortDirection = fieldDetailsSortDirection === "asc" ? "desc" : "asc";
      } else {
        fieldDetailsSortKey = key;
        fieldDetailsSortDirection = "asc";
      }
      updateFieldSortButtonState();
      renderFieldDetailsTable();
    });
  });

  if (operationSelect) {
    operationSelect.addEventListener("change", () => {
      applyQueryTemplate(operationSelect.value);
    });
  }

  if (columnSortSelect) {
    columnSortSelect.addEventListener("change", () => {
      currentColumnSortMode = columnSortSelect.value || "frequency";
      refreshCurrentReadResults();
    });
  }

  if (rowSortSelect) {
    rowSortSelect.addEventListener("change", () => {
      currentRowSortColumn = rowSortSelect.value || "";
      refreshCurrentReadResults();
    });
  }

  if (runQueryButton) {
    runQueryButton.addEventListener("click", async () => {
      clearFeedback(queryFeedback);
      let payload;

      try {
        payload = JSON.parse(String(queryEditor?.value || "{}"));
      } catch {
        setFeedback(queryFeedback, "Query JSON must be valid JSON.", true);
        return;
      }

      if (operationSelect) {
        payload.operation = operationSelect.value;
      }
      if (!payload.entity) {
        payload.entity = "main_records";
      }

      runQueryButton.disabled = true;
      setFeedback(queryFeedback, "Running query...", false);

      try {
        const result = await apiPost("/api/query", payload);
        lastQueryResult = result;
        lastRequestedColumns = payload.columns || null;  // Store requested columns
        renderQueryResult(result);
        await refreshDashboardStats();
        if (downloadButton) downloadButton.disabled = false;
        setFeedback(queryFeedback, "Query completed.", false);
      } catch (error) {
        setFeedback(queryFeedback, String(error.message || error), true);
      } finally {
        runQueryButton.disabled = false;
      }
    });
  }

  if (downloadButton) {
    downloadButton.addEventListener("click", downloadQueryJson);
  }

  document.querySelectorAll(".run-acid-btn").forEach((button) => {
    button.addEventListener("click", async () => {
      const testName = button.getAttribute("data-test");
      if (!testName) return;
      await runSingleAcidTest(testName, false);
    });
  });

  const runAllButton = document.getElementById("btn-run-all-acid");
  if (runAllButton) {
    runAllButton.addEventListener("click", runAllAcidTests);
  }

  document.querySelectorAll(".run-advanced-acid-btn").forEach((button) => {
    button.addEventListener("click", async () => {
      const testName = button.getAttribute("data-test");
      if (!testName) return;
      await runSingleAcidTest(testName, true);
    });
  });

  const runAllAdvButton = document.getElementById("btn-run-all-advanced-acid");
  if (runAllAdvButton) {
    runAllAdvButton.addEventListener("click", runAllAdvancedAcidTests);
  }

  window.addEventListener("beforeunload", () => {
    if (dashboardStatsTimer) {
      clearInterval(dashboardStatsTimer);
      dashboardStatsTimer = null;
    }
  });
}

async function initializeDashboard() {
  const operationSelect = document.getElementById("query-operation");
  const downloadButton = document.getElementById("btn-download-json");
  const columnSortSelect = document.getElementById("query-column-sort");
  const rowSortSelect = document.getElementById("query-row-sort");
  const statusFilter = document.getElementById("field-status-filter");
  const toggleDetailsBtn = document.getElementById("btn-toggle-field-details");
  const detailsPanel = document.getElementById("field-details-panel");

  if (downloadButton) downloadButton.disabled = true;
  if (operationSelect) applyQueryTemplate(operationSelect.value);
  if (columnSortSelect) {
    currentColumnSortMode = columnSortSelect.value || "frequency";
  }
  if (rowSortSelect) {
    currentRowSortColumn = rowSortSelect.value || "";
  }

  fieldDetailsRows = [];
  fieldDetailsSortKey = "field_name";
  fieldDetailsSortDirection = "asc";
  fieldDetailsFilter = "all";
  fieldDetailsVisible = false;

  if (statusFilter) statusFilter.value = "all";
  if (toggleDetailsBtn) toggleDetailsBtn.textContent = "View Field Details ->";
  if (detailsPanel) detailsPanel.classList.add("hidden");
  updateFieldSortButtonState();

  await refreshDashboardStats();

  if (dashboardStatsTimer) {
    clearInterval(dashboardStatsTimer);
  }
  dashboardStatsTimer = setInterval(refreshDashboardStats, 10000);
}

function boot() {
  if (isDashboardPage()) {
    attachDashboardHandlers();
    initializeDashboard();
    return;
  }

  if (isLandingPage()) {
    attachLandingHandlers();
    refreshLanding();
    setInterval(refreshLanding, 10000);
    return;
  }

  if (isSetupPage()) {
    attachSetupHandlers();
  }
}

boot();
