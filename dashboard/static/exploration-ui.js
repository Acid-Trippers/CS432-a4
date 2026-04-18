/**
 * Session Exploration UI Module
 * Handles rendering and interactions for:
 * 1. Entities list
 * 2. Query history 
 * 3. Query result details
 * 4. Entity schema & instances
 */

let explorationState = {
  sessionId: null,
  currentPage: 0,
  historyPageSize: 20,
  instancesPageSize: 20,
  currentEntity: null,
  currentInstancesPage: 0,
};

function getSessionId() {
  return sessionStorage.getItem("dashboard_session_id") || "";
}

// ============================================================================
// FEATURE 1: Load and Display Entities List
// ============================================================================

async function loadEntities() {
  const sessionId = getSessionId();
  if (!sessionId) return;

  const loadingEl = document.getElementById("entities-loading");
  const listEl = document.getElementById("entities-list");
  const emptyEl = document.getElementById("entities-empty");
  const errorEl = document.getElementById("entities-error");
  const tbodyEl = document.getElementById("entities-tbody");

  try {
    loadingEl.classList.remove("hidden");
    listEl.classList.add("hidden");
    emptyEl.classList.add("hidden");
    errorEl.classList.add("hidden");

    const response = await fetch(`/api/sessions/${sessionId}/entities`, {
      headers: { "X-Session-ID": sessionId },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    const entities = data.entities || [];

    loadingEl.classList.add("hidden");

    if (entities.length === 0) {
      emptyEl.classList.remove("hidden");
      return;
    }

    tbodyEl.innerHTML = entities
      .map(
        (entity) => `
        <tr>
          <td><strong>${escapeHtml(entity.entity_name)}</strong></td>
          <td>${entity.query_count}</td>
          <td>${(entity.operations || []).join(", ")}</td>
          <td class="meta-text">${formatTimestamp(entity.last_queried)}</td>
          <td class="action-cell">
            <button class="btn btn-sm" onclick="openEntityDetails('${escapeHtml(entity.entity_name)}')">View</button>
          </td>
        </tr>
      `
      )
      .join("");

    listEl.classList.remove("hidden");
  } catch (error) {
    loadingEl.classList.add("hidden");
    errorEl.classList.remove("hidden");
    errorEl.textContent = `Error loading entities: ${error.message}`;
  }
}

// ============================================================================
// FEATURE 5: Load and Display Query History
// ============================================================================

async function loadQueryHistory(page = 0) {
  const sessionId = getSessionId();
  if (!sessionId) return;

  const operation = document.getElementById("history-operation-filter").value;
  const status = document.getElementById("history-status-filter").value;

  const loadingEl = document.getElementById("history-loading");
  const listEl = document.getElementById("history-list");
  const emptyEl = document.getElementById("history-empty");
  const errorEl = document.getElementById("history-error");
  const tbodyEl = document.getElementById("history-tbody");

  try {
    loadingEl.classList.remove("hidden");
    listEl.classList.add("hidden");
    emptyEl.classList.add("hidden");
    errorEl.classList.add("hidden");

    const params = new URLSearchParams({
      limit: explorationState.historyPageSize,
      offset: page * explorationState.historyPageSize,
    });
    if (operation) params.append("operation", operation);
    if (status) params.append("status", status);

    const response = await fetch(`/api/sessions/${sessionId}/query-history?${params}`, {
      headers: { "X-Session-ID": sessionId },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    const queries = data.queries || [];

    loadingEl.classList.add("hidden");

    if (queries.length === 0) {
      emptyEl.classList.remove("hidden");
      return;
    }

    explorationState.currentPage = page;

    tbodyEl.innerHTML = queries
      .map(
        (query) => `
        <tr>
          <td class="meta-text">${formatTimestamp(query.timestamp)}</td>
          <td><strong>${query.operation}</strong></td>
          <td>${escapeHtml(query.entity || "—")}</td>
          <td><span class="status-badge ${query.status === "success" ? "good" : "error"}">${query.status}</span></td>
          <td>${query.row_count !== null ? query.row_count : "—"}</td>
          <td>${query.execution_time_ms ? query.execution_time_ms.toFixed(1) : "—"}</td>
          <td class="action-cell">
            <button class="btn btn-sm" onclick="openQueryResult('${escapeHtml(query.query_id)}')">View</button>
          </td>
        </tr>
      `
      )
      .join("");

    const pageInfo = document.getElementById("history-page-info");
    pageInfo.textContent = `Page ${page + 1} (showing ${queries.length}/${data.total} total)`;

    document.getElementById("btn-history-prev").disabled = page === 0;
    document.getElementById("btn-history-next").disabled = queries.length < explorationState.historyPageSize;

    listEl.classList.remove("hidden");
  } catch (error) {
    loadingEl.classList.add("hidden");
    errorEl.classList.remove("hidden");
    errorEl.textContent = `Error loading query history: ${error.message}`;
  }
}

// ============================================================================
// FEATURE 4: Open and Display Query Result
// ============================================================================

async function openQueryResult(queryId) {
  const sessionId = getSessionId();
  if (!sessionId) return;

  try {
    const response = await fetch(`/api/sessions/${sessionId}/query-results/${queryId}`, {
      headers: { "X-Session-ID": sessionId },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();

    // Populate metadata
    document.getElementById("result-query-id").textContent = queryId;
    document.getElementById("result-timestamp").textContent = formatTimestamp(data.timestamp);
    document.getElementById("result-operation").textContent = data.payload.operation || "—";
    document.getElementById("result-entity").textContent = escapeHtml(data.payload.entity || "—");
    document.getElementById("result-status").textContent = data.status || "—";

    // Populate payload
    document.getElementById("result-payload-json").textContent = JSON.stringify(
      data.payload,
      null,
      2
    );

    // Populate result data if READ operation
    const dataSection = document.getElementById("result-data-section");
    if (data.result?.operation === "READ" && data.result?.data) {
      dataSection.classList.remove("hidden");
      const tableEl = document.getElementById("result-data-table");
      
      // Handle both dict (keyed by record_id) and array formats
      let dataToRender = data.result.data;
      if (typeof dataToRender === "object" && !Array.isArray(dataToRender)) {
        // Convert dict to array
        dataToRender = Object.values(dataToRender);
      }
      
      tableEl.innerHTML = renderDataTable(dataToRender);
    } else {
      dataSection.classList.add("hidden");
    }

    // Show modal
    document.getElementById("query-result-modal").showModal();
  } catch (error) {
    alert(`Error loading query result: ${error.message}`);
  }
}

// ============================================================================
// FEATURE 2 & 3: Open Entity Details (Schema + Instances)
// ============================================================================

async function openEntityDetails(entityName) {
  explorationState.currentEntity = entityName;
  explorationState.currentInstancesPage = 0;
  document.getElementById("entity-name-header").textContent = escapeHtml(entityName);

  // Reset to Schema tab and clear any previous instances state
  const modal = document.getElementById("entity-details-modal");
  modal.querySelectorAll(".tab-button").forEach((b) => b.classList.remove("active"));
  modal.querySelectorAll(".tab-content").forEach((t) => t.classList.add("hidden"));
  modal.querySelector('[data-tab="schema"]')?.classList.add("active");
  document.getElementById("tab-schema")?.classList.remove("hidden");
  document.getElementById("instances-data")?.classList.add("hidden");
  document.getElementById("instances-empty")?.classList.add("hidden");
  document.getElementById("instances-loading")?.classList.add("hidden");

  // Load schema then show modal
  await loadEntitySchema(entityName);
  modal.showModal();
}

async function loadEntitySchema(entityName) {
  const sessionId = getSessionId();
  if (!sessionId) return;

  try {
    const response = await fetch(
      `/api/sessions/${sessionId}/entities/${encodeURIComponent(entityName)}/schema?include_samples=true&include_stats=true`,
      { headers: { "X-Session-ID": sessionId } }
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    const fields = data.fields || [];

    const tbodyEl = document.getElementById("schema-tbody");
    tbodyEl.innerHTML = fields
      .map(
        (field) => `
        <tr>
          <td><strong>${escapeHtml(field.field_name)}</strong></td>
          <td class="meta-text">${field.dominant_type}</td>
          <td><span class="routing-badge ${field.routing?.toLowerCase()}">${field.routing}</span></td>
          <td>${(field.frequency * 100).toFixed(1)}%</td>
          <td>${field.cardinality ? field.cardinality.toFixed(3) : "—"}</td>
          <td>${field.type_stability ? (field.type_stability * 100).toFixed(0) + "%" : "—"}</td>
        </tr>
      `
      )
      .join("");
  } catch (error) {
    alert(`Error loading schema: ${error.message}`);
  }
}

async function loadEntityInstances(entityName) {
  const sessionId = getSessionId();
  console.log("[Instances] Loading instances for:", entityName, "sessionId:", sessionId);
  if (!sessionId) {
    console.error("[Instances] No session ID found");
    return;
  }

  const limit = parseInt(document.getElementById("instances-limit").value, 10);
  const source = document.getElementById("instances-source").value;
  console.log("[Instances] Limit:", limit, "Source:", source, "Page:", explorationState.currentInstancesPage);

  const loadingEl = document.getElementById("instances-loading");
  const dataEl = document.getElementById("instances-data");
  const emptyEl = document.getElementById("instances-empty");

  try {
    loadingEl.classList.remove("hidden");
    dataEl.classList.add("hidden");
    emptyEl.classList.add("hidden");

    const offset = explorationState.currentInstancesPage * limit;
    const url = `/api/sessions/${sessionId}/entities/${encodeURIComponent(entityName)}/instances?limit=${limit}&offset=${offset}&from_source=${source}`;
    
    console.log("[Instances] Fetching from URL:", url);

    const response = await fetch(url, { headers: { "X-Session-ID": sessionId } });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log("[Instances] Response data:", data);
    
    const instances = data.instances || [];
    console.log("[Instances] Got", instances.length, "instances from source:", data.source);

    loadingEl.classList.add("hidden");

    if (instances.length === 0) {
      console.log("[Instances] No instances found, showing empty message");
      emptyEl.classList.remove("hidden");
      return;
    }

    const tableEl = document.getElementById("instances-table");
    tableEl.innerHTML = renderDataTable(instances);

    const pageInfo = document.getElementById("instances-page-info");
    pageInfo.textContent = `Page ${explorationState.currentInstancesPage + 1} (showing ${instances.length}/${data.total_count} total)`;

    document.getElementById("btn-instances-prev").disabled = explorationState.currentInstancesPage === 0;
    document.getElementById("btn-instances-next").disabled = instances.length < limit;

    dataEl.classList.remove("hidden");
    console.log("[Instances] Successfully loaded and rendered instances");
  } catch (error) {
    loadingEl.classList.add("hidden");
    console.error("[Instances] Error loading instances:", error);
    alert(`Error loading instances: ${error.message}`);
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

function renderDataTable(data) {
  if (!Array.isArray(data) || data.length === 0) {
    return "<p class='meta-text'>No data to display</p>";
  }

  // Extract all unique keys
  const allKeys = new Set();
  data.forEach((row) => {
    if (typeof row === "object" && row !== null) {
      Object.keys(row).forEach((k) => allKeys.add(k));
    }
  });
  const keys = Array.from(allKeys);

  const html = `
    <div class="result-table-wrap">
      <table class="result-table">
        <thead>
          <tr>
            ${keys.map((k) => `<th>${escapeHtml(k)}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${data
            .map(
              (row) =>
                `<tr>${keys
                  .map((k) => {
                    const val = row[k];
                    const display =
                      val === null ? "—" : typeof val === "object" ? JSON.stringify(val) : String(val);
                    return `<td>${escapeHtml(display)}</td>`;
                  })
                  .join("")}</tr>`
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
  return html;
}

function formatTimestamp(iso) {
  if (!iso) return "—";
  const date = new Date(iso);
  return date.toLocaleString();
}

function escapeHtml(text) {
  if (typeof text !== "string") return String(text);
  const map = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  };
  return text.replace(/[&<>"']/g, (m) => map[m]);
}

// ============================================================================
// Event Listeners
// ============================================================================

document.addEventListener("DOMContentLoaded", () => {
  // Load initial data
  loadEntities();
  loadQueryHistory(0);

  // Refresh buttons
  document.getElementById("btn-refresh-entities")?.addEventListener("click", () => {
    loadEntities();
  });

  document.getElementById("btn-refresh-history")?.addEventListener("click", () => {
    explorationState.currentPage = 0;
    loadQueryHistory(0);
  });

  // History filters
  document.getElementById("history-operation-filter")?.addEventListener("change", () => {
    explorationState.currentPage = 0;
    loadQueryHistory(0);
  });

  document.getElementById("history-status-filter")?.addEventListener("change", () => {
    explorationState.currentPage = 0;
    loadQueryHistory(0);
  });

  // History pagination
  document.getElementById("btn-history-prev")?.addEventListener("click", () => {
    if (explorationState.currentPage > 0) {
      loadQueryHistory(explorationState.currentPage - 1);
    }
  });

  document.getElementById("btn-history-next")?.addEventListener("click", () => {
    loadQueryHistory(explorationState.currentPage + 1);
  });

  // Query result modal close
  document.getElementById("btn-result-close")?.addEventListener("click", () => {
    document.getElementById("query-result-modal").close();
  });

  // Entity details modal - tab switching
  // Tab buttons are inside a <dialog> so we use event delegation on the document
  // to ensure clicks are caught even after the dialog is shown.
  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".tab-button");
    if (!btn) return;

    const tabName = btn.dataset.tab;

    // Deactivate all tabs within the same modal
    const modal = btn.closest("dialog");
    if (!modal) return;
    modal.querySelectorAll(".tab-button").forEach((b) => b.classList.remove("active"));
    modal.querySelectorAll(".tab-content").forEach((t) => t.classList.add("hidden"));

    // Activate clicked tab
    btn.classList.add("active");
    modal.querySelector(`#tab-${tabName}`)?.classList.remove("hidden");

    // Auto-load instances when switching to the instances tab
    if (tabName === "instances" && explorationState.currentEntity) {
      explorationState.currentInstancesPage = 0;
      loadEntityInstances(explorationState.currentEntity);
    }
  });

  // Load instances button (manual re-fetch / source change)
  document.getElementById("btn-load-instances")?.addEventListener("click", () => {
    if (explorationState.currentEntity) {
      explorationState.currentInstancesPage = 0;
      loadEntityInstances(explorationState.currentEntity);
    }
  });

  // Instances pagination
  document.getElementById("btn-instances-prev")?.addEventListener("click", () => {
    if (explorationState.currentInstancesPage > 0) {
      explorationState.currentInstancesPage--;
      if (explorationState.currentEntity) {
        loadEntityInstances(explorationState.currentEntity);
      }
    }
  });

  document.getElementById("btn-instances-next")?.addEventListener("click", () => {
    explorationState.currentInstancesPage++;
    if (explorationState.currentEntity) {
      loadEntityInstances(explorationState.currentEntity);
    }
  });

  // Re-fetch when limit or source changes while instances tab is visible
  document.getElementById("instances-limit")?.addEventListener("change", () => {
    const instancesTab = document.getElementById("tab-instances");
    if (instancesTab && !instancesTab.classList.contains("hidden") && explorationState.currentEntity) {
      explorationState.currentInstancesPage = 0;
      loadEntityInstances(explorationState.currentEntity);
    }
  });

  document.getElementById("instances-source")?.addEventListener("change", () => {
    const instancesTab = document.getElementById("tab-instances");
    if (instancesTab && !instancesTab.classList.contains("hidden") && explorationState.currentEntity) {
      explorationState.currentInstancesPage = 0;
      loadEntityInstances(explorationState.currentEntity);
    }
  });

  // Entity details modal close
  document.getElementById("btn-entity-close")?.addEventListener("click", () => {
    document.getElementById("entity-details-modal").close();
  });
});