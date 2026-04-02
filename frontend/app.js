const els = {
  host: document.getElementById("host"),
  username: document.getElementById("username"),
  password: document.getElementById("password"),
  databaseSelect: document.getElementById("databaseSelect"),
  databaseSelectWrap: document.getElementById("databaseSelectWrap"),
  databaseManual: document.getElementById("databaseManual"),
  databaseManualWrap: document.getElementById("databaseManualWrap"),
  connectBtn: document.getElementById("connectBtn"),
  connectStatus: document.getElementById("connectStatus"),
  importSection: document.getElementById("importSection"),
  progressSection: document.getElementById("progressSection"),
  logsSection: document.getElementById("logsSection"),
  fileInput: document.getElementById("fileInput"),
  stopOnError: document.getElementById("stopOnError"),
  maxErrors: document.getElementById("maxErrors"),
  showQueryLogs: document.getElementById("showQueryLogs"),
  resume: document.getElementById("resume"),
  importBtn: document.getElementById("importBtn"),
  cancelBtn: document.getElementById("cancelBtn"),
  dropTablesBtn: document.getElementById("dropTablesBtn"),
  clearLogsBtn: document.getElementById("clearLogsBtn"),
  downloadLogsBtn: document.getElementById("downloadLogsBtn"),
  importLoader: document.getElementById("importLoader"),
  importLoaderText: document.getElementById("importLoaderText"),
  progressBar: document.getElementById("progressBar"),
  metaPercent: document.getElementById("metaPercent"),
  metaQueries: document.getElementById("metaQueries"),
  metaEta: document.getElementById("metaEta"),
  metaElapsed: document.getElementById("metaElapsed"),
  metaTotalEstimate: document.getElementById("metaTotalEstimate"),
  progressText: document.getElementById("progressText"),
  tableProgressList: document.getElementById("tableProgressList"),
  taskUpload: document.getElementById("taskUpload"),
  taskUploading: document.getElementById("taskUploading"),
  taskSource: document.getElementById("taskSource"),
  taskWorker: document.getElementById("taskWorker"),
  taskParse: document.getElementById("taskParse"),
  taskExecute: document.getElementById("taskExecute"),
  taskFinalize: document.getElementById("taskFinalize"),
  taskResult: document.getElementById("taskResult"),
  logPanel: document.getElementById("logPanel"),
  confirmModal: document.getElementById("confirmModal"),
  confirmImportBtn: document.getElementById("confirmImportBtn"),
  dismissModalBtn: document.getElementById("dismissModalBtn"),
  cancelConfirmModal: document.getElementById("cancelConfirmModal"),
  confirmCancelBtn: document.getElementById("confirmCancelBtn"),
  dismissCancelModalBtn: document.getElementById("dismissCancelModalBtn"),
  clearLogsConfirmModal: document.getElementById("clearLogsConfirmModal"),
  confirmClearLogsBtn: document.getElementById("confirmClearLogsBtn"),
  dismissClearLogsModalBtn: document.getElementById("dismissClearLogsModalBtn"),
  dropTablesConfirmModal: document.getElementById("dropTablesConfirmModal"),
  confirmDropTablesBtn: document.getElementById("confirmDropTablesBtn"),
  dismissDropTablesModalBtn: document.getElementById("dismissDropTablesModalBtn"),
  toastHost: document.getElementById("toastHost"),
};

let activeJobId = null;
let ws = null;
let isConnected = false;
let isImporting = false;
let isTaskLocked = false;
let isCancelRequested = false;
let latestState = null;
let latestLogs = [];
let processStartedAtMs = null;
const FORM_SETTINGS_KEY = "mysqlImporter.formSettings.v1";

function hasSelectedFile() {
  return Boolean(els.fileInput.files && els.fileInput.files.length > 0);
}

function updateFileActionState() {
  const fileReady = hasSelectedFile();
  els.importBtn.disabled = !fileReady || isTaskLocked || isImporting;
  // Keep cancel disabled unless import is actively running and a file exists.
  els.cancelBtn.disabled = !fileReady || !isImporting || !activeJobId;
}

function getFormSettings() {
  try {
    const raw = localStorage.getItem(FORM_SETTINGS_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (_) {
    return {};
  }
}

function saveFormSettings() {
  const previous = getFormSettings();
  const data = {
    host: els.host.value.trim(),
    username: els.username.value.trim(),
    // Password is intentionally not persisted for security.
    databaseManual: els.databaseManual.value.trim(),
    databaseSelected: els.databaseSelect.value.trim(),
    stopOnError: els.stopOnError.checked,
    maxErrors: els.maxErrors.value,
    showQueryLogs: els.showQueryLogs.checked,
    resume: els.resume.checked,
    connected: Boolean(previous.connected),
    cachedDatabases: Array.isArray(previous.cachedDatabases) ? previous.cachedDatabases : [],
  };
  try {
    localStorage.setItem(FORM_SETTINGS_KEY, JSON.stringify(data));
  } catch (_) {}
}

function applyFormSettings() {
  const data = getFormSettings();
  if (typeof data.host === "string") els.host.value = data.host;
  if (typeof data.username === "string") els.username.value = data.username;
  if (typeof data.databaseManual === "string") els.databaseManual.value = data.databaseManual;
  if (typeof data.maxErrors === "string") els.maxErrors.value = data.maxErrors;
  if (typeof data.stopOnError === "boolean") els.stopOnError.checked = data.stopOnError;
  if (typeof data.showQueryLogs === "boolean") els.showQueryLogs.checked = data.showQueryLogs;
  if (typeof data.resume === "boolean") els.resume.checked = data.resume;
}

function bindFormPersistence() {
  const fields = [
    els.host,
    els.username,
    els.databaseManual,
    els.databaseSelect,
    els.stopOnError,
    els.maxErrors,
    els.showQueryLogs,
    els.resume,
  ];
  fields.forEach((el) => {
    el.addEventListener("input", saveFormSettings);
    el.addEventListener("change", saveFormSettings);
  });
}

function dbName() {
  return els.databaseManual.value.trim() || els.databaseSelect.value.trim();
}

function setConnectedUI(connected) {
  isConnected = connected;
  els.databaseSelectWrap.classList.toggle("hidden", !connected);
  els.databaseManualWrap.classList.toggle("hidden", !connected);
  els.importSection.classList.toggle("hidden", !connected);
  els.progressSection.classList.toggle("hidden", !connected);
  els.logsSection.classList.toggle("hidden", !connected);
}

function ensureMonitoringVisible() {
  els.progressSection.classList.remove("hidden");
  els.logsSection.classList.remove("hidden");
}

function setImportLoading(loading, text = "Import is processing...") {
  isImporting = loading;
  els.importLoader.classList.toggle("hidden", !loading);
  els.importLoaderText.textContent = text;
  if (loading) {
    ensureMonitoringVisible();
  }
  els.connectBtn.disabled = loading || isTaskLocked;
  els.confirmImportBtn.disabled = loading || isTaskLocked;
  els.fileInput.disabled = loading || isTaskLocked;
  els.downloadLogsBtn.disabled = loading || !activeJobId;
  els.clearLogsBtn.disabled = loading || isTaskLocked;
  els.dropTablesBtn.disabled = loading || isTaskLocked;
  updateFileActionState();
}

function setActionLock(locked) {
  isTaskLocked = locked;
  els.connectBtn.disabled = locked;
  els.confirmImportBtn.disabled = locked;
  els.fileInput.disabled = locked;
  els.clearLogsBtn.disabled = locked;
  els.dropTablesBtn.disabled = locked;
  updateFileActionState();
}

function formatEta(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return "ETA n/a";
  const s = Math.max(0, Number(seconds));
  if (s < 60) return `ETA ${Math.round(s)}s`;
  const mins = Math.floor(s / 60);
  const rem = Math.round(s % 60);
  return `ETA ${mins}m ${rem}s`;
}

function formatDuration(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return "n/a";
  const total = Math.max(0, Math.round(Number(seconds)));
  const hrs = Math.floor(total / 3600);
  const mins = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hrs > 0) return `${hrs}h ${mins}m ${secs}s`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

function saveConnectionState(connected, databases = null) {
  const existing = getFormSettings();
  const next = {
    ...existing,
    connected: Boolean(connected),
    cachedDatabases: Array.isArray(databases)
      ? databases
      : Array.isArray(existing.cachedDatabases)
        ? existing.cachedDatabases
        : [],
  };
  try {
    localStorage.setItem(FORM_SETTINGS_KEY, JSON.stringify(next));
  } catch (_) {}
}

function renderDatabases(databases) {
  els.databaseSelect.innerHTML = `<option value="">-- Select --</option>`;
  databases.forEach((db) => {
    const opt = document.createElement("option");
    opt.value = db;
    opt.textContent = db;
    els.databaseSelect.appendChild(opt);
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function scrollToBottom(el) {
  if (!el) return;
  requestAnimationFrame(() => {
    el.scrollTop = el.scrollHeight;
  });
}

function scrollActiveTableIntoView() {
  const container = els.tableProgressList;
  if (!container) return;
  const activeRow = container.querySelector(".table-row.active");
  if (!activeRow) return;
  requestAnimationFrame(() => {
    activeRow.scrollIntoView({ block: "nearest", behavior: "smooth" });
  });
}

let toastTimer = null;
function showToast(message, type = "success") {
  if (!els.toastHost) {
    const host = document.createElement("div");
    host.id = "toastHost";
    host.className = "toast-host";
    host.setAttribute("aria-live", "polite");
    host.setAttribute("aria-atomic", "true");
    document.body.appendChild(host);
    els.toastHost = host;
  }
  if (toastTimer) {
    clearTimeout(toastTimer);
    toastTimer = null;
  }
  const safeType = ["success", "error", "info", "warning"].includes(type) ? type : "info";
  els.toastHost.innerHTML = `<div class="toast ${safeType}">${escapeHtml(message)}</div>`;
  els.toastHost.classList.add("show");
  toastTimer = setTimeout(() => {
    els.toastHost.classList.remove("show");
  }, 2800);
}

function renderTableProgress(meta) {
  const tables = Array.isArray(meta?.tables) ? meta.tables : [];
  if (!tables.length) {
    els.tableProgressList.innerHTML = `<div class="subtle">Waiting for table activity...</div>`;
    return;
  }

  const rows = tables.map((table) => {
    const status = table.status === "running" ? "active" : table.status;
    const safeStatus = ["pending", "active", "done", "error"].includes(status) ? status : "pending";
    const operation = table.last_operation ? `${table.last_operation}` : "QUERY";
    const counts = `ok=${table.executed || 0} failed=${table.failed || 0}`;
    let durationText = "duration=n/a";
    if (table.duration_seconds != null) {
      durationText = `duration=${formatDuration(table.duration_seconds)}`;
    } else if (table.started_at && safeStatus === "active") {
      durationText = `elapsed=${formatDuration((Date.now() / 1000) - Number(table.started_at))}`;
    }
    return `
      <div class="table-row ${safeStatus}">
        <div class="table-row-head">
          <span class="table-row-name">${escapeHtml(table.name)}</span>
          <span class="table-row-meta">${escapeHtml(operation)} | ${escapeHtml(counts)} | ${escapeHtml(durationText)}</span>
        </div>
        <div class="table-row-bar"><div class="table-row-bar-inner"></div></div>
      </div>
    `;
  });
  els.tableProgressList.innerHTML = rows.join("");
  scrollActiveTableIntoView();
}

function setTaskState(el, state, text) {
  el.classList.remove("pending", "active", "done", "error");
  el.classList.add(state);
  el.classList.remove("manual-progress");
  const bar = el.querySelector(".task-progress-bar");
  if (bar) bar.style.width = "";
  if (text) {
    const label = el.querySelector(".task-label");
    if (label) label.textContent = text;
  }
}

function setTaskProgress(el, percent) {
  const bar = el.querySelector(".task-progress-bar");
  if (!bar) return;
  const clamped = Math.max(0, Math.min(100, Number(percent) || 0));
  el.classList.add("manual-progress");
  bar.style.width = `${clamped}%`;
}

function resetChecklist() {
  setTaskState(els.taskUploading, "pending", "Uploading dump file");
  setTaskState(els.taskUpload, "pending", "Upload stored on server");
  setTaskState(els.taskSource, "pending", "SQL source prepared");
  setTaskState(els.taskWorker, "pending", "Worker started");
  setTaskState(els.taskParse, "pending", "Parsing SQL statements");
  setTaskState(els.taskExecute, "pending", "Executing queries in batches");
  setTaskState(els.taskFinalize, "pending", "Finalizing import");
  setTaskState(els.taskResult, "pending", "Waiting for result");
}

function updateChecklist(state, logs) {
  if (!state) return;
  if (isCancelRequested && ["queued", "running"].includes(state.status)) {
    setTaskState(els.taskExecute, "error", `Cancellation requested (${state.executed_queries || 0} completed)`);
    setTaskState(els.taskFinalize, "error", "Stopping process...");
    setTaskState(els.taskResult, "error", "Cancelling import");
    return;
  }
  const logsLower = (logs || []).join("\n").toLowerCase();
  const has = (text) => logsLower.includes(text);

  if (has("upload stored")) {
    setTaskState(els.taskUploading, "done", "Uploading dump file");
    setTaskState(els.taskUpload, "done", "Upload stored on server");
  }
  if (has("using sql source")) setTaskState(els.taskSource, "done", "SQL source prepared");
  if (state.status === "queued") {
    setTaskState(els.taskWorker, "active", "Worker waiting to start");
  }
  if (state.status === "running") {
    setTaskState(els.taskWorker, "done", "Worker started");
    if (state.parsed_queries > 0) {
      setTaskState(els.taskParse, "done", `Parsing SQL statements (${state.parsed_queries})`);
      setTaskState(els.taskExecute, "active", `Executing queries (${state.executed_queries})`);
    } else {
      setTaskState(els.taskParse, "active", "Parsing SQL statements");
    }
  }
  if (state.status === "completed") {
    setTaskState(els.taskUpload, "done", "Upload received");
    setTaskState(els.taskSource, "done", "SQL source prepared");
    setTaskState(els.taskWorker, "done", "Worker started");
    setTaskState(els.taskParse, "done", `Parsing complete (${state.parsed_queries})`);
    setTaskState(els.taskExecute, "done", `Execution complete (${state.executed_queries})`);
    setTaskState(els.taskFinalize, "done", "Finalization complete");
    setTaskState(els.taskResult, "done", "Import completed");
    return;
  }
  if (state.status === "failed") {
    setTaskState(els.taskExecute, "error", `Execution stopped (${state.executed_queries || 0} completed)`);
    setTaskState(els.taskFinalize, "error", "Finalization failed");
    setTaskState(els.taskResult, "error", "Import failed");
    return;
  }
  if (state.status === "cancelled") {
    setTaskState(els.taskExecute, "error", `Execution cancelled (${state.executed_queries || 0} completed)`);
    setTaskState(els.taskFinalize, "error", "Cancelled during import");
    setTaskState(els.taskResult, "error", "Import cancelled");
    return;
  }
  if (state.status === "running" && state.percent >= 95) {
    setTaskState(els.taskFinalize, "active", "Finalizing import");
  }
}

function setProgress(state) {
  latestState = state;
  const pct = Number(state.percent || 0).toFixed(2);
  els.progressBar.style.width = `${pct}%`;
  const eta = formatEta(state.eta_seconds);
  const q = `parsed=${state.parsed_queries} executed=${state.executed_queries} failed=${state.failed_queries} skipped=${state.skipped_queries}`;
  const completedTables = Number(state.meta?.completed_tables || 0);
  const totalTables = Number(state.meta?.table_total || 0);
  els.metaPercent.textContent = `${pct}%`;
  els.metaQueries.textContent = totalTables > 0 ? `${q} | tables=${completedTables}/${totalTables}` : q;
  els.metaEta.textContent = eta;
  const elapsedSec = processStartedAtMs ? (Date.now() - processStartedAtMs) / 1000 : null;
  els.metaElapsed.textContent = `Elapsed ${formatDuration(elapsedSec)}`;
  if (elapsedSec != null && state.eta_seconds != null) {
    els.metaTotalEstimate.textContent = `Total est ${formatDuration(elapsedSec + Number(state.eta_seconds || 0))}`;
  } else {
    els.metaTotalEstimate.textContent = "Total est n/a";
  }
  els.progressText.textContent = `${state.status} | ${pct}% | ${eta}`;
  if (["queued", "running"].includes(state.status)) {
    ensureMonitoringVisible();
    if (isCancelRequested) {
      setImportLoading(false, "Cancelling import...");
      setActionLock(false);
    } else {
      setActionLock(true);
      setImportLoading(true, "Import is processing...");
    }
  } else if (["completed", "failed", "cancelled"].includes(state.status)) {
    isCancelRequested = false;
    setActionLock(false);
    setImportLoading(false, `Import ${state.status}.`);
  }
  renderTableProgress(state.meta || {});
  updateChecklist(latestState, latestLogs);
}

function setLogs(logs) {
  latestLogs = logs || [];
  if (isImporting) {
    ensureMonitoringVisible();
  }
  const html = latestLogs
    .map((line) => {
      const lower = line.toLowerCase();
      let cls = "info";
      if (lower.includes("[success]")) cls = "success";
      if (lower.includes("[warning]") || lower.includes("[warn]")) cls = "warning";
      if (lower.includes("[error]")) cls = "error";
      return `<div class="log-line ${cls}">${escapeHtml(line)}</div>`;
    })
    .join("");
  els.logPanel.innerHTML = html;
  scrollToBottom(els.logPanel);
  updateChecklist(latestState, latestLogs);
}

async function jsonRequest(url, method = "GET", body = null) {
  const options = { method, headers: {} };
  if (body) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }
  const res = await fetch(url, options);
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const payload = await res.json();
      detail = payload.detail || detail;
    } catch (_) {}
    throw new Error(detail);
  }
  return res.json();
}

async function connectAndLoadDatabases() {
  const creds = {
    host: els.host.value.trim(),
    username: els.username.value.trim(),
    password: els.password.value,
  };
  const connectResp = await jsonRequest("/api/imports/connect", "POST", creds);
  if (connectResp.resolved_host) {
    const resolvedHost = connectResp.resolved_port
      ? `${connectResp.resolved_host}:${connectResp.resolved_port}`
      : `${connectResp.resolved_host}`;
    els.host.value = resolvedHost;
    creds.host = resolvedHost;
  }
  const dbsResp = await jsonRequest("/api/imports/databases", "POST", creds);
  if (dbsResp.resolved_host) {
    els.host.value = dbsResp.resolved_port
      ? `${dbsResp.resolved_host}:${dbsResp.resolved_port}`
      : `${dbsResp.resolved_host}`;
  }
  renderDatabases(dbsResp.databases);

  const persisted = getFormSettings();
  if (persisted.databaseSelected && dbsResp.databases.includes(persisted.databaseSelected)) {
    els.databaseSelect.value = persisted.databaseSelected;
  }
  saveFormSettings();
  saveConnectionState(true, dbsResp.databases);
}

async function uploadFile() {
  const file = els.fileInput.files?.[0];
  if (!file) {
    throw new Error("Please select a .sql or .zip file.");
  }
  return new Promise((resolve, reject) => {
    const fd = new FormData();
    fd.append("file", file);
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/imports/upload");

    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable) return;
      const pct = (event.loaded / event.total) * 100;
      setTaskState(els.taskUploading, "active", `Uploading dump file (${pct.toFixed(1)}%)`);
      setTaskProgress(els.taskUploading, pct);
      if (processStartedAtMs) {
        const elapsedSec = (Date.now() - processStartedAtMs) / 1000;
        const uploadTotalSec = pct > 0 ? elapsedSec / (pct / 100) : null;
        const uploadRemain = uploadTotalSec != null ? Math.max(uploadTotalSec - elapsedSec, 0) : null;
        els.metaElapsed.textContent = `Elapsed ${formatDuration(elapsedSec)}`;
        els.metaEta.textContent = uploadRemain != null ? `ETA ${formatDuration(uploadRemain)}` : "ETA n/a";
        els.metaTotalEstimate.textContent =
          uploadTotalSec != null ? `Total est ${formatDuration(uploadTotalSec)}` : "Total est n/a";
      }
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText));
        } catch (_) {
          reject(new Error("Upload response could not be parsed."));
        }
      } else {
        let detail = "Upload failed.";
        try {
          detail = JSON.parse(xhr.responseText).detail || detail;
        } catch (_) {}
        reject(new Error(detail));
      }
    };

    xhr.onerror = () => reject(new Error("Upload failed due to network error."));
    xhr.send(fd);
  });
}

function openWebSocket(jobId) {
  if (ws) ws.close();
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(`${proto}//${window.location.host}/ws/imports/${jobId}`);
  ws.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    setProgress(payload.state);
    setLogs(payload.logs || []);
  };
}

async function pollStatus(jobId) {
  try {
    const data = await jsonRequest(`/api/imports/${jobId}/status`);
    setProgress(data.state);
    setLogs(data.logs || []);
    if (!["completed", "failed", "cancelled"].includes(data.state.status)) {
      setTimeout(() => pollStatus(jobId), 1500);
    }
  } catch (_) {
    setTimeout(() => pollStatus(jobId), 2000);
  }
}

function resetRuntimeImportView() {
  if (ws) {
    try {
      ws.close();
    } catch (_) {}
  }
  ws = null;
  activeJobId = null;
  isImporting = false;
  isTaskLocked = false;
  isCancelRequested = false;
  latestState = null;
  latestLogs = [];
  processStartedAtMs = null;
  setImportLoading(false, "Import state cleared.");
  setActionLock(false);
  els.progressBar.style.width = "0%";
  els.progressText.textContent = "No active import.";
  els.metaPercent.textContent = "0.00%";
  els.metaQueries.textContent = "parsed=0 executed=0 failed=0 skipped=0";
  els.metaEta.textContent = "ETA n/a";
  els.metaElapsed.textContent = "Elapsed n/a";
  els.metaTotalEstimate.textContent = "Total est n/a";
  els.logPanel.innerHTML = "";
  resetChecklist();
  renderTableProgress({});
  updateFileActionState();
}

els.connectBtn.addEventListener("click", async () => {
  els.connectStatus.textContent = "Connecting...";
  setConnectedUI(false);
  try {
    await connectAndLoadDatabases();
    els.connectStatus.textContent = "Connected. Databases loaded.";
    setConnectedUI(true);
    saveConnectionState(true);
  } catch (err) {
    els.connectStatus.textContent = `Connection error: ${err.message}`;
    setConnectedUI(false);
    saveConnectionState(false, []);
  }
});

els.importBtn.addEventListener("click", () => {
  if (!isConnected) {
    els.progressText.textContent = "Connect first to unlock import.";
    return;
  }
  els.confirmModal.showModal();
});

els.dismissModalBtn.addEventListener("click", () => {
  els.confirmModal.close();
});

els.dismissCancelModalBtn.addEventListener("click", () => {
  els.cancelConfirmModal.close();
});

els.dismissClearLogsModalBtn.addEventListener("click", () => {
  els.clearLogsConfirmModal.close();
});

els.dismissDropTablesModalBtn.addEventListener("click", () => {
  els.dropTablesConfirmModal.close();
});

els.confirmImportBtn.addEventListener("click", async () => {
  els.confirmModal.close();
  isCancelRequested = false;
  processStartedAtMs = Date.now();
  let uploadFinished = false;
  let startRequested = false;
  setActionLock(true);
  els.progressText.textContent = "Preparing upload and starting import...";
  els.logPanel.innerHTML = "";
  resetChecklist();
  els.metaPercent.textContent = "0.00%";
  els.metaQueries.textContent = "parsed=0 executed=0 failed=0 skipped=0";
  els.metaEta.textContent = "ETA n/a";
  els.metaElapsed.textContent = "Elapsed 0s";
  els.metaTotalEstimate.textContent = "Total est n/a";
  els.progressBar.style.width = "0%";
  renderTableProgress({});
  try {
    const database = dbName();
    if (!database) throw new Error("Database name is required.");
    if (els.resume.checked) {
      ensureMonitoringVisible();
      els.progressText.textContent = "Resume mode: rebuilding state and replaying last completed table...";
    }

    setTaskState(els.taskUploading, "active", "Uploading dump file (0.0%)");
    setTaskProgress(els.taskUploading, 1);
    const upload = await uploadFile();
    uploadFinished = true;
    setTaskState(els.taskUploading, "done", "Uploading dump file");
    setTaskState(els.taskUpload, "done", "Upload stored on server");

    activeJobId = upload.job_id;
    setTaskState(els.taskSource, "active", "Preparing SQL source");
    const payload = {
      job_id: activeJobId,
      host: els.host.value.trim(),
      username: els.username.value.trim(),
      password: els.password.value,
      database,
      stop_on_error: els.stopOnError.checked,
      max_errors: Number(els.maxErrors.value || 100),
      show_query_logs: els.showQueryLogs.checked,
      resume: els.resume.checked,
    };
    startRequested = true;
    const startResp = await jsonRequest("/api/imports/start", "POST", payload);
    setTaskState(els.taskSource, "done", "SQL source prepared");
    setTaskState(els.taskWorker, "active", "Worker started");
    const mode = startResp.execution_mode ? ` (${startResp.execution_mode})` : "";
    els.progressText.textContent = `Import queued${mode}. Waiting for progress updates...`;
    setImportLoading(true, "Import is processing...");
    openWebSocket(activeJobId);
    pollStatus(activeJobId);
  } catch (err) {
    setImportLoading(false, "Import did not start.");
    if (!uploadFinished) {
      setTaskState(els.taskUploading, "error", "Uploading dump file failed");
    } else if (!startRequested) {
      setTaskState(els.taskSource, "error", "Preparing SQL source failed");
    } else {
      setTaskState(els.taskWorker, "error", "Worker could not be started");
      setTaskState(els.taskResult, "error", "Import failed to start");
    }
    els.progressText.textContent = `Start error: ${err.message}`;
    setActionLock(false);
  }
});

els.cancelBtn.addEventListener("click", () => {
  if (!activeJobId) return;
  els.cancelConfirmModal.showModal();
});

els.clearLogsBtn.addEventListener("click", () => {
  els.clearLogsConfirmModal.showModal();
});

els.dropTablesBtn.addEventListener("click", () => {
  if (!isConnected) {
    showToast("Connect first before dropping tables.", "warning");
    return;
  }
  const database = dbName();
  if (!database) {
    showToast("Select or enter a database first.", "warning");
    return;
  }
  els.dropTablesConfirmModal.showModal();
});

els.confirmCancelBtn.addEventListener("click", async () => {
  els.cancelConfirmModal.close();
  if (!activeJobId) return;
  try {
    isCancelRequested = true;
    setImportLoading(false, "Cancelling import...");
    setActionLock(false);
    setTaskState(els.taskExecute, "error", "Cancellation requested");
    setTaskState(els.taskFinalize, "error", "Stopping process...");
    setTaskState(els.taskResult, "error", "Cancelling import");
    els.progressText.textContent = "Cancellation requested. Stopping process...";
    await jsonRequest(`/api/imports/${activeJobId}/cancel`, "POST");
    showToast("Cancellation requested. Stopping import...", "warning");
  } catch (err) {
    isCancelRequested = false;
    setImportLoading(false, "Cancel failed.");
    els.progressText.textContent = `Cancel error: ${err.message}`;
    showToast(`Cancel failed: ${err.message}`, "error");
  }
});

els.confirmClearLogsBtn.addEventListener("click", async () => {
  els.clearLogsConfirmModal.close();
  try {
    await jsonRequest("/api/imports/clear", "POST");
    resetRuntimeImportView();
    els.connectStatus.textContent = "Import files/logs cleared. Database settings preserved.";
    showToast("Logs and temporary import files cleared.", "success");
  } catch (err) {
    els.progressText.textContent = `Clear logs error: ${err.message}`;
    showToast(`Clear logs failed: ${err.message}`, "error");
  }
});

els.confirmDropTablesBtn.addEventListener("click", async () => {
  els.dropTablesConfirmModal.close();
  try {
    const database = dbName();
    if (!database) throw new Error("Database name is required.");
    const payload = {
      host: els.host.value.trim(),
      username: els.username.value.trim(),
      password: els.password.value,
      database,
    };
    const result = await jsonRequest("/api/imports/drop-tables", "POST", payload);
    const dropped = Number(result?.dropped_count || 0);
    showToast(`Dropped ${dropped} table(s) from ${database}.`, "success");
    els.progressText.textContent = `Dropped ${dropped} table(s) from ${database}.`;
  } catch (err) {
    showToast(`Drop tables failed: ${err.message}`, "error");
    els.progressText.textContent = `Drop tables error: ${err.message}`;
  }
});

els.downloadLogsBtn.addEventListener("click", () => {
  if (!activeJobId) return;
  window.location.href = `/api/imports/${activeJobId}/logs/download`;
});

els.fileInput.addEventListener("change", updateFileActionState);

applyFormSettings();
bindFormPersistence();
const persisted = getFormSettings();
if (persisted.connected) {
  if (Array.isArray(persisted.cachedDatabases)) {
    renderDatabases(persisted.cachedDatabases);
    if (
      persisted.databaseSelected &&
      persisted.cachedDatabases.includes(persisted.databaseSelected)
    ) {
      els.databaseSelect.value = persisted.databaseSelected;
    }
  }
  setConnectedUI(true);
  els.connectStatus.textContent = "Connection restored from saved state.";
} else {
  setConnectedUI(false);
}
setImportLoading(false);
resetChecklist();
renderTableProgress({});
updateFileActionState();
