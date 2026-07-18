/* ─── Helpers ─── */
const $ = (id) => document.getElementById(id);

const state = {
  runId: null,
  startTime: null,
  pollTimer: null,
  durationTimer: null,
  elapsedSec: 0,
  lastOutputLen: 0,
  artifacts: [],
  currentPreviewPath: null,
};

const AGENTS = [
  { key: "refinement",       idx: 1, title: "Iterative Refinement via Prompts",           icon: "🔄" },
  { key: "source_discovery", idx: 2, title: "Schema Discovery & Extraction",       icon: "🔎" },
  { key: "schema_drift",     idx: 3, title: "Data & Schema Migration Assistant",        icon: "⚙️" },
  { key: "transformation",   idx: 4, title: "Transformation Notebooks / Scripts",        icon: "🔁" },
  { key: "trust_quality",    idx: 5, title: "AI Suggestions & Code Scaffolding",  icon: "💡" },
  { key: "deployment",       idx: 6, title: "Deployment + CI/CD Auto-Gen (and infra)",         icon: "☁️" },
];
const AGENT_BY_KEY = Object.fromEntries(AGENTS.map(a => [a.key, a]));

/* ─── Status / Badge ─── */
function setStatus(kind, text) {
  const pill = $("statusPill");
  const dot  = pill.querySelector(".dot");
  dot.classList.remove("dot--green", "dot--blue");
  dot.classList.add(kind === "ok" ? "dot--green" : "dot--blue");
  $("statusText").textContent = text;
}
function setBadge(kind, text) {
  const b = $("rtBadge");
  b.className = "badge " + ({ ok:"badge--green", warn:"badge--amber", bad:"badge--danger" }[kind] || "badge--blue");
  b.textContent = text;
}

/* ─── Progress ─── */
function setProgress(pct) {
  pct = Math.max(0, Math.min(100, Math.round(pct)));
  $("progressFill").style.width = pct + "%";
  $("progressPct").textContent  = pct + "%";
  $("radial").style.setProperty("--pct", pct);
  $("radialPct").textContent    = pct + "%";
}

/* ─── Log ─── */
function pushLog(line) {
  const box = $("logBox");
  box.textContent += (box.textContent ? "\n" : "") + line;
  box.scrollTop = box.scrollHeight;
}
function clearLog() { $("logBox").textContent = ""; }

/* ─── Duration ─── */
function startDuration() {
  state.elapsedSec = 0;
  clearInterval(state.durationTimer);
  state.durationTimer = setInterval(() => {
    state.elapsedSec++;
    const m = String(Math.floor(state.elapsedSec / 60)).padStart(2, "0");
    const s = String(state.elapsedSec % 60).padStart(2, "0");
    $("infoDuration").textContent = `${m}:${s}`;
  }, 1000);
}
function stopDuration() { clearInterval(state.durationTimer); }

/* ─── Journey ─── */
function renderJourney(activeKey = "", statusText = "Ready", pct = 0) {
  const host     = $("journey");
  host.innerHTML = "";
  const activeAgent = AGENT_BY_KEY[activeKey];
  const activeIdx   = activeAgent ? activeAgent.idx : 0;

  AGENTS.forEach((a) => {
    const isDone   = activeIdx > a.idx || statusText === "done" || statusText === "Completed";
    const isActive = activeIdx === a.idx;
    const div      = document.createElement("div");
    div.className  = "step" + (isDone ? " step--done" : "") + (isActive ? " step--active" : "");
    div.innerHTML  = `
      <div class="step__top">
        <div class="step__num">${a.idx}</div>
        <div class="step__icon">${a.icon}</div>
      </div>
      <div class="step__title">${a.title}</div>
      <div class="step__status">
        <span class="dot ${isDone ? "dot--green" : isActive ? "dot--blue" : ""}"></span>
        <span style="font-size:12px;color:${isDone ? "var(--green)" : isActive ? "var(--blue)" : "var(--muted)"}">
          ${isDone ? "Completed" : isActive ? "In Progress" : "Waiting"}
        </span>
      </div>
      ${isDone && !isActive ? `<div style="font-size:11px;color:var(--muted);margin-top:4px;">✓ Done</div>` : ""}
      <div class="step__bar">
        <div class="step__barFill" style="width:${isActive ? pct : isDone ? 100 : 0}%"></div>
      </div>
      ${isActive ? `<div style="font-size:11px;color:var(--blue);margin-top:4px;text-align:right;">${pct}%</div>` : ""}
    `;
    host.appendChild(div);
  });

  $("activeAgents").textContent = activeIdx ? String(Math.min(activeIdx, 6)) : "0";
  $("agentsState").textContent  = statusText === "running" ? "In Progress" : (statusText || "Idle");
}

/* ─── Artifact Helpers ─── */
function shortName(path) { return (path || "").split(/[/\\]/).pop() || path; }

function artifactIcon(name) {
  const n = (name || "").toLowerCase();
  if (n.endsWith(".py"))                       return "🐍";
  if (n.endsWith(".sql"))                      return "🗄️";
  if (n.endsWith(".ipynb"))                    return "📓";
  if (n.endsWith(".json"))                     return "{ }";
  if (n.endsWith(".yml") || n.endsWith(".yaml")) return "⚙️";
  if (n.endsWith(".md"))                       return "📄";
  return "▦";
}
function artifactLabel(name) {
  const n = (name || "").toLowerCase();
  if (n.endsWith(".py"))   return "PySpark Script";
  if (n.endsWith(".sql"))  return "SQL File";
  if (n.endsWith(".ipynb"))return "Databricks Notebook";
  if (n.endsWith(".json")) return "JSON Config";
  if (n.endsWith(".yml") || n.endsWith(".yaml")) return "YAML Config";
  if (n.endsWith(".md"))   return "Markdown Doc";
  return "Artifact";
}

/* ─── Artifact Cards ─── */
function renderArtifactCards(targetId, items) {
  const grid = $(targetId);
  if (!grid) return;
  grid.innerHTML = "";
  if (!items || items.length === 0) {
    grid.innerHTML = `<div class="muted" style="padding:12px;grid-column:1/-1;">No artifacts yet. Run the pipeline to generate artifacts.</div>`;
    return;
  }
  const meta = $("artifactSectionMeta");
  if (meta && targetId === "artifactGrid") meta.textContent = `${items.length} artifact(s) generated`;

  items.forEach((fullPath) => {
    const name = shortName(fullPath);
    const card = document.createElement("div");
    card.className = "artCard";
    card.innerHTML = `
      <div class="artIcon" style="font-size:18px;">${artifactIcon(name)}</div>
      <div class="artName" title="${fullPath}">${name}</div>
      <div class="artMeta">${artifactLabel(name)}</div>
      <div class="artActions">
        <button class="miniBtn" data-action="preview"  data-path="${fullPath}">Preview</button>
        <button class="miniBtn" data-action="download" data-path="${fullPath}" style="flex:0;padding:8px 6px;">⬇</button>
      </div>
    `;
    grid.appendChild(card);
  });

  grid.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      if (btn.dataset.action === "preview")  openPreview(btn.dataset.path);
      if (btn.dataset.action === "download") downloadArtifact(btn.dataset.path);
    });
  });
}

/* ─── Preview Modal ─── */
async function openPreview(path) {
  state.currentPreviewPath = path;
  const name = shortName(path);
  $("previewModal").style.display = "flex";
  $("previewTitle").textContent   = name;
  $("previewMeta").textContent    = artifactLabel(name) + "  ·  " + path;
  $("previewIcon").textContent    = artifactIcon(name);
  $("previewBody").textContent    = "Loading...";

  try {
    const res  = await fetch(`/api/artifact/preview?path=${encodeURIComponent(path)}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      $("previewBody").textContent = `Error ${res.status}: ${err.error || res.statusText}`;
      return;
    }
    const data = await res.json();
    $("previewBody").textContent = data.content || "(empty file)";
  } catch (e) {
    $("previewBody").textContent = `Failed to load preview:\n${e.message}`;
  }
}
function closePreview() {
  $("previewModal").style.display = "none";
  state.currentPreviewPath = null;
}
function downloadArtifact(path) {
  const a = document.createElement("a");
  a.href     = `/api/artifact/download?path=${encodeURIComponent(path)}`;
  a.download = shortName(path);
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

/* ─── Recent Runs ─── */
function renderRecentRuns() {
  const host = $("recentRuns");
  const runs = [
    { name:"Customer_360_Migration", state:"In Progress", dot:"dot--blue"  },
    { name:"Sales_Data_Pipeline",    state:"Completed",   dot:"dot--green" },
    { name:"HR_Analytics_Pipeline",  state:"Completed",   dot:"dot--green" },
    { name:"Finance_Migration",      state:"Failed",      dot:""           },
    { name:"Inventory_ETL",          state:"Completed",   dot:"dot--green" },
  ];
  host.innerHTML = "";
  runs.forEach(r => {
    const el = document.createElement("div");
    el.className = "runItem";
    el.innerHTML = `
      <div class="runName">${r.name}</div>
      <div class="runState"><span class="dot ${r.dot}"></span><span>${r.state}</span></div>`;
    host.appendChild(el);
  });
}

/* ─── File List ─── */
function renderSelectedFiles() {
  const input = $("supportFiles");
  const list  = $("fileList");
  if (!input || !list) return;
  list.innerHTML = "";
  const files = Array.from(input.files || []);
  if (!files.length) { list.innerHTML = `<div class="muted">No files selected.</div>`; return; }
  files.forEach(f => {
    const el = document.createElement("div");
    el.className = "fileItem";
    el.innerHTML = `<span title="${f.name}">${f.name}</span><b>${(f.size/1024).toFixed(1)} KB</b>`;
    list.appendChild(el);
  });
}

/* ─── API Helpers ─── */
async function apiPost(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) { const t = await res.text(); throw new Error(`${url} → ${res.status}: ${t}`); }
  return res.json();
}
async function apiGet(url) {
  const res = await fetch(url);
  if (!res.ok) { const t = await res.text(); throw new Error(`${url} → ${res.status}: ${t}`); }
  return res.json();
}

/* ─── Run Info ─── */
function setRunInfo() {
  const rid = state.runId || "—";
  $("runIdPill").textContent    = rid;
  $("infoRunId").textContent    = rid;
  $("infoPipeline").textContent = $("pipelineSelect").value;
  $("infoStart").textContent    = state.startTime ? new Date(state.startTime).toLocaleString() : "—";
}

/* ─── Apply Status ─── */
function applyStatus(data) {
  const statusRaw = (data.status || "").toLowerCase();
  const agentKey  = data.current_agent || "";
  const agentInfo = AGENT_BY_KEY[agentKey] || {};
  const agentIdx  = agentInfo.idx || 0;

  $("rtTitle").textContent   = agentKey ? `Agent ${agentIdx}: ${agentInfo.title || agentKey}` : "Agent: —";
  $("infoAgent").textContent = agentIdx ? `Agent ${agentIdx} of 6` : "—";

  const pct = agentIdx ? Math.min(100, Math.round(((agentIdx - 1) / 6) * 100) + 14) : 0;
  setProgress(pct);
  renderJourney(agentKey, statusRaw, pct);

  if (statusRaw === "done" || statusRaw.includes("complete")) {
    setStatus("ok", "Status: Completed"); setBadge("ok", "Completed");
    $("radialSub").textContent = "Completed";
    stopPolling(); stopDuration();
    pushLog("[UI] ✅ Pipeline completed successfully!");
  } else if (statusRaw === "error") {
    setStatus("bad", "Status: Error"); setBadge("bad", "Error");
    $("radialSub").textContent = "Error";
    stopPolling(); stopDuration();
    pushLog(`[UI] ❌ Error: ${data.error || "unknown"}`);
  } else if (statusRaw.includes("wait")) {
    setStatus("info", "Status: Waiting"); setBadge("warn", "Waiting Approval");
    $("radialSub").textContent = "Waiting";
  } else {
    setStatus("info", "Status: In Progress"); setBadge("info", "In Progress");
    $("radialSub").textContent = "In Progress";
  }

  /* Logs from agent outputs */
  const outputs = Array.isArray(data.outputs) ? data.outputs : [];
  if (outputs.length && outputs.length !== state.lastOutputLen) {
    clearLog();
    outputs.forEach(o => {
      pushLog(`\n── Agent: ${o.agent} ──────────────────`);
      (o.content || "").split("\n").slice(0, 80).forEach(l => pushLog(l));
    });
    state.lastOutputLen = outputs.length;
  }

  /* Artifacts */
  const arts = Array.isArray(data.artifacts_written) ? data.artifacts_written : [];
  if (arts.length !== state.artifacts.length) {
    state.artifacts = arts;
    $("artifactCountMini").textContent = arts.length;
    $("kpiArtifacts").textContent      = arts.length || "214";
    renderArtifactCards("artifactMiniGrid", arts.slice(0, 4));
    renderArtifactCards("artifactGrid", arts);
  }
}

/* ─── Polling ─── */
function startPolling() {
  stopPolling();
  if (!state.runId) return;
  state.pollTimer = setInterval(async () => {
    try { applyStatus(await apiGet(`/api/status/${state.runId}`)); }
    catch (err) { pushLog(`[UI] Poll error: ${err.message}`); }
  }, 1800);
}
function stopPolling() { clearInterval(state.pollTimer); state.pollTimer = null; }

/* ─── Save Inputs ─── */
async function saveInputs() {
  const msgEl = $("saveInputsMsg");
  if (!state.runId) {
    if (msgEl) msgEl.textContent = "⚠ Start a run first.";
    pushLog("[UI] No run_id yet. Start Phase 1 or Full Pipeline first.");
    return;
  }
  const fd = new FormData();
  fd.append("user_story", $("userStory") ? $("userStory").value : "");
  Array.from(($("supportFiles") || {}).files || []).forEach(f => fd.append("files", f));
  if (msgEl) msgEl.textContent = "Uploading...";
  const res   = await fetch(`/api/inputs/${state.runId}`, { method: "POST", body: fd });
  const data  = await res.json();
  const count = (data.saved_files || []).length;
  if (msgEl) msgEl.textContent = `✅ Saved (${count} file${count !== 1 ? "s" : ""})`;
  pushLog(`[UI] Inputs saved — ${count} file(s) attached to run ${state.runId}`);
}

/* ─── Build payload (always includes user_story) ─── */
function buildPayload() {
  const story = ($("userStory") && $("userStory").value.trim())
    ? $("userStory").value.trim()
    : `Build ETL pipeline for ${$("pipelineSelect").value}. ${($("approvalComment") || {}).value || ""}`.trim();
  return {
    user_story:    story,
    pipeline_name: $("pipelineSelect").value,
    comments:      ($("approvalComment") || {}).value || "",
  };
}

/* ─── Run Phase 1 ─── */
async function runPhase1() {
  clearLog();
  pushLog("[UI] Starting Phase 1 (Refinement + Source Discovery + Schema Drift)...");
  setStatus("info", "Status: In Progress"); setBadge("info", "In Progress");
  state.startTime = new Date().toISOString();
  state.lastOutputLen = 0;
  setRunInfo(); startDuration();

  const resp = await apiPost("/api/run_phase1", buildPayload());
  state.runId = resp.run_id;
  setRunInfo();
  pushLog(`[UI] Phase 1 done. run_id = ${state.runId}`);
  applyStatus({
    run_id: resp.run_id, status: "waiting", current_agent: "source_discovery",
    outputs: resp.outputs || [], artifacts_written: resp.artifacts_written || [],
  });
  startPolling();
}

/* ─── Run Phase 2 ─── */
async function runPhase2() {
  if (!state.runId) { pushLog("[UI] No run_id — run Phase 1 first."); return; }
  pushLog("[UI] Continuing Phase 2 (Transformation + Trust Quality → Deployment )...");
  setBadge("info", "In Progress"); startDuration();
  const resp = await apiPost(`/api/run_phase2/${state.runId}`, buildPayload());
  pushLog(`[UI] Phase 2 complete. Artifacts: ${(resp.artifacts_written || []).length}`);
  applyStatus({
    run_id: resp.run_id, status: "done", current_agent: "deployment",
    outputs: resp.outputs || [], artifacts_written: resp.artifacts_written || [],
  });
}

/* ─── Run Full Pipeline ─── */
async function runAll() {
  clearLog();
  pushLog("[UI] Starting full 6-agent pipeline...");
  setStatus("info", "Status: In Progress"); setBadge("info", "In Progress");
  state.startTime = new Date().toISOString();
  state.lastOutputLen = 0;
  setRunInfo(); startDuration();
  const resp = await apiPost("/api/run_async", buildPayload());
  state.runId = resp.run_id;
  setRunInfo();
  pushLog(`[UI] Pipeline started. run_id = ${state.runId}`);
  startPolling();
}

/* ─── Init ─── */
document.addEventListener("DOMContentLoaded", () => {
  renderJourney("", "Ready", 0);
  renderRecentRuns();
  setProgress(0);
  $("radialSub").textContent = "Ready";
  $("rtTitle").textContent   = "Agent: —";
  setBadge("info", "Ready");
  setStatus("info", "Status: Ready");
  renderArtifactCards("artifactMiniGrid", []);
  renderArtifactCards("artifactGrid", []);

  $("pipelineSelect").addEventListener("change", () => {
    $("infoPipeline").textContent = $("pipelineSelect").value;
  });

  $("btnRunPhase1").addEventListener("click", () => runPhase1().catch(e => pushLog("[UI] " + e.message)));
  $("btnRunPhase2").addEventListener("click", () => runPhase2().catch(e => pushLog("[UI] " + e.message)));
  $("btnRunAll").addEventListener("click",    () => runAll().catch(e    => pushLog("[UI] " + e.message)));

  $("btnApprove").addEventListener("click", () => {
    pushLog("[UI] ✅ Approved. Continuing Phase 2...");
    runPhase2().catch(e => pushLog("[UI] " + e.message));
  });
  $("btnRequest").addEventListener("click", () =>
    pushLog("[UI] Request changes noted."));
  $("btnRegenerate").addEventListener("click", () => {
    pushLog("[UI] Regenerating Phase 1...");
    runPhase1().catch(e => pushLog("[UI] " + e.message));
  });
  $("btnStop").addEventListener("click", () => {
    stopPolling(); stopDuration();
    setStatus("info", "Status: Stopped"); setBadge("bad", "Stopped");
    $("radialSub").textContent = "Stopped";
    pushLog("[UI] Pipeline stopped.");
  });

  const sf = $("supportFiles");
  if (sf) sf.addEventListener("change", renderSelectedFiles);
  const bs = $("btnSaveInputs");
  if (bs) bs.addEventListener("click", () => saveInputs().catch(e => {
    pushLog("[UI] " + e.message);
    const m = $("saveInputsMsg");
    if (m) m.textContent = "❌ " + e.message;
  }));

  const closeBtn = $("previewClose");
  if (closeBtn) closeBtn.addEventListener("click", closePreview);
  const dlBtn = $("previewDownloadBtn");
  if (dlBtn) dlBtn.addEventListener("click", () => {
    if (state.currentPreviewPath) downloadArtifact(state.currentPreviewPath);
  });
  const modal = $("previewModal");
  if (modal) modal.addEventListener("click", e => { if (e.target === modal) closePreview(); });
  document.addEventListener("keydown", e => { if (e.key === "Escape") closePreview(); });
});