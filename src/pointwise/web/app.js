/* Pointwise web UI — vanilla JS, no build step, no external dependencies. */
"use strict";

const state = {
  version: null,
  stages: [],
  sources: [],
  stage: null,
  columns: [],
  rows: [],
  selectedRid: null,
};

const $ = (id) => document.getElementById(id);
const fmt = (v) => (v === null || v === undefined ? "∅" : String(v));
const nbytes = (n) => n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(2)} MB`;

async function api(path, opts) {
  const res = await fetch(path, opts);
  const body = await res.json();
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body;
}
async function post(path, payload) {
  return api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

// --------------------------------------------------------------- bootstrap
async function boot() {
  try {
    const health = await api("/api/health");
    $("health").textContent = `当前版本: ${health.current || "无"}`;
    $("health").className = "pill ok";
  } catch (err) {
    $("health").textContent = "服务不可用";
    return;
  }
  await loadVersions();
  if (state.version) await loadAll();
  bindStatic();
}

async function loadVersions(select) {
  const data = await api("/api/versions");
  const opts = data.versions;
  const cur = opts.find((v) => v.is_current) || opts[0];
  state.version = cur ? cur.version : null;
  for (const sel of [$("versionSelect"), $("diffFrom"), $("diffTo")]) {
    sel.innerHTML = "";
    for (const v of opts) {
      const o = document.createElement("option");
      o.value = v.version;
      o.textContent = `${v.version}${v.is_current ? " ★" : ""} · ${v.note || ""}`;
      sel.appendChild(o);
    }
  }
  $("versionSelect").value = state.version || "";
  if (opts.length >= 2) {
    $("diffFrom").value = opts[0].version;
    $("diffTo").value = opts[opts.length - 1].version;
  }
}

async function loadAll() {
  const q = `?version=${encodeURIComponent(state.version)}`;
  const [meta, stats] = await Promise.all([
    api(`/api/stages${q}`),
    api(`/api/stats${q}`),
  ]);
  state.stages = meta.stages;
  state.sources = meta.sources;
  renderStageTabs();
  renderStats(stats);
  await loadRows();
}

function renderStageTabs() {
  const tabs = $("stageTabs");
  tabs.innerHTML = "";
  const names = [...state.sources, ...state.stages.map((s) => s.name)];
  for (const name of names) {
    const b = document.createElement("button");
    b.textContent = name;
    b.className = name === state.stage ? "active" : "";
    b.onclick = () => { state.stage = name; renderStageTabs(); loadRows(); };
    tabs.appendChild(b);
  }
  if (!state.stage && names.length) state.stage = names[0];
}

// --------------------------------------------------------------- stats
function renderStats(stats) {
  const e = stats.evidence;
  const s = stats.sources;
  $("statsBody").innerHTML = `
    <div class="stat"><div class="k">来源行（活跃 / 撤销 / 合计）</div>
      <div class="v">${s.active} / ${s.retracted} / ${s.total}</div></div>
    <div class="stat"><div class="k">边记录数（含重数 n）</div>
      <div class="v">${e.edge_records}</div><div class="note">丢弃记录 ${e.drop_records} 条，均带原因</div></div>
    <div class="stat"><div class="k">实际证据占用（紧凑边方案）</div>
      <div class="v">${nbytes(e.edge_bytes + e.drop_bytes)}</div>
      <div class="note">版本目录合计 ${nbytes(e.total_version_bytes)}</div></div>
    <div class="stat"><div class="k">若全量物化路径（估算）</div>
      <div class="v">${nbytes(e.estimated_full_path_bytes)}</div>
      <div class="note">${e.saving_ratio == null ? "" : `紧凑方案约省 ${(e.saving_ratio * 100).toFixed(1)}%`}</div></div>
    <div class="stat"><div class="k">最大阶段深度 / 覆盖范围</div>
      <div class="v">${e.max_stage_depth} 层</div>
      <div class="note">${e.coverage}</div></div>
    <div class="stat"><div class="k">估算方法</div>
      <div class="v" style="font-size:12px;font-weight:400">${e.estimate_note}</div></div>`;

  const scope = stats.explainability_scope;
  $("statsBody").innerHTML += `
    <div class="stat" style="grid-column:1/-1"><div class="k">可解释范围</div>
      <ul class="tight">
        <li>${scope.row_level}</li><li>${scope.rules}</li>
        <li>${scope.drops}</li><li>${scope.zero}</li><li>${scope.retraction}</li>
      </ul></div>`;

  const z = stats.zero_explanations || [];
  $("zeroBody").innerHTML = z.length
    ? z.map((x) => `<div class="drop-card">
        <code>${x.stage}</code> 分组 <code>${x.group_rid}</code> 的 <code>${x.alias}</code>=0：
        <span class="zero-${x.zero_kind}">${x.zero_kind === "cancelled" ? "冲销" : "输入本身为零"}</span>
        — ${x.reason}（正行 ${x.positive_rows} / 负行 ${x.negative_rows} / 零行 ${x.zero_rows}）
        <button data-zero='${JSON.stringify([x.stage, x.group_rid, x.alias]).replace(/'/g, "&#39;")}'
          class="zero-btn">展开证据</button>
      </div>`).join("")
    : `<p class="muted">本版本没有值为 0 的求和聚合。</p>`;
  document.querySelectorAll(".zero-btn").forEach((b) =>
    (b.onclick = () => showZero(...JSON.parse(b.dataset.zero))));

  const v = stats.verification;
  $("verifyBody").innerHTML = `
    <div class="stat"><div class="k">${v.ok ? "✅ 全部边/丢弃记录引用有效" : "❌ 发现断链"}</div>
      <div class="note">阶段 ${v.stages} · rid ${v.known_rids} · 边 ${v.edges} · 丢弃 ${v.drops}</div>
      ${v.issues.length ? `<ul class="tight">${v.issues.map((i) => `<li>${i}</li>`).join("")}</ul>` : ""}
    </div>`;
}

// --------------------------------------------------------------- rows table
async function loadRows(extra) {
  const params = new URLSearchParams({ stage: state.stage, ...(extra || {}) });
  if (state.version) params.set("version", state.version);
  const data = await api(`/api/rows?${params}`);
  state.columns = data.columns ? [...data.columns] : [];
  state.rows = data.rows;

  const colSel = $("filterCol");
  colSel.innerHTML = state.columns.map((c) => `<option>${c}</option>`).join("");
  const grpSel = $("groupBy");
  grpSel.innerHTML = `<option value="">不分组</option>` +
    state.columns.map((c) => `<option>${c}</option>`).join("");

  const tableHead = document.querySelector("#rowsTable thead");
  tableHead.innerHTML = `<tr><th>rid（点击展开证据）</th>${state.columns.map((c) => `<th>${c}</th>`).join("")}</tr>`;
  const body = document.querySelector("#rowsTable tbody");
  body.innerHTML = state.rows.map((r) =>
    `<tr><td class="rid-link" data-rid="${r.rid}">${r.rid}</td>` +
    state.columns.map((c) => `<td>${fmt(r.data[c])}</td>`).join("") + `</tr>`).join("")
    || `<tr><td colspan="${state.columns.length + 1}" class="muted">无数据</td></tr>`;
  body.querySelectorAll(".rid-link").forEach((td) =>
    (td.onclick = () => showLineage(td.dataset.rid)));

  if (data.groups) {
    const box = $("groupResult");
    box.classList.remove("hidden");
    box.innerHTML = `<b>分组计数（${data.groups.length} 组，筛选后 ${data.total} 行）</b><br>` +
      data.groups.map((g) => `<span class="chip">${g.key} <b>${g.count}</b></span>`).join("");
  } else {
    $("groupResult").classList.add("hidden");
  }
}

// --------------------------------------------------------------- lineage
async function showLineage(rid) {
  state.selectedRid = rid;
  switchTab("lineage");
  const q = `?version=${encodeURIComponent(state.version)}&rid=${encodeURIComponent(rid)}`;
  const [exp, src, paths] = await Promise.all([
    api(`/api/expand${q}`),
    api(`/api/sources${q}`),
    api(`/api/paths${q}&limit=200`),
  ]);
  const rules = (exp.rules || []).map((r) =>
    `<div class="rule"><b>规则 ${r.rule_id}</b>（${r.type}）：${r.description}</div>`).join("");
  const inputs = (exp.inputs || []).map((i) => `
    <div class="evidence-node ${i.stage && state.sources.includes(i.stage) ? "source" : ""}">
      <div class="meta">
        <span class="rid-link" data-rid="${i.rid}">${i.rid}</span>
        · 阶段 <code>${i.stage}</code>
        · 参与重数 <span class="mult">n=${i.n}</span>${i.missing ? " · ⚠️ 证据缺失" : ""}
      </div>
      <div>${i.data ? renderData(i.data) : "<span class='muted'>（来源行数据不可见）</span>"}</div>
    </div>`).join("");
  const part = Object.entries(src.participation).map(([k, n]) =>
    `<li><code>${k}</code> 参与 <span class="mult">${n}</span> 次（多路/扇出不抹除）</li>`).join("");
  $("lineageBody").innerHTML = `
    <div class="evidence-node">
      <div class="meta">结果行 <code>${exp.rid}</code> · 阶段 <code>${exp.stage}</code></div>
      <div>${exp.data ? renderData(exp.data) : ""}</div>
    </div>
    <h3>应用的规则</h3>${rules || "<p class='muted'>来源行，无上游规则。</p>"}
    <h3>直接输入（一层）</h3>${inputs || "<p class='muted'>已是来源行。</p>"}
    <h3>来源参与统计（递归全路径，重数相乘）</h3>
    <ul class="tight">${part || "<li class='muted'>无</li>"}</ul>
    <h3>完整路径（按需枚举，上限 200）</h3>
    <p class="muted">共 ${paths.path_count} 条${paths.truncated ? "（已截断）" : ""}，展示前 20 条：</p>
    <ul class="tight">${paths.paths.slice(0, 20).map((p) => `<li>${p.map((x) => `<code>${x}</code>`).join(" → ")}</li>`).join("")}</ul>`;
  $("lineageBody").querySelectorAll(".rid-link[data-rid]").forEach((el) =>
    (el.onclick = () => showLineage(el.dataset.rid)));
}

function renderData(data) {
  return Object.entries(data).map(([k, v]) =>
    `<span class="chip">${k}: <b>${fmt(v)}</b></span>`).join(" ");
}

async function showZero(stage, rid, alias) {
  const q = `?version=${encodeURIComponent(state.version)}&stage=${stage}&rid=${encodeURIComponent(rid)}&alias=${alias}`;
  const data = await api(`/api/explain-zero${q}`);
  switchTab("lineage");
  let html = `<div class="evidence-node">
    <div class="meta">零值解释 · <code>${stage}</code> / <code>${rid}</code> / <code>${alias}</code></div>
    <p><span class="zero-${data.kind}">${data.message}</span></p>`;
  if (data.kind === "cancelled" || data.kind === "all_zero") {
    html += `<ul class="tight">
      <li>正向合计：<b>${data.positive_sum}</b>；负向合计：<b>${data.negative_sum}</b></li>
      <li>零值输入行：${data.zero_value_rows}</li></ul>
      <h3>逐条贡献（含重数）</h3>
      <ul class="tight">${data.entries.map((x) =>
        `<li><code>${x.rid}</code> × ${x.n} → 值 ${fmt(x.value)}</li>`).join("")}</ul>
      <h3>来源参与</h3><ul class="tight">${Object.entries(data.source_participation)
        .map(([k, n]) => `<li><code>${k}</code> 参与 ${n} 次</li>`).join("")}</ul>`;
  }
  html += `</div>`;
  $("lineageBody").innerHTML = html;
}

// --------------------------------------------------------------- drops
async function loadDrops() {
  const params = new URLSearchParams({ version: state.version });
  if ($("dropReason").value) params.set("reason", $("dropReason").value);
  if ($("dropRule").value) params.set("rule", $("dropRule").value);
  const data = await api(`/api/drops?${params}`);
  $("dropsBody").innerHTML = `<p class="muted">共 ${data.total} 条</p>` +
    data.drops.map((d) => `<div class="drop-card">
      <div class="meta"><code>${d.in}</code> · 阶段 <code>${d.stage}</code> · 规则 <b>${d.rule}</b></div>
      <div>${d.reason}</div>
      ${d.data ? `<div class="muted">行数据：${renderData(d.data)}</div>` : ""}
      <div><span class="rid-link" data-rid="${d.in}">查看该行证据</span></div>
    </div>`).join("");
  $("dropsBody").querySelectorAll(".rid-link").forEach((el) =>
    (el.onclick = () => showLineage(el.dataset.rid)));
}

// --------------------------------------------------------------- diff
async function loadDiff() {
  const data = await api(
    `/api/diff?from=${encodeURIComponent($("diffFrom").value)}&to=${encodeURIComponent($("diffTo").value)}`);
  const tombs = [...data.tombstones_added.map((r) => `撤销 <code>${r}</code>`),
    ...data.tombstones_removed.map((r) => `恢复 <code>${r}</code>`)].join("，");
  let html = `<p class="muted">${data.old_version} → ${data.new_version}；${tombs || "无来源撤销变化"}</p>
    <p>合计：<span class="diff-add">新增 ${data.summary.added}</span> ·
       <span class="diff-rm">删除 ${data.summary.removed}</span> ·
       <span class="diff-ch">变更 ${data.summary.changed}</span></p>`;
  for (const s of data.stages) {
    if (!s.added.length && !s.removed.length && !s.changed.length) continue;
    html += `<div class="evidence-node"><div class="meta">阶段 <code>${s.stage}</code></div>`;
    for (const r of s.removed)
      html += `<div class="diff-rm">− ${r.rid} ${renderData(r.data)} <span class="muted">来源 ${Object.keys(r.sources).join(", ")}</span></div>`;
    for (const a of s.added)
      html += `<div class="diff-add">+ ${a.rid} ${renderData(a.data)} <span class="muted">来源 ${Object.keys(a.sources).join(", ")}</span></div>`;
    for (const c of s.changed)
      html += `<div class="diff-ch">~ ${c.rid}: ${renderData(c.before)} → ${renderData(c.after)}</div>`;
    html += `</div>`;
  }
  $("diffBody").innerHTML = html;
}

// --------------------------------------------------------------- actions
async function retract(restore) {
  const rid = $("ridInput").value.trim();
  if (!rid) return alert("请输入来源 rid");
  $("actionBody").innerHTML = "<p class='muted'>重算并原子发布新版本中…</p>";
  try {
    const data = await post(restore ? "/api/restore" : "/api/retract", { rid });
    $("actionBody").innerHTML = `<div class="evidence-node">
      已${restore ? "恢复" : "撤销"} <code>${rid}</code>，新版本 <code>${data.version}</code>。
      旧版本仍保留，可在顶部版本下拉中切回对比。
      <div class="muted">校验：${data.stats.verification.ok ? "✅ 证据完整" : "❌ " + data.stats.verification.issues.join("; ")}</div>
    </div>`;
    await loadVersions();
    await loadAll();
  } catch (err) {
    $("actionBody").innerHTML = `<div class="drop-card">失败：${err.message}</div>`;
  }
}

// --------------------------------------------------------------- wiring
function switchTab(name) {
  document.querySelectorAll(".tabs button").forEach((b) =>
    b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tabpane").forEach((p) =>
    p.classList.toggle("hidden", p.id !== `tab-${name}`));
}

function bindStatic() {
  document.querySelectorAll(".tabs button").forEach((b) =>
    (b.onclick = () => {
      switchTab(b.dataset.tab);
      if (b.dataset.tab === "drops") loadDrops();
    }));
  $("filterBtn").onclick = () => loadRows({
    filter_col: $("filterCol").value, filter_op: $("filterOp").value, filter_val: $("filterVal").value,
  });
  $("groupBtn").onclick = () => loadRows({
    filter_col: $("filterCol").value, filter_op: $("filterOp").value,
    filter_val: $("filterVal").value, group_by: $("groupBy").value,
  });
  $("resetBtn").onclick = () => { $("filterVal").value = ""; $("groupBy").value = ""; loadRows(); };
  $("dropSearchBtn").onclick = loadDrops;
  $("diffBtn").onclick = loadDiff;
  $("retractBtn").onclick = () => retract(false);
  $("restoreBtn").onclick = () => retract(true);
  $("versionSelect").onchange = async (e) => {
    state.version = e.target.value; state.stage = null; await loadAll();
  };
  $("releaseBtn").onclick = async () => {
    await post("/api/release", { version: $("versionSelect").value });
    await loadVersions();
  };
  $("rebuildBtn").onclick = async () => {
    await post("/api/rebuild", { note: "网页手动重算" });
    await loadVersions();
    await loadAll();
  };
  $("gcBtn").onclick = async () => {
    const r = await post("/api/gc", { keep: 3 });
    alert(`已清理版本：${r.removed.join(", ") || "无（current 与最近版本保留）"}`);
    await loadVersions();
  };
}

boot();
