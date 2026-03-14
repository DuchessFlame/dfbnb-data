/**
 * DFBNB Curve Tables UI (lazy chunks)
 *
 * Data location:
 * - baseUrl defaults to /wp-content/uploads/curves
 * - can be overridden by window.dfbnbData.curves_base_url
 *
 * API (called by guide.js):
 *   window.__DFBNB_CURVES_API.mount(pathname)
 *   window.__DFBNB_CURVES_API.search(query)
 *   window.__DFBNB_CURVES_API.prefetch(pathname)
 */

(function () {
  "use strict";

  /* ─── Register API first — before any path guards ─────────────────────── */
  // Must exist before guide.js tries to call .mount() on SPA nav,
  // even if the current URL isn't a curves page.
  window.__DFBNB_CURVES_API = window.__DFBNB_CURVES_API || {};

  const API = window.__DFBNB_CURVES_API;

  /* ─── State ────────────────────────────────────────────────────────────── */

  let UI_KEY = "";

  function uiLoad() {
    if (!UI_KEY) return {};
    try { return JSON.parse(localStorage.getItem(UI_KEY) || "{}"); }
    catch { return {}; }
  }

  function uiSave(obj) {
    if (!UI_KEY) return;
    try { localStorage.setItem(UI_KEY, JSON.stringify(obj || {})); } catch {}
  }

  let uiState = {
    groups: {},
    perks:  {},
    curves: {},
  };

  const CACHE = {
    index:  null,
    chunks: new Map()
  };

  /* ─── Base URL ─────────────────────────────────────────────────────────── */

  const DEFAULT_BASE_URL = location.origin + "/wp-content/uploads/curves";
  const BASE_URL =
    (window.dfbnbData && typeof window.dfbnbData.curves_base_url === "string" &&
     window.dfbnbData.curves_base_url.trim())
      ? window.dfbnbData.curves_base_url.trim().replace(/\/$/, "")
      : DEFAULT_BASE_URL;

  /* ─── Helpers ──────────────────────────────────────────────────────────── */

  function byId(id) { return document.getElementById(id); }

  function safeText(v) { return String(v == null ? "" : v).trim(); }

  function escapeHtml(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function escapeAttr(s) { return escapeHtml(s).replaceAll(" ", "_"); }

  function debounce(fn, ms) {
    let t = null;
    return function () {
      clearTimeout(t);
      t = setTimeout(() => fn.apply(this, arguments), ms);
    };
  }

  /* ─── Path routing ─────────────────────────────────────────────────────── */

  function isPerkCardsPath(pathname) {
    const p = String(pathname || "");
    return /\/(?:bnb\/)?curve-tables\/perk-cards\/?$/.test(p);
  }

  function isCurvesMasterIndexPath(pathname) {
    const p = String(pathname || "");
    return /\/(?:bnb\/)?curve-tables\/other\/?$/.test(p);
  }

  /* ─── Data fetching ────────────────────────────────────────────────────── */

  async function loadPerkCards() {
    const url = BASE_URL + "/perk_cards.json";
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin" });
    if (!r.ok) throw new Error("perk_cards_http_" + r.status);
    return r.json();
  }

  async function loadIndex() {
    if (CACHE.index) return CACHE.index;
    const url = BASE_URL + "/index.json";
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin" });
    if (!r.ok) throw new Error("curves_index_http_" + r.status);
    CACHE.index = await r.json();
    return CACHE.index;
  }

  async function loadChunk(url) {
    if (CACHE.chunks.has(url)) return CACHE.chunks.get(url);
    const r = await fetch(url, { cache: "force-cache", credentials: "same-origin" });
    if (!r.ok) throw new Error("curves_chunk_http_" + r.status);
    const j = await r.json();
    CACHE.chunks.set(url, j);
    return j;
  }

  /* ─── Formatting ───────────────────────────────────────────────────────── */

  function fmtNum(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) return "N/A";
    if (Math.abs(v) >= 100000) return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
    return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }

  /* ─── Render helpers ───────────────────────────────────────────────────── */

  function renderShell() {
    return `
      <div class="dfbnb-curves">
        <div class="dfbnb-curves__bar">
          <div class="dfbnb-curves__title">Curve Tables</div>
          <div class="dfbnb-curves__controls">
            <input class="dfbnb-curves__search" data-curves-search type="search" placeholder="Search EDID or FormID..." />
            <select class="dfbnb-curves__select" data-curves-category></select>
            <select class="dfbnb-curves__select" data-curves-sort></select>
          </div>
          <div class="dfbnb-curves__count" data-curves-count>0 curves</div>
        </div>
        <div class="dfbnb-curves__list" data-curves-list></div>
      </div>
    `;
  }

  function renderError(msg) {
    return `<div class="dfbnb-curves__error">${escapeHtml(msg)}</div>`;
  }

  function renderLoadingPanel() {
    return `<div class="dfbnb-curves__loading">Loading curve points…</div>`;
  }

  function renderErrorPanel(msg) {
    return `<div class="dfbnb-curves__panelError">${escapeHtml(msg)}</div>`;
  }

  function hydrateFilters(els, index) {
    els.category.innerHTML = [
      `<option value="__all__">All Categories</option>`,
      ...index.categories.map(c =>
        `<option value="${escapeAttr(c.id)}">${escapeHtml(c.title)} (${c.count})</option>`)
    ].join("");

    els.sort.innerHTML = [
      `<option value="browse">Browse Order</option>`,
      `<option value="edid-az">EDID A–Z</option>`,
      `<option value="edid-za">EDID Z–A</option>`,
      `<option value="points-high">Most Points</option>`,
      `<option value="points-low">Fewest Points</option>`
    ].join("");
  }

  function sortCurves(list, sort) {
    const copy = list.slice();
    if (sort === "edid-az")     return copy.sort((a, b) => (a.edid || "").localeCompare(b.edid || "") || a.id.localeCompare(b.id));
    if (sort === "edid-za")     return copy.sort((a, b) => (b.edid || "").localeCompare(a.edid || "") || a.id.localeCompare(b.id));
    if (sort === "points-high") return copy.sort((a, b) => (b.points - a.points) || (a.edid || "").localeCompare(b.edid || ""));
    if (sort === "points-low")  return copy.sort((a, b) => (a.points - b.points) || (a.edid || "").localeCompare(b.edid || ""));
    return copy;
  }

  function renderCard(c, index) {
    const catTitle = (index.categories && index.categories.find(x => x.id === c.category)?.title) || c.category || "";
    const descHtml = c.desc ? `<div class="dfbnb-curves__desc">${escapeHtml(c.desc)}</div>` : "";
    const softCapPill = c.softCap
      ? `<span class="dfbnb-curves__pill dfbnb-curves__pillSoftCap">Soft Cap: Lv ${fmtNum(c.softCap.x)}</span>`
      : "";
    return `
      <div class="dfbnb-curves__card" data-curve-card="${escapeAttr(c.id)}" data-expanded="0">
        <div class="dfbnb-curves__cardTop">
          <div class="dfbnb-curves__cardMain">
            <div class="dfbnb-curves__edid">${escapeHtml(c.edid || "(no EDID)")}</div>
            ${descHtml}
            <div class="dfbnb-curves__meta">
              ${catTitle ? `<span class="dfbnb-curves__pill">${escapeHtml(catTitle)}</span>` : ""}
              <span class="dfbnb-curves__pill">FormID: ${escapeHtml(c.id)}</span>
              <span class="dfbnb-curves__pill">${Number(c.points || 0).toLocaleString()} points</span>
              <span class="dfbnb-curves__pill">${escapeHtml(c.xLabel || "X")}: ${fmtNum(c.xMin)} → ${fmtNum(c.xMax)}</span>
              <span class="dfbnb-curves__pill">${escapeHtml(c.yLabel || "Y")}: ${fmtNum(c.yMin)} → ${fmtNum(c.yMax)}</span>
              ${softCapPill}
            </div>
          </div>
          <div class="dfbnb-curves__cardActions">
            <button class="dfbnb-curves__btn" data-curve-toggle="${escapeAttr(c.id)}">Expand</button>
          </div>
        </div>
        <div class="dfbnb-curves__panel" data-curve-panel></div>
      </div>
    `;
  }

  function renderPointsTable(curve) {
    const table = (curve.displayTable && curve.displayTable.length) ? curve.displayTable : (curve.points || []);
    const xLabel = escapeHtml(curve.xLabel || "Input");
    const yLabel = escapeHtml(curve.yLabel || "Result");
    const softCapX = curve.softCap ? curve.softCap.x : null;

    const max = 500;
    const shown = table.slice(0, max);
    const rows = shown.map(p => {
      const isSoftCap = softCapX !== null && p.x === softCapX;
      const cls = isSoftCap ? ' class="dfbnb-curves__rowSoftCap"' : "";
      const marker = isSoftCap ? ' <span class="dfbnb-curves__softCapTag">SOFT CAP</span>' : "";
      return `<tr${cls}><td>${fmtNum(p.x)}${marker}</td><td>${fmtNum(p.y)}</td></tr>`;
    }).join("");

    const note = table.length > max
      ? `<div class="dfbnb-curves__note">Showing first ${max} of ${table.length.toLocaleString()} rows.</div>`
      : "";
    return `
      ${note}
      <div class="dfbnb-curves__tableScroller">
        <table class="dfbnb-curves__table">
          <thead><tr><th>${xLabel}</th><th>${yLabel}</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderCurvePanel(curve) {
    const id = curve.id;
    const descHtml = curve.desc
      ? `<div class="dfbnb-curves__panelDesc">${escapeHtml(curve.desc)}</div>`
      : "";
    const softCapHtml = curve.softCap
      ? `<div class="dfbnb-curves__panelSoftCap">Soft Cap at ${escapeHtml(curve.xLabel || "X")} = ${fmtNum(curve.softCap.x)} (${escapeHtml(curve.yLabel || "Y")} = ${fmtNum(curve.softCap.y)}). Beyond this point, gains diminish significantly.</div>`
      : "";
    const xLabel = escapeHtml(curve.xLabel || "Input");
    const yLabel = escapeHtml(curve.yLabel || "Result");
    return `
      ${descHtml}
      ${softCapHtml}
      <div class="dfbnb-curves__panelGrid">
        <div class="dfbnb-curves__chart">
          <div class="dfbnb-curves__panelHeading">Chart</div>
          <div class="dfbnb-curves__chartBox">
            <div class="dfbnb-curves__axisLabel dfbnb-curves__axisY">${yLabel}</div>
            <svg class="dfbnb-curves__svg" viewBox="0 0 600 260" preserveAspectRatio="none"
                 data-curve-svg="${escapeAttr(id)}"></svg>
            <div class="dfbnb-curves__axisLabel dfbnb-curves__axisX">${xLabel}</div>
          </div>
        </div>
        <div class="dfbnb-curves__tableWrap">
          <div class="dfbnb-curves__panelHeading">Values</div>
          ${renderPointsTable(curve)}
        </div>
      </div>
    `;
  }

  function drawCurveSvg(svgEl, points, softCap) {
    if (!svgEl) return;
    if (!points || !points.length) { svgEl.innerHTML = ""; return; }

    const W = 600, H = 260, pad = 28;
    let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;
    for (const p of points) {
      xMin = Math.min(xMin, p.x); xMax = Math.max(xMax, p.x);
      yMin = Math.min(yMin, p.y); yMax = Math.max(yMax, p.y);
    }
    if (xMin === xMax) xMax = xMin + 1;
    if (yMin === yMax) yMax = yMin + 1;

    const sx = x => pad + ((x - xMin) / (xMax - xMin)) * (W - pad * 2);
    const sy = y => (H - pad) - ((y - yMin) / (yMax - yMin)) * (H - pad * 2);

    let svg = "";

    // Grid lines (4 horizontal, 4 vertical)
    for (let i = 0; i <= 4; i++) {
      const gy = pad + (i / 4) * (H - pad * 2);
      svg += `<line x1="${pad}" y1="${gy}" x2="${W - pad}" y2="${gy}" stroke="rgba(255,255,255,0.06)" stroke-width="1"/>`;
      const gx = pad + (i / 4) * (W - pad * 2);
      svg += `<line x1="${gx}" y1="${pad}" x2="${gx}" y2="${H - pad}" stroke="rgba(255,255,255,0.06)" stroke-width="1"/>`;
    }

    // Axis tick labels (min/max)
    svg += `<text x="${pad}" y="${H - 4}" fill="rgba(255,255,255,0.35)" font-size="10" text-anchor="start">${fmtNum(xMin)}</text>`;
    svg += `<text x="${W - pad}" y="${H - 4}" fill="rgba(255,255,255,0.35)" font-size="10" text-anchor="end">${fmtNum(xMax)}</text>`;
    svg += `<text x="${pad - 4}" y="${H - pad}" fill="rgba(255,255,255,0.35)" font-size="10" text-anchor="end">${fmtNum(yMin)}</text>`;
    svg += `<text x="${pad - 4}" y="${pad + 4}" fill="rgba(255,255,255,0.35)" font-size="10" text-anchor="end">${fmtNum(yMax)}</text>`;

    // Curve path
    const pathD = points.map((p, i) =>
      `${i === 0 ? "M" : "L"} ${sx(p.x).toFixed(2)} ${sy(p.y).toFixed(2)}`
    ).join(" ");
    svg += `<path d="${pathD}" class="dfbnb-curves__svgPath"/>`;

    // Soft cap marker
    if (softCap) {
      const scx = sx(softCap.x).toFixed(2);
      const scy = sy(softCap.y).toFixed(2);
      svg += `<line x1="${scx}" y1="${pad}" x2="${scx}" y2="${H - pad}" stroke="rgba(255,107,107,0.45)" stroke-width="1" stroke-dasharray="6,4"/>`;
      svg += `<circle cx="${scx}" cy="${scy}" r="5" fill="#ff6b6b" stroke="rgba(0,0,0,0.5)" stroke-width="1.5"/>`;
      svg += `<text x="${Number(scx) + 8}" y="${Number(scy) - 8}" fill="#ff6b6b" font-size="10" font-weight="bold">Soft Cap</text>`;
    }

    // Hover interaction: invisible circles + tooltip group
    const step = Math.max(1, Math.floor(points.length / 40));
    for (let i = 0; i < points.length; i += step) {
      const p = points[i];
      const cx = sx(p.x).toFixed(2);
      const cy = sy(p.y).toFixed(2);
      svg += `<circle cx="${cx}" cy="${cy}" r="12" fill="transparent" class="dfbnb-curves__svgHitArea" data-tip-x="${fmtNum(p.x)}" data-tip-y="${fmtNum(p.y)}"/>`;
      svg += `<circle cx="${cx}" cy="${cy}" r="3" fill="var(--accent)" opacity="0" class="dfbnb-curves__svgDot"/>`;
    }

    // Tooltip element (positioned via JS on hover)
    svg += `<g class="dfbnb-curves__svgTooltip" style="display:none"><rect rx="4" ry="4" fill="rgba(0,0,0,0.85)" stroke="var(--accent)" stroke-width="1"/><text fill="white" font-size="11" font-weight="bold"></text></g>`;

    svgEl.innerHTML = svg;

    // Wire hover
    svgEl.addEventListener("pointerenter", function handler(e) {
      const hit = e.target.closest(".dfbnb-curves__svgHitArea");
      if (!hit) return;
      const dot = hit.nextElementSibling;
      if (dot) dot.style.opacity = "1";
    }, true);
    svgEl.addEventListener("pointerleave", function handler(e) {
      const hit = e.target.closest(".dfbnb-curves__svgHitArea");
      if (!hit) return;
      const dot = hit.nextElementSibling;
      if (dot) dot.style.opacity = "0";
      const tip = svgEl.querySelector(".dfbnb-curves__svgTooltip");
      if (tip) tip.style.display = "none";
    }, true);
    svgEl.addEventListener("pointermove", function handler(e) {
      const hit = e.target.closest(".dfbnb-curves__svgHitArea");
      const tip = svgEl.querySelector(".dfbnb-curves__svgTooltip");
      if (!hit || !tip) { if (tip) tip.style.display = "none"; return; }
      const tx = hit.getAttribute("data-tip-x");
      const ty = hit.getAttribute("data-tip-y");
      const txt = tip.querySelector("text");
      const rect = tip.querySelector("rect");
      txt.textContent = `${tx}, ${ty}`;
      const cx = Number(hit.getAttribute("cx"));
      const cy = Number(hit.getAttribute("cy"));
      const tw = txt.textContent.length * 6.5 + 16;
      const th = 22;
      const tipX = Math.min(cx + 10, W - tw - 4);
      const tipY = Math.max(cy - th - 8, 2);
      rect.setAttribute("x", tipX);
      rect.setAttribute("y", tipY);
      rect.setAttribute("width", tw);
      rect.setAttribute("height", th);
      txt.setAttribute("x", tipX + 8);
      txt.setAttribute("y", tipY + 15);
      tip.style.display = "";
    }, true);
  }

  async function resolveCurvePoints(index, curveStub) {
    const cat = curveStub.category;
    const chunkFiles = (index.chunks && index.chunks[cat]) ? index.chunks[cat] : [];
    if (!chunkFiles.length) throw new Error("no_chunks_for_" + cat);

    for (const rel of chunkFiles) {
      const url = BASE_URL + "/" + rel;
      const chunk = await loadChunk(url);
      const found = (chunk && Array.isArray(chunk.curves))
        ? chunk.curves.find(c => c.id === curveStub.id)
        : null;
      if (found) return found;
    }
    throw new Error("curve_not_found_in_chunks_" + curveStub.id);
  }

  /* ─── Main index list ──────────────────────────────────────────────────── */

  function renderList(els, index) {
    const q   = safeText(els.search.value).toLowerCase();
    const cat = safeText(els.category.value) || "__all__";
    const sort = safeText(els.sort.value) || "browse";

    let list = index.curves.slice();
    if (cat !== "__all__") list = list.filter(c => c.category === cat);
    if (q) list = list.filter(c =>
      safeText(c.edid).toLowerCase().includes(q) ||
      safeText(c.id).toLowerCase().includes(q)
    );
    list = sortCurves(list, sort);

    els.count.textContent = `${list.length.toLocaleString()} curves`;
    els.list.innerHTML = list.map(c => renderCard(c, index)).join("");
  }

  function wireEvents(root, els, index) {
    const rerender = () => renderList(els, index);
    els.search.addEventListener("input", debounce(rerender, 120));
    els.category.addEventListener("change", rerender);
    els.sort.addEventListener("change", rerender);

    els.list.addEventListener("click", async (e) => {
      const btn = e.target.closest("[data-curve-toggle]");
      if (!btn) return;

      const id   = btn.getAttribute("data-curve-toggle");
      const card = els.list.querySelector(`[data-curve-card="${CSS.escape(id)}"]`);
      if (!card) return;

      const expanded = card.getAttribute("data-expanded") === "1";
      const panel    = card.querySelector("[data-curve-panel]");

      if (expanded) {
        card.setAttribute("data-expanded", "0");
        btn.textContent = "Expand";
        panel.innerHTML = "";
        return;
      }

      card.setAttribute("data-expanded", "1");
      btn.textContent = "Collapse";
      panel.innerHTML = renderLoadingPanel();

      try {
        const stub = index.curves.find(c => c.id === id);
        if (!stub) throw new Error("stub_not_found");
        const full = await resolveCurvePoints(index, stub);
        panel.innerHTML = renderCurvePanel(full);
        drawCurveSvg(panel.querySelector(`[data-curve-svg="${CSS.escape(id)}"]`), full.points, full.softCap);
      } catch {
        panel.innerHTML = renderErrorPanel("Failed to load curve points.");
      }
    });
  }

  /* ─── Perk Cards page helpers ──────────────────────────────────────────── */

  function perkGroupKey(name) {
    const n = safeText(name);
    if (/^LGN_/i.test(n))         return "legendary";
    if (/^(zzz_)?GHL_/i.test(n))  return "ghoul";
    return "normal";
  }

  function perkGroupTitle(key) {
    if (key === "legendary") return "Legendary Perk Cards";
    if (key === "ghoul")     return "Ghoul Perk Cards";
    return "Perk Cards";
  }

  /* ─── mount ────────────────────────────────────────────────────────────── */

  API.mount = async function mount(pathname) {
    // Use the pathname guide.js passes — do NOT fall back to location.pathname here.
    const path = String(pathname || location.pathname || "");

    if (!isCurvesMasterIndexPath(path) && !isPerkCardsPath(path)) return;

    const body = byId("dfbnbGuideBody") || document.body;
    if (!body) return;

    let host = byId("dfbnbCurves");
    if (!host) {
      host = document.createElement("div");
      host.id = "dfbnbCurves";
      body.appendChild(host);
    }

    host.innerHTML = renderShell();

    const root = host.querySelector(".dfbnb-curves");
    const els = {
      search:   root.querySelector("[data-curves-search]"),
      category: root.querySelector("[data-curves-category]"),
      sort:     root.querySelector("[data-curves-sort]"),
      count:    root.querySelector("[data-curves-count]"),
      list:     root.querySelector("[data-curves-list]"),
    };

    try {
      /* ── Perk Cards page ──────────────────────────────────────── */
      if (isPerkCardsPath(path)) {
        const perkData = await loadPerkCards();

        els.category.style.display = "none";
        els.sort.style.display     = "none";
        els.search.placeholder     = "Search perk card name...";

        // Expand All / Close All
        const bar     = root.querySelector(".dfbnb-curves__bar");
        const controls = root.querySelector(".dfbnb-curves__controls");
        const btnRow  = document.createElement("div");
        btnRow.className = "dfbnb-curves__meta";
        btnRow.innerHTML = `
          <button class="dfbnb-curves__btn" data-action="expand-all">Expand All</button>
          <button class="dfbnb-curves__btn dfbnb-curves__btnSecondary" data-action="collapse-all">Close All</button>
        `;
        bar.insertBefore(btnRow, controls.nextSibling);

        const perks = Array.isArray(perkData.perks) ? perkData.perks : [];

        // Set UI_KEY for perk cards page
        UI_KEY = "dfbnb_ui:curves:perk_cards";
        uiState = Object.assign(uiState, uiLoad());

        // Group into legendary / ghoul / normal
        const groups = { legendary: [], ghoul: [], normal: [] };
        for (const p of perks) {
          const k = perkGroupKey((p && (p.pcrdEdid || p.edid || p.name)) || "");
          groups[k].push(p);
        }
        for (const k of Object.keys(groups)) {
          groups[k].sort((a, b) =>
            safeText(a?.name).toLowerCase().localeCompare(safeText(b?.name).toLowerCase())
          );
        }

        els.count.textContent = `${perks.length.toLocaleString()} perk cards`;

        const groupOrder = ["legendary", "ghoul", "normal"];
        els.list.innerHTML = groupOrder.map(gKey => {
          const title = perkGroupTitle(gKey);
          const arr   = groups[gKey] || [];
          const gid   = escapeAttr("group_" + gKey);
          return `
            <div class="dfbnb-curves__card" data-perk-group="${gid}" data-expanded="0">
              <div class="dfbnb-curves__cardTop">
                <div class="dfbnb-curves__cardMain">
                  <div class="dfbnb-curves__edid">${escapeHtml(title)}</div>
                  <div class="dfbnb-curves__meta">
                    <span class="dfbnb-curves__pill">${arr.length.toLocaleString()} perk cards</span>
                  </div>
                </div>
                <div class="dfbnb-curves__cardActions">
                  <button class="dfbnb-curves__btn" data-group-toggle="${gid}">Expand</button>
                </div>
              </div>
              <div class="dfbnb-curves__panel" data-group-panel style="display:none">
                <div class="dfbnb-curves__list" data-group-list>
                  ${arr.map((p, i) => {
                    const pid = escapeAttr(p.pcrdFormId || (gKey + "_" + String(i)));
                    return `
                      <div class="dfbnb-curves__card"
                           data-perk-card="${pid}"
                           data-expanded="0"
                           data-perk-name="${escapeAttr(p.name || "")}">
                        <div class="dfbnb-curves__cardTop">
                          <div class="dfbnb-curves__cardMain">
                            <div class="dfbnb-curves__edid">${escapeHtml(p.name || "(no name)")}</div>
                            <div class="dfbnb-curves__meta">
                              <span class="dfbnb-curves__pill">Perk Card</span>
                              <span class="dfbnb-curves__pill">FormID: ${escapeHtml(p.pcrdFormId || "")}</span>
                              <span class="dfbnb-curves__pill">${(p.curves ? p.curves.length : 0).toLocaleString()} curves</span>
                            </div>
                          </div>
                          <div class="dfbnb-curves__cardActions">
                            <button class="dfbnb-curves__btn" data-perk-toggle="${pid}">Expand</button>
                          </div>
                        </div>
                        <div class="dfbnb-curves__panel" data-perk-panel></div>
                      </div>
                    `;
                  }).join("")}
                </div>
              </div>
            </div>
          `;
        }).join("");

        // Search filter for perk cards
        els.search.addEventListener("input", debounce(() => {
          const q = safeText(els.search.value).toLowerCase();
          let shown = 0;
          els.list.querySelectorAll("[data-perk-card]").forEach(card => {
            const name = safeText(card.getAttribute("data-perk-name")).toLowerCase();
            const ok   = !q || name.includes(q);
            card.style.display = ok ? "" : "none";
            if (ok) shown++;
          });
          els.count.textContent = `${shown.toLocaleString()} perk cards`;
        }, 120));

        // Click delegation: Expand All / Close All, group toggles, perk toggles, curve toggles
        els.list.addEventListener("click", async (e) => {

          // Expand All / Close All
          const actionBtn = e.target.closest("[data-action]");
          if (actionBtn) {
            const act = actionBtn.getAttribute("data-action");
            const allGroups = els.list.querySelectorAll("[data-perk-group]");
            const allPerks  = els.list.querySelectorAll("[data-perk-card]");

            const setGroupOpen = (groupCard, open) => {
              const gid   = groupCard.getAttribute("data-perk-group");
              const panel = groupCard.querySelector("[data-group-panel]");
              const btn   = groupCard.querySelector(`[data-group-toggle="${CSS.escape(gid)}"]`);
              groupCard.setAttribute("data-expanded", open ? "1" : "0");
              if (btn)   btn.textContent        = open ? "Collapse" : "Expand";
              if (panel) panel.style.display    = open ? "" : "none";
            };

            const setPerkOpen = (perkCard, open) => {
              const pid   = perkCard.getAttribute("data-perk-card");
              const btn   = perkCard.querySelector(`[data-perk-toggle="${CSS.escape(pid)}"]`);
              const panel = perkCard.querySelector("[data-perk-panel]");
              perkCard.setAttribute("data-expanded", open ? "1" : "0");
              if (btn)          btn.textContent = open ? "Collapse" : "Expand";
              if (!open && panel) panel.innerHTML = "";
            };

            if (act === "expand-all") {
              allGroups.forEach(g => setGroupOpen(g, true));
              allPerks.forEach(p  => setPerkOpen(p, false));
              uiSave(uiState);
              return;
            }
            if (act === "collapse-all") {
              allPerks.forEach(p  => setPerkOpen(p, false));
              allGroups.forEach(g => setGroupOpen(g, false));
              uiSave(uiState);
              return;
            }
          }

          // Group expand (show/hide, keep contents)
          const groupBtn = e.target.closest("[data-group-toggle]");
          if (groupBtn) {
            const gid       = groupBtn.getAttribute("data-group-toggle");
            const groupCard = els.list.querySelector(`[data-perk-group="${CSS.escape(gid)}"]`);
            if (!groupCard) return;
            const expanded  = groupCard.getAttribute("data-expanded") === "1";
            const panel     = groupCard.querySelector("[data-group-panel]");
            if (!panel) return;
            groupCard.setAttribute("data-expanded", expanded ? "0" : "1");
            groupBtn.textContent   = expanded ? "Expand" : "Collapse";
            panel.style.display    = expanded ? "none" : "";
            return;
          }

          // Perk card expand
          const perkBtn = e.target.closest("[data-perk-toggle]");
          if (perkBtn) {
            const pid      = perkBtn.getAttribute("data-perk-toggle");
            const card     = els.list.querySelector(`[data-perk-card="${CSS.escape(pid)}"]`);
            if (!card) return;
            const expanded = card.getAttribute("data-expanded") === "1";
            const panel    = card.querySelector("[data-perk-panel]");
            if (expanded) {
              card.setAttribute("data-expanded", "0");
              perkBtn.textContent = "Expand";
              panel.innerHTML = "";
              return;
            }
            card.setAttribute("data-expanded", "1");
            perkBtn.textContent = "Collapse";

            // Find the perk by FormID (the pid was encoded with escapeAttr)
            const perk = perks.find(x => escapeAttr(x.pcrdFormId || "") === pid) ||
                         perks.find(x => escapeAttr(x.name || "") === (card.getAttribute("data-perk-name") || ""));
            const curves = (perk && Array.isArray(perk.curves)) ? perk.curves : [];
            panel.innerHTML = `
              <div class="dfbnb-curves__list">
                ${curves.map(c => renderCard(c, { categories: [] })).join("")}
              </div>
            `;
            return;
          }

          // Sub-curve expand (inside a perk)
          const btn = e.target.closest("[data-curve-toggle]");
          if (!btn) return;
          const id       = btn.getAttribute("data-curve-toggle");
          // Walk up from btn to find the closest curve card — may be inside a perk panel
          const card     = btn.closest("[data-curve-card]");
          if (!card) return;
          const expanded = card.getAttribute("data-expanded") === "1";
          const panel    = card.querySelector("[data-curve-panel]");

          if (expanded) {
            card.setAttribute("data-expanded", "0");
            btn.textContent = "Expand";
            panel.innerHTML = "";
            return;
          }

          card.setAttribute("data-expanded", "1");
          btn.textContent = "Collapse";
          panel.innerHTML = renderLoadingPanel();

          try {
            const fakeIndex = { chunks: perkData.chunks || {} };
            let stub = null;
            for (const p of perks) {
              const found = (p.curves || []).find(cc => cc.id === id);
              if (found) { stub = found; break; }
            }
            if (!stub) throw new Error("stub_not_found");
            const full = await resolveCurvePoints(fakeIndex, stub);
            panel.innerHTML = renderCurvePanel(full);
            drawCurveSvg(panel.querySelector(`[data-curve-svg="${CSS.escape(id)}"]`), full.points, full.softCap);
          } catch {
            panel.innerHTML = renderErrorPanel("Failed to load curve points.");
          }
        });

        return;
      }

      /* ── Master index page ────────────────────────────────────── */
      UI_KEY = "dfbnb_ui:curves:index";
      uiState = Object.assign(uiState, uiLoad());

      const index = await loadIndex();
      hydrateFilters(els, index);
      renderList(els, index);
      wireEvents(root, els, index);

    } catch (e) {
      host.innerHTML = renderError("Failed to load curve tables. Check uploads path and JSON outputs.");
    }
  };

  /* ─── prefetch ─────────────────────────────────────────────────────────── */
  API.prefetch = function prefetch(pathname) {
    const path = String(pathname || location.pathname || "");
    if (isCurvesMasterIndexPath(path)) {
      // Warm the index only — chunks are loaded on-demand
      loadIndex().catch(() => {});
    } else if (isPerkCardsPath(path)) {
      loadPerkCards().catch(() => {});
    }
  };

  /* ─── search ───────────────────────────────────────────────────────────── */
  API.search = function search(query) {
    const q = String(query || "").trim().toLowerCase();

    // Remove previous highlights
    const host = byId("dfbnbCurves");
    if (!host) return;
    host.querySelectorAll("mark.dfbnb-mark").forEach(m => {
      m.replaceWith(document.createTextNode(m.textContent));
    });

    if (!q) return;

    // Highlight text in EDID and pill nodes using TreeWalker
    const walker = document.createTreeWalker(
      host,
      NodeFilter.SHOW_TEXT,
      {
        acceptNode(node) {
          // Only highlight text inside EDID divs and pill spans
          const p = node.parentElement;
          if (!p) return NodeFilter.FILTER_REJECT;
          if (p.closest(".dfbnb-curves__svg")) return NodeFilter.FILTER_REJECT;
          if (p.closest("[data-curve-panel]") && !p.closest("[data-curve-svg]")) {
            return NodeFilter.FILTER_ACCEPT;
          }
          if (p.classList.contains("dfbnb-curves__edid") ||
              p.classList.contains("dfbnb-curves__pill")) {
            return NodeFilter.FILTER_ACCEPT;
          }
          return NodeFilter.FILTER_REJECT;
        }
      }
    );

    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);

    let firstMark = null;
    for (const node of nodes) {
      const text = node.textContent;
      const idx  = text.toLowerCase().indexOf(q);
      if (idx === -1) continue;
      const before = document.createTextNode(text.slice(0, idx));
      const mark   = document.createElement("mark");
      mark.className   = "dfbnb-mark";
      mark.textContent = text.slice(idx, idx + q.length);
      const after = document.createTextNode(text.slice(idx + q.length));
      node.replaceWith(before, mark, after);
      if (!firstMark) firstMark = mark;
    }

    if (firstMark) {
      try { firstMark.scrollIntoView({ block: "center", behavior: "smooth" }); }
      catch { firstMark.scrollIntoView(true); }
    }
  };

})();
