/* df-bnb-activities.js
   DF/BNB Activities Rewards — March 2026

   Handles only Activities reward pages.
   Detects pages via URL paths containing "/df/activities/" and isActivityPage() function.
*/

(() => {
  "use strict";

  const MODULE_ID = "activities";
  const VERSION   = "2026-03-MOCKUP-V3";

  // ── Item image gallery config ──────────────────────────────────────────
  // Maps lowercase item names to an array of { src, alt } image objects.
  // Images render inside a clickable expand row directly beneath the item.
  const ITEM_IMAGES = {
    "plan: brotherhood civvies": [
      { src: "/wp-content/uploads/guide-images/activities/surface-to-air/FO76SR_Brotherhood_civvies.avif",      alt: "Brotherhood Civvies — Front" },
      { src: "/wp-content/uploads/guide-images/activities/surface-to-air/FO76SR_Brotherhood_civvies_back.avif",  alt: "Brotherhood Civvies — Back" },
    ],
    "blue ridge caravan outfit": [
      { src: "/wp-content/uploads/guide-images/activities/riding-shotgun/FO76WL_Blue_Ridge_Caravan_outfit.avif",      alt: "Blue Ridge Caravan Outfit — Front" },
      { src: "/wp-content/uploads/guide-images/activities/riding-shotgun/FO76WL_Blue_Ridge_Caravan_outfit_back.avif", alt: "Blue Ridge Caravan Outfit — Back" },
    ],
    "blue ridge caravan gas mask": [
      { src: "/wp-content/uploads/guide-images/activities/riding-shotgun/FO76WL_Blueridge_Caravan_gas_mask.avif", alt: "Blue Ridge Caravan Gas Mask" },
    ],
  };

  // Render an image gallery row that spans the full table width.
  // Returns a <tr> containing a <details> expand with the images inside.
  function renderImageGalleryRow(images, colSpan) {
    const details = document.createElement("details");
    details.style.cssText = "margin: 4px 0 8px; cursor: pointer;";
    const summary = document.createElement("summary");
    summary.textContent = "\uD83D\uDDBC\uFE0F View preview";
    summary.style.cssText = "font-size: 0.85em; color: #2c6e49; font-style: italic; padding: 2px 0; user-select: none;";
    details.appendChild(summary);
    const gallery = document.createElement("div");
    gallery.style.cssText = "display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; padding: 10px 0;";
    images.forEach(img => {
      const imgEl = document.createElement("img");
      imgEl.src = img.src;
      imgEl.alt = img.alt || "";
      imgEl.loading = "lazy";
      imgEl.style.cssText = "max-width: 280px; width: 100%; height: auto; border-radius: 6px; border: 1px solid #d5c9a1;";
      gallery.appendChild(imgEl);
    });
    details.appendChild(gallery);
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.setAttribute("colspan", colSpan);
    td.style.cssText = "padding: 0 8px 4px; border: none;";
    td.appendChild(details);
    tr.appendChild(td);
    return tr;
  }

  // ── Label mapping (internal EDID labels → public-facing names) ──────────
  const LABEL_MAP = {
    "Scrip":                                  "Legendary Scrip",
    "Mods Weapons Ranged Gatling Plasma":     "Gatling Plasma Mod Plans",
    "Public Event Rewards Caps":              "Caps",
    "Public Event Rewards Legendary Items":   "Legendary Items",
    "U-Mine-It Maps":                         "U Mine It Maps",
    // NOTE: Legendary, Regional, and Underarmour labels are now handled generatively
    // by prettify_lvli_label in the Python build script. Static overrides removed.
  };

  // Item-name prettification: ONLY safe transforms for names inside item tables.
  // Does NOT strip ammo types or collapse underarmour — those are heading-level only.
  function prettifyItemName(name) {
    if (!name) return name;
    let m;
    if ((m = name.match(/^Legendary\s+Items?\s+(?:Armor|Armou?r)\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Armour Piece`;
    if ((m = name.match(/^Legendary\s+Items?\s+Power\s*(?:Armor|Armou?r)\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Power Armour Piece`;
    if ((m = name.match(/^Legendary\s+Items?\s+Weapons?\s+Melee\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Melee Weapon`;
    if ((m = name.match(/^Legendary\s+Items?\s+Weapons?\s+Ranged\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Ranged Weapon`;
    if ((m = name.match(/^Legendary\s+Items?\s+Weapons?\s+(?:Any\s+)?Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Weapon`;
    if ((m = name.match(/^Legendary\s*(\d+)\s*Star$/i)))
      return `Legendary Items (${m[1]}\u2605)`;
    return name;
  }

  // Generative label transform: applied AFTER LABEL_MAP lookup (fallback for labels
  // that the Python build script already prettified but still need final polish).
  function transformLabel(label) {
    // Already in LABEL_MAP?
    if (LABEL_MAP[label]) return LABEL_MAP[label];
    // Legendary Items names → clean display names with star rating
    let m;
    if ((m = label.match(/^Legendary\s+Items?\s+(?:Armor|Armou?r)\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Armour Piece`;
    if ((m = label.match(/^Legendary\s+Items?\s+Power\s*(?:Armor|Armou?r)\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Power Armour Piece`;
    if ((m = label.match(/^Legendary\s+Items?\s+Weapons?\s+Melee\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Melee Weapon`;
    if ((m = label.match(/^Legendary\s+Items?\s+Weapons?\s+Ranged\s+Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Ranged Weapon`;
    if ((m = label.match(/^Legendary\s+Items?\s+Weapons?\s+(?:Any\s+)?Rank\s*(\d+)$/i)))
      return `Legendary ${m[1]}\u2605 Weapon`;
    // Legendary N Star → Legendary Items (N★)
    if ((m = label.match(/^Legendary\s*(\d+)\s*Star$/i)))
      return `Legendary Items (${m[1]}\u2605)`;
    // Underarmour plans: show friendly title, raw plan name goes in expand body
    if (/^Plan:\s.*Underarmou?r\s+Style/i.test(label) || /Underarmou?r.*Style/i.test(label))
      return "Underarmour Style Plan";
    // Ammo heading labels: strip to just "Ammo" for top-level expand headings
    // Only match heading-style labels (e.g. "Ammo Fusion Cell"), not item names
    if (/^Ammo\s+\w/i.test(label)) return "Ammo";
    // NOTE: "Unique Activity Rewards" merging is now handled by the isUniqueReward
    // flag on tree nodes (set by the Python build).  Individual node labels are kept
    // so they can appear as sub-expand titles inside the merged section.
    return label;
  }

  // ── Condition string → plain-English translations ──────────────────────
  const CONDITION_LABELS = {
    "Requires: One of Us":                          "Requires the Enclave quest \u201COne of Us\u201D to be completed",
    "Toggle: Double Legendary Item":                "Bethesda toggle \u2014 double legendary drop (may not always be active)",
    "Toggle: Gold Treasury Note Loot Enabled":      "Bethesda toggle \u2014 Treasury Note drops (may not always be active)",
    "Toggle: Public Events Bobbleheads":            "Bethesda toggle \u2014 Bobblehead drops (may not always be active)",
    "Toggle: Public Events Stable Flux":            "Bethesda toggle \u2014 Stable Flux drops (may not always be active)",
    "Toggle: U-Mine-It Map":                        "Bethesda toggle \u2014 U Mine It Map drops (may not always be active)",
    "Toggle: P62 Lcp The Drifter Quest Keycard Enabled": "Requires Drifter quest keycard system to be enabled",
  };

  // Pattern-based condition translations for dynamic conditions
  const CONDITION_PATTERNS = [
    // Python now produces "Requires Plan: X to be learned" or "Won\u2019t drop if..."
    // This is a fallback for any raw HasLearnedRecipe strings that slip through.
    [/^HasLearnedRecipe\b/i,            "Requires the base plan to be learned"],
    // Pass through already-translated strings unchanged
    [/^Requires Plan:/i,                (m) => m.input],
    [/^Won\u2019t drop if you\u2019ve already learned/i, (m) => m.input],
    [/^\d+%\s+chance\s+to\s+drop$/i,   (m) => m.input],
    [/^GetLevel\(\).*?(\d+)/i,          (m) => `Requires player level ${m[1]}+`],
    [/^GetIsPlayerGhoul.*?1\.0/i,       "Ghoul character only"],
    [/^GetIsPlayerGhoul.*?0\.0/i,       "Human character only"],
    [/^GetIsPlayerGhoul/i,              "Ghoul character only"],  // generic fallback
    [/^GetQuestCompleted.*?One of Us/i,  "Requires the Enclave quest \u201COne of Us\u201D"],
    [/^GetInCurrentLocation.*?"([^"]+)"/i, (m) => `Region: ${m[1]}`],
    [/^PlayerHasQuest/i,                "Requires a specific active quest"],
    [/^GetNumTimesCompletedQuest/i,     "Requires quest completion"],
    [/^GetStageDoneUniqueQuest/i,       "Requires unique quest stage"],
    [/^GetStageDoneCurrentInstance/i,   "Requires current instance quest stage"],
    [/^HasEntitlement.*?0\.0/i,         null],  // Internal — hide (Atom Shop check)
    [/^HasEntitlement/i,               null],  // Internal — hide (generic fallback)
    [/^GetPublicEventHasMutation/i,     "Requires a specific Daily Ops mutation"],
    [/^IsActivePlayer/i,               null],  // Internal condition — hide
    [/^GetVMQuestVariable/i,           null],  // Internal — hide
    [/^GetGlobalValue/i,               null],  // Internal — hide
    [/^GetItemCount/i,                 null],  // Internal — hide
    [/^GetValue\b/i,                   null],  // Internal — hide
    [/^Subject\./i,                    null],  // Raw xEdit condition string — hide
    [/^Toggle:/i,                      (m) => `Bethesda toggle \u2014 ${m.input.replace(/^Toggle:\s*/i, "")} (may not always be active)`],
  ];

  // Scrub internal game-data noise out of plan names embedded in condition strings.
  // Handles raw EDID artefacts like "CAMPTitle", "PlayerTitle", "co Cond Proxy".
  function cleanPlanName(str) {
    return str
      .replace(/\bco\s+Cond\s+Proxy\b/gi, "")     // strip xEdit proxy noise
      .replace(/\bCAMPTitle\b/g, "")                // remove raw EDID token
      .replace(/\bPlayerTitle\b/g, "")              // remove raw EDID token
      .replace(/\bCAMP\s+Title\b/gi, "")            // remove expanded EDID token
      .replace(/\bPlayer\s+Title\b/gi, "")          // remove expanded EDID token
      .replace(/\b(?:Prefix|Suffix|Both)\b/gi, "")  // strip PLYT/CMPT markers
      .replace(/\b[A-Z][a-z]?\d{1,3}[A-Za-z]?\b/g, "")  // quest IDs: E01B, FF06, Sr01
      .replace(/\b(?:Bo|MOON|Storm|NWOT|ATX)\b/gi, "")   // known quest/system prefixes
      .replace(/\bmod\b/gi, "")                     // strip "mod" noise
      .replace(/\bworkshop\b/gi, "")                // strip "workshop" noise
      .replace(/\bDisplays?\b/gi, "")               // strip "Display/Displays" noise
      .replace(/\bmaterial\b/gi, "")                 // strip "material" noise
      .replace(/\s{2,}/g, " ")                      // collapse extra spaces
      .replace(/\s+([,.:])/, "$1")                  // tidy space-before-punctuation
      .replace(/Plan:\s*$/i, "Plan: Unknown")        // safety: don't return empty plan
      .trim();
  }

  function translateCondition(cond) {
    const s = String(cond || "").trim();
    if (!s) return "";
    // Exact match first
    if (CONDITION_LABELS[s]) return CONDITION_LABELS[s];
    // Pattern match
    for (const [pattern, replacement] of CONDITION_PATTERNS) {
      const m = s.match(pattern);
      if (m) {
        if (replacement === null) return "";  // hide internal conditions
        const result = typeof replacement === "function" ? replacement(m) : replacement;
        // Clean plan name noise out of any condition that references a plan
        return /Plan:|Recipe:/i.test(result) ? cleanPlanName(result) : result;
      }
    }
    // Also clean raw pass-through strings that reference plans
    return /Plan:|Recipe:/i.test(s) ? cleanPlanName(s) : s;
  }

  // Clean condition strings: filter internal game conditions, translate, remove empties
  function cleanConditions(conditions) {
    return (conditions || [])
      .filter(c => {
        const s = String(c || "").trim();
        return s && !/GetRandomPercent/i.test(s);
      })
      .map(c => translateCondition(c))
      .filter(c => c !== "");
  }

  // No truncation — show all items in every list
  const MAX_PREVIEW_ITEMS = Infinity;

  // ── Page guards ────────────────────────────────────────────────────────────

  function getBodyDataset() {
    return (document.body && document.body.dataset) ? document.body.dataset : {};
  }

  function stripTrailingSlash(p) {
    const s = String(p || "").trim();
    return (s.length > 1 && s.endsWith("/")) ? s.slice(0, -1) : s;
  }

  function slugFromPathname() {
    const parts = stripTrailingSlash(window.location.pathname || "").split("/").filter(Boolean);
    return parts.length ? parts[parts.length - 1] : "";
  }

  function isActivityPage(data) {
    const t = String(data?.type || data?.eventType || data?.pageType || "").toLowerCase();
    if (t === "activity" || t === "activities") return true;
    const path = String(window.location.pathname || "").toLowerCase();
    return path.includes("/activit");
  }

  function isEventsRewardsPage(data) {
    const path = String(window.location.pathname || "").toLowerCase();
    if (!path.includes("/df/activities/")) return false;
    return isActivityPage(data || {});
  }

  function getPageId() {
    const pid = (getBodyDataset().pageId || getBodyDataset().pageID || "").toString().trim();
    return pid || `page:${stripTrailingSlash(window.location.pathname)}`;
  }

  function lsKey(pageId, key) { return `${pageId}::${MODULE_ID}::${key}`; }

  // ── Root injection ─────────────────────────────────────────────────────────

  function ensureRootEl() {
    let r = document.querySelector("[data-dfbnb-activities-root]");
    if (r) return r;
    const candidates = [
      "#dfbnbGuideBody",".dfbnb-guideBody","[data-dfbnb-guide-body]",
      ".dfbnb-guide__body",".dfbnb-guide-body",".dfbnb-guide__content",
      ".dfbnb-guide-content","main","article","body"
    ];
    let host = null;
    for (const sel of candidates) { const e = document.querySelector(sel); if (e) { host = e; break; } }
    r = document.createElement("div");
    r.setAttribute("data-dfbnb-activities-root", "");
    try {
      const after = host.querySelector(".dfbnb-guide__section,.dfbnb-guide__panel,.dfbnb-guide__card");
      if (after && after.parentNode === host) host.insertBefore(r, after.nextSibling);
      else host.appendChild(r);
    } catch { host.appendChild(r); }
    return r;
  }

  // ── Fetch ──────────────────────────────────────────────────────────────────

  async function fetchJson(url) {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`Fetch failed: ${res.status} ${res.statusText}`);
    return res.json();
  }

  function coalesce(...vals) {
    for (const v of vals) if (v !== null && v !== undefined && String(v).trim() !== "") return v;
    return "";
  }

  function getDataUrlsFromPage() {
    const ds = getBodyDataset();
    const byPageUrl = coalesce(
      ds.activitiesRewardsByPageJson, ds.activitiesRewardsJsonByPage,
      ds.activitiesRewardsJson, ds.activitiesRewardsDataUrl,
      // Legacy fallbacks for old data attribute names
      ds.eventsRewardsByPageJson, ds.eventsRewardsJsonByPage,
      ds.eventsRewardsJson, ds.eventsRewardsDataUrl
    );
    const patchlogUrl = coalesce(
      ds.patchlogActivitiesRewardsUrl, ds.activitiesRewardsPatchlogUrl,
      ds.patchlogUrlActivities, ds.patchlogUrl,
      // Legacy fallbacks
      ds.patchlogEventsRewardsUrl, ds.patchlogUrlEvents
    );
    const slug = coalesce(ds.pageSlug, ds.slug, ds.guideSlug, ds.postSlug, slugFromPathname());
    const reg  = window.__DFBNB_JSON_URLS || window.DFBNB_JSON_URLS || {};
    return {
      byPageUrl: byPageUrl || reg.activities_rewards_by_page || reg.activitiesRewardsByPage || reg.events_rewards_by_page || reg.eventsRewardsByPage || "",
      patchlogUrl, slug
    };
  }

  // ── DOM helpers ────────────────────────────────────────────────────────────

  function el(tag, attrs = {}, children = []) {
    const node = document.createElement(tag);
    Object.entries(attrs).forEach(([k, v]) => {
      if (v === null || v === undefined) return;
      if (k === "class") node.className = String(v);
      else if (k === "html") node.innerHTML = String(v);
      else node.setAttribute(k, String(v));
    });
    children.forEach(c => node.appendChild(typeof c === "string" ? document.createTextNode(c) : c));
    return node;
  }

  function fmtPct(n) {
    const x = Number(n);
    if (!isFinite(x)) return "";
    if (x === 0) return "0%";
    // Exact integer: "100%", "50%"
    if (x === Math.floor(x)) return `${x}%`;
    // Tiny values (< 1%): 6 decimal places
    if (x < 1) return `${x.toFixed(6).replace(/0+$/, '').replace(/\.$/, '.0')}%`;
    // Values with repeating decimals or precision needed: use 6dp, trim trailing zeros
    const full = x.toFixed(6);
    const trimmed = full.replace(/0+$/, '').replace(/\.$/, '');
    return `${trimmed}%`;
  }

  function safeText(v) { return String(v ?? "").trim(); }

  // ── Export popup ───────────────────────────────────────────────────────────

  function closeExportPopup() {
    const o = document.getElementById("dfbnbEvrExportOverlay");
    if (o) o.remove();
  }

  function openExportPopup({ mode, onlySelected, data, rootEl }) {
    closeExportPopup();
    const overlay = el("div", { class: "dfbnbEvrOverlay", id: "dfbnbEvrExportOverlay" });
    const pop     = el("div", { class: "dfbnbEvrPopup" });
    const title   = el("div", { class: "dfbnbEvrPopupTitle" }, ["Choose export style"]);
    const row     = el("div", { class: "dfbnbEvrPopupRow" });

    function makeBtn(label, variant) {
      const b = el("button", { type: "button" }, [label]);
      b.addEventListener("click", () => {
        closeExportPopup();
        if (mode === "print") openPosterPrintWindow({ onlySelected, variant, data, rootEl });
        else downloadPosterPNG({ onlySelected, variant, data, rootEl });
      });
      return b;
    }

    const close = el("div", { class: "dfbnbEvrPopupClose" }, ["Cancel"]);
    close.addEventListener("click", closeExportPopup);
    row.append(makeBtn("Colour copy", "colour"), makeBtn("Black & White copy", "bw"));
    pop.append(title, row, close);
    overlay.appendChild(pop);
    overlay.addEventListener("click", e => { if (e.target === overlay) closeExportPopup(); });
    document.body.appendChild(overlay);
  }

  function loadScriptOnce(src) {
    return new Promise((resolve, reject) => {
      if ([...document.scripts].some(s => s.src === src)) return resolve();
      const s = document.createElement("script");
      s.src = src; s.async = true; s.onload = resolve; s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  async function ensureCanvasLib() {
    if (window.html2canvas) return;
    await loadScriptOnce("https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js");
    if (!window.html2canvas) throw new Error("html2canvas missing");
  }

  function posterFileName(variant, eventName) {
    const clean = String(eventName || "event").toLowerCase()
      .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 80) || "event";
    return `activity-rewards-${clean}-${variant === "bw" ? "black-and-white" : "colour"}.png`;
  }

  function escapeHtml(str) {
    return String(str ?? "")
      .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
      .replace(/"/g,"&quot;").replace(/'/g,"&#039;");
  }

  function buildPosterHTML({ onlySelected, variant, data, rootEl }) {
    void onlySelected;
    const title  = safeText(data?.name) || "Activity Rewards";
    const isBW   = variant === "bw";
    const bg     = isBW ? "#ffffff" : "#f5f1e3";
    const ink    = "#1b1b1b";
    const muted  = "#444";
    const box    = isBW ? "#f3f3f3" : "#efe8d2";
    const border = isBW ? "#cfcfcf" : "#d7caa6";

    const sections = [];
    if (rootEl) {
      rootEl.querySelectorAll("details.dfbnb-expand").forEach(d => {
        const t     = (d.querySelector(".dfbnb-expand__title")?.textContent || "").trim();
        const body  = d.querySelector(".dfbnb-expand__body");
        const table = body?.querySelector("table");
        const empty = body?.querySelector(".dfbnb-evr__empty");
        const inner = table ? table.outerHTML
                    : empty ? `<div class="empty">${escapeHtml(empty.textContent || "")}</div>`
                    : `<div class="empty">No data.</div>`;
        if (t) sections.push({ title: t, html: inner });
      });
    }

    return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<title>${escapeHtml(title)} - Rewards</title>
<style>
  *{box-sizing:border-box}body{margin:0;padding:0;font-family:system-ui,-apple-system,sans-serif;color:${ink};background:${bg}}
  .page{width:297mm;min-height:210mm;padding:14mm;background:${bg}}
  .hdr{border:1px solid ${border};background:${box};border-radius:14px;padding:14px}
  .hdr-title{font-weight:950;letter-spacing:.04em;text-transform:uppercase;font-size:18px}
  .hdr-lines{margin-top:10px;font-size:13px;color:${muted};line-height:1.35}.hdr-lines .line{margin-top:4px}
  .stamp{margin-top:10px;font-size:12px;color:${muted};opacity:.85}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
  .sec{border:1px solid ${border};background:${box};border-radius:14px;padding:12px}
  .sec-title{font-weight:950;text-transform:uppercase;letter-spacing:.04em;font-size:14px;opacity:.9;margin-bottom:8px}
  .empty{font-size:13px;color:${muted};opacity:.9}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th,td{text-align:left;padding:8px 6px;border-bottom:1px solid rgba(0,0,0,0.12);vertical-align:top}th{font-weight:900}
  .ftr{margin-top:10px;font-size:11px;color:${muted};opacity:.85}
</style></head><body>
<div class="page">
  <div class="hdr">
    <div class="hdr-title">${escapeHtml(title)} All Rewards</div>
    <div class="stamp">Exported: ${escapeHtml(new Date().toLocaleString())}</div>
  </div>
  <div class="grid">${sections.map(s => `
    <div class="sec">
      <div class="sec-title">${escapeHtml(s.title)}</div>
      <div class="sec-body">${s.html}</div>
    </div>`).join("")}
  </div>
  <div class="ftr">Exported copy may become outdated after game updates or data corrections.</div>
</div></body></html>`;
  }

  function openPosterPrintWindow(opts) {
    const w = window.open("", "_blank");
    if (!w) return;
    w.document.open(); w.document.write(buildPosterHTML(opts)); w.document.close();
    try { w.focus(); } catch {}
    w.onload = () => { try { w.print(); } catch {} };
  }

  async function downloadPosterPNG(opts) {
    await ensureCanvasLib();
    const tmp = el("iframe", {});
    tmp.style.cssText = "position:fixed;left:-9999px;top:0;width:297mm;height:210mm;border:0";
    document.body.appendChild(tmp);
    const doc = tmp.contentDocument;
    doc.open(); doc.write(buildPosterHTML(opts)); doc.close();
    await new Promise(res => setTimeout(res, 450));
    const pageEl = doc.querySelector(".page");
    if (!pageEl) { tmp.remove(); throw new Error("Poster .page not found"); }
    const canvas = await window.html2canvas(pageEl, { backgroundColor: null, scale: 2, useCORS: true });
    const a = el("a", { href: canvas.toDataURL("image/png"),
                        download: posterFileName(opts.variant, safeText(opts.data?.name) || "activity") });
    document.body.appendChild(a); a.click(); a.remove();
    tmp.remove();
  }

  // ── Render helpers ─────────────────────────────────────────────────────────

  function renderWarning(root, title, message) {
    root.appendChild(el("div", { class: "dfbnb-warning" }, [
      el("div", { class: "dfbnb-warning__title" }, [title]),
      el("div", { class: "dfbnb-warning__msg" },   [message])
    ]));
  }

  // ── Expand persistence via localStorage ───────────────────────────────────

  function expandStateKey(pageId, title) {
    return `${pageId}::${MODULE_ID}::expand::${title.replace(/\s+/g, "_")}`;
  }

  function loadExpandState(pageId, title, defaultOpen) {
    try {
      const v = localStorage.getItem(expandStateKey(pageId, title));
      if (v === null) return defaultOpen;
      return v === "1";
    } catch { return defaultOpen; }
  }

  function saveExpandState(pageId, title, open) {
    try { localStorage.setItem(expandStateKey(pageId, title), open ? "1" : "0"); } catch {}
  }

  // ── renderExpand with persist + optional pill ──────────────────────────────

  function renderExpand({ title, subtitle, content, open = false, pageId = "", pill = null, warningNote = null }) {
    const persisted = pageId ? loadExpandState(pageId, title, open) : open;
    const d = el("details", { class: "dfbnb-expand" });
    if (persisted) d.open = true;

    const summary = el("summary", { class: "dfbnb-expand__summary" });
    const titleEl = el("div", { class: "dfbnb-expand__title" }, [title]);
    const row     = el("div", { class: "dfbnb-expand__summary-row" });
    row.appendChild(titleEl);
    if (pill) {
      const pillEl = el("div", { class: "dfbnb-expand__pill" }, [pill]);
      row.appendChild(pillEl);
    }
    summary.appendChild(row);
    if (subtitle) summary.appendChild(el("div", { class: "dfbnb-expand__sub" }, [subtitle]));
    if (warningNote) {
      const warn = el("div", { class: "dfbnb-expand__sub", style: "font-style:italic; opacity:0.65; font-size:0.85em;" }, ["\u26A0 " + warningNote]);
      summary.appendChild(warn);
    }
    d.appendChild(summary);

    const b = el("div", { class: "dfbnb-expand__body" });
    b.appendChild(content);
    d.appendChild(b);

    if (pageId) {
      d.addEventListener("toggle", () => saveExpandState(pageId, title, d.open));
    }
    return d;
  }

  function renderTable(headers, rows) {
    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    headers.forEach(h => trh.appendChild(el("th", { class: "dfbnb-evr__th" }, [h])));
    thead.appendChild(trh);
    const tbody = el("tbody");
    rows.forEach(r => {
      const tr = el("tr");
      r.forEach((cell, idx) => {
        const td = el("td", { class: "dfbnb-evr__cell", "data-col": String(idx) });
        if (cell instanceof Node) td.appendChild(cell);
        else td.textContent = String(cell ?? "");
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(thead); table.appendChild(tbody);
    return table;
  }

  // ── Activity: Base Rewards ─────────────────────────────────────────────────

  function renderActivityBaseRewards(data) {
    const wrap  = el("div", { class: "dfbnb-act-base" });
    const ad    = data?.activityData?.baseRewards;
    const free  = data?.freeRewards || [];

    if (!ad) {
      const tiers    = Array.isArray(data?.baseRewards?.tiers) ? data.baseRewards.tiers : [];
      const baseTier = tiers.find(t => !t.tier || t.tier === "") || tiers[0] || {};
      const rows = [];
      if (baseTier.xp != null) rows.push({ sortKey: "xp", name: "XP", qty: Number(baseTier.xp).toLocaleString(), dropRate: "100%" });
      if (baseTier.caps != null) rows.push({ sortKey: "caps", name: "Caps", qty: Number(baseTier.caps).toLocaleString(), dropRate: "100%" });
      rows.sort((a, b) => a.sortKey.localeCompare(b.sortKey));
      if (!rows.length) { wrap.appendChild(el("div", { class: "dfbnb-evr__empty" }, ["No base reward data found."])); return wrap; }
      const tbl = el("table", { class: "dfbnb-evr__table dfbnb-act-base__table" });
      tbl.appendChild(el("thead", {}, [el("tr", {}, [el("th", { class: "dfbnb-evr__th" }, ["Reward"]), el("th", { class: "dfbnb-evr__th" }, ["Value / Drop Rate"]), el("th", { class: "dfbnb-evr__th" }, ["Qty"])])]));
      const tbody = document.createElement("tbody");
      rows.forEach(r => tbody.appendChild(el("tr", {}, [el("td", { class: "dfbnb-evr__cell dfbnb-evr__cell--label" }, [r.name]), el("td", { class: "dfbnb-evr__cell" }, [r.dropRate]), el("td", { class: "dfbnb-evr__cell" }, [r.qty ?? "—"])])));
      tbl.appendChild(tbody); wrap.appendChild(tbl); return wrap;
    }

    const rows = [];
    const capsBreakdown = ad.capsBreakdown;
    if (capsBreakdown && capsBreakdown.length) {
      const totalCaps = capsBreakdown.reduce((sum, e) => sum + e.caps, 0);
      rows.push({
        sortKey: "caps", name: "Caps",
        value: `Up to ${totalCaps}`,
        dropRate: "Conditional",
        subRows: capsBreakdown.map(e => ({
          label: e.condition ? `↳ ${e.label} (bonus)` : `↳ ${e.label}`,
          value: String(e.caps),
          dropRate: e.condition ? "Bonus" : "100%",
        })),
      });
    } else {
      rows.push({
        sortKey: "caps", name: "Caps",
        value: ad.caps != null ? Number(ad.caps).toLocaleString() : "—",
        dropRate: ad.caps != null ? "100%" : "—",
      });
    }

    const li = ad.legendaryItems || {};
    rows.push({
      sortKey: "legendary items", name: "Legendary Items",
      value: li.rank != null ? `Rank ${li.rank}` : "—",
      dropRate: li.dropRate != null ? `${Number(li.dropRate).toFixed(1)}%` : (li.rank != null ? "100%" : "—"),
    });

    rows.push({
      sortKey: "legendary modules", name: "Legendary Modules",
      value: ad.legendaryModules != null ? `×${ad.legendaryModules}` : "—",
      dropRate: ad.legendaryModules != null ? "100%" : "—",
    });

    const scrip = ad.legendaryScrip || {};
    const scripItems = scrip.items || [];
    if (scripItems.length) {
      rows.push({
        sortKey: "legendary scrip", name: "Legendary Scrip",
        value: "3–5",
        dropRate: "100%",
        subRows: scripItems.map(s => ({
          qty: s.qty, dropRate: s.dropRate != null ? `${Number(s.dropRate).toFixed(1)}%` : "—"
        })),
      });
    } else {
      rows.push({ sortKey: "legendary scrip", name: "Legendary Scrip", value: "—", dropRate: "—" });
    }

    rows.push({
      sortKey: "treasury notes", name: "Treasury Notes",
      value: ad.treasuryNotes != null ? `×${ad.treasuryNotes}` : "—",
      dropRate: ad.treasuryNotes != null ? "100%" : "—",
    });

    const umap = ad.uMineItMaps || {};
    rows.push({
      sortKey: "u mine it maps", name: "U Mine It Maps",
      value: umap.dropRate != null ? "×1" : "—",
      dropRate: umap.dropRate != null ? `${Number(umap.dropRate).toFixed(1)}%` : "—",
    });

    rows.push({
      sortKey: "xp", name: "XP",
      value: ad.xp != null ? Number(ad.xp).toLocaleString() : "—",
      dropRate: ad.xp != null ? "100%" : "—",
    });

    rows.sort((a, b) => a.sortKey.localeCompare(b.sortKey));

    const tbl   = el("table", { class: "dfbnb-evr__table dfbnb-act-base__table" });
    const thead = el("thead", {}, [el("tr", {}, [
      el("th", { class: "dfbnb-evr__th" }, ["Reward"]),
      el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]),
      el("th", { class: "dfbnb-evr__th" }, ["Value"]),
    ])]);
    tbl.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach(row => {
      tbody.appendChild(el("tr", {}, [
        el("td", { class: "dfbnb-evr__cell dfbnb-evr__cell--label" }, [row.name]),
        el("td", { class: "dfbnb-evr__cell dfbnb-act-base__rate" }, [row.dropRate]),
        el("td", { class: "dfbnb-evr__cell" }, [row.value]),
      ]));

      if (row.subRows) {
        row.subRows.forEach(sub => {
          // Support both scrip format ({qty, dropRate}) and caps/labeled format ({label, value, dropRate})
          const subLabel = sub.label || `↳ ×${sub.qty}`;
          const subValue = sub.value || "";
          tbody.appendChild(el("tr", { class: "dfbnb-act-base__subrow" }, [
            el("td", { class: "dfbnb-evr__cell dfbnb-act-base__subindent" }, [subLabel]),
            el("td", { class: "dfbnb-evr__cell dfbnb-act-base__rate" }, [sub.dropRate]),
            el("td", { class: "dfbnb-evr__cell" }, [subValue]),
          ]));
        });
      }
    });

    tbl.appendChild(tbody);
    wrap.appendChild(tbl);
    return wrap;
  }

  // ── Activity: xEdit-style LVLI tree rendering ─────────────────────────────

  function renderLeafReward(node) {
    const wrap = el("div", { class: "dfbnb-tree__leaf" });
    const dr = node.dropRate != null ? fmtPct(node.dropRate) : "100%";
    const qtyStr = node.qty && node.qty > 1 ? ` ×${node.qty}` : "";
    const cleanConds = cleanConditions(node.conditions);
    const condStr = cleanConds.length ? ` (${cleanConds.join("; ")})` : "";

    const row = el("div", { class: "dfbnb-tree__leaf-row" });
    row.appendChild(el("span", { class: "dfbnb-tree__leaf-name" }, [node.name || node.formid || "—"]));
    row.appendChild(el("span", { class: "dfbnb-tree__leaf-meta" }, [`${dr}${qtyStr}${condStr}`]));
    wrap.appendChild(row);
    return wrap;
  }

  function countTreeItems(node) {
    if (!node) return 0;
    const items = (node.items || []).length;
    const childItems = (node.children || []).reduce((sum, c) => sum + countTreeItems(c), 0);
    return items + childItems;
  }

  function renderLeafPlanReward(node, pageId) {
    // Solo Plan:/Recipe: items get expand treatment with drop rate pill
    const rawName = node.name || node.formid || "—";
    const name = transformLabel(rawName);
    const dr = node.dropRate != null ? fmtPct(node.dropRate) : "100%";
    const pillText = node.dropRate != null && node.dropRate < 100 ? dr : null;

    const content = document.createElement("div");
    const pills = el("div", { class: "dfbnb-evr__plan-pills" });

    // Show the actual in-game plan name in a table row (same size/style as Caps items)
    // Use the raw plan name (e.g. "Plan: Operative Underarmor Style")
    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Item"]));
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]));
    thead.appendChild(trh);
    const tbody = el("tbody");
    const tr = el("tr");
    tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [rawName]));
    tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [dr]));
    tbody.appendChild(tr);
    table.appendChild(thead);
    table.appendChild(tbody);
    content.appendChild(table);

    // Tradeable / Non-Tradeable pill
    if (node.tradeable === true) {
      pills.appendChild(el("span", { class: "dfbnb-evr__plan-pill dfbnb-evr__plan-pill--trade-yes" }, ["Tradeable"]));
    } else if (node.tradeable === false) {
      pills.appendChild(el("span", { class: "dfbnb-evr__plan-pill dfbnb-evr__plan-pill--trade-no" }, ["Not Tradeable"]));
    }

    // Conditions — translated to plain English
    const cleanConds = cleanConditions(node.conditions);
    if (cleanConds.length) {
      content.appendChild(el("div", { class: "dfbnb-evr__plan-detail" }, [cleanConds.join("; ")]));
    }

    if (pills.children.length) content.appendChild(pills);

    // Subtitle: "Chance drop of one item from this list · 1 item"
    const subtitleText = (node.dropRate != null && node.dropRate < 100)
      ? `${dr} chance to drop \u00B7 1 item`
      : `Guaranteed drop \u00B7 1 item`;

    return renderExpand({
      title: name,
      subtitle: subtitleText,
      content: content,
      open: false,
      pageId: pageId,
      pill: null,
    });
  }

  // ── Helpers for tree node rendering ───────────────────────────────────────

  // Determine if a pick-one pool is guaranteed (item rates sum to ~100%)
  function isGuaranteedPool(node) {
    if (node.useAll) return false;
    const items = node.items || [];
    const children = node.children || [];
    if (items.length) {
      const sum = items.reduce((s, it) => s + (it.dropRate != null ? Number(it.dropRate) : 0), 0);
      return sum >= 99.5;
    }
    if (children.length) {
      const sum = children.reduce((s, c) => s + (c.entryRate != null ? Number(c.entryRate) : 0), 0);
      return sum >= 99.5;
    }
    return true;
  }

  // Recursively flatten a tree node to a flat array of leaf items
  // with cumulative drop rates calculated by multiplying rates down the tree.
  // parentRate is a 0-1 fraction.
  // Region aliases used by flattenTreeItems and region-aware rendering.
  // Maps lowercase variants (including abbreviations found in LVLI EDIDs) → canonical name.
  const REGION_ALIASES = {
    "the mire": "The Mire", "mire": "The Mire",
    "toxic valley": "Toxic Valley",
    "savage divide": "Savage Divide",
    "ash heap": "Ash Heap", "ashheap": "Ash Heap",
    "cranberry bog": "Cranberry Bog",
    "forest": "Forest",
    "skyline valley": "Skyline Valley",
    "burning springs": "Burning Springs",
  };

  // Extract canonical region name from a label like "Recipes Power Armor Region Cranberry Bog"
  function extractRegionFromLabel(label) {
    const lower = (label || "").toLowerCase();
    // Check longest aliases first so "toxic valley" matches before partial hits
    const sorted = Object.keys(REGION_ALIASES).sort((a, b) => b.length - a.length);
    for (const alias of sorted) {
      if (lower.includes(alias)) return REGION_ALIASES[alias];
    }
    return null;
  }

  function flattenTreeItems(node, parentRate = 1.0, inheritedRegion = null) {
    const results = [];
    const items = node.items || [];
    const children = node.children || [];

    // Detect region from this node's label (e.g. "Recipes Armor Region Cranberry Bog")
    const nodeRegion = extractRegionFromLabel(node.label) || inheritedRegion;

    items.forEach(it => {
      const rate = (it.dropRate != null ? it.dropRate / 100 : 1) * parentRate;
      const rawName = it.name || it.formid || "\u2014";
      const flat = {
        name: rawName,
        qty: it.qty || 1,
        dropRate: rate * 100,
        conditions: (it.conditions || []).filter(c => {
          const s = String(c || "").trim();
          return s && !/GetRandomPercent/i.test(s);
        }),
      };
      // Preserve sig and edid so downstream code can classify items by category
      if (it.sig) flat.sig = it.sig;
      if (it.edid) flat.edid = it.edid;
      // Propagate mod slots and custom mod info for ARMO/WEAP items
      if (it.modSlots) flat.modSlots = it.modSlots;
      if (it.customModName) flat.customModName = it.customModName;
      if (it.customModDescription) flat.customModDescription = it.customModDescription;
      // Tag with source region so downstream code can group by region
      if (nodeRegion) flat._region = nodeRegion;
      results.push(flat);
    });

    children.forEach(child => {
      const childRate = (child.entryRate != null ? child.entryRate / 100 : 1) * parentRate;
      // Inherit child-level conditions to all its descendants
      const childConds = (child.conditions || []).filter(c => {
        const s = String(c || "").trim();
        return s && !/GetRandomPercent/i.test(s);
      });
      const flattened = flattenTreeItems(child, childRate, nodeRegion);
      flattened.forEach(item => {
        if (childConds.length) {
          item.conditions = [...new Set([...childConds, ...item.conditions])];
        }
      });
      results.push(...flattened);
    });

    return results;
  }

  // Measure max nesting depth of a tree node
  function treeDepth(node) {
    const children = node.children || [];
    if (!children.length) return 0;
    return 1 + Math.max(...children.map(c => treeDepth(c)));
  }

  // Helper: get the display name for an item, prefixed with Unique value if present
  // e.g. "Last Bastion - Urban Scout Armour Chest Piece"
  function getModSlotItemName(itemName, modSlots) {
    if (!modSlots || !modSlots.length) return itemName;
    const uniqueSlot = modSlots.find(s => (s.label || "").toLowerCase() === "unique" || (s.label || "").toLowerCase() === "custom");
    if (uniqueSlot && uniqueSlot.value) {
      return `${uniqueSlot.value} ${itemName}`;
    }
    return itemName;
  }

  // Helper: render mod slots as individual table rows (not pills)
  // Format: one row per slot showing "Label  Value"
  // Includes Legendary 4★ N/A if no 4th star is present but lower stars exist
  function renderModSlotRows(tbody, modSlots, colSpan, customModName, customModDescription) {
    if (!modSlots || !modSlots.length) return;

    // Determine which legendary star slots exist
    const legendaryStars = new Set();
    let maxLegendaryStar = 0;
    modSlots.forEach(slot => {
      const m = (slot.label || "").match(/Legendary\s+(\d)/);
      if (m) {
        const n = parseInt(m[1], 10);
        legendaryStars.add(n);
        if (n > maxLegendaryStar) maxLegendaryStar = n;
      }
    });

    // Build ordered list: Custom Mod first, then legendary stars (with N/A gaps), then others
    const orderedSlots = [];

    // 1. Custom Mod — prefer build-script-supplied name, fall back to ObjectTemplate slot value
    if (customModName) {
      orderedSlots.push({ label: "Custom Mod", value: customModName, isCustomMod: true });
    } else {
      const uniqueSlot = modSlots.find(s => {
        const l = (s.label || "").toLowerCase();
        return l === "unique" || l === "custom";
      });
      if (uniqueSlot) {
        orderedSlots.push({ label: "Custom Mod", value: uniqueSlot.value });
      }
    }

    // 2. Add legendary stars in order, filling gaps with N/A
    if (maxLegendaryStar > 0) {
      for (let star = 1; star <= Math.max(maxLegendaryStar, 4); star++) {
        const existing = modSlots.find(s => {
          const m = (s.label || "").match(/Legendary\s+(\d)/);
          return m && parseInt(m[1], 10) === star;
        });
        if (existing) {
          orderedSlots.push({ label: existing.label, value: existing.value });
        } else {
          orderedSlots.push({ label: `Legendary ${star}\u2605`, value: "N/A", isNA: true });
        }
      }
    }

    // 3. Add non-legendary, non-unique slots (Lining, Material, Appearance, etc.)
    modSlots.forEach(slot => {
      const label = (slot.label || "").toLowerCase();
      if (!label.includes("legendary") && label !== "unique" && label !== "custom") {
        orderedSlots.push({ label: slot.label, value: slot.value });
      }
    });

    // Render each slot as a row
    orderedSlots.forEach(slot => {
      const slotRow = el("tr", { class: "dfbnb-evr__modslot-row" });
      const slotCell = el("td", { colspan: colSpan, class: "dfbnb-evr__modslot-cell dfbnb-evr__modslot-cell--row" });
      const labelSpan = el("span", { class: "dfbnb-evr__modslot-row-label" }, [slot.label]);
      const valueSpan = el("span", {
        class: "dfbnb-evr__modslot-row-value" + (slot.isNA ? " dfbnb-evr__modslot-row-value--na" : "")
      }, [slot.value]);
      slotCell.appendChild(labelSpan);
      slotCell.appendChild(document.createTextNode(" "));
      slotCell.appendChild(valueSpan);
      slotRow.appendChild(slotCell);
      tbody.appendChild(slotRow);
      // Custom mod description sub-row
      if (slot.isCustomMod && customModDescription) {
        const descRow = el("tr", { class: "dfbnb-evr__modslot-row" });
        const descCell = el("td", { colspan: colSpan, class: "dfbnb-evr__modslot-cell dfbnb-evr__modslot-desc" });
        descCell.textContent = customModDescription;
        descRow.appendChild(descCell);
        tbody.appendChild(descRow);
      }
    });
  }

  // Render a flat items table with conditions below items
  function renderFlatItemsTable(items, nodeConditions = []) {
    const hasQty = items.some(it => it.qty && it.qty > 1);
    const headers = ["Item"];
    if (hasQty) headers.push("Qty");
    headers.push("Drop Rate");
    const colSpan = String(headers.length);

    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    headers.forEach(h => trh.appendChild(el("th", { class: "dfbnb-evr__th" }, [h])));
    thead.appendChild(trh);
    const tbody = el("tbody");

    // Dedupe and sort items
    const seen = new Set();
    const deduped = items.filter(it => {
      const key = it.name + "|" + (it.qty || 1);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    deduped.sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    deduped.forEach(it => {
      const dr = it.dropRate != null ? it.dropRate : 0;
      const drStr = fmtPct(dr);
      const tr = el("tr");
      const displayName = getModSlotItemName(it.name, it.modSlots);
      tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [displayName]));
      if (hasQty) tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [it.qty > 1 ? `\u00D7${it.qty}` : ""]));
      const drCell = el("td", { class: "dfbnb-evr__cell" });
      if (dr === 0) drCell.appendChild(el("span", { class: "dfbnb-tree__unavailable" }, [drStr]));
      else drCell.textContent = drStr;
      tr.appendChild(drCell);
      tbody.appendChild(tr);

      // Mod slot rows for ARMO/WEAP items (displayed as individual rows, not pills)
      if (it.modSlots && it.modSlots.length) {
        renderModSlotRows(tbody, it.modSlots, colSpan, it.customModName, it.customModDescription);
      }

      // Per-item conditions — translate and filter empties
      const cleanConds = cleanConditions(it.conditions);
      if (cleanConds.length) {
        const labelRow = el("tr");
        labelRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [
          el("span", { class: "dfbnb-act-base__cond-label" }, ["Drop Conditions:"])
        ]));
        tbody.appendChild(labelRow);
        cleanConds.forEach(cond => {
          const condRow = el("tr");
          condRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [cond]));
          tbody.appendChild(condRow);
        });
      }
    });

    // Pool-level conditions (shown after all items)
    const poolConds = cleanConditions(nodeConditions);
    if (poolConds.length) {
      const labelRow = el("tr");
      labelRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [
        el("span", { class: "dfbnb-act-base__cond-label" }, ["Drop Conditions:"])
      ]));
      tbody.appendChild(labelRow);
      poolConds.forEach(cond => {
        const condRow = el("tr");
        condRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [cond]));
        tbody.appendChild(condRow);
      });
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    return table;
  }

  // Helper: ordinal roll suffix (2 → "2nd Roll", 3 → "3rd Roll")
  function ordinalRoll(n) {
    if (n === 2) return "2nd Roll";
    if (n === 3) return "3rd Roll";
    return `${n}th Roll`;
  }

  // Detect if a node wraps a "Regional Loot Pool" (single child that fans out into regions)
  function hasRegionalLootPool(node) {
    const children = node.children || [];
    if (children.length === 1) {
      const child = children[0];
      const cl = (child.label || "").toLowerCase();
      if (cl.includes("regional") || cl.includes("region")) {
        return (child.children || []).length > 1;
      }
    }
    return false;
  }

  // Build an items table with optional truncation (more-row) and per-item + pool-level conditions
  function renderItemsTableMockup(itemsList, poolConditions = [], truncate = true, showQty = false, showPerItemConds = false) {
    const hasQty = showQty || itemsList.some(it => it.qty && it.qty > 1);
    const headers = ["Item"];
    if (hasQty) headers.push("Qty");
    headers.push("Individual Drop Rate");
    const colSpan = String(headers.length);

    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    headers.forEach(h => trh.appendChild(el("th", { class: "dfbnb-evr__th" }, [h])));
    thead.appendChild(trh);
    const tbody = el("tbody");

    // Sort items: by star rating (1★ first, then 2★, then 3★), then alphabetically
    // Use prettifyItemName so that raw "Legendary Items Armor Rank1" names
    // get their star rating extracted correctly for sorting.
    function extractStarRating(name) {
      const pretty = prettifyItemName(name || "");
      const m = (pretty || "").match(/(\d)\u2605/);
      return m ? parseInt(m[1], 10) : 0;
    }
    const sorted = [...itemsList].sort((a, b) => {
      const starA = extractStarRating(a.name), starB = extractStarRating(b.name);
      if (starA !== starB) {
        // Items WITH stars go first (sorted ascending), items without stars go last
        if (starA === 0 && starB !== 0) return 1;
        if (starA !== 0 && starB === 0) return -1;
        return starA - starB;
      }
      return (a.name || "").localeCompare(b.name || "");
    });

    // Always show all items — no truncation
    const shown = sorted;
    const remaining = 0;

    shown.forEach(it => {
      const dr = it.dropRate != null ? it.dropRate : 0;
      const drStr = dr === 0 ? "\u2013" : fmtPct(dr); // en dash for 0%
      const tr = el("tr");
      const baseName = getModSlotItemName(it.name, it.modSlots);
      const nameText = prettifyItemName(baseName) || "\u2014";
      tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [nameText]));
      if (hasQty) tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [`\u00D7${it.qty || 1}`]));
      const drCell = el("td", { class: "dfbnb-evr__cell" });
      if (dr === 0) drCell.appendChild(el("span", { class: "dfbnb-tree__unavailable" }, [drStr]));
      else drCell.textContent = drStr;
      tr.appendChild(drCell);
      tbody.appendChild(tr);

      // Mod slot rows for ARMO/WEAP items (displayed as individual rows, not pills)
      if (it.modSlots && it.modSlots.length) {
        renderModSlotRows(tbody, it.modSlots, colSpan, it.customModName, it.customModDescription);
      }

      // Per-item conditions — only shown in Unique Activity Rewards expands
      if (showPerItemConds) {
        const cleanConds = cleanConditions(it.conditions);
        if (cleanConds.length) {
          const labelRow = el("tr");
          labelRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [
            el("span", { class: "dfbnb-act-base__cond-label" }, ["Drop Conditions:"])
          ]));
          tbody.appendChild(labelRow);
          cleanConds.forEach(cond => {
            const condRow = el("tr");
            condRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [cond]));
            tbody.appendChild(condRow);
          });
        }
      }

      // Item image gallery (e.g. Blue Ridge Caravan outfit/mask previews)
      const uerImgKey = (it.name || "").trim().toLowerCase();
      const uerImages = ITEM_IMAGES[uerImgKey];
      if (uerImages) tbody.appendChild(renderImageGalleryRow(uerImages, colSpan));
    });

    // "+ N more items…" row
    if (remaining > 0) {
      const moreRow = el("tr", { class: "more-row" });
      moreRow.appendChild(el("td", { class: "dfbnb-evr__cell", colspan: colSpan }, [`+ ${remaining} more items\u2026`]));
      tbody.appendChild(moreRow);
    }

    // Pool-level conditions after all items
    const poolConds = cleanConditions(poolConditions);
    if (poolConds.length) {
      const labelRow = el("tr");
      labelRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [
        el("span", { class: "dfbnb-act-base__cond-label" }, ["Drop Conditions:"])
      ]));
      tbody.appendChild(labelRow);
      poolConds.forEach(cond => {
        const condRow = el("tr");
        condRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: colSpan }, [cond]));
        tbody.appendChild(condRow);
      });
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    return table;
  }

  function renderLvliTreeNode(node, pageId, depth = 0, eventRegions = null, seenLabels = null) {
    if (!node) return document.createDocumentFragment();
    if (!seenLabels) seenLabels = new Map();

    // Leaf item (non-LVLI direct reward)
    if (node.type === "leaf") {
      const itemName = String(node.name || "");
      if (itemName.startsWith("Plan:") || itemName.startsWith("Recipe:")) {
        return renderLeafPlanReward(node, pageId);
      }
      return renderLeafReward(node);
    }

    // LVLI node — render as expandable section
    const items = node.items || [];
    const children = node.children || [];
    // Filter out Toggle: conditions from pool-level conditions when a warningNote
    // will be shown — avoids duplicating the toggle info in both the warning banner
    // and the drop conditions table.
    const rawConditions = node.conditions || [];
    const conditions = rawConditions.filter(c => {
      const s = String(c || "").trim();
      return !/^Toggle:/i.test(s);
    });

    // ── Label: apply LABEL_MAP + duplicate detection ──────────────────────
    let label = node._displayLabel || node.label || node.edid || "Reward Pool";
    label = transformLabel(label);

    const labelKey = label.toLowerCase();
    // Only mark as "2nd Roll" etc. if this node has a Toggle condition — that's the
    // actual indicator of a toggled double roll.  Same-name nodes in different pools
    // (e.g. "Enclave Urban Scout Armour" at top level AND under Enclave Activity Rewards)
    // are distinct pools, not duplicate rolls.
    const hasToggle = rawConditions.some(c => /^Toggle:/i.test(String(c || "").trim()));
    if (seenLabels.has(labelKey) && hasToggle) {
      const count = seenLabels.get(labelKey) + 1;
      seenLabels.set(labelKey, count);
      label = `${label} (${ordinalRoll(count)})`;
    } else if (!seenLabels.has(labelKey)) {
      seenLabels.set(labelKey, 1);
    }

    // ── Count items for subtitle ──────────────────────────────────────────
    const totalItems = countTreeItems(node);

    // ── Subtitle ──────────────────────────────────────────────────────
    let subtitle;
    const guaranteed = isGuaranteedPool(node);
    // Check both entryRate (sub-LVLI within a tree) and gmrwDropRate (GMRW-level condition)
    const _gmrwRate = node.gmrwDropRate != null ? Number(node.gmrwDropRate) : null;
    const _entryRate = node.entryRate != null ? Number(node.entryRate) : null;
    const entryRate = (_gmrwRate != null && _gmrwRate < 100) ? _gmrwRate : _entryRate;
    const isChancePool = (entryRate != null && entryRate < 100);

    if (node.useAll) {
      const poolCount = children.length || items.length;
      const hasChance = children.some(c => c.entryRate != null && Number(c.entryRate) < 100)
                     || items.some(it => it.dropRate != null && Number(it.dropRate) < 100);
      if (isChancePool) {
        subtitle = `${fmtPct(entryRate)} chance \u00B7 ${poolCount} ${poolCount === 1 ? "reward list" : "reward lists"}`;
      } else if (hasChance) {
        subtitle = poolCount === 1
          ? `1 reward list \u00B7 rolled on completion`
          : `${poolCount} reward lists \u00B7 each rolled on completion`;
      } else {
        subtitle = poolCount === 1
          ? `1 reward list \u00B7 guaranteed on completion`
          : `${poolCount} reward lists \u00B7 all guaranteed on completion`;
      }
    } else if (isChancePool) {
      subtitle = `${fmtPct(entryRate)} chance to drop one item \u00B7 ${totalItems} ${totalItems === 1 ? "item" : "items"}`;
    } else if (guaranteed) {
      subtitle = `Guaranteed drop of one item \u00B7 ${totalItems} ${totalItems === 1 ? "item" : "items"}`;
    } else {
      subtitle = `Chance drop of one item \u00B7 ${totalItems} ${totalItems === 1 ? "item" : "items"}`;
    }

    // ── Roll count: GMRW RewardedItemCount > 1 means the game rolls this list N times ──
    const rollCount = node.rollCount != null ? Number(node.rollCount) : 1;
    if (rollCount > 1) {
      // Determine qty range across items in this list
      const allQty = (items.length ? items : []).map(it => Number(it.qty) || 1);
      const minQty = allQty.length ? Math.min(...allQty) : 1;
      const maxQty = allQty.length ? Math.max(...allQty) : 1;
      const qtyRange = minQty === maxQty ? `${minQty}` : `${minQty} to ${maxQty}`;
      subtitle = `List rolls ${rollCount} times on completion \u00B7 ${qtyRange} ${maxQty === 1 ? "item" : "items"} awarded per roll`;
    }

    // ── Toggle warning detection (uses rawConditions, before toggle filtering) ──
    let warningNote = null;
    const toggleCond = rawConditions.find(c => /^Toggle:/i.test(String(c || "").trim()));
    if (toggleCond) {
      // Duplicate label with toggle → this is a toggled second roll
      if (seenLabels.get(labelKey.replace(/ \(.*\)$/, "")) > 1) {
        warningNote = "This second roll is enabled or disabled at Bethesda\u2019s discretion and may not always be active.";
      } else {
        warningNote = "This reward is enabled or disabled at Bethesda\u2019s discretion and may not always be active.";
      }
    }

    // ── Region pool detection ─────────────────────────────────────────────
    const isRegionWrapper = hasRegionalLootPool(node);
    // No warningNote needed for region wrappers — the subtitle explains the mechanic

    // ── Build content ─────────────────────────────────────────────────────
    const content = document.createElement("div");

    if (isRegionWrapper) {
      // ── Region-aware sub-expands ────────────────────────────────
      // The game picks ONE region based on event location, NOT randomly.
      // Show only regions the event actually runs in, each as a sub-expand.
      // Items within each region are flattened into a single table.
      const rlp = children[0]; // the Regional Loot Pool

      // Collect all region nodes from the tree (may be nested under categories/sub-pools)
      function collectRegionNodes(node) {
        const results = [];
        (node.children || []).forEach(child => {
          const cl = (child.label || "").toLowerCase();
          const isRegion = cl.includes("forest") || cl.includes("toxic valley") ||
            cl.includes("savage divide") || cl.includes("ash heap") ||
            cl.includes("mire") || cl.includes("cranberry bog") ||
            cl.includes("skyline valley") || cl.includes("burning springs") ||
            cl.includes("rewards") && !cl.includes("pool") && !cl.includes("all");
          if (isRegion && (child.items?.length || child.children?.length)) {
            results.push(child);
          } else {
            // Recurse deeper
            results.push(...collectRegionNodes(child));
          }
        });
        return results;
      }

      const allRegionNodes = collectRegionNodes(rlp);

      // REGION_ALIASES is now at module scope (shared with flattenTreeItems)

      // Extract the canonical region name from a label like
      // "Recipes Armor Region Ash Heap" or "Ash Heap Rewards" → "Ash Heap"
      function extractRegionName(label) {
        return extractRegionFromLabel(label)
          || (label || "Unknown Region")
              .replace(/\s*Rewards?\s*$/i, "")
              .replace(/^.*Region\s+/i, "")
              .trim()
          || "Unknown Region";
      }

      // Normalize for comparison — always maps to canonical region name
      function normalizeRegion(name) {
        return extractRegionName(name).toLowerCase();
      }

      // Filter to only regions the event is in (normalize both sides)
      const activeRegionNames = (eventRegions || []).map(r => normalizeRegion(r));
      const matchedRegions = allRegionNodes.filter(rn => {
        const rnNorm = normalizeRegion(rn.label || "");
        return activeRegionNames.some(ar =>
          rnNorm.includes(ar) || ar.includes(rnNorm)
        );
      });

      // If no regions matched (data issue), show all regions as fallback
      const regionsToShow = matchedRegions.length ? matchedRegions : allRegionNodes;

      // Aggregate items per region — group by canonical region name
      // so "Recipes Armor Region Ash Heap" and "Recipes Mods Armor Region Ash Heap"
      // both merge under "Ash Heap"
      const regionGroups = new Map(); // canonicalRegion → [{name, qty, dropRate, conditions}]
      regionsToShow.forEach(regionNode => {
        const regionLabel = extractRegionName(regionNode.label);
        if (!regionGroups.has(regionLabel)) {
          regionGroups.set(regionLabel, []);
        }
        const items = regionGroups.get(regionLabel);

        // Flatten this region's items (rate=1.0 since the game selects this region)
        const flat = flattenTreeItems(regionNode, 1.0);
        flat.forEach(it => {
          // Dedupe: skip if same item name already exists in this region
          if (!items.some(existing => existing.name === it.name)) {
            items.push({
              name: it.name,
              qty: it.qty || 1,
              dropRate: it.dropRate,
              conditions: it.conditions || [],
            });
          }
        });
      });

      // Build a sub-expand for each region (alphabetical order)
      let totalItemCount = 0;
      const sortedRegionGroups = [...regionGroups.entries()].sort((a, b) => a[0].localeCompare(b[0]));
      sortedRegionGroups.forEach(([regionLabel, items]) => {
        totalItemCount += items.length;
        const regionContent = document.createElement("div");
        regionContent.appendChild(renderItemsTableMockup(items, [], false));
        content.appendChild(renderExpand({
          title: regionLabel,
          subtitle: `Guaranteed drop of one item \u00B7 ${items.length} ${items.length === 1 ? "item" : "items"}`,
          content: regionContent,
          open: false,
          pageId,
        }));
      });

      subtitle = `Regional loot pool \u2014 rewards depend on which region the event is active in \u00B7 ${regionGroups.size} ${regionGroups.size === 1 ? "region" : "regions"}`;
      warningNote = null; // No warning needed — we only show relevant regions

    } else if (node.useAll && children.length > 0) {
      // UseAll: each child becomes a sub-expand with its own pill showing entry chance
      children.forEach(child => {
        content.appendChild(renderLvliTreeNode(child, pageId, depth + 1, eventRegions, seenLabels));
      });

      // Also render direct items if any
      if (items.length) {
        content.appendChild(renderItemsTableMockup(
          items.map(it => {
            const rawN = it.name || it.formid || "\u2014";
            const mapped = {
              name: rawN,
              qty: it.qty || 1,
              dropRate: it.dropRate != null ? it.dropRate : 0,
              conditions: it.conditions || [],
            };
            if (it.modSlots) mapped.modSlots = it.modSlots;
            if (it.customModName) mapped.customModName = it.customModName;
            if (it.customModDescription) mapped.customModDescription = it.customModDescription;
            return mapped;
          }),
          conditions,
          false,
          /scrap/i.test(label)
        ));
      }

    } else if (!node.useAll && children.length > 0 && items.length === 0) {
      // Pick-one pool with ONLY children (no direct items): flatten into a single table
      // This handles Legendary Items (Rank1/2/3), Enclave Urban Scout Armour (5 pieces), etc.
      const parentMult = isChancePool ? entryRate / 100 : 1;
      const flatItems = flattenTreeItems(node, parentMult);
      // Propagate the node's own conditions to each flat item so they render per-item
      // (child conditions are already propagated by flattenTreeItems, but the parent
      // node's conditions are not — e.g. "Requires Plan: Gatling Plasma to be learned")
      const nodeConds = cleanConditions(conditions);
      if (nodeConds.length) {
        flatItems.forEach(fi => {
          fi.conditions = [...new Set([...nodeConds, ...(fi.conditions || [])])];
        });
      }
      content.appendChild(renderItemsTableMockup(flatItems, [], false, false, true));
      // Update subtitle count — respect entryRate for guaranteed/chance wording
      const flatCount = flatItems.length;
      if (isChancePool) {
        subtitle = `${fmtPct(entryRate)} chance to drop one item \u00B7 ${flatCount} ${flatCount === 1 ? "item" : "items"}`;
      } else {
        subtitle = (guaranteed
          ? "Guaranteed drop of one item"
          : "Chance drop of one item"
        ) + ` \u00B7 ${flatCount} ${flatCount === 1 ? "item" : "items"}`;
      }

    } else {
      // Simple items-only node (no children)
      // When this pool has a non-100% entryRate, multiply it into item rates
      // so users see the absolute drop chance (e.g. 5% instead of 100%)
      const rateMultiplier = isChancePool ? entryRate / 100 : 1;
      // Propagate node-level conditions to each item so they render per-item
      // (e.g. "Requires Plan: Gatling Plasma to be learned" on each plan row)
      const nodeCondsForItems = cleanConditions(conditions);
      if (items.length) {
        content.appendChild(renderItemsTableMockup(
          items.map(it => {
            const rawN = it.name || it.formid || "\u2014";
            const rawDr = it.dropRate != null ? it.dropRate : 0;
            const itemConds = it.conditions || [];
            const mapped = {
              name: rawN,
              qty: it.qty || 1,
              dropRate: rawDr * rateMultiplier,
              conditions: nodeCondsForItems.length
                ? [...new Set([...nodeCondsForItems, ...itemConds])]
                : itemConds,
            };
            if (it.modSlots) mapped.modSlots = it.modSlots;
            if (it.customModName) mapped.customModName = it.customModName;
            if (it.customModDescription) mapped.customModDescription = it.customModDescription;
            return mapped;
          }),
          [],
          false,
          /scrap/i.test(label),
          true
        ));
      }
    }

    if (!items.length && !children.length) {
      content.appendChild(el("div", { class: "dfbnb-evr__empty" }, ["No items resolved."]));
    }

    return renderExpand({
      title: label,
      subtitle,
      content,
      open: depth === 0,
      pageId,
      pill: null,
      warningNote,
    });
  }

  // ── XP expand (first section before tree) ────────────────────────────────

  function renderXpExpand(data, pageId) {
    const ad = data?.activityData?.baseRewards;
    const tiers = data?.baseRewards?.tiers || [];

    // Try activityData first, fall back to baseRewards tiers
    let xpVal = ad?.xp;
    if (xpVal == null && tiers.length) {
      xpVal = tiers[0]?.xp;
    }
    if (xpVal == null) return null;

    const content = document.createElement("div");
    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Item"]));
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Amount"]));
    thead.appendChild(trh);
    const tbody = el("tbody");

    // Multi-tier GMRW XP breakdown (e.g. Riding Shotgun: brahmin survived, packages found)
    const xpBreakdown = ad?.xpBreakdown;
    if (xpBreakdown && xpBreakdown.length > 1) {
      let totalXp = 0;
      xpBreakdown.forEach(entry => {
        const tr = el("tr");
        const label = entry.condition
          ? `${entry.label} (bonus)`
          : entry.label;
        tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [label]));
        tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [String(entry.xp)]));
        tbody.appendChild(tr);
        totalXp += entry.xp;
      });
      // Total row
      const trTotal = el("tr");
      trTotal.style.borderTop = "1px solid var(--border-color, #444)";
      const tdTotalLabel = el("td", { class: "dfbnb-evr__cell" }, ["Maximum Total XP"]);
      tdTotalLabel.style.fontWeight = "bold";
      const tdTotalVal = el("td", { class: "dfbnb-evr__cell" }, [String(totalXp)]);
      tdTotalVal.style.fontWeight = "bold";
      trTotal.appendChild(tdTotalLabel);
      trTotal.appendChild(tdTotalVal);
      tbody.appendChild(trTotal);
    }
    // Stage-based XP (activities with multiple score checkpoints)
    else if (ad?.xpByStage && ad.xpByStage.length > 1) {
      ad.xpByStage.forEach(stage => {
        const tr = el("tr");
        tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [stage.label]));
        tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [String(stage.xp)]));
        tbody.appendChild(tr);
      });
    } else {
    // Check for multiple XP values (success/failed)
    const xpSuccess = ad?.xpSuccess || xpVal;
    const xpFailed = ad?.xpFailed;

    if (xpFailed != null && xpFailed !== xpSuccess) {
      const trS = el("tr");
      trS.appendChild(el("td", { class: "dfbnb-evr__cell" }, ["Event Completion XP"]));
      trS.appendChild(el("td", { class: "dfbnb-evr__cell" }, [String(xpSuccess)]));
      tbody.appendChild(trS);

      const trF = el("tr");
      trF.appendChild(el("td", { class: "dfbnb-evr__cell" }, ["Event Failure XP"]));
      trF.appendChild(el("td", { class: "dfbnb-evr__cell" }, [String(xpFailed)]));
      tbody.appendChild(trF);
    } else {
      const tr = el("tr");
      tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, ["Event Completion XP"]));
      tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [String(xpVal)]));
      tbody.appendChild(tr);
    }
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    content.appendChild(table);

    const xpSubtitle = (xpBreakdown && xpBreakdown.length > 1)
      ? "Base XP scales with player level \u00B7 bonus XP awarded for completing optional objectives"
      : "Awarded on completion \u00B7 scales with player level and other buffs";

    return renderExpand({
      title: "Experience (XP)",
      subtitle: xpSubtitle,
      content,
      open: true,
      pageId,
      pill: null,
    });
  }

  function renderCapsExpand(data, pageId) {
    const ad = data?.activityData?.baseRewards;
    const capsBreakdown = ad?.capsBreakdown;
    if (!capsBreakdown || capsBreakdown.length < 2) return null;

    const content = document.createElement("div");
    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Item"]));
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Amount"]));
    thead.appendChild(trh);
    const tbody = el("tbody");

    let totalCaps = 0;
    capsBreakdown.forEach(entry => {
      const tr = el("tr");
      const label = entry.condition
        ? `${entry.label} (bonus)`
        : entry.label;
      tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [label]));
      tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [String(entry.caps)]));
      tbody.appendChild(tr);
      totalCaps += entry.caps;
    });

    // Total row
    const trTotal = el("tr");
    trTotal.style.borderTop = "1px solid var(--border-color, #444)";
    const tdTotalLabel = el("td", { class: "dfbnb-evr__cell" }, ["Maximum Total Caps"]);
    tdTotalLabel.style.fontWeight = "bold";
    const tdTotalVal = el("td", { class: "dfbnb-evr__cell" }, [String(totalCaps)]);
    tdTotalVal.style.fontWeight = "bold";
    trTotal.appendChild(tdTotalLabel);
    trTotal.appendChild(tdTotalVal);
    tbody.appendChild(trTotal);

    table.appendChild(thead);
    table.appendChild(tbody);
    content.appendChild(table);

    return renderExpand({
      title: "Caps",
      subtitle: "Bonus caps awarded for completing optional objectives",
      content,
      open: true,
      pageId,
      pill: null,
    });
  }

  // ── Categorise a unique reward node by its dominant item signature ───────
  // Classify a single item into a smart category based on name + sig
  function classifyItemCategory(item) {
    const name = (item.name || item.edid || "").trim();
    const sig = (item.sig || "MISC").toUpperCase();
    // Titles — Camp Title: X or Player Title: X
    if (/^(Camp|Player)\s+Title:/i.test(name)) return "Titles";
    // Underarmour mod plans
    if (/underarm/i.test(name) && /^(plan|recipe):/i.test(name)) return "Underarmour Mod Plans";
    // Regular plans/recipes (sig=BOOK but not titles or underarmour)
    if (sig === "BOOK" || /^(plan|recipe):/i.test(name)) return "Plans & Recipes";
    // Everything else by sig
    if (sig === "WEAP") return "Weapon Rewards";
    if (sig === "ARMO") return "Armour Rewards";
    if (sig === "AMMO") return "Ammo";
    if (sig === "ALCH") return "Aid";
    if (sig === "CNCY") return "Currency";
    if (sig === "KEYM") return "Key Items";
    return "Miscellaneous";
  }

  // Classify an LVLI node by looking at ALL items recursively
  function categoriseUniqueNode(node) {
    if (node.type === "leaf") return classifyItemCategory(node);
    const catCounts = {};
    function countCats(n) {
      (n.items || []).forEach(it => {
        const cat = classifyItemCategory(it);
        catCounts[cat] = (catCounts[cat] || 0) + 1;
      });
      (n.children || []).forEach(c => countCats(c));
    }
    countCats(node);
    let best = "Miscellaneous", bestN = 0;
    for (const [cat, n] of Object.entries(catCounts)) {
      if (n > bestN) { best = cat; bestN = n; }
    }
    return best;
  }

  function renderRewardTree(data, pageId) {
    const tree = data?.activityData?.rewardTree;
    if (!Array.isArray(tree) || !tree.length) return null;

    // Extract unique region names from event data for region pool filtering
    const regionLocations = Array.isArray(data?.regionLocations) ? data.regionLocations : [];
    const eventRegions = [...new Set(
      regionLocations.map(rl => (rl.region || "").trim()).filter(Boolean)
    )];

    const wrap = document.createElement("div");
    wrap.className = "dfbnb-tree";

    // 1. XP expand first
    const xpExpand = renderXpExpand(data, pageId);
    if (xpExpand) wrap.appendChild(xpExpand);

    // 1b. Caps expand (bonus caps from optional objectives like Riding Shotgun packages)
    const capsExpand = renderCapsExpand(data, pageId);
    if (capsExpand) wrap.appendChild(capsExpand);

    // 2. Separate tree into standard and unique (event-specific) nodes.
    const standardNodes = [];
    const uniqueNodes = [];
    tree.forEach(node => {
      if (node.isUniqueReward) {
        uniqueNodes.push(node);
      } else {
        standardNodes.push(node);
      }
    });

    // Relocate Enclave Urban Scout Armour and Enclave Plasma Gun nodes into Enclave Activity Rewards
    // so all four enclave reward pools live under one parent expand.
    const _encActNode = standardNodes.find(n => /^enclave activity rewards$/i.test(n.label || ""));
    if (_encActNode) {
      const _toMove = [];
      const _keepStd = [];
      standardNodes.forEach(n => {
        const _lbl = (n.label || "").toLowerCase();
        if (n !== _encActNode && (
          _lbl.includes("enclave urban scout") || _lbl.includes("enclave plasma gun")
        )) {
          _toMove.push(n);
        } else {
          _keepStd.push(n);
        }
      });
      if (_toMove.length) {
        if (!_encActNode.children) _encActNode.children = [];
        _toMove.forEach(n => _encActNode.children.push(n));
        standardNodes.length = 0;
        standardNodes.push(..._keepStd);
      }
    }

    // 3. Unique Activity Rewards expand — always SECOND (right after XP)
    if (uniqueNodes.length > 0) {
      const uerContent = document.createElement("div");

      // Separate leaf items from LVLI nodes
      const leafItems = [];
      const lvliNodes = [];
      uniqueNodes.forEach(node => {
        if (node.type === "leaf") {
          leafItems.push(node);
        } else {
          lvliNodes.push(node);
        }
      });

      // ── Group leaf items by smart category ──
      const leafGroups = {};
      leafItems.forEach(leaf => {
        const cat = classifyItemCategory(leaf);
        if (!leafGroups[cat]) leafGroups[cat] = [];
        leafGroups[cat].push(leaf);
      });

      // ── Helper: build a blurb for a group of LVLI-sourced items in a category ──
      function lvliCategoryBlurb(items, sourceNodes, dupeNote) {
        const count = items.length;
        const dupe = dupeNote ? ` \u00B7 ${dupeNote}` : "";
        // Collect tier labels from source nodes (e.g. "Front Brahmin Survived")
        const tierLabels = sourceNodes
          .map(n => n.tierLabel)
          .filter(Boolean);
        const tierSuffix = tierLabels.length ? ` \u00B7 ${tierLabels.join(", ")}` : "";
        // Check for rollCount (e.g. Death Blossoms seeds ×3)
        // If any source node has rollCount > 1, show the rolls blurb
        const rollNode = sourceNodes.find(n => (n.rollCount || 1) > 1);
        if (rollNode) {
          const rc = Number(rollNode.rollCount);
          const allQty = items.map(it => Number(it.qty) || 1);
          const minQty = allQty.length ? Math.min(...allQty) : 1;
          const maxQty = allQty.length ? Math.max(...allQty) : 1;
          const qtyRange = minQty === maxQty ? `${minQty}` : `${minQty} to ${maxQty}`;
          return `List rolls ${rc} times on completion \u00B7 ${qtyRange} ${maxQty === 1 ? "item" : "items"} awarded per roll${dupe}${tierSuffix}`;
        }
        // Check GMRW-level drop rate (e.g. 10% GetRandomPercent)
        const chanceNode = sourceNodes.find(n => {
          const gmrw = n.gmrwDropRate != null ? Number(n.gmrwDropRate) : null;
          const entry = n.entryRate != null ? Number(n.entryRate) : null;
          const rate = gmrw != null ? gmrw : entry;
          return rate != null && rate < 100;
        });
        if (chanceNode) {
          const gmrw = chanceNode.gmrwDropRate != null ? Number(chanceNode.gmrwDropRate) : null;
          const entry = chanceNode.entryRate != null ? Number(chanceNode.entryRate) : null;
          const rate = gmrw != null ? gmrw : entry;
          return `${fmtPct(rate)} chance to drop one item \u00B7 ${count} ${count === 1 ? "item" : "items"}${dupe}${tierSuffix}`;
        }
        // Check if the source pool is guaranteed (pick-one with rates summing to ~100%)
        const guaranteedNode = sourceNodes.find(n => isGuaranteedPool(n));
        if (guaranteedNode) {
          return `Guaranteed drop of one item \u00B7 ${count} ${count === 1 ? "item" : "items"}${dupe}${tierSuffix}`;
        }
        // Fallback
        return `Chance drop of one item \u00B7 ${count} ${count === 1 ? "item" : "items"}${dupe}${tierSuffix}`;
      }

      // ── Helper: build a consistent blurb for leaf category sub-expands ──
      function leafCategoryBlurb(items) {
        if (items.length === 1) {
          const dr = items[0].dropRate;
          if (dr != null && dr < 100) {
            return `${fmtPct(dr)} chance to drop \u00B7 1 item`;
          }
          return `Guaranteed drop \u00B7 1 item`;
        }
        return `${items.length} items \u00B7 ${items.length === 1 ? "rolled on completion" : "each rolled on completion"}`;
      }

      // ── Category display order (shared by LVLI groups and leaf groups) ──
      const catOrder = ["Titles", "Weapon Rewards", "Armour Rewards", "Ammo", "Underarmour Mod Plans", "Plans & Recipes", "Aid", "Currency", "Key Items", "Miscellaneous"];
      // Legendary tiers (e.g. "Legendary Items (2★)") sort after standard categories
      // but before Miscellaneous, by star rating.
      function catSortKey(cat) {
        const idx = catOrder.indexOf(cat);
        if (idx >= 0) return idx;
        // Legendary Items (N★) → sort by star rating after Key Items
        const legMatch = cat.match(/^Legendary.*?(\d)/);
        if (legMatch) return catOrder.indexOf("Key Items") + 0.5 + parseInt(legMatch[1], 10) * 0.01;
        return catOrder.length;
      }

      // ── Region filtering helper for Plans & Recipes ──────────────────────────
      // Items with "Region: X" conditions should only appear if X matches one of
      // the event's active regions.  Normalise both sides for comparison.
      const normRegionNames = (eventRegions || []).map(r => r.toLowerCase().replace(/^the\s+/, "").trim());
      function itemPassesRegionCheck(item) {
        const conds = item.conditions || [];
        const regionConds = conds.filter(c => /^Region:\s/i.test(String(c)));
        if (!regionConds.length) return true;           // no region restriction — keep
        if (!normRegionNames.length) return true;       // no event regions known — keep all
        return regionConds.some(rc => {
          const rn = String(rc).replace(/^Region:\s*/i, "").toLowerCase().replace(/^the\s+/, "").trim();
          return normRegionNames.some(er => rn.includes(er) || er.includes(rn));
        });
      }

      // ── Flatten ALL LVLI items, classify EACH item individually by category ──
      // This fixes mixed-category LVLIs (e.g. an LVLI with a title + underarmour plan)
      // getting lumped under one category.  Each item goes to its correct category.
      const lvliCatGroups = {};   // cat → { items: [], sourceNodes: [] }
      lvliNodes.forEach(node => {
        // Compute effective rate multiplier for absolute drop rates
        const gmrwRate = node.gmrwDropRate != null ? Number(node.gmrwDropRate) : null;
        const entryRate = node.entryRate != null ? Number(node.entryRate) : null;
        const effectiveRate = gmrwRate != null ? gmrwRate : entryRate;
        const parentMult = (effectiveRate != null && effectiveRate < 100) ? effectiveRate / 100 : 1;
        const flatItems = flattenTreeItems(node, parentMult);

        // Legendary item nodes (e.g. "Legendary Items (2★)") keep their own
        // label as the category so each star tier gets its own sub-expand
        // with its own blurb (showing tier conditions like "Front Brahmin Survived").
        const isLegendaryNode = /^Legendary/i.test(node.label || "");

        // Track which categories this LVLI's items fall into
        const nodeCats = new Set();
        flatItems.forEach(item => {
          // Classify this individual item (uses sig + name patterns)
          const cat = isLegendaryNode ? node.label : classifyItemCategory(item);

          // Region filter: for Plans & Recipes, only include items whose region
          // conditions match the event's region(s).  This prevents showing plans
          // from all 8 regions when the event only runs in one.
          if (cat === "Plans & Recipes" && !itemPassesRegionCheck(item)) return;

          nodeCats.add(cat);
          if (!lvliCatGroups[cat]) lvliCatGroups[cat] = { items: [], sourceNodes: [] };
          lvliCatGroups[cat].items.push(item);
          // Track which LVLI nodes contributed to this category (for blurb mechanics)
          if (!lvliCatGroups[cat].sourceNodes.includes(node)) {
            lvliCatGroups[cat].sourceNodes.push(node);
          }
        });

        // If ALL items from this LVLI fall into "Miscellaneous", rename the group
        // to the LVLI's prettified label (e.g. "Enclave Plasma Gun Mod Boxes")
        // instead of the generic "Miscellaneous" heading
        if (nodeCats.size === 1 && nodeCats.has("Miscellaneous") && flatItems.length > 0) {
          const prettyLabel = transformLabel(node.label || node.edid || "Reward Pool");
          if (prettyLabel && prettyLabel !== "Miscellaneous") {
            const miscGroup = lvliCatGroups["Miscellaneous"];
            // Only rename if this LVLI is the sole contributor to Miscellaneous
            if (miscGroup && miscGroup.sourceNodes.length === 1 && miscGroup.sourceNodes[0] === node) {
              if (!lvliCatGroups[prettyLabel]) {
                lvliCatGroups[prettyLabel] = miscGroup;
                delete lvliCatGroups["Miscellaneous"];
              }
            }
          }
        }

        // If LVLI had NO items after flattening, use prettified label as placeholder
        if (!flatItems.length) {
          const label = transformLabel(node.label || node.edid || "Reward Pool");
          if (!lvliCatGroups[label]) lvliCatGroups[label] = { items: [], sourceNodes: [] };
          lvliCatGroups[label].sourceNodes.push(node);
        }
      });

      // Render each LVLI category group as a flat sub-expand (or region sub-expands if items have _region tags)
      function renderLvliCategoryExpand(cat, group) {
        const { items, sourceNodes } = group;
        if (!items.length) return;
        const content = document.createElement("div");

        // Check source nodes (and their children) for duplicateRollNote
        let dupeNote = "";
        function findDupeNote(nodes) {
          for (const n of (nodes || [])) {
            if (n.duplicateRollNote) { dupeNote = n.duplicateRollNote; return; }
            if (n.children) findDupeNote(n.children);
          }
        }
        findDupeNote(sourceNodes);

        // ── Region sub-expands: if items have _region tags, group by region ──
        const regionItems = items.filter(it => it._region);
        const nonRegionItems = items.filter(it => !it._region);
        const hasRegions = regionItems.length > 0;

        if (hasRegions) {
          // Group by canonical region name
          const regionGroups = new Map();
          regionItems.forEach(it => {
            const r = it._region;
            if (!regionGroups.has(r)) regionGroups.set(r, []);
            // Dedupe by name within each region
            const bucket = regionGroups.get(r);
            if (!bucket.some(existing => existing.name === it.name)) {
              bucket.push(it);
            }
          });

          // Filter to only regions the event is active in
          const activeNorm = (eventRegions || []).map(r => r.toLowerCase().replace(/^the\s+/, "").trim());
          let regionsToRender;
          if (activeNorm.length) {
            regionsToRender = [...regionGroups.entries()].filter(([regionName]) => {
              const rn = regionName.toLowerCase().replace(/^the\s+/, "").trim();
              return activeNorm.some(ar => rn.includes(ar) || ar.includes(rn));
            });
            // Fallback: if no matches, show all (data issue)
            if (!regionsToRender.length) regionsToRender = [...regionGroups.entries()];
          } else {
            regionsToRender = [...regionGroups.entries()];
          }

          // Sort regions alphabetically and render a sub-expand for each
          regionsToRender.sort(([a], [b]) => a.localeCompare(b));
          regionsToRender.forEach(([regionName, rItems]) => {
            const regionContent = document.createElement("div");
            regionContent.appendChild(renderItemsTableMockup(rItems, [], false, false, true));
            content.appendChild(renderExpand({
              title: regionName,
              subtitle: `Guaranteed drop of one item \u00B7 ${rItems.length} ${rItems.length === 1 ? "item" : "items"}`,
              content: regionContent,
              open: false,
              pageId,
            }));
          });

          // Also render any non-region items as a flat table below the region sub-expands
          if (nonRegionItems.length) {
            content.appendChild(renderItemsTableMockup(nonRegionItems, [], false, false, true));
          }

          const regionCount = regionsToRender.length;
          const totalShown = regionsToRender.reduce((sum, [, ri]) => sum + ri.length, 0) + nonRegionItems.length;
          uerContent.appendChild(renderExpand({
            title: cat,
            subtitle: `Regional loot pool \u2014 rewards depend on which region the event is active in \u00B7 ${regionCount} ${regionCount === 1 ? "region" : "regions"}`,
            content,
            open: false,
            pageId,
          }));
        } else {
          // Standard flat rendering (no region data)
          content.appendChild(renderItemsTableMockup(items, [], false, false, true));
          uerContent.appendChild(renderExpand({
            title: cat,
            subtitle: lvliCategoryBlurb(items, sourceNodes, dupeNote),
            content,
            open: false,
            pageId,
          }));
        }
      }

      // Render LVLI category expands in sorted order (standard catOrder first,
      // then legendary tiers by star rating, then remaining custom categories).
      Object.keys(lvliCatGroups)
        .filter(cat => lvliCatGroups[cat] && lvliCatGroups[cat].items.length)
        .sort((a, b) => catSortKey(a) - catSortKey(b))
        .forEach(cat => renderLvliCategoryExpand(cat, lvliCatGroups[cat]));

      // ── Render leaf item groups as categorised sub-expands (flat tables only) ──

      function renderLeafCategoryExpand(cat, items) {
        const hasQty = items.some(l => l.qty && l.qty > 1);
        const colSpan = String(hasQty ? 3 : 2);
        const table = el("table", { class: "dfbnb-evr__table" });
        const thead = el("thead"), trh = el("tr");
        trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Item"]));
        if (hasQty) trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Qty"]));
        trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]));
        thead.appendChild(trh);
        const tbody = el("tbody");
        items.sort((a, b) => (a.name || "").localeCompare(b.name || ""));
        items.forEach(leaf => {
          const tr = el("tr");
          tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [leaf.name || leaf.formid || "\u2014"]));
          if (hasQty) tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [leaf.qty > 1 ? `\u00D7${leaf.qty}` : ""]));
          const dr = leaf.dropRate != null ? fmtPct(leaf.dropRate) : "100%";
          tr.appendChild(el("td", { class: "dfbnb-evr__cell" }, [dr]));
          tbody.appendChild(tr);
          const conds = cleanConditions(leaf.conditions);
          if (conds.length) {
            const condRow = el("tr");
            const condCell = el("td", { class: "dfbnb-act-base__cond", colspan: colSpan });
            condCell.innerHTML = "<strong>Drop Conditions:</strong> " + conds.map(c => `<em>${c}</em>`).join("; ");
            condRow.appendChild(condCell);
            tbody.appendChild(condRow);
          }
        });
        table.appendChild(thead);
        table.appendChild(tbody);
        const leafContent = document.createElement("div");
        leafContent.appendChild(table);
        uerContent.appendChild(renderExpand({
          title: cat,
          subtitle: leafCategoryBlurb(items),
          content: leafContent,
          open: true,
          pageId,
        }));
      }

      Object.keys(leafGroups)
        .filter(cat => leafGroups[cat] && leafGroups[cat].length)
        .sort((a, b) => catSortKey(a) - catSortKey(b))
        .forEach(cat => renderLeafCategoryExpand(cat, leafGroups[cat]));

      // ── Subtitle: count total sub-expands rendered ──
      const subExpandCount = uerContent.querySelectorAll(":scope > details.dfbnb-expand").length;
      let uerSubtitle;
      if (subExpandCount > 1) {
        uerSubtitle = `${subExpandCount} reward lists \u00B7 each rolled on completion`;
      } else if (subExpandCount === 1) {
        uerSubtitle = "1 reward list \u00B7 rolled on completion";
      } else {
        const totalLeaves = leafItems.length;
        uerSubtitle = totalLeaves === 1
          ? `1 item \u00B7 rolled on completion`
          : `${totalLeaves} items \u00B7 each rolled on completion`;
      }

      wrap.appendChild(renderExpand({
        title: "Unique Activity Rewards",
        subtitle: uerSubtitle,
        content: uerContent,
        open: false,
        pageId,
      }));
    } else {
      // No unique rewards — show empty state
      const emptyContent = document.createElement("div");
      emptyContent.appendChild(el("div", { class: "dfbnb-expand__sub", style: "padding:8px 12px; opacity:0.7;" }, ["No unique rewards for this activity."]));
      wrap.appendChild(renderExpand({
        title: "Unique Activity Rewards",
        subtitle: "",
        content: emptyContent,
        open: false,
        pageId,
      }));
    }

    // 4. Sort standard: "Activity Rewards" goes LAST, everything else in original order
    standardNodes.sort((a, b) => {
      const aIsAR = (a.label || "").toLowerCase() === "activity rewards";
      const bIsAR = (b.label || "").toLowerCase() === "activity rewards";
      if (aIsAR && !bIsAR) return 1;
      if (!aIsAR && bIsAR) return -1;
      return 0;
    });

    // 5. Shared seenLabels across standard nodes (separate from unique)
    const stdSeenLabels = new Map();

    // 6. Render standard tree nodes as their own expands
    standardNodes.forEach(node => {
      wrap.appendChild(renderLvliTreeNode(node, pageId, 0, eventRegions, stdSeenLabels));
    });

    return wrap;
  }

  // ── Activity: header card ──────────────────────────────────────────────────

  function renderActivityHeader({ pageId, rootEl, data }) {
    const top = el("div", { class: "dfbnb-evrTop" });

    let title = safeText(data?.name);
    if (!title || title.toLowerCase() === "event") {
      const slug = slugFromPathname();
      if (slug) title = slug
        .replace(/-all-rewards$/i, "")
        .replace(/-/g, " ")
        .replace(/\b\w/g, c => c.toUpperCase());
    }
    if (!title) title = "Activity";

    top.appendChild(el("div", { class: "dfbnb-evrTitle" }, [`Activity: ${title} All Rewards`]));

    const desc = safeText(
      data?.description || data?.desc ||
      data?.questDescription || data?.questDesc || ""
    );
    if (desc) top.appendChild(el("div", { class: "dfbnb-evrDesc" }, [desc]));

    // Page-specific notes rendered right after the description
    const _slug = slugFromPathname();
    if (_slug && _slug.includes("riding-shotgun")) {
      top.appendChild(el("div", { class: "dfbnb-evrDesc" }, [
        "Riding Shotgun is excluded from the time-based rotation pool. You can repeat this activity every 20 minutes upon speaking to Vinny."
      ]));
    }
    if (_slug && _slug.includes("jail-break")) {
      const jbNote = el("div", { class: "dfbnb-evrDesc" });
      jbNote.innerHTML = 'Jail Break has been disabled as per <a href="https://bethesda.net/en/article/0sXOoFYwO5TxarqDKVzts/fallout-76-the-backwoods-release-notes" target="_blank" rel="noopener">Fallout 76: The Backwoods Release Notes</a> (March 3, 2026).';
      top.appendChild(jbNote);
    }
    if (_slug && _slug.includes("dogwood-die-off")) {
      const ddNote = el("div", { class: "dfbnb-evrDesc" });
      ddNote.innerHTML = 'Dogwood Die Off has been disabled as per <a href="https://bethesda.net/en/article/0sXOoFYwO5TxarqDKVzts/fallout-76-the-backwoods-release-notes" target="_blank" rel="noopener">Fallout 76: The Backwoods Release Notes</a> (March 3, 2026).';
      top.appendChild(ddNote);
    }

    const regionLocations = Array.isArray(data?.regionLocations) ? data.regionLocations : [];
    if (!regionLocations.length && (data?.region || data?.location)) {
      regionLocations.push({ region: data.region || "", location: data.location || "" });
    }
    if (regionLocations.length) {
      // Group all locations by region name (not just consecutive)
      const regionMap = new Map();
      regionLocations.forEach(rl => {
        const r = rl.region || "";
        if (!regionMap.has(r)) regionMap.set(r, []);
        if (rl.location) {
          const locs = regionMap.get(r);
          if (!locs.includes(rl.location)) locs.push(rl.location);
        }
      });
      // Sort regions alphabetically
      const sortedRegions = [...regionMap.entries()].sort((a, b) => a[0].localeCompare(b[0]));
      sortedRegions.forEach(([region, locations]) => {
        if (region) top.appendChild(el("div", { class: "dfbnb-evrLine" }, [
          el("span", { class: "dfbnb-evrLineLabel" }, ["Region: "]), region
        ]));
        locations.sort((a, b) => a.localeCompare(b));
        locations.forEach(loc => {
          top.appendChild(el("div", { class: "dfbnb-evrLine dfbnb-evrLine--location" }, [
            el("span", { class: "dfbnb-evrLineLabel" }, ["Location: "]), loc
          ]));
        });
      });
    }

    const actions = el("div", { class: "dfbnb-evrActions" });
    [
      ["Expand All",        () => rootEl.querySelectorAll("details.dfbnb-expand").forEach(d => { d.open = true;  })],
      ["Close All",         () => rootEl.querySelectorAll("details.dfbnb-expand").forEach(d => { d.open = false; })],
    ].forEach(([label, handler]) => {
      const b = el("button", { class: "dfbnb-evrBtn", type: "button" }, [label]);
      b.addEventListener("click", handler);
      actions.appendChild(b);
    });
    top.appendChild(actions);

    // Progress bar removed from activity pages (CSS kept, just not rendered)

    return top;
  }

  // ── Activity: simple item-list table ───────────────────────────────────────

  const RAW_FORMID_RE = /^[0-9A-Fa-f]{8}$/;

  function renderPoolItemList(poolsArr, perItemRateOverride = null) {
    const wrap = document.createElement("div");
    const seen = new Set();
    const rows = [];
    let skipped = 0;
    poolsArr.forEach(pool => {
      const pChance = pool.poolChance != null ? Number(pool.poolChance) : null;
      (pool.items || []).forEach(it => {
        const key = it.formid || it.name || "";
        if (key && seen.has(key)) return;
        if (key) seen.add(key);
        const name = it.name || "";
        if (!name || RAW_FORMID_RE.test(name)) { skipped++; return; }
        const dr = perItemRateOverride != null ? perItemRateOverride
                 : (it.dropRate != null ? Number(it.dropRate) : pChance);
        rows.push([
          name,
          it.qty ? `×${it.qty}` : "—",
          dr != null ? `${dr.toFixed(2)}%` : "—",
        ]);
      });
    });
    rows.sort((a, b) => (a[0] || "").localeCompare(b[0] || ""));
    if (!rows.length) {
      const msg = skipped
        ? `${skipped} item(s) could not be resolved — FormID lookup missing from TSV exports.`
        : "No items found.";
      wrap.appendChild(el("div", { class: "dfbnb-evr__empty" }, [msg]));
      return wrap;
    }
    wrap.appendChild(renderTable(["Item", "Qty", "Drop Rate"], rows));
    if (skipped) {
      wrap.appendChild(el("div", { class: "dfbnb-evr__empty" }, [
        `+ ${skipped} item(s) not shown — FormID not resolved.`
      ]));
    }
    return wrap;
  }

  // ── Activity: Chem Rewards expand ──────────────────────────────────────────

  function renderChemRewardsExpand(data, pageId) {
    const adChems = data?.activityData?.chemRewards;

    if (!Array.isArray(adChems) || !adChems.length) {
      const pools = data?.pools || [];
      const chemPools = pools.filter(p => {
        const e = String(p.lvliEdid || p.edid || "").toLowerCase();
        const hasChems = e.includes("_chems_") || (p.poolTypes || []).some(pt => pt.type === "chems");
        return hasChems;
      });
      if (!chemPools.length) return null;
      return renderExpand({ title: "Chem Rewards", content: renderPoolItemList(chemPools), open: false, pageId });
    }

    const sorted = [...adChems].sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    const hasConditions = sorted.some(item =>
      Array.isArray(item.conditions) && item.conditions.length > 0
    );

    const wrap = document.createElement("div");
    const tbl  = el("table", { class: "dfbnb-evr__table" });
    const headerCells = [
      el("th", { class: "dfbnb-evr__th" }, ["Item"]),
      el("th", { class: "dfbnb-evr__th" }, ["Qty"]),
      el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]),
    ];
    if (hasConditions) headerCells.push(el("th", { class: "dfbnb-evr__th" }, ["Conditions"]));
    tbl.appendChild(el("thead", {}, [el("tr", {}, headerCells)]));
    const tbody = document.createElement("tbody");
    sorted.forEach(item => {
      const dr = item.dropRate != null ? fmtPct(Number(item.dropRate)) : "—";
      const cells = [
        el("td", { class: "dfbnb-evr__cell dfbnb-evr__cell--label" }, [item.name || "—"]),
        el("td", { class: "dfbnb-evr__cell" }, [`×${item.qty || 1}`]),
        el("td", { class: "dfbnb-evr__cell" }, [dr]),
      ];
      if (hasConditions) {
        const conds = Array.isArray(item.conditions) ? item.conditions.filter(c => {
          const s = String(c || "").trim();
          return s && !/GetRandomPercent/i.test(s);
        }) : [];
        cells.push(el("td", { class: "dfbnb-evr__cell" }, [conds.map(translateCondition).join("; ") || "—"]));
      }
      tbody.appendChild(el("tr", {}, cells));
    });
    tbl.appendChild(tbody);
    wrap.appendChild(tbl);

    return renderExpand({ title: "Chem Rewards", content: wrap, open: false, pageId });
  }

  // ── Plan Progress ──────────────────────────────────────────────────────────

  let _planProgress = {};
  let _planProgressPageId = "";
  const _uerItemIds = new Set();

  function planProgressKey(pageId) { return `${pageId}::${MODULE_ID}::planProgress`; }

  function loadPlanProgress(pageId) {
    try { return JSON.parse(localStorage.getItem(planProgressKey(pageId)) || "{}"); } catch { return {}; }
  }

  function savePlanProgress(pageId) {
    try { localStorage.setItem(planProgressKey(pageId), JSON.stringify(_planProgress)); } catch {}
  }

  function updatePlanProgressUI(pageId, total) {
    const useUer  = _uerItemIds.size > 0;
    const countTotal = useUer ? _uerItemIds.size : total;
    const done = useUer
      ? [..._uerItemIds].filter(id => !!_planProgress[id]).length
      : Object.values(_planProgress).filter(Boolean).length;
    const pct  = countTotal ? Math.round((done / countTotal) * 100) : 0;
    const fill = document.querySelector(".dfbnb-evr__plan-fill");
    const text = document.querySelector(".dfbnb-evr__plan-progress-text");
    const pill = document.querySelector(".dfbnb-evr__plan-pct-pill");
    if (fill) fill.style.width = `${pct}%`;
    if (text) text.textContent = `Completed ${done} of ${countTotal}`;
    if (pill) pill.style.display = "none";
  }

  // ── Activity: Unique Event Rewards expand ──────────────────────────────────

  function renderUniqueEventRewardsExpand(data, pageId) {
    const adUer = data?.activityData?.uniqueEventRewards;
    let allItems;

    if (Array.isArray(adUer) && adUer.length) {
      allItems = adUer.map(item => ({
        name:       item.name || "",
        formid:     item.formid || "",
        edid:       item.edid || "",
        dropRate:   item.dropRate != null ? Number(item.dropRate) : null,
        tradeable:  item.tradeable ?? null,
        affix:      item.affix || null,
        conditions: item.conditions || [],
        imageUrl:   item.imageUrl || null,
        kind:       item.kind || null,
        modSlots:   item.modSlots || null,
        customModName: item.customModName || null,
        customModDescription: item.customModDescription || null,
      }));

      const pools = data?.pools || [];
      const encArmour = pools.filter(p => p.isEnclaveArmour);
      const encPlasma = pools.filter(p => p.isEnclavePlasmaGun);
      if (encArmour.length) {
        const subItems = encArmour.flatMap(p => (p.items || []).map(it => ({
          name: it.name || "", dropRate: it.dropRate != null ? Number(it.dropRate) : null,
        })));
        allItems.push({
          name: "Enclave Urban Scout Armour", formid: encArmour[0].lvliFormID || "", edid: encArmour[0].lvliEdid || "",
          dropRate: encArmour[0].poolChance != null ? Number(encArmour[0].poolChance) : null,
          tradeable: null, affix: null, conditions: [], imageUrl: null, kind: "enclave_pool",
          modSlots: null, subItems,
        });
      }
      if (encPlasma.length) {
        const subItems = encPlasma.flatMap(p => (p.items || []).map(it => ({
          name: it.name || "", dropRate: it.dropRate != null ? Number(it.dropRate) : null,
        })));
        allItems.push({
          name: "Enclave Plasma Gun Mod Boxes", formid: encPlasma[0].lvliFormID || "", edid: encPlasma[0].lvliEdid || "",
          dropRate: encPlasma[0].poolChance != null ? Number(encPlasma[0].poolChance) : null,
          tradeable: null, affix: null, conditions: [], imageUrl: null, kind: "enclave_pool",
          modSlots: null, subItems,
        });
      }

      const byName = new Map();
      allItems.forEach(item => {
        const key = (item.name || "").trim().toLowerCase();
        if (!key) return;
        if (!byName.has(key)) { byName.set(key, []); }
        byName.get(key).push(item);
      });
      const consolidated = [];
      const seen = new Set();
      allItems.forEach(item => {
        const key = (item.name || "").trim().toLowerCase();
        if (seen.has(key)) return;
        seen.add(key);
        const group = byName.get(key) || [item];
        if (group.length > 1) {
          const tiers = group.map(g => ({ qty: g.qty || 1, dropRate: g.dropRate }));
          const merged = { ...group[0], _hasTiers: true, _tiers: tiers };
          consolidated.push(merged);
        } else {
          consolidated.push(item);
        }
      });
      allItems = consolidated;
    } else {
      allItems = [];
    }

    _planProgress      = loadPlanProgress(pageId);
    _planProgressPageId = pageId;

    _uerItemIds.clear();
    allItems.forEach(item => {
      const id = item.formid || item.name || "";
      if (id) _uerItemIds.add(id);
    });

    const content = document.createElement("div");
    if (!allItems.length) {
      content.appendChild(el("div", { class: "dfbnb-evr__empty" }, ["No unique rewards for this activity."]));
    } else {
      // Simplified display without detailed rows
      const table = el("table", { class: "dfbnb-evr__table" });
      const thead = el("thead"), trh = el("tr");
      trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Item"]));
      trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]));
      thead.appendChild(trh);

      const tbody = el("tbody");
      allItems.forEach(it => {
        const tr = el("tr");
        const nameCell = el("td", { class: "dfbnb-evr__cell" });
        nameCell.textContent = it.name || "";
        tr.appendChild(nameCell);

        const drCell = el("td", { class: "dfbnb-evr__cell" });
        drCell.textContent = it.dropRate != null ? fmtPct(it.dropRate) : "—";
        tr.appendChild(drCell);
        tbody.appendChild(tr);

        // Mod slot breakdown rows (for unique weapons/armour with legendary effects)
        if (it.modSlots && it.modSlots.length) {
          renderModSlotRows(tbody, it.modSlots, "2", it.customModName, it.customModDescription);
        }
      });
      table.appendChild(thead);
      table.appendChild(tbody);
      content.appendChild(table);
    }

    setTimeout(() => updatePlanProgressUI(pageId, allItems.length), 0);
    return renderExpand({ title: "Unique Activity Rewards", content, open: false, pageId });
  }

  // ── Activity: Plans expand ─────────────────────────────────────────────────

  function renderPlansExpand(data, pageId) {
    const adPlans = data?.activityData?.planRewards;

    let items = [];

    if (Array.isArray(adPlans) && adPlans.length) {
      const seen = new Set();
      adPlans.forEach(it => {
        const key = it.formid || it.name || "";
        if (key && seen.has(key)) return;
        if (key) seen.add(key);
        items.push(it);
      });
    } else {
      return null;
    }

    if (!items.length) return null;

    items.sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    const content = document.createElement("div");

    const table = el("table", { class: "dfbnb-evr__table" });
    const thead = el("thead"), trh = el("tr");
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Item"]));
    trh.appendChild(el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]));
    thead.appendChild(trh);

    const tbody = el("tbody");
    items.forEach(it => {
      const tr = el("tr");
      const nameCell = el("td", { class: "dfbnb-evr__cell" });
      nameCell.textContent = it.name || "";
      tr.appendChild(nameCell);

      const drCell = el("td", { class: "dfbnb-evr__cell" });
      const dr = it.dropRate != null ? Number(it.dropRate) : null;
      drCell.textContent = dr != null ? fmtPct(dr) : "—";
      tr.appendChild(drCell);
      tbody.appendChild(tr);

      // Per-plan conditions (region, level requirements, etc.)
      const planConds = cleanConditions(it.conditions);
      if (planConds.length) {
        const condLabelRow = el("tr");
        condLabelRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: "2" }, [
          el("span", { class: "dfbnb-act-base__cond-label" }, ["Drop Conditions:"])
        ]));
        tbody.appendChild(condLabelRow);
        planConds.forEach(cond => {
          const condRow = el("tr");
          condRow.appendChild(el("td", { class: "dfbnb-act-base__cond", colspan: "2" }, [cond]));
          tbody.appendChild(condRow);
        });
      }

      // Item image gallery (e.g. Brotherhood Civvies front/back)
      const planImgKey = (it.name || "").trim().toLowerCase();
      const planImages = ITEM_IMAGES[planImgKey];
      if (planImages) tbody.appendChild(renderImageGalleryRow(planImages, "2"));
    });

    table.appendChild(thead);
    table.appendChild(tbody);
    content.appendChild(table);

    return renderExpand({
      title: "Plan Rewards", content, open: false, pageId,
      subtitle: `${items.length} plans \u00B7 rolled on completion`,
    });
  }

  // ── Activity: Region Rewards expand ────────────────────────────────────────

  function renderRegionRewardsExpand(data, pageId) {
    const adRegions = data?.activityData?.regionRewards;
    if (!adRegions) return null;

    const availableRegions = adRegions.availableRegions || [];
    const byRegion         = adRegions.byRegion || {};
    if (!availableRegions.length && !Object.keys(byRegion).length) return null;

    const outerContent = document.createElement("div");

    availableRegions.forEach(regionName => {
      const regionItems = byRegion[regionName] || [];
      if (!regionItems.length) return;

      const byCategory = {};
      regionItems.forEach(item => {
        const cat = item.category || "Other";
        if (!byCategory[cat]) byCategory[cat] = [];
        byCategory[cat].push(item);
      });

      const regionContent = document.createElement("div");

      Object.entries(byCategory).sort(([a], [b]) => a.localeCompare(b)).forEach(([cat, items]) => {
        const catLabel = el("div", { class: "dfbnb-act-region-cat-label" }, [`${cat} (${items.length})`]);
        regionContent.appendChild(catLabel);

        const tbl = el("table", { class: "dfbnb-evr__table" });
        tbl.appendChild(el("thead", {}, [el("tr", {}, [
          el("th", { class: "dfbnb-evr__th" }, ["Item"]),
          el("th", { class: "dfbnb-evr__th" }, ["Drop Rate"]),
        ])]));
        const tbody = document.createElement("tbody");
        items.sort((a, b) => (a.name || "").localeCompare(b.name || "")).forEach(item => {
          const dr = item.dropRate != null ? `${Number(item.dropRate).toFixed(2)}%` : "—";
          tbody.appendChild(el("tr", {}, [
            el("td", { class: "dfbnb-evr__cell dfbnb-evr__cell--label" }, [item.name || item.formid || "—"]),
            el("td", { class: "dfbnb-evr__cell" }, [dr]),
          ]));
        });
        tbl.appendChild(tbody);
        regionContent.appendChild(tbl);
      });

      outerContent.appendChild(renderExpand({
        title: regionName,
        subtitle: `${regionItems.length} items`,
        content: regionContent,
        open: false, pageId,
      }));
    });

    if (!outerContent.children.length) return null;

    return renderExpand({
      title: "Region Rewards",
      subtitle: `${availableRegions.length} regions`,
      content: outerContent,
      open: false, pageId,
    });
  }

  // ── Header + Controls ──────────────────────────────────────────────────────

  function clearModuleLocalStorage(pageId) {
    Object.keys(localStorage).forEach(k => {
      if (k.startsWith(`${pageId}::${MODULE_ID}::`)) localStorage.removeItem(k);
    });
  }

  // ── Main render ────────────────────────────────────────────────────────────

  function renderAll({ rootEl, pageId, data, guideApi }) {
    rootEl.innerHTML = "";
    rootEl.classList.add("dfbnb-evr");

    rootEl.appendChild(renderActivityHeader({ pageId, rootEl, data }));

    ;(data.warnings || []).forEach(w =>
      renderWarning(rootEl, w.title || "Warning", w.message || "Missing data.")
    );

    // Reward Tree (xEdit-style expandable LVLI sections)
    const treeEl = renderRewardTree(data, pageId);
    if (treeEl) {
      rootEl.appendChild(treeEl);
    } else {
      // Fallback to old category-based rendering if no tree data
      const chemExpand = renderChemRewardsExpand(data, pageId);
      if (chemExpand) rootEl.appendChild(chemExpand);
      rootEl.appendChild(renderUniqueEventRewardsExpand(data, pageId));
      const regionExpand = renderRegionRewardsExpand(data, pageId);
      if (regionExpand) rootEl.appendChild(regionExpand);
      const plansExpand = renderPlansExpand(data, pageId);
      if (plansExpand) rootEl.appendChild(plansExpand);
    }

    if (guideApi?.registerSearchTarget)
      guideApi.registerSearchTarget({ id: MODULE_ID, rootEl, getText: () => rootEl.innerText || "" });
    if (guideApi?.registerModule)
      guideApi.registerModule(MODULE_ID, api);
  }

  // ── Data loading ───────────────────────────────────────────────────────────

  async function loadDataForPage(guideApi) {
    const { byPageUrl, slug } = getDataUrlsFromPage();
    const pathKey = stripTrailingSlash(window.location.pathname || "");

    if (guideApi?.getJsonUrl) {
      const u = guideApi.getJsonUrl("activities_rewards_by_page");
      if (u) {
        const j = await fetchJson(u);
        if (j?.byPage) {
          if (slug    && j.byPage[slug])    return j.byPage[slug];
          if (pathKey && j.byPage[pathKey]) return j.byPage[pathKey];
        }
      }
    }

    if (byPageUrl) {
      const j = await fetchJson(byPageUrl);
      if (j?.byPage) {
        if (slug    && j.byPage[slug])    return j.byPage[slug];
        if (pathKey && j.byPage[pathKey]) return j.byPage[pathKey];
      }
    }

    return null;
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  const emptyData = () => ({
    name: "Activity", freeRewards: [], baseRewards: { tiers: [] },
    pools: [], banners: [], scenarios: []
  });

  const api = {
    async prefetch() {
      const { byPageUrl } = getDataUrlsFromPage();
      if (!byPageUrl) return;
      try { await fetchJson(byPageUrl); } catch (_) {}
    },

    async mount(guideApi, data) {
      if (!isEventsRewardsPage(data)) return;
      const pageId = getPageId();
      const rootEl = ensureRootEl();
      let pageData = data;

      if (!pageData) {
        try {
          pageData = await loadDataForPage(guideApi);
        } catch (err) {
          pageData = { ...emptyData(), warnings: [{ title: "Load error", message: String(err?.message || err) }] };
        }
      }

      if (!pageData) {
        const { byPageUrl, slug } = getDataUrlsFromPage();
        pageData = {
          ...emptyData(),
          warnings: [{
            title: "Missing data",
            message: [
              "No JSON matched this page.",
              `slug: ${slug || "(none)"}`,
              `path: ${stripTrailingSlash(window.location.pathname)}`,
              `byPageUrl: ${byPageUrl || "(not provided on page)"}`
            ].join(" | ")
          }]
        };
      }

      renderAll({ rootEl, pageId, data: pageData, guideApi });
    },

    search(query) {
      const q      = String(query || "").trim().toLowerCase();
      const rootEl = document.querySelector("[data-dfbnb-activities-root]");
      if (!rootEl) return;
      rootEl.querySelectorAll(".dfbnb-search-hit").forEach(n => n.classList.remove("dfbnb-search-hit"));
      if (!q) return;
      rootEl.querySelectorAll(".dfbnb-evr__cell,.dfbnb-expand__title,.dfbnb-notice__line,.dfbnb-warning__msg")
        .forEach(n => { if ((n.textContent || "").toLowerCase().includes(q)) n.classList.add("dfbnb-search-hit"); });
    }
  };

  // ── Auto-mount ─────────────────────────────────────────────────────────────

  document.addEventListener("DOMContentLoaded", async () => {
    try { console.info("[DFBNB Activities]", VERSION, window.location.pathname); } catch (_) {}
    const guideApi = window.DFBNB_Guide || window.DFBNB_GuideShell || null;
    try { await api.prefetch(); } catch (_) {}
    try { await api.mount(guideApi); } catch (_) {}
  });

  window.__DFBNB_ACTIVITIES_API = api;

})();
