/* =========================================================
   DF / BNB GUIDE SHELL (FULL)
   - Data from:
       /wp-content/uploads/nav.json
       /wp-content/uploads/guide_index.tsv
   - Fast-load patterns pulled from Home + Category shells:
       preload fetch hints, cache-friendly fetch, prefetch on intent,
       idle work deferral, stable localStorage keys
   - Outer shell fixed, inner guide switches via dropdowns
   ========================================================= */

(function () {
  "use strict";

  // Shell safety: ensure this exists even if the shell loader changed
  window.setActivePageId = window.setActivePageId || function () {};

const shell = document.getElementById("df-bnb-shell");
if (!shell) return;

// Allow this file to be loaded on any shell page.
// Auto-boot only on supported shell page types.
const PAGE_KIND = String(shell.getAttribute("data-page") || "").trim();
const IS_GUIDE_PAGE = PAGE_KIND === "guide";
const IS_MEMBER_PINS_PAGE = PAGE_KIND === "member-pins";

  const PAGE_ID = PAGE_KIND || "guide";
  const lsKey = (k) => `${PAGE_ID}:${k}`;

  // JSON URL registry used by modules (Events Rewards, etc.)
window.__DFBNB_JSON_URLS = window.__DFBNB_JSON_URLS || {};

if (window.dfbnbData) {
  if (window.dfbnbData.events_rewards_by_page_url) {
    window.__DFBNB_JSON_URLS.events_rewards_by_page = window.dfbnbData.events_rewards_by_page_url;
  }
  if (window.dfbnbData.events_rewards_patchlog_url) {
    window.__DFBNB_JSON_URLS.patchlog_events_rewards = window.dfbnbData.events_rewards_patchlog_url;
  }
}

  // Toggle this to true ONLY when updating JSON/TSV and you need instant changes.
  const DEV_NO_CACHE = false;

    // ===== Account pins (user_meta via WP REST) =====
  const REST_BASE = (window.dfbnbData && safeText(window.dfbnbData.rest_url)) ? safeText(window.dfbnbData.rest_url) : "";
  const REST_NONCE = (window.dfbnbData && safeText(window.dfbnbData.rest_nonce)) ? safeText(window.dfbnbData.rest_nonce) : "";
const IS_LOGGED_IN =
  !!(window.dfbnbData && String(window.dfbnbData.is_logged_in || "") === "1");


    // Members-only pins (server-only, no localStorage)

  async function apiGetPins() {
    if (!IS_LOGGED_IN || !REST_BASE) return { guides: [], categories: [] };

    const url = REST_BASE.replace(/\/+$/, "") + "/pins";
    const res = await fetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: {
        "Accept": "application/json",
        ...(REST_NONCE ? { "X-WP-Nonce": REST_NONCE } : {})
      }
    });

    if (!res.ok) throw new Error("Pins GET failed (" + res.status + ")");
    const data = await res.json();

    const pins = (data && data.pins && typeof data.pins === "object") ? data.pins : {};
    return {
      guides: Array.isArray(pins.guides) ? pins.guides.map(String) : [],
      categories: Array.isArray(pins.categories) ? pins.categories.map(String) : []
    };
  }

  async function apiSavePins(pins) {
    if (!IS_LOGGED_IN || !REST_BASE) return pins;

    const url = REST_BASE.replace(/\/+$/, "") + "/pins";
    const res = await fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        ...(REST_NONCE ? { "X-WP-Nonce": REST_NONCE } : {})
      },
      body: JSON.stringify({ pins })
    });

    if (!res.ok) throw new Error("Pins POST failed (" + res.status + ")");
  }
  
  // =========================
  // VIEWS (global, server-first)
  // =========================
  async function apiPostView(id) {

    if (!REST_BASE) return;
    const url = REST_BASE.replace(/\/+$/, "") + "/view";
    try {
      await fetch(url, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ id })
      });
    } catch (e) {
      // quiet fail (local fallback handled elsewhere)
    }
  }

  async function apiGetViewsBatch(ids) {
    if (!REST_BASE) return null;
    const url = REST_BASE.replace(/\/+$/, "") + "/views/batch";
    try {
      const res = await fetch(url, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ ids })
      });
      if (!res.ok) return null;
      const j = await res.json();
      return (j && j.ok && j.map) ? j.map : null;
    } catch (e) {
      return null;
    }
  }

  function bumpLocalViewFallback(id) {
    try {
      const k = "dfbnb:views_local_v1";
      const raw = localStorage.getItem(k);
      const obj = raw ? JSON.parse(raw) : {};
      const cur = obj && obj[id] && Number.isFinite(obj[id]) ? obj[id] : 0;
      obj[id] = cur + 1;
      localStorage.setItem(k, JSON.stringify(obj));
      return obj[id];
    } catch (e) {
      return null;
    }
  }

  function readLocalViewFallback(id) {
    try {
      const k = "dfbnb:views_local_v1";
      const raw = localStorage.getItem(k);
      const obj = raw ? JSON.parse(raw) : {};
      const cur = obj && obj[id] && Number.isFinite(obj[id]) ? obj[id] : 0;
      return cur;
    } catch (e) {
      return 0;
    }
   }

  // =========================================================
  // PINS (Unified: Guides + Challenges)
  // - server-first when logged in
  // - localStorage fallback when logged out / endpoint fails
  // - one-time migration local -> server if server empty
  // =========================================================
  const PINS_LS_KEY = "dfbnb_pins_guides_v1";
  const PINS_MIGRATED_LS_KEY = "dfbnb_pins_guides_migrated_v1";

  let __dfbnbPinsLoaded = false;
  let __dfbnbPinsSet = new Set();
  let __dfbnbPinsSaveTimer = null;

  function readLocalPins() {
    try {
      const raw = localStorage.getItem(PINS_LS_KEY);
      const arr = raw ? JSON.parse(raw) : [];
      return Array.isArray(arr) ? arr.map(String).filter(Boolean) : [];
    } catch (e) {
      return [];
    }
  }

  function writeLocalPins(list) {
    try {
      const arr = Array.isArray(list) ? list.map(String).filter(Boolean) : [];
      localStorage.setItem(PINS_LS_KEY, JSON.stringify(arr.slice(0, 500)));
    } catch (e) {}
  }

  function setPinsFromArray(arr) {
    __dfbnbPinsSet = new Set((Array.isArray(arr) ? arr : []).map(String).filter(Boolean));
  }

  async function loadPinsFromServerOrLocal() {
    if (__dfbnbPinsLoaded) return;

    const local = readLocalPins();

    // Logged out: local only
    if (!IS_LOGGED_IN) {
      setPinsFromArray(local);
      __dfbnbPinsLoaded = true;
      return;
    }

    // Logged in: server-first
    try {
      const j = await apiGetPins();
      const serverGuides = (j && Array.isArray(j.guides)) ? j.guides.map(String).filter(Boolean) : [];

      if (serverGuides.length > 0) {
        setPinsFromArray(serverGuides);
        writeLocalPins(serverGuides);
      } else {
        // server empty: use local
        setPinsFromArray(local);

        // one-time migration local -> server
        const migrated = localStorage.getItem(PINS_MIGRATED_LS_KEY) === "1";
        if (!migrated && local.length > 0) {
          try {
            await apiSavePins({ guides: local, categories: [] });
            localStorage.setItem(PINS_MIGRATED_LS_KEY, "1");
          } catch (e) {}
        }
      }
    } catch (e) {
      // server failed: local fallback
      setPinsFromArray(local);
    }

    __dfbnbPinsLoaded = true;
  }

  function schedulePinsSaveToServer() {
    // Always persist local for instant UI and logged-out behavior
    writeLocalPins(Array.from(__dfbnbPinsSet));

    if (!IS_LOGGED_IN) return;

    if (__dfbnbPinsSaveTimer) clearTimeout(__dfbnbPinsSaveTimer);
    __dfbnbPinsSaveTimer = setTimeout(async () => {
      try {
        const guides = Array.from(__dfbnbPinsSet);
        await apiSavePins({ guides, categories: [] });
      } catch (e) {}
    }, 800);
  }

  function setPinButtonUI(isPinned) {
    const btn = byId("dfbnbPinBtn") || document.querySelector('[data-pin-btn], .dfbnb-pin-btn');
    if (!btn) return;

    btn.setAttribute("aria-pressed", isPinned ? "true" : "false");
    btn.classList.toggle("is-pinned", !!isPinned);

    // icon-only UI (no language dependency)
    const label = isPinned ? "Unpin guide" : "Pin guide";
    btn.setAttribute("aria-label", label);
    btn.title = label;
  }

  async function wirePinButtonForGuide(guideUrl) {
    const btn = byId("dfbnbPinBtn") || document.querySelector('[data-pin-btn], .dfbnb-pin-btn');
    if (!btn) return;

    const url = normalizePath(guideUrl);

    await loadPinsFromServerOrLocal();
    setPinButtonUI(__dfbnbPinsSet.has(url));

    btn.addEventListener("click", () => {
      const pinned = __dfbnbPinsSet.has(url);
      if (pinned) __dfbnbPinsSet.delete(url);
      else __dfbnbPinsSet.add(url);

      schedulePinsSaveToServer();
      setPinButtonUI(!pinned);
    });
  }

  // Static data file paths (confirmed upload locations)
  const DEFAULT_NAV_URL = location.origin + "/wp-content/uploads/nav.json";
  const DEFAULT_GUIDE_INDEX_URL = location.origin + "/wp-content/uploads/guide_index.tsv";

  // Allow optional server-provided overrides (matches Home pattern)
  const NAV_URL = (window.dfbnbData && safeText(window.dfbnbData.nav_url)) ? safeText(window.dfbnbData.nav_url) : DEFAULT_NAV_URL;

  // =========================================================
// Per-guide header overrides (only apply if guide data is blank)
// Keys must match the page pathname (start + end with /)
// =========================================================
const GUIDE_META_OVERRIDES = {
  "/df/calculators/season-ticket/": {
    thanks: "Damashu",
    imageCredit: "", // optional
    creditTo: ""     // optional
  },

  "/df/titles/player-titles/checklist/": {
    thanks: "NerditBabe",
    imageCredit: "", // optional
    creditTo: ""     // optional
  }
};

function applyGuideMetaOverrides(guide, pathname) {
  try {
    if (!guide) return;

    const key = normalizePath(pathname || guide.url || location.pathname || "");
    const o = GUIDE_META_OVERRIDES[key];
    if (!o) return;

    if (safeText(o.thanks) && !safeText(guide.thanks)) {
      guide.thanks = safeText(o.thanks);
    }

    if (safeText(o.imageCredit) && !safeText(guide.imageCredit) && !safeText(guide.image_credit)) {
      guide.imageCredit = safeText(o.imageCredit);
    }

    if (safeText(o.creditTo) && !safeText(guide.creditTo) && !safeText(guide.credit_to)) {
      guide.creditTo = safeText(o.creditTo);
    }
  } catch {}
}

  const GUIDE_INDEX_URL = (window.dfbnbData && safeText(window.dfbnbData.guide_index_url)) ? safeText(window.dfbnbData.guide_index_url) : DEFAULT_GUIDE_INDEX_URL;

  /* ===================== PATCH LOG (Shell) ===================== */

  // Optional manifest that maps current pathname -> patch log feed URL.
  // If not configured, Patch Log simply will not render.
  const PATCH_LOG_MANIFEST_URL =
    (window.dfbnbData && safeText(window.dfbnbData.patch_log_manifest_url))
      ? safeText(window.dfbnbData.patch_log_manifest_url)
      : "";

  // Cache the manifest + per-feed patch logs so we don't refetch on every navigation.
  let __dfbnbPatchLogManifestCache = null;
  let __dfbnbPatchLogManifestPromise = null;

  const __dfbnbPatchLogFeedCache = Object.create(null);
  const __dfbnbPatchLogFeedPromise = Object.create(null);

  async function fetchPatchLogManifest() {
    if (!PATCH_LOG_MANIFEST_URL) return null;
    if (__dfbnbPatchLogManifestCache) return __dfbnbPatchLogManifestCache;
    if (__dfbnbPatchLogManifestPromise) return __dfbnbPatchLogManifestPromise;

    __dfbnbPatchLogManifestPromise = (async () => {
      try {
        const res = await fetch(PATCH_LOG_MANIFEST_URL, { cache: "default" });
        if (!res.ok) return null;
        const raw = await res.json();
        __dfbnbPatchLogManifestCache = raw || null;
        return __dfbnbPatchLogManifestCache;
      } catch {
        return null;
      } finally {
        __dfbnbPatchLogManifestPromise = null;
      }
    })();

    return __dfbnbPatchLogManifestPromise;
  }

  function normalizePatchLogJSON(raw) {
    // Accept:
    // - { entries: [...] }
    // - [ ... ] (treated as entries)
    // - { ...singleEntry } (wrapped)
    if (!raw) return { entries: [] };
    if (Array.isArray(raw)) return { entries: raw };
    if (Array.isArray(raw.entries)) return { entries: raw.entries };
    return { entries: [raw] };
  }

  function countPatchEntry(entry) {
    const a = Array.isArray(entry.added) ? entry.added.length : 0;
    const r = Array.isArray(entry.removed) ? entry.removed.length : 0;
    const c = Array.isArray(entry.changed) ? entry.changed.length : 0;
    return { a, r, c, total: a + r + c };
  }

  function formatPatchLogTsUTC(tsRaw) {
  const s = safeText(tsRaw || "");
  if (!s) return "";

  // If it's ISO-like, format in UTC as "YYYY-MM-DD HH:MMZ"
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    const hh = String(d.getUTCHours()).padStart(2, "0");
    const mm = String(d.getUTCMinutes()).padStart(2, "0");
    return `${y}-${m}-${day} ${hh}:${mm}Z`;
  }

  // Fallback: show whatever string we got (still safeText)
  return s;
}

  function renderPatchLogEntry(entry) {
    const wrap = document.createElement("div");
    wrap.className = "dfbnbPatchLogEntry";

    const meta = document.createElement("div");
    meta.className = "dfbnbPatchLogMeta";

  // Prefer explicit timestamp fields; fall back quietly.
const tsRaw = safeText(entry.ts || entry.timestamp || entry.date || "");
const ts = formatPatchLogTsUTC(tsRaw);
if (ts) meta.textContent = ts;

    const stats = countPatchEntry(entry);

    const body = document.createElement("div");
    body.className = "dfbnbPatchLogBody";

    const current = document.createElement("div");
    current.className = "dfbnbPatchLogSmall";
    current.textContent = `Current: ${Number.isFinite(entry.current) ? entry.current : "—"}  Added: ${stats.a}  Removed: ${stats.r}  Changed: ${stats.c}`;

    const added = document.createElement("div");
    added.className = "dfbnbPatchLogRow";
    added.innerHTML = `<strong>Added:</strong> ${stats.a ? safeText((entry.added || []).join(", ")) : "—"}`;

    const removed = document.createElement("div");
    removed.className = "dfbnbPatchLogRow";
    removed.innerHTML = `<strong>Removed:</strong> ${stats.r ? safeText((entry.removed || []).join(", ")) : "—"}`;

    const changed = document.createElement("div");
    changed.className = "dfbnbPatchLogRow";
    changed.innerHTML = `<strong>Changed:</strong> ${stats.c ? safeText((entry.changed || []).join(", ")) : "—"}`;

    body.appendChild(current);
    body.appendChild(added);
    body.appendChild(removed);
    body.appendChild(changed);

    if (meta.textContent) wrap.appendChild(meta);
    wrap.appendChild(body);

    return wrap;
  }

  async function fetchPatchLogFeed(url) {
    const u = safeText(url);
    if (!u) return null;

    if (__dfbnbPatchLogFeedCache[u]) return __dfbnbPatchLogFeedCache[u];
    if (__dfbnbPatchLogFeedPromise[u]) return __dfbnbPatchLogFeedPromise[u];

    __dfbnbPatchLogFeedPromise[u] = (async () => {
      try {
        const res = await fetch(u, { cache: "default" });
        if (!res.ok) return null;
        const raw = await res.json();
        const norm = normalizePatchLogJSON(raw);
        __dfbnbPatchLogFeedCache[u] = norm;
        return norm;
      } catch {
        return null;
      } finally {
        delete __dfbnbPatchLogFeedPromise[u];
      }
    })();

    return __dfbnbPatchLogFeedPromise[u];
  }

  function getPatchLogHost() {
  return document.getElementById("dfbnbPatchLogShell");
}

function renderEmptyPatchLog(host, message) {
  if (!host) return;

  const panel = document.createElement("section");
  panel.className = "dfbnbPatchLog";

  const headBtn = document.createElement("button");
  headBtn.type = "button";
  headBtn.className = "dfbnbPatchLogHead";
  headBtn.setAttribute("aria-expanded", "false");

  const title = document.createElement("div");
  title.className = "dfbnbPatchLogTitle";
  title.textContent = "PATCH LOG";
  headBtn.appendChild(title);

  const inner = document.createElement("div");
  inner.className = "dfbnbPatchLogInner";
  inner.hidden = true;

  headBtn.addEventListener("click", () => {
    const isOpen = headBtn.getAttribute("aria-expanded") === "true";
    headBtn.setAttribute("aria-expanded", isOpen ? "false" : "true");
    inner.hidden = isOpen ? true : false;
  });

  const msg = document.createElement("div");
  msg.className = "dfbnbPatchLogSmall";
  msg.textContent = message || "No patch notes yet for this page.";
  inner.appendChild(msg);

  panel.appendChild(headBtn);
  panel.appendChild(inner);

  host.innerHTML = "";
  host.appendChild(panel);
  host.style.display = "";
}

function clearPatchLogHost(message) {
  const host = getPatchLogHost();
  if (!host) return;

  // Always show the PATCH LOG panel, even when empty.
  renderEmptyPatchLog(host, message || "No patch notes yet for this page.");
}

async function updatePatchLogForPath(pathname) {
  const host = getPatchLogHost();
  if (!host) return;

  // Always show something, even if patch log isn't configured yet.
  if (!PATCH_LOG_MANIFEST_URL) {
    clearPatchLogHost("No patch notes yet for this page.");
    return;
  }

  const manifest = await fetchPatchLogManifest();
  const byPage = (manifest && manifest.byPage && typeof manifest.byPage === "object") ? manifest.byPage : null;
  if (!byPage) {
    clearPatchLogHost("Patch log is temporarily unavailable.");
    return;
  }

  const key = normalizePath(pathname || location.pathname || "");
  const ent = byPage[key] || null;
  const feedUrl = (ent && ent.url) ? safeText(ent.url) : "";

  if (!feedUrl) {
    clearPatchLogHost("No patch notes yet for this page.");
    return;
  }

  const doRender = async () => {
    const data = await fetchPatchLogFeed(feedUrl);
    if (!data || !Array.isArray(data.entries) || !data.entries.length) {
      clearPatchLogHost("No patch notes yet for this page.");
      return;
    }

    const entries = data.entries.slice(0, 20);

    const panel = document.createElement("section");
    panel.className = "dfbnbPatchLog";

    const headBtn = document.createElement("button");
    headBtn.type = "button";
    headBtn.className = "dfbnbPatchLogHead";
    headBtn.setAttribute("aria-expanded", "false");

    const title = document.createElement("div");
    title.className = "dfbnbPatchLogTitle";
    title.textContent = "PATCH LOG";
    headBtn.appendChild(title);

    const inner = document.createElement("div");
    inner.className = "dfbnbPatchLogInner";
    inner.hidden = true;

    headBtn.addEventListener("click", () => {
      const isOpen = headBtn.getAttribute("aria-expanded") === "true";
      headBtn.setAttribute("aria-expanded", isOpen ? "false" : "true");
      inner.hidden = isOpen ? true : false;
    });

    const frag = document.createDocumentFragment();
    for (const e of entries) frag.appendChild(renderPatchLogEntry(e));
    inner.appendChild(frag);

    panel.appendChild(headBtn);
    panel.appendChild(inner);

    host.innerHTML = "";
    host.appendChild(panel);
    host.style.display = "";
  };

  // Lazy: do not block paint.
  try {
    if ("requestIdleCallback" in window) {
      window.requestIdleCallback(() => { doRender(); }, { timeout: 1200 });
    } else {
      setTimeout(() => { doRender(); }, 50);
    }
  } catch {
    setTimeout(() => { doRender(); }, 50);
  }
}

  // Preload-as-fetch hints (cheap win)
  preloadFetch(NAV_URL);
  preloadFetch(GUIDE_INDEX_URL);

  // Load optional feature CSS without touching PHP.
// Used to keep df-bnb-guide.css small.
function ensureFeatureCSSLoaded(id, href) {
  if (!href) return;
  if (document.getElementById(id)) return;

  const link = document.createElement("link");
  link.id = id;
  link.rel = "stylesheet";
  link.href = href;
  document.head.appendChild(link);
}

// Build a CSS URL inside /assets/ next to the theme root (keeps ?ver= in sync)
// Replaces 6 identical blocks of script-tag introspection scattered across lazy-mount functions.
// Returns { modSrc, query } or null if the guide script tag can't be found.
function loadAssetModule(jsFileName) {
  const s = document.querySelector('script[src*="df-bnb-guide.js"]');
  const src = s ? String(s.src || "") : "";
  if (!src) return Promise.reject(new Error("[loadAssetModule] df-bnb-guide.js script tag not found"));

  const qIndex = src.indexOf("?");
  const base = (qIndex >= 0) ? src.slice(0, qIndex) : src;
  const query = (qIndex >= 0) ? src.slice(qIndex) : "";

  const lastSlash = base.lastIndexOf("/");
  if (lastSlash < 0) return Promise.reject(new Error("[loadAssetModule] Could not resolve base dir"));

  const baseDir = base.slice(0, lastSlash + 1); // ends with /
  const modSrc = baseDir + "assets/" + jsFileName + query;

  return new Promise((resolve, reject) => {
    const tag = document.createElement("script");
    tag.id = "dfbnb-" + jsFileName.replace(/\.js$/, "").replace(/[^a-z0-9-]/gi, "-") + "-js";
    tag.src = modSrc;
    tag.async = true;
    tag.onload = resolve;
    tag.onerror = reject;
    document.head.appendChild(tag);
  });
}

// Build a CSS URL inside /assets/ next to the theme root (keeps ?ver= in sync)
function cssAssetURL(cssFileName) {
  const s = document.querySelector('script[src*="df-bnb-guide.js"]');
  const src = s ? String(s.src || "") : "";
  if (!src) return "";

  // Build a cache-busting query for dynamically-loaded feature assets.
  // Prefer server-provided bundle version (updates when any feature asset changes),
  // otherwise fall back to the df-bnb-guide.js query string.
  const qIndex = src.indexOf("?");
  const base = (qIndex >= 0) ? src.slice(0, qIndex) : src;

  let query = "";
  const bundleVer =
    (window.dfbnbData && (window.dfbnbData.assets_ver || window.dfbnbData.assetsVer))
      ? String(window.dfbnbData.assets_ver || window.dfbnbData.assetsVer)
      : "";

  if (bundleVer) query = "?ver=" + encodeURIComponent(bundleVer);
  else query = (qIndex >= 0) ? src.slice(qIndex) : "";

  // baseDir = folder that df-bnb-guide.js is in
  const lastSlash = base.lastIndexOf("/");
  if (lastSlash < 0) return "";

  const baseDir = base.slice(0, lastSlash + 1); // ends with /
  return baseDir + "assets/" + cssFileName + query;
}

function isChallengesChecklistPath(pathname) {
  const p = String(pathname || location.pathname || "");
  if (p.indexOf("/challenges/") !== -1) return true;

  // Scouts checklists are /scouts/<tadpole|possum>/checklist/
  if (p.indexOf("/scouts/") !== -1 && /\/checklist\/?$/.test(p)) return true;

  // Mini seasons pages you mentioned end in "challenge-checklist" (mounting only, data binding comes later)
  if (p.indexOf("/seasons/") !== -1 && /challenge-checklist\/?$/.test(p)) return true;

  return false;
}

function ensureChallengesCSSIfNeeded(pathname) {
  if (!isChallengesChecklistPath(pathname)) return;

  const href = cssAssetURL("df-bnb-challenges.css");
  ensureFeatureCSSLoaded("dfbnb-challenges-css", href);
}

/* =========================
   CAMP TITLES (CMPT) FEATURE MOUNT
   - routes under /camp/titles/
   ========================= */
function isCampTitlesPath(pathname) {
  const p = String(pathname || location.pathname || "");

  // Titles feature pages (checklist + generator)
  // NEW DF routes:
  // - /df/titles/camp-titles/(checklist|generator)/
  // - /df/titles/player-titles/(checklist|generator)/
  //
  // Back-compat (old routes for checklist only):
  // - /df/camp/camp-titles/checklist/
  // - /df/collectables/player-titles/checklist/
  return (
    /\/titles\/camp-titles\/(checklist|generator)\/?$/.test(p) ||
    /\/titles\/player-titles\/(checklist|generator)\/?$/.test(p) ||
    /\/camp\/camp-titles\/checklist\/?$/.test(p) ||
    /\/collectables\/player-titles\/checklist\/?$/.test(p)
  );
}

function isTitlesGeneratorPath(pathname) {
  const p = String(pathname || location.pathname || "");
  return (
    /\/titles\/camp-titles\/generator\/?$/.test(p) ||
    /\/titles\/player-titles\/generator\/?$/.test(p)
  );
}

function ensureCampTitlesCSSIfNeeded(pathname) {
  if (!isCampTitlesPath(pathname)) return;

  const href = cssAssetURL("df-bnb-titles.css");
  ensureFeatureCSSLoaded("dfbnb-titles-css", href);
}

/* =========================
   PLAN SYSTEM (plans + skins + big bloom)
   ========================= */
function isPlanSystemPath(pathname) {
  const p = String(pathname || location.pathname || "");

  // DF plan checklists
  if (/\/df\/plan-checklists\/(apparel|armour|backpack-mod|recipe|weapon)\/?$/.test(p)) return true;

  // BNB plan checklists
  if (/\/bnb\/plan-checklists\/(apparel|armour|backpack-mod|recipe|weapon)\/?$/.test(p)) return true;

  // BNB skins pages
  if (p.indexOf("/bnb/armour/body-armour/body-armour-skins/") !== -1) return true;
  if (p.indexOf("/bnb/armour/power-armour/power-armour-skins/") !== -1) return true;
  if (p.indexOf("/bnb/armour/underarmour/underarmour-skins/") !== -1) return true;

  return false;
}

function ensurePlanSystemCSSIfNeeded(pathname) {
  if (!isPlanSystemPath(pathname)) return;

  const href = cssAssetURL("df-bnb-plan-system.css");
  ensureFeatureCSSLoaded("dfbnb-plan-system-css", href);
}

/* =========================
   DF CALCULATORS (3 pages only)
   ========================= */
function isDfCalculatorsPath(pathname) {
  const p = String(pathname || location.pathname || "");
  return (
    /\/df\/calculators\/build-inspiration-generator\/?$/.test(p) ||
    /\/df\/calculators\/outfit-inspiration-generator\/?$/.test(p) ||
    /\/df\/calculators\/the-big-bloom-reward-crafting-calculator\/?$/.test(p) ||
    /\/df\/calculators\/season-ticket\/?$/.test(p)
  );
}

function ensureDfCalculatorsCSSIfNeeded(pathname) {
  if (!isDfCalculatorsPath(pathname)) return;

  const href = cssAssetURL("df-bnb-df-calculator.css");
  ensureFeatureCSSLoaded("dfbnb-df-calculator-css", href);
}

async function maybeMountDfCalculators(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isDfCalculatorsPath(p)) return;

  // Ensure mount point exists
  const body = document.getElementById("dfbnbGuideBody");
  if (body && !document.getElementById("dfbnbDfCalc")) {
    const host = document.createElement("div");
    host.id = "dfbnbDfCalc";
    body.appendChild(host);
  }

  // Already loaded?
  if (window.__DFBNB_DF_CALC_API && typeof window.__DFBNB_DF_CALC_API.mount === "function") {
    await window.__DFBNB_DF_CALC_API.mount(p);
    return;
  }

  const alreadyTag =
    document.getElementById("dfbnb-df-calculator-js") ||
    document.querySelector('script[src*="df-bnb-df-calculator.js"]');

  if (alreadyTag) {
    if (window.__DFBNB_DF_CALC_API && typeof window.__DFBNB_DF_CALC_API.mount === "function") {
      await window.__DFBNB_DF_CALC_API.mount(p);
    }
    return;
  }

  // Load module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-df-calculator.js");

  if (window.__DFBNB_DF_CALC_API && typeof window.__DFBNB_DF_CALC_API.mount === "function") {
    await window.__DFBNB_DF_CALC_API.mount(p);
  } else {
    console.warn("[DF_CALC] Loaded df-bnb-df-calculator.js but __DFBNB_DF_CALC_API.mount is missing.");
  }
}

/* =========================
   EVENTS REWARDS (Events/Activities)
   ========================= */
function isEventsRewardsPath(pathname) {
  const p = String(pathname || location.pathname || "");

  // Master rewards pages under existing DF hubs
  // - /df/activities/rewards/
  // - /df/public-events/rewards/
  // - /df/seasonal-events/rewards/
  if (/^\/df\/(activities|public-events|seasonal-events)\/rewards\/?$/.test(p)) return true;

  // Any /df/.../-reward-checklist/ path should mount the events rewards module
  // Covers: activities, public-events, seasonal-events, daily-ops, expos, raids
  if (/-reward-checklist\/?$/.test(p) && /^\/df\//.test(p)) return true;

  return false;
}

function ensureEventsRewardsCSSIfNeeded(pathname) {
  if (!isEventsRewardsPath(pathname)) return;

  const href = cssAssetURL("df-bnb-events-rewards.css");
  ensureFeatureCSSLoaded("dfbnb-events-rewards-css", href);
}

/* =========================
   CURVE TABLES
   ========================= */
function isCurveTablesPath(pathname) {
  const p = String(pathname || location.pathname || "");

  // Match BOTH:
  // - /.../curve-tables/
  // - /.../curve-tables/<category>/
  // - /.../calculators-curve-tables/
  // - /.../calculators-curve-tables/<category>/
  return /\/curve-tables(\/|$)/.test(p) || /\/calculators-curve-tables(\/|$)/.test(p);
}

function ensureCurvesCSSIfNeeded(pathname) {
  if (!isCurveTablesPath(pathname)) return;

  const href = cssAssetURL("df-bnb-curves.css");
  ensureFeatureCSSLoaded("dfbnb-curves-css", href);
}

async function maybeMountCurves(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isCurveTablesPath(p)) return;

  const body = document.getElementById("dfbnbGuideBody");
  if (body && !document.getElementById("dfbnbCurves")) {
    const host = document.createElement("div");
    host.id = "dfbnbCurves";
    body.appendChild(host);
  }

  // Already loaded?
  if (window.__DFBNB_CURVES_API && typeof window.__DFBNB_CURVES_API.mount === "function") {
    await window.__DFBNB_CURVES_API.mount(p);
    return;
  }

  const alreadyTag =
    document.getElementById("dfbnb-curves-js") ||
    document.querySelector('script[src*="df-bnb-curves.js"]');

  if (alreadyTag) {
    if (window.__DFBNB_CURVES_API && typeof window.__DFBNB_CURVES_API.mount === "function") {
      await window.__DFBNB_CURVES_API.mount(p);
    }
    return;
  }

  // Load module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-curves.js");

  if (window.__DFBNB_CURVES_API && typeof window.__DFBNB_CURVES_API.mount === "function") {
    await window.__DFBNB_CURVES_API.mount(p);
  } else {
    console.warn("[CURVES] Loaded df-bnb-curves.js but __DFBNB_CURVES_API.mount is missing.");
  }
}

async function maybeMountEventsRewards(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isEventsRewardsPath(p)) return;

  // Ensure the app has a mount point (guide shell does not inject WP HTML on app routes)
  const body = document.getElementById("dfbnbGuideBody");
  if (body && !document.getElementById("dfbnbevents")) {
    const host = document.createElement("div");
    host.id = "dfbnbevents";
    body.appendChild(host);
  }

  // If already loaded (WP-enqueued or dynamically injected), mount again (SPA safety)
  const alreadyLoaded =
    (window.__DFBNB_EVENTS_REWARDS_API && typeof window.__DFBNB_EVENTS_REWARDS_API.mount === "function") ||
    document.getElementById("dfbnb-events-rewards-js") ||
    document.querySelector('script[src*="df-bnb-events-rewards.js"]');

  if (alreadyLoaded) {
    if (window.__DFBNB_EVENTS_REWARDS_API && typeof window.__DFBNB_EVENTS_REWARDS_API.mount === "function") {
      await window.__DFBNB_EVENTS_REWARDS_API.mount(p);
    } else {
      console.warn("[EVENTS] Events script appears loaded but __DFBNB_EVENTS_REWARDS_API.mount is missing.");
    }
    return;
  }

  // Load module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-events-rewards.js");

  if (window.__DFBNB_EVENTS_REWARDS_API && typeof window.__DFBNB_EVENTS_REWARDS_API.mount === "function") {
    await window.__DFBNB_EVENTS_REWARDS_API.mount(p);
  } else {
    console.warn("[EVENTS] Loaded df-bnb-events-rewards.js but __DFBNB_EVENTS_REWARDS_API.mount is missing.");
  }
}

  async function maybeMountPlanSystem(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isPlanSystemPath(p)) return;

  // If already loaded (WP-enqueued or injected), mount again (SPA safety)
  if (window.__DFBNB_PLAN_SYSTEM_API && typeof window.__DFBNB_PLAN_SYSTEM_API.mount === "function") {
    await window.__DFBNB_PLAN_SYSTEM_API.mount(p);
    return;
  }

  const injectedTag = document.getElementById("dfbnb-plan-system-js");
  const wpTag = document.querySelector('script[src*="df-bnb-plan-system.js"]');

  // If WP already enqueued the file, wait briefly for the global to appear, then mount.
  if (wpTag && !injectedTag) {
    const start = Date.now();
    while (Date.now() - start < 3000) {
      if (window.__DFBNB_PLAN_SYSTEM_API && typeof window.__DFBNB_PLAN_SYSTEM_API.mount === "function") {
        await window.__DFBNB_PLAN_SYSTEM_API.mount(p);
        return;
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    console.warn("[PLAN] WP script exists but __DFBNB_PLAN_SYSTEM_API.mount did not appear in time; falling back to inject.");
    // Fall through to injector below.
  }

  // If we injected it earlier in this session, wait for global, then mount.
  if (injectedTag) {
    if (window.__DFBNB_PLAN_SYSTEM_API && typeof window.__DFBNB_PLAN_SYSTEM_API.mount === "function") {
      await window.__DFBNB_PLAN_SYSTEM_API.mount(p);
    } else {
      console.warn("[PLAN] Injected script tag exists but __DFBNB_PLAN_SYSTEM_API.mount is missing.");
    }
    return;
  }

  // Load module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-plan-system.js");

  if (window.__DFBNB_PLAN_SYSTEM_API && typeof window.__DFBNB_PLAN_SYSTEM_API.mount === "function") {
    await window.__DFBNB_PLAN_SYSTEM_API.mount(p);
  } else {
    console.warn("[PLAN] Loaded df-bnb-plan-system.js but __DFBNB_PLAN_SYSTEM_API.mount is missing.");
  }
}

/* =========================
   ARMOUR MODS (BNB armour mod checklists)
   - /bnb/armour/power-armour/power-armour-mods/
   - /bnb/armour/body-armour/body-armour-mods/
   - /bnb/armour/underarmour/underarmour-mods/
   ========================= */
function isArmourModsPath(pathname) {
  const p = String(pathname || location.pathname || "");
  return (
    p.indexOf("/bnb/armour/power-armour/power-armour-mods") !== -1 ||
    p.indexOf("/bnb/armour/body-armour/body-armour-mods") !== -1 ||
    p.indexOf("/bnb/armour/underarmour/underarmour-mods") !== -1
  );
}

function ensureArmourModsCSSIfNeeded(pathname) {
  if (!isArmourModsPath(pathname)) return;
  const href = cssAssetURL("df-bnb-armour.css");
  ensureFeatureCSSLoaded("dfbnb-armour-css", href);
}

async function maybeMountArmourMods(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isArmourModsPath(p)) return;

  const body = document.getElementById("dfbnbGuideBody");
  if (body && !document.getElementById("dfbnbArmour")) {
    const host = document.createElement("div");
    host.id = "dfbnbArmour";
    body.appendChild(host);
  }

  if (window.__DFBNB_ARMOUR_API && typeof window.__DFBNB_ARMOUR_API.mount === "function") {
    await window.__DFBNB_ARMOUR_API.mount(p);
    return;
  }

  const alreadyTag =
    document.getElementById("dfbnb-armour-js") ||
    document.querySelector('script[src*="df-bnb-armour.js"]');

  if (alreadyTag) {
    if (window.__DFBNB_ARMOUR_API && typeof window.__DFBNB_ARMOUR_API.mount === "function") {
      await window.__DFBNB_ARMOUR_API.mount(p);
    }
    return;
  }

  const s = document.querySelector('script[src*="df-bnb-guide.js"]');
  const src = s ? String(s.src || "") : "";
  if (!src) return;

  const qIndex = src.indexOf("?");
  const base = (qIndex >= 0) ? src.slice(0, qIndex) : src;
  const query = (qIndex >= 0) ? src.slice(qIndex) : "";
  const lastSlash = base.lastIndexOf("/");
  if (lastSlash < 0) return;

  const baseDir = base.slice(0, lastSlash + 1);
  const modSrc = baseDir + "assets/df-bnb-armour.js" + query;

  await new Promise((resolve, reject) => {
    const tag = document.createElement("script");
    tag.id = "dfbnb-armour-js";
    tag.src = modSrc;
    tag.async = true;
    tag.onload = resolve;
    tag.onerror = reject;
    document.head.appendChild(tag);
  });

  if (window.__DFBNB_ARMOUR_API && typeof window.__DFBNB_ARMOUR_API.mount === "function") {
    await window.__DFBNB_ARMOUR_API.mount(p);
  } else {
    console.warn("[ARMOUR] Loaded df-bnb-armour.js but __DFBNB_ARMOUR_API.mount is missing.");
  }
}

// =========================
// COLLECTABLES CHECKLIST (Bobbleheads, Plushies, Notes, etc.)
// =========================
function isCollectablesChecklistPath(pathname) {
  const p = String(pathname || location.pathname || "");
  return /\/collectables\/(?:bobbleheads|plushies|notes|holotape-games|magazines|holotapes)\/checklist\/?$/.test(p);
}

function ensureCollectablesCSSIfNeeded(pathname) {
  if (!isCollectablesChecklistPath(pathname)) return;
  const href = cssAssetURL("df-bnb-collectables.css");
  ensureFeatureCSSLoaded("dfbnb-collectables-css", href);
}

async function maybeMountCollectables(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isCollectablesChecklistPath(p)) return;

  if (document.getElementById("dfbnb-df-bnb-collectables-js")) {
    if (window.__DFBNB_COLLECTABLES_API && typeof window.__DFBNB_COLLECTABLES_API.mount === "function") {
      await window.__DFBNB_COLLECTABLES_API.mount(p);
    }
    return;
  }

  await loadAssetModule("df-bnb-collectables.js");

  if (window.__DFBNB_COLLECTABLES_API && typeof window.__DFBNB_COLLECTABLES_API.mount === "function") {
    await window.__DFBNB_COLLECTABLES_API.mount(p);
  } else {
    console.warn("[COLLECTABLES] Loaded df-bnb-collectables.js but __DFBNB_COLLECTABLES_API.mount is missing.");
  }
}

// =========================
// CAMP TITLES: MANIFEST -> dfbnbData URL hydration
// =========================
let __cmptManifestLoaded = false;

function hasCampTitlesURLs() {
  const d = window.dfbnbData || {};
  return !!(safeText(d.cmpt_tsv_url) && safeText(d.cobj_tsv_url) && safeText(d.lvli_tsv_url) && safeText(d.glob_tsv_url) && safeText(d.gmrw_tsv_url));
}

async function ensureCampTitlesDataURLs() {
  if (__cmptManifestLoaded) return;
  if (hasCampTitlesURLs()) { __cmptManifestLoaded = true; return; }

  const d = (window.dfbnbData = window.dfbnbData || {});
  const manifestURL = safeText(d.cmpt_manifest_url);

  // If you haven’t provided a manifest URL in dfbnbData, do nothing.
  if (!manifestURL) { __cmptManifestLoaded = true; return; }

  try {
const j = await fetchJSON(manifestURL);
const tsv = (j && j.tsv) ? j.tsv : null;

function withVer(url, ver) {
  const u = safeText(url);
  const v = safeText(ver);
  if (!u || !v) return u;
  return u + (u.indexOf("?") >= 0 ? "&" : "?") + "v=" + encodeURIComponent(v);
}

if (tsv && typeof tsv === "object") {
  const ver = safeText(j && j.generatedAt);

  // Keep the manifest version available (useful for “first seen” release year logic)
  d.cmpt_generated_at = safeText(d.cmpt_generated_at) || ver;

  // Required TSVs for Camp Titles checklist logic
  d.cmpt_tsv_url = safeText(d.cmpt_tsv_url) || withVer(tsv.cmpt, ver);
  d.cobj_tsv_url = safeText(d.cobj_tsv_url) || withVer(tsv.cobj, ver);
  d.lvli_tsv_url = safeText(d.lvli_tsv_url) || withVer(tsv.lvli, ver);
  d.glob_tsv_url = safeText(d.glob_tsv_url) || withVer(tsv.glob, ver);
  d.gmrw_tsv_url = safeText(d.gmrw_tsv_url) || withVer(tsv.gmrw, ver);

  // Optional TSVs (used for Tradeable/Unsellable and Season name resolution)
  // Only hydrate them if present in the manifest so nothing hard-fails.
  const bookURL = tsv.book || tsv.BOOK;
  const seasonsURL = tsv.seasons || tsv.SEASONS;

  if (bookURL) {
    d.book_tsv_url = safeText(d.book_tsv_url) || withVer(bookURL, ver);
  }
  if (seasonsURL) {
    d.seasons_tsv_url = safeText(d.seasons_tsv_url) || withVer(seasonsURL, ver);
  }

}

  } catch (e) {
    // Quiet fail: module will show its own “URLs not set” error box.
  } finally {
    __cmptManifestLoaded = true;
  }
}

  // ===== Footer: year + socials + return-to-top =====
const SOCIALS = {
  df: {
    bluesky: "https://bsky.app/profile/duchessflame.bsky.social",
    facebook: "https://www.facebook.com/DuchessFlame",
    instagram: "https://www.instagram.com/theduchessflame/",
    kofi: "https://ko-fi.com/duchessflame",
    reddit: "https://www.reddit.com/user/TheDuchessFlame/",
    tiktok: "https://www.tiktok.com/@duchessflame",
    twitch: "https://www.twitch.tv/theduchessflame",
    x: "https://x.com/DuchessFlame",
    youtube: "https://www.youtube.com/@DuchessFlame"
  },
  bnb: {
    bluesky: "https://bsky.app/profile/buffsnbrewfo76.bsky.social",
    facebook: "https://www.facebook.com/buffsnbrewFB",
    kofi: "https://ko-fi.com/duchessflame",
    x: "https://x.com/buffsnbrewfo76"
  }
};

const SOCIAL_LABEL = {
  bluesky: "Bluesky",
  facebook: "Facebook",
  instagram: "Instagram",
  kofi: "Ko-fi",
  reddit: "Reddit",
  tiktok: "TikTok",
  twitch: "Twitch",
  x: "X",
  youtube: "YouTube"
};

const ICONS = {
  bluesky: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 11.2c-1.4-2-5.2-6.2-8-7.9C1.6 2 1 2.2 1 4c0 1.4.8 12 1.3 13.7.2.6.8 1 1.4 1.1 2 .3 6.7.8 8.3-1.8.1-.1.2-.3.3-.4.1.1.2.3.3.4 1.6 2.6 6.3 2.1 8.3 1.8.6-.1 1.2-.5 1.4-1.1C22.2 16 23 5.4 23 4c0-1.8-.6-2-3-0.7-2.8 1.7-6.6 5.9-8 7.9z"/></svg>',
  facebook: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M13.5 22v-8h2.7l.4-3h-3.1V9.1c0-.9.3-1.5 1.6-1.5H16.7V5c-.3 0-1.5-.2-2.8-.2-2.8 0-4.7 1.7-4.7 4.8V11H6.5v3h2.7v8h4.3z"/></svg>',
  instagram: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 2h10a5 5 0 0 1 5 5v10a5 5 0 0 1-5 5H7a5 5 0 0 1-5-5V7a5 5 0 0 1 5-5zm5 6.2A3.8 3.8 0 1 0 15.8 12 3.8 3.8 0 0 0 12 8.2zm6.1-.9a.9.9 0 1 0 .9.9.9.9 0 0 0-.9-.9zM12 10a2 2 0 1 1-2 2 2 2 0 0 1 2-2z"/></svg>',
  kofi: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 4h12a4 4 0 0 1 0 8h-2v2a6 6 0 0 1-12 0V4zm10 6h2a2 2 0 0 0 0-4h-2v4z"/></svg>',
  reddit: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 12.2c0-.8-.7-1.6-1.7-2.1.1-.3.2-.6.2-1 0-1.1-.9-2-2-2-.5 0-1 .2-1.4.6-1.4-.5-3-.8-4.7-.9l.8-3.7 2.6.6a1.5 1.5 0 1 0 .3-1.4L10 1.5 8.9 6.3c-1.7.1-3.3.4-4.7.9A2 2 0 0 0 2.8 6.6c-1.1 0-2 .9-2 2 0 .4.1.7.2 1C0 10.6 0 11.4 0 12.2c0 3.1 3.6 5.6 8 5.6s8-2.5 8-5.6z"/></svg>',
  tiktok: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M16 2c.4 3 2.4 4.8 5 5v3.2c-1.9 0-3.6-.6-5-1.6V16a6 6 0 1 1-6-6c.3 0 .7 0 1 .1V13a3 3 0 1 0 2 2.8V2h3z"/></svg>',
  twitch: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 3h18v11l-4 4h-4l-2 2H9v-2H4V3zm3 3v10h3v2l2-2h5l3-3V6H7zm8 2h2v6h-2V8zm-4 0h2v6h-2V8z"/></svg>',
  x: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M18.9 2H22l-7.3 8.3L23.3 22h-6.8l-5.3-6.6L5.4 22H2.2l7.9-9L0.9 2h6.9l4.8 6.1L18.9 2z"/></svg>',
  youtube: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21.8 8.1a3 3 0 0 0-2.1-2.1C17.9 5.5 12 5.5 12 5.5s-5.9 0-7.7.5A3 3 0 0 0 2.2 8.1 31.6 31.6 0 0 0 2 12a31.6 31.6 0 0 0 .2 3.9 3 3 0 0 0 2.1 2.1c1.8.5 7.7.5 7.7.5s5.9 0 7.7-.5a3 3 0 0 0 2.1-2.1c.2-1.3.2-2.6.2-3.9s0-2.6-.2-3.9zM10 15.5v-7l6 3.5-6 3.5z"/></svg>'
};

function setFooterYear() {
  const y = document.getElementById("copyrightYear");
  if (y) y.textContent = String(new Date().getFullYear());
}

function renderFooterSocials(active) {
  const brand = (active === "df") ? "df" : "bnb";
  const wrap = document.getElementById("footerSocials");
  if (!wrap) return;

  const links = SOCIALS[brand] || SOCIALS.bnb;
  wrap.innerHTML = "";

  Object.keys(links).forEach((key) => {
    const href = links[key];
    if (!href) return;

    const a = document.createElement("a");
    a.className = "social-btn";
    a.href = href;
    a.target = "_blank";
    a.rel = "noopener";
    a.setAttribute("aria-label", SOCIAL_LABEL[key] || key);
    a.innerHTML = ICONS[key] || "";

    wrap.appendChild(a);
  });
}

function wireReturnToTopButton() {
  const btn = document.getElementById("dfbnbReturnTop");
  if (!btn) return;
  btn.addEventListener("click", () => {
    try { window.scrollTo({ top: 0, behavior: "smooth" }); }
    catch { window.scrollTo(0, 0); }
  });
}

function formatDateDDMMMYYYY(iso) {
  const s = safeText(iso);
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (!m) return "";
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  if (!y || mo < 1 || mo > 12 || d < 1 || d > 31) return "";
  return String(d).padStart(2, "0") + " " + months[mo - 1] + " " + y;
}

async function maybeMountChallenges(pathname) {
  // Fast path check (no module load unless it’s a checklist page)
  const p = String(pathname || location.pathname || "");
  const isChecklist =
    p.indexOf("/challenges/") !== -1 ||
    (p.indexOf("/scouts/") !== -1 && /\/checklist\/?$/.test(p)) ||
    (p.indexOf("/seasons/") !== -1 && /challenge-checklist\/?$/.test(p));

  if (!isChecklist) return;

  // Already loaded?
  if (window.__DFBNB_CHALLENGES_API && typeof window.__DFBNB_CHALLENGES_API.mount === "function") {
    await window.__DFBNB_CHALLENGES_API.mount(pathname);
    return;
  }

  // Load the module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-challenges.js");

  if (window.__DFBNB_CHALLENGES_API && typeof window.__DFBNB_CHALLENGES_API.mount === "function") {
    await window.__DFBNB_CHALLENGES_API.mount(pathname);
  }
}

async function maybeMountCampTitles(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isCampTitlesPath(p)) return;

// If already loaded, mount it again (needed for SPA virtual renders)
if (document.getElementById("dfbnb-titles-js")) {
  if (window.__DFBNB_TITLES_API && typeof window.__DFBNB_TITLES_API.mount === "function") {
    await window.__DFBNB_TITLES_API.mount(pathname);
  } else {
    console.warn("[TITLES] Script tag exists but __DFBNB_TITLES_API.mount is missing");
  }
  return;
}

  // Load the module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-titles.js");

  // Now mount Titles (this actually runs init -> fetch -> render)
  if (window.__DFBNB_TITLES_API && typeof window.__DFBNB_TITLES_API.mount === "function") {
    await window.__DFBNB_TITLES_API.mount(pathname);
  } else {
    console.warn("[TITLES] Loaded df-bnb-titles.js but __DFBNB_TITLES_API.mount is missing.");
  }
}

async function maybeMountTitlesGenerator(pathname) {
  const p = String(pathname || location.pathname || "");
  if (!isTitlesGeneratorPath(p)) return;

  // If script already injected, just mount again (SPA safety)
  if (document.getElementById("dfbnb-titles-generator-js")) {
    if (window.__DFBNB_TITLES_GENERATOR_API && typeof window.__DFBNB_TITLES_GENERATOR_API.mount === "function") {
      await window.__DFBNB_TITLES_GENERATOR_API.mount(p);
    } else {
      console.warn("[GENERATOR] Script tag exists but __DFBNB_TITLES_GENERATOR_API.mount is missing.");
    }
    return;
  }

  // Load module from /assets/ next to df-bnb-guide.js (keeps ?ver= intact)
  await loadAssetModule("df-bnb-titles-generator.js");

  if (window.__DFBNB_TITLES_GENERATOR_API && typeof window.__DFBNB_TITLES_GENERATOR_API.mount === "function") {
    await window.__DFBNB_TITLES_GENERATOR_API.mount(p);
  } else {
    console.warn("[GENERATOR] Loaded df-bnb-titles-generator.js but __DFBNB_TITLES_GENERATOR_API.mount is missing.");
  }
}

// Public SPA API: lets Category shell mount a guide view without full reload.
window.__DFBNB_GUIDE_API = window.__DFBNB_GUIDE_API || {};
window.__DFBNB_GUIDE_API.renderVirtual = async function renderVirtualGuide(pathname, navByBrand, tsvParsed) {
  // Render skeleton immediately
renderAppSkeleton();
ensureChallengesCSSIfNeeded(pathname);
ensureCampTitlesCSSIfNeeded(pathname);
ensurePlanSystemCSSIfNeeded(pathname);
ensureEventsRewardsCSSIfNeeded(pathname);
ensureCurvesCSSIfNeeded(pathname);
ensureDfCalculatorsCSSIfNeeded(pathname);
ensureArmourModsCSSIfNeeded(pathname);
ensureCollectablesCSSIfNeeded(pathname);

  // Apply brand theme ASAP
  const brand = resolveBrandFromPath(pathname) || getSavedBrand() || "bnb";
  setBrandMix(brand);

  // Use provided data if available, otherwise fetch (safe fallback)
  const nav = navByBrand || await fetchJSON(NAV_URL);
  const parsed = tsvParsed || parseTSV(await fetchText(GUIDE_INDEX_URL));

  const rows = (parsed && parsed.rows) ? parsed.rows : [];
  const model = buildGuideModel(rows, brand);

  wireDropdowns(model, brand);

  const current = resolveGuideFromURL(model, pathname);
  if (!current) {
    setGuideHeader({ title: "Guide not found", updated: "", views: 0, likes: 0, author: "", thanks: "" });
    clearGuideBody();
        updatePatchLogForPath(pathname || location.pathname);
    return;
  }

  setDropdownStateToGuide(model, current);

// Apply per-guide header overrides (Thanks / Image Credit / Credit)
applyGuideMetaOverrides(current, current.url || location.pathname);

  setGuideHeader(current);
  wirePinButtonForGuide(current.url);
  wireShareButtonForGuide(current.title, current.url);
clearGuideBody();

const didInject = await injectGuideBodyFromHTML(pathname);
if (!didInject) {
  await maybeMountChallenges(pathname);
  await maybeMountCampTitles(pathname);
  await maybeMountTitlesGenerator(pathname);
  await maybeMountPlanSystem(pathname);
  await maybeMountEventsRewards(pathname);
  await maybeMountCurves(pathname);
  await maybeMountDfCalculators(pathname);
  await maybeMountArmourMods(pathname);
  await maybeMountCollectables(pathname);
}

  updatePatchLogForPath(current.url || pathname);

  // Global view tracking (server-first). Uses normalized pathname id.
  (function () {
    const id = normalizePath(current && current.url ? current.url : pathname);
    bumpLocalViewFallback(id); // immediate quiet fallback for UI

    const viewsEl = byId("dfbnbGuideViews");
    if (viewsEl) viewsEl.textContent = String(readLocalViewFallback(id) || 0);

    apiPostView(id); // fire-and-forget write

    apiGetViewsBatch([id]).then((map) => {
      if (!map || !map[id]) return;
      const total = Number.isFinite(map[id].total) ? map[id].total : null;
      if (total != null && viewsEl) viewsEl.textContent = String(total);
    });
  })();


  idle(() => prefetchCategoryLikelyGuides(model, current));
};

// Auto-boot only on supported shell pages.
if (IS_GUIDE_PAGE || IS_MEMBER_PINS_PAGE) {
  boot().catch((e) => {
    renderFatal("Guide shell failed to load.", e);
  });
}

async function renderMemberPinsPage(model, brand) {
  // Use the Guide Shell skeleton UI so Search + Patch Log exist
  // Hide the dropdown controls since this page is “Pinned Posts”
  const controls = document.querySelector(".dfbnb-controls");
  if (controls) controls.style.display = "none";

  const search = byId("dfbnbGuideSearch");
  if (search) {
    search.placeholder = "Search pinned posts…";
    wireGuideSearch(search);
  }

  // Header
  setGuideHeader({
    title: "Pinned Posts",
    updated: "",
    views: 0,
    likes: 0,
    author: "",
    thanks: ""
  });

  clearGuideBody();
  const body = byId("dfbnbGuideBody");
  if (!body) return;

  // Server pins only (members-only page)
  let pins = { guides: [], categories: [] };
  try {
    pins = await apiGetPins();
  } catch (e) {
    // If pins endpoint fails, render a clear message
    body.innerHTML = `
      <div class="dfbnb-detailBox">
        <div class="dfbnb-detailTitle">Pinned Posts</div>
        <div class="dfbnb-detailText">Could not load your pins. Please refresh and try again.</div>
      </div>
    `;
    return;
  }

  const pinnedUrls = new Set((pins.guides || []).map(u => normalizePath(String(u || ""))));
  const pinnedCats = (pins.categories || []).map(s => String(s || "").trim()).filter(Boolean);

  // Build list from:
  // - explicitly pinned guide URLs
  // - all guides inside pinned categories
  const out = [];
  const seen = new Set();

  // 1) URL pins
  for (const u of pinnedUrls) {
    const g = model.pages.find(x => x && x.url === u);
    if (g && !seen.has(g.url)) {
      out.push(g);
      seen.add(g.url);
    }
  }

  // 2) Category pins
  for (const cat of pinnedCats) {
    const list = model.byCat.get(cat) || [];
    for (const g of list) {
      if (g && g.url && !seen.has(g.url)) {
        out.push(g);
        seen.add(g.url);
      }
    }
  }

  // Render
  if (!out.length) {
    body.innerHTML = `
      <div class="dfbnb-detailBox">
        <div class="dfbnb-detailTitle">Pinned Posts</div>
        <div class="dfbnb-detailText">No pinned guides yet. Open a guide and use the Pin button.</div>
      </div>
    `;
    return;
  }

  // Sort alphabetically (stable)
  out.sort((a, b) => String(a.title || "").localeCompare(String(b.title || ""), undefined, { sensitivity: "base", numeric: true }));

  body.innerHTML = `
    <section class="dfbnb-detailBox">
      <div class="dfbnb-detailTitle">Pinned Posts</div>
      <div class="dfbnb-detailText">Click a pinned guide to open it.</div>
      <div class="dfbnb-pinsList">
        ${out.map(g => `
          <a class="dfbnb-pinItem" href="${escapeHtml(g.url)}">
            <div class="dfbnb-pinTitle">${escapeHtml(g.title)}</div>
            ${g.updated ? `<div class="dfbnb-pinMeta">Updated: ${escapeHtml(g.updated)}</div>` : ``}
          </a>
        `).join("")}
      </div>
    </section>
  `;
}

  async function boot() {
    // Render skeleton immediately (fast first paint)
renderAppSkeleton();
ensureChallengesCSSIfNeeded(location.pathname);
ensureCampTitlesCSSIfNeeded(location.pathname);
ensurePlanSystemCSSIfNeeded(location.pathname);
ensureEventsRewardsCSSIfNeeded(location.pathname);
ensureCurvesCSSIfNeeded(location.pathname);
ensureDfCalculatorsCSSIfNeeded(location.pathname);
ensureArmourModsCSSIfNeeded(location.pathname);
ensureCollectablesCSSIfNeeded(location.pathname);

    // Apply brand theme ASAP (so colors are correct before content arrives)
    const brand = (window.dfbnbData && safeText(window.dfbnbData.brand)) || resolveBrandFromPath(location.pathname) || getSavedBrand() || "bnb";
    setBrandMix(brand);

    // Fetch in parallel
    const [navByBrand, tsvText] = await Promise.all([
      fetchJSON(NAV_URL),
      fetchText(GUIDE_INDEX_URL)
    ]);

    // Parse TSV and build page list
    const parsed = parseTSV(tsvText);
  const rows = (parsed && parsed.rows) ? parsed.rows : [];
  const model = buildGuideModel(rows, brand);

  // Member pins page: render pinned lists instead of the normal guide router
  if (IS_MEMBER_PINS_PAGE) {
    await renderMemberPinsPage(model, brand);
    updatePatchLogForPath(pathname || location.pathname);
    return;
  }

  wireDropdowns(model, brand);

    // Resolve current guide from URL and render header state
    const path = window.DFBNB_VIRTUAL_PATH || location.pathname;
const current = resolveGuideFromURL(model, path);

    if (!current) {
      setGuideHeader({
        title: "Guide not found",
        updated: "",
        views: 0,
        likes: 0,
        author: "",
        thanks: ""
      });
      // Leave body blank as requested
      return;
    }

    // Set dropdown defaults to match current guide
    setDropdownStateToGuide(model, current);

    // Render header for that guide
    applyGuideMetaOverrides(current, current.url || location.pathname);

    setGuideHeader(current);
    wirePinButtonForGuide(current.url);
        wireShareButtonForGuide(current.title, current.url);

// Clear body, then either inject WP HTML (normal guides) or mount SPA features (apps)
clearGuideBody();

const didInject = await injectGuideBodyFromHTML(location.pathname);
if (!didInject) {
  await maybeMountChallenges(location.pathname);
  await maybeMountCampTitles(location.pathname);
  await maybeMountTitlesGenerator(location.pathname);
  await maybeMountPlanSystem(location.pathname);
  await maybeMountEventsRewards(location.pathname);
  await maybeMountCurves(location.pathname);
  await maybeMountDfCalculators(location.pathname);
  await maybeMountArmourMods(location.pathname);
  await maybeMountCollectables(location.pathname);
}

    updatePatchLogForPath(current.url || location.pathname);

    // Prefetch likely next guides in this category in idle time
    idle(() => prefetchCategoryLikelyGuides(model, current));

    // Support back/forward without full reload
    // Guard prevents stacking if boot() is ever called more than once.
if (!window.__dfbnbPopstateBound) {
  window.__dfbnbPopstateBound = true;
  window.addEventListener("popstate", () => {
    const g = resolveGuideFromURL(model, location.pathname);
    if (!g) return;
    setDropdownStateToGuide(model, g);
    navigateToGuide(g).catch(() => {});
  });
}

  }

  /* =========================================================
     RENDER: skeleton and fatal
     ========================================================= */

  function renderAppSkeleton() {
    const app = document.createElement("div");
    app.className = "dfbnb-guide-app";
    app.innerHTML = `
      <div id="dfbnbCatStickyBar" class="dfbnb-cat-stickybar">
        <div class="dfbnb-cat-left"></div>
<div class="dfbnb-cat-right" id="dfbnbGuideAuthSlot"></div>
      </div>

      <main class="dfbnb-wrap">
        <div id="dfbnbCatHeaderBanner" class="dfbnb-cat-banner" aria-label="Logo banner">
          <a id="dfbnbCatHomeLink" href="/" class="dfbnb-cat-home" data-hard-nav="1" aria-label="Home">
            <img id="dfbnbCatBrandLogo" alt="" />
          </a>
        </div>

        <section class="dfbnb-guideHeader" aria-label="Guide header">
<div class="dfbnb-guideTitleRow">
  <h1 id="dfbnbGuideTitle">
    <span class="dfbnb-skel dfbnb-skel-line lg" style="display:block; width:78%;"></span>
  </h1>

  <div class="dfbnb-guideActions" aria-label="Guide actions">
    <button id="dfbnbPinBtn"
            class="dfbnb-pin-btn"
            type="button"
            aria-pressed="false"
            aria-label="Pin guide"
            title="Pin guide">
      <span class="dfbnb-pin-ico" aria-hidden="true">
        <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
          <path d="M14 3l7 7-3 3v4H6v-4L3 10l7-7h4Z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/>
          <path d="M12 17v5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
        </svg>
      </span>
    </button>

    <button id="dfbnbShareBtn"
            class="dfbnb-share-btn"
            type="button"
            aria-label="Share this guide"
            title="Share">
      <span class="dfbnb-share-ico" aria-hidden="true">
        ${svgShare()}
      </span>
    </button>
  </div>
</div>

          <div class="dfbnb-metaPills" aria-label="Meta">

            <div class="dfbnb-pill">
              <span>Updated</span>
              <strong id="dfbnbGuideUpdated"></strong>
            </div>

            <div class="dfbnb-pill" title="Views">
              <span class="dfbnb-ico" aria-hidden="true">
                ${svgEye()}
              </span>
              <strong id="dfbnbGuideViews">0</strong>
            </div>

            <div class="dfbnb-pill" title="Likes">
              <span class="dfbnb-ico" aria-hidden="true">
                ${svgHeart()}
              </span>
              <strong id="dfbnbGuideLikes">0</strong>
            </div>
          </div>

          <div class="dfbnb-byline" aria-label="Written by and credits">
            <div class="dfbnb-kv"><strong>Written by:</strong> <span id="dfbnbGuideAuthor"></span></div>

            <div class="dfbnb-kv" id="dfbnbGuideThanksWrap" style="display:none;">
              <strong>With thanks to:</strong> <span id="dfbnbGuideThanks"></span>
            </div>

            <div class="dfbnb-kv" id="dfbnbGuideImageCreditWrap" style="display:none;">
              <strong>Image credit:</strong> <span id="dfbnbGuideImageCredit"></span>
            </div>

            <div class="dfbnb-kv" id="dfbnbGuideCreditToWrap" style="display:none;">
              <strong>Credit to:</strong> <span id="dfbnbGuideCreditTo"></span>
            </div>
          </div>

          <div class="dfbnb-controls" aria-label="Guide controls">
            <div class="dfbnb-control">
              <label for="dfbnbGuideCategorySelect">Category</label>
              <select id="dfbnbGuideCategorySelect"></select>
            </div>
            <div class="dfbnb-control">
              <label for="dfbnbGuideSelect">Guide</label>
              <select id="dfbnbGuideSelect"></select>
            </div>
          </div>

          <div class="dfbnb-control">
            <label for="dfbnbGuideSearch">Search this guide</label>
            <input id="dfbnbGuideSearch" type="search" placeholder="Search this guide…" autocomplete="off" />
          </div>
        </section>

  <section class="dfbnb-guideBody" id="dfbnbGuideBody">
  <div class="dfbnb-skel dfbnb-skel-line" style="width:62%;"></div>
  <div class="dfbnb-skel-gap"></div>
  <div class="dfbnb-skel dfbnb-skel-line" style="width:90%;"></div>
  <div class="dfbnb-skel-gap"></div>
  <div class="dfbnb-skel dfbnb-skel-line" style="width:84%;"></div>
  <div class="dfbnb-skel-gap"></div>
  <div class="dfbnb-skel dfbnb-skel-line" style="width:70%;"></div>
  <div class="dfbnb-skel-gap"></div>
  <div class="dfbnb-skel dfbnb-skel-line" style="width:92%;"></div>
  <div class="dfbnb-skel-gap"></div>
  <div class="dfbnb-skel dfbnb-skel-line" style="width:58%;"></div>
</section>


<section class="dfbnbPatchLogShell" id="dfbnbPatchLogShell" aria-label="Patch Log" style="display:none;"></section>

<div class="returnTopBar">
  <button id="dfbnbReturnTop"
          class="returnTopBtn"
          aria-label="Return to top"
          type="button">
    Return to Top
  </button>
</div>

<footer class="footer" role="contentinfo">
  <div class="copy">© 2021–<span id="copyrightYear"></span></div>
  <div class="footerSocials" id="footerSocials" aria-label="Social links"></div>
  <div class="copy right">All content subject to change</div>
</footer>

      </main>
    `;

    shell.innerHTML = "";
    shell.appendChild(app);
    setFooterYear();
    renderFooterSocials(resolveBrandFromPath(location.pathname) || "bnb");
    wireReturnToTopButton();
  }

  function renderFatal(msg, err) {
    try { console.error(err); } catch (e) {}
    shell.innerHTML = `
      <div style="padding:24px; color:#fff; font-family:system-ui;">
        <div style="font-weight:900; font-size:18px; margin-bottom:8px;">${escapeHtml(msg)}</div>
        <div style="opacity:0.75; font-size:14px;">${escapeHtml(err && err.message ? err.message : String(err || ""))}</div>
      </div>
    `;
  }

function clearGuideBody() {
  const body = byId("dfbnbGuideBody");
  if (!body) return;

  // Clear existing UI content
  body.innerHTML = "";

  // If WP editor content exists (full page load), inject it into the body.
  const wp = document.getElementById("dfbnbWpContent");
  if (wp) {
    const clone = wp.cloneNode(true);
    clone.style.display = "";
    body.appendChild(clone);

    // Optional: enable intent-prefetch for any links inside the WP content.
    wirePrefetchForAnchors(body);
  }
}

  /* =========================================================
     BRAND: mixPct support (matches Home behavior)
     ========================================================= */

  function getSavedBrand() {
    try {
      const s = safeLower(localStorage.getItem("dfbnb:activeBrand"));
      if (s === "df" || s === "bnb") return s;
    } catch (e) {}
    return "bnb";
  }

  function resolveBrandFromPath(pathname) {
    const p = String(pathname || "").toLowerCase();
    if (p.startsWith("/df/")) return "df";
    if (p.startsWith("/bnb/")) return "bnb";
    return null;
  }

  function svgPin() {
    return `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M14 3l7 7-3 3v4H6v-4L3 10l7-7h4Z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/>
      <path d="M12 17v5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
    </svg>`;
  }

    function svgShare() {
    return `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M16 8a3 3 0 1 0-2.8-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
      <path d="M6 14a3 3 0 1 0 0 6 3 3 0 0 0 0-6Z" fill="none" stroke="currentColor" stroke-width="2"/>
      <path d="M18 10a3 3 0 1 0 0 6 3 3 0 0 0 0-6Z" fill="none" stroke="currentColor" stroke-width="2"/>
      <path d="M8.6 16.4 15.4 13.6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
      <path d="M8.6 15.6 15.4 12.4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
    </svg>`;
  }

  async function shareUrl(url, title) {
    const href = safeText(url || location.href);
    const text = safeText(title || document.title || "");

    if (navigator.share) {
      try { await navigator.share({ title: text, url: href }); } catch (e) {}
      return;
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      try { await navigator.clipboard.writeText(href); } catch (e) {}
      return;
    }

    try { window.prompt("Copy link:", href); } catch (e) {}
  }

  function wireShareButtonForGuide(guideTitle, guideUrl) {
    const btn = byId("dfbnbShareBtn");
    if (!btn) return;

    btn.onclick = (e) => {
      e.preventDefault();
      e.stopPropagation();
      shareUrl(guideUrl || location.href, guideTitle || document.title || "");
    };
  }

  function svgUser() {
    return `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M12 12a4 4 0 1 0-4-4 4 4 0 0 0 4 4Z" fill="none" stroke="currentColor" stroke-width="2"/>
      <path d="M4 21a8 8 0 0 1 16 0" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
    </svg>`;
  }

  function getMemberUrlsForBrand(brand) {
    const d = window.dfbnbData || {};
    const isDf = (brand === "df");

    const pinned = safeText(isDf ? d.pinned_posts_df : d.pinned_posts_bnb) || (isDf ? "/df/member/pinned-posts/" : "/bnb/member/pinned-posts/");
    const member = safeText(isDf ? d.member_hub_df   : d.member_hub_bnb)   || (isDf ? "/df/member/" : "/bnb/member/");
    const profile = safeText(d.profile_url) || "/wp-admin/profile.php";

    return { pinned, member, profile };
  }

  function renderGuideStickyAuthArea(brand) {
    const slot = byId("dfbnbGuideAuthSlot");
    if (!slot) return;

    slot.innerHTML = "";

    if (!IS_LOGGED_IN) {
      const aLogin = document.createElement("a");
      aLogin.id = "dfbnbAuthLogin";
      aLogin.className = "dfbnb-auth-btn";
      aLogin.textContent = "Log in";
      aLogin.href = "/wp-login.php";

      const aSignup = document.createElement("a");
      aSignup.id = "dfbnbAuthSignup";
      aSignup.className = "dfbnb-auth-btn primary";
      aSignup.textContent = "Sign up";
      aSignup.href = "/wp-login.php?action=register";

      slot.appendChild(aLogin);
      slot.appendChild(aSignup);
      return;
    }

    const name = safeText(window.dfbnbData && window.dfbnbData.user_display_name) || "Member";
    const urls = getMemberUrlsForBrand(brand);

    const aPins = document.createElement("a");
    aPins.className = "dfbnb-auth-btn dfbnb-ico-btn";
    aPins.href = urls.pinned;
    aPins.title = "Pinned posts";
    aPins.setAttribute("aria-label", "Pinned posts");
    aPins.innerHTML = svgPin();

    const welcome = document.createElement("div");
    welcome.className = "dfbnb-userpill";
    welcome.title = "Signed in";
    welcome.innerHTML = `Welcome! <strong>${name.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</strong>`;

    const aAccount = document.createElement("a");
    aAccount.className = "dfbnb-auth-btn dfbnb-ico-btn";
    aAccount.href = urls.profile;
    aAccount.title = "Account";
    aAccount.setAttribute("aria-label", "Account");
    aAccount.innerHTML = svgUser();

    slot.appendChild(aPins);
    slot.appendChild(welcome);
    slot.appendChild(aAccount);
  }

  function memberBaseForBrand(b) {
  return (b === "df") ? "/df/member/" : "/bnb/member/";
}

function pinsUrlForBrand(b) {
  return memberBaseForBrand(b) + "pinned-posts/";
}

  function setBrandMix(brand) {
    const b = (brand === "df") ? "df" : "bnb";

    // In your system, 0% mix means BNB, 100% means DF
    const mix01 = (b === "df") ? 1 : 0;
    shell.style.setProperty("--mix", String(mix01));
    shell.style.setProperty("--mixPct", Math.round(mix01 * 100) + "%");

// Sticky bar auth area (logged-out vs logged-in UI)
// (Render first, then patch hrefs so we’re not editing non-existent nodes.)
renderGuideStickyAuthArea(b);

// Brand-correct auth URLs + return to the exact page after login/signup
const loginBtn = byId("dfbnbAuthLogin");
const signupBtn = byId("dfbnbAuthSignup");

const bNow = (b === "df") ? "df" : "bnb";
const here = encodeURIComponent(window.location.href);

if (loginBtn)  loginBtn.href  = `/wp-login.php?redirect_to=${here}&dfbnb_brand=${bNow}`;
if (signupBtn) signupBtn.href = `/wp-login.php?action=register&redirect_to=${here}&dfbnb_brand=${bNow}`;

    // Update Home link targets (Guide + Banner)

    const homeHref = "/";

// Banner home link (logo click)
const bannerHome = byId("dfbnbCatHomeLink");
if (bannerHome) bannerHome.setAttribute("href", homeHref);

const logo = byId("dfbnbCatBrandLogo");
if (logo) {
  // Prefer localized PHP data if present, otherwise fall back to the same hardcoded URLs as Category.
  const fallback = {
    df: {
      name: "DuchessFlame",
      logo: "https://cdn.streamelements.com/uploads/01kdmejygsyprddb6gkkcxemeq.webp"
    },
    bnb: {
      name: "Buffs n Brew",
      logo: "https://cdn.streamelements.com/uploads/01kdej42bxxkzan8dnw1s7e9xp.webp"
    }
  };

  const dfLogo  = (window.dfbnbData && safeText(window.dfbnbData.df_logo)) ? safeText(window.dfbnbData.df_logo) : fallback.df.logo;
  const bnbLogo = (window.dfbnbData && safeText(window.dfbnbData.bnb_logo)) ? safeText(window.dfbnbData.bnb_logo) : fallback.bnb.logo;

  const src = (b === "df") ? dfLogo : bnbLogo;
  logo.src = src;

  const altName = (b === "df") ? fallback.df.name : fallback.bnb.name;
  logo.alt = `${altName} Logo`;
}
  }

  /* =========================================================
     MODEL: parse TSV and build guide lists
     ========================================================= */

  function buildGuideModel(rows, brand) {
    const b = (brand === "df") ? "df" : "bnb";

    // Filter to this brand and to "page" rows only (same principle as category shell)
    const pages = rows.filter(r => safeLower(r.brand) === b && safeLower(r.nodeType) === "page");

    // Categories based on topCategory column (fall back to topTitle if needed)
    const categories = uniq(pages.map(r => safeText(r.topCategory) || safeText(r.topTitle)).filter(Boolean)).sort(alpha);

    // Group pages by category
    const byCat = new Map();
    for (const r of pages) {
      const cat = safeText(r.topCategory) || safeText(r.topTitle) || "";
      if (!cat) continue;
      if (!byCat.has(cat)) byCat.set(cat, []);
      byCat.get(cat).push(normalizeGuideRow(r));
    }

// Sort guides alphabetically by Item Name, then by Region
for (const [cat, list] of byCat.entries()) {
  list.sort((a, b) => {

    const parse = (title) => {
      const t = String(title || "").trim();
      const parts = t.split(" - ");
      return {
        item: parts[0] || "",
        region: parts[1] || ""
      };
    };

    const A = parse(a.title);
    const B = parse(b.title);

    // Primary: Item name
    const itemCompare = A.item.localeCompare(B.item, undefined, {
      sensitivity: "base",
      numeric: true
    });

    if (itemCompare !== 0) return itemCompare;

    // Secondary: Region
    return A.region.localeCompare(B.region, undefined, {
      sensitivity: "base",
      numeric: true
    });
  });
}

    return { brand: b, categories, byCat, pages };
  }

  function normalizeGuideRow(r) {
    return {
      id: safeText(r.id) || safeText(r.slug) || "",
      title: safeText(r.title) || safeText(r.menuTitle) || "Untitled",
      url: normalizePath(safeText(r.url) || ""),
      updated: safeText(r.updated) || "",    // your TSV may not have this yet
      author: safeText(r.author) || "",      // same
      thanks: safeText(r.thanks) || "",      // same
      views: toInt(r.views),
      likes: toInt(r.likes),
      browseOrder: toInt(r.browseOrder)
    };
  }

  function resolveGuideFromURL(model, pathname) {
    const path = normalizePath(pathname);
    for (const cat of model.categories) {
      const list = model.byCat.get(cat) || [];
      const hit = list.find(g => g.url === path);
      if (hit) return hit;
    }
    return null;
  }

  /* =========================================================
     UI: dropdowns and switching guides without leaving page
     ========================================================= */

  function wireDropdowns(model) {
    const catSel = byId("dfbnbGuideCategorySelect");
    const guideSel = byId("dfbnbGuideSelect");
    const search = byId("dfbnbGuideSearch");

    if (!catSel || !guideSel) return;

    catSel.innerHTML = model.categories.map(c => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join("");

    // Default category: first
    catSel.value = model.categories[0] || "";
    fillGuideSelect(model, catSel.value, "");

 catSel.addEventListener("change", () => {
  const cat = catSel.value;
  fillGuideSelect(model, cat, "");
  const g = getSelectedGuide(model);
  if (!g) return;
  navigateToGuide(g).catch(() => {});
});

guideSel.addEventListener("change", () => {
  const g = getSelectedGuide(model);
  if (!g) return;
  navigateToGuide(g).catch(() => {});
});

   if (search) {
  wireGuideSearch(search);
}
  }

  /* =========================================================
   GUIDE SEARCH (Ctrl+F style highlight)
   - If an active app provides a search API, use it.
   - Otherwise, highlight matches inside #dfbnbGuideBody.
   ========================================================= */

function ensureSearchStyles() {
  if (document.getElementById("dfbnb-search-style")) return;
  const st = document.createElement("style");
  st.id = "dfbnb-search-style";
  st.textContent = `
    mark.dfbnb-mark {
      padding: 0 2px;
      border-radius: 3px;
    }
    mark.dfbnb-mark.is-active {
      outline: 2px solid currentColor;
    }
  `;
  document.head.appendChild(st);
}

function escapeRegExp(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function clearMarks(root) {
  if (!root) return;
  const marks = root.querySelectorAll("mark.dfbnb-mark");
  marks.forEach((m) => {
    const text = document.createTextNode(m.textContent || "");
    m.replaceWith(text);
  });
  // normalize merges adjacent text nodes after unwrap
  root.normalize();
}

function markMatchesInElement(el, re) {
  // Only operate on elements that contain plain text (we will rewrite their children)
  const text = el.textContent || "";
  if (!text) return 0;

  let count = 0;
  const parts = [];
  let last = 0;

  // Robust callback: offset is always the second-to-last argument
  text.replace(re, (...args) => {
    const match = args[0];
    const offset = args[args.length - 2]; // number

    if (!Number.isFinite(offset)) return match;

    parts.push(document.createTextNode(text.slice(last, offset)));

    const mark = document.createElement("mark");
    mark.className = "dfbnb-mark";
    mark.textContent = match;
    parts.push(mark);

    last = offset + match.length;
    count++;
    return match;
  });

  if (!count) return 0;

  parts.push(document.createTextNode(text.slice(last)));

  // Clear children without innerHTML string rewriting
  while (el.firstChild) el.removeChild(el.firstChild);
  for (const p of parts) el.appendChild(p);

  return count;
}

function runGenericBodySearch(query) {
  const body = document.getElementById("dfbnbGuideBody");
  if (!body) return;

  clearMarks(body);

  const q = String(query || "").trim();
  if (!q) return;

  const re = new RegExp(escapeRegExp(q), "gi");

  // Target common readable elements, avoid scripts/styles/etc
  const candidates = body.querySelectorAll(
    "h1,h2,h3,h4,h5,h6,p,li,td,th,blockquote,figcaption,span,a,button,strong,em"
  );

  let total = 0;
  for (const el of candidates) {
    // Skip elements that are likely “UI chrome” with lots of nested nodes
    if (!el || !el.textContent) continue;
    // Avoid re-marking inside existing marks (already cleared, but safe)
    if (el.closest("mark.dfbnb-mark")) continue;

    total += markMatchesInElement(el, re);
  }

  if (!total) return;

  // Scroll to first match and mark it as active
  const first = body.querySelector("mark.dfbnb-mark");
  if (first) {
    first.classList.add("is-active");
    try { first.scrollIntoView({ block: "center", behavior: "smooth" }); }
    catch { first.scrollIntoView(true); }
  }
}

function runAppSearchIfAvailable(query) {
  // Titles checklist app
  if (window.__DFBNB_TITLES_API && typeof window.__DFBNB_TITLES_API.search === "function") {
    window.__DFBNB_TITLES_API.search(query);
    return true;
  }

  // Titles generator app
  if (window.__DFBNB_TITLES_GENERATOR_API && typeof window.__DFBNB_TITLES_GENERATOR_API.search === "function") {
    window.__DFBNB_TITLES_GENERATOR_API.search(query);
    return true;
  }

  // DF calculators app (3 pages only)
  if (window.__DFBNB_DF_CALC_API && typeof window.__DFBNB_DF_CALC_API.search === "function") {
    window.__DFBNB_DF_CALC_API.search(query);
    return true;
  }

  return false;
}

function wireGuideSearch(inputEl) {
  ensureSearchStyles();

  let last = "";

  inputEl.addEventListener("input", () => {
    const q = String(inputEl.value || "");
    if (q === last) return;
    last = q;

    // Prefer app-level search when on app routes
    const usedApp = runAppSearchIfAvailable(q);
    if (!usedApp) runGenericBodySearch(q);
  });

  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      inputEl.value = "";
      last = "";
      const usedApp = runAppSearchIfAvailable("");
      if (!usedApp) runGenericBodySearch("");
    }
  });
}

  function fillGuideSelect(model, category, keepUrl) {
    const guideSel = byId("dfbnbGuideSelect");
    if (!guideSel) return;

    const list = model.byCat.get(category) || [];
    guideSel.innerHTML = list.map(g => `<option value="${escapeHtml(g.url)}">${escapeHtml(g.title)}</option>`).join("");

    if (keepUrl && list.some(g => g.url === keepUrl)) {
      guideSel.value = keepUrl;
    } else if (list[0]) {
      guideSel.value = list[0].url;
    }
  }

  function getSelectedGuide(model) {
    const catSel = byId("dfbnbGuideCategorySelect");
    const guideSel = byId("dfbnbGuideSelect");
    if (!catSel || !guideSel) return null;

    const cat = catSel.value;
    const url = normalizePath(guideSel.value);
    const list = model.byCat.get(cat) || [];
    return list.find(g => g.url === url) || null;
  }

  function setDropdownStateToGuide(model, guide) {
    const catSel = byId("dfbnbGuideCategorySelect");
    const guideSel = byId("dfbnbGuideSelect");
    if (!catSel || !guideSel) return;

    // Find the category that contains this guide
    let foundCat = "";
    for (const c of model.categories) {
      const list = model.byCat.get(c) || [];
      if (list.some(g => g.url === guide.url)) {
        foundCat = c;
        break;
      }
    }
    if (!foundCat) return;

    catSel.value = foundCat;
    fillGuideSelect(model, foundCat, guide.url);
  }

async function navigateToGuide(guide) {
  if (!guide || !guide.url) return;

  const path = normalizePath(guide.url);

  // Update URL without leaving page
  history.pushState({}, "", path);

  // Update header + buttons
  setGuideHeader(guide);
  wirePinButtonForGuide(path);
  wireShareButtonForGuide(guide.title, path);

  // Clear body first
  clearGuideBody();

  // For normal WP guides/pages, inject their HTML into the shell (no reload needed)
  let didInject = false;
  try {
    didInject = await injectGuideBodyFromHTML(path);
  } catch (e) {
    didInject = false;
  }

  // If we injected WP HTML, we are done (apps mount only on app routes)
  if (didInject) {
    try { await updatePatchLogForPath(path); } catch (e) {}
    idle(() => prefetchLikelyNext(guide));
    return;
  }

  // Otherwise, mount SPA apps (Challenges / Titles / Generator / Plans / Events Rewards)
  ensureChallengesCSSIfNeeded(path);
  ensureCampTitlesCSSIfNeeded(path);
  ensurePlanSystemCSSIfNeeded(path);
  ensureEventsRewardsCSSIfNeeded(path);
  ensureCurvesCSSIfNeeded(path);
  ensureDfCalculatorsCSSIfNeeded(path);
  ensureArmourModsCSSIfNeeded(path);
  ensureCollectablesCSSIfNeeded(path);

  try { await ensureCampTitlesDataURLs(); } catch (e) {}

  try { await maybeMountChallenges(path); } catch (e) {}
  try { await maybeMountCampTitles(path); } catch (e) {}
  try { await maybeMountTitlesGenerator(path); } catch (e) {}
  try { await maybeMountPlanSystem(path); } catch (e) {}
  try { await maybeMountEventsRewards(path); } catch (e) {}
  try { await maybeMountCurves(path); } catch (e) {}
  try { await maybeMountDfCalculators(path); } catch (e) {}
  try { await maybeMountArmourMods(path); } catch (e) {}
  try { await maybeMountCollectables(path); } catch (e) {}

  try { await updatePatchLogForPath(path); } catch (e) {}

  idle(() => prefetchLikelyNext(guide));
}

  function setGuideHeader(guide) {
    const title = byId("dfbnbGuideTitle");
    const updated = byId("dfbnbGuideUpdated");
    const views = byId("dfbnbGuideViews");
    const likes = byId("dfbnbGuideLikes");
    const author = byId("dfbnbGuideAuthor");
    const thanksWrap = byId("dfbnbGuideThanksWrap");
    const thanks = byId("dfbnbGuideThanks");
    const imgCreditWrap = byId("dfbnbGuideImageCreditWrap");
    const imgCredit = byId("dfbnbGuideImageCredit");
    const creditToWrap = byId("dfbnbGuideCreditToWrap");
    const creditTo = byId("dfbnbGuideCreditTo");

    if (title) title.textContent = guide.title || "Untitled";
if (updated) {
  const perGuide = safeText(guide.updated);
  const globalIso =
    (window.dfbnbData && safeText(window.dfbnbData.site_updated_iso))
      ? safeText(window.dfbnbData.site_updated_iso)
      : "";

  updated.textContent =
    formatDateDDMMMYYYY(perGuide) ||
    formatDateDDMMMYYYY(globalIso) ||
    "";
}

    if (views) views.textContent = String(guide.views || 0);
    if (likes) likes.textContent = String(guide.likes || 0);
if (author) {
  const perGuide =
    safeText(guide.writtenBy) ||
    safeText(guide.author) ||
    "";

  const globalAuthor =
    window.dfbnbData && safeText(window.dfbnbData.default_author)
      ? safeText(window.dfbnbData.default_author)
      : "";

  const finalAuthor = perGuide || globalAuthor;

  author.textContent = finalAuthor;
}

    const t = safeText(guide.thanks);
    if (thanksWrap && thanks) {
      if (t) {
        thanks.textContent = t;
        thanksWrap.style.display = "";
      } else {
        thanks.textContent = "";
        thanksWrap.style.display = "none";
      }
    }

    const ic = safeText(guide.imageCredit) || safeText(guide.image_credit);
if (imgCreditWrap && imgCredit) {
  if (ic) {
    imgCredit.textContent = ic;
    imgCreditWrap.style.display = "";
  } else {
    imgCredit.textContent = "";
    imgCreditWrap.style.display = "none";
  }
}

const ct = safeText(guide.creditTo) || safeText(guide.credit_to);
if (creditToWrap && creditTo) {
  if (ct) {
    creditTo.textContent = ct;
    creditToWrap.style.display = "";
  } else {
    creditTo.textContent = "";
    creditToWrap.style.display = "none";
  }
}
  }

  /* =========================================================
     PREFETCH: intent-based (McMaster style)
     ========================================================= */

  function prefetchCategoryLikelyGuides(model, current) {
    if (!current) return;

    // Find category list for current guide
    for (const cat of model.categories) {
      const list = model.byCat.get(cat) || [];
      const idx = list.findIndex(g => g.url === current.url);
      if (idx < 0) continue;

      // Prefetch next 2 and previous 1 in the same category
      const picks = [list[idx + 1], list[idx + 2], list[idx - 1]].filter(Boolean);
      for (const g of picks) prefetchUrl(g.url);
      break;
    }
  }

  function prefetchLikelyNext(current) {
    if (!current || !current.url) return;
    // Prefetch the current URL itself lightly (helps on revisit)
    prefetchUrl(current.url);
  }

  // Prefetch internal links when hovered/focused (you can also call this on guide body later)
  function wirePrefetchForAnchors(root) {
    if (!root) return;
    const anchors = root.querySelectorAll('a[href^="/"]');
    anchors.forEach(a => {
      a.addEventListener("mouseenter", () => prefetchUrl(a.getAttribute("href")), { passive: true });
      a.addEventListener("focus", () => prefetchUrl(a.getAttribute("href")), { passive: true });
    });
  }

  const prefetchSeen = new Set();
  function prefetchUrl(href) {
    try {
      const u = new URL(href, location.origin);
      if (u.origin !== location.origin) return;

      const path = u.pathname + u.search;
      if (prefetchSeen.has(path)) return;
      prefetchSeen.add(path);

      // Low priority fetch. Browser cache does the heavy lifting.
      fetch(u.href, { credentials: "same-origin", cache: DEV_NO_CACHE ? "no-store" : "default" })
        .then(() => {})
        .catch(() => {});
    } catch (e) {}
  }

  function preloadFetch(url) {
    try {
      const u = new URL(url, location.href);
      if (u.origin !== location.origin) return;

      const link = document.createElement("link");
      link.rel = "preload";
      link.as = "fetch";
      link.href = u.href;
      link.crossOrigin = "anonymous";
      document.head.appendChild(link);
    } catch (e) {}
  }

  /* =========================================================
     FETCH HELPERS (Home + Category pattern)
     ========================================================= */

  async function fetchJSON(url) {
    if (!url) return null;
    const cleanUrl = String(url);
    const join = cleanUrl.indexOf("?") >= 0 ? "&" : "?";
    const bustedUrl = cleanUrl + join + "v=" + Date.now();

    try {
      const res = await fetch(DEV_NO_CACHE ? bustedUrl : cleanUrl, {
        cache: DEV_NO_CACHE ? "no-store" : "default",
        credentials: "same-origin"
      });
      if (!res.ok) throw new Error(`Failed fetch: ${url} (${res.status})`);
      return await res.json();
    } catch (e) {
      // One retry with bust if caching got weird
      try {
        const res2 = await fetch(bustedUrl, { cache: "no-store", credentials: "same-origin" });
        if (!res2.ok) throw new Error(`Failed fetch: ${url} (${res2.status})`);
        return await res2.json();
      } catch (e2) {
        throw e2;
      }
    }
  }

  async function fetchText(url) {
    if (!url) return "";
    const cleanUrl = String(url);
    const join = cleanUrl.indexOf("?") >= 0 ? "&" : "?";
    const bustedUrl = cleanUrl + join + "v=" + Date.now();

    const res = await fetch(DEV_NO_CACHE ? bustedUrl : cleanUrl, {
      credentials: "same-origin",
      cache: DEV_NO_CACHE ? "no-store" : "default"
    });

    if (!res.ok) throw new Error(`Failed fetch: ${url} (${res.status})`);
    return await res.text();
  }

  function getBrandFromPath(pathname) {
  const p = String(pathname || "");
  return p.startsWith("/df/") ? "df" : "bnb";
}

/* =========================================================
   SPA HTML SWAP (non-app guides/pages)
   ========================================================= */

async function injectGuideBodyFromHTML(pathname) {
  const targetPath = normalizePath(pathname || location.pathname || "");
  if (!targetPath) return false;

  // Do NOT inject server HTML on SPA app routes (they mount their own UI)
  if (
    isChallengesChecklistPath(targetPath) ||
    isCampTitlesPath(targetPath) ||
    isTitlesGeneratorPath(targetPath) ||
    isPlanSystemPath(targetPath) ||
    isEventsRewardsPath(targetPath) ||
    isCurveTablesPath(targetPath) ||
    isDfCalculatorsPath(targetPath)
  ) {
    return false;
  }

  const bodyEl = byId("dfbnbGuideBody");
  if (!bodyEl) return false;

  // Fetch the actual WP page HTML, then steal just the guide body section.
  const html = await fetchText(targetPath);
  const doc = new DOMParser().parseFromString(html, "text/html");

const incoming =
  doc.getElementById("dfbnbWpContent") ||          // your hidden WP content wrapper (most reliable)
  doc.getElementById("dfbnbGuideBody") ||          // if a guide shell page got fetched
  doc.querySelector(".dfbnb-guideBody") ||
  doc.querySelector("main .entry-content") ||
  doc.querySelector("article .entry-content");

  if (!incoming) return false;

  bodyEl.innerHTML = incoming.innerHTML;

  // Re-enable link prefetch inside the injected content
  try { wirePrefetchForAnchors(bodyEl); } catch (e) {}

  return true;
}

  /* =========================================================
     TSV parser (robust header mapping)
     ========================================================= */

  function parseTSV(tsv) {
    const text = String(tsv || "").trim();
    if (!text) return { cols: [], rows: [] };

    const lines = text.split(/\r?\n/).filter(Boolean);
    const header = lines.shift();
    if (!header) return { cols: [], rows: [] };

    const cols = header.split("\t").map(c => c.trim());
    const rows = [];

    for (const line of lines) {
      const parts = line.split("\t");
      const row = {};
      for (let i = 0; i < cols.length; i++) row[cols[i]] = parts[i] == null ? "" : parts[i];
      // Normalize common keys used across your TSV variants
      row.id = row.id || row.ID || row.Id || "";
      row.brand = row.brand || row.Brand || "";
      row.nodeType = row.nodeType || row.NodeType || row.type || "";
      row.topCategory = row.topCategory || row.TopCategory || row.top_category || "";
      row.topTitle = row.topTitle || row.TopTitle || row.top || "";
      row.title = row.title || row.Title || "";
      row.menuTitle = row.menuTitle || row.MenuTitle || "";
      row.slug = row.slug || row.Slug || "";
      row.url = row.url || row.URL || "";
      row.browseOrder = row.browseOrder || row.BrowseOrder || "";
      rows.push(row);
    }

    return { cols, rows };
  }

  /* =========================================================
     UTIL
     ========================================================= */

  function byId(id) { return document.getElementById(id); }

  function safeText(s) {
    const t = (s == null) ? "" : String(s);
    return t.trim();
  }

  function safeLower(s) { return safeText(s).toLowerCase(); }

  function escapeHtml(s) {
    return String(s || "")
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll('"',"&quot;")
      .replaceAll("'","&#039;");
  }

 function normalizePath(p) {
  let s = String(p || "/");

  // Full URL? keep pathname.
  if (s.startsWith("http://") || s.startsWith("https://")) {
    try { s = new URL(s).pathname; } catch (e) {}
  }

  s = s.split("#")[0].split("?")[0];

  if (!s.startsWith("/")) s = "/" + s;
  s = s.replace(/\/{2,}/g, "/");
  if (!s.endsWith("/")) s += "/";

  return collapseDoubleLastPath(s);
}

  function collapseDoubleLastPath(path) {
  const parts = String(path || "/").split("/").filter(Boolean);
  if (parts.length >= 2 && parts[parts.length - 1] === parts[parts.length - 2]) {
    parts.pop();
    return "/" + parts.join("/") + "/";
  }
  return String(path || "/").replace(/\/+$/, "/");
}

  function toInt(v) {
    const n = parseInt(String(v || "").trim(), 10);
    return isFinite(n) ? n : 0;
  }

  function uniq(arr) {
    const out = [];
    const seen = new Set();
    for (const x of arr) {
      const k = safeText(x);
      if (!k || seen.has(k)) continue;
      seen.add(k);
      out.push(k);
    }
    return out;
  }

  function alpha(a, b) {
    return String(a || "").localeCompare(String(b || ""), undefined, { sensitivity: "base" });
  }

  function idle(fn) {
    try {
      if ("requestIdleCallback" in window) return window.requestIdleCallback(fn, { timeout: 1200 });
    } catch (e) {}
    return setTimeout(fn, 0);
  }

  /* =========================================================
     ICONS
     ========================================================= */

  function svgEye() {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7z"></path>
        <circle cx="12" cy="12" r="3"></circle>
      </svg>
    `;
  }

  function svgHeart() {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M20.8 4.6c-1.7-1.6-4.4-1.5-6 .2L12 7.6 9.2 4.8c-1.6-1.7-4.3-1.8-6-.2-1.9 1.7-2 4.7-.3 6.6l8.1 8.2 8.1-8.2c1.7-1.9 1.6-4.9-.3-6.6z"></path>
      </svg>
    `;
  }


  /* =========================================================
     SMART HOVER PREFETCH
     - Prefetches same-origin guide/category/app links
     - Avoids duplicates
     ========================================================= */
  (function setupSmartPrefetch() {

    const prefetched = new Set();

    function shouldPrefetch(url) {
      try {
        const u = new URL(url, location.origin);
        if (u.origin !== location.origin) return false;
        if (prefetched.has(u.href)) return false;
        if (u.pathname.startsWith("/wp-admin")) return false;
        if (u.pathname.startsWith("/wp-json")) return false;
        return true;
      } catch {
        return false;
      }
    }

    function prefetchDocument(url) {
      if (!shouldPrefetch(url)) return;

      const link = document.createElement("link");
      link.rel = "prefetch";
      link.as = "document";
      link.href = url;

      document.head.appendChild(link);
      prefetched.add(new URL(url, location.origin).href);
    }

    document.addEventListener("mouseover", function(e) {
      const a = e.target.closest("a[href]");
      if (!a) return;
      if (a.dataset.noPrefetch === "1") return;

      const url = a.href;

// If it's a Titles page link, prefetch its JSON too
if (url.includes("/titles") && window.__DFBNB_TITLES_API?.prefetch) {
  window.__DFBNB_TITLES_API.prefetch();
}
      setTimeout(() => prefetchDocument(url), 80);
    }, { passive: true });

    document.addEventListener("touchstart", function(e) {
      const a = e.target.closest("a[href]");
      if (!a) return;
      if (a.dataset.noPrefetch === "1") return;

      prefetchDocument(a.href);
    }, { passive: true });

  })();

})();