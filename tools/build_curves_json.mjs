#!/usr/bin/env node
/**
 * Build Curve Tables JSON (index + lazy chunks) from CURV_Export_*_POINTS.tsv
 *
 * Input expected columns (minimum):
 *   FormID, EDID, X, Y, JsonPath
 *
 * Output:
 *   dist/curves/index.json
 *   dist/curves/meta.json
 *   dist/curves/chunks/<category>/<category>.<n>.json
 */

import fs from "fs";
import path from "path";

const INPUT = process.env.CURV_POINTS_TSV || "tsv/CURV_Export_March_2026_POINTS.tsv";
const OUT_DIR = process.env.CURV_OUT_DIR || "dist/curves";
const CHUNK_MAX_CURVES = Number(process.env.CURV_CHUNK_MAX_CURVES || 200);

// Needed for perk-cards cross-reference output
const PCRD_TSV  = process.env.PCRD_TSV  || "tsv/PCRD_Export_March_2026.tsv";
const SPEL_EFF_TSV = process.env.SPEL_EFF_TSV || "tsv/SPEL_Export_March_2026_EFFECTS.tsv";

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }
function readText(filePath) { return fs.readFileSync(filePath, "utf8"); }
function writeJson(filePath, data) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

function parseTSV(tsvText) {
  const lines = tsvText.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return { header: [], rows: [] };

  const header = lines[0].split("\t").map((h, i) => {
  let s = (h ?? "").trim();
  // Strip UTF-8 BOM if present (common in xEdit TSV exports)
  if (i === 0) s = s.replace(/^\uFEFF/, "");
  return s;
});
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split("\t");
    const row = {};
    for (let c = 0; c < header.length; c++) row[header[c]] = (cols[c] ?? "").trim();
    rows.push(row);
  }
  return { header, rows };
}

function safeCategoryFromJsonPath(jsonPath) {
  if (!jsonPath) return "other";
  const norm = jsonPath.replace(/\\/g, "/").toLowerCase();
  const idx = norm.indexOf("/json/");
  if (idx === -1) return "other";
  const rest = norm.slice(idx + "/json/".length);
  const parts = rest.split("/").filter(Boolean);
  if (!parts.length) return "other";
  const folder = parts[0].replace(/[^a-z0-9_-]/g, "");
  return folder || "other";
}

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeFormId(formId) {
  if (!formId) return "";
  let s = String(formId).trim().toUpperCase();
  if (s.startsWith("0X")) s = s.slice(2);
  if (/^[0-9A-F]+$/.test(s)) s = s.padStart(8, "0");
  return s;
}

function clampMinMax(curve) {
  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;
  for (const p of curve.points) {
    xMin = Math.min(xMin, p.x);
    xMax = Math.max(xMax, p.x);
    yMin = Math.min(yMin, p.y);
    yMax = Math.max(yMax, p.y);
  }
  if (!curve.points.length) { xMin = 0; xMax = 0; yMin = 0; yMax = 0; }
  curve.xMin = xMin; curve.xMax = xMax; curve.yMin = yMin; curve.yMax = yMax;
  return curve;
}

function groupBy(arr, fn) {
  return arr.reduce((acc, item) => {
    const k = fn(item) || "other";
    (acc[k] ||= []).push(item);
    return acc;
  }, {});
}

function isRejectedEdid(edid) {
  const s = String(edid || "").trim();
  if (!s) return false;

  const u = s.toUpperCase();

  // Filter out cut/post stuff early (matches your naming patterns)
  // Examples: "DEL POST ...", "DEL_POST_...", "ZZZ_...", "CUT_..."
  if (u.startsWith("DEL")) return true;
  if (u.startsWith("CUT")) return true;
  if (u.startsWith("ZZZ")) return true;

  return false;
}

function extractFormIdsFromRef(refText) {
  const s = String(refText || "").trim();
  if (!s) return [];

  const out = [];

  // Format A: "0089EA90:Something:GLOB"
  const m = /^([0-9A-Fa-f]{8}):/.exec(s);
  if (m) out.push(m[1].toUpperCase());

  // Format B: "Name [GLOB:0085AD24]" or "[PERK:01234567]" etc
  const re = /\[[A-Z0-9_]+:([0-9A-Fa-f]{8})\]/g;
  let mm;
  while ((mm = re.exec(s)) !== null) out.push(mm[1].toUpperCase());

  // De-dupe
  return Array.from(new Set(out));
}

function pushIfFormId(set, s) {
  const id = normalizeFormId(s);
  if (id) set.add(id);
}

function titleCaseCategory(id) {
  const map = {
    legendaryperks: "Legendary Perks",
    itemcondition: "Item Condition",
    encounterwave: "Encounter Wave"
  };
  if (map[id]) return map[id];

  return id
    .split(/[-_]/g)
    .filter(Boolean)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function build() {
  const absIn = path.resolve(INPUT);
  if (!fs.existsSync(absIn)) {
    console.error(`[build_curves_json] Missing input TSV: ${absIn}`);
    process.exit(1);
  }

  const { rows } = parseTSV(readText(absIn));
  const curvesMap = new Map();

  for (const r of rows) {
    const formId = normalizeFormId(r.FormID || r.formid || r.formId);
    const edid = (r.EDID || r.edid || "").trim();
    const x = toNum(r.X ?? r.x);
    const y = toNum(r.Y ?? r.y);
    const jsonPath = (r.JsonPath || r.jsonpath || r.Path || "").trim();

    if (!formId || x === null || y === null) continue;

    // Stop CUT / DEL / ZZZ at the source
    if (isRejectedEdid(edid)) continue;

    let curve = curvesMap.get(formId);
    if (!curve) {
      curve = { id: formId, edid: edid || "", jsonPath: jsonPath || "", category: safeCategoryFromJsonPath(jsonPath), points: [] };
      curvesMap.set(formId, curve);
    }
    if (!curve.edid && edid) curve.edid = edid;
    if (!curve.jsonPath && jsonPath) curve.jsonPath = jsonPath;

    curve.points.push({ x, y });
  }

  const curves = Array.from(curvesMap.values()).map(c => {
    c.points.sort((a, b) => (a.x - b.x) || (a.y - b.y));
    c.pointsCount = c.points.length;
    return clampMinMax(c);
  });

  curves.sort((a, b) => {
    const ac = a.category.localeCompare(b.category);
    if (ac) return ac;
    const ae = (a.edid || "").localeCompare(b.edid || "");
    if (ae) return ae;
    return a.id.localeCompare(b.id);
  });

  const indexCurves = curves.map(c => ({
    id: c.id,
    edid: c.edid,
    category: c.category,
    points: c.pointsCount,
    xMin: c.xMin, xMax: c.xMax,
    yMin: c.yMin, yMax: c.yMax
  }));

  const categoriesMap = new Map();
  for (const c of indexCurves) categoriesMap.set(c.category, (categoriesMap.get(c.category) || 0) + 1);

  const categories = Array.from(categoriesMap.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, count]) => ({ id, title: titleCaseCategory(id), count }));

  const chunksRoot = path.join(OUT_DIR, "chunks");
  ensureDir(chunksRoot);

  const chunkIndex = {};
  const byCat = groupBy(curves, c => c.category);

  for (const [cat, list] of Object.entries(byCat)) {
    const catDir = path.join(chunksRoot, cat);
    ensureDir(catDir);

    const files = [];
    let chunkNum = 0;

    for (let i = 0; i < list.length; i += CHUNK_MAX_CURVES) {
      const slice = list.slice(i, i + CHUNK_MAX_CURVES).map(c => ({
        id: c.id,
        edid: c.edid,
        category: c.category,
        xMin: c.xMin, xMax: c.xMax,
        yMin: c.yMin, yMax: c.yMax,
        points: c.points
      }));

      const fileName = `${cat}.${chunkNum}.json`;
      writeJson(path.join(catDir, fileName), { category: cat, chunk: chunkNum, count: slice.length, curves: slice });
      files.push(`chunks/${cat}/${fileName}`);
      chunkNum++;
    }

    chunkIndex[cat] = files;
  }

  const meta = {
    buildTimeUTC: new Date().toISOString(),
    input: path.basename(INPUT),
    curves: indexCurves.length,
    points: curves.reduce((sum, c) => sum + c.pointsCount, 0),
    chunkMaxCurves: CHUNK_MAX_CURVES
  };

  writeJson(path.join(OUT_DIR, "meta.json"), meta);
  writeJson(path.join(OUT_DIR, "index.json"), { meta, categories, chunks: chunkIndex, curves: indexCurves });

  // =========================================================
  // PERK CARDS INDEX (rock-solid: PCRD link-set FormIDs vs CURV Ref# FormIDs)
  // Output: dist/curves/perk_cards.json
  // =========================================================

const absPcrd = path.resolve(PCRD_TSV);
const absSpelEff = path.resolve(SPEL_EFF_TSV);

if (fs.existsSync(absPcrd) && fs.existsSync(absSpelEff)) {
  const pcrdRows = parseTSV(readText(absPcrd)).rows;
  const spelEffRows = parseTSV(readText(absSpelEff)).rows;

  // --------------------------------------------------
  // Build SPEL_FormID -> Set<CURV_FormID>
  // --------------------------------------------------
  const spelToCurv = new Map();

  for (const r of spelEffRows) {
    const spelId = normalizeFormId(r.SPEL_FormID);
    const curvId = normalizeFormId(r.CVTO_CURV_FormID);
    if (!spelId || !curvId) continue;

    if (!spelToCurv.has(spelId)) spelToCurv.set(spelId, new Set());
    spelToCurv.get(spelId).add(curvId);
  }

    // Quick lookup for curve stubs (from indexCurves we just built)
    const curveStubById = new Map(indexCurves.map(c => [c.id, c]));

    // Build perk groups
    const perkGroups = [];

    for (const r of pcrdRows) {
      const pcrdFormId = normalizeFormId(r.PCRD_FormID);
      const pcrdEdid = String(r.PCRD_EDID || "").trim();
      const pcrdName = String(r.MNAM_Name || r.PCRD_EDID || "").trim();

      if (!pcrdFormId) continue;

      // Build “link set” of IDs for this perk card
      const linkSet = new Set();
      pushIfFormId(linkSet, r.PCRD_FormID);
      pushIfFormId(linkSet, r.PCDV_GLOB_FormID);

      // Rank PERKs (up to 12 in your export)
      for (let i = 1; i <= 12; i++) {
        pushIfFormId(linkSet, r[`RankPERK_${i}_FormID`]);
      }

// --------------------------------------------------
// Deterministic chain:
// PCRD → Rank PERK FormIDs → SPEL → CURV
// --------------------------------------------------

const curveIdsSet = new Set();

// Collect all possible SPEL FormIDs from this perk
const spelCandidates = new Set();

// Rank PERK FormIDs already pushed into linkSet earlier
for (const fid of linkSet) {
  // If this FormID is a spell and exists in spelToCurv, use it
  if (spelToCurv.has(fid)) {
    spelCandidates.add(fid);
  }
}

// Now collect curves from those spells
for (const spelId of spelCandidates) {
  const curvSet = spelToCurv.get(spelId);
  if (!curvSet) continue;

  for (const curvId of curvSet) {
    if (curveStubById.has(curvId))
      curveIdsSet.add(curvId);
  }
}

// If no curves found, skip perk
if (!curveIdsSet.size) continue;

const curveIds = Array.from(curveIdsSet);

      // Curves ABC by EDID (fallback to id)
      const curves = curveIds
        .map(id => curveStubById.get(id))
        .filter(Boolean)
        .sort((a, b) => (a.edid || "").localeCompare(b.edid || "") || a.id.localeCompare(b.id));

      perkGroups.push({
        pcrdFormId,
        pcrdEdid,
        name: pcrdName || pcrdEdid || pcrdFormId,
        curves
      });
    }

    // Perks ABC by display name
    perkGroups.sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));

    // Limit chunks to only categories used by perk curves (keeps file smaller)
    const usedCats = new Set();
    for (const g of perkGroups) for (const c of g.curves) usedCats.add(c.category);

    const perkChunks = {};
    for (const cat of usedCats) perkChunks[cat] = chunkIndex[cat] || [];

    writeJson(path.join(OUT_DIR, "perk_cards.json"), {
      meta,
      perks: perkGroups,
      chunks: perkChunks
    });

    console.log(`[build_curves_json] perk_cards.json: ${perkGroups.length} perks`);
  } else {
    console.log(`[build_curves_json] perk_cards.json skipped (missing PCRD_TSV or SPEL_EFF_TSV)`);
  }

  console.log(`[build_curves_json] OK`);
  console.log(`- input: ${INPUT}`);
  console.log(`- curves: ${meta.curves}, points: ${meta.points}`);
  console.log(`- out: ${OUT_DIR}`);
}

build();