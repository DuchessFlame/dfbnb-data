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

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readText(filePath) {
  return fs.readFileSync(filePath, "utf8");
}

function writeJson(filePath, data) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

function parseTSV(tsvText) {
  const lines = tsvText.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return { header: [], rows: [] };

  const header = lines[0].split("\t").map(h => h.trim());
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
  // Example: ...\curvetables\json\armor\something.json
  // We take the folder after ".../json/"
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

function clampMinMax(curve) {
  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;

  for (const p of curve.points) {
    xMin = Math.min(xMin, p.x);
    xMax = Math.max(xMax, p.x);
    yMin = Math.min(yMin, p.y);
    yMax = Math.max(yMax, p.y);
  }

  if (!curve.points.length) {
    xMin = 0; xMax = 0; yMin = 0; yMax = 0;
  }

  curve.xMin = xMin;
  curve.xMax = xMax;
  curve.yMin = yMin;
  curve.yMax = yMax;
  return curve;
}

function normalizeFormId(formId) {
  // Keep as string, uppercase, strip 0x if present, left pad to 8 if numeric-looking.
  if (!formId) return "";
  let s = String(formId).trim().toUpperCase();
  if (s.startsWith("0X")) s = s.slice(2);
  // If it is hex digits only, pad.
  if (/^[0-9A-F]+$/.test(s)) s = s.padStart(8, "0");
  return s;
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

    const key = formId;
    let curve = curvesMap.get(key);
    if (!curve) {
      curve = {
        id: formId,
        edid: edid || "",
        jsonPath: jsonPath || "",
        category: safeCategoryFromJsonPath(jsonPath),
        points: []
      };
      curvesMap.set(key, curve);
    }

    // Prefer first non-empty EDID / JsonPath if duplicates exist
    if (!curve.edid && edid) curve.edid = edid;
    if (!curve.jsonPath && jsonPath) curve.jsonPath = jsonPath;

    curve.points.push({ x, y });
  }

  // Sort points by x then y
  const curves = Array.from(curvesMap.values()).map(c => {
    c.points.sort((a, b) => (a.x - b.x) || (a.y - b.y));
    c.pointsCount = c.points.length;
    return clampMinMax(c);
  });

  // Sort curves for stable output
  curves.sort((a, b) => {
    const ac = a.category.localeCompare(b.category);
    if (ac) return ac;
    const ae = (a.edid || "").localeCompare(b.edid || "");
    if (ae) return ae;
    return a.id.localeCompare(b.id);
  });

  // Build index list (no points)
  const indexCurves = curves.map(c => ({
    id: c.id,
    edid: c.edid,
    category: c.category,
    points: c.pointsCount,
    xMin: c.xMin,
    xMax: c.xMax,
    yMin: c.yMin,
    yMax: c.yMax
  }));

  const categoriesMap = new Map();
  for (const c of indexCurves) {
    categoriesMap.set(c.category, (categoriesMap.get(c.category) || 0) + 1);
  }
  const categories = Array.from(categoriesMap.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, count]) => ({ id, title: titleCaseCategory(id), count }));

  // Write chunks
  const chunksRoot = path.join(OUT_DIR, "chunks");
  ensureDir(chunksRoot);

  const chunkIndex = {}; // category -> list of chunk files
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
        xMin: c.xMin,
        xMax: c.xMax,
        yMin: c.yMin,
        yMax: c.yMax,
        points: c.points
      }));

      const fileName = `${cat}.${chunkNum}.json`;
      const outPath = path.join(catDir, fileName);

      writeJson(outPath, {
        category: cat,
        chunk: chunkNum,
        count: slice.length,
        curves: slice
      });

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

  const index = {
    meta,
    categories,
    chunks: chunkIndex,
    curves: indexCurves
  };

  ensureDir(OUT_DIR);
  writeJson(path.join(OUT_DIR, "meta.json"), meta);
  writeJson(path.join(OUT_DIR, "index.json"), index);

  console.log(`[build_curves_json] OK`);
  console.log(`- input: ${INPUT}`);
  console.log(`- curves: ${meta.curves}, points: ${meta.points}`);
  console.log(`- out: ${OUT_DIR}`);
}

function groupBy(arr, fn) {
  return arr.reduce((acc, item) => {
    const k = fn(item) || "other";
    (acc[k] ||= []).push(item);
    return acc;
  }, {});
}

function titleCaseCategory(id) {
  // Special cases you can add later
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

build();