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
const PCRD_TSV     = process.env.PCRD_TSV     || "tsv/PCRD_Export_March_2026.tsv";
const PERK_TSV     = process.env.PERK_TSV     || "tsv/PERK_Export_March_2026.tsv";
const CURV_HDR_TSV = process.env.CURV_HDR_TSV || "tsv/CURV_Export_March_2026.tsv";

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
  const reB = /\[[A-Z0-9_]+:([0-9A-Fa-f]{8})\]/g;
  let mm;
  while ((mm = reB.exec(s)) !== null) out.push(mm[1].toUpperCase());

  // Format C: "SPEL:AbPerkFoo[007ACE71]" (PERK EffectLink columns - bare FormID in brackets)
  const reC = /\[([0-9A-Fa-f]{8})\]/g;
  while ((mm = reC.exec(s)) !== null) out.push(mm[1].toUpperCase());

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
    legendarymods: "Legendary Mods",
    itemcondition: "Item Condition",
    encounterwave: "Encounter Wave",
    perkcardpacks: "Perk Card Packs",
    fasttravelcostcurvedistancejson: "Fast Travel Cost (Distance)",
    fasttravelcostmultcurvejson: "Fast Travel Cost (Multiplier)",
    fasttraveloverencumberedcostmultcurvejson: "Fast Travel (Over-Encumbered)",
    movecampcostcurvejson: "Move Camp Cost",
    xp_curvejson: "XP Curve",
  };
  if (map[id]) return map[id];

  // Strip trailing "json" from IDs like "xp_curvejson"
  let clean = id.replace(/json$/i, "");

  return clean
    .split(/[-_]/g)
    .filter(Boolean)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

// =========================================================
// Description generator, soft cap detection, display tables
// =========================================================

/** Parse EDID into structured parts for description generation */
function parseEdid(edid) {
  const s = String(edid || "").trim();
  if (!s) return { raw: s, subject: "", metric: "", prefix: "" };

  // Strip common prefixes
  let work = s;
  let prefix = "";
  const prefixMatch = /^(CT_|cr|ab|Ab|Ench|ench|Weap_|PA_)/i.exec(work);
  if (prefixMatch) { prefix = prefixMatch[1]; work = work.slice(prefix.length); }

  // Split on underscores and CamelCase
  const parts = work
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .split(/[_]+/)
    .filter(Boolean);

  // Last part is often the metric
  const metricWords = ["BaseDMG", "DMG", "Damage", "DR", "ER", "Health", "HP",
    "Bonus", "Scale", "Mult", "Cost", "Rate", "Speed", "Weight",
    "Chance", "Duration", "Radius", "Range", "Regen", "Resist",
    "Armor", "AP", "Stagger", "Bleed", "Reload", "Bash", "DPS",
    "Tier", "Level", "Count", "Offset", "Min", "Max"];

  let metric = "";
  let subject = parts.join(" ");

  for (let i = parts.length - 1; i >= 0; i--) {
    const p = parts[i];
    if (metricWords.some(m => p.toLowerCase().includes(m.toLowerCase()))) {
      metric = parts.slice(i).join(" ");
      subject = parts.slice(0, i).join(" ");
      break;
    }
  }

  return { raw: s, subject: subject || work, metric, prefix };
}

/** Convert parsed EDID subject to human-readable name */
function humanize(s) {
  return s
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/(\d+)/g, " $1 ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Determine X-axis label based on category and X range */
function inferXLabel(category, xMin, xMax) {
  // Specific categories
  if (category === "special") return "SPECIAL Stat Value";
  if (category === "perkcardpacks") return "Player Level";
  if (category === "legendaryperks") return "Legendary Perk Rank";

  // X range heuristics
  if (xMax <= 5 && xMin >= 0) return "Rank";
  if (xMax <= 10 && xMin >= 1) return "Star Rating / Rank";
  if (xMax <= 15 && xMin >= 1) return "SPECIAL Stat Value";
  if (xMax <= 100) return "Player Level";
  if (xMax <= 500) return "Player Level";
  return "Input Value";
}

/** Determine Y-axis label based on category, EDID, and Y range */
function inferYLabel(category, edid, yMin, yMax) {
  const e = (edid || "").toLowerCase();

  // Explicit metric keywords
  if (/basedmg|_dmg|damage/i.test(e)) return "Base Damage";
  if (/\bdr\b|damageresist/i.test(e)) return "Damage Resistance";
  if (/\ber\b|energyresist/i.test(e)) return "Energy Resistance";
  if (/health|hp\b/i.test(e)) return "Health";
  if (/armor(?!ed)/i.test(e) && !/bonus/i.test(e)) return "Armor Value";
  if (/regen|aprecovery/i.test(e)) return "Regeneration Rate";
  if (/reload/i.test(e)) return "Reload Speed Modifier";
  if (/bash/i.test(e)) return "Bash Damage";
  if (/stagger/i.test(e)) return "Stagger Chance";
  if (/bleed/i.test(e)) return "Bleed Damage";
  if (/chance/i.test(e)) return "Chance (%)";
  if (/duration/i.test(e)) return "Duration (seconds)";
  if (/cost/i.test(e)) return "Cost (Caps)";
  if (/xp|experience/i.test(e)) return "XP Required";
  if (/weight/i.test(e)) return "Weight";
  if (/speed/i.test(e)) return "Speed Modifier";
  if (/resist/i.test(e)) return "Resistance Value";

  // Range-based heuristics
  if (yMin >= 0 && yMax <= 1.5) return "Multiplier";
  if (yMin >= 0 && yMax <= 100 && category !== "creatures") return "Bonus Value";

  // Category defaults
  const catDefaults = {
    weapons: "Damage / Stat Value",
    armor: "Armor / Resistance Value",
    creatures: "Stat Value",
    player: "Player Stat Value",
    perks: "Perk Bonus",
    spells: "Effect Magnitude",
    enchantments: "Enchantment Value",
    legendary: "Legendary Bonus",
    legendarymods: "Mod Value",
    itemcondition: "Condition Factor",
    bobbleheads: "Bobblehead Bonus",
    mutations: "Mutation Effect",
    econ: "Economy Value",
    crafting: "Crafting Multiplier",
    cobj: "Construction Value",
    workshop: "Workshop Value",
    brewing: "Brewing Effect",
    encounterwave: "Encounter Value",
    vendors: "Vendor Value",
  };

  return catDefaults[category] || "Output Value";
}

/** Generate a human-readable description for a curve */
function generateDescription(edid, category, xMin, xMax, yMin, yMax) {
  const parsed = parseEdid(edid);
  const subj = humanize(parsed.subject);
  const cat = category || "other";

  // Special well-known curves
  const knownDescs = {
    xp_curvejson: "XP required to reach each player level.",
    fasttravelcostcurvedistancejson: "Fast travel caps cost based on distance traveled.",
    fasttravelcostmultcurvejson: "Multiplier applied to fast travel cost.",
    fasttraveloverencumberedcostmultcurvejson: "Extra fast travel cost when over-encumbered.",
    movecampcostcurvejson: "Caps cost to move your C.A.M.P. based on distance.",
  };
  if (knownDescs[cat] && xMin !== undefined) return knownDescs[cat];

  // Perk bonus curves (category = "perks")
  if (cat === "perks" && /bonus/i.test(edid)) {
    const perkName = subj.replace(/\s*Bonus.*$/i, "").trim();
    if (yMax <= 1.5 && yMin >= 0) {
      return `${perkName} perk card bonus multiplier, scaling by player level. A value of 1.0 means no change; higher means a stronger effect.`;
    }
    return `${perkName} perk card bonus value, scaling by player level.`;
  }

  // Weapons
  if (cat === "weapons") {
    if (/basedmg|_dmg/i.test(edid)) {
      return `Base damage for ${subj}, scaling by player level. Higher levels deal more damage.`;
    }
    return `${subj} weapon stat, scaling by level.`;
  }

  // Armor
  if (cat === "armor") {
    if (/\bdr\b/i.test(edid)) return `Damage resistance for ${subj} armor, scaling by level.`;
    if (/\ber\b/i.test(edid)) return `Energy resistance for ${subj} armor, scaling by level.`;
    if (/health/i.test(edid)) return `Health/durability for ${subj} armor, scaling by level.`;
    return `${subj} armor stat, scaling by level.`;
  }

  // Creatures
  if (cat === "creatures") {
    const tierMatch = /tier\s*(\d+)/i.exec(edid);
    const tierStr = tierMatch ? ` (Tier ${tierMatch[1]})` : "";
    if (/armor/i.test(edid)) return `Creature armor value${tierStr}, scaling by creature level.`;
    if (/health|hp/i.test(edid)) return `Creature health${tierStr}, scaling by creature level.`;
    if (/dmg|damage/i.test(edid)) return `Creature damage${tierStr}, scaling by creature level.`;
    return `Creature stat${tierStr} for ${subj}, scaling by level.`;
  }

  // Player
  if (cat === "player") {
    return `Player ${subj.toLowerCase()} stat, scaling by level.`;
  }

  // Item condition
  if (cat === "itemcondition") {
    return `${subj} condition/durability factor, scaling by item level.`;
  }

  // Legendary mods
  if (cat === "legendarymods") {
    return `Legendary mod: ${subj} effect value, scaling by star rating.`;
  }

  // Legendary
  if (cat === "legendary") {
    return `Legendary ${subj.toLowerCase()} value, scaling by legendary rank.`;
  }

  // Spells
  if (cat === "spells") {
    return `Spell effect: ${subj}, scaling by caster level.`;
  }

  // Enchantments
  if (cat === "enchantments") {
    return `Enchantment: ${subj} effect magnitude, scaling by level.`;
  }

  // Encounter wave
  if (cat === "encounterwave") {
    return `Encounter wave: ${subj} scaling by player level or zone.`;
  }

  // Bobbleheads
  if (cat === "bobbleheads") {
    return `Bobblehead ${subj.toLowerCase()} bonus, scaling by player level.`;
  }

  // Mutations
  if (cat === "mutations") {
    return `Mutation: ${subj} effect value.`;
  }

  // Econ
  if (cat === "econ") {
    return `Economy: ${subj} value, scaling by level or rank.`;
  }

  // Crafting / COBJ
  if (cat === "crafting" || cat === "cobj") {
    return `Crafting: ${subj} scaling value.`;
  }

  // Workshop
  if (cat === "workshop") {
    return `Workshop: ${subj} value, scaling by level.`;
  }

  // Brewing
  if (cat === "brewing") {
    return `Brewing: ${subj} effect value.`;
  }

  // Perk card packs
  if (cat === "perkcardpacks") {
    return `Perk card pack: ${subj} probability or count, by player level.`;
  }

  // Vendors
  if (cat === "vendors") {
    return `Vendor: ${subj} value.`;
  }

  // Generic fallback
  return `${subj} curve table value, scaling by input.`;
}

/**
 * Detect soft cap: the X value where Y gains start diminishing significantly.
 * Returns { x, y, ratio } or null if no clear soft cap.
 *
 * Algorithm:
 * 1. Calculate the rate of change (slope) between consecutive points
 * 2. Find the peak slope in the first half of the curve
 * 3. Find where slope drops below 25% of peak and stays low
 * 4. That transition point is the soft cap
 */
function detectSoftCap(points) {
  if (!points || points.length < 4) return null;

  // Need at least some range
  const xRange = points[points.length - 1].x - points[0].x;
  const yRange = Math.abs(points[points.length - 1].y - points[0].y);
  if (xRange <= 0 || yRange < 0.001) return null;

  // Calculate slopes between consecutive points
  const slopes = [];
  for (let i = 1; i < points.length; i++) {
    const dx = points[i].x - points[i - 1].x;
    if (dx <= 0) continue;
    const dy = points[i].y - points[i - 1].y;
    slopes.push({ x: (points[i].x + points[i - 1].x) / 2, slope: dy / dx, idx: i });
  }
  if (slopes.length < 3) return null;

  // Determine if curve is increasing or decreasing
  const totalSlope = (points[points.length - 1].y - points[0].y) / xRange;
  const absSlopes = slopes.map(s => Math.abs(s.slope));

  // Find peak absolute slope in the first half
  const halfIdx = Math.floor(slopes.length / 2);
  let peakSlope = 0;
  for (let i = 0; i <= halfIdx; i++) {
    if (absSlopes[i] > peakSlope) peakSlope = absSlopes[i];
  }
  if (peakSlope < 0.0001) return null;

  // Find where slope drops below 25% of peak and stays below 35% for 2+ consecutive
  const threshold = peakSlope * 0.25;
  const sustainThreshold = peakSlope * 0.35;

  for (let i = 1; i < slopes.length - 1; i++) {
    if (absSlopes[i] < threshold) {
      // Check if it stays low
      let staysLow = true;
      const lookAhead = Math.min(i + 3, slopes.length);
      for (let j = i + 1; j < lookAhead; j++) {
        if (absSlopes[j] > sustainThreshold) { staysLow = false; break; }
      }
      if (staysLow) {
        // The soft cap is at this point
        const scIdx = slopes[i].idx;
        return {
          x: points[scIdx].x,
          y: points[scIdx].y,
          ratio: absSlopes[i] / peakSlope
        };
      }
    }
  }

  return null;
}

/**
 * Build a pre-calculated display table with interpolated values.
 * Steps: 1-100 by 1s if xMax <= 100, otherwise adaptive stepping.
 * Injects the soft cap point if present.
 */
function buildDisplayTable(points, softCap) {
  if (!points || !points.length) return [];

  const xMin = points[0].x;
  const xMax = points[points.length - 1].x;
  const range = xMax - xMin;

  if (range <= 0) return [{ x: xMin, y: points[0].y }];

  // Determine step size
  let step;
  if (range <= 50) step = 1;
  else if (range <= 100) step = 1;
  else if (range <= 500) step = 5;
  else if (range <= 1000) step = 10;
  else if (range <= 5000) step = 50;
  else step = 500;

  // Generate X values
  const xValues = new Set();
  for (let x = Math.ceil(xMin); x <= xMax; x += step) {
    xValues.add(x);
  }
  // Always include actual min and max
  xValues.add(xMin);
  xValues.add(xMax);
  // Include actual raw points at boundaries
  for (const p of points) xValues.add(p.x);
  // Include soft cap point
  if (softCap) xValues.add(softCap.x);

  // Sort
  const sortedX = Array.from(xValues).sort((a, b) => a - b);

  // Linear interpolation
  const table = [];
  let pIdx = 0;

  for (const x of sortedX) {
    // Advance pIdx to find the bracketing points
    while (pIdx < points.length - 1 && points[pIdx + 1].x < x) pIdx++;

    let y;
    if (x <= points[0].x) {
      y = points[0].y;
    } else if (x >= points[points.length - 1].x) {
      y = points[points.length - 1].y;
    } else {
      // Find bracketing pair
      let lo = 0, hi = points.length - 1;
      for (let i = 0; i < points.length - 1; i++) {
        if (points[i].x <= x && points[i + 1].x >= x) {
          lo = i; hi = i + 1; break;
        }
      }
      const dx = points[hi].x - points[lo].x;
      if (dx === 0) {
        y = points[lo].y;
      } else {
        const t = (x - points[lo].x) / dx;
        y = points[lo].y + t * (points[hi].y - points[lo].y);
      }
    }

    table.push({ x, y: Math.round(y * 10000) / 10000 });
  }

  // Cap at 500 rows for sanity
  if (table.length > 500) {
    // Resample to 500 even steps
    const resampled = [];
    const tableStep = Math.max(1, Math.floor(table.length / 500));
    for (let i = 0; i < table.length; i += tableStep) {
      resampled.push(table[i]);
    }
    // Always include last
    if (resampled[resampled.length - 1].x !== table[table.length - 1].x) {
      resampled.push(table[table.length - 1]);
    }
    return resampled;
  }

  return table;
}

/** Enrich a curve object with description, labels, soft cap, display table */
function enrichCurve(curve) {
  const desc = generateDescription(curve.edid, curve.category, curve.xMin, curve.xMax, curve.yMin, curve.yMax);
  const xLabel = inferXLabel(curve.category, curve.xMin, curve.xMax);
  const yLabel = inferYLabel(curve.category, curve.edid, curve.yMin, curve.yMax);
  const softCap = detectSoftCap(curve.points);
  const displayTable = buildDisplayTable(curve.points, softCap);

  curve.desc = desc;
  curve.xLabel = xLabel;
  curve.yLabel = yLabel;
  if (softCap) curve.softCap = { x: softCap.x, y: Math.round(softCap.y * 10000) / 10000 };
  curve.displayTable = displayTable;

  return curve;
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

  // Enrich all curves with descriptions, labels, soft caps, display tables
  for (const c of curves) enrichCurve(c);

  const indexCurves = curves.map(c => ({
    id: c.id,
    edid: c.edid,
    category: c.category,
    points: c.pointsCount,
    xMin: c.xMin, xMax: c.xMax,
    yMin: c.yMin, yMax: c.yMax,
    desc: c.desc || "",
    xLabel: c.xLabel || "",
    yLabel: c.yLabel || "",
    ...(c.softCap ? { softCap: c.softCap } : {})
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
        desc: c.desc || "",
        xLabel: c.xLabel || "",
        yLabel: c.yLabel || "",
        ...(c.softCap ? { softCap: c.softCap } : {}),
        points: c.points,
        displayTable: c.displayTable || []
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
  // PERK CARDS INDEX
  // Chain: CURV refs → SPEL/PERK/ENCH → PERK EffectLinks → PCRD RankPERKs
  // Output: dist/curves/perk_cards.json
  // =========================================================

  const absPcrd    = path.resolve(PCRD_TSV);
  const absPerk    = path.resolve(PERK_TSV);
  const absCurvHdr = path.resolve(CURV_HDR_TSV);

  if (fs.existsSync(absPcrd) && fs.existsSync(absPerk) && fs.existsSync(absCurvHdr)) {
    const pcrdRows    = parseTSV(readText(absPcrd)).rows;
    const perkRows    = parseTSV(readText(absPerk)).rows;
    const curvHdrRows = parseTSV(readText(absCurvHdr)).rows;

    // Quick lookup for curve stubs (from indexCurves we just built)
    const curveStubById = new Map(indexCurves.map(c => [c.id, c]));

    // --------------------------------------------------
    // Step 1: Build refFormId → Set<CURV_FormID> from CURV header refs
    // Each CURV row has Ref1..RefN columns like "FormID:EDID:TYPE"
    // These are records that REFERENCE (use) this curve table.
    // --------------------------------------------------
    const refToCurvs = new Map();

    for (const r of curvHdrRows) {
      const curvId = normalizeFormId(r.CURV_FormID);
      if (!curvId || !curveStubById.has(curvId)) continue;

      // Scan all Ref columns
      for (const [key, val] of Object.entries(r)) {
        if (!/^Ref\d+$/.test(key)) continue;
        const v = String(val || "").trim();
        if (!v) continue;

        // Extract FormIDs from ref text (formats: "FormID:EDID:TYPE" or "[TYPE:FormID]")
        const ids = extractFormIdsFromRef(v);
        for (const refId of ids) {
          if (!refToCurvs.has(refId)) refToCurvs.set(refId, new Set());
          refToCurvs.get(refId).add(curvId);
        }
      }
    }

    console.log(`[build_curves_json] CURV ref index: ${refToCurvs.size} unique referencing FormIDs`);

    // --------------------------------------------------
    // Step 2: Build PERK_FormID → Set<EffectLink FormIDs> from PERK TSV
    // EffectLink columns contain "TYPE:EDID[FormID]" entries (e.g. "SPEL:AbPerkFoo[00AABBCC]")
    // --------------------------------------------------
    const perkToLinks = new Map();

    for (const r of perkRows) {
      const perkId = normalizeFormId(r.PERK_FormID);
      if (!perkId) continue;

      const linkIds = perkToLinks.get(perkId) || new Set();

      for (let i = 1; i <= 30; i++) {
        const el = String(r[`EffectLink_${i}`] || "").trim();
        if (!el) continue;
        const ids = extractFormIdsFromRef(el);
        for (const id of ids) linkIds.add(id);
      }

      // Also grab Spell_FormID and CurveTable_FormID if present
      pushIfFormId(linkIds, r.Spell_FormID);
      pushIfFormId(linkIds, r.CurveTable_FormID);

      if (linkIds.size) perkToLinks.set(perkId, linkIds);
    }

    console.log(`[build_curves_json] PERK link index: ${perkToLinks.size} perks with links`);

    // --------------------------------------------------
    // Step 3: For each PCRD, collect curves via:
    //   PCRD → RankPERK FormIDs → PERK EffectLinks (SPELs etc.) → CURV refs
    //   Also check if the PERK itself is referenced by a CURV
    // --------------------------------------------------
    const perkGroups = [];

    for (const r of pcrdRows) {
      const pcrdFormId = normalizeFormId(r.PCRD_FormID);
      const pcrdEdid = String(r.PCRD_EDID || "").trim();
      const pcrdName = String(r.MNAM_Name || r.PCRD_EDID || "").trim();

      if (!pcrdFormId) continue;

      // Collect rank PERK FormIDs
      const rankPerkIds = new Set();
      for (let i = 1; i <= 12; i++) {
        const pid = normalizeFormId(r[`RankPERK_${i}_FormID`]);
        if (pid) rankPerkIds.add(pid);
      }

      const curveIdsSet = new Set();

      for (const perkId of rankPerkIds) {
        // Check if any CURV directly references this PERK
        const directCurvs = refToCurvs.get(perkId);
        if (directCurvs) {
          for (const cid of directCurvs) curveIdsSet.add(cid);
        }

        // Get PERK's EffectLinks (SPELs, ENCHs, etc.)
        const links = perkToLinks.get(perkId);
        if (!links) continue;

        for (const linkId of links) {
          // Check if any CURV references this linked record
          const linkedCurvs = refToCurvs.get(linkId);
          if (linkedCurvs) {
            for (const cid of linkedCurvs) curveIdsSet.add(cid);
          }
        }
      }

      // If no curves found, skip perk card
      if (!curveIdsSet.size) continue;

      // Curves ABC by EDID (fallback to id)
      const curves = Array.from(curveIdsSet)
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

    console.log(`[build_curves_json] perk_cards.json: ${perkGroups.length} perks, ${Array.from(usedCats).length} chunk categories`);
  } else {
    const missing = [];
    if (!fs.existsSync(absPcrd))    missing.push("PCRD_TSV");
    if (!fs.existsSync(absPerk))    missing.push("PERK_TSV");
    if (!fs.existsSync(absCurvHdr)) missing.push("CURV_HDR_TSV");
    console.log(`[build_curves_json] perk_cards.json skipped (missing: ${missing.join(", ")})`);
  }

  console.log(`[build_curves_json] OK`);
  console.log(`- input: ${INPUT}`);
  console.log(`- curves: ${meta.curves}, points: ${meta.points}`);
  console.log(`- out: ${OUT_DIR}`);
}

build();