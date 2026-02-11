// tools/build-axolotl-rotations-json.mjs
import fs from "fs";
import path from "path";

function readTextLatin1(p) {
  // LVLI exports often contain non-UTF8 chars (eg accented names), so read as latin1 to avoid crashes.
  return fs.readFileSync(p, "latin1");
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function parseTSV(tsvText) {
  const lines = String(tsvText)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((l) => l.trim().length > 0);

  if (!lines.length) return [];

  const header = lines[0].split("\t").map((h) => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split("\t");
    const row = {};
    for (let c = 0; c < header.length; c++) {
      row[header[c]] = (parts[c] ?? "").trim();
    }
    rows.push(row);
  }
  return rows;
}

function listFilesRecursive(dir) {
  const out = [];
  const items = fs.readdirSync(dir, { withFileTypes: true });
  for (const it of items) {
    const p = path.join(dir, it.name);
    if (it.isDirectory()) out.push(...listFilesRecursive(p));
    else out.push(p);
  }
  return out;
}

function pickLatestLvliEntriesTsv(repoRoot) {
  const tsvRoot = path.join(repoRoot, "tsv");
  if (!fs.existsSync(tsvRoot)) {
    throw new Error(`Missing tsv/ folder at ${tsvRoot}. Put LVLI exports under tsv/.`);
  }

  const all = listFilesRecursive(tsvRoot);

  // Match: LVLI_Export_Feb_2026_LVLI_Entries.tsv (or similar)
  const matches = all.filter((p) => /LVLI_Export_.*_LVLI_Entries\.tsv$/i.test(path.basename(p)));

  if (!matches.length) {
    throw new Error(`No LVLI_Export_*_LVLI_Entries.tsv found under ${tsvRoot}`);
  }

  // Choose newest by file modified time.
  matches.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return matches[0];
}

function prettyFromLocRegion(keyword) {
  // LocRegionForestFloodlands -> Forest Floodlands
  let s = String(keyword || "").trim();
  s = s.replace(/^LocRegion/i, "");
  s = s.replace(/([a-z])([A-Z])/g, "$1 $2").trim();
  return s || "";
}

function prettyFromFishRef(lvloRef) {
  // Example:
  // 0080070C:Fishing_Fish_Small_Axolotl01_CharcoalAxolotl:FISH
  const ref = String(lvloRef || "");
  const m = ref.match(/:([^:]+):FISH\b/i);
  if (!m) return "";

  const edid = m[1]; // Fishing_Fish_Small_Axolotl01_CharcoalAxolotl
  const parts = edid.split("_");
  const last = parts[parts.length - 1] || "";
  // CharcoalAxolotl -> Charcoal Axolotl
  return last.replace(/([a-z])([A-Z])/g, "$1 $2").trim();
}

function extractMonthIndex(row) {
  // Look through Cond columns for LCP_Fishing_Axolotl_MonthlyIndex
  for (const k of Object.keys(row)) {
    if (!/^Cond\d+$/i.test(k)) continue;
    const s = row[k];
    if (!s) continue;

    if (s.includes("LCP_Fishing_Axolotl_MonthlyIndex")) {
      const mm = s.match(/MonthlyIndex\s+\[GLOB:[0-9A-F]+\].*?(\d+)\.000000\b/i);
      if (mm) return Number(mm[1]);
    }
  }
  return null;
}

function extractRegions(row) {
  const regions = new Set();

  for (const k of Object.keys(row)) {
    if (!/^Cond\d+$/i.test(k)) continue;
    const s = row[k];
    if (!s) continue;

    // Example:
    // ... LocRegionBurningSprings [KYWD:007AE59D] ...
    const m = s.match(/\b(LocRegion[A-Za-z0-9_]+)\s+\[KYWD:/);
    if (m) {
      const pretty = prettyFromLocRegion(m[1]);
      if (pretty) regions.add(pretty);
    }
  }

  return Array.from(regions);
}

function main() {
  const repoRoot = process.cwd();
  const outDir = path.join(repoRoot, "dist");
  const outPath = path.join(outDir, "axolotl-rotations.json");

  const entriesPath = pickLatestLvliEntriesTsv(repoRoot);

  const tsv = readTextLatin1(entriesPath);
  const rows = parseTSV(tsv);

  // Target list: Fishing_LLS_FishCollection_Axolotls
  const axoRows = rows.filter((r) => String(r.LVLI_EDID || "").trim() === "Fishing_LLS_FishCollection_Axolotls");

  if (!axoRows.length) {
    throw new Error(`No rows found for LVLI_EDID=Fishing_LLS_FishCollection_Axolotls in ${entriesPath}`);
  }

  const months = {};

  for (const r of axoRows) {
    const idx = extractMonthIndex(r);
    if (!idx || idx < 1 || idx > 12) continue;

    const name = prettyFromFishRef(r.LVLO_Reference);
    const regions = extractRegions(r);

    months[String(idx)] = {
      name: name || "TBA",
      regions,
      image: null
    };
  }

  const found = Object.keys(months).length;
  if (found !== 12) {
    console.warn(`WARNING: Found ${found}/12 month entries for axolotls. Output may be incomplete.`);
  }

  // This shape matches your df-bnb-home.js renderer:
  // axoData.months["2"] => { name, regions, image }
  const payload = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),

    timezone: "America/New_York",
       rollover: { timeLocal: "12:00:00" },

    months
  };

  ensureDir(outDir);
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + "\n", "utf8");

  console.log(`Source LVLI Entries: ${entriesPath}`);
  console.log(`Wrote ${outPath} (${Object.keys(months).length} months)`);
}

main();
