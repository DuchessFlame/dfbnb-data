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

/**
 * Find the newest FISH_Export_*.tsv in tsv/ and return its path, or null if absent.
 * The FISH TSV has columns: FormID, EDID, FULL, ...
 * The FULL column is the human-readable in-game display name (e.g. "Clay Axolotl").
 * This takes precedence over deriving a name from the EDID string.
 * To regenerate: export FISH records from xEdit as a TSV and drop it in tsv/.
 */
function pickLatestFishTsv(repoRoot) {
  const tsvRoot = path.join(repoRoot, "tsv");
  if (!fs.existsSync(tsvRoot)) return null;

  const all = listFilesRecursive(tsvRoot);
  // Match: FISH_Export_March_2026.tsv (or similar)
  const matches = all.filter((p) => /FISH_Export_.*\.tsv$/i.test(path.basename(p)));
  if (!matches.length) return null;

  matches.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return matches[0];
}

/**
 * Build a Map<edid_lowercase, displayName> from the FISH TSV.
 *
 * The display name we want is NOT in the FULL column (which is blank for fish records).
 * It lives in the FIRL column — this is what xEdit labels "FIRI / Item Reward".
 * The TSV exporter outputs it as the full xEdit reference string, e.g.:
 *
 *   Fishing_Fish_Meal_Small_Raw_Axolotl03_BrownAxolotl "Clay Axolotl" [ALCH:008006CF]
 *
 * We extract the display name from the double-quoted segment.
 *
 * Column name note: xEdit calls this field FIRI; the TSV exporter writes it as FIRL.
 * They are the same field at the same position. The script accepts either header name
 * so it works regardless of which xEdit version produced the export.
 *
 * Usage: fishNames.get("fishing_fish_small_axolotl03_brownaxolotl") => "Clay Axolotl"
 */
function buildFishNameMap(fishTsvPath) {
  if (!fishTsvPath || !fs.existsSync(fishTsvPath)) return new Map();

  const tsv = readTextLatin1(fishTsvPath);
  const rows = parseTSV(tsv);
  const map = new Map();

  for (const r of rows) {
    const edid = String(r.EDID || "").trim();
    if (!edid) continue;

    // The item-reward field may be exported as FIRL or FIRI depending on xEdit version.
    const rewardRef = String(r.FIRL || r.FIRI || "").trim();

    if (rewardRef) {
      // Reference format: EDID "Display Name" [TYPE:FormID]
      // Extract the double-quoted display name.
      const m = rewardRef.match(/"([^"]+)"/);
      if (m && m[1]) {
        map.set(edid.toLowerCase(), m[1].trim());
        continue;
      }
    }

    // Fallback: use FULL if it happens to be populated (rare for FISH records).
    const full = String(r.FULL || "").trim();
    if (full) {
      map.set(edid.toLowerCase(), full);
    }
  }

  return map;
}

function prettyFromLocRegion(keyword) {
  // LocRegionForestFloodlands -> Forest Floodlands
  let s = String(keyword || "").trim();
  s = s.replace(/^LocRegion/i, "");
  s = s.replace(/([a-z])([A-Z])/g, "$1 $2").trim();
  return s || "";
}

function prettyFromFishRef(lvloRef, fishNames) {
  // Example LVLO_Reference value:
  // 0080070C:Fishing_Fish_Small_Axolotl01_CharcoalAxolotl:FISH
  const ref = String(lvloRef || "");
  const m = ref.match(/:([^:]+):FISH\b/i);
  if (!m) return "";

  const edid = m[1]; // e.g. Fishing_Fish_Small_Axolotl03_BrownAxolotl

  // Prefer the in-game display name from the FISH TSV (FULL column).
  // This is important because EDID and display name can differ —
  // e.g. EDID "BrownAxolotl" has FULL "Clay Axolotl" in game.
  if (fishNames && fishNames.size > 0) {
    const fromFish = fishNames.get(edid.toLowerCase());
    if (fromFish) return fromFish;
  }

  // Fallback: derive a readable name from the EDID's last segment.
  // CharcoalAxolotl -> Charcoal Axolotl
  const parts = edid.split("_");
  const last = parts[parts.length - 1] || "";
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

// Month index -> image URL shown on homepage
const AXO_MONTH_IMAGES = {
  "1":  "https://cdn.streamelements.com/uploads/01kdej8na6cc2gz0q3heam70f8.png", // Charcoal
  "2":  "https://cdn.streamelements.com/uploads/01kdej8n6v5bzz07v6a704vg2f.png", // Pink
  "3":  "https://cdn.streamelements.com/uploads/01kdej8p2cwcevdem3rjgb6ckx.png", // Brown (Clay)
  "4":  "https://cdn.streamelements.com/uploads/01kdej8mr4kyt32bvh911768ww.png", // Dotted
  "5":  "https://cdn.streamelements.com/uploads/01kdej8p4w3g8k3kkhffs8xvx4.png", // Purple
  "6":  "https://cdn.streamelements.com/uploads/01kdej8p3d4k5p7khnpemgvyhd.png", // Banded
  "7":  "https://cdn.streamelements.com/uploads/01kdej8p0wy2qvhfefqefax2fm.png", // Scaled
  "8":  "https://cdn.streamelements.com/uploads/01key7rqv3dc3yh8nabts235r8.webp", // Striped
  "9":  "https://cdn.streamelements.com/uploads/01kdej8n6s3gj5fzwxwcfr1xpa.png", // Shadow
  "10": "https://cdn.streamelements.com/uploads/01kdej8p1fr3d8thseydq7t56d.png", // Spotted
  "11": "https://cdn.streamelements.com/uploads/01kdej8nx04tykm9tebknkx1mq.png", // Speckled
  "12": "https://cdn.streamelements.com/uploads/01kdej8payhzwde401rgns4x60.png"  // Stone
};


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

  // Load the FISH TSV to get accurate in-game display names (FULL column).
  // Fish EDID names can differ from their display names — e.g. EDID "BrownAxolotl"
  // has the in-game FULL name "Clay Axolotl". Without this lookup the wrong name
  // would be shown on the website.
  const fishTsvPath = pickLatestFishTsv(repoRoot);
  const fishNames = buildFishNameMap(fishTsvPath);
  if (fishTsvPath) {
    console.log(`Source FISH TSV:        ${fishTsvPath} (${fishNames.size} named fish)`);
  } else {
    console.warn("WARNING: No FISH_Export_*.tsv found — fish names will be derived from EDID (may be inaccurate).");
  }

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

    const name = prettyFromFishRef(r.LVLO_Reference, fishNames);
    const regions = extractRegions(r);

const key = String(idx);

months[key] = {
  name: name || "TBA",
  regions,
  image: AXO_MONTH_IMAGES[key] || null
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

  console.log(`Source LVLI Entries:    ${entriesPath}`);
  console.log(`Wrote ${outPath} (${Object.keys(months).length} months)`);
}

main();
