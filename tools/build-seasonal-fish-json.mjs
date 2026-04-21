// tools/build-seasonal-fish-json.mjs
//
// Builds dist/seasonal-fish.json from the latest FISH + LVLI xEdit exports.
// Mirrors tools/build-axolotl-rotations-json.mjs in style and sort rules.
//
// Data flow:
//   1. FISH TSV  — find every "SeasonalFish_Fish_*" record (excludes zzz_ dev rows).
//      - Pull the display name from the FIRI/FIRL quoted field (FULL is blank for fish).
//      - Pull regional spawn list from Ref1..RefN references to
//        Fishing_LLS_FishCollection_<Region>_Uncommon LVLIs
//        (and Burn_Fishing_LLS_FishCollection_BurningSprings_Uncommon).
//      - Detect Local Legend entries by "_LocalLegend_" in the EDID.
//   2. LVLI_Entries TSV — read Fishing_LLS_FishCollection_SeasonalFish_Uncommon.
//      Each entry's condition includes
//        Subject.GetGlobalValue(LCP_Fishing_SeasonalFish_SeasonIndex) == <1..4>
//      Index 1=Spring, 2=Summer, 3=Fall, 4=Winter (verified against the in-game
//      Seasonal Fishing Report notes placed at Fisherman's Rest).
//   3. Merge — one fish per season, plus an optional localLegend per season.
//
// Robustness:
//   - Bethesda can move a fish between regional lists without touching the
//     seasonal list; we pick up the change automatically from the FISH Ref columns.
//   - Bethesda can add Spring/Fall/Winter common fish; this script will emit them
//     once they appear in SeasonalFish_Uncommon.
//   - Existing image URLs in dist/seasonal-fish.json are preserved on re-run
//     (keyed by EDID) so hand-curated artwork survives rebuilds.

import fs from "fs";
import path from "path";

/* ------------------------------------------------------------------ */
/* Shared helpers (match the axolotl script)                           */
/* ------------------------------------------------------------------ */

function readTextLatin1(p) {
  // LVLI exports can contain non-UTF8 chars (accented names etc.) — latin1 avoids crashes.
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

// Sort TSV filenames by the date encoded in their name rather than file mtime.
// Git checkouts give every file the same mtime, making mtime-based sorting unreliable.
const MONTH_ORDER = {
  jan:1, january:1, feb:2, february:2, mar:3, march:3,
  apr:4, april:4, may:5, jun:6, june:6, jul:7, july:7,
  aug:8, august:8, sep:9, september:9, oct:10, october:10,
  nov:11, november:11, dec:12, december:12
};

function filenameDateScore(filepath) {
  const name = path.basename(filepath).toLowerCase();
  const yearM = name.match(/(20\d{2})/);
  const year = yearM ? Number(yearM[1]) : 0;
  let month = 0;
  for (const [key, val] of Object.entries(MONTH_ORDER)) {
    if (name.includes(key)) { month = val; break; }
  }
  return year * 100 + month;
}

function pickLatestFishTsv(repoRoot) {
  const tsvRoot = path.join(repoRoot, "tsv");
  if (!fs.existsSync(tsvRoot)) {
    throw new Error(`Missing tsv/ folder at ${tsvRoot}. Put FISH + LVLI exports under tsv/.`);
  }
  const all = listFilesRecursive(tsvRoot);
  const matches = all.filter((p) => /FISH_Export_.*\.tsv$/i.test(path.basename(p)));
  if (!matches.length) {
    throw new Error(`No FISH_Export_*.tsv found under ${tsvRoot}`);
  }
  matches.sort((a, b) => filenameDateScore(b) - filenameDateScore(a));
  return matches[0];
}

function pickLatestLvliEntriesTsv(repoRoot) {
  const tsvRoot = path.join(repoRoot, "tsv");
  const all = listFilesRecursive(tsvRoot);
  const matches = all.filter((p) => /LVLI_Export_.*_LVLI_Entries\.tsv$/i.test(path.basename(p)));
  if (!matches.length) {
    throw new Error(`No LVLI_Export_*_LVLI_Entries.tsv found under ${tsvRoot}`);
  }
  matches.sort((a, b) => filenameDateScore(b) - filenameDateScore(a));
  return matches[0];
}

/* ------------------------------------------------------------------ */
/* Season rollover (Kevin's rule)                                      */
/* ------------------------------------------------------------------ */
//
// The in-game LCP_Fishing_SeasonalFish_SeasonIndex auto-rotates on the FIRST
// TUESDAY strictly after the North American astronomical equinox/solstice.
// (Source: Kevin, the dev who wrote the seasonal fish system.)
//
// We hard-code the equinox/solstice dates per year so the build is deterministic
// and doesn't depend on an astronomy library at build time. Add new years here
// as needed — values are the standard NA calendar dates.
const EQUINOX_SOLSTICE_NA = {
  // year: { spring: "YYYY-MM-DD", summer: ..., fall: ..., winter: ... }
  2024: { spring: "2024-03-19", summer: "2024-06-20", fall: "2024-09-22", winter: "2024-12-21" },
  2025: { spring: "2025-03-20", summer: "2025-06-20", fall: "2025-09-22", winter: "2025-12-21" },
  2026: { spring: "2026-03-20", summer: "2026-06-21", fall: "2026-09-22", winter: "2026-12-21" },
  2027: { spring: "2027-03-20", summer: "2027-06-21", fall: "2027-09-23", winter: "2027-12-22" },
  2028: { spring: "2028-03-20", summer: "2028-06-20", fall: "2028-09-22", winter: "2028-12-21" },
  2029: { spring: "2029-03-20", summer: "2029-06-21", fall: "2029-09-22", winter: "2029-12-21" },
  2030: { spring: "2030-03-20", summer: "2030-06-21", fall: "2030-09-22", winter: "2030-12-21" }
};

// Return the ISO date of the first Tuesday strictly AFTER the given ISO date.
// Uses UTC math so the result is stable regardless of the build server tz.
function firstTuesdayAfter(iso) {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  // Advance at least one day (strictly after).
  do {
    dt.setUTCDate(dt.getUTCDate() + 1);
  } while (dt.getUTCDay() !== 2); // 2 = Tuesday
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

// Build a flat list of season rollover dates across the years we know about.
// Each entry: { date: "YYYY-MM-DD", season: "spring"|"summer"|"fall"|"winter" }
// Sorted ascending. The client picks the current season by finding the most
// recent entry whose date <= today.
function buildRolloverDates() {
  const out = [];
  const years = Object.keys(EQUINOX_SOLSTICE_NA).map(Number).sort((a, b) => a - b);
  for (const y of years) {
    const e = EQUINOX_SOLSTICE_NA[y];
    out.push({ date: firstTuesdayAfter(e.spring), season: "spring" });
    out.push({ date: firstTuesdayAfter(e.summer), season: "summer" });
    out.push({ date: firstTuesdayAfter(e.fall),   season: "fall"   });
    out.push({ date: firstTuesdayAfter(e.winter), season: "winter" });
  }
  out.sort((a, b) => a.date.localeCompare(b.date));
  return out;
}

/* ------------------------------------------------------------------ */
/* Region mapping                                                      */
/* ------------------------------------------------------------------ */

// Map the EDID stem used in regional fish-collection LVLIs to the display name
// the site already uses (matches df-bnb-home.js normRegion conventions).
const REGION_STEM_TO_DISPLAY = {
  forest:         "The Forest",
  ash:            "Ash Heap",
  savagedivide:   "Savage Divide",
  skyline:        "Skyline Valley",
  cranberry:      "Cranberry Bog",
  mire:           "The Mire",
  toxic:          "Toxic Valley",
  burningsprings: "Burning Springs"
};

// Extract the region stem from a regional Fish-Collection LVLI EDID.
//   Fishing_LLS_FishCollection_Skyline_Uncommon         -> "skyline"
//   Burn_Fishing_LLS_FishCollection_BurningSprings_Uncommon -> "burningsprings"
// Returns null if the EDID isn't a regional uncommon/common collection.
function regionStemFromCollectionEdid(edid) {
  const s = String(edid || "");
  const m = s.match(/FishCollection_([A-Za-z0-9]+?)_(?:Uncommon|Common|Rare)\b/i);
  if (!m) return null;
  const stem = m[1].toLowerCase();
  // Ignore the cross-region "SeasonalFish" aggregator and any other non-region stems.
  if (stem === "seasonalfish" || stem === "all" || stem === "generic") return null;
  return stem;
}

function displayNameForStem(stem) {
  if (!stem) return null;
  return REGION_STEM_TO_DISPLAY[stem] || null;
}

/* ------------------------------------------------------------------ */
/* FISH TSV parsing                                                    */
/* ------------------------------------------------------------------ */

// Pull the display name out of the FIRI / FIRL quoted meal reference:
//   SeasonalFish_Meal_Small_Raw_Fernskipper "Fernskipper" [ALCH:008A581E]
function displayFromMealRef(rewardRef) {
  const s = String(rewardRef || "").trim();
  if (!s) return "";
  const m = s.match(/"([^"]+)"/);
  return m ? m[1].trim() : "";
}

// Walk Ref1..RefN columns and collect regional display names.
function regionsFromFishRefs(row) {
  const seen = new Set();
  const out = [];
  const seenStems = [];

  const refCount = Number(row.ReferencedByCount || 0) || 0;
  const maxCols = Math.max(refCount, 10); // belt-and-braces

  for (let i = 1; i <= maxCols; i++) {
    const raw = row["Ref" + i];
    if (!raw) continue;
    // Format: 007D499B:Fishing_LLS_FishCollection_Ash_Uncommon:LVLI
    const m = String(raw).match(/:([^:]+):LVLI\b/i);
    if (!m) continue;
    const stem = regionStemFromCollectionEdid(m[1]);
    if (!stem) continue;
    if (seen.has(stem)) continue;
    seen.add(stem);
    const display = displayNameForStem(stem);
    if (display) {
      out.push(display);
      seenStems.push(stem);
    } else {
      // Unknown stem — emit as pretty-cased for visibility; script still works.
      const pretty = stem.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/\b\w/g, (c) => c.toUpperCase());
      out.push(pretty);
      seenStems.push(stem);
    }
  }

  return { regions: out, regionKeywords: seenStems };
}

// Parse the season token out of a Local Legend EDID:
//   SeasonalFish_Fish_LocalLegend_SummerGlassGhost -> "summer"
function seasonFromLocalLegendEdid(edid) {
  const s = String(edid || "");
  const m = s.match(/_LocalLegend_(Spring|Summer|Fall|Autumn|Winter)/i);
  if (!m) return null;
  const tok = m[1].toLowerCase();
  return tok === "autumn" ? "fall" : tok;
}

/* ------------------------------------------------------------------ */
/* LVLI_Entries parsing — season index per fish                        */
/* ------------------------------------------------------------------ */

// From a LVLI_Entries row belonging to Fishing_LLS_FishCollection_SeasonalFish_Uncommon,
// return { fishEdid, seasonIndex } or null if we can't parse it.
function seasonalEntryInfo(row) {
  const ref = String(row.LVLO_Reference || "");
  const fishMatch = ref.match(/:([^:]+):FISH\b/i);
  if (!fishMatch) return null;
  const fishEdid = fishMatch[1];

  let seasonIndex = null;
  for (const k of Object.keys(row)) {
    if (!/^Cond\d+$/i.test(k)) continue;
    const c = row[k];
    if (!c) continue;
    if (!c.includes("LCP_Fishing_SeasonalFish_SeasonIndex")) continue;
    // Example:
    //   Subject.GetGlobalValue(...LCP_Fishing_SeasonalFish_SeasonIndex [GLOB:008B3A09]...) 10000000 1.000000
    // Grab the trailing numeric literal.
    const nm = c.match(/(\d+)\.0+\b\s*$/);
    if (nm) {
      seasonIndex = Number(nm[1]);
      break;
    }
  }

  if (!seasonIndex) return null;
  return { fishEdid, seasonIndex };
}

/* ------------------------------------------------------------------ */
/* Season index mapping                                                */
/* ------------------------------------------------------------------ */

// In-game SeasonIndex -> US meteorological season key
const SEASON_INDEX_TO_KEY = { 1: "spring", 2: "summer", 3: "fall", 4: "winter" };

const SEASON_META = {
  spring: { label: "Spring", months: [3, 4, 5]  },
  summer: { label: "Summer", months: [6, 7, 8]  },
  fall:   { label: "Fall",   months: [9, 10, 11] },
  winter: { label: "Winter", months: [12, 1, 2]  }
};

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

function main() {
  const repoRoot = process.cwd();
  const outDir = path.join(repoRoot, "dist");
  const outPath = path.join(outDir, "seasonal-fish.json");

  const fishTsvPath = pickLatestFishTsv(repoRoot);
  const lvliEntriesPath = pickLatestLvliEntriesTsv(repoRoot);

  const fishRows = parseTSV(readTextLatin1(fishTsvPath));
  const lvliEntriesRows = parseTSV(readTextLatin1(lvliEntriesPath));

  // Preserve hand-curated fields (image URLs, description overrides) across rebuilds.
  // Keyed by fishEditorId (case-insensitive).
  const existingImages = new Map(); // edid_lower -> image url
  try {
    if (fs.existsSync(outPath)) {
      const prev = JSON.parse(fs.readFileSync(outPath, "utf8"));
      const prevSeasons = prev && prev.seasons ? prev.seasons : {};
      for (const key of Object.keys(prevSeasons)) {
        const s = prevSeasons[key];
        if (s && s.fish && s.fish.fishEditorId && s.fish.image) {
          existingImages.set(String(s.fish.fishEditorId).toLowerCase(), s.fish.image);
        }
        if (s && s.localLegend && s.localLegend.fishEditorId && s.localLegend.image) {
          existingImages.set(String(s.localLegend.fishEditorId).toLowerCase(), s.localLegend.image);
        }
      }
    }
  } catch (e) {
    console.warn("Could not read previous seasonal-fish.json — image carry-over skipped.");
  }

  // --- Step 1: index every SeasonalFish_Fish_* record from FISH TSV ---
  const seasonalFishByEdid = new Map(); // edid_lower -> { formId, edid, name, regions[], regionKeywords[], isLocalLegend, lltag }
  for (const r of fishRows) {
    const edid = String(r.EDID || "").trim();
    if (!edid) continue;
    if (!/^SeasonalFish_Fish_/i.test(edid)) continue; // skip zzz_ dev rows
    const rewardRef = String(r.FIRL || r.FIRI || "").trim();
    const name = displayFromMealRef(rewardRef) || String(r.FULL || "").trim();
    const { regions, regionKeywords } = regionsFromFishRefs(r);
    const isLocalLegend = /_LocalLegend_/i.test(edid);
    const seasonTag = isLocalLegend ? seasonFromLocalLegendEdid(edid) : null;

    seasonalFishByEdid.set(edid.toLowerCase(), {
      fishFormId: String(r.FormID || "").trim(),
      fishEditorId: edid,
      name: name || edid.split("_").pop().replace(/([a-z])([A-Z])/g, "$1 $2").trim() || "TBA",
      regions,
      regionKeywords,
      isLocalLegend,
      seasonTag // only populated for local legends
    });
  }

  if (!seasonalFishByEdid.size) {
    throw new Error(`No SeasonalFish_Fish_* rows found in ${fishTsvPath}`);
  }

  // --- Step 2: read LVLI_Entries for Fishing_LLS_FishCollection_SeasonalFish_Uncommon ---
  const seasonalUncommonRows = lvliEntriesRows.filter(
    (r) => String(r.LVLI_EDID || "").trim() === "Fishing_LLS_FishCollection_SeasonalFish_Uncommon"
  );
  if (!seasonalUncommonRows.length) {
    throw new Error(
      `No LVLI_Entries rows found for Fishing_LLS_FishCollection_SeasonalFish_Uncommon in ${lvliEntriesPath}`
    );
  }

  // Map: seasonIndex (1..4) -> fishEdid (the main common seasonal fish for that season)
  const seasonIndexToFishEdid = new Map();
  for (const r of seasonalUncommonRows) {
    const info = seasonalEntryInfo(r);
    if (!info) continue;
    // Skip generic filler (RidgeTrout etc.) — only keep fish whose EDID is a SeasonalFish_Fish_*
    if (!/^SeasonalFish_Fish_/i.test(info.fishEdid)) continue;
    // Keep first occurrence per season.
    if (!seasonIndexToFishEdid.has(info.seasonIndex)) {
      seasonIndexToFishEdid.set(info.seasonIndex, info.fishEdid);
    }
  }

  // --- Step 3: assemble per-season payload ---
  const seasons = {};
  for (const key of ["spring", "summer", "fall", "winter"]) {
    seasons[key] = {
      seasonIndex: null,
      label: SEASON_META[key].label,
      months: SEASON_META[key].months,
      fish: null,
      localLegend: null
    };
  }

  for (const [seasonIndex, fishEdid] of seasonIndexToFishEdid.entries()) {
    const key = SEASON_INDEX_TO_KEY[seasonIndex];
    if (!key) continue;
    const fish = seasonalFishByEdid.get(fishEdid.toLowerCase());
    if (!fish) continue;
    seasons[key].seasonIndex = seasonIndex;
    seasons[key].fish = {
      name: fish.name,
      fishFormId: fish.fishFormId,
      fishEditorId: fish.fishEditorId,
      regionKeywords: fish.regionKeywords,
      regions: fish.regions,
      image: existingImages.get(fish.fishEditorId.toLowerCase()) || ""
    };
  }

  // Local legends — attach by their own season tag (from EDID).
  for (const fish of seasonalFishByEdid.values()) {
    if (!fish.isLocalLegend || !fish.seasonTag) continue;
    const key = fish.seasonTag;
    if (!seasons[key]) continue;
    seasons[key].localLegend = {
      name: fish.name,
      fishFormId: fish.fishFormId,
      fishEditorId: fish.fishEditorId,
      regionKeywords: fish.regionKeywords,
      regions: fish.regions,
      image: existingImages.get(fish.fishEditorId.toLowerCase()) || ""
    };
  }

  // --- Step 4: write payload ---
  const payload = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    source: {
      fishTsv: path.basename(fishTsvPath),
      lvliEntriesTsv: path.basename(lvliEntriesPath),
      seasonalListLvliEditorId: "Fishing_LLS_FishCollection_SeasonalFish_Uncommon",
      seasonIndexGlobal: "LCP_Fishing_SeasonalFish_SeasonIndex",
      weekendToggleGlobal: "LTT_WeekendSeasonalFish_Toggle"
    },
    timezone: "America/New_York",
    // Kevin's rule: the in-game season index auto-rotates on the first Tuesday
    // strictly AFTER the North American astronomical equinox/solstice.
    // rolloverDates is the source of truth for the client; monthToSeason is a
    // soft fallback only (used if the client ever lands outside the known years).
    seasonRule: {
      method: "first_tuesday_after_equinox_na",
      description: "Season rotates on the first Tuesday strictly after the NA equinox/solstice.",
      rolloverDates: buildRolloverDates(),
      monthToSeason: {
        "1":"winter","2":"winter","3":"spring","4":"spring","5":"spring",
        "6":"summer","7":"summer","8":"summer",
        "9":"fall","10":"fall","11":"fall","12":"winter"
      }
    },
    weekendEvent: {
      note: "Seasonal fish are only catchable during Bethesda's weekend seasonal fish events.",
      toggleGlobal: "LTT_WeekendSeasonalFish_Toggle"
    },
    // Display-name mapping for region keywords (parallel to axolotl-rotations.json).
    regionKeywordToDisplay: {
      Forest: "The Forest",
      Ash: "Ash Heap",
      SavageDivide: "Savage Divide",
      Skyline: "Skyline Valley",
      Cranberry: "Cranberry Bog",
      Mire: "The Mire",
      Toxic: "Toxic Valley",
      BurningSprings: "Burning Springs"
    },
    seasons
  };

  ensureDir(outDir);
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + "\n", "utf8");

  // Reporting
  console.log(`Source FISH TSV:        ${fishTsvPath}`);
  console.log(`Source LVLI Entries:    ${lvliEntriesPath}`);
  const summary = Object.entries(seasons).map(([k, v]) => {
    const f = v.fish ? `${v.fish.name} [${v.fish.regions.join(", ") || "?"}]` : "—";
    const ll = v.localLegend ? `LL: ${v.localLegend.name}` : "";
    return `  ${k.padEnd(6)} (idx ${v.seasonIndex ?? "?"}): ${f}${ll ? "  " + ll : ""}`;
  }).join("\n");
  console.log(`Wrote ${outPath}\n${summary}`);
}

main();
