#!/usr/bin/env python3
"""
src/build_fishing_calculator_json.py
Reads Fallout 76 fishing data from FISH, GLOB, and LVLI TSV exports.
Computes cascading spawn-rate probabilities and outputs fishing.json to dist/.

Outputs include:
  - globs          Raw GLOB percentages keyed by Weather_Bait_FishType
  - fish           Full fish catalogue (name, type, region, size, season, etc.)
  - spawnRates     Pre-computed effective cascade rates (with Local Legend)
  - spawnRatesNoLegend   Same rates without Local Legend step
  - regions        Sorted list of all regions found
  - junkRareChance Junk rare vs common split percentage
  - seasonalFish   Seasonal fish data (name, season, regions, type)
  - waterloggedGifts  Waterlogged gift data (odds, tiers)
  - weekendSeasonalFish  Weekend seasonal fish event data (odds, bait sub-lists)
  - axolotlRotation  Monthly axolotl-to-region mapping

No external dependencies — runs on stdlib only.
"""

import json
import os
import re
import sys
import glob as globmod
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")
DIST_FILE = os.path.join(DIST_DIR, "fishing.json")

# ── What's Biting? — Public Events that reward Improved Bait ──────────────────
# The "Public Events" list under Improved Bait on the What's Biting guide is
# GENERATIVE and DATA-DERIVED, straight from the game exports — not a guess at
# "which public events probably give bait".
#
# In the game files the Improved Bait MISC item is handed out through a leveled
# list, Fishing_LL_Rewards_ImprovedBait (form id WB_BAIT_POOL_FID). A handful of
# reward leveled-lists distribute that pool (e.g. RA_LL_Rewards_PublicEvents,
# the per-quest reward lists for Jail Break, Scorched Earth, the Skyline caravan
# escort and the public bounty hunt). An event grants Improved Bait iff its
# GMRW quest-reward references one of those lists. We compute it in two steps:
#   1. walk the LVLI tree up from the bait pool to every list that distributes
#      it (handles any future nesting automatically), then
#   2. scan GMRW for every event whose reward references one of those lists.
#
# This auto-updates when Bethesda attaches/detaches the reward, and naturally
# EXCLUDES public events that don't grant bait (Welcoming Committee, Monster
# Mash, Gearin' Up …) and the non-event sources listed elsewhere on the guide
# (the Casting Off quest and the daily "Big Fish in a Small Pond").
WB_BAIT_POOL_FID = "0081137A"   # Fishing_LL_Rewards_ImprovedBait

# Optional manual additions — e.g. cut content you want kept with a strike
# through. Set "cut": True to render the name struck through. Empty by default;
# the live list is fully data-derived.
WB_EVENTS_EXTRA = []

def _wb_latest(rel_pattern):
    """Newest file under the repo root matching a tsv/ glob pattern."""
    matches = sorted(globmod.glob(os.path.join(SCRIPT_DIR, "..", rel_pattern)))
    return matches[-1] if matches else None

def _wb_read_rows(path):
    """Read a TSV into a list of split rows, tolerant of export encodings."""
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                return [ln.rstrip("\n").split("\t") for ln in f]
        except UnicodeDecodeError:
            continue
    return []

def build_whats_biting_public_events():
    """Data-derived list of public events that reward Improved Bait.

    Traces the bait pool (Fishing_LL_Rewards_ImprovedBait, WB_BAIT_POOL_FID) up
    the leveled-list tree to the reward lists that distribute it, then reads
    GMRW for every event whose quest reward uses one of those lists. Returns
    [{"name": str, "cut"?: bool}, ...] sorted by name, or None if the exports
    can't be read (the front-end then uses its built-in fallback list)."""
    lvli = _wb_latest("tsv/LVLI_Export_*_LVLI_Entries.tsv")
    gmrw = _wb_latest("tsv/GMRW_Export_*.tsv")
    if not lvli or not gmrw:
        print("[fishing] whatsBiting: LVLI/GMRW export missing — "
              "skipping publicEvents (front-end will use its fallback)",
              file=sys.stderr)
        return None

    # child fid → set of parent leveled-list fids
    parents = {}
    for row in _wb_read_rows(lvli):
        if len(row) < 4:
            continue
        lst = row[0].strip().upper()
        for cell in row[3:]:
            m = re.match(r"([0-9A-Fa-f]{8}):", cell)
            if m:
                parents.setdefault(m.group(1).upper(), set()).add(lst)

    # Upward closure: every list that distributes the bait pool.
    bait_lists = set()
    frontier = {WB_BAIT_POOL_FID.upper()}
    while frontier:
        nxt = set()
        for fid in frontier:
            for parent in parents.get(fid, ()):
                if parent not in bait_lists:
                    bait_lists.add(parent)
                    nxt.add(parent)
        frontier = nxt
    if not bait_lists:
        print(f"[fishing] whatsBiting: bait pool {WB_BAIT_POOL_FID} not "
              "referenced by any leveled list", file=sys.stderr)
        return None

    lvli_ref = re.compile(r"([0-9A-Fa-f]{8}):[^:\t]*:LVLI")
    prefix = re.compile(r"^(Event:|Activity:)\s*")
    names = set()
    for row in _wb_read_rows(gmrw):
        if len(row) < 4:
            continue
        row_fids = {m.upper() for m in lvli_ref.findall("\t".join(row))}
        if not (row_fids & bait_lists):
            continue
        name = prefix.sub("", (row[2] or "").strip()).strip()
        if name:
            names.add(name)
    if not names:
        print("[fishing] whatsBiting: no events reference the bait reward "
              "lists — skipping publicEvents", file=sys.stderr)
        return None

    events = [{"name": n} for n in names]
    have = {e["name"] for e in events}
    for extra in WB_EVENTS_EXTRA:
        if extra.get("name") and extra["name"] not in have:
            events.append(dict(extra))
    events.sort(key=lambda e: e["name"].lower())
    return events

# ── Region mappings ──────────────────────────────────────────────────────────
REGION_MAP = {
    "_Forest_": "Forest",
    "_Ash_": "Ash Heap",
    "_Cranberry_": "Cranberry Bog",
    "_Mire_": "Mire",
    "_Toxic_": "Toxic Valley",
    "_Savage_": "Savage Divide",
    "_SavageDivide_": "Savage Divide",
    "_Skyline_": "Skyline Valley",
    "_BurningSprings_": "Burning Springs",
    "_Generic_": "All Regions",
}

SIZE_MAP = {
    "_Small_": "Small",
    "_Medium_": "Medium",
    "_Large_": "Large",
}

# ── Axolotl monthly rotation ────────────────────────────────────────────────
AXOLOTL_MONTHS = {
    "01": "Charcoal",   "02": "Pink",      "03": "Clay",
    "04": "Dotted",     "05": "Purple",    "06": "Banded",
    "07": "Scaled",     "08": "Striped",   "09": "Shadow",
    "10": "Spotted",    "11": "Speckled",  "12": "Stone",
}

# LVLI keyword -> friendly region name (used to parse axolotl conditions)
LOC_KEYWORD_MAP = {
    "LocRegionBurningSprings":   "Burning Springs",
    "LocRegionMountain":         "Savage Divide",
    "LocRegionCranberryBog":     "Cranberry Bog",
    "LocRegionForestFloodlands": "Forest",
    "LocRegionStorm":            "Skyline Valley",
    "LocRegionSwampForest":      "Mire",
    "LocRegionMTR":              "Ash Heap",
    "LocRegionToxicValley":      "Toxic Valley",
}

# ── Local Legend locations ───────────────────────────────────────────────────
LEGEND_LOCATIONS = {
    "MawBegotten": "Big Maw",
    "WavyWillard": "Wavy Willard's Water Park",
    "OrganGrinder": "Organ Cave",
    "Deathjaw": "Hocking Hill",
    "SummerGlassGhost": "Glassed Cavern",
    "SludgeEye": "The Sludge Works",
}

# ── Seasonal fish: season index -> season name ───────────────────────────────
SEASON_INDEX = {1: "Spring", 2: "Summer", 3: "Fall", 4: "Winter"}

# Seasonal fish -> which regions they appear in (from LVLI entries data)
SEASONAL_FISH_REGIONS = {
    "Orange Overseer":  {"season": "Spring", "seasonIndex": 1,
                         "regions": ["Savage Divide", "Skyline Valley", "Toxic Valley"]},
    "Fernskipper":      {"season": "Summer", "seasonIndex": 2,
                         "regions": ["Cranberry Bog", "Mire", "Skyline Valley"]},
    "Fester Koi":       {"season": "Fall",   "seasonIndex": 3,
                         "regions": ["Ash Heap", "Toxic Valley", "Burning Springs"]},
    "Bog Sucker":       {"season": "Winter", "seasonIndex": 4,
                         "regions": ["Forest", "Ash Heap", "Savage Divide"]},
    "Glass Ghost":      {"season": "Summer", "seasonIndex": 2,
                         "regions": ["Cranberry Bog"],
                         "isLocalLegend": True,
                         "location": "Glassed Cavern"},
    "Sludge Eye":       {"season": "Fall",   "seasonIndex": 3,
                         "regions": ["Ash Heap"],
                         "isLocalLegend": True,
                         "location": "The Sludge Works"},
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def read_tsv(filepath, encoding_fallbacks=None):
    """Read a TSV file with encoding fallback."""
    if encoding_fallbacks is None:
        encoding_fallbacks = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    for enc in encoding_fallbacks:
        try:
            with open(filepath, "r", encoding=enc) as f:
                lines = f.read().strip().split("\n")
            return [line.split("\t") for line in lines]
        except (UnicodeDecodeError, IOError):
            continue
    raise ValueError(f"Could not read {filepath} with any encoding")


def find_latest_tsv(pattern):
    """Find the latest TSV file matching a pattern (by modification time)."""
    files = globmod.glob(os.path.join(TSV_DIR, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


# ---------------------------------------------------------------------------
# WHAT'S BITING — generative weather conditions + qualifying weather stations
# ---------------------------------------------------------------------------
# The weather expands on the What's Biting guide list, per fishing-weather type:
#   * natural conditions  — derived from the Fishing_IsNatural*Weather_Condition
#     CNDF records (CNDF_Export_*.tsv). Each condition tests for weather keywords
#     via GetCurrentWeatherHasKeyword; we map each keyword to a readable line.
#   * weather stations    — read from dist/weather_stations.json, which already
#     classifies every CAMP weather station into a fishing-weather category
#     (built by build_allies_pets_weather_json.py from the WTHR keywords).
# Both lists therefore auto-update as Bethesda adds/removes weather or stations.

# Weather keyword EDID -> the human line shown in the guide. Keyed by the game's
# own weather keywords so the set is data-driven; only a brand-new keyword needs
# a line added here (until then _pretty_keyword() gives a sensible fallback).
WB_KEYWORD_LABELS = {
    "s_wt_StormMistyRainy":                   "It’s misty and rainy",
    "s_wt_StormRain":                         "It’s raining",
    "s_wt_StormRainOcclusion":                "It’s heavy rain with low visibility",
    "ATX_Weather_WeatherTypeKW_ThunderStorm": "There’s a thunderstorm",
    "s_wt_StormRad":                          "There’s a radiation storm",
    "s_wt_StormNuke":                         "There’s a Nuke Zone",
    "s_wt_Sandstorm":                         "There’s a sandstorm",
}

# Which Fishing_IsNatural*Weather_Condition feeds each guide weather category,
# in the order the keyword lines should appear. No Weather is the catch-all
# fallback (any other CAMP weather), so it has no natural keyword list.
WB_NATURAL_CNDF = {
    "rainy":     ["Fishing_IsNaturalRainyWeather_Condition"],
    "nuclear":   ["Fishing_IsNaturalRadWeather_Condition",
                  "Fishing_IsNaturalNukeWeather_Condition"],
    "sandstorm": ["Fishing_IsNaturalSandstormWeather_Condition"],
    "noWeather": [],
}

# weather_stations.json fishingWeather label (may carry a trailing note in
# parentheses) -> guide category. Matched on prefix. Nuke + Rad both => nuclear.
WB_STATION_CATEGORY = [
    ("No Weather", "noWeather"),
    ("Rain",       "rainy"),
    ("Nuke Storm", "nuclear"),
    ("Rad Storm",  "nuclear"),
    ("Sandstorm",  "sandstorm"),
]

WB_CATEGORY_ORDER = ["noWeather", "rainy", "nuclear", "sandstorm"]


def _pretty_keyword(edid):
    """Fallback readable line for a weather keyword not in WB_KEYWORD_LABELS."""
    s = edid
    for pre in ("ATX_Weather_WeatherTypeKW_", "ATX_Weather_", "s_wt_Storm", "s_wt_"):
        if s.startswith(pre):
            s = s[len(pre):]
            break
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).strip()
    return "There’s " + ((s[:1].lower() + s[1:]) if s else edid) + " weather"


def _cndf_keyword_lines(cndf_rows, edid_wanted):
    """Readable lines for the natural-weather keywords tested by one CNDF record."""
    if not cndf_rows:
        return []
    header = cndf_rows[0]
    try:
        i_edid = header.index("EDID")
    except ValueError:
        i_edid = 1
    cond_idx = [i for i, h in enumerate(header) if h.startswith("Cond")]
    lines, seen = [], set()
    for row in cndf_rows[1:]:
        if len(row) <= i_edid or row[i_edid] != edid_wanted:
            continue
        for ci in cond_idx:
            if ci >= len(row):
                continue
            cell = row[ci].strip()
            if not cell:
                continue
            parts = cell.split("|")
            # parts[2] = function name, parts[5] = "<KW_EDID> [KYWD:xxxxxx]"
            if len(parts) < 6 or parts[2].strip() != "GetCurrentWeatherHasKeyword":
                continue
            kw = parts[5].split(" [")[0].strip()
            if not kw or kw in seen:
                continue
            seen.add(kw)
            lines.append(WB_KEYWORD_LABELS.get(kw) or _pretty_keyword(kw))
    return lines


def build_whats_biting_weather(cndf_file):
    """Generative weather-conditions block for the What's Biting guide.

    Returns {"order": [...], "categories": {cat: {naturalConditions, stations}}}.
    """
    cndf_rows = read_tsv(cndf_file) if cndf_file else None

    # Qualifying weather stations per category, from the committed feed.
    stations = {k: [] for k in WB_CATEGORY_ORDER}
    ws_path = os.path.join(DIST_DIR, "weather_stations.json")
    try:
        with open(ws_path, "r", encoding="utf-8") as f:
            ws = json.load(f)
        for item in ws.get("items", []):
            fw = (item.get("fishingWeather") or "").strip()
            if not fw:
                continue
            cat = next((c for pre, c in WB_STATION_CATEGORY if fw.startswith(pre)), None)
            if not cat:
                continue
            m = re.search(r"\(([^)]+)\)", item.get("displayName") or "")
            if m:
                stations[cat].append(m.group(1).strip())
    except (IOError, ValueError):
        print("[fishing] whatsBitingWeather: weather_stations.json missing — "
              "station lists skipped", file=sys.stderr)

    categories = {}
    for cat in WB_CATEGORY_ORDER:
        nat = []
        for edid in WB_NATURAL_CNDF.get(cat, []):
            nat.extend(_cndf_keyword_lines(cndf_rows, edid))
        categories[cat] = {
            "naturalConditions": nat,
            "stations": sorted(set(stations.get(cat, []))),
        }
    return {"order": WB_CATEGORY_ORDER, "categories": categories}


def extract_quoted_name(field):
    """Extract display name from a field like: EditorID "Display Name" [TYPE:ID]"""
    if not field or not field.strip():
        return None
    m = re.search(r'"([^"]+)"', field)
    if m:
        return m.group(1)
    if "[" in field:
        name = field[:field.index("[")].strip()
        if name:
            return name
    return None


def parse_axolotl_regions(lvli_entries_file):
    """Extract axolotl month-to-regions mapping from LVLI entries data."""
    rows = read_tsv(lvli_entries_file)
    header = rows[0] if rows else []
    col_idx = {}
    for i, col_name in enumerate(header):
        col_idx[col_name.strip()] = i

    edid_col = col_idx.get("LVLI_EDID", 1)
    cond_cols = []
    for c in range(1, 11):
        key = f"Cond{c}"
        if key in col_idx:
            cond_cols.append(col_idx[key])

    month_regions = {}
    for row in rows[1:]:
        if len(row) <= edid_col:
            continue
        if row[edid_col].strip() != "Fishing_LLS_FishCollection_Axolotls":
            continue

        regions = []
        month_num = None
        for ci in cond_cols:
            if ci >= len(row) or not row[ci].strip():
                continue
            cond = row[ci].strip()

            loc_match = re.search(
                r'LocationHierarchyHasKeyword\(.+?,\s*(LocRegion\w+)\s*\[', cond)
            if loc_match:
                kw = loc_match.group(1)
                friendly = LOC_KEYWORD_MAP.get(kw)
                if friendly:
                    regions.append(friendly)
                else:
                    print(f"[fishing] Warning: unknown axolotl region keyword '{kw}'",
                          file=sys.stderr)
                    regions.append(kw)
                continue

            month_match = re.search(
                r'GetGlobalValue\(.+?LCP_Fishing_Axolotl_MonthlyIndex.+?\)\s+\S+\s+([\d.]+)',
                cond)
            if month_match:
                month_num = int(float(month_match.group(1)))

        if month_num and regions:
            month_str = f"{month_num:02d}"
            month_regions[month_str] = regions

    if len(month_regions) < 12:
        print(f"[fishing] Warning: only found {len(month_regions)}/12 axolotl month-region "
              f"mappings from LVLI data", file=sys.stderr)

    return month_regions


# ── Fish EDID parser ─────────────────────────────────────────────────────────

def parse_fish_edid(edid):
    """Extract type, region, size, and other attributes from fish EDID."""
    if edid.startswith("zzz_"):
        return None

    fish_type = None
    region = None
    size = None
    is_glowing = False
    axolotl_month = None
    legend_location = None
    season = None
    is_seasonal = False

    if edid.startswith("SeasonalFish_"):
        is_seasonal = True
        if "LocalLegend" in edid:
            fish_type = "Local Legend"
            for loc_key, loc_name in LEGEND_LOCATIONS.items():
                if loc_key in edid:
                    legend_location = loc_name
                    break
        else:
            fish_type = "Seasonal"

        if "Seasonal_Spring" in edid or "_Spring" in edid:
            season = "Spring"
        elif "Seasonal_Summer" in edid or "_Summer" in edid:
            season = "Summer"
        elif "Seasonal_Fall" in edid or "_Fall" in edid:
            season = "Fall"
        elif "Seasonal_Winter" in edid or "_Winter" in edid:
            season = "Winter"

    elif edid.startswith("LTT_Fish_WaterLoggedGift"):
        fish_type = "Waterlogged Gift"
        if "Tier_03" in edid:
            size = "Large"
        elif "Tier_02" in edid:
            size = "Medium"
        elif "Tier_01" in edid:
            size = "Small"
        return {
            "type": fish_type, "region": "All Regions", "size": size,
            "isGlowing": False, "axolotlMonth": None,
            "legendLocation": None, "season": None, "isSeasonal": False,
        }

    elif "Junk_Common" in edid or "Junk_Rare" in edid:
        fish_type = "Junk"
        if "Rare" in edid:
            size = "Rare"
        else:
            size = "Common"
    elif "LocalLegend" in edid:
        fish_type = "Local Legend"
        for loc_key, loc_name in LEGEND_LOCATIONS.items():
            if loc_key in edid:
                legend_location = loc_name
                break
    elif "Glowing" in edid:
        fish_type = "Glowing Fish"
        is_glowing = True
    elif "Axolotl" in edid:
        fish_type = "Axolotl"
        for i in range(len(edid) - 1):
            if edid[i:i+2].isdigit() and "Axolotl" in edid[:i+2]:
                axolotl_month = edid[i:i+2]
                break
    elif "Universal" in edid:
        fish_type = "Common Region Fish"
    elif "Generic" in edid:
        fish_type = "Generic Fish"
    else:
        for region_key in REGION_MAP:
            if region_key in edid:
                fish_type = "Uncommon Region Fish"
                break

    if not is_seasonal or fish_type == "Local Legend":
        for region_key, region_name in REGION_MAP.items():
            if region_key in edid:
                region = region_name
                break

    if size is None:
        for size_key, size_name in SIZE_MAP.items():
            if size_key in edid:
                size = size_name
                break

    if region is None and fish_type not in ("Local Legend", "Junk", "Seasonal", "Waterlogged Gift"):
        region = "Unknown"
    if size is None and fish_type not in ("Local Legend", "Junk"):
        size = "Unknown"

    return {
        "type": fish_type,
        "region": region,
        "size": size,
        "isGlowing": is_glowing,
        "axolotlMonth": axolotl_month,
        "legendLocation": legend_location,
        "season": season,
        "isSeasonal": is_seasonal,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    try:
        fish_file = find_latest_tsv("FISH_Export_*.tsv")
        glob_file = find_latest_tsv("GLOB_Export_*.tsv")
        lvli_entries_file = find_latest_tsv("LVLI_Export_*_LVLI_Entries.tsv")
        lvli_list_file = find_latest_tsv("LVLI_Export_*_LVLI_List.tsv")

        for label, f in [("FISH", fish_file), ("GLOB", glob_file),
                         ("LVLI_Entries", lvli_entries_file), ("LVLI_List", lvli_list_file)]:
            if not f:
                print(f"[fishing] No {label} TSV found", file=sys.stderr)
                sys.exit(1)

        fish_rows = read_tsv(fish_file)
        glob_rows = read_tsv(glob_file)

        # Extract axolotl month-to-region mapping from LVLI data
        axolotl_month_regions = parse_axolotl_regions(lvli_entries_file)
        if axolotl_month_regions:
            print(f"[fishing] Axolotl regions extracted from LVLI: "
                  f"{len(axolotl_month_regions)} months")
            for m in sorted(axolotl_month_regions):
                print(f"  Month {m} ({AXOLOTL_MONTHS.get(m, '?')}): "
                      f"{', '.join(axolotl_month_regions[m])}")
        else:
            print("[fishing] Warning: no axolotl regions found in LVLI",
                  file=sys.stderr)

        fish_header = fish_rows[0] if fish_rows else []
        col_idx = {}
        for i, col_name in enumerate(fish_header):
            col_idx[col_name.strip()] = i
        firi_col = col_idx.get("FIRI", 16)
        full_col = col_idx.get("FULL", 2)

        fish_data = {}
        all_regions = set()
        has_burning_springs = False

        for row in fish_rows[1:]:
            if len(row) < 3:
                continue
            form_id = row[0].strip()
            edid = row[1].strip()
            firi_val = row[firi_col].strip() if len(row) > firi_col else ""
            full_val = row[full_col].strip() if len(row) > full_col else ""
            full = firi_val if firi_val else full_val

            if not form_id or not edid:
                continue

            parsed = parse_fish_edid(edid)
            if parsed is None or parsed["type"] is None:
                continue

            name = extract_quoted_name(full) or edid

            entry = {
                "formId": form_id,
                "edid": edid,
                "name": name,
                "type": parsed["type"],
                "region": parsed["region"],
                "size": parsed["size"],
                "isGlowing": parsed["isGlowing"],
                "month": None,
                "monthRegions": None,
                "legendLocation": parsed["legendLocation"],
                "season": parsed.get("season"),
                "isSeasonal": parsed.get("isSeasonal", False),
            }

            if parsed["axolotlMonth"]:
                entry["month"] = parsed["axolotlMonth"]
                mn = parsed["axolotlMonth"]
                if mn in AXOLOTL_MONTHS:
                    entry["monthVariant"] = AXOLOTL_MONTHS[mn]
                if mn in axolotl_month_regions:
                    entry["monthRegions"] = axolotl_month_regions[mn]

            if parsed.get("isSeasonal") and name in SEASONAL_FISH_REGIONS:
                sf = SEASONAL_FISH_REGIONS[name]
                entry["season"] = sf["season"]
                entry["seasonIndex"] = sf["seasonIndex"]
                entry["seasonRegions"] = sf["regions"]
                if sf.get("isLocalLegend"):
                    entry["type"] = "Local Legend"
                    entry["legendLocation"] = sf.get("location")
                    entry["isSeasonal"] = True

            if entry["region"] and entry["region"] not in ("Unknown", "All Regions"):
                all_regions.add(entry["region"])
                if entry["region"] == "Burning Springs":
                    has_burning_springs = True
            if entry.get("monthRegions"):
                for r in entry["monthRegions"]:
                    all_regions.add(r)
            if entry.get("seasonRegions"):
                for r in entry["seasonRegions"]:
                    all_regions.add(r)

            fish_data[form_id] = entry

        # Parse GLOB values
        glob_values = {}
        junk_rare_chance = 33.33
        waterlogged_gift_odds = 15.0
        weekend_seasonal_fish_odds = 30.0

        for row in glob_rows[1:]:
            if len(row) < 3:
                continue
            edid = row[1].strip()
            try:
                value = float(row[2].strip())
            except (ValueError, IndexError):
                continue

            if edid == "Fishing_Odds_Junk_Rare":
                junk_rare_chance = value
            elif edid == "LTT_Odds_WaterLoggedGifts":
                waterlogged_gift_odds = value
            elif edid == "LTT_WeekendSeasonalFish_Odds":
                weekend_seasonal_fish_odds = value
            elif edid.startswith("Fishing_Odds_"):
                parts = edid.split("_")
                if len(parts) >= 5:
                    weather = parts[2]
                    bait = parts[3]
                    fish_type = "_".join(parts[4:])
                    glob_values[f"{weather}_{bait}_{fish_type}"] = value

        # Build cascade spawn rates
        weathers = ["No", "Rain", "Nuke"]
        baits = ["Common", "Improved", "Superb"]
        spawn_rates = {}
        spawn_rates_no_legend = {}

        for weather_short in weathers:
            wk = {"No": "NoWeather", "Rain": "RainWeather",
                  "Nuke": "NukeWeather"}[weather_short]
            spawn_rates[weather_short] = {}
            spawn_rates_no_legend[weather_short] = {}

            for bait in baits:
                bk = {"Common": "CommonBait", "Improved": "ImprovedBait",
                      "Superb": "SuperbBait"}[bait]

                if bait == "Common":
                    steps = [
                        ("Local Legend Fish", f"{wk}_{bk}_LocalLegendFish"),
                        ("Glowing Fish",     f"{wk}_{bk}_GlowingFish"),
                        ("Uncommon Region Fish", f"{wk}_{bk}_UnCommonRegionFish"),
                        ("Common Region Fish",   f"{wk}_{bk}_CommonRegionFish"),
                        ("Generic Fish",         f"{wk}_{bk}_GenericFish"),
                        ("Junk", None),
                    ]
                elif bait == "Improved":
                    steps = [
                        ("Local Legend Fish", f"{wk}_{bk}_LocalLegendFish"),
                        ("Glowing Fish",     f"{wk}_{bk}_GlowingFish"),
                        ("Axolotl",          f"{wk}_{bk}_Axolotl"),
                        ("Uncommon Region Fish", f"{wk}_{bk}_UnCommonRegionFish"),
                        ("Common Region Fish",   f"{wk}_{bk}_CommonRegionFish"),
                        ("Generic Fish",         f"{wk}_{bk}_GenericFish"),
                    ]
                else:
                    steps = [
                        ("Local Legend Fish", f"{wk}_{bk}_LocalLegendFish"),
                        ("Glowing Fish",     f"{wk}_{bk}_GlowingFish"),
                        ("Axolotl",          f"{wk}_{bk}_Axolotl"),
                        ("Uncommon Region Fish", None),
                    ]

                rates = {}
                cum_fail = 1.0
                for i, (step_name, glob_key) in enumerate(steps):
                    is_last = (i == len(steps) - 1)
                    if glob_key and glob_key in glob_values:
                        raw = glob_values[glob_key] / 100.0
                    else:
                        raw = 1.0
                    if is_last and not glob_key:
                        effective = cum_fail * 100.0
                    else:
                        effective = raw * cum_fail * 100.0
                    rates[step_name] = round(effective, 2)
                    if not is_last or glob_key:
                        cum_fail *= (1.0 - raw)

                rates_nl = {}
                filtered = [(sn, gk) for sn, gk in steps
                            if sn != "Local Legend Fish"]
                cum_fail_nl = 1.0
                for i, (step_name, glob_key) in enumerate(filtered):
                    is_last = (i == len(filtered) - 1)
                    if glob_key and glob_key in glob_values:
                        raw = glob_values[glob_key] / 100.0
                    else:
                        raw = 1.0
                    if is_last and not glob_key:
                        effective = cum_fail_nl * 100.0
                    else:
                        effective = raw * cum_fail_nl * 100.0
                    rates_nl[step_name] = round(effective, 2)
                    if not is_last or glob_key:
                        cum_fail_nl *= (1.0 - raw)

                spawn_rates[weather_short][bait] = rates
                spawn_rates_no_legend[weather_short][bait] = rates_nl

        # Build axolotl rotation
        axolotl_rotation = []
        for month_num in sorted(AXOLOTL_MONTHS.keys()):
            axolotl_rotation.append({
                "month": int(month_num),
                "variant": AXOLOTL_MONTHS[month_num],
                "regions": axolotl_month_regions.get(month_num, []),
            })

        # Build seasonal fish data
        seasonal_fish_list = []
        for name, sf in SEASONAL_FISH_REGIONS.items():
            seasonal_fish_list.append({
                "name": name,
                "season": sf["season"],
                "seasonIndex": sf["seasonIndex"],
                "regions": sf["regions"],
                "isLocalLegend": sf.get("isLocalLegend", False),
                "location": sf.get("location"),
            })

        # Build waterlogged gift data
        waterlogged_gifts = {
            "odds": round(waterlogged_gift_odds, 2),
            "tiers": [
                {"bait": "Common",   "name": "Small Waterlogged Gift",  "size": "Small"},
                {"bait": "Improved", "name": "Waterlogged Gift",        "size": "Medium"},
                {"bait": "Superb",   "name": "Large Waterlogged Gift",  "size": "Large"},
            ],
        }

        # Weekend seasonal fish event data
        weekend_seasonal = {
            "odds": round(weekend_seasonal_fish_odds, 2),
            "baitPools": {
                "Common":   {"seasonalUncommon": 110, "genericMedium": 150, "junk": 100},
                "Improved": {"seasonalUncommon": 60,  "sawgills": 40},
                "Superb":   {"seasonalUncommon": 100},
            },
        }

        # Assemble output
        fish_list = list(fish_data.values())

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "globs": glob_values,
            "fish": fish_list,
            "spawnRates": spawn_rates,
            "spawnRatesNoLegend": spawn_rates_no_legend,
            "regions": sorted(list(all_regions)),
            "junkRareChance": round(junk_rare_chance, 2),
            "axolotlRotation": axolotl_rotation,
            "seasonalFish": seasonal_fish_list,
            "waterloggedGifts": waterlogged_gifts,
            "weekendSeasonalFish": weekend_seasonal,
        }

        # Generative "What's Biting" public-events list (Improved Bait sources).
        wb_events = build_whats_biting_public_events()
        if wb_events:
            output["whatsBiting"] = {"publicEvents": wb_events}

        # Generative weather conditions + qualifying weather stations.
        cndf_file = find_latest_tsv("CNDF_Export_*.tsv")
        output["whatsBitingWeather"] = build_whats_biting_weather(cndf_file)

        os.makedirs(DIST_DIR, exist_ok=True)
        with open(DIST_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        print(f"[fishing] OK: {len(fish_list)} fish, {len(all_regions)} regions"
              + (", Burning Springs detected" if has_burning_springs else "")
              + f", waterlogged odds {waterlogged_gift_odds}%"
              + f", seasonal odds {weekend_seasonal_fish_odds}%"
              + (f", {len(wb_events)} public events" if wb_events else "")
              + f" -> dist/fishing.json")

    except Exception as e:
        print(f"[fishing] Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
   
        sys.exit(1)


if __name__ == "__main__":
    main()
