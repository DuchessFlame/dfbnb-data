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
    """Find the latest TSV file matching a pattern."""
    files = sorted(globmod.glob(os.path.join(TSV_DIR, pattern)))
    return files[-1] if files else None


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

        os.makedirs(DIST_DIR, exist_ok=True)
        with open(DIST_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        print(f"[fishing] OK: {len(fish_list)} fish, {len(all_regions)} regions"
              + (", Burning Springs detected" if has_burning_springs else "")
              + f", waterlogged odds {waterlogged_gift_odds}%"
              + f", seasonal odds {weekend_seasonal_fish_odds}%"
              + f" -> dist/fishing.json")

    except Exception as e:
        print(f"[fishing] Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
