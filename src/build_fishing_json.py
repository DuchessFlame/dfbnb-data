#!/usr/bin/env python3
"""
src/build_fishing_json.py
Reads Fallout 76 fishing data from TSV files and GLOB values, computes cascading
spawn rate probabilities, and outputs fishing.json to dist/.
No external dependencies — runs on stdlib only.
"""

import json
import os
import sys
import glob
from datetime import datetime, timezone

from patchlog_utils import write_patchlog_feed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")
DIST_FILE = os.path.join(DIST_DIR, "fishing.json")

# Region mappings
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

# Size mappings
SIZE_MAP = {
    "_Small_": "Small",
    "_Medium_": "Medium",
    "_Large_": "Large",
}

# Axolotl month to variant mapping
AXOLOTL_MONTHS = {
    "01": "Charcoal",
    "02": "Pink",
    "03": "Clay",
    "04": "Dotted",
    "05": "Purple",
    "06": "Banded",
    "07": "Scaled",
    "08": "Striped",
    "09": "Shadow",
    "10": "Spotted",
    "11": "Speckled",
    "12": "Stone",
}

# Axolotl month to regions mapping (hardcoded)
AXOLOTL_MONTH_REGIONS = {
    "01": ["Skyline Valley", "Savage Divide"],  # Charcoal
    "02": ["Cranberry Bog", "Forest"],  # Pink
    "03": ["Skyline Valley", "Toxic Valley"],  # Clay
    "04": ["Mire", "Ash Heap"],  # Dotted
    "05": ["Skyline Valley", "Cranberry Bog"],  # Purple
    "06": ["Mire", "Toxic Valley"],  # Banded
    "07": ["Ash Heap", "Forest"],  # Scaled
    "08": ["Mire", "Skyline Valley"],  # Striped
    "09": ["Ash Heap", "Toxic Valley"],  # Shadow
    "10": ["Toxic Valley", "Savage Divide"],  # Spotted
    "11": ["Forest", "Cranberry Bog"],  # Speckled
    "12": ["Ash Heap", "Toxic Valley"],  # Stone
}

# Local Legend locations (hardcoded)
LEGEND_LOCATIONS = {
    "MawBegotten": "Big Maw",
    "WavyWillard": "Wavy Willard's Water Park",
    "OrganGrinder": "Organ Cave",
    "Deathjaw": "Hocking Hill",
}


def read_tsv(filepath, encoding_fallbacks=None):
    """Read a TSV file with encoding fallback."""
    if encoding_fallbacks is None:
        encoding_fallbacks = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    for enc in encoding_fallbacks:
        try:
            with open(filepath, "r", encoding=enc) as f:
                lines = f.read().strip().split("\n")

            # Parse TSV
            rows = []
            for line in lines:
                row = line.split("\t")
                rows.append(row)
            return rows
        except (UnicodeDecodeError, IOError):
            continue

    raise ValueError(f"Could not read {filepath} with any encoding")


def find_latest_tsv(pattern):
    """Find the latest TSV file matching a pattern."""
    files = sorted(glob.glob(os.path.join(TSV_DIR, pattern)))
    if not files:
        return None
    return files[-1]


def parse_fish_edid(edid):
    """Extract type, region, size, and other attributes from fish EDID."""
    # Check for deprecated/seasonal
    if edid.startswith("zzz_") or edid.startswith("LTT_"):
        return None

    fish_type = None
    region = None
    size = None
    is_glowing = False
    axolotl_month = None
    legend_location = None

    # Type classification
    if "Junk_Common" in edid:
        fish_type = "Junk"
    elif "Junk_Rare" in edid:
        fish_type = "Junk"
    elif "LocalLegend" in edid:
        fish_type = "Local Legend"
        # Extract legend location
        for loc_key in LEGEND_LOCATIONS:
            if loc_key in edid:
                legend_location = LEGEND_LOCATIONS[loc_key]
                break
    elif "Glowing" in edid:
        fish_type = "Glowing"
        is_glowing = True
    elif "Axolotl" in edid:
        fish_type = "Axolotl"
        # Extract month number
        for i in range(len(edid) - 1):
            if edid[i:i+2].isdigit() and "Axolotl" in edid[:i+2]:
                axolotl_month = edid[i:i+2]
                break
    elif "Universal" in edid:
        fish_type = "Common Region"
    elif "Generic" in edid:
        fish_type = "Generic"
    else:
        # Check if region is in EDID; if so, it's Uncommon Region
        for region_key in REGION_MAP:
            if region_key in edid:
                fish_type = "Uncommon Region"
                break

    # Extract region
    for region_key, region_name in REGION_MAP.items():
        if region_key in edid:
            region = region_name
            break

    # Extract size
    for size_key, size_name in SIZE_MAP.items():
        if size_key in edid:
            size = size_name
            break

    # Default region/size if not found
    if region is None and fish_type != "Local Legend":
        region = "Unknown"
    if size is None and fish_type != "Local Legend":
        size = "Unknown"

    return {
        "type": fish_type,
        "region": region,
        "size": size,
        "isGlowing": is_glowing,
        "axolotlMonth": axolotl_month,
        "legendLocation": legend_location,
    }


def extract_fish_name(edid, full_field):
    """Extract display name from FULL field or derive from EDID.

    FULL column formats:
      Fishing_Fish_Meal_Small_Raw_Generic_Redbelly "Redbelly" [ALCH:007CE4E9]
      BaseballGlove "Baseball Glove" [MISC:00059A77]
      Fishing_Fish_Meal_LocalLegend_Raw_MawBegotten "Ryl-Tkannoth, Maw-Begotten" [ALCH:00804F74]
    The display name is always the part inside double-quotes.
    """
    import re

    if full_field and full_field.strip():
        # First try: extract the quoted display name
        match = re.search(r'"([^"]+)"', full_field)
        if match:
            return match.group(1)

        # Second try: if no quotes, take text before the first bracket
        if "[" in full_field:
            name = full_field[:full_field.index("[")].strip()
            if name:
                return name

    # Fallback: derive from EDID (shouldn't normally reach here)
    if edid.startswith("Fishing_Fish_"):
        parts = edid[len("Fishing_Fish_"):].split("_")
    elif edid.startswith("Burn_Fish_"):
        parts = edid[len("Burn_Fish_"):].split("_")
    else:
        parts = edid.split("_")

    skip = {"Small", "Medium", "Large", "LocalLegend", "Generic", "Universal",
            "Junk", "Common", "Rare", "Glowing", "Meal", "Raw",
            "Forest", "Ash", "Cranberry", "Mire", "Toxic", "Savage",
            "SavageDivide", "Skyline", "BurningSprings"}
    name_parts = [p for p in parts if p not in skip and not (p.startswith("Axolotl") and len(p) > 7 and p[7:9].isdigit())]
    return " ".join(name_parts) if name_parts else edid


def main():
    """Main script execution."""
    try:
        # Find latest TSV files
        fish_file = find_latest_tsv("FISH_Export_*.tsv")
        glob_file = find_latest_tsv("GLOB_Export_*.tsv")
        lvli_entries_file = find_latest_tsv("LVLI_Export_*_LVLI_Entries.tsv")
        lvli_list_file = find_latest_tsv("LVLI_Export_*_LVLI_List.tsv")

        if not fish_file:
            print("[fishing] No FISH_Export_*.tsv found", file=sys.stderr)
            sys.exit(1)
        if not glob_file:
            print("[fishing] No GLOB_Export_*.tsv found", file=sys.stderr)
            sys.exit(1)
        if not lvli_entries_file:
            print("[fishing] No LVLI_Export_*_LVLI_Entries.tsv found", file=sys.stderr)
            sys.exit(1)
        if not lvli_list_file:
            print("[fishing] No LVLI_Export_*_LVLI_List.tsv found", file=sys.stderr)
            sys.exit(1)

        # Read TSV files
        fish_rows = read_tsv(fish_file)
        glob_rows = read_tsv(glob_file)
        lvli_entries_rows = read_tsv(lvli_entries_file)
        lvli_list_rows = read_tsv(lvli_list_file)

        # Parse FISH data
        fish_header = fish_rows[0] if fish_rows else []
        fish_data = {}
        all_regions = set()
        has_burning_springs = False

        # Build column index map from header
        col_idx = {}
        if fish_header:
            for i, h in enumerate(fish_header):
                col_idx[h.strip()] = i

        firi_col = col_idx.get("FIRI", 16)  # Display name lives in FIRI, not FULL
        full_col = col_idx.get("FULL", 2)

        for row in fish_rows[1:]:
            if len(row) < 3:
                continue

            form_id = row[0].strip()
            edid = row[1].strip()
            # Display name is in the FIRI column (has pattern: EditorID "DisplayName" [ALCH:...])
            # The FULL column (index 2) is empty for FISH records.
            firi_val = row[firi_col].strip() if len(row) > firi_col else ""
            full_val = row[full_col].strip() if len(row) > full_col else ""
            full = firi_val if firi_val else full_val

            if not form_id or not edid:
                continue

            parsed = parse_fish_edid(edid)
            if parsed is None:  # Skip deprecated
                continue

            if parsed["type"] is None:
                continue

            name = extract_fish_name(edid, full)

            fish_entry = {
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
            }

            # Handle Axolotl months
            if parsed["axolotlMonth"]:
                fish_entry["month"] = parsed["axolotlMonth"]
                month_num = parsed["axolotlMonth"]
                if month_num in AXOLOTL_MONTH_REGIONS:
                    fish_entry["monthRegions"] = AXOLOTL_MONTH_REGIONS[month_num]

            # Collect regions
            if fish_entry["region"] and fish_entry["region"] != "Unknown":
                all_regions.add(fish_entry["region"])
                if fish_entry["region"] == "Burning Springs":
                    has_burning_springs = True

            if fish_entry["monthRegions"]:
                for r in fish_entry["monthRegions"]:
                    all_regions.add(r)
                    if r == "Burning Springs":
                        has_burning_springs = True

            fish_data[form_id] = fish_entry

        # Parse GLOB data
        glob_header = glob_rows[0] if glob_rows else []
        glob_values = {}
        junk_rare_chance = 33.33

        for row in glob_rows[1:]:
            if len(row) < 3:
                continue

            form_id = row[0].strip()
            edid = row[1].strip()
            value_str = row[2].strip() if len(row) > 2 else "0"

            if not edid.startswith("Fishing_Odds_"):
                continue

            try:
                value = float(value_str)
            except ValueError:
                continue

            if edid == "Fishing_Odds_Junk_Rare":
                junk_rare_chance = value
            else:
                # Parse pattern: Fishing_Odds_{Weather}_{Bait}_{FishType}
                parts = edid.split("_")
                if len(parts) >= 5:
                    weather = parts[2]
                    bait = parts[3]
                    fish_type = "_".join(parts[4:])
                    glob_key = f"{weather}_{bait}_{fish_type}"
                    glob_values[glob_key] = value

        # Build spawn rates
        spawn_rates = {}
        spawn_rates_no_legend = {}

        weathers = ["No", "Rain", "Nuke"]
        baits = ["Common", "Improved", "Superb"]

        for weather_short in weathers:
            if weather_short == "No":
                weather_key = "NoWeather"
            elif weather_short == "Rain":
                weather_key = "RainWeather"
            else:  # Nuke
                weather_key = "NukeWeather"

            spawn_rates[weather_short] = {}
            spawn_rates_no_legend[weather_short] = {}

            for bait in baits:
                # Define cascade order for this bait
                if bait == "Common":
                    steps = [
                        ("Local Legend", f"{weather_key}_CommonBait_LocalLegendFish"),
                        ("Glowing Fish", f"{weather_key}_CommonBait_GlowingFish"),
                        ("Uncommon Region Fish", f"{weather_key}_CommonBait_UnCommonRegionFish"),
                        ("Common Region Fish", f"{weather_key}_CommonBait_CommonRegionFish"),
                        ("Generic Fish", f"{weather_key}_CommonBait_GenericFish"),
                        ("Junk", None),  # No GLOB, always fallback
                    ]
                elif bait == "Improved":
                    steps = [
                        ("Local Legend", f"{weather_key}_ImprovedBait_LocalLegendFish"),
                        ("Glowing Fish", f"{weather_key}_ImprovedBait_GlowingFish"),
                        ("Axolotl", f"{weather_key}_ImprovedBait_Axolotl"),
                        ("Uncommon Region Fish", f"{weather_key}_ImprovedBait_UnCommonRegionFish"),
                        ("Common Region Fish", f"{weather_key}_ImprovedBait_CommonRegionFish"),
                        ("Generic Fish", f"{weather_key}_ImprovedBait_GenericFish"),
                    ]
                else:  # Superb
                    steps = [
                        ("Local Legend", f"{weather_key}_SuperbBait_LocalLegendFish"),
                        ("Glowing Fish", f"{weather_key}_SuperbBait_GlowingFish"),
                        ("Axolotl", f"{weather_key}_SuperbBait_Axolotl"),
                        ("Uncommon Region Fish", None),  # Fallback, no GLOB
                    ]

                # Calculate cascading probabilities
                rates = {}
                rates_no_legend = {}

                cumulative_fail = 1.0
                for i, (step_name, glob_key) in enumerate(steps):
                    is_last_step = (i == len(steps) - 1)

                    if glob_key and glob_key in glob_values:
                        raw_chance = glob_values[glob_key] / 100.0
                    else:
                        # Fallback step: use all remaining probability
                        raw_chance = 1.0

                    if is_last_step and not glob_key:
                        # Last step with no GLOB value: capture all remaining
                        effective = cumulative_fail * 100.0
                    else:
                        effective = raw_chance * cumulative_fail * 100.0

                    rates[step_name] = round(effective, 2)

                    if not is_last_step or glob_key:
                        cumulative_fail *= (1.0 - raw_chance)

                # Calculate without Local Legend
                cumulative_fail_no_legend = 1.0
                for i, (step_name, glob_key) in enumerate(steps):
                    if step_name == "Local Legend":
                        continue

                    # Find position in filtered list
                    filtered_steps = [(sn, gk) for sn, gk in steps if sn != "Local Legend"]
                    is_last_step = (filtered_steps[-1][0] == step_name)

                    if glob_key and glob_key in glob_values:
                        raw_chance = glob_values[glob_key] / 100.0
                    else:
                        raw_chance = 1.0

                    if is_last_step and not glob_key:
                        # Last step with no GLOB value: capture all remaining
                        effective = cumulative_fail_no_legend * 100.0
                    else:
                        effective = raw_chance * cumulative_fail_no_legend * 100.0

                    rates_no_legend[step_name] = round(effective, 2)

                    if not is_last_step or glob_key:
                        cumulative_fail_no_legend *= (1.0 - raw_chance)

                spawn_rates[weather_short][bait] = rates
                spawn_rates_no_legend[weather_short][bait] = rates_no_legend

        # Build output
        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "globs": glob_values,
            "fish": list(fish_data.values()),
            "spawnRates": spawn_rates,
            "spawnRatesNoLegend": spawn_rates_no_legend,
            "regions": sorted(list(all_regions)),
            "junkRareChance": round(junk_rare_chance, 2),
        }

        # Write output
        os.makedirs(DIST_DIR, exist_ok=True)
        with open(DIST_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        # Print summary
        print(f"[fishing] OK — {len(fish_data)} fish, {len(all_regions)} regions" +
              (", Burning Springs detected" if has_burning_springs else "") +
              f" written to dist/fishing.json")

        # Generate patchlog feed
        write_patchlog_feed(
            dist_dir=DIST_DIR,
            feed_name="patchlog_latest_df_fishing.json",
            current_items=list(fish_data.values()),
            key_field="formId",
            name_field="name",
            compare_fields=["name", "size", "regions"],
            prev_json_path="dist/fishing.json",
            items_extractor=lambda d: d.get("fish", []),
        )

    except Exception as e:
        print(f"[fishing] Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
