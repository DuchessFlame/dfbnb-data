#!/usr/bin/env python3
"""
src/build_seasonal_fish_guide_json.py
-------------------------------------
Builds dist/seasonal_fish_guide.json -- the data feed for the DF/BNB
"Seasonal Fish" GUIDE page (df-bnb-fishing.js, seasonal-fish view).

This is intentionally separate from dist/seasonal-fish.json, which is the
lightweight feed the home-page events calendar consumes. This guide feed is
much richer: one record per seasonal fish with regions, size, the raw meal
item, the filet/base ingredient it produces, every cooked recipe for the
season, the lifetime challenges that track it, and catch-rate / bait notes.

Sources (latest dated export of each, by Month_Year in the filename):
  - FISH_Export_*.tsv   seasonal fish records (season keyword, size, regions
                        via Fishing_LLS_FishCollection_<Region>_Uncommon refs)
  - ALCH_Export_*.tsv   raw meal items (value, "Medium Spring Fish" subtitle)
                        and cooked seasonal meals (SeasonalFish_Meal_<Season>)
  - COBJ_Export_*.tsv   crafting recipes (ingredients, station, unlock)
  - CHAL_Export_*.tsv   lifetime "Catch ... Seasonal Fish" challenges

No external dependencies -- stdlib only. Mirrors the read_tsv / find_latest
conventions used by src/build_fishing_calculator_json.py.
"""

import json
import os
import re
import sys
import glob
from datetime import datetime, timezone
import tsv_source          # one resolver for every export selection

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")
DIST_FILE = os.path.join(DIST_DIR, "seasonal_fish_guide.json")

# Lifetime-challenge rewards (Atoms / Score) are NOT carried in the CHAL export
# (ENAM is empty on every lifetime challenge) and the seasonal fishing challenges
# have no GMRW link either, so the reward label cannot be derived from the TSVs.
# This small hand-maintained file lets the reward be shown next to each challenge.
# Key = challenge EDID, value = reward label string (e.g. "Atoms", "Score",
# "50 Score"). Edit it once and it persists across rebuilds. Missing keys fall
# back to a generic "Atoms" label for lifetime challenges.
REWARD_OVERRIDES_FILE = os.path.join(SCRIPT_DIR, "fishing_challenge_rewards.json")


def load_reward_overrides():
    try:
        with open(REWARD_OVERRIDES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(k): str(v) for k, v in (data.get("rewards", data)).items()}
    except (IOError, ValueError):
        return {}


def reward_for_challenge(edid, overrides):
    """Reward label for a challenge: override if present, else a sensible default."""
    if edid in overrides:
        return overrides[edid]
    # Lifetime challenges pay Atoms or Score; without data we default to Atoms and
    # let the overrides file correct any that actually pay Score.
    if edid.startswith("Challenge_Lifetime_"):
        return "Atoms"
    return None

# Season index <-> name (verified against the in-game Fishing Report notes)
SEASON_ORDER = ["spring", "summer", "fall", "winter"]
SEASON_LABEL = {"spring": "Spring", "summer": "Summer", "fall": "Fall", "winter": "Winter"}
SEASON_INDEX = {"spring": 1, "summer": 2, "fall": 3, "winter": 4}
SEASON_MONTHS = {"spring": [3, 4, 5], "summer": [6, 7, 8], "fall": [9, 10, 11], "winter": [12, 1, 2]}

# ALCH keyword formid -> season key (cooked-meal grouping)
SEASON_MEAL_KEYWORD = {
    "008B33D9": "summer",   # SeasonalFish_Meal_Summer
    "008EFCB6": "fall",     # SeasonalFish_Meal_Fall
    # Spring / Winter meal keywords do not exist yet (coming soon).
}

# FishType_<Region> keyword formid -> friendly region (used for Local Legends,
# whose regional spawn isn't expressed through the Uncommon LVLI refs).
FISHTYPE_REGION_KEYWORD = {
    "007BC5E6": "Cranberry Bog",      # Fishing_FishType_CranberryBog
    "007BC5EA": "Ash Heap",           # Fishing_FishType_AshHeap
}

# Specific spots for the SEASONAL Local Legends (the named fishing hole), derived
# from the GetInCurrentLocation(...) conditions on each entry of the
# Fishing_LLS_FishCollection_LocalLegends LVLI (00804F7F), cross-checked against
# LCTN_Export. The legend only bites at this one spot, during its season.
#   Glass Ghost -> LocCranberryGlassedCavernLocation (00081B25) = Glassed Cavern
#   Sludge Eye  -> LocMTRSludgeWorksLocation         (00553509) = The Sludge Works
# NOTE: the full-time Local Legends (Maw Begotten/Big Maw, Wavy Willard/Wavy
# Willard's Water Park, Organ Grinder/Organ Cave, Deathjaw/Ash Cave) sit in the
# SAME LVLI pool but are NOT seasonal -- they are intentionally kept out of this
# guide for now (see legendsNote in catchInfo).
LEGEND_LOCATION = {
    "SeasonalFish_Fish_LocalLegend_SummerGlassGhost": "Glassed Cavern",
    "SeasonalFish_Fish_LocalLegend_SludgeEye": "The Sludge Works",
}

# Region keyword (in the Uncommon LVLI editor id) -> display name.
REGION_LVLI_TO_DISPLAY = {
    "Forest": "The Forest",
    "Ash": "Ash Heap",
    "Cranberry": "Cranberry Bog",
    "Mire": "The Mire",
    "Toxic": "Toxic Valley",
    "SavageDivide": "Savage Divide",
    "Savage": "Savage Divide",
    "Skyline": "Skyline Valley",
    "BurningSprings": "Burning Springs",
}

# Friendly names for the handful of crafting ingredients used by seasonal meals.
INGREDIENT_NAMES = {
    "c_Wood": "Wood",
    "CookingOil": "Cooking Oil",
    "Cooking_Razorgrain_Flour": "Razorgrain Flour",
    "TatoVegetableFruit": "Tato",
    "CarrotVegetable": "Carrot",
    "CookingFlavor_Salt": "Salt",
    "CookingFlavor_Pepper": "Pepper",
    "CookingFlavor_Spices": "Spices",
    "WaterBoiled": "Boiled Water",
    "DeathclawEgg": "Deathclaw Egg",
    "Cream": "Cream",
    "SwampPlantCookedTofu": "Cooked Tofu",
    "VegetableSlipperCactus": "Slipper Cactus",
    "Fish_Fishbits": "Fish Bits",
    "SeasonalFish_Meal_SummerFillet": "Summer Filet",
    "SeasonalFish_Meal_SpringFillet": "Spring Filet",
    "SeasonalFish_Meal_WinterFillet": "Winter Filet",
    "SeasonalFish_Meal_FallEelgrass": "Fall Sour Eelgrass",
}

# Season rollover schedule -- first Tuesday strictly after the NA equinox/solstice.
# (Kept in sync with dist/seasonal-fish.json; the renderer uses this to show the
# current season and each fish's next active window.)
ROLLOVER_DATES = [
    {"date": "2024-03-26", "season": "spring"}, {"date": "2024-06-25", "season": "summer"},
    {"date": "2024-09-24", "season": "fall"},   {"date": "2024-12-24", "season": "winter"},
    {"date": "2025-03-25", "season": "spring"}, {"date": "2025-06-24", "season": "summer"},
    {"date": "2025-09-23", "season": "fall"},   {"date": "2025-12-23", "season": "winter"},
    {"date": "2026-03-24", "season": "spring"}, {"date": "2026-06-23", "season": "summer"},
    {"date": "2026-09-29", "season": "fall"},   {"date": "2026-12-22", "season": "winter"},
    {"date": "2027-03-23", "season": "spring"}, {"date": "2027-06-22", "season": "summer"},
    {"date": "2027-09-28", "season": "fall"},   {"date": "2027-12-28", "season": "winter"},
    {"date": "2028-03-21", "season": "spring"}, {"date": "2028-06-27", "season": "summer"},
    {"date": "2028-09-26", "season": "fall"},   {"date": "2028-12-26", "season": "winter"},
    {"date": "2029-03-27", "season": "spring"}, {"date": "2029-06-26", "season": "summer"},
    {"date": "2029-09-25", "season": "fall"},   {"date": "2029-12-25", "season": "winter"},
    {"date": "2030-03-26", "season": "spring"}, {"date": "2030-06-25", "season": "summer"},
    {"date": "2030-09-24", "season": "fall"},   {"date": "2030-12-24", "season": "winter"},
]

MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


# ----------------------------- helpers -----------------------------

def read_tsv(filepath):
    """Read a TSV file with encoding fallback; returns list of column-lists."""
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                text = f.read()
            return [line.split("\t") for line in text.replace("\r\n", "\n")
                    .replace("\r", "\n").split("\n") if line.strip()]
        except (UnicodeDecodeError, IOError):
            continue
    raise ValueError("Could not read " + filepath + " with any encoding")


def _date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def find_latest_tsv(pattern):
    """Latest base TSV matching pattern, by the date embedded in the filename.

    Prefers the plain '<Name>_<Month>_<Year>.tsv' export over suffixed siblings
    like '..._Effects.tsv' or '..._LVLI_Entries.tsv'.
    """
    files = glob.glob(os.path.join(TSV_DIR, pattern))
    if not files:
        return None
    base = [f for f in files if re.search(r"_[A-Za-z]+_\d{4}\.tsv$", os.path.basename(f))]
    return sorted(base or files, key=_date_key)[-1]


def header_index(rows):
    return {h.strip(): i for i, h in enumerate(rows[0])} if rows else {}


def cell(row, idx):
    return row[idx].strip() if idx is not None and len(row) > idx else ""


def quoted(s):
    m = re.search(r'"([^"]+)"', s or "")
    return m.group(1) if m else ""


def first_int(values):
    for v in values:
        v = (v or "").strip()
        if re.fullmatch(r"\d+", v):
            return int(v)
    return None


def pretty_ingredient(edid):
    if edid in INGREDIENT_NAMES:
        return INGREDIENT_NAMES[edid]
    name = re.sub(r"^(c_|Cooking_|CookingFlavor_|Vegetable|Seasonal)", "", edid)
    name = re.sub(r"(Vegetable|Fruit|Herb|Cooked|Raw)$", "", name)
    name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name).strip()
    return name or edid


# ----------------------------- parsers -----------------------------

def parse_seasonal_fish(fish_rows):
    """Every live SeasonalFish_Fish_* record, keyed by formid."""
    idx = header_index(fish_rows)
    out = {}
    for row in fish_rows[1:]:
        edid = cell(row, idx.get("EDID"))
        if not edid.startswith("SeasonalFish_Fish_") or edid.startswith("zzz"):
            continue
        form_id = cell(row, idx.get("FormID"))
        line = "\t".join(row)

        # Season + size from the keyword columns.
        season = None
        for s in SEASON_ORDER:
            if "Fishing_FishType_Seasonal_" + SEASON_LABEL[s] in line:
                season = s
                break
        size = "Small" if "Fishing_FishSize_Small" in line else \
               "Large" if "Fishing_FishSize_Large" in line else \
               "Medium" if "Fishing_FishSize_Medium" in line else "Unknown"
        is_legend = "_LocalLegend_" in edid or "Fishing_FishType_LocalLegend" in line

        # Display name + raw meal ALCH id (from the quoted "...[ALCH:xxxx]" field).
        name = quoted(line)
        alch_m = re.search(r"\[ALCH:([0-9A-Fa-f]+)\]", line)
        raw_meal_id = alch_m.group(1).upper() if alch_m else None

        # Regions: from Fishing_LLS_FishCollection_<Region>_Uncommon refs.
        regions = []
        for rm in re.finditer(r"Fishing_LLS_FishCollection_([A-Za-z]+)_Uncommon", line):
            disp = REGION_LVLI_TO_DISPLAY.get(rm.group(1))
            if disp and disp not in regions:
                regions.append(disp)
        # Local Legends don't sit in the Uncommon lists -- use the FishType region kw.
        if not regions:
            for kw_id, disp in FISHTYPE_REGION_KEYWORD.items():
                if kw_id in line and disp not in regions:
                    regions.append(disp)

        out[form_id] = {
            "formId": form_id,
            "edid": edid,
            "name": name,
            "season": season,
            "seasonLabel": SEASON_LABEL.get(season, ""),
            "size": size,
            "isLocalLegend": is_legend,
            "regions": regions,
            "location": LEGEND_LOCATION.get(edid),
            "rawMealId": raw_meal_id,
        }
    return out


def parse_alch(alch_rows):
    """Index ALCH by formid; return (by_id, by_edid, season_meals)."""
    idx = header_index(alch_rows)
    by_id, by_edid = {}, {}
    season_meals = {s: [] for s in SEASON_ORDER}
    for row in alch_rows[1:]:
        edid = cell(row, idx.get("ALCH_EDID"))
        if not edid.startswith("SeasonalFish_Meal_") or edid.startswith("zzz"):
            continue
        rec = {
            "formId": cell(row, idx.get("ALCH_FormID")).upper(),
            "edid": edid,
            "name": cell(row, idx.get("FULL")),
            "subtitle": cell(row, idx.get("DESC")),
            "value": first_int([cell(row, idx.get("Value"))]) or 0,
            "keywords": cell(row, idx.get("Keywords_Flat")),
        }
        by_id[rec["formId"]] = rec
        by_edid[edid] = rec
        # Cooked seasonal meals carry a SeasonalFish_Meal_<Season> keyword.
        if "MealTypeCooked" in rec["keywords"]:
            for kw_id, season in SEASON_MEAL_KEYWORD.items():
                if kw_id in rec["keywords"]:
                    season_meals[season].append(rec)
    return by_id, by_edid, season_meals


def parse_cobj(cobj_rows):
    """Parse seasonal recipes.

    Returns (by_output, filet_from):
      by_output  -- cooked-meal recipes keyed by their (unique) output edid.
      filet_from -- maps a raw fish-meal edid -> the filet/eelgrass it makes.
                    Several fish can share one filet output, so this can't be
                    keyed by output (that would collapse duplicates).
    """
    idx = header_index(cobj_rows)
    by_output = {}
    filet_from = {}
    for row in cobj_rows[1:]:
        edid = cell(row, idx.get("COBJ_EDID"))
        out_edid = cell(row, idx.get("CNAM_EDID"))
        if not out_edid.startswith("SeasonalFish_Meal_") or edid.startswith("zzz"):
            continue
        from_edid = cell(row, idx.get("GNAM_EDID"))

        # "FilletFrom<Fish>" recipes convert a raw fish into its season filet.
        if "FilletFrom" in edid:
            filet_from[from_edid] = {"name": cell(row, idx.get("CNAM_FULL")),
                                     "edid": out_edid}
            continue

        ingredients = []
        for part in cell(row, idx.get("FVPA")).split("|"):
            seg = part.split(":")
            if len(seg) >= 2 and seg[0]:
                try:
                    qty = int(seg[1])
                except ValueError:
                    qty = 1
                ingredients.append({"item": pretty_ingredient(seg[0]),
                                    "edid": seg[0], "qty": qty})
        by_output[out_edid] = {
            "recipeEdid": edid,
            "name": cell(row, idx.get("CNAM_FULL")),
            "outputEdid": out_edid,
            "ingredients": ingredients,
            "station": cell(row, idx.get("BNAM_FULL")) or "Cooking Station",
            "unlock": cell(row, idx.get("GNAM_FULL")),     # may be a challenge or item
            "unlockEdid": from_edid,
        }
    return by_output, filet_from


def parse_challenges(chal_rows, reward_overrides=None):
    """Live seasonal-fishing challenges grouped by season, plus the META."""
    reward_overrides = reward_overrides or {}
    idx = header_index(chal_rows)
    by_season = {s: [] for s in SEASON_ORDER}
    meta = []
    for row in chal_rows[1:]:
        edid = cell(row, idx.get("EDID"))
        if "Fishing_SeasonalFish" not in edid or edid.startswith("zzz") \
                or "_Condition" in edid or "_SUB" in edid:
            continue
        full = cell(row, idx.get("FULL"))
        count = first_int([cell(row, idx.get("TNAM")), cell(row, idx.get("CNAM"))])
        rec = {
            "edid": edid,
            "name": full,
            "tracker": cell(row, idx.get("SNAM")),
            "count": count or 1,
            "isLocalLegend": "_LocalLegend" in edid,
            "reward": reward_for_challenge(edid, reward_overrides),
        }
        if "AllSeasons_META" in edid:
            rec["count"] = count or 4
            meta.append(rec)
            continue
        for s in SEASON_ORDER:
            if SEASON_LABEL[s] in edid:
                by_season[s].append(rec)
                break
    # Sort each season: "catch one" (1) -> "catch many" -> local legend last.
    for s in by_season:
        by_season[s].sort(key=lambda c: (c["isLocalLegend"], c["count"]))
    return by_season, meta


# ----------------------------- build -----------------------------

def main():
    try:
        fish_file = find_latest_tsv("FISH_Export_*.tsv")
        alch_file = find_latest_tsv("ALCH_Export_*.tsv")
        cobj_file = find_latest_tsv("COBJ_Export_*.tsv")
        chal_file = find_latest_tsv("CHAL_Export_*.tsv")
        for label, fp in [("FISH", fish_file), ("ALCH", alch_file),
                          ("COBJ", cobj_file), ("CHAL", chal_file)]:
            if not fp:
                print("[seasonal-guide] No " + label + "_Export_*.tsv found", file=sys.stderr)
                sys.exit(1)

        fish = parse_seasonal_fish(read_tsv(fish_file))
        alch_by_id, alch_by_edid, season_meals = parse_alch(read_tsv(alch_file))
        cobj_by_output, raw_meal_edid_to_output = parse_cobj(read_tsv(cobj_file))
        reward_overrides = load_reward_overrides()
        chal_by_season, meta = parse_challenges(read_tsv(chal_file), reward_overrides)

        seasons = []
        for s in SEASON_ORDER:
            label = SEASON_LABEL[s]
            sfish = [f for f in fish.values() if f["season"] == s]
            sfish.sort(key=lambda f: (f["isLocalLegend"], f["name"]))

            fish_out = []
            for f in sfish:
                raw = alch_by_id.get(f["rawMealId"] or "")
                raw_edid = raw["edid"] if raw else None
                filet = raw_meal_edid_to_output.get(raw_edid) if raw_edid else None
                fish_out.append({
                    "name": f["name"],
                    "formId": f["formId"],
                    "edid": f["edid"],
                    "season": label,
                    "size": f["size"],
                    "isLocalLegend": f["isLocalLegend"],
                    "regions": f["regions"],
                    "location": f["location"],
                    "rawMeal": ({"name": raw["name"], "subtitle": raw["subtitle"],
                                 "value": raw["value"]} if raw else None),
                    "filet": filet,
                    "catchType": ("Local Legend (top of the cascade at its location)"
                                  if f["isLocalLegend"]
                                  else "Joins the Uncommon Region pool in-season"),
                })

            # Cooked recipes for the season (skip the raw fish-meal items).
            recipes = []
            for meal in season_meals.get(s, []):
                rec = cobj_by_output.get(meal["edid"])
                if not rec:
                    continue
                recipes.append({
                    "name": meal["name"],
                    "subtitle": meal["subtitle"],
                    "value": meal["value"],
                    "ingredients": rec["ingredients"],
                    "station": rec["station"],
                    "unlock": rec["unlock"],
                })
            recipes.sort(key=lambda r: r["name"])

            # Coming-soon flags.
            coming = []
            if not any(f["isLocalLegend"] for f in sfish):
                coming.append(label + " Local Legend -- not yet released")
            if s in ("spring", "winter"):
                coming.append(label + " cooked recipes -- only the base filet exists so far")

            seasons.append({
                "key": s,
                "label": label,
                "seasonIndex": SEASON_INDEX[s],
                "months": SEASON_MONTHS[s],
                "fish": fish_out,
                "recipes": recipes,
                "challenges": chal_by_season.get(s, []),
                "comingSoon": coming,
            })

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "source": {
                "fishTsv": os.path.basename(fish_file),
                "alchTsv": os.path.basename(alch_file),
                "cobjTsv": os.path.basename(cobj_file),
                "chalTsv": os.path.basename(chal_file),
            },
            "timezone": "America/New_York",
            "seasonRule": {
                "method": "first_tuesday_after_equinox_na",
                "description": "Each seasonal fish is catchable for its whole "
                               "season. The season rotates on the first Tuesday "
                               "strictly after the North American equinox/solstice.",
                "rolloverDates": ROLLOVER_DATES,
            },
            "weekendEvent": {
                "windowWeeks": [1, 6],
                "window": "Thursday 12:00 PM -> Monday 12:00 PM (game time)",
                "note": "A Weekender event runs in week 1 and week 6 of every "
                        "season, boosting your chance of catching seasonal fish "
                        "on top of the normal catch rates.",
            },
            "catchInfo": {
                "summary": "Seasonal fish join the Uncommon Region Fish pool in "
                           "their listed regions during their season. Local "
                           "Legends sit at the very top of the catch cascade but "
                           "only at one specific spot.",
                "baitNote": "Better bait raises your odds of the rarer rungs of "
                            "the cascade: Superb Bait gives the best shot at "
                            "Uncommon Region (seasonal) and Local Legend fish. "
                            "Improved or Superb Bait is required for Axolotls but "
                            "is not needed for seasonal fish.",
                "calculatorNote": "Use the Fishing Calculator for exact per-cast "
                                  "percentages by weather, bait and region.",
                "legendsNote": "This guide covers the SEASONAL Local Legends only "
                               "-- the ones locked to a single spot during one "
                               "season (Glass Ghost at Glassed Cavern in Summer, "
                               "Sludge Eye at The Sludge Works in Fall). The "
                               "year-round \"full-time\" Local Legends (Maw "
                               "Begotten, Wavy Willard, Organ Grinder, Deathjaw) "
                               "share the same catch pool but are covered "
                               "separately.",
            },
            "metaChallenges": meta,
            "seasons": seasons,
        }

        os.makedirs(DIST_DIR, exist_ok=True)
        with open(DIST_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        n_fish = sum(len(s["fish"]) for s in seasons)
        n_rec = sum(len(s["recipes"]) for s in seasons)
        n_chal = sum(len(s["challenges"]) for s in seasons) + len(meta)
        print("[seasonal-guide] OK -- " + str(n_fish) + " fish, " + str(n_rec)
              + " cooked recipes, " + str(n_chal)
              + " challenges written to dist/seasonal_fish_guide.json")

    except Exception as e:
        print("[seasonal-guide] Error: " + str(e), file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
