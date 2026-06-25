#!/usr/bin/env python3
"""
src/build_axolotl_guide_json.py
-------------------------------
Builds the data feed for the DF/BNB "Axolotl Guide" page
(df-bnb-fishing.js, axolotl-guide view).

Axolotls are a Small fishing catch with a 12-slot MONTHLY rotation: a different
colour variant is available each calendar month, and each variant only spawns in
two specific regions. Axolotls require Improved or Superb Bait (Common Bait can
never hook one). All of this is derived generatively from the game-data exports:

  - FISH_Export_*.tsv          axolotl fish records (formid, size, the raw-meal
                               ALCH ref that carries the in-game display name)
  - ALCH_Export_*.tsv          raw-meal items (caps value)
  - CHAL_Export_*.tsv          the "Catch All Axolotls" lifetime META challenge
  - LVLI_Export_*_LVLI_Entries.tsv
        Fishing_LLS_FishCollection_Axolotls   -> month index + region pair per
                                                 colour (live)
        Fishing_LLS_FishCollection_GoldAxolotls -> the PTS Golden Axolotl rotation

Variant artwork is merged in from dist/axolotl-rotations.json (month -> image).

Two modes:
  (default / live)  reads tsv/      -> dist/axolotl_guide.json      (12 variants)
  --pts             reads tsv/pts/  -> dist/pts/axolotl_guide.json  (12 variants
                    + the Golden Axolotl preview block)

The global PTS toggle (df-bnb-pts.js) redirects fetch requests from dist/ to
dist/pts/, so when PTS preview is ON the renderer automatically loads the PTS
version of axolotl_guide.json without any page-specific toggle.

Env overrides (used by the in-session sandbox verifier, ignored in CI):
  AXO_TSV_DIR   override the TSV directory
  AXO_ROT_FILE  override the axolotl-rotations.json path
  AXO_OUT       override the output file path

No external dependencies -- stdlib only.
"""

import json
import os
import re
import sys
import glob
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")

PTS = "--pts" in sys.argv

# ── Region keyword -> friendly name (same mapping as the calculator builder) ──
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

MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

REWARD_OVERRIDES_FILE = os.path.join(SCRIPT_DIR, "fishing_challenge_rewards.json")


# ----------------------------- helpers -----------------------------

def tsv_dir():
    if os.environ.get("AXO_TSV_DIR"):
        return os.environ["AXO_TSV_DIR"]
    return os.path.join(SCRIPT_DIR, "..", "tsv", "pts" if PTS else "")


def read_tsv(filepath):
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
    """Newest-first sort key. Handles both '<Name>_<Month>_<Year>.tsv' (live) and
    '<Name>_PTS_<YYYY-MM-DD>_<HHMM>...tsv' (PTS) filenames; falls back to mtime."""
    name = os.path.basename(path)
    m = re.search(r"_(\d{4})-(\d{2})-(\d{2})_(\d{4})", name)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
    m = re.search(r"_([A-Za-z]+)_(\d{4})", name)
    if m:
        return (int(m.group(2)), MONTHS.get(m.group(1).lower(), 0), 0, 0)
    try:
        return (0, 0, 0, int(os.path.getmtime(path)))
    except OSError:
        return (0, 0, 0, 0)


def find_latest_tsv(pattern):
    files = glob.glob(os.path.join(tsv_dir(), pattern))
    if not files:
        return None
    # Prefer the plain dated base export (e.g. ALCH_Export_June_2026.tsv or the
    # PTS ALCH_Export_PTS_2026-06-22_1305.tsv) over suffixed siblings such as
    # '..._Effects.tsv'. If the caller's pattern only matches suffixed files
    # (e.g. '*_LVLI_Entries.tsv'), fall back to the full match list.
    base = [f for f in files if re.search(
        r"(_[A-Za-z]+_\d{4}|_\d{4}-\d{2}-\d{2}_\d{4})\.tsv$", os.path.basename(f))]
    return sorted(base or files, key=_date_key)[-1]


def header_index(rows):
    return {h.strip(): i for i, h in enumerate(rows[0])} if rows else {}


def cell(row, idx):
    return row[idx].strip() if idx is not None and len(row) > idx else ""


def first_int(values):
    for v in values:
        v = (v or "").strip()
        if re.fullmatch(r"\d+", v):
            return int(v)
    return None


def load_reward_overrides():
    try:
        with open(REWARD_OVERRIDES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(k): str(v) for k, v in (data.get("rewards", data)).items()}
    except (IOError, ValueError):
        return {}


def reward_for_challenge(edid, overrides):
    if edid in overrides:
        return overrides[edid]
    if edid.startswith("Challenge_Lifetime_"):
        return "Atoms"
    return None


def load_rotation_images():
    """month-number (int) -> artwork URL, from dist/axolotl-rotations.json."""
    path = os.environ.get("AXO_ROT_FILE") or os.path.join(DIST_DIR, "axolotl-rotations.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        out = {}
        for mk, rec in (data.get("months") or {}).items():
            try:
                out[int(mk)] = rec.get("image", "")
            except ValueError:
                pass
        return out
    except (IOError, ValueError):
        return {}


# ----------------------------- parsers -----------------------------

def parse_axolotl_fish(fish_rows):
    """formid -> {edid, name (in-game display), size, rawMealId, splash, sound,
    isGold}. Excludes the deprecated zzz_ rows."""
    idx = header_index(fish_rows)
    out = {}
    for row in fish_rows[1:]:
        edid = cell(row, idx.get("EDID"))
        if "Axolotl" not in edid or edid.startswith("zzz"):
            continue
        line = "\t".join(row)
        form_id = cell(row, idx.get("FormID")).upper()
        size = "Small" if "Fishing_FishSize_Small" in line else \
               "Large" if "Fishing_FishSize_Large" in line else \
               "Medium" if "Fishing_FishSize_Medium" in line else "Unknown"
        # The raw-meal ALCH ref carries the authoritative display name, e.g.
        #   Fishing_Fish_Meal_Small_Raw_Axolotl03_BrownAxolotl "Clay Axolotl" [ALCH:008006CF]
        nm = re.search(r'\[?ALCH:[0-9A-Fa-f]+\]?', line)
        meal_m = re.search(r'"([^"]+)"\s*\[ALCH:([0-9A-Fa-f]+)\]', line)
        name = meal_m.group(1) if meal_m else edid
        raw_meal_id = meal_m.group(2).upper() if meal_m else None
        # Gold gets a Legendary splash + sound override (special catch fanfare).
        splash = bool(re.search(r"WaterSplash_\w*Glowing", line))
        legend_snd = bool(re.search(r"LegendarySplashSoundOverride", line))
        out[form_id] = {
            "formId": form_id,
            "edid": edid,
            "name": name,
            "size": size,
            "rawMealId": raw_meal_id,
            "isGold": "_Gold" in edid or "Axolotl_Gold" in edid,
            "specialSplash": splash and ("_Gold" in edid),
            "legendarySound": legend_snd and ("_Gold" in edid),
        }
    return out


def parse_axolotl_value(alch_rows):
    """ALCH formid -> caps value (best-effort)."""
    idx = header_index(alch_rows)
    fid = idx.get("ALCH_FormID", idx.get("FormID"))
    val = idx.get("Value")
    out = {}
    for row in alch_rows[1:]:
        f = cell(row, fid).upper()
        if not f:
            continue
        out[f] = first_int([cell(row, val)]) or 0
    return out


def parse_rotation(lvli_entries_rows, list_edid):
    """month-index (int) -> {refFormId, regions[]} from a fishing axolotl LVLI."""
    idx = header_index(lvli_entries_rows)
    ei = idx.get("LVLI_EDID", 1)
    ri = idx.get("LVLO_Reference")
    cond_cols = [idx[k] for k in idx if k.startswith("Cond")]
    out = {}
    for row in lvli_entries_rows[1:]:
        if cell(row, ei) != list_edid:
            continue
        ref = cell(row, ri)
        ref_fid = ref.split(":")[0].upper() if ref else ""
        regions, month = [], None
        for ci in cond_cols:
            c = cell(row, ci)
            if not c:
                continue
            m = re.search(r"LocationHierarchyHasKeyword\(.+?,\s*(LocRegion\w+)\s*\[", c)
            if m:
                regions.append(LOC_KEYWORD_MAP.get(m.group(1), m.group(1)))
            mm = re.search(r"MonthlyIndex.+?\)\s+\S+\s+([\d.]+)", c)
            if mm:
                month = int(float(mm.group(1)))
        if month:
            out[month] = {"refFormId": ref_fid, "regions": regions}
    return out


def parse_axolotl_challenges(chal_rows, overrides):
    """The lifetime axolotl challenge(s) -- the 'Catch All Axolotls' META."""
    idx = header_index(chal_rows)
    out = []
    for row in chal_rows[1:]:
        edid = cell(row, idx.get("EDID"))
        if "Fishing_Axolotl" not in edid or edid.startswith("zzz") \
                or "_Condition" in edid or "_SUB" in edid:
            continue
        count = first_int([cell(row, idx.get("TNAM")), cell(row, idx.get("CNAM"))])
        out.append({
            "edid": edid,
            "name": cell(row, idx.get("FULL")),
            "tracker": cell(row, idx.get("SNAM")),
            "count": count or 1,
            "reward": reward_for_challenge(edid, overrides),
        })
    return out


# ----------------------------- build -----------------------------

def build_variants(rotation, fish, values, images):
    variants = []
    for month in range(1, 13):
        rot = rotation.get(month)
        if not rot:
            continue
        f = fish.get(rot["refFormId"], {})
        variants.append({
            "monthIndex": month,
            "monthName": MONTH_NAMES[month - 1],
            "name": f.get("name") or rot["refFormId"],
            "formId": rot["refFormId"],
            "edid": f.get("edid", ""),
            "size": f.get("size", "Small"),
            "regions": rot["regions"],
            "image": images.get(month, ""),
            "rawValue": values.get(f.get("rawMealId") or "", 0),
        })
    return variants


def main():
    try:
        fish_file = find_latest_tsv("FISH_Export_*.tsv")
        alch_file = find_latest_tsv("ALCH_Export_*.tsv")
        chal_file = find_latest_tsv("CHAL_Export_*.tsv")
        lvli_file = find_latest_tsv("LVLI_Export_*_LVLI_Entries.tsv")
        for label, fp in [("FISH", fish_file), ("ALCH", alch_file),
                          ("CHAL", chal_file), ("LVLI entries", lvli_file)]:
            if not fp:
                print("[axolotl-guide] No " + label + " export found in "
                      + tsv_dir(), file=sys.stderr)
                sys.exit(1)

        fish = parse_axolotl_fish(read_tsv(fish_file))
        values = parse_axolotl_value(read_tsv(alch_file))
        images = load_rotation_images()
        overrides = load_reward_overrides()
        lvli_rows = read_tsv(lvli_file)

        rotation = parse_rotation(lvli_rows, "Fishing_LLS_FishCollection_Axolotls")
        variants = build_variants(rotation, fish, values, images)
        challenges = parse_axolotl_challenges(read_tsv(chal_file), overrides)

        output = {
            "generated": datetime.now(timezone.utc).isoformat(),
            "isPts": PTS,
            "source": {
                "fishTsv": os.path.basename(fish_file),
                "alchTsv": os.path.basename(alch_file),
                "chalTsv": os.path.basename(chal_file),
                "lvliTsv": os.path.basename(lvli_file),
            },
            "timezone": "America/New_York",
            "rotationRule": {
                "type": "monthly",
                "description": "A different Axolotl colour is catchable each "
                               "calendar month. The rotation is driven by the "
                               "LCP_Fishing_Axolotl_MonthlyIndex global and "
                               "advances on the 1st of each month. Each variant "
                               "only spawns in its two listed regions that month.",
            },
            "catchInfo": {
                "summary": "Axolotls are a Small catch that sits on its own rung "
                           "of the fishing cascade, above the Uncommon Region "
                           "pool. Only the current month's colour can be caught, "
                           "and only in that variant's two regions.",
                "baitNote": "Axolotls require Improved or Superb Bait -- Common "
                            "Bait can never hook one. Superb Bait gives the best "
                            "per-cast odds.",
                "calculatorNote": "Use the Fishing Calculator for exact per-cast "
                                  "Axolotl percentages by weather, bait and region.",
            },
            "challenges": challenges,
            "variants": variants,
        }

        if PTS:
            gold = next((f for f in fish.values() if f["isGold"]), None)
            gold_rot = parse_rotation(lvli_rows, "Fishing_LLS_FishCollection_GoldAxolotls")
            gold_months = [{
                "monthIndex": m,
                "monthName": MONTH_NAMES[m - 1],
                "regions": gold_rot[m]["regions"],
            } for m in range(1, 13) if m in gold_rot]
            output["golden"] = {
                "available": bool(gold),
                "name": (gold or {}).get("name", "Gold Axolotl"),
                "formId": (gold or {}).get("formId", ""),
                "edid": (gold or {}).get("edid", ""),
                "size": (gold or {}).get("size", "Small"),
                "rawValue": values.get((gold or {}).get("rawMealId") or "", 0),
                "specialSplash": bool((gold or {}).get("specialSplash")),
                "legendarySound": bool((gold or {}).get("legendarySound")),
                "rotation": gold_months,
                "note": "PTS PREVIEW -- the Golden Axolotl is a rare, legendary "
                        "catch (special splash + fanfare) being tested on the "
                        "Public Test Server. It follows the same monthly region "
                        "rotation as the standard colours. Not yet on live.",
            }
        else:
            # Tell the live renderer a PTS preview feed exists to lazy-load.
            output["ptsPreview"] = {
                "available": True,
                "dataFile": "axolotl_guide_pts.json",
                "note": "A PTS preview (Golden Axolotl) is available via the "
                        "PTS Preview toggle.",
            }

        out_file = os.environ.get("AXO_OUT") or (
            os.path.join(DIST_DIR, "pts", "axolotl_guide.json") if PTS
            else os.path.join(DIST_DIR, "axolotl_guide.json"))
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        extra = " + Golden Axolotl" if PTS else ""
        print("[axolotl-guide] OK -- " + str(len(variants)) + " monthly variants, "
              + str(len(challenges)) + " challenge(s)" + extra
              + " -> " + os.path.basename(out_file))

    except Exception as e:
        print("[axolotl-guide] Error: " + str(e), file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
