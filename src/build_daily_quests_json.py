#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_daily_quests_json.py

Reads QUEST TSVs from tsv/ (or tsv/pts/ with --pts) and outputs a JSON file
listing every repeatable daily Pip-Boy quest.

Output:
    dist/daily_quests/daily_quests.json        (live)
    dist/pts/daily_quests/daily_quests.json    (--pts)

Usage:
    python src/build_daily_quests_json.py           # live build
    python src/build_daily_quests_json.py --pts     # PTS build
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
from pathlib import Path
import tsv_source          # one resolver for every export selection

# ── Paths ──────────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parent.parent

# ── Month ordering for filename-date sorting ───────────────────────────────

_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def _filename_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def newest(pattern: str) -> str | None:
    """Pick the most recent file matching *pattern* by filename date."""
    full_pattern = str(_REPO_ROOT / pattern)
    files = glob.glob(full_pattern)
    if not files:
        return None
    files.sort(key=lambda x: (_filename_date_key(x), os.path.basename(x)))
    return files[-1]


# ── TSV helpers ────────────────────────────────────────────────────────────

def read_tsv(path: str) -> list[dict]:
    """Read a TSV file and return a list of row dicts."""
    if not path or not os.path.isfile(path):
        return []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        return list(reader)


def pick(row: dict, *keys, default: str = "") -> str:
    """Return the first non-empty value from *keys* in *row*."""
    for k in keys:
        v = row.get(k, "")
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


# ── Filters ────────────────────────────────────────────────────────────────

CUT_PREFIXES = ("zzz", "ZZZ", "CUT", "DEL", "test_", "Test_", "Debug")

QUEST_TYPE_DAILY = "Daily"

# Quest types to skip entirely — these are events/activities, not Pip-Boy dailies
QUEST_TYPES_SKIP = {
    "Event", "Public Event", "Module", "Server", "Caravan", "Daily Ops", "Raid",
    "Primary", "Side Quest", "Secondary", "Miscellaneous", "Expedition", "None",
}


def is_cut(edid: str) -> bool:
    """Skip cut/removed content."""
    return edid.startswith(CUT_PREFIXES)


def is_internal(edid: str, full: str) -> bool:
    """Filter out internal/debug/template quests."""
    if is_cut(edid):
        return True
    if full.startswith("[") or not full.strip():
        return True
    if "Template" in edid and ("Template" in full or full.startswith("[")):
        return True
    if "Dialogue" in edid and ("Dialogue" in full or "Bot Dialogue" in full):
        return True
    if edid.startswith("TEMPLATE") or full.startswith("TEMPLATE"):
        return True
    return False


# ── Location parsing ──────────────────────────────────────────────────────

_RE_QUOTED_LOC = re.compile(r'"([^"]+)"')


def parse_location(raw: str) -> str:
    """Extract the human-readable location name from the LNAM field.

    Input formats:
        LocWhitespringTheWhitespringRefugeLocation "The Whitespring Refuge" [LCTN:006240BC]
        LocBlueRidgeShenandoahHQLocation "Blue Ridge Shenandoah HQ" [LCTN:0077383B]

    Returns the quoted name, or empty string if none found.
    """
    if not raw:
        return ""
    m = _RE_QUOTED_LOC.search(raw)
    return m.group(1) if m else ""


# ── Region mapping ────────────────────────────────────────────────────────
# Map EDID prefixes to human-readable region/faction names for subtitle info.

EDID_REGION_MAP = {
    # Wastelanders factions
    "W05_Daily_R": "Crater",
    "W05_Daily_Foundation": "Foundation",
    "W05_Daily_Photo": "Foundation / Crater",
    # Whitespring Refuge
    "XPD_Fuel_": "The Whitespring Refuge",
    "XPD_Hub_": "The Whitespring Refuge",
    "XPD_HubRE_": "The Whitespring Refuge",
    "XPD_Giuseppe_": "The Whitespring Refuge",
    # Moonshine Jamboree / Costa del Sol / Skyline Drive
    "Moon_SQ": "Costa del Sol",
    "MOON_SQ": "Costa del Sol",
    # Pioneer Scouts
    "D01C_": "Pioneer Scouts",
    # Biv
    "D01A_": "Biv E. Ridge",
    # Forest region
    "FF05_": "The Forest",
    "FFZ13_": "The Forest",
    # Savage Divide
    "SFS02_": "Savage Divide",
    "SFZ03_": "Savage Divide",
    "SFZ04_": "Savage Divide",
    "SFZ14_": "Savage Divide",
    # Cranberry Bog
    "CBZ03_": "Cranberry Bog",
    "CB05_": "Cranberry Bog",
    # Toxic Valley
    "TW004": "Toxic Valley",
    "TW010": "Toxic Valley",
    "TWZ03": "Toxic Valley",
    "TWZ11": "Toxic Valley",
    "TWZ13": "Toxic Valley",
    # Ash Heap
    "MTR04_": "Ash Heap",
    "MTRz02_": "Ash Heap",
    # The Mire
    "MTNS05_": "The Mire",
    "LC129_": "The Mire",
    # Brotherhood of Steel
    "BoSZ04": "Brotherhood of Steel",
    # Responders
    "NPE_DQ01_": "Responders",
    # Fishing
    "Fishing_": "Appalachia",
    # Decorator
    "MILE_HQ_": "Blue Ridge Shenandoah HQ",
}


def get_region(edid: str, location: str) -> str:
    """Derive region/faction from EDID prefix or location field."""
    for prefix, region in EDID_REGION_MAP.items():
        if edid.startswith(prefix):
            return region
    # Fallback: use parsed location if available
    if location:
        return location
    return ""


# ── Clean display name ────────────────────────────────────────────────────

def clean_display_name(full: str) -> str:
    """Clean up quest display names for the page.

    - Remove alias references like <Alias=Loc_AlcoholName>
    - Keep the name player-friendly
    """
    # Replace alias placeholders with friendly text
    name = re.sub(r"<Alias=[^>]+>", "...", full)
    return name.strip()


# ── Main build ─────────────────────────────────────────────────────────────

def build(pts: bool = False) -> None:
    """Read QUEST TSV, filter to daily Pip-Boy quests, output JSON."""

    if pts:
        quest_path = newest("tsv/pts/QUEST_Export_PTS_*.tsv")
        dist_dir = _REPO_ROOT / "dist" / "pts" / "daily_quests"
    else:
        quest_path = newest("tsv/QUEST_Export_*.tsv")
        dist_dir = _REPO_ROOT / "dist" / "daily_quests"

    if not quest_path:
        print(f"[daily-quests] No QUEST TSV found ({'PTS' if pts else 'live'}). Skipping.")
        return

    print(f"[daily-quests] Reading: {os.path.basename(quest_path)}")
    rows = read_tsv(quest_path)
    print(f"[daily-quests] Total QUEST rows: {len(rows)}")

    daily_quests = []

    for row in rows:
        edid = pick(row, "EDID")
        full = pick(row, "FULL - Name", "FULL")
        desc = pick(row, "DESC - Description", "DESC")
        quest_type = pick(row, "Quest Type")
        location_raw = pick(row, "LNAM - Location", "LNAM")

        # Skip internal/template/cut quests
        if is_internal(edid, full):
            continue

        # Only daily quests
        if quest_type != QUEST_TYPE_DAILY:
            continue

        # Skip Daily Ops (separate page)
        if "DailyOps" in edid or "Daily_Ops" in edid:
            continue

        # Parse location
        location = parse_location(location_raw)

        # Derive region for subtitle
        region = get_region(edid, location)

        # Clean display name
        display_name = clean_display_name(full)

        quest = {
            "formId": pick(row, "FormID"),
            "edid": edid,
            "displayName": display_name,
            "description": desc,
            "questType": quest_type,
            "location": location,
            "region": region,
        }

        daily_quests.append(quest)

    # Sort alphabetically by display name
    daily_quests.sort(key=lambda q: q["displayName"].lower())

    # Build output
    output = {
        "generated": True,
        "questCount": len(daily_quests),
        "quests": daily_quests,
    }

    dist_dir.mkdir(parents=True, exist_ok=True)
    out_path = dist_dir / "daily_quests.json"

    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, indent=2, ensure_ascii=False)

    print(f"[daily-quests] Wrote {len(daily_quests)} daily quests → {out_path}")


# ── CLI ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build Daily Pip-Boy Quests JSON")
    parser.add_argument("--pts", action="store_true", help="Build from PTS data")
    args = parser.parse_args()
    build(pts=args.pts)


if __name__ == "__main__":
    main()
