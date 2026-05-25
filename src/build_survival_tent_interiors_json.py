#!/usr/bin/env python3
"""
src/build_survival_tent_interiors_json.py
==========================================
Reads the newest tsv/SurvivalTentInteriors_Export_*.tsv exported by
xEdit (!!!Wordpress - ExportSurvivalTentInteriorsToTSV.pas) and writes
dist/survival_tent_interiors.json.

The TSV has one row per PackIn storage CELL whose EDID
  - starts with "PackIn"
  - contains "SurvivalTent"
Each row has fixed metadata columns plus a variable-width section of
Item_N columns. Each Item_N cell packs five sub-fields joined by '|':
    refrFormID | baseSig | baseFormID | baseEDID | baseFULL

The JSON produced is keyed two ways so the website JS can look up by
either ENTM FormID (for direct skin -> items) or by CELL EDID (for
shared/baseline cells that several skins reuse):

  {
    "_generated":  "YYYY-MM-DDTHH:MM:SSZ",
    "_source":     "SurvivalTentInteriors_Export_<Mon>_<Year>.tsv",
    "_signatures": ["ACTI","FURN","CONT"],
    "by_entm":  { "<ENTM FormID>": <cell-obj>, ... },
    "by_cell":  { "<CELL EDID>":   <cell-obj>, ... }
  }

Each <cell-obj>:
  {
    "cellFormId":  "008EE1EC",
    "cellEdid":    "PackInF1S25SurvivalTentHuntersBlindPKINStorageCell",
    "pkinFormId":  "008EE1F4",
    "pkinEdid":    "F1_S25_SurvivalTent_HuntersBlind_PKIN",
    "entmFormId":  "008EE1F0",
    "entmEdid":    "SCORE_S25_F1_ENTM_SurvivalTent_Skin_HuntersBlind",
    "entmFull":    "Hunter's Blind Survival Tent",
    "isDev":       false,                                 // true for ZZZ_* / DUPLICATE / Copy01 cells
    "items":       [ <item>, ... ]
  }

Each <item>:
  {
    "type":        "Sleeping",                            // derived bucket
    "name":        "Cot",                                 // base record FULL
    "refrFormId":  "008EE217",
    "baseSig":     "FURN",
    "baseFormId":  "008EE1F3",
    "baseEdid":    "F1_SurvivalTent_HuntersBlind_Cot"
  }

Cells with zero items are dropped entirely.

Usage:
    python build_survival_tent_interiors_json.py
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_ROOT   = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_PATH  = os.path.join(SCRIPT_DIR, "..", "dist", "survival_tent_interiors.json")

TSV_GLOB   = "SurvivalTentInteriors_Export_*.tsv"

# Item sub-field positions inside a packed Item_N cell.
ITEM_FIELDS = ("refrFormId", "baseSig", "baseFormId", "baseEdid", "baseFull")


# ── Type buckets ────────────────────────────────────────────────────
# Derived from base record EDID. Order matters — first match wins, so
# put the more specific patterns above the broader ones.
#
# Buckets and pattern lists were chosen to reproduce the labels the
# existing hardcoded TENT_DATA used: Sleeping, Cooking, Stash Box,
# Scrap Box, Ammo Storage, Aid Box, Weapons Workbench, Armor Workbench,
# Tinker's Workbench, Punch Card Machine, Light, Decor.

TYPE_PATTERNS = [
    ("Punch Card Machine",  [r"punchcard"]),
    ("Weapons Workbench",   [r"workbench.*weapon", r"weapon.*workbench", r"workbenchweapons"]),
    ("Armor Workbench",     [r"workbench.*armor",  r"armor.*workbench",  r"workbencharmor"]),
    ("Tinker's Workbench",  [r"workbench.*tinker", r"tinker.*workbench", r"workbenchtinkers"]),
    ("Cooking",             [r"cooking", r"bbq_?grill", r"bbqgrill",
                             r"workbenchcooking", r"campfire"]),
    ("Sleeping",            [r"sleepingbag", r"sleeping_bag", r"\bcot\b",
                             r"\bbed\b", r"hammock", r"npcbed"]),
    ("Ammo Storage",        [r"ammo.?storage", r"ammostoragebox"]),
    ("Aid Box",             [r"aid.?box", r"aidbox"]),
    ("Scrap Box",           [r"scrap.?box", r"scrapbox"]),
    ("Stash Box",           [r"stash.?box", r"stashbox", r"stash_container", r"stashcontainer"]),
    ("Light",               [r"\blight", r"lamp", r"lightbulb"]),
    ("Radio",               [r"radio"]),
    ("Instrument",          [r"instrument_", r"banjo", r"guitar"]),
    ("Chair",               [r"\bchair\b", r"throne", r"asylum_chair"]),
    ("Decor",               [r"barrel", r"metalbarrel"]),
]

_COMPILED_TYPE_PATTERNS = [
    (label, [re.compile(p, re.IGNORECASE) for p in pats])
    for label, pats in TYPE_PATTERNS
]


def derive_type(base_edid: str, base_full: str) -> str:
    haystack = (base_edid or "") + " " + (base_full or "")
    for label, regexes in _COMPILED_TYPE_PATTERNS:
        for rgx in regexes:
            if rgx.search(haystack):
                return label
    return "Other"


def is_dev_cell(cell_edid: str, pkin_edid: str, entm_form: str) -> bool:
    """A cell is treated as dev/cut if its PKIN is ZZZ_*, or if its
    CELL EDID is a clearly-marked duplicate/copy AND there's no
    ENTM linking to it."""
    pe = (pkin_edid or "")
    ce = (cell_edid or "")
    if pe.startswith("ZZZ_"):
        return True
    looks_like_dup = ("DUPLICATE" in ce) or re.search(r"Copy\d+", ce)
    if looks_like_dup and not entm_form:
        return True
    return False


def parse_item_cell(cell: str) -> dict | None:
    if not cell or not cell.strip():
        return None
    parts = cell.split("|")
    # Pad to expected width so partial cells don't blow up.
    while len(parts) < len(ITEM_FIELDS):
        parts.append("")
    record = dict(zip(ITEM_FIELDS, parts[: len(ITEM_FIELDS)]))
    base_edid = record.get("baseEdid", "")
    base_full = record.get("baseFull", "")
    return {
        "type":       derive_type(base_edid, base_full),
        "name":       base_full or base_edid,
        "refrFormId": record.get("refrFormId", ""),
        "baseSig":    record.get("baseSig", ""),
        "baseFormId": record.get("baseFormId", ""),
        "baseEdid":   base_edid,
    }


def find_latest_tsv() -> str:
    pattern = os.path.join(TSV_ROOT, TSV_GLOB)
    matches = sorted(glob.glob(pattern))
    if not matches:
        print(f"[tent-interiors] No TSV matching {pattern}", file=sys.stderr)
        sys.exit(1)
    return matches[-1]


def main() -> None:
    tsv_path = find_latest_tsv()
    print(f"[tent-interiors] Reading {os.path.basename(tsv_path)}")

    by_entm: dict[str, dict] = {}
    by_cell: dict[str, dict] = {}

    total_rows   = 0
    kept_rows    = 0
    dev_rows     = 0
    dropped_zero = 0

    with open(tsv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        item_cols = [c for c in reader.fieldnames or [] if c.startswith("Item_")]

        for row in reader:
            total_rows += 1

            items_raw = [row.get(c, "") for c in item_cols]
            items = [it for it in (parse_item_cell(c) for c in items_raw) if it]

            if not items:
                dropped_zero += 1
                continue

            cell_edid  = (row.get("CELL_EDID")  or "").strip()
            pkin_edid  = (row.get("PKIN_EDID")  or "").strip()
            entm_form  = (row.get("ENTM_FormID") or "").strip()

            dev = is_dev_cell(cell_edid, pkin_edid, entm_form)
            if dev:
                dev_rows += 1

            obj = {
                "cellFormId": (row.get("CELL_FormID") or "").strip(),
                "cellEdid":   cell_edid,
                "pkinFormId": (row.get("PKIN_FormID") or "").strip(),
                "pkinEdid":   pkin_edid,
                "entmFormId": entm_form,
                "entmEdid":   (row.get("ENTM_EDID")   or "").strip(),
                "entmFull":   (row.get("ENTM_FULL")   or "").strip(),
                "isDev":      dev,
                "items":      items,
            }

            if cell_edid:
                by_cell[cell_edid] = obj
            if entm_form and not dev:
                # If two cells link to the same ENTM (rare), prefer the
                # one with more items.
                existing = by_entm.get(entm_form)
                if existing is None or len(items) > len(existing["items"]):
                    by_entm[entm_form] = obj

            kept_rows += 1

    out = {
        "_generated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source":     os.path.basename(tsv_path),
        "_signatures": ["ACTI", "FURN", "CONT"],
        "by_entm":     dict(sorted(by_entm.items())),
        "by_cell":     dict(sorted(by_cell.items())),
    }

    os.makedirs(os.path.dirname(DIST_PATH), exist_ok=True)
    with open(DIST_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"[tent-interiors] Rows: {total_rows} total, "
          f"{kept_rows} kept ({dev_rows} dev), {dropped_zero} dropped (no items)")
    print(f"[tent-interiors] by_entm: {len(by_entm)} entries")
    print(f"[tent-interiors] by_cell: {len(by_cell)} entries")
    print(f"[tent-interiors] OK -> {os.path.relpath(DIST_PATH, start=os.path.join(SCRIPT_DIR, '..'))}")


if __name__ == "__main__":
    main()
