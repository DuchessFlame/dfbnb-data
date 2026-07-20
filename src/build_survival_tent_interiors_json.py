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
    "by_cell":  { "<CELL EDID>":   <cell-obj>, ... },
    "tents":    [ <tent>, ... ]
  }

"tents" is the FULL, generative skin list that drives
/df/atom-shop/survival-tent-skins/. It used to be a hand-maintained
TENT_DATA array inside df-bnb-atom-shop.js, which meant a new skin in the
game files never appeared on the page (live or PTS) until someone edited
the JS by hand. It is now derived from the newest ENTM_Export_*.tsv:
every record whose EDID contains "_ENTM_SurvivalTent_Skin_", minus the
ZZZ_ dev/cut ones, joined to its interior items by ENTM FormID.

Each <tent> (shape matches what the renderer consumes):
  {
    "formId":  "008B7A23",
    "edid":    "ATX_F1_ENTM_SurvivalTent_Skin_GNNVan",
    "name":    "GNN News Van (Survival Tent)",
    "desc":    "The GNN News Van is perfect for ...",   // boilerplate tail cut
    "rent":    "ATX_F1_ENTM_SurvivalTent_Skin_GNNVan",  // entitlement EDID
    "rarity":  "Superior",                              // from ItemRarity KYWD
    "items":   [ {"type","name","formId","edid"}, ... ] // base records
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
ENTM_GLOB  = "ENTM_Export_*.tsv"

# Every survival tent skin entitlement carries this in its EDID.
TENT_SKIN_MARKER = "_ENTM_SurvivalTent_Skin_"

# ── Newest-TSV picker ───────────────────────────────────────────────
# Plain alphabetical sorting is WRONG for month-name exports: "May" sorts
# after "June"/"July", so sorted(glob(...))[-1] silently reads a stale file.
# On the PTS channel this is worse — normalize_pts_tsv.py renames the newest
# PTS pull to a live-style month name, but the alphabetical pick grabbed the
# live May file instead, making dist/pts/ byte-identical to live. Sort by the
# parsed (year, month) with mtime as a tiebreaker, mirroring the newest()
# helper used by the other builders.
_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def _filename_date_key(path):
    """Extract (year, month_number) from names like *_Export_June_2026.tsv."""
    base = os.path.basename(path).lower()
    m = re.search(r"_([a-z]+)_(\d{4})", base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    return (0, 0)  # unknown → sort low so parseable dates always win

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
    # NOTE: 'campfire' deliberately omitted from Cooking — base records
    # like Moon_CampfireGuitar would otherwise mis-classify as Cooking.
    # All real survival-tent cooking stations match 'cooking'/'bbq'/etc.
    ("Cooking",             [r"cooking", r"bbq_?grill", r"bbqgrill",
                             r"workbenchcooking"]),
    ("Sleeping",            [r"sleepingbag", r"sleeping_bag", r"\bcot\b",
                             r"\bbed\b", r"hammock", r"npcbed"]),
    ("Ammo Storage",        [r"ammo.?storage", r"ammostoragebox"]),
    ("Aid Box",             [r"aid.?box", r"aidbox"]),
    ("Scrap Box",           [r"scrap.?box", r"scrapbox"]),
    ("Stash Box",           [r"stash.?box", r"stashbox", r"stash_container", r"stashcontainer"]),
    ("Light",               [r"\blight", r"lamp", r"lightbulb"]),
    ("Radio",               [r"radio"]),
    # Instrument before Chair — "CampfireGuitar" should be Instrument, not
    # caught by a stray pattern elsewhere.
    ("Instrument",          [r"instrument_", r"banjo", r"guitar", r"metalbarrel"]),
    ("Chair",               [r"\bchair\b", r"throne", r"asylum_chair",
                             r"\bbench\b", r"npcbench"]),
    ("Decor",               [r"\bbarrel\b"]),
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


def find_latest_tsv(pattern_glob: str = TSV_GLOB, required: bool = True) -> str | None:
    pattern = os.path.join(TSV_ROOT, pattern_glob)
    matches = sorted(
        glob.glob(pattern),
        key=lambda p: (_filename_date_key(p), os.path.getmtime(p)),
    )
    if not matches:
        if required:
            print(f"[tent-interiors] No TSV matching {pattern}", file=sys.stderr)
            sys.exit(1)
        return None
    return matches[-1]


# ── Tent skin list (generative) ─────────────────────────────────────
# The page's skin list is derived from ENTM, not hand-maintained. Two
# small bits of cleanup are needed to match how the skins read in game:

# Every tent DESC ends with the same store boilerplate. Cut from the
# first boilerplate sentence onward so only the flavour text survives.
_DESC_TAIL_RE = re.compile(
    r"\s*(The Survival Tent can be placed"
    r"|This offer is exclusive to Fallout 1st"
    r"|You must be an active Fallout 1st"
    r"|Available\s"
    r"|-\s*UNLOCKS A VARIANT)"
    r".*$",
    re.IGNORECASE | re.DOTALL,
)

_RARITY_RE = re.compile(r"ATX_ItemRarity_([A-Za-z]+)")


def clean_tent_desc(raw: str) -> str:
    s = (raw or "").strip()
    s = _DESC_TAIL_RE.sub("", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def rarity_from_keywords(keywords: str) -> str:
    m = _RARITY_RE.search(keywords or "")
    return m.group(1) if m else ""


def build_tents(by_entm: dict[str, dict]) -> list[dict]:
    """Enumerate every survival tent skin from the newest ENTM export and
    attach its interior items. ENTM is the source of truth for the list:
    a skin with no interior export still shows up (with no items) rather
    than silently vanishing from the page."""
    entm_path = find_latest_tsv(ENTM_GLOB, required=False)
    if not entm_path:
        print("[tent-interiors] WARNING: no ENTM_Export_*.tsv found — "
              "'tents' will be empty", file=sys.stderr)
        return []

    print(f"[tent-interiors] Reading tent skins from {os.path.basename(entm_path)}")
    tents: list[dict] = []
    skipped_dev = 0

    with open(entm_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            edid = (row.get("EDID") or "").strip()
            if TENT_SKIN_MARKER not in edid:
                continue
            if edid.startswith("ZZZ_"):
                skipped_dev += 1
                continue

            form_id = (row.get("FormID") or "").strip().upper()
            name    = (row.get("FULL") or "").strip()
            if not form_id or not name:
                continue

            cell = by_entm.get(form_id) or {}
            items = [
                {
                    "type":   it.get("type", ""),
                    "name":   it.get("name", ""),
                    "formId": it.get("baseFormId", ""),
                    "edid":   it.get("baseEdid", ""),
                }
                for it in cell.get("items", [])
            ]

            tents.append({
                "formId": form_id,
                "edid":   edid,
                "name":   name,
                "desc":   clean_tent_desc(row.get("DESC") or ""),
                "rent":   edid,
                "rarity": rarity_from_keywords(row.get("KEYWORDS") or ""),
                "items":  items,
            })

    tents.sort(key=lambda t: t["name"].lower())
    no_items = sum(1 for t in tents if not t["items"])
    print(f"[tent-interiors] tents: {len(tents)} skins "
          f"({skipped_dev} ZZZ dev skipped, {no_items} with no interior export)")
    return tents


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

    tents = build_tents(by_entm)

    out = {
        "_generated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source":     os.path.basename(tsv_path),
        "_signatures": ["ACTI", "FURN", "CONT"],
        "by_entm":     dict(sorted(by_entm.items())),
        "by_cell":     dict(sorted(by_cell.items())),
        "tents":       tents,
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
