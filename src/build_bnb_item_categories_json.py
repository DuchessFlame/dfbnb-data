from __future__ import annotations

"""
build_bnb_item_categories_json.py
===================================
Generates dist/bnb-item-categories.json from the xEdit KYWD and ALCH TSV
exports. Produces categorised lists of craftable/consumable items for the
chef portal's Mules tab (item picker) and Crafting tab (alcohol / chems /
serums sections that were previously missing).

Why this exists
---------------
The Mules tab used to seed its item dropdowns from a hand-curated subset
pulled from "PC Buffs n Brew Order Log.xlsx" (see mule-defaults.json).
That list drifted out of sync with the game every patch, and chefs kept
reporting missing items when new food / chems shipped. This builder
replaces the ad-hoc curation with a deterministic category resolver
driven by the KYWD cross-reference file and a small set of keyword ->
category rules.

Data sources
------------
tsv/KYWD_Export_*_Refs.tsv     "Who references this keyword?" dump.
                               Columns: KeywordFormID, KeywordEDID,
                               RefIndex, RefFormID, RefEDID, RefSignature.
                               We filter to RefSignature == 'ALCH' and use
                               the resulting FormID set as the category
                               membership.

tsv/ALCH_Export_*.tsv          The ingestible record dump from
                               ExportALCHToTSV.pas. Columns include
                               FormID, EDID, FULL, DESC, Weight, Value,
                               Keywords_Flat, Keyword_1..N. We use it as
                               a sidecar to resolve FULL display names,
                               filter out cut/test records, and apply
                               per-category exclusion keyword checks.

The reason we need BOTH files is that KYWD_Refs tells us WHICH records
have a keyword, but not the keyword set per record — so if we want to
filter "ObjectTypeFood items that DON'T have MealTypeRaw", we either
need to walk KYWD_Refs for every exclude keyword and subtract, OR read
the per-record Keywords_Flat column from the ALCH TSV. This script does
the subtraction by cross-referencing both — see CategoryRule.resolve().

Category rules
--------------
All form IDs below come from KYWD records (verified against
KYWD_Export_Apr_2026.tsv). The source-of-truth for the user's
categorisation was a hand-written list given in chat 2026-04-11:

  food and tea:      00055ECC, 000F4AEC, 00572648, 0057264D, 0013486A,
                     0000CF91, 0051B569, 0000EE21, 0000CF93, 000F4AED,
                     00284969, 005477E0, 005477DF, 005477DE, 005477DD,
                     005477DC, 004869B3, 0029452F
                     [filter out raw ingredient keywords + alcohol kws]
  canned:            no form id list — use MealTypePackaged (0013485D)
  nuka cola:         000F4AEC, 003D3668, 0013484B   [EDID must contain Nuka]
  chems:             000F4AEB, 00450D72, 006CCCB0, 000F4AE7, 0011C58A,
                     004175DE, 004175E4, 0004C948, 000842A1, 004175EA,
                     0006FA1B, 005477E8, 005477E7, 005477E6, 005477E5,
                     005477E4, 005477E3, 005477E2, 005477E1, 003078BD
  alcohol:           000F4AEC, 00864CC6, 004878E3, 0010C416
                     [filter out teas, nuka, water, juice, soda]
  serums:            00393F7F
  prewar_candy:      0044D49C   [plus EDID suffix '_PreWar_Clean']

Global exclusions (applied to every category):
  - EDID prefix in CUT, ZZZ, ZZZZ, POST, DEL, TEST, DEBUG  (cut content)
  - Empty FULL name                                        (not a real item)
  - Raw ingredient keywords (for food only): MealTypeRaw,
    IngredientTypeMeat/Veg/Fruit/Herb                     (raw, not a dish)

Output shape
------------
dist/bnb-item-categories.json = {
  "version":       "YYYY-MM-DD",
  "generated":     "<ISO-8601 UTC>",
  "source_kywd":   "KYWD_Export_Apr_2026_Refs.tsv",
  "source_alch":   "ALCH_Export_Apr_2026.tsv",
  "alch_record_count": 982,
  "categories": {
    "food":     [ {edid, name, form_id, weight, value, keywords, ...}, ... ],
    "canned":   [...],
    "nuka_cola":[...],
    "chems":    [...],
    "alcohol":  [...],
    "serums":   [...],
    "prewar_candy": [...]
  },
  "stats": {
    "food":     {"included": 137, "excluded_cut": 2, "excluded_raw": 41, ...},
    ...
  }
}

Each category entry is sorted A-Z by display name (FULL). Ingredient
resolution is NOT embedded here — the portal's Crafting tab already
looks up ingredients from cobj-recipes.json by display name, and the
Mules tab doesn't need ingredient data at all. Keeping the ingredient
linkage in cobj-recipes.json means we only have one source of truth for
"what does crafting Item X require".

Usage
-----
  python build_bnb_item_categories_json.py
  python build_bnb_item_categories_json.py --data-dir /path/to/tsvs --outdir /path/to/dist

Exit codes:
  0  success, file written
  1  no input TSVs found (category file NOT written)
  2  serialisation / write error (atomic write aborted, old file intact)
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from diagnostics import Diagnostics  # noqa: E402
except Exception:  # pragma: no cover - diagnostics is optional at runtime
    Diagnostics = None  # type: ignore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# EDID prefixes that always mean "cut content / test / debug / removed".
# Matched case-insensitively against the start of the record's EDID.
CUT_PREFIXES = ("CUT", "ZZZ", "ZZZZ", "POST", "DEL", "TEST", "DEBUG")

# Form IDs are always upper-case 8-char hex strings in this module. xEdit
# exports sometimes lower-case them; we normalise on read.
def norm_fid(s: str) -> str:
    s = (s or "").strip().upper()
    # Strip any stray '0x' prefix or zero-padding noise
    if s.startswith("0X"):
        s = s[2:]
    return s.zfill(8)[-8:]


# Keywords that mean "this is a raw ingredient, not a completed dish".
# Food list excludes any record with ANY of these keywords, so the list
# surfaces only things like "Blamco Mac and Cheese" not "Razorgrain".
RAW_INGREDIENT_KEYWORDS: Set[str] = {
    norm_fid("00134858"),  # MealTypeRaw
    norm_fid("0013486B"),  # IngredientTypeMeat
    norm_fid("002944CE"),  # IngredientTypeVegetable
    norm_fid("002944D1"),  # IngredientTypeFruit
    norm_fid("002944CF"),  # IngredientTypeHerb
    norm_fid("002DBF89"),  # IngredientTypeDiseaseRidden_New
    norm_fid("002DBF8A"),  # IngredientTypeIrradiated_New
}


# ---------------------------------------------------------------------------
# Category definitions
# ---------------------------------------------------------------------------

# Each category is a dict with:
#   "include_kywds":  list[str]   (form IDs — a record needs ANY of these)
#   "exclude_kywds":  list[str]   (optional — kill any record with ANY of these)
#   "exclude_raw":    bool        (if True, also kill any raw ingredient)
#   "edid_substring": str         (optional — EDID must contain this, case-insens.)
#   "edid_regex":     str         (optional — EDID must match this regex)

CATEGORIES: Dict[str, Dict[str, Any]] = {
    "food": {
        "include_kywds": [
            "00055ECC",  # ObjectTypeFood
            "000F4AEC",  # ObjectTypeDrink  (umbrella incl. tea/water — filtered)
            "00572648",  # ObjectTypeCanSpoilDUPLICATE000
            "0057264D",  # ObjectTypeCanSpoil
            "0013486A",  # DrinkTypeWaterToxic
            "0000CF91",  # DrinkTypeWaterPurified
            "0051B569",  # DrinkTypeWaterFEV
            "0000EE21",  # DrinkTypeWaterDirty
            "0000CF93",  # DrinkTypeWaterBoiled
            "000F4AED",  # DrinkTypeWater
            "00284969",  # DrinkTypeTea
            "005477E0", "005477DF", "005477DE", "005477DD", "005477DC",
            "004869B3",  # ChemTypeFury/DayTripper/DaddyO/Calmex/Bufftats/Buffout
            "0029452F",  # DrinkTypeJuice
        ],
        "exclude_kywds": [
            # alcohol is its own category, keep it out of food
            "0010C416",  # DrinkTypeAlcohol
            "004878E3",  # DrinkTypeLiquor
            "00864CC6",  # DrinkTypeSarsaparilla
        ],
        "exclude_raw": True,
    },
    "canned": {
        # No form-id list from the user — they asked to "go to ALCH and
        # look for canned food items". The canonical marker is the
        # MealTypePackaged keyword (e.g. Dogfood, Cram, BlamcoMac...
        # _PreWar_Clean variants) which is the game's own "this is a
        # sealed/packaged container" tag.
        "include_kywds": [
            "0013485D",  # MealTypePackaged
        ],
        "exclude_raw": True,
    },
    "nuka_cola": {
        "include_kywds": [
            "000F4AEC",  # ObjectTypeDrink  (too broad alone — see edid_substring)
            "003D3668",  # DrinkTypeSodaIcon
            "0013484B",  # DrinkTypeSoda
        ],
        # The Drink / Soda keywords cover everything soft-drink-like
        # (regular sodas, Nuka, Vim, Sunset Sarsaparilla). Narrow to
        # Nuka by requiring the EDID to contain "Nuka".
        "edid_substring": "nuka",
        "exclude_raw": True,
    },
    "chems": {
        "include_kywds": [
            "000F4AEB",  # ObjectTypeStimpak
            "00450D72",  # ObjectTypeSalve
            "006CCCB0",  # ObjectTypeRadX
            "000F4AE7",  # ObjectTypeChem
            "0011C58A",  # ObjectTypeBloodPack
            "004175DE",  # ObjectTypeAntibiotics
            "004175E4",  # ChemTypeSuperStimpak
            "0004C948",  # ChemTypeStimpack
            "000842A1",  # ChemTypeStealthBoy
            "004175EA",  # ChemTypeSkeetoSpit
            "0006FA1B",  # ChemTypeRadaway
            "005477E8", "005477E7", "005477E6", "005477E5", "005477E4",
            "005477E3", "005477E2", "005477E1",  # Psychotats..Mentats line
            "003078BD",  # ChemTypeHealing
        ],
        "exclude_kywds": [
            # Serums are their own category
            "00393F7F",  # ObjectTypeSerum
        ],
        "exclude_raw": True,
    },
    "alcohol": {
        "include_kywds": [
            "000F4AEC",  # ObjectTypeDrink (too broad — filtered below)
            "00864CC6",  # DrinkTypeSarsaparilla
            "004878E3",  # DrinkTypeLiquor
            "0010C416",  # DrinkTypeAlcohol
        ],
        "exclude_kywds": [
            # Everything drink-shaped that isn't alcohol
            "00284969",  # DrinkTypeTea
            "003D3668",  # DrinkTypeSodaIcon  (soda/nuka)
            "0013484B",  # DrinkTypeSoda
            "000F4AED",  # DrinkTypeWater
            "0013486A",  # DrinkTypeWaterToxic
            "0000CF91",  # DrinkTypeWaterPurified
            "0051B569",  # DrinkTypeWaterFEV
            "0000EE21",  # DrinkTypeWaterDirty
            "0000CF93",  # DrinkTypeWaterBoiled
            "0029452F",  # DrinkTypeJuice
        ],
        "exclude_raw": True,
    },
    "serums": {
        "include_kywds": [
            "00393F7F",  # ObjectTypeSerum
        ],
        "exclude_raw": True,
    },
    "prewar_candy": {
        "include_kywds": [
            "0044D49C",  # ObjectTypeCandy
        ],
        # Also include any pre-war clean food (BlamcoMacAndCheese_PreWar_Clean,
        # DandyBoyApples_PreWar_Clean etc.) by virtue of EDID suffix match in
        # post_include_edid_suffix — that extra pass happens in resolve_category
        # after keyword filtering.
        "post_include_edid_suffix": "_PreWar_Clean",
        "exclude_raw": True,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def today_ymd() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def clean(s: Any) -> str:
    if s is None:
        return ""
    t = str(s).strip()
    # Some xEdit exports wrap strings with stray double-quotes when they
    # contain commas — strip one layer of them.
    if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
        t = t[1:-1].strip()
    return t


def edid_is_cut(edid: str) -> bool:
    e = clean(edid).upper()
    return any(e.startswith(p) for p in CUT_PREFIXES)


def parse_keywords_flat(flat: str) -> Set[str]:
    """Return the set of keyword form IDs embedded in a Keywords_Flat cell.

    Cells look like 'ObjectTypeFood[00055ECC] | MealTypeRaw[00134858] | ...'
    We extract the 8-char hex inside the brackets and normalise.
    """
    out: Set[str] = set()
    if not flat:
        return out
    for m in re.finditer(r"\[([0-9A-Fa-f]{8})\]", flat):
        out.add(norm_fid(m.group(1)))
    return out


# ---------------------------------------------------------------------------
# Input loaders
# ---------------------------------------------------------------------------

def load_alch_records(tsv_path: str) -> Dict[str, Dict[str, Any]]:
    """Load the ALCH TSV into a dict keyed by normalised FormID.

    Each value contains everything the category resolver needs:
      edid, name, form_id, weight, value, dnam, keywords (Set[str])
    """
    records: Dict[str, Dict[str, Any]] = {}
    with open(tsv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Support both column naming conventions:
            #   Old/generic: FormID, EDID
            #   April 2026+: ALCH_FormID, ALCH_EDID
            fid = norm_fid(row.get("ALCH_FormID", "") or row.get("FormID", ""))
            if not fid:
                continue
            edid = clean(row.get("ALCH_EDID", "") or row.get("EDID", ""))
            name = clean(row.get("FULL", ""))
            desc = clean(row.get("DESC", ""))
            weight = clean(row.get("Weight", ""))
            value = clean(row.get("Value", ""))
            dnam = clean(row.get("DNAM_AddictionName", "") or row.get("DNAM", ""))
            kws = parse_keywords_flat(row.get("Keywords_Flat", ""))
            records[fid] = {
                "form_id": fid,
                "edid": edid,
                "name": name,
                "desc": desc,
                "weight": weight,
                "value": value,
                "dnam": dnam,
                "keywords": kws,
            }
    return records


def load_kywd_refs(tsv_path: str, sig_filter: Optional[Iterable[str]] = None) -> Dict[str, Set[str]]:
    """Load the KYWD_Refs TSV into a dict: keyword_fid -> set of ref_fids.

    Only rows with RefSignature in sig_filter are kept. When sig_filter
    is None we keep everything (rarely what the caller wants, but cheap).
    """
    want_sigs: Optional[Set[str]] = None
    if sig_filter is not None:
        want_sigs = {s.upper() for s in sig_filter}

    out: Dict[str, Set[str]] = {}
    with open(tsv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            sig = clean(row.get("RefSignature", "")).upper()
            if want_sigs is not None and sig not in want_sigs:
                continue
            kfid = norm_fid(row.get("KeywordFormID", ""))
            rfid = norm_fid(row.get("RefFormID", ""))
            if not kfid or not rfid:
                continue
            out.setdefault(kfid, set()).add(rfid)
    return out


def find_latest_tsv(data_dir: str, pattern: str) -> Optional[str]:
    """Return the most recent TSV matching pattern (by mtime).

    We prefer mtime over filename sort because filenames use month names
    ("March", "Apr") which don't sort lexically.
    """
    matches = glob.glob(os.path.join(data_dir, pattern))
    if not matches:
        return None
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return matches[0]


# ---------------------------------------------------------------------------
# Category resolver
# ---------------------------------------------------------------------------

def resolve_category(
    name: str,
    rule: Dict[str, Any],
    kywd_to_alch: Dict[str, Set[str]],
    alch_records: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Apply a category rule and return (items_sorted, stats)."""
    stats = {
        "candidates": 0,
        "excluded_cut": 0,
        "excluded_no_name": 0,
        "excluded_by_kywd": 0,
        "excluded_raw": 0,
        "excluded_missing_alch": 0,
        "excluded_by_edid_substring": 0,
        "included": 0,
        "post_included_via_suffix": 0,
    }

    # 1. Union all "in-set" candidates from include_kywds
    include_kywds = [norm_fid(k) for k in rule.get("include_kywds", [])]
    candidate_fids: Set[str] = set()
    for kfid in include_kywds:
        candidate_fids |= kywd_to_alch.get(kfid, set())
    stats["candidates"] = len(candidate_fids)

    # 2. Subtract anything matching exclude_kywds
    exclude_kywds = [norm_fid(k) for k in rule.get("exclude_kywds", [])]
    exclude_fids: Set[str] = set()
    for kfid in exclude_kywds:
        exclude_fids |= kywd_to_alch.get(kfid, set())
    if exclude_fids:
        before = len(candidate_fids)
        candidate_fids -= exclude_fids
        stats["excluded_by_kywd"] = before - len(candidate_fids)

    # 3. Narrow by EDID substring / regex if the rule asks for it
    edid_substring = rule.get("edid_substring")
    edid_regex = rule.get("edid_regex")
    exclude_raw = bool(rule.get("exclude_raw"))

    items: List[Dict[str, Any]] = []
    for fid in candidate_fids:
        rec = alch_records.get(fid)
        if rec is None:
            stats["excluded_missing_alch"] += 1
            continue
        if edid_is_cut(rec["edid"]):
            stats["excluded_cut"] += 1
            continue
        if not rec["name"]:
            stats["excluded_no_name"] += 1
            continue
        if exclude_raw and (rec["keywords"] & RAW_INGREDIENT_KEYWORDS):
            stats["excluded_raw"] += 1
            continue
        if edid_substring and edid_substring.lower() not in rec["edid"].lower():
            stats["excluded_by_edid_substring"] += 1
            continue
        if edid_regex and not re.search(edid_regex, rec["edid"], re.IGNORECASE):
            stats["excluded_by_edid_substring"] += 1
            continue
        items.append(_export_record(rec))

    # 4. Post-include: EDID suffix (e.g. '_PreWar_Clean'). Pulls in records
    #    that wouldn't otherwise be in the candidate set.
    post_suffix = rule.get("post_include_edid_suffix")
    if post_suffix:
        post_suffix_lc = post_suffix.lower()
        already = {it["form_id"] for it in items}
        for fid, rec in alch_records.items():
            if fid in already:
                continue
            if not rec["edid"].lower().endswith(post_suffix_lc):
                continue
            if edid_is_cut(rec["edid"]):
                continue
            if not rec["name"]:
                continue
            if exclude_raw and (rec["keywords"] & RAW_INGREDIENT_KEYWORDS):
                continue
            items.append(_export_record(rec))
            stats["post_included_via_suffix"] += 1

    stats["included"] = len(items)

    # 5. Sort A-Z by display name (stable secondary key on EDID for
    #    deterministic ordering when two items share a display name).
    items.sort(key=lambda it: (it["name"].lower(), it["edid"].lower()))

    return items, stats


def _export_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Shape an internal record dict for JSON output."""
    return {
        "form_id": rec["form_id"],
        "edid": rec["edid"],
        "name": rec["name"],
        "weight": rec["weight"],
        "value": rec["value"],
        "dnam": rec["dnam"],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Build bnb-item-categories.json from KYWD + ALCH TSV exports")
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV input dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Output dir (default: dist)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    diag = None
    if Diagnostics is not None:
        try:
            diag = Diagnostics(source="bnb_item_categories", outdir=args.outdir)
        except Exception:
            diag = None

    # --- Locate inputs ---
    kywd_refs_path = find_latest_tsv(args.data_dir, "KYWD_Export_*_Refs.tsv")
    # Exclude _Effects files — we want the base ALCH export only.
    alch_candidates = [
        p for p in glob.glob(os.path.join(args.data_dir, "ALCH_Export_*.tsv"))
        if "_Effects" not in os.path.basename(p)
    ]
    alch_candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    alch_path = alch_candidates[0] if alch_candidates else None

    if not kywd_refs_path:
        msg = f"No KYWD_Export_*_Refs.tsv found in {args.data_dir}"
        print(f"ERROR: {msg}", file=sys.stderr)
        if diag:
            diag.error("bnb_categories.input.kywd_missing", msg)
            diag.save()
        return 1

    if not alch_path:
        msg = f"No ALCH_Export_*.tsv found in {args.data_dir}"
        print(f"ERROR: {msg}", file=sys.stderr)
        if diag:
            diag.error("bnb_categories.input.alch_missing", msg)
            diag.save()
        return 1

    print(f"KYWD_Refs: {os.path.basename(kywd_refs_path)}", file=sys.stderr)
    print(f"ALCH:      {os.path.basename(alch_path)}", file=sys.stderr)

    # --- Load ---
    print("Loading KYWD refs (ALCH + FLST filter)...", file=sys.stderr)
    kywd_to_alch = load_kywd_refs(kywd_refs_path, sig_filter=["ALCH"])
    print(f"  {len(kywd_to_alch)} keywords referenced by ALCH records", file=sys.stderr)

    print("Loading ALCH records...", file=sys.stderr)
    alch_records = load_alch_records(alch_path)
    print(f"  {len(alch_records)} ALCH records", file=sys.stderr)

    # Sanity warn: if the ALCH TSV is suspiciously small the fresh file
    # hasn't synced yet. Don't abort — still produce output — but flag it
    # loudly in the diagnostics and stderr so the chef portal maintainer
    # knows to re-run the build after the sync catches up.
    if len(alch_records) < 500:
        msg = (
            f"ALCH_Export TSV only has {len(alch_records)} records. "
            "A complete FO76 export is typically 900+; the current file "
            "may be a pre-fix partial export that stopped mid-run. "
            "Output will reflect whatever records ARE in the file, so "
            "some categories may be empty or sparse. Re-run the build "
            "after the fresh TSV is in place."
        )
        print(f"WARNING: {msg}", file=sys.stderr)
        if diag:
            diag.warning("bnb_categories.alch.undersized", msg,
                         context={"records": len(alch_records), "file": os.path.basename(alch_path)})

    # --- Resolve each category ---
    categories_out: Dict[str, List[Dict[str, Any]]] = {}
    stats_out: Dict[str, Dict[str, int]] = {}
    for cat_name, rule in CATEGORIES.items():
        print(f"Resolving {cat_name}...", file=sys.stderr)
        items, stats = resolve_category(cat_name, rule, kywd_to_alch, alch_records)
        categories_out[cat_name] = items
        stats_out[cat_name] = stats
        print(
            f"  {stats['included']} items "
            f"(candidates={stats['candidates']}, "
            f"excluded_kywd={stats['excluded_by_kywd']}, "
            f"excluded_cut={stats['excluded_cut']}, "
            f"excluded_raw={stats['excluded_raw']}, "
            f"excluded_no_name={stats['excluded_no_name']}, "
            f"post_suffix={stats['post_included_via_suffix']})",
            file=sys.stderr,
        )

    # --- Assemble output ---
    output: Dict[str, Any] = {
        "version": today_ymd(),
        "generated": now_iso(),
        "source_kywd": os.path.basename(kywd_refs_path),
        "source_alch": os.path.basename(alch_path),
        "alch_record_count": len(alch_records),
        "categories": categories_out,
        "stats": stats_out,
        "category_rules": {
            # Mirror the rule set so downstream consumers can see exactly
            # which keyword ids drove each category without re-reading
            # this source file.
            name: {
                "include_kywds": [norm_fid(k) for k in rule.get("include_kywds", [])],
                "exclude_kywds": [norm_fid(k) for k in rule.get("exclude_kywds", [])],
                "exclude_raw": bool(rule.get("exclude_raw")),
                "edid_substring": rule.get("edid_substring", ""),
                "post_include_edid_suffix": rule.get("post_include_edid_suffix", ""),
            }
            for name, rule in CATEGORIES.items()
        },
    }

    output_path = os.path.join(args.outdir, "bnb-item-categories.json")

    # Atomic write (same pattern as build_cobj_recipes_json.py) — serialise
    # to a string first so a bad payload doesn't zero the destination,
    # then write-and-rename so the destination is either the old file or
    # the new one, never half-populated.
    try:
        payload = json.dumps(output, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"ERROR serialising {output_path}: {e}", file=sys.stderr)
        if diag:
            diag.error("bnb_categories.serialize.failed", "Failed to serialise payload", detail=str(e))
            diag.save()
        return 2

    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=".bnb-item-categories-",
        suffix=".json.tmp",
        dir=args.outdir,
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, output_path)
    except Exception as e:
        print(f"ERROR writing {output_path}: {e}", file=sys.stderr)
        try:
            if os.path.exists(tmp_path):
                os.replace(tmp_path, output_path + ".bad")
        except Exception:
            pass
        if diag:
            diag.error("bnb_categories.write.failed", "Failed to write output", detail=str(e))
            diag.save()
        return 2

    # Round-trip sanity check
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            json.load(f)
    except Exception as e:
        print(f"ERROR round-tripping {output_path}: {e}", file=sys.stderr)
        if diag:
            diag.error("bnb_categories.roundtrip.failed", "Output file failed JSON round-trip", detail=str(e))
            diag.save()
        return 2

    total_items = sum(len(v) for v in categories_out.values())
    print(f"Wrote {output_path} ({total_items} items across {len(categories_out)} categories)", file=sys.stderr)

    if diag:
        diag.info(
            "bnb_categories.build.summary",
            f"Built {total_items} items across {len(categories_out)} categories",
            context={
                "total_items": total_items,
                "alch_records": len(alch_records),
                "source_alch": os.path.basename(alch_path),
                "source_kywd": os.path.basename(kywd_refs_path),
                "stats": stats_out,
            },
        )
        diag.save()

    return 0


if __name__ == "__main__":
    sys.exit(main())
