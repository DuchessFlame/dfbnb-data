#!/usr/bin/env python3
"""
build_drop_chances_data.py
==========================
Reads xEdit TSV exports (LVLI, GLOB, CURV, item-name records) and produces
dist/drop_chances_data.json — the data file consumed by the interactive
Drop Chances calculator on buffsnbrew.com.

Output format mirrors the structure expected by df-bnb-drop-chances.js:
  {
    "meta":    { "patch": "P65", "patchLabel": "Update 65 · 20 Jan 2026", "built": "..." },
    "edids":   { "<FormID>": "<SIG><EDID> <DisplayName>", ... },
    "globals": { "<FormID>": <float>, ... },
    "curves":  { "<FormID>": [ {"x":<n>, "y":<n>}, ... ], ... },
    "lists":   { "<FormID>": { "LVLF":<int>, "LVCV":<float>, "LVLG":"<fid>",
                                "LVCT":"<fid>", "LVMV":<float>, "LVMG":"<fid>",
                                "LVMT":"<fid>", "Entries":[...], "Conditions":[...] }, ... }
  }

Usage:
  python build_drop_chances_data.py
  python build_drop_chances_data.py --tsv-root tsv --outdir dist
  python build_drop_chances_data.py --patch-label "Update 65 · 20 Jan 2026"
"""

from __future__ import annotations

import argparse
import csv
import glob as _glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import tsv_source          # one resolver for every export selection

# ---------------------------------------------------------------------------
# 0. Shared helpers (subset from rng76.py to keep this script standalone)
# ---------------------------------------------------------------------------

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

def newest(pattern: str, exclude_substrings=None) -> str:
    files = _glob.glob(pattern)
    if exclude_substrings:
        files = [f for f in files
                 if not any(s in os.path.basename(f) for s in exclude_substrings)]
    if not files:
        raise FileNotFoundError(f"No files matching {pattern}")
    files.sort(key=lambda x: (_filename_date_key(x), os.path.basename(x)))
    return files[-1]

def read_tsv(path: str) -> List[Dict[str, str]]:
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))

def pick(row: Dict[str, str], *keys: str, default: str = "") -> str:
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
    return default

def safe_float(s, default=None):
    try:
        return float(str(s).strip())
    except Exception:
        return default

def safe_int(s, default=0):
    try:
        return int(str(s).strip())
    except Exception:
        return default

def extract_formid(ref_str: str) -> str:
    """Extract FormID from strings like '0038927A:LL_ChanceNone_85_ECON:GLOB'."""
    if not ref_str:
        return ""
    return ref_str.split(":")[0].strip().upper()

def extract_edid_from_ref(ref_str: str) -> str:
    """Extract EDID from reference strings like '0038927A:LL_ChanceNone_85_ECON:GLOB'."""
    if not ref_str:
        return ""
    parts = ref_str.split(":")
    if len(parts) >= 2:
        return parts[1].strip()
    return ""

def extract_sig_from_ref(ref_str: str) -> str:
    """Extract record signature from reference strings like '0038927A:Name:LVLI'."""
    if not ref_str:
        return ""
    parts = ref_str.split(":")
    if len(parts) >= 3:
        return parts[2].strip()
    return ""


# ---------------------------------------------------------------------------
# 1. CONDITION PARSING
# ---------------------------------------------------------------------------

# Regex for xEdit condition format:
# Subject.FunctionName(hex, hex, Ref [TYPE:hex], Ref [TYPE:hex], runon) OPERATOR VALUE
# or simpler: Subject.FunctionName(hex, hex, hex, hex, runon) FLAGS VALUE
_COND_PATTERN = re.compile(
    r'(?P<runon>\w+)\.(?P<func>\w+)\('
    r'(?P<params>[^)]*)\)'
    r'\s+(?P<flags>[0-9A-Fa-f]+)'
    r'\s+(?P<value>.+)',
    re.IGNORECASE
)

_REF_IN_PARAM = re.compile(r'(\w+)\s*\[(\w+):([0-9A-Fa-f]+)\]')

def parse_condition(cond_str: str) -> Optional[Dict[str, Any]]:
    """Parse a single xEdit condition string into a structured dict."""
    cond_str = cond_str.strip()
    if not cond_str:
        return None

    # Remove outer quotes if present
    if cond_str.startswith('"') and cond_str.endswith('"'):
        cond_str = cond_str[1:-1].replace('""', '"')

    m = _COND_PATTERN.match(cond_str)
    if not m:
        return None

    func = m.group("func")
    run_on = m.group("runon")
    params_raw = m.group("params")
    flags_hex = m.group("flags")
    value_str = m.group("value").strip()

    # Parse operator from flags
    try:
        flags_int = int(flags_hex, 16)
    except ValueError:
        flags_int = 0

    # Operator is encoded in bits 5-7 of the flags
    operator_kind = (flags_int >> 5) & 0x7
    # AND/OR is bit 0
    is_or = (flags_int & 1) != 0

    # Parse value — could be a float or a GLOB reference
    value = None
    value_ref = None
    # Check if value contains a GLOB reference like "SomeGlobal [GLOB:0043C4A8]"
    glob_match = re.search(r'\[GLOB:([0-9A-Fa-f]+)\]', value_str)
    if glob_match:
        value_ref = glob_match.group(1).upper().lstrip("0").rjust(8, "0")
    else:
        value = safe_float(value_str, 0.0)

    # Parse parameters — extract FormID references
    param1_ref = "00000000"
    param2_ref = "00000000"
    param_refs = _REF_IN_PARAM.findall(params_raw)
    for i, (name, sig, fid) in enumerate(param_refs):
        fid_upper = fid.upper().lstrip("0").rjust(8, "0")
        if i == 0:
            param1_ref = fid_upper
        elif i == 1:
            param2_ref = fid_upper

    result = {
        "FunctionName": func,
        "RunOn": run_on,
        "Operator": flags_int,
        "Param1Ref": param1_ref,
        "Param2Ref": param2_ref,
    }
    if value is not None:
        result["Value"] = value
    if value_ref:
        result["Ref"] = value_ref

    return result


# ---------------------------------------------------------------------------
# 2. LOAD AND BUILD DATA STRUCTURES
# ---------------------------------------------------------------------------

def build_edids(tsv_root: str) -> Dict[str, str]:
    """
    Build the edids dict: FormID → "SIG EDID DisplayName".
    Mirrors Chrzasz's format where edids["FormID"] = "LVLI EditorID Display Name".
    """
    edids: Dict[str, str] = {}

    # Load LVLI EDIDs from the List TSV
    try:
        lvli_path = newest(os.path.join(tsv_root, "LVLI_Export_*_LVLI_List.tsv"))
        for row in read_tsv(lvli_path):
            fid = pick(row, "LVLI_FormID")
            edid = pick(row, "LVLI_EDID")
            full = pick(row, "LVLI_FULL")
            if fid:
                edids[fid.upper()] = f"LVLI {edid} {full}".strip()
    except FileNotFoundError:
        print("[WARN] No LVLI List TSV found", file=sys.stderr)

    # Load item names from various record types
    _item_loaders = [
        ("BOOK_Export_*.tsv", ["_Locations"], "BOOK_FormID", "FormID", "BOOK_EDID", "EDID", "BOOK_FULL", "FULL", "BOOK"),
        ("MISC_Export_*.tsv", [], "MISC_FormID", "FormID", "MISC_EDID", "EDID", "MISC_FULL", "FULL - Name", "MISC"),
        ("KEYM_Export_*.tsv", ["_Locations", "_Refs", "_KYWD"], "KEYM_FormID", "FormID", "KEYM_EDID", "EDID", "KEYM_FULL", "FULL", "KEYM"),
        ("ARMO_Export_*.tsv", ["_SLOTS", "_ObjectTemplate"], "ARMO_FormID", "FormID", "ARMO_EDID", "EDID", "ARMO_FULL", "FULL", "ARMO"),
        ("WEAP_Export_*.tsv", ["_ObjectTemplate", "_DNAM"], "WEAP_FormID", "FormID", "WEAP_EDID", "EDID", "WEAP_FULL", "FULL - Name", "WEAP"),
        ("ALCH_Export_*.tsv", ["_Effects"], "ALCH_FormID", "FormID", "ALCH_EDID", "EDID", "ALCH_FULL", "FULL - Name", "ALCH"),
        ("AMMO_Export_*.tsv", [], "AMMO_FormID", "FormID", "AMMO_EDID", "EDID", "AMMO_FULL", "FULL - Name", "AMMO"),
    ]

    for pattern, excludes, fid_key1, fid_key2, edid_key1, edid_key2, full_key1, full_key2, sig in _item_loaders:
        try:
            files = _glob.glob(os.path.join(tsv_root, pattern))
            if excludes:
                files = [f for f in files if not any(s in os.path.basename(f) for s in excludes)]
            if not files:
                continue
            files.sort(key=lambda x: (_filename_date_key(x), os.path.basename(x)))
            path = files[-1]
            for row in read_tsv(path):
                fid = pick(row, fid_key1, fid_key2)
                edid = pick(row, edid_key1, edid_key2)
                full = pick(row, full_key1, full_key2, "FULL", "Name")
                if fid and fid.upper() not in edids:
                    edids[fid.upper()] = f"{sig} {edid} {full}".strip()
        except Exception as e:
            print(f"[WARN] Loading {pattern}: {e}", file=sys.stderr)

    # Also load GLOB EDIDs
    try:
        glob_path = newest(os.path.join(tsv_root, "GLOB_Export_*.tsv"))
        for row in read_tsv(glob_path):
            fid = pick(row, "FormID", "GLOB_FormID")
            edid = pick(row, "EDID", "GLOB_EDID")
            if fid and fid.upper() not in edids:
                edids[fid.upper()] = f"GLOB {edid}"
    except FileNotFoundError:
        pass

    # Load CURV EDIDs
    try:
        curv_path = newest(os.path.join(tsv_root, "CURV_Export_*_POINTS.tsv"))
        for row in read_tsv(curv_path):
            fid = pick(row, "FormID")
            edid = pick(row, "EDID")
            if fid and fid.upper() not in edids:
                edids[fid.upper()] = f"CURV {edid}"
    except FileNotFoundError:
        pass

    return edids


def build_globals(tsv_root: str) -> Dict[str, float]:
    """Build globals dict: FormID → float value."""
    globs: Dict[str, float] = {}
    try:
        path = newest(os.path.join(tsv_root, "GLOB_Export_*.tsv"))
        for row in read_tsv(path):
            fid = pick(row, "FormID", "GLOB_FormID")
            val = safe_float(pick(row, "FLTV", "GLOB_FLTV"), None)
            if fid and val is not None:
                globs[fid.upper()] = val
    except FileNotFoundError:
        print("[WARN] No GLOB TSV found", file=sys.stderr)
    return globs


def build_curves(tsv_root: str) -> Dict[str, List[Dict[str, float]]]:
    """Build curves dict: FormID → [{x, y}, ...]."""
    curves: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    try:
        path = newest(os.path.join(tsv_root, "CURV_Export_*_POINTS.tsv"))
        for row in read_tsv(path):
            fid = pick(row, "FormID")
            x = safe_float(pick(row, "X"), None)
            y = safe_float(pick(row, "Y"), None)
            if fid and x is not None and y is not None:
                curves[fid.upper()].append({"x": x, "y": y})
    except FileNotFoundError:
        print("[WARN] No CURV POINTS TSV found", file=sys.stderr)
    # Sort each curve by X
    for fid in curves:
        curves[fid].sort(key=lambda p: p["x"])
    return dict(curves)


def build_leveled_lists(tsv_root: str) -> Dict[str, Dict[str, Any]]:
    """Build the full leveled lists dict from LVLI TSVs."""
    lists: Dict[str, Dict[str, Any]] = {}

    # Step 1: Load list headers
    try:
        list_path = newest(os.path.join(tsv_root, "LVLI_Export_*_LVLI_List.tsv"))
    except FileNotFoundError:
        print("[ERROR] No LVLI List TSV found", file=sys.stderr)
        return lists

    for row in read_tsv(list_path):
        fid = pick(row, "LVLI_FormID")
        if not fid:
            continue
        fid = fid.upper()

        # Parse flags — stored as bit string like "001", "11", "101" etc.
        flags_str = pick(row, "LVLF_Flags", default="0")
        # Convert bit-positional string to integer
        try:
            flags = int(flags_str, 2) if all(c in "01" for c in flags_str) else int(flags_str)
        except ValueError:
            flags = 0

        lvli: Dict[str, Any] = {"LVLF": flags}

        # ChanceNone value (direct)
        cn_val = safe_float(pick(row, "LVCV_ChanceNoneValue"), None)
        if cn_val is not None and cn_val > 0:
            lvli["LVCV"] = cn_val

        # ChanceNone global reference
        cn_glob = extract_formid(pick(row, "LVLG_ChanceNoneGlobal"))
        if cn_glob and cn_glob != "00000000":
            lvli["LVLG"] = cn_glob

        # ChanceNone curve reference
        cn_curv_raw = pick(row, "LVCT_ChanceNoneCurve")
        if cn_curv_raw:
            cn_curv_match = re.search(r'\[(?:CURV|GLOB):([0-9A-Fa-f]+)\]', cn_curv_raw)
            if cn_curv_match:
                lvli["LVCT"] = cn_curv_match.group(1).upper().lstrip("0").rjust(8, "0")

        # Max value
        max_val = safe_float(pick(row, "LVMV_MaxValue"), None)
        if max_val is not None and max_val > 0:
            lvli["LVMV"] = int(max_val) if max_val == int(max_val) else max_val

        # Max global reference
        max_glob = extract_formid(pick(row, "LVMG_MaxGlobal"))
        if max_glob and max_glob != "00000000":
            lvli["LVMG"] = max_glob

        # Max curve reference
        max_curv_raw = pick(row, "LVMT_MaxCurve")
        if max_curv_raw:
            max_curv_match = re.search(r'\[(?:CURV|GLOB):([0-9A-Fa-f]+)\]', max_curv_raw)
            if max_curv_match:
                lvli["LVMT"] = max_curv_match.group(1).upper().lstrip("0").rjust(8, "0")

        # List-level conditions
        list_conds = []
        for i in range(1, 5):
            c = pick(row, f"ListCond{i}")
            if c:
                parsed = parse_condition(c)
                if parsed:
                    list_conds.append(parsed)
        if list_conds:
            lvli["Conditions"] = list_conds

        lists[fid] = lvli

    # Step 2: Load entries
    try:
        entries_path = newest(os.path.join(tsv_root, "LVLI_Export_*_LVLI_Entries.tsv"))
    except FileNotFoundError:
        print("[ERROR] No LVLI Entries TSV found", file=sys.stderr)
        return lists

    for row in read_tsv(entries_path):
        fid = pick(row, "LVLI_FormID")
        if not fid:
            continue
        fid = fid.upper()

        if fid not in lists:
            # Entry references a list not in our headers — create minimal entry
            lists[fid] = {"LVLF": 0}

        # Build the entry object
        ref_raw = pick(row, "LVLO_Reference")
        obj_fid = extract_formid(ref_raw)
        if not obj_fid:
            continue

        entry: Dict[str, Any] = {"Object": obj_fid}

        # Entry-level ChanceNone value
        ecn = safe_float(pick(row, "LVOV_ChanceNoneValue"), None)
        if ecn is not None and ecn > 0:
            entry["LVOV"] = ecn

        # Entry-level ChanceNone global
        ecn_glob = extract_formid(pick(row, "LVOG_ChanceNoneGlobal"))
        if ecn_glob and ecn_glob != "00000000":
            entry["LVOG"] = ecn_glob

        # Entry-level ChanceNone curve
        ecn_curv_raw = pick(row, "LVOC_ChanceNoneCurve")
        if ecn_curv_raw:
            ecn_curv_match = re.search(r'\[(?:CURV|GLOB):([0-9A-Fa-f]+)\]', ecn_curv_raw)
            if ecn_curv_match:
                entry["LVOC"] = ecn_curv_match.group(1).upper().lstrip("0").rjust(8, "0")

        # Quantity
        qty = safe_float(pick(row, "LVIV_Quantity"), None)
        if qty is not None and qty != 1.0:
            entry["LVIV"] = int(qty) if qty == int(qty) else qty

        # Quantity global
        qty_glob = extract_formid(pick(row, "LVIG_QuantityGlobal"))
        if qty_glob and qty_glob != "00000000":
            entry["LVIG"] = qty_glob

        # Quantity curve
        qty_curv_raw = pick(row, "LVIT_QuantityCurve")
        if qty_curv_raw:
            qty_curv_match = re.search(r'\[(?:CURV|GLOB):([0-9A-Fa-f]+)\]', qty_curv_raw)
            if qty_curv_match:
                entry["LVIT"] = qty_curv_match.group(1).upper().lstrip("0").rjust(8, "0")

        # Minimum level
        min_lvl = safe_float(pick(row, "LVLV_MinimumLevel"), None)
        if min_lvl is not None and min_lvl > 1:
            entry["LVLV"] = int(min_lvl)

        # Minimum level global
        min_lvl_glob = extract_formid(pick(row, "LVLG_MinimumLevelGlobal"))
        if min_lvl_glob and min_lvl_glob != "00000000":
            entry["LVLG_MinLvl"] = min_lvl_glob

        # Entry conditions
        entry_conds = []
        for i in range(1, 11):
            c = pick(row, f"Cond{i}")
            if c:
                parsed = parse_condition(c)
                if parsed:
                    entry_conds.append(parsed)
        if entry_conds:
            entry["Conditions"] = entry_conds

        # Append to the list's entries
        if "Entries" not in lists[fid]:
            lists[fid]["Entries"] = []
        lists[fid]["Entries"].append(entry)

    return lists


# ---------------------------------------------------------------------------
# 3. MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build drop chances data JSON")
    parser.add_argument("--tsv-root", default="tsv", help="Path to TSV directory")
    parser.add_argument("--outdir", default="dist", help="Output directory")
    parser.add_argument("--patch-label", default="", help="Patch label e.g. 'Update 65 · 20 Jan 2026'")
    parser.add_argument("--patch-id", default="", help="Patch ID e.g. 'P65'")
    args = parser.parse_args()

    tsv_root = args.tsv_root
    outdir = args.outdir

    print(f"[build_drop_chances_data] Loading from {tsv_root}...")

    # Build all four data structures
    print("  Loading EDIDs...")
    edids = build_edids(tsv_root)
    print(f"    → {len(edids)} editor IDs")

    print("  Loading globals...")
    globs = build_globals(tsv_root)
    print(f"    → {len(globs)} globals")

    print("  Loading curves...")
    curvs = build_curves(tsv_root)
    print(f"    → {len(curvs)} curve tables")

    print("  Loading leveled lists...")
    lists = build_leveled_lists(tsv_root)
    print(f"    → {len(lists)} leveled lists")

    # Count total entries
    total_entries = sum(len(v.get("Entries", [])) for v in lists.values())
    print(f"    → {total_entries} total entries across all lists")

    # Assemble output
    output = {
        "meta": {
            "patchId": args.patch_id or "current",
            "patchLabel": args.patch_label or "Current Patch",
            "built": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "stats": {
                "edids": len(edids),
                "globals": len(globs),
                "curves": len(curvs),
                "lists": len(lists),
                "entries": total_entries,
            }
        },
        "edids": edids,
        "globals": globs,
        "curves": curvs,
        "lists": lists,
    }

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, "drop_chances_data.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(",", ":"), ensure_ascii=False)

    file_size = os.path.getsize(out_path)
    print(f"\n[build_drop_chances_data] Wrote {out_path}")
    print(f"  Size: {file_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
