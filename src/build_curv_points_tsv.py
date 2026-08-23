#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_curv_points_tsv.py
Python port of fo76-tools/misc/curvetables/build-curv-points-tsv.ps1.

Reads a CURV records TSV (produced by the xEdit ExportCURVToTSV.pas script)
and the Bethesda-extracted curve-table JSON tree, then writes a POINTS TSV
with one row per {X,Y} point per curve:

    FormID    EDID    X    Y    JsonPath

Same resolve order as the PS1:
    1. JASF_Path as a direct relative path under <json-root>
    2. JASF_Path leaf filename, recursively anywhere under <json-root>
    3. JsonFileName field, recursively
    4. "<EDID>.json", recursively
    5. "<EDID stripped of leading CT_>.json", recursively
       (catches legacy curves like CT_FoodHealth_TastyMeat which have no
       JASF_Path and whose on-disk filename drops the CT_ prefix)

Also mirrors the PS1's recursive point harvesting: {x,y}/{X,Y} pairs in
objects and 2-element [x,y] lists, nested anywhere in the JSON structure.

Why a Python port?
    The PS1 works great on Duchess's Windows machine (handles OneDrive
    Files-On-Demand, has real file I/O). But GitHub Actions CI runs
    Ubuntu, and reinstating a "run this PS1 before each build" step is
    the exact friction we're trying to remove. With this Python version,
    dfbnb-patch-build.yml can regenerate the POINTS TSV fresh on every
    run from the checked-in records TSV + the checked-in
    data/curvetables/json/ tree — no external deps, no local "don't
    forget to run the PS1" step.

Usage (CI):
    python src/build_curv_points_tsv.py \
        --records-tsv tsv/CURV_Export_Apr_2026_CURV.tsv \
        --json-root data/curvetables/json \
        --out tsv/CURV_Export_Apr_2026_POINTS.tsv

Usage (local, auto-discover):
    python src/build_curv_points_tsv.py
        # picks the newest tsv/CURV_Export_*_CURV.tsv, uses
        # data/curvetables/json (if present) else
        # ../fo76-tools/misc/curvetables/json, writes the matching
        # _POINTS.tsv next to the records file.

Exit codes:
    0 — success (even if some records were unresolved; see summary counts)
    1 — missing required input (records TSV or JSON root)
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional
import tsv_source          # one resolver for every export selection

# --------------------------------------------------------------------------
# Filename-date helpers — picking the newest CURV records TSV is the same
# name_date_key logic the other builders use, so CI picks the right month
# when there are multiple exports in tsv/.
# --------------------------------------------------------------------------

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _name_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def _newest_records_tsv(tsv_dir: Path) -> Optional[Path]:
    """Pick the most recent CURV_Export_*_CURV.tsv (or legacy
    CURV_Export_*.tsv_CURV.tsv) by parsed month/year, mtime as
    tiebreaker. Excludes POINTS and CurvePoints."""
    candidates = []
    for pattern in ("CURV_Export_*_CURV.tsv", "CURV_Export_*.tsv_CURV.tsv"):
        candidates.extend(glob.glob(str(tsv_dir / pattern)))
    candidates = [c for c in candidates
                  if "_POINTS" not in os.path.basename(c)
                  and "CurvePoints" not in os.path.basename(c)]
    if not candidates:
        return None
    candidates.sort(key=lambda p: (_name_date_key(p), os.path.basename(p)))
    return Path(candidates[-1])


# --------------------------------------------------------------------------
# JSON filename index — built once per run so the 4k+ .json files aren't
# re-scanned for every record.
# --------------------------------------------------------------------------

def _build_filename_index(json_root: Path) -> dict:
    """Map lowercase filename → full Path, for recursive lookups.

    Mirrors PS1's Get-ChildItem -Recurse -Filter behaviour but faster.
    """
    idx: dict = {}
    for fp in json_root.rglob("*.json"):
        idx.setdefault(fp.name.lower(), fp)
    return idx


def _resolve_json(
    json_root: Path,
    jasf_path: str,
    json_filename: str,
    edid: str,
    fname_index: dict,
) -> tuple:
    """Return (Path, reported_rel) or (None, None).

    reported_rel is the string the PS1 would include in the JsonPath
    column (the JASF relative or the matched filename). We keep the PS1's
    reporting convention for drop-in TSV compatibility.
    """
    # 1 + 2. JASF_Path direct, then by leaf filename
    rel = (jasf_path or "").strip().replace("/", "\\").lstrip("\\")
    if rel:
        candidate = json_root / rel.replace("\\", os.sep)
        if candidate.is_file():
            return candidate, rel
        candidate_lc = json_root / rel.replace("\\", os.sep).lower()
        if candidate_lc.is_file():
            return candidate_lc, rel
        leaf = Path(rel).name.lower()
        hit = fname_index.get(leaf)
        if hit:
            return hit, rel

    # 3. JsonFileName field direct
    if json_filename:
        hit = fname_index.get(json_filename.lower())
        if hit:
            return hit, json_filename

    # 4. "<EDID>.json"
    if edid:
        hit = fname_index.get(f"{edid}.json".lower())
        if hit:
            return hit, f"{edid}.json"
        # 5. CT_-stripped fallback (matches the PS1 patch). Older curves
        # like CT_FoodHealth_TastyMeat have no JASF_Path and their JSON
        # is at foodhealth_tastymeat.json — i.e. CT_ prefix removed.
        m = re.match(r"^CT_(.+)$", edid, re.IGNORECASE)
        if m:
            stem = m.group(1)
            hit = fname_index.get(f"{stem}.json".lower())
            if hit:
                return hit, f"{stem}.json"

    return None, None


# --------------------------------------------------------------------------
# Point harvesting — same recursive walk as the PS1. Accepts:
#   {x: N, y: N} / {X: N, Y: N}
#   [N, N] two-element arrays
#   Any nested structure containing the above
# --------------------------------------------------------------------------

def _harvest_points(node, out: list) -> None:
    if node is None:
        return
    if isinstance(node, dict):
        xv = node.get("x", node.get("X"))
        yv = node.get("y", node.get("Y"))
        if xv is not None and yv is not None:
            try:
                out.append((float(xv), float(yv)))
            except (TypeError, ValueError):
                pass
        for v in node.values():
            _harvest_points(v, out)
        return
    if isinstance(node, list):
        # Direct numeric pair [x, y]
        if len(node) == 2 and all(isinstance(v, (int, float)) for v in node):
            out.append((float(node[0]), float(node[1])))
            return
        for v in node:
            _harvest_points(v, out)


def _fmt_num(f: float) -> str:
    """Match the PS1's number formatting — ints as ints ("300" not
    "300.0"), floats with as few digits as faithfully represent the
    value."""
    if f == int(f):
        return str(int(f))
    s = repr(f)
    # Trim trailing zeros on plain decimals, without touching scientific
    # notation or meaningful trailing digits.
    if re.match(r"^-?\d+\.\d+$", s):
        s = s.rstrip("0").rstrip(".") or "0"
    return s


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def _derive_output_path(records_tsv: Path) -> Path:
    """`CURV_Export_Apr_2026_CURV.tsv` -> `CURV_Export_Apr_2026_POINTS.tsv`.
    Legacy `CURV_Export_Apr_2026.tsv_CURV.tsv` also maps to the new
    `_POINTS.tsv` naming so both CI and local produce the same thing."""
    name = records_tsv.name
    m = re.match(r"^(CURV_Export_[A-Za-z]+_\d{4})", name)
    if m:
        stem = m.group(1)
    else:
        stem = re.sub(r"(?:\.tsv)?_CURV\.tsv$", "", name)
        stem = re.sub(r"\.tsv$", "", stem)
    return records_tsv.parent / f"{stem}_POINTS.tsv"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--records-tsv", type=Path, default=None,
                    help="CURV records TSV. Default: auto-discover the newest in tsv/.")
    ap.add_argument("--json-root", type=Path, default=None,
                    help="Curve-table JSON root (fo76-tools/misc/curvetables/json or data/curvetables/json).")
    ap.add_argument("--out", type=Path, default=None,
                    help="Output POINTS TSV. Default: derived from records filename.")
    ap.add_argument("--repo-root", type=Path, default=None,
                    help="Override repo root. Default: script's parent's parent.")
    args = ap.parse_args(argv)

    repo_root = args.repo_root or Path(__file__).resolve().parents[1]

    # ---- Resolve records TSV ---------------------------------------------
    records = args.records_tsv
    if records is None:
        records = _newest_records_tsv(repo_root / "tsv")
        if records is None:
            print(f"ERROR: no CURV_Export_*_CURV.tsv found in {repo_root / 'tsv'}",
                  file=sys.stderr)
            return 1
    if not records.is_file():
        print(f"ERROR: records TSV not found: {records}", file=sys.stderr)
        return 1

    # ---- Resolve JSON root -----------------------------------------------
    json_root = args.json_root
    if json_root is None:
        # Priority: in-repo committed copy (works in CI), then sibling
        # fo76-tools checkout, then the OneDrive fallback.
        candidates = [
            repo_root / "data" / "curvetables" / "json",
            repo_root.parent / "fo76-tools" / "misc" / "curvetables" / "json",
            Path.home() / "OneDrive" / "GitHub" / "fo76-tools" / "misc" / "curvetables" / "json",
        ]
        for c in candidates:
            if c.is_dir():
                json_root = c
                break
        if json_root is None:
            print("ERROR: no curvetables JSON root found. Tried:", file=sys.stderr)
            for c in candidates:
                print(f"  {c}", file=sys.stderr)
            return 1
    if not json_root.is_dir():
        print(f"ERROR: JSON root not a directory: {json_root}", file=sys.stderr)
        return 1

    # ---- Resolve output path --------------------------------------------
    out = args.out or _derive_output_path(records)

    print(f"[curv-points] Records : {records}")
    print(f"[curv-points] JSONs   : {json_root}")
    print(f"[curv-points] Output  : {out}")

    # ---- Build filename index --------------------------------------------
    print("[curv-points] Indexing JSON tree...")
    fname_index = _build_filename_index(json_root)
    print(f"[curv-points]   {len(fname_index)} unique .json filenames")

    # ---- Walk records TSV + convert --------------------------------------
    rows_out = []
    n_processed = 0
    n_skipped_no_json = 0
    n_skipped_no_points = 0
    n_points = 0
    unresolved_samples: list = []

    with records.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            n_processed += 1
            form_id = ((row.get("CURV_FormID") or row.get("FormID") or "")
                       .strip().upper())
            if not form_id:
                continue
            edid = (row.get("CURV_EDID") or row.get("EDID") or "").strip()
            jasf = (row.get("JASF_Path") or "").strip()
            jfn = (row.get("JsonFileName") or "").strip()

            fpath, rel_used = _resolve_json(
                json_root, jasf, jfn, edid, fname_index,
            )
            if fpath is None:
                n_skipped_no_json += 1
                if len(unresolved_samples) < 5:
                    unresolved_samples.append(edid or form_id)
                continue

            try:
                data = json.loads(fpath.read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                print(f"[curv-points] WARN JSON parse failed {fpath}: {e}",
                      file=sys.stderr)
                n_skipped_no_json += 1
                continue

            pts = []
            _harvest_points(data, pts)
            if not pts:
                n_skipped_no_points += 1
                continue

            # Dedupe + sort. Matches PS1: Sort-Object x, y | Get-Unique.
            seen = set()
            uniq = []
            for x, y in sorted(pts, key=lambda p: (p[0], p[1])):
                k = (x, y)
                if k in seen:
                    continue
                seen.add(k)
                uniq.append(k)

            # Build the JsonPath column. PS1 reports the full on-disk
            # path, but we want something that's stable across machines —
            # use the path relative to json_root with backslashes, which
            # matches the JASF convention the downstream consumers already
            # handle.
            try:
                rel_posix = fpath.relative_to(json_root).as_posix()
            except ValueError:
                rel_posix = fpath.as_posix()
            reported_path = rel_posix.replace("/", "\\")

            for x, y in uniq:
                rows_out.append(
                    (form_id.zfill(8), edid, _fmt_num(x), _fmt_num(y), reported_path)
                )
                n_points += 1

    # ---- Write TSV -------------------------------------------------------
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["FormID", "EDID", "X", "Y", "JsonPath"])
        for r in rows_out:
            w.writerow(r)

    print()
    print("[curv-points] ----- summary -----")
    print(f"[curv-points] Records processed      : {n_processed}")
    print(f"[curv-points] Skipped (no JSON match): {n_skipped_no_json}")
    print(f"[curv-points] Skipped (empty JSON)   : {n_skipped_no_points}")
    print(f"[curv-points] Total point rows       : {n_points}")
    if unresolved_samples:
        print(f"[curv-points] Sample unresolved EDIDs  : {', '.join(unresolved_samples)}")
    print(f"[curv-points] Wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
