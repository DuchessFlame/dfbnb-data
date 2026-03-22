#!/usr/bin/env python3
"""
build_daily_ops_json.py — Daily Ops All Rewards

Reads the shared REHO rewards JSON (built by build_reho_json.py) and enriches
the Daily Ops page data with tradeable status from BOOK + ARMO TSVs.

Outputs:
  dist/daily_ops/daily_ops_rewards.json

Tradeable logic (same pattern as build_titles_json.py):
  - BOOK TSV:  Plans/recipes. If row contains NonPlayerTradeable / NonPlayerTradable
               / UnsellableObject → non-tradeable, else tradeable.
  - ARMO TSV:  Apparel/outfits. Same keyword check.
  - Items not found in either TSV get no tradeable field.

Usage:
  python build_daily_ops_json.py

Requires: build_reho_json.py to have run first (needs dist/reho/reho_rewards_by_page.json).
"""

import json
import csv
import os
import glob
from pathlib import Path
from typing import Dict, List


# ---------------------------------------------------------------------------
# TSV HELPERS
# ---------------------------------------------------------------------------

def read_tsv(path: str) -> List[Dict[str, str]]:
    """Read a TSV file, trying UTF-8-sig first then cp1252."""
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))


def is_non_tradeable(row: Dict[str, str]) -> bool:
    """
    Check whether a TSV row contains keywords indicating non-tradeable.

    Matches the logic in build_titles_json.py book_tradeable_map():
    - NonPlayerTradeable (common Bethesda naming)
    - NonPlayerTradable  (variant spelling)
    - UnsellableObject   (also treated as non-tradeable)
    """
    blob = " ".join(str(v) for v in row.values() if v).lower()
    return (
        "nonplayertradeable" in blob
        or "nonplayertradable" in blob
        or "unsellableobject" in blob
    )


def latest_tsv(tsv_dir: Path, pattern: str, exclude_keywords=None) -> str | None:
    """Return the newest TSV matching a glob pattern, optionally excluding filenames."""
    exclude_keywords = [kw.lower() for kw in (exclude_keywords or [])]
    candidates = sorted(
        glob.glob(str(tsv_dir / pattern)),
        key=lambda x: os.path.getmtime(x),
    )
    if exclude_keywords:
        candidates = [
            p for p in candidates
            if not any(kw in os.path.basename(p).lower() for kw in exclude_keywords)
        ]
    return candidates[-1] if candidates else None


# ---------------------------------------------------------------------------
# TRADEABLE MAP BUILDER
# ---------------------------------------------------------------------------

def build_tradeable_map(tsv_dir: Path) -> Dict[str, bool]:
    """
    Build a FormID → bool tradeable map from the latest BOOK and ARMO TSVs.

    BOOK rows cover plans/recipes (isPlan=true items).
    ARMO rows cover apparel/outfit drops (isPlan=false items).
    """
    tradeable: Dict[str, bool] = {}

    # --- BOOK TSV (plans/recipes) ---
    book_path = latest_tsv(tsv_dir, "BOOK_Export_*.tsv", exclude_keywords=["location"])
    if book_path:
        count = 0
        for row in read_tsv(book_path):
            fid = (row.get("FormID") or "").strip().upper()
            if not fid:
                continue
            tradeable[fid] = not is_non_tradeable(row)
            count += 1
        print(f"  BOOK tradeable: {count} entries from {os.path.basename(book_path)}")
    else:
        print("  Warning: No BOOK TSV found")

    # --- ARMO TSV (apparel/outfits) ---
    armo_path = latest_tsv(
        tsv_dir, "ARMO_Export_*.tsv",
        exclude_keywords=["objecttemplate", "slot"],
    )
    if armo_path:
        count = 0
        for row in read_tsv(armo_path):
            # Handle both column naming conventions:
            #   Dec 2025 format: "FormID"
            #   March 2026 format: "ARMO_FormID"
            fid = (
                row.get("FormID")
                or row.get("ARMO_FormID")
                or ""
            ).strip().upper()
            if not fid:
                continue
            # BOOK takes priority — only add if not already set
            if fid not in tradeable:
                tradeable[fid] = not is_non_tradeable(row)
                count += 1
        print(f"  ARMO tradeable: {count} new entries from {os.path.basename(armo_path)}")
    else:
        print("  Warning: No ARMO TSV found")

    print(f"  Total tradeable map: {len(tradeable)} entries")
    return tradeable


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    base = Path(os.path.dirname(os.path.dirname(__file__)))
    tsv_dir = base / "tsv"
    reho_path = base / "dist" / "reho" / "reho_rewards_by_page.json"
    out_dir = base / "dist" / "daily_ops"
    out_file = out_dir / "daily_ops_rewards.json"

    print("Building Daily Ops rewards JSON...")

    # 1. Load the shared REHO data
    if not reho_path.exists():
        print(f"  Error: {reho_path} not found — run build_reho_json.py first")
        return 1

    with open(reho_path) as f:
        reho = json.load(f)

    slug = "daily-ops-all-rewards"
    by_page = reho.get("byPage", {})
    page_data = by_page.get(slug)

    if not page_data:
        print(f"  Error: No data for '{slug}' in reho JSON")
        return 1

    print(f"  Loaded REHO data: {len(page_data.get('pools', []))} pools")

    # 2. Build tradeable map from BOOK + ARMO TSVs
    tradeable_map = build_tradeable_map(tsv_dir)

    # 3. Enrich items with tradeable field
    enriched = 0
    for pool in page_data.get("pools", []):
        for item in pool.get("items", []):
            fid = (item.get("formid") or item.get("formId") or "").strip().upper()
            if fid in tradeable_map:
                item["tradeable"] = tradeable_map[fid]
                enriched += 1

    print(f"  Enriched {enriched} items with tradeable status")

    # 4. Write output
    out_dir.mkdir(parents=True, exist_ok=True)

    output = {
        "slug": slug,
        "pageType": page_data.get("pageType", "dailyops"),
        "name": page_data.get("name", "Daily Ops"),
        "meta": page_data.get("meta", {}),
        "pools": page_data.get("pools", []),
        "baseRewards": page_data.get("baseRewards", {}),
    }

    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Wrote {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
