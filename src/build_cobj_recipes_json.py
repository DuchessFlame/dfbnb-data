from __future__ import annotations

"""
build_cobj_recipes_json.py
===========================
Generates dist/cobj-recipes.json from xEdit COBJ TSV export.

Reads COBJ entries and extracts crafting recipes for food and chemical items.
Maps menu item display names to their ingredient requirements.

Input files (place in tsv/ folder or pass via --data-dir):
  COBJ_Export_*.tsv       (xEdit tab-separated export)

Output:
  dist/cobj-recipes.json  → { "recipes": { "Item Name": { "Ingredient": count } } }

Usage:
  python build_cobj_recipes_json.py
  python build_cobj_recipes_json.py --data-dir /path/to/tsvs --outdir /path/to/dist
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def today_ymd() -> str:
    """Return current UTC date as YYYY-MM-DD."""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def clean_str(s: Any) -> str:
    """Strip whitespace and remove surrounding quotes from a string."""
    if s is None:
        return ""
    s = str(s).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    return s


def should_skip(edid: str) -> bool:
    """Check if EDID starts with a cut/placeholder prefix."""
    e = clean_str(edid).upper()
    return any(e.startswith(p) for p in CUT_PREFIXES)


def parse_fvpa(fvpa_str: str) -> Dict[str, int]:
    """
    Parse FVPA field into ingredient dict.
    Format: "Material:Count | Material:Count | ..."
    Returns: { "Material": count, ... }
    """
    ingredients: Dict[str, int] = {}
    if not fvpa_str:
        return ingredients

    fvpa_str = clean_str(fvpa_str).strip()
    if not fvpa_str:
        return ingredients

    # Split by pipe delimiter
    parts = fvpa_str.split("|")
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Parse "Material:Count" format
        if ":" in part:
            mat, cnt = part.split(":", 1)
            mat = clean_str(mat).strip()
            cnt_str = clean_str(cnt).strip()
            try:
                count = int(cnt_str)
                if mat:
                    ingredients[mat] = ingredients.get(mat, 0) + count
            except (ValueError, TypeError):
                pass

    return ingredients


def read_tsv(path: str) -> List[Dict[str, str]]:
    """Read a TSV file and return list of row dicts."""
    rows: List[Dict[str, str]] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if reader.fieldnames is None:
                print(f"ERROR: {path} has no header row", file=sys.stderr)
                return rows
            rows = [dict(r) for r in reader]
    except Exception as e:
        print(f"ERROR reading {path}: {e}", file=sys.stderr)
    return rows


def find_tsv_files(data_dir: str, pattern: str = "COBJ_Export_*.tsv") -> List[str]:
    """Find TSV files matching pattern in data_dir."""
    paths = glob.glob(os.path.join(data_dir, pattern))
    return sorted(paths)


# ---------------------------------------------------------------------------
# Recipe extraction
# ---------------------------------------------------------------------------

def build_recipes(cobj_rows: List[Dict[str, str]]) -> Dict[str, Dict[str, int]]:
    """
    Extract recipes from COBJ rows.
    Returns: { "Item Display Name": { "Ingredient": count, ... } }
    """
    recipes: Dict[str, Dict[str, int]] = {}
    skipped_count = 0
    no_fvpa_count = 0

    for row in cobj_rows:
        edid = row.get("COBJ_EDID", "")
        display_name = clean_str(row.get("CNAM_FULL", ""))
        fvpa = row.get("FVPA", "")

        # Skip cut content
        if should_skip(edid):
            skipped_count += 1
            continue

        # Skip if no display name
        if not display_name:
            no_fvpa_count += 1
            continue

        # Skip if no FVPA (ingredients)
        if not clean_str(fvpa):
            no_fvpa_count += 1
            continue

        # Parse ingredients
        ingredients = parse_fvpa(fvpa)
        if not ingredients:
            no_fvpa_count += 1
            continue

        # Add recipe (first match wins for duplicates, or use the one with most ingredients)
        if display_name not in recipes:
            recipes[display_name] = ingredients
        else:
            # If we have a duplicate, keep the one with more ingredients
            if len(ingredients) > len(recipes[display_name]):
                recipes[display_name] = ingredients

    print(f"Recipes extracted: {len(recipes)}", file=sys.stderr)
    print(f"Skipped (cut content): {skipped_count}", file=sys.stderr)
    print(f"Skipped (no FVPA or no display name): {no_fvpa_count}", file=sys.stderr)

    return recipes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build cobj-recipes.json from COBJ TSV export"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="tsv",
        help="Directory containing COBJ_Export_*.tsv files (default: tsv)",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="dist",
        help="Output directory for cobj-recipes.json (default: dist)",
    )
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.outdir, exist_ok=True)

    # Find and read COBJ TSV files
    tsv_files = find_tsv_files(args.data_dir, "COBJ_Export_*.tsv")
    if not tsv_files:
        print(f"ERROR: No COBJ_Export_*.tsv files found in {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(tsv_files)} COBJ TSV file(s)", file=sys.stderr)

    all_rows: List[Dict[str, str]] = []
    for tsv_path in tsv_files:
        print(f"Reading {os.path.basename(tsv_path)}...", file=sys.stderr)
        rows = read_tsv(tsv_path)
        all_rows.extend(rows)
        print(f"  Loaded {len(rows)} rows", file=sys.stderr)

    print(f"Total rows: {len(all_rows)}", file=sys.stderr)

    # Build recipes
    recipes = build_recipes(all_rows)

    # Build output JSON
    output: Dict[str, Any] = {
        "version": today_ymd(),
        "generated": now_iso(),
        "count": len(recipes),
        "recipes": recipes,
    }

    # Write JSON
    output_path = os.path.join(args.outdir, "cobj-recipes.json")
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Wrote {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"ERROR writing {output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
