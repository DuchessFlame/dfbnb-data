from __future__ import annotations

"""
build_menu_items_json.py
========================
Generates dist/menu-items.json from the curated menu-items.tsv source.

This is the source of truth for the Buffs n Brew staff portal menu — the items
the chef can be asked to craft, their per-platform pricing, and per-customer
order limits. Edit tsv/menu-items.tsv to add/remove/reprice items, then push
or run the workflow to rebuild dist/menu-items.json.

Input file (place in tsv/ folder or pass via --data-dir):
  menu-items.tsv

Expected TSV columns:
  name              Display name (must match COBJ CNAM_FULL for raw breakdown)
  category          "Food" or "Chem"
  build             Optional build tags, comma separated (e.g. "XP,Herbivore")
  price_xbox        Cap price on XBOX
  price_ps          Cap price on PlayStation
  price_pc          Cap price on PC
  limit_new         Max units per order for new customers
  limit_existing    Max units per order for existing customers

Output:
  dist/menu-items.json
    {
      "version":   "YYYY-MM-DD",
      "generated": ISO 8601 timestamp,
      "count":     N,
      "menu_items": [ { name, category, build, price{}, orderLimits{} }, ... ]
    }

Usage:
  python build_menu_items_json.py
  python build_menu_items_json.py --data-dir /path/to/tsvs --outdir /path/to/dist
"""

import argparse
import csv
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List


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
    """Strip whitespace and surrounding quotes."""
    if s is None:
        return ""
    s = str(s).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    return s


def to_int(s: Any, default: int = 0) -> int:
    """Coerce a value to int, falling back to default if it can't be parsed."""
    s = clean_str(s)
    if not s:
        return default
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return default


def parse_build(s: Any) -> List[str]:
    """
    Build tags are stored as a comma-separated string in the TSV
    (e.g. "XP,Herbivore,Hybrid"). Return a list of cleaned tags, or [] if blank.
    """
    s = clean_str(s)
    if not s:
        return []
    return [tag.strip() for tag in s.split(",") if tag.strip()]


# ---------------------------------------------------------------------------
# TSV reading
# ---------------------------------------------------------------------------

REQUIRED_COLS = (
    "name",
    "category",
    "build",
    "price_xbox",
    "price_ps",
    "price_pc",
    "limit_new",
    "limit_existing",
)


def read_menu_tsv(path: str) -> List[Dict[str, Any]]:
    """Read menu-items.tsv and return a list of normalised item dicts."""
    items: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        print(f"ERROR: TSV file not found at {path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if reader.fieldnames is None:
                print(f"ERROR: {path} has no header row", file=sys.stderr)
                sys.exit(1)

            missing = [c for c in REQUIRED_COLS if c not in reader.fieldnames]
            if missing:
                print(
                    f"ERROR: {path} is missing required columns: {missing}",
                    file=sys.stderr,
                )
                sys.exit(1)

            for row in reader:
                name = clean_str(row.get("name", ""))
                if not name:
                    continue  # skip blank rows

                category = clean_str(row.get("category", ""))
                # Normalise the few category casings we accept.
                if category.lower() in ("food", "foods"):
                    category = "Food"
                elif category.lower() in ("chem", "chems", "chemical"):
                    category = "Chem"

                build_tags = parse_build(row.get("build", ""))
                # Preserve the historic shape: empty string when no tags so the
                # JSON looks identical to the hand-curated file the portal was
                # built against.
                build_value: Any = build_tags if build_tags else ""

                items.append({
                    "name": name,
                    "category": category,
                    "build": build_value,
                    "price": {
                        "XBOX":        to_int(row.get("price_xbox"), 0),
                        "PlayStation": to_int(row.get("price_ps"), 0),
                        "PC":          to_int(row.get("price_pc"), 0),
                    },
                    "orderLimits": {
                        "newCustomer":      to_int(row.get("limit_new"), 0),
                        "existingCustomer": to_int(row.get("limit_existing"), 0),
                    },
                })
    except Exception as e:
        print(f"ERROR reading {path}: {e}", file=sys.stderr)
        sys.exit(1)

    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build menu-items.json from menu-items.tsv"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="tsv",
        help="Directory containing menu-items.tsv (default: tsv)",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="dist",
        help="Output directory for menu-items.json (default: dist)",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    tsv_path = os.path.join(args.data_dir, "menu-items.tsv")
    print(f"Reading {tsv_path}...", file=sys.stderr)
    items = read_menu_tsv(tsv_path)
    print(f"Loaded {len(items)} menu items", file=sys.stderr)

    food_count = sum(1 for i in items if i["category"] == "Food")
    chem_count = sum(1 for i in items if i["category"] == "Chem")
    other_count = len(items) - food_count - chem_count
    print(
        f"  Food: {food_count}  Chem: {chem_count}  Other: {other_count}",
        file=sys.stderr,
    )

    # Sort by category then by name (case-insensitive) for stable diffs.
    items.sort(key=lambda x: (x["category"], x["name"].lower()))

    output: Dict[str, Any] = {
        "version":    today_ymd(),
        "generated":  now_iso(),
        "count":      len(items),
        "menu_items": items,
    }

    out_path = os.path.join(args.outdir, "menu-items.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Wrote {out_path}", file=sys.stderr)
    except Exception as e:
        print(f"ERROR writing {out_path}: {e}", file=sys.stderr)
        sys.exit(1)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
