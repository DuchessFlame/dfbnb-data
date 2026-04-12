from __future__ import annotations

"""
build_bnb_menu_json.py
======================
Generates dist/bnb-menu.json — the data source for the Buffs n Brew menu page
rendered at /bnb/buffs-n-brew/menu/ by the df-bnb-menu.js feature module.

Source of truth:
    tsv/BnB_Menu_Items.tsv

Schema (TSV columns):
    menu category   name   edid   form_id   build   price_xbox   price_ps
    price_pc        limit_new      limit_existing   order category   notes

Output shape:
    {
      "version":    "YYYY-MM-DD",
      "generated":  "YYYY-MM-DDTHH:MM:SS+00:00",
      "count":      <total items>,
      "categories": [
        {
          "name":  "Alcohol",
          "count": <n>,
          "items": [
            {
              "name":           "Appalachian Ale",
              "price_xbox":     "10",
              "price_ps":       "15",
              "price_pc":       "15",
              "limit_new":      "30",
              "limit_existing": "30",
              "build":          ""
            },
            ...
          ]
        },
        ...
      ]
    }

Design notes:
  - The page renders all 9 fine-grained menu categories (NOT the coarse
    Food/Chem buckets used by build_menu_items_json.py).
  - Items are sorted A-Z within each category, and categories A-Z overall.
  - Missing prices render as blank — we do NOT substitute fallbacks.
  - "order category" and "notes" columns are stripped (not shown on the page).
  - The "edid" and "form_id" columns are stripped (internal only).

Diagnostics:
  Warnings for rows missing a category or name, and errors for rows missing
  any price. Written to dist/diagnostics.json under source="bnb_menu".

Usage:
    python build_bnb_menu_json.py
    python build_bnb_menu_json.py --data-dir tsv --outdir dist
"""

import argparse
import csv
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagnostics import Diagnostics  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INPUT_TSV = "BnB_Menu_Items.tsv"
OUTPUT_JSON = "bnb-menu.json"

REQUIRED_COLS = {
    "menu category",
    "name",
    "price_xbox",
    "price_ps",
    "price_pc",
    "limit_new",
    "limit_existing",
    "build",
    "mutation",
    "buff",
}

PRICE_FIELDS = ("price_xbox", "price_ps", "price_pc")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def today_ymd() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def clean(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


# ---------------------------------------------------------------------------
# Load + build
# ---------------------------------------------------------------------------

def load_rows(path: str, diag: Diagnostics) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        diag.error(
            "bnb_menu.tsv.missing",
            f"{INPUT_TSV} not found - no menu will be emitted.",
            detail=path,
        )
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            diag.error(
                "bnb_menu.tsv.empty",
                f"{INPUT_TSV} has no header row.",
                detail=path,
            )
            return []
        missing = REQUIRED_COLS - set(reader.fieldnames)
        if missing:
            diag.error(
                "bnb_menu.tsv.bad_schema",
                f"{INPUT_TSV} is missing required columns: {sorted(missing)}",
                detail=f"Found columns: {reader.fieldnames}",
            )
            return []
        return [dict(r) for r in reader]


def build_categories(rows: List[Dict[str, str]], diag: Diagnostics) -> List[Dict[str, Any]]:
    # Group rows by menu category
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    seen_names_per_cat: Dict[str, set] = {}

    for i, row in enumerate(rows, start=2):  # line 1 is the header
        cat = clean(row.get("menu category", ""))
        name = clean(row.get("name", ""))

        if not cat:
            diag.warning(
                "bnb_menu.row.missing_category",
                f"Row {i}: skipping — no menu category set.",
                detail=f"name={name!r}",
            )
            continue
        if not name:
            diag.warning(
                "bnb_menu.row.missing_name",
                f"Row {i}: skipping — no item name set.",
                detail=f"category={cat!r}",
            )
            continue

        # Dedupe within a category (keep first seen)
        seen = seen_names_per_cat.setdefault(cat, set())
        key = name.lower()
        if key in seen:
            diag.info(
                "bnb_menu.row.duplicate",
                f"Row {i}: duplicate {name!r} in {cat!r} — keeping first.",
                detail=name,
            )
            continue
        seen.add(key)

        item: Dict[str, Any] = {
            "name":           name,
            "price_xbox":     clean(row.get("price_xbox", "")),
            "price_ps":       clean(row.get("price_ps", "")),
            "price_pc":       clean(row.get("price_pc", "")),
            "limit_new":      clean(row.get("limit_new", "")),
            "limit_existing": clean(row.get("limit_existing", "")),
            "build":          clean(row.get("build", "")),
            "mutation":       clean(row.get("mutation", "")),
            "buff":           clean(row.get("buff", "")),
        }

        missing_prices = [f for f in PRICE_FIELDS if not item[f]]
        if missing_prices:
            diag.error(
                "bnb_menu.row.missing_price",
                f"{name!r} ({cat}) is missing price for: "
                f"{', '.join(p.replace('price_', '') for p in missing_prices)}",
                detail=name,
                context={"name": name, "category": cat, "missing_fields": missing_prices},
            )

        grouped.setdefault(cat, []).append(item)

    # Sort items A-Z within categories, categories A-Z overall
    out: List[Dict[str, Any]] = []
    for cat in sorted(grouped.keys(), key=lambda s: s.lower()):
        items = sorted(grouped[cat], key=lambda r: r["name"].lower())
        out.append({
            "name":  cat,
            "count": len(items),
            "items": items,
        })
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build bnb-menu.json from BnB_Menu_Items.tsv")
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Output dir (default: dist)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="bnb_menu", outdir=args.outdir)

    tsv_path = os.path.join(args.data_dir, INPUT_TSV)
    rows = load_rows(tsv_path, diag)
    categories = build_categories(rows, diag)

    total = sum(c["count"] for c in categories)
    print(
        f"BnB menu: {total} items across {len(categories)} categories",
        file=sys.stderr,
    )
    for c in categories:
        print(f"  {c['name']}: {c['count']}", file=sys.stderr)

    diag.info(
        "bnb_menu.build.summary",
        f"Wrote {total} menu items across {len(categories)} categories.",
        context={"total": total, "categories": [c["name"] for c in categories]},
    )

    output: Dict[str, Any] = {
        "version":    today_ymd(),
        "generated":  now_iso(),
        "count":      total,
        "categories": categories,
    }

    output_path = os.path.join(args.outdir, OUTPUT_JSON)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Wrote {output_path}", file=sys.stderr)
    except Exception as exc:
        diag.error("bnb_menu.write.failed", "Failed to write bnb-menu.json", detail=str(exc))
        diag.save()
        sys.exit(1)

    diag.save()


if __name__ == "__main__":
    main()
