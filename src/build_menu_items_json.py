from __future__ import annotations

"""
build_menu_items_json.py
========================
Generates dist/menu-items.json from the master TSV (BnB_Menu_Items.tsv).

Source of truth:
    tsv/BnB_Menu_Items.tsv   (maintained by build_bnb_menu_sync.py)
    dist/cobj-recipes.json   (game data — used only to tag craftability)

Why this exists (and why it changed)
------------------------------------
The staff portal's Crafting tab and Order Log both call `loadMenuItems()` to
get "every item BnB sells". Historically this builder walked cobj-recipes.json
+ menu-overrides.tsv and only emitted craftable Food/Chems (plus an
`include_without_cobj` escape hatch). That made menu-overrides.tsv a second
source of truth that drifted out of sync with BnB_Menu_Items.tsv.

Now the master TSV owns the menu. Every row becomes a menu_items[] entry with
its fine-grained category preserved (Alcohol / Canned / Chems / Serums /
Soda & Drinks / Condiments & Non-Perishable / Dishes & Teas / Ingredients /
Fish / Pre War Food / Magazines & Bobbleheads / Other). The staff portal's
classifyCraftingMenu() already understands these categories directly, and the
Order Log's finished-vs-break-down decision keys off the category string
returned from this file.

Output shape (unchanged for consumers):
    {
      "version":    "YYYY-MM-DD",
      "generated":  "YYYY-MM-DDTHH:MM:SS+00:00",
      "count":      <n>,
      "menu_items": [
        {
          "name":           "Beer",
          "category":       "Alcohol",
          "order_category": "chems",
          "build":          "",
          "price_xbox":     "10",
          "price_ps":       "20",
          "price_pc":       "20",
          "limit_new":      "50",
          "limit_existing": "100",
          "source":         "cobj" | "no_cobj"
        },
        ...
      ]
    }

Notes
-----
- `source` is kept for back-compat with any consumer that inspects it.
  "cobj"    = has a COBJ recipe (craftable)
  "no_cobj" = menu item without a recipe (looted / served / cut-content-but-still-sold)
- Missing prices raise `menu.missing_price` (ERROR). Missing limits raise
  `menu.missing_limit` (WARNING). Diagnostics land in dist/diagnostics.json
  under source="menu_items".
- menu-overrides.tsv is no longer read. If it's still in tsv/ it is ignored.

Usage:
  python build_menu_items_json.py
  python build_menu_items_json.py --data-dir tsv --outdir dist
"""

import argparse
import csv
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagnostics import Diagnostics  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MASTER_TSV = "BnB_Menu_Items.tsv"
COBJ_JSON = "cobj-recipes.json"
OUTPUT_JSON = "menu-items.json"

REQUIRED_MASTER_COLS = {
    "menu category",
    "name",
    "build",
    "price_xbox",
    "price_ps",
    "price_pc",
    "limit_new",
    "limit_existing",
    "order category",
}

PRICE_FIELDS = ("price_xbox", "price_ps", "price_pc")
LIMIT_FIELDS = ("limit_new", "limit_existing")


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
# Loaders
# ---------------------------------------------------------------------------

def load_cobj_names(path: str, diag: Diagnostics) -> Set[str]:
    """Return the set of recipe names (lowercased) for craftability tagging.

    We don't need the ingredient counts here — that's the sync step's job.
    This builder only cares "does a recipe exist for this item?" so the
    `source` field can reflect cobj vs no_cobj.
    """
    if not os.path.isfile(path):
        diag.warning(
            "menu_items.cobj.missing",
            "cobj-recipes.json not found - every item will be tagged no_cobj.",
            detail=path,
        )
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        diag.error(
            "menu_items.cobj.unreadable",
            "cobj-recipes.json could not be parsed - every item will be tagged no_cobj.",
            detail=str(exc),
        )
        return set()
    recipes = data.get("recipes", {}) or {}
    return {str(k).strip().lower() for k in recipes.keys() if clean(k)}


def load_master_rows(path: str, diag: Diagnostics) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        diag.error(
            "menu_items.master.missing",
            f"{MASTER_TSV} not found - no menu items will be emitted.",
            detail=path,
        )
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            diag.error(
                "menu_items.master.empty",
                f"{MASTER_TSV} has no header row.",
                detail=path,
            )
            return []
        missing = REQUIRED_MASTER_COLS - set(reader.fieldnames)
        if missing:
            diag.error(
                "menu_items.master.bad_schema",
                f"{MASTER_TSV} is missing required columns: {sorted(missing)}",
                detail=f"Found columns: {reader.fieldnames}",
            )
            return []
        return [dict(r) for r in reader]


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_menu(
    rows: List[Dict[str, str]],
    cobj_names: Set[str],
    diag: Diagnostics,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    for i, row in enumerate(rows, start=2):  # line 1 is the header
        name = clean(row.get("name", ""))
        category = clean(row.get("menu category", ""))

        if not name:
            diag.warning(
                "menu_items.row.missing_name",
                f"Row {i}: skipping - no item name set.",
                detail=f"category={category!r}",
            )
            continue
        if not category:
            diag.warning(
                "menu_items.row.missing_category",
                f"Row {i}: {name!r} skipped - no menu category set.",
                detail=name,
            )
            continue

        # De-dupe on name (case-insensitive). The master TSV should already be
        # unique but the sync step or a hand-edit could introduce duplicates.
        key = name.lower()
        if key in seen_keys:
            diag.info(
                "menu_items.row.duplicate",
                f"Row {i}: duplicate {name!r} - keeping first.",
                detail=name,
            )
            continue
        seen_keys.add(key)

        source = "cobj" if key in cobj_names else "no_cobj"

        item: Dict[str, Any] = {
            "name":           name,
            "category":       category,
            "order_category": clean(row.get("order category", "")).lower(),
            "build":          clean(row.get("build", "")),
            "price_xbox":     clean(row.get("price_xbox", "")),
            "price_ps":       clean(row.get("price_ps", "")),
            "price_pc":       clean(row.get("price_pc", "")),
            "limit_new":      clean(row.get("limit_new", "")),
            "limit_existing": clean(row.get("limit_existing", "")),
            "source":         source,
        }

        missing_prices = [f for f in PRICE_FIELDS if not item[f]]
        if missing_prices:
            diag.error(
                "menu_items.missing_price",
                f"{name!r} ({category}) is missing price for: "
                f"{', '.join(p.replace('price_', '') for p in missing_prices)}",
                detail=name,
                context={"name": name, "category": category, "missing_fields": missing_prices},
            )
        missing_limits = [f for f in LIMIT_FIELDS if not item[f]]
        if missing_limits:
            diag.warning(
                "menu_items.missing_limit",
                f"{name!r} ({category}) is missing limit for: "
                f"{', '.join(l.replace('limit_', '') for l in missing_limits)}",
                detail=name,
                context={"name": name, "category": category, "missing_fields": missing_limits},
            )

        out.append(item)

    # Stable sort: category then name
    out.sort(key=lambda r: (r["category"].lower(), r["name"].lower()))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build menu-items.json from the master BnB_Menu_Items.tsv",
    )
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Output dir (default: dist)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="menu_items", outdir=args.outdir)

    cobj_path = os.path.join(args.outdir, COBJ_JSON)
    cobj_names = load_cobj_names(cobj_path, diag)

    master_path = os.path.join(args.data_dir, MASTER_TSV)
    rows = load_master_rows(master_path, diag)
    menu_items = build_menu(rows, cobj_names, diag)

    from collections import Counter as _Counter
    cat_counts = _Counter(i["category"] for i in menu_items)
    cat_summary = "  ".join(f"{cat}: {ct}" for cat, ct in cat_counts.most_common())
    print(f"Menu items: {len(menu_items)}  ({cat_summary})", file=sys.stderr)

    src_counts = _Counter(i["source"] for i in menu_items)
    print(
        f"  source split: cobj={src_counts.get('cobj', 0)}  "
        f"no_cobj={src_counts.get('no_cobj', 0)}",
        file=sys.stderr,
    )

    diag.info(
        "menu_items.build.summary",
        f"Wrote {len(menu_items)} menu items.",
        context={
            "total": len(menu_items),
            "categories": dict(cat_counts),
            "source": dict(src_counts),
        },
    )

    output: Dict[str, Any] = {
        "version":    today_ymd(),
        "generated":  now_iso(),
        "count":      len(menu_items),
        "menu_items": menu_items,
    }
    output_path = os.path.join(args.outdir, OUTPUT_JSON)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Wrote {output_path}", file=sys.stderr)
    except Exception as exc:
        diag.error("menu_items.write.failed", "Failed to write menu-items.json", detail=str(exc))
        diag.save()
        sys.exit(1)

    diag.save()


if __name__ == "__main__":
    main()
