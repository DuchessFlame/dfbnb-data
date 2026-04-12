from __future__ import annotations

"""
build_menu_items_json.py
========================
Generates dist/menu-items.json by merging:
  1. dist/cobj-recipes.json       (source of truth for craftable items + category)
  2. tsv/menu-overrides.tsv       (BnB business data: prices, limits, build tags,
                                   category overrides, include_without_cobj)

Design goals:
  - Every patch, the COBJ TSV is refreshed -> cobj-recipes.json regenerates ->
    this builder runs and any NEW craftable food/chem items automatically appear
    in menu-items.json with empty prices (flagged via diagnostics as "new_item"
    and "missing_price") so you know what to price.
  - Items that are NOT in COBJ (e.g. Addictol, Halloween Candy, looted items)
    can still be sold by setting include_without_cobj=1 in the overrides TSV.
  - The category can be overridden per item via category_override (e.g. when
    the game keyword says Food but BnB categorises it as Chem).

Diagnostics are written to dist/diagnostics.json under the "menu_items" section.

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
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagnostics import Diagnostics  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OVERRIDES_TSV = "menu-overrides.tsv"
COBJ_JSON = "cobj-recipes.json"
OUTPUT_JSON = "menu-items.json"

REQUIRED_OVERRIDE_COLS = {
    "name",
    "price_xbox",
    "price_ps",
    "price_pc",
    "limit_new",
    "limit_existing",
    "build",
    "category_override",
    "include_without_cobj",
    "skip",
}

PRICE_FIELDS = ("price_xbox", "price_ps", "price_pc")
LIMIT_FIELDS = ("limit_new", "limit_existing")

CATEGORY_LABEL = {
    "food": "Food",
    "chem": "Chem",
    "canned": "Canned",
    "alcohol": "Alcohol",
    "soda": "Soda",
    "other": "Other",
}


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


def is_truthy(s: Any) -> bool:
    v = clean(s).lower()
    return v in ("1", "true", "yes", "y", "t")


def load_cobj(path: str) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, Any]]]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"cobj-recipes.json not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    recipes = data.get("recipes", {}) or {}
    meta = data.get("recipe_meta", {}) or {}
    return recipes, meta


def load_overrides(path: str, diag: Diagnostics) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        diag.error(
            "menu.overrides.missing",
            "menu-overrides.tsv not found - no menu items will have prices.",
            detail=path,
        )
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            diag.error(
                "menu.overrides.empty",
                "menu-overrides.tsv has no header row.",
                detail=path,
            )
            return []
        missing = REQUIRED_OVERRIDE_COLS - set(reader.fieldnames)
        if missing:
            diag.error(
                "menu.overrides.bad_schema",
                f"menu-overrides.tsv is missing required columns: {sorted(missing)}",
                detail=f"Found columns: {reader.fieldnames}",
            )
        return [dict(r) for r in reader]


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

def build_menu(
    recipes: Dict[str, Dict[str, int]],
    meta: Dict[str, Dict[str, Any]],
    overrides: List[Dict[str, str]],
    diag: Diagnostics,
) -> List[Dict[str, Any]]:
    # Index overrides by canonical lower-case name
    by_name: Dict[str, Dict[str, str]] = {}
    duplicate_names: List[str] = []
    for row in overrides:
        n = clean(row.get("name", ""))
        if not n:
            continue
        key = n.lower()
        if key in by_name:
            duplicate_names.append(n)
            continue
        by_name[key] = row

    for n in duplicate_names:
        diag.warning(
            "menu.overrides.duplicate",
            f"Duplicate override row for {n!r} - keeping the first occurrence.",
            detail=n,
        )

    out: List[Dict[str, Any]] = []
    used_override_keys: set = set()
    skipped_count = 0

    # 1. Walk every COBJ food/chem item and emit it.
    #    Alcohol, soda, canned, serum, and other categories are handled
    #    via include_without_cobj overrides or category_override in the TSV.
    for name, ingredients in recipes.items():
        m = meta.get(name, {})
        cobj_cat = m.get("category", "other")
        if cobj_cat not in ("food", "chem"):
            continue

        ov = by_name.get(name.lower())
        if ov:
            used_override_keys.add(name.lower())
            if is_truthy(ov.get("skip", "")):
                skipped_count += 1
                continue
            final_cat = clean(ov.get("category_override", "")).lower() or cobj_cat
            if final_cat not in CATEGORY_LABEL:
                diag.warning(
                    "menu.override.bad_category",
                    f"Override category {final_cat!r} is not food/chem for {name!r}; falling back to COBJ category {cobj_cat!r}.",
                    detail=name,
                )
                final_cat = cobj_cat
            if final_cat != cobj_cat:
                diag.info(
                    "menu.category_conflict",
                    f"{name!r}: COBJ suggests {cobj_cat!r}, override sets {final_cat!r}.",
                    context={"name": name, "cobj": cobj_cat, "override": final_cat},
                )
            item = _make_item(name, final_cat, ov, source="cobj", diag=diag, meta=m)
        else:
            # New item from COBJ without a price entry yet
            diag.warning(
                "menu.new_item",
                f"New craftable item {name!r} has no entry in menu-overrides.tsv - add a row to set price/limits.",
                detail=name,
                context={"name": name, "category": cobj_cat, "keywords": m.get("bench_keywords", [])},
            )
            item = {
                "name": name,
                "category": CATEGORY_LABEL[cobj_cat],
                "build": "",
                "price_xbox": "",
                "price_ps": "",
                "price_pc": "",
                "limit_new": "",
                "limit_existing": "",
                "source": "cobj",
            }
            diag.error(
                "menu.missing_price",
                f"Menu item {name!r} has no prices set.",
                detail=name,
                context={"name": name, "category": cobj_cat},
            )
        out.append(item)

    # 2. Walk overrides flagged include_without_cobj for items we haven't emitted
    for ov in overrides:
        name = clean(ov.get("name", ""))
        if not name:
            continue
        key = name.lower()
        if key in used_override_keys:
            continue
        if is_truthy(ov.get("skip", "")):
            used_override_keys.add(key)
            skipped_count += 1
            continue
        if not is_truthy(ov.get("include_without_cobj", "")):
            # Orphan: override exists but item isn't in COBJ and user didn't flag it.
            diag.warning(
                "menu.orphan_override",
                f"Override {name!r} is not found in COBJ and not marked include_without_cobj. Item will not appear on the menu.",
                detail=name,
                context={"name": name},
            )
            continue
        used_override_keys.add(key)
        final_cat = clean(ov.get("category_override", "")).lower()
        if final_cat not in CATEGORY_LABEL:
            diag.error(
                "menu.include_without_cobj.bad_category",
                f"{name!r} is flagged include_without_cobj but category_override is missing/invalid ({final_cat!r}).",
                detail=name,
                context={"name": name, "category_override": clean(ov.get("category_override", ""))},
            )
            continue
        item = _make_item(name, final_cat, ov, source="override_only", diag=diag, meta=None)
        out.append(item)

    if skipped_count:
        diag.info(
            "menu.skipped",
            f"Skipped {skipped_count} items marked skip=1 in menu-overrides.tsv.",
            context={"count": skipped_count},
        )

    # Sort for stable output: category then name
    out.sort(key=lambda r: (r["category"], r["name"].lower()))
    return out


def _make_item(
    name: str,
    category_lc: str,
    ov: Dict[str, str],
    source: str,
    diag: Diagnostics,
    meta: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    item = {
        "name": name,
        "category": CATEGORY_LABEL.get(category_lc, category_lc.title() or "Food"),
        "build": clean(ov.get("build", "")),
        "price_xbox": clean(ov.get("price_xbox", "")),
        "price_ps": clean(ov.get("price_ps", "")),
        "price_pc": clean(ov.get("price_pc", "")),
        "limit_new": clean(ov.get("limit_new", "")),
        "limit_existing": clean(ov.get("limit_existing", "")),
        "source": source,
    }

    missing_prices = [f for f in PRICE_FIELDS if not item[f]]
    if missing_prices:
        diag.error(
            "menu.missing_price",
            f"{name!r} is missing price for: {', '.join(p.replace('price_','') for p in missing_prices)}",
            detail=name,
            context={"name": name, "missing_fields": missing_prices},
        )
    missing_limits = [f for f in LIMIT_FIELDS if not item[f]]
    if missing_limits:
        diag.warning(
            "menu.missing_limit",
            f"{name!r} is missing limit for: {', '.join(l.replace('limit_','') for l in missing_limits)}",
            detail=name,
            context={"name": name, "missing_fields": missing_limits},
        )
    return item


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build menu-items.json from COBJ + overrides")
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Output dir (default: dist)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="menu_items", outdir=args.outdir)

    cobj_path = os.path.join(args.outdir, COBJ_JSON)
    try:
        recipes, meta = load_cobj(cobj_path)
    except Exception as exc:
        diag.error(
            "menu.cobj.missing",
            "Could not load cobj-recipes.json - run build_cobj_recipes_json.py first.",
            detail=str(exc),
        )
        diag.save()
        sys.exit(1)

    overrides_path = os.path.join(args.data_dir, OVERRIDES_TSV)
    overrides = load_overrides(overrides_path, diag)

    menu_items = build_menu(recipes, meta, overrides, diag)

    from collections import Counter as _Counter
    cat_counts = _Counter(i["category"] for i in menu_items)
    cat_summary = "  ".join(f"{cat}: {ct}" for cat, ct in cat_counts.most_common())
    print(f"Menu items: {len(menu_items)}  ({cat_summary})", file=sys.stderr)

    diag.info(
        "menu.build.summary",
        f"Wrote {len(menu_items)} menu items.",
        context={"total": len(menu_items), "categories": dict(cat_counts)},
    )

    output: Dict[str, Any] = {
        "version": today_ymd(),
        "generated": now_iso(),
        "count": len(menu_items),
        "menu_items": menu_items,
    }
    output_path = os.path.join(args.outdir, OUTPUT_JSON)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Wrote {output_path}", file=sys.stderr)
    except Exception as exc:
        diag.error("menu.write.failed", "Failed to write menu-items.json", detail=str(exc))
        diag.save()
        sys.exit(1)

    diag.save()


if __name__ == "__main__":
    main()
