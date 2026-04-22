#!/usr/bin/env python3
"""
build_farming_guides_json.py
============================
Builds dist/farming_guides.json — the single data file consumed by the
Farming - Guides JS module (df-bnb-farming-guides.js) for ALL pages in
the category.

Pages currently fed by this builder:
  - ingredient-search   (the main one — every food/chem ingredient and
                         every recipe it goes into)
  - farming-planner     (UI-only for now; built later)
  - food-storage-mule   (static guide, no data needed yet)
  - food-that-can-be-canned (auto-list of canned items + their bases)

Inputs (latest "Apr_2026" exports preferred, falls back if missing):
  tsv/ALCH_Export_Apr_2026.tsv
  tsv/ALCH_Export_Apr_2026_Effects.tsv
  tsv/COBJ_Export_Apr_2026.tsv

Output:
  dist/farming_guides.json

The JSON shape is:
{
  "version":   "YYYY-MM-DD",
  "generated": "<ISO-8601 UTC>",
  "pages": {
    "ingredient-search": {
      "ingredients": [ {edid, formId, name, type, weight, value,
                        spoils_to, is_canned, canned_base, used_in:[
                          {recipe_edid, recipe_name, qty, workbench,
                           category, all_ingredients:[{edid,name,qty}],
                           output: {edid, name, formId, type, spoils_to,
                                    is_canned, weight, value, effects:[str]}
                          } ] } ]
    },
    "food-that-can-be-canned": {
      "items": [ {edid, formId, name, canned_edid, canned_name} ]
    }
  }
}

Usage:
  python build_farming_guides_json.py
  python build_farming_guides_json.py --data-dir tsv --outdir dist
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from cut_content import is_cut

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Order matters — first match wins when the same export exists for several
# months. Newer first.
ALCH_GLOBS = [
    "ALCH_Export_Apr_2026.tsv",
    "ALCH_Export_March_2026.tsv",
    "ALCH_Export_Feb_2026.tsv",
    "ALCH_Export_Dec_2025.tsv",
]
ALCH_EFFECTS_GLOBS = [
    "ALCH_Export_Apr_2026_Effects.tsv",
]
COBJ_GLOBS = [
    "COBJ_Export_Apr_2026.tsv",
    "COBJ_Export_March_2026.tsv",
    "COBJ_Export_Feb_2026.tsv",
    "COBJ_Export_Dec_2025.tsv",
]

# Workbench EDID -> friendly category label used in the UI pills.
WORKBENCH_CATEGORY = [
    (re.compile(r"Cook",   re.IGNORECASE), ("cooking",  "Cooking")),
    (re.compile(r"Brew",   re.IGNORECASE), ("brewing",  "Brewing")),
    (re.compile(r"Cannery",re.IGNORECASE), ("cannery",  "Cannery")),
    (re.compile(r"Chem",   re.IGNORECASE), ("chem",     "Chem Lab")),
    (re.compile(r"Tinker", re.IGNORECASE), ("tinker",   "Tinker's Workbench")),
]

# Food / ingredient keyword classes (any of these qualifies an ALCH as a
# food ingredient for the purposes of this builder).
FOOD_KW_PREFIXES = ("IngredientType", "PlantType", "FoodType", "MealType")
FOOD_KW_EXACT = {
    "ObjectTypeFood", "ObjectTypeBeverage", "ObjectTypeDrink",
    "ObjectTypeFish", "ObjectTypeFungus", "ObjectTypeWaterFood",
    "ObjectTypeCandy", "ObjectTypeCakesPies", "ObjectTypeBubblegum",
    "ObjectTypeFlowers",
}

# Ingredient "type" pretty labels — derived from the most-specific keyword
# present on the ALCH, in priority order.
TYPE_PRIORITY = [
    ("IsCanned",               "Canned"),
    ("MealTypePackaged",       "Packaged"),
    ("MealTypeGourmet",        "Gourmet"),
    ("MealTypeTasty",          "Tasty"),
    ("MealTypeCooked",         "Cooked"),
    ("MealTypeSteak",          "Steak"),
    ("MealTypeSoup",           "Soup"),
    ("MealTypeRaw",            "Raw"),
    ("IngredientTypeFruit",    "Fruit"),
    ("IngredientTypeVegetable","Vegetable"),
    ("IngredientTypeHerb",     "Herb"),
    ("IngredientTypeMeat",     "Meat"),
    ("IngredientTypeEgg",      "Egg"),
    ("IngredientTypeFungus",   "Fungus"),
    ("IngredientTypePastry",   "Pastry"),
    ("IngredientTypeFlavor",   "Flavor"),
    ("PlantTypeBerry",         "Berry"),
    ("PlantTypeFlowers",       "Flower"),
    ("PlantTypeFlux",          "Flux"),
    ("PlantTypeMutfruit",      "Mutfruit"),
    ("PlantTypeFungus",        "Fungus"),
    ("PlantTypeVegetable",     "Vegetable"),
    ("PlantTypeSlipperCactus", "Cactus"),
    ("ObjectTypeFish",         "Fish"),
    ("ObjectTypeFungus",       "Fungus"),
    ("ObjectTypeBeverage",     "Beverage"),
    ("ObjectTypeDrink",        "Drink"),
    ("ObjectTypeFlowers",      "Flower"),
    ("ObjectTypeCandy",        "Candy"),
    ("ObjectTypeCakesPies",    "Cakes & Pies"),
    ("ObjectTypeBubblegum",    "Gum"),
    ("ObjectTypeFood",         "Food"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def today_ymd() -> str:
    return dt.date.today().isoformat()


def find_first(data_dir: str, names: List[str]) -> Optional[str]:
    for n in names:
        p = os.path.join(data_dir, n)
        if os.path.isfile(p):
            return p
    return None


def read_tsv(path: str) -> List[Dict[str, str]]:
    if not path:
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


KW_RE = re.compile(r"^(.+?)\[([0-9A-Fa-f]+)\]$")


def parse_keywords(kw_flat: str) -> List[str]:
    if not kw_flat:
        return []
    out = []
    for token in kw_flat.split(" | "):
        m = KW_RE.match(token.strip())
        if m:
            out.append(m.group(1))
    return out


def is_food_ingredient(kws: List[str], row: Dict[str, str]) -> bool:
    if any(k in FOOD_KW_EXACT for k in kws):
        return True
    if any(k.startswith(FOOD_KW_PREFIXES) for k in kws):
        return True
    if (row.get("ENIT_IsCanned") or "").strip().lower() == "true":
        return True
    return False


def pretty_type(kws: List[str], row: Dict[str, str]) -> str:
    is_canned = (row.get("ENIT_IsCanned") or "").strip().lower() == "true"
    for tag, label in TYPE_PRIORITY:
        if tag == "IsCanned":
            if is_canned:
                return label
            continue
        if tag in kws:
            return label
    return "Food"


def workbench_category(bnam_edid: str, bnam_full: str) -> Tuple[str, str]:
    s = (bnam_edid or "") + " " + (bnam_full or "")
    for rx, val in WORKBENCH_CATEGORY:
        if rx.search(s):
            return val
    return ("other", bnam_full or bnam_edid or "Other")


def parse_fvpa(fvpa: str) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    if not fvpa:
        return out
    for token in fvpa.split("|"):
        token = token.strip()
        if not token or ":" not in token:
            continue
        edid, qty = token.rsplit(":", 1)
        try:
            n = int(qty)
        except ValueError:
            try:
                n = int(float(qty))
            except ValueError:
                n = 1
        out.append((edid.strip(), n))
    return out


def safe_float(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def safe_int(s: str) -> Optional[int]:
    f = safe_float(s)
    return int(f) if f is not None else None


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(data_dir: str, outdir: str) -> str:
    alch_path = find_first(data_dir, ALCH_GLOBS)
    cobj_path = find_first(data_dir, COBJ_GLOBS)
    eff_path  = find_first(data_dir, ALCH_EFFECTS_GLOBS)

    if not alch_path or not cobj_path:
        raise SystemExit(
            f"Missing required TSVs (ALCH={alch_path!r}, COBJ={cobj_path!r}) "
            f"under {data_dir!r}"
        )

    alch = read_tsv(alch_path)
    cobj = read_tsv(cobj_path)
    effects = read_tsv(eff_path) if eff_path else []

    # ALCH lookups
    alch_by_fid: Dict[str, Dict[str, str]] = {r["ALCH_FormID"]: r for r in alch}
    alch_by_edid: Dict[str, Dict[str, str]] = {r["ALCH_EDID"]: r for r in alch}

    # Effects -> by ALCH FormID -> list of MGEF full names (de-duped, ordered)
    eff_by_fid: Dict[str, List[str]] = defaultdict(list)
    for e in effects:
        fid = e.get("ALCH_FormID", "")
        name = (e.get("MGEF_FULL") or e.get("MGEF_EDID") or "").strip()
        if not fid or not name:
            continue
        if name not in eff_by_fid[fid]:
            eff_by_fid[fid].append(name)

    # Pre-compute ALCH "lite" record (used both for the ingredient list and
    # as the recipe's `output` block).
    def alch_lite(r: Dict[str, str]) -> Dict[str, Any]:
        kws = parse_keywords(r.get("Keywords_Flat", ""))
        fid = r.get("ALCH_FormID", "")
        return {
            "edid":        r.get("ALCH_EDID", ""),
            "formId":      fid,
            "name":        r.get("FULL") or r.get("ALCH_EDID") or "",
            "type":        pretty_type(kws, r),
            "weight":      safe_float(r.get("Weight", "")),
            "value":       safe_int(r.get("Value", "")),
            "spoils_to":   r.get("ENIT_SpoiledItem_FULL", "").strip() or None,
            "is_canned":   (r.get("ENIT_IsCanned") or "").strip().lower() == "true",
            "canned_base": r.get("ENIT_CannedBase_FULL", "").strip() or None,
            "effects":     eff_by_fid.get(fid, []),
        }

    # Build a map: ingredient EDID -> list of recipes that consume it.
    # Each recipe entry includes the FULL ingredient list (so the UI can show
    # "to make this dish you also need X, Y, Z").
    ingredient_uses: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for r in cobj:
        out_fid  = r.get("CNAM_FormID", "")
        out_name = r.get("CNAM_FULL", "") or r.get("CNAM_EDID", "")
        recipe_edid = r.get("COBJ_EDID", "")
        if not out_name and not out_fid:
            continue

        # Skip cut/test/debug recipes and cut outputs
        if is_cut(recipe_edid):
            continue
        out_edid = r.get("CNAM_EDID", "")
        if is_cut(out_edid):
            continue

        ings = parse_fvpa(r.get("FVPA", ""))
        if not ings:
            continue

        out_alch = alch_by_fid.get(out_fid)
        # Only interested in recipes whose OUTPUT is an ALCH (= consumable).
        if not out_alch:
            continue

        wb_cat, wb_label = workbench_category(
            r.get("BNAM_EDID", ""), r.get("BNAM_FULL", "")
        )

        # Resolve the full ingredient list (for the dish breakdown)
        all_ings_resolved: List[Dict[str, Any]] = []
        for ing_edid, qty in ings:
            ing_row = alch_by_edid.get(ing_edid)
            ing_name = ing_row.get("FULL") if ing_row else ing_edid
            all_ings_resolved.append({
                "edid": ing_edid,
                "name": ing_name or ing_edid,
                "qty":  qty,
            })

        recipe_payload = {
            "recipe_edid":  recipe_edid,
            "recipe_name":  out_name,
            "workbench":    wb_label,
            "category":     wb_cat,
            "output":       alch_lite(out_alch),
            "all_ingredients": all_ings_resolved,
        }

        # Add this recipe to every one of its ingredients (with that
        # ingredient's qty stamped on top).
        for ing_edid, qty in ings:
            entry = dict(recipe_payload)
            entry["qty"] = qty
            ingredient_uses[ing_edid].append(entry)

    # Build the master ingredient list — only ingredients that
    #   (a) actually appear as inputs to a recipe AND
    #   (b) qualify as a "food" item by keyword/spoil
    ingredients: List[Dict[str, Any]] = []
    for ing_edid, uses in ingredient_uses.items():
        if is_cut(ing_edid):
            continue
        row = alch_by_edid.get(ing_edid)
        if not row:
            continue
        kws = parse_keywords(row.get("Keywords_Flat", ""))
        if not is_food_ingredient(kws, row):
            continue

        lite = alch_lite(row)
        # Sort recipes alphabetically for stable UI rendering
        uses_sorted = sorted(uses, key=lambda r: (r["recipe_name"] or "").lower())
        lite["used_in"] = uses_sorted
        lite["used_in_count"] = len(uses_sorted)
        ingredients.append(lite)

    # Sort ingredients alphabetically by display name
    ingredients.sort(key=lambda i: (i["name"] or "").lower())

    # Food-that-can-be-canned page list
    canned_items: List[Dict[str, Any]] = []
    for r in alch:
        if (r.get("ENIT_IsCanned") or "").strip().lower() != "true":
            continue
        if is_cut(r.get("ALCH_EDID", "")):
            continue
        canned_items.append({
            "edid":         r.get("ALCH_EDID", ""),
            "formId":       r.get("ALCH_FormID", ""),
            "name":         r.get("FULL") or r.get("ALCH_EDID") or "",
            "canned_base":  r.get("ENIT_CannedBase_FULL") or None,
            "canned_base_edid": r.get("ENIT_CannedBase_EDID") or None,
        })
    canned_items.sort(key=lambda i: (i["name"] or "").lower())

    payload = {
        "version":   today_ymd(),
        "generated": now_iso(),
        "sources": {
            "alch":         os.path.basename(alch_path),
            "cobj":         os.path.basename(cobj_path),
            "alch_effects": os.path.basename(eff_path) if eff_path else None,
        },
        "pages": {
            "ingredient-search": {
                "count_ingredients": len(ingredients),
                "ingredients":       ingredients,
            },
            "farming-planner": {
                # UI-only at this stage. Planner reads from localStorage queue
                # populated by the Ingredient Search checkboxes.
                "_status": "ui-only-v1",
            },
            "food-storage-mule": {
                # Static guide page — no data needed yet.
                "_status": "static",
            },
            "food-that-can-be-canned": {
                "count_items": len(canned_items),
                "items":       canned_items,
            },
        },
    }

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, "farming_guides.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"[build_farming_guides_json] wrote {out_path}  "
        f"ingredients={len(ingredients)}  canned={len(canned_items)}"
    )
    return out_path


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="tsv",
                    help="Directory holding the xEdit TSV exports")
    ap.add_argument("--outdir",   default="dist",
                    help="Directory to write farming_guides.json into")
    args = ap.parse_args(argv)
    build(args.data_dir, args.outdir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
