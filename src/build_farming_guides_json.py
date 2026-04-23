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
# KYWD refs includes CMPO (Component) records — used to resolve ingredient
# EDIDs like "c_Wood" to their pretty names ("Wood").
KYWD_REFS_GLOBS = [
    "KYWD_Export_Apr_2026_Refs.tsv",
    "KYWD_Export_March_2026_Refs.tsv",
]
# MISC records cover raw materials / containers used as recipe components
# (e.g. Cannery_Clean_Can -> "Clean Can") that don't appear in ALCH.
MISC_GLOBS = [
    "MISC_Export_Apr_2026.tsv",
    "MISC_Export_Mar_2026.tsv",
]

# Workbench EDID -> (category slug, friendly label). Labels mirror the
# in-game station names so "Workbench: Cooking Station" reads naturally.
WORKBENCH_CATEGORY = [
    (re.compile(r"Cook",    re.IGNORECASE), ("cooking",  "Cooking Station")),
    (re.compile(r"Brew",    re.IGNORECASE), ("brewing",  "Brewing Station")),
    (re.compile(r"Cannery", re.IGNORECASE), ("cannery",  "Cannery Station")),
    (re.compile(r"Chem",    re.IGNORECASE), ("chem",     "Chemistry Station")),
    (re.compile(r"Tinker",  re.IGNORECASE), ("tinker",   "Tinker's Workbench")),
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

# Mutation perk detection — which perk doubles the output?
CARNIVORE_KWS = {"IngredientTypeMeat", "IngredientTypeEgg", "ObjectTypeFish"}
HERBIVORE_KWS = {
    "IngredientTypeFruit", "IngredientTypeVegetable", "IngredientTypeHerb",
    "IngredientTypeFungus", "PlantTypeBerry", "PlantTypeVegetable",
    "PlantTypeFungus", "PlantTypeMutfruit", "PlantTypeFlowers",
    "PlantTypeSlipperCactus", "ObjectTypeFungus", "ObjectTypeFlowers",
}

# Pretty names for ingredient EDIDs that lack a FULL name in the ALCH table
# (usually MISC items used as recipe components).
INGREDIENT_NAME_OVERRIDES: Dict[str, str] = {
    "Cannery_Clean_Can": "Clean Can",
}

# Base spoil time per HealthCurve / spoil-curve FormID. Sourced from the
# community-maintained spoilage spreadsheet (01/01/2022 revision) — the
# HealthCurve that each ALCH references doubles as its spoil-category
# identifier, and every item in the same category shares a base time.
# Values are in minutes at the default 1.0× spoil rate (no storage bonus,
# no perks). Stationary storage is 1.0×; fast travel reduces to 0×.
SPOIL_CURVE_TIME: Dict[str, Tuple[int, str]] = {
    "003D8EB8": (360, "Tasty Meat Large"),
    "003D8EB9": (360, "Tasty Vegetable Large"),
    "003D8EE8": (90,  "Raw Vegetable Large"),
    "003D8EE9": (468, "Tasty Fruit Large"),
    "003D91C7": (108, "Raw Fruit Large"),
    "003D91C8": (90,  "Raw Meat Large"),
    "003D97F4": (288, "Cooked Meat Large"),
    "003D97F5": (288, "Cooked Vegetable Large"),
    "003D9FBB": (374, "Cooked Fruit Large"),
    "0052B5B9": (240, "Cooked Vegetable"),
    "0052B5BA": (312, "Cooked Fruit"),
    "0052B5BB": (240, "Cooked Meat"),
    "0052B5BC": (90,  "Raw Fruit"),
    "0052B5BD": (75,  "Raw Meat"),
    "0052B5BE": (75,  "Raw Vegetable"),
    "0052B5BF": (300, "Tasty Meat"),
    "0052B5C0": (390, "Tasty Fruit"),
    "0052B5C1": (300, "Tasty Vegetable"),
    # Brewing (ferment) — separate mechanic but rendered the same way.
    "003F0B3D": (60,  "Brewing Short Fermentation"),
    "003F0B3E": (120, "Brewing Long Fermentation"),
}

# Ways to extend spoil time. Each entry is (pct_reduction, label, source).
# "Reduced spoil rate" extends time: t = base × (1 + pct/100).
SPOIL_REDUCTIONS: List[Tuple[int, str, str]] = [
    (30, "Good with Salt 1",          "Luck perk card"),
    (50, "Refrigerated Backpack mod", "Possum vendor"),
    (60, "Good with Salt 2 / Fridge", "Luck perk / Atom Shop"),
    (90, "Good with Salt 3",          "Luck perk card"),
]

# Regions used in regional-recipe variants (Disease Cure, Healing Salve,
# Detoxing Salve). Listed longest-first so the alternation in REGION_RE
# matches "TheMire" before "Mire" and avoids false positives.
REGION_PRETTY: Dict[str, str] = {
    "TheMire":      "The Mire",
    "CranberryBog": "Cranberry Bog",
    "SavageDivide": "Savage Divide",
    "ToxicValley":  "Toxic Valley",
    "AshHeap":      "Ash Heap",
    "Forest":       "Forest",
    "Mire":         "The Mire",
}

# Match a region token only at genuine word boundaries — either an
# underscore/start-of-string on the left and underscore/end/uppercase on
# the right, or a camelCase transition (lowercase->Uppercase). This is
# what stops "Mire" from matching inside "Mirelurk".
REGION_RE = re.compile(
    r'(?<=_|[a-z])'
    r'(TheMire|CranberryBog|SavageDivide|ToxicValley|AshHeap|Forest|Mire)'
    r'(?=_|$|[A-Z])'
)

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


def mutation_perk(kws: List[str]) -> Optional[str]:
    """Determine which mutation perk benefits from this item's keywords."""
    is_carn = bool(CARNIVORE_KWS & set(kws))
    is_herb = bool(HERBIVORE_KWS & set(kws))
    if is_carn and is_herb:
        return "Carnivore, Herbivore (Hybrid)"
    if is_carn:
        return "Carnivore"
    if is_herb:
        return "Herbivore"
    return None


def workbench_category(bnam_edid: str, bnam_full: str) -> Tuple[str, str]:
    s = (bnam_edid or "") + " " + (bnam_full or "")
    for rx, val in WORKBENCH_CATEGORY:
        if rx.search(s):
            return val
    return ("other", bnam_full or bnam_edid or "Other")


def extract_region(recipe_edid: str) -> Optional[str]:
    """Return the pretty region name if this recipe is a regional variant.

    Disease Cure, Healing Salve and Detoxing Salve all exist as one recipe
    per region (Forest/Toxic Valley/Savage Divide/Ash Heap/The Mire/Cranberry
    Bog) so the same output item can be crafted using local ingredients.
    The region is embedded in the recipe EDID.

    Uses word-boundary lookarounds so "Mire" doesn't match inside unrelated
    EDIDs like "MirelurkMeatTastyJerky".
    """
    if not recipe_edid:
        return None
    m = REGION_RE.search(recipe_edid)
    if not m:
        return None
    return REGION_PRETTY.get(m.group(1))


def cooking_duplicates_to_skip(cobj: List[Dict[str, str]]) -> set:
    """COBJ EDIDs of cooking-workbench recipes that duplicate a chem sibling.

    Items like Disease Cure and Healing Salve each have both a cooking and
    a chem/chemlab recipe with identical ingredients. The chem version is
    canonical; cooking siblings get filtered out so they don't render as
    near-identical cards on the ingredient-search page.
    """
    all_edids = {r.get("COBJ_EDID", "") for r in cobj if r.get("COBJ_EDID")}
    skip = set()
    for edid in all_edids:
        if not edid.endswith("_WorkbenchCooking"):
            continue
        base = edid[:-len("_WorkbenchCooking")]
        if (base + "_WorkbenchChemLab") in all_edids \
                or (base + "_WorkbenchChem") in all_edids:
            skip.add(edid)
    return skip


def compute_spoil_time(curve_fid: str) -> Optional[Dict[str, Any]]:
    """Build a spoil-time payload for an ALCH's HealthCurve FormID.

    Returns None if the curve isn't in the spoilage lookup — i.e. the
    item doesn't spoil (chems, water, etc.). Otherwise returns the base
    time plus pre-computed "extended" times for each common reduction
    tier (perks / fridge / backpack mod) so the UI doesn't have to do
    the math itself.
    """
    if not curve_fid:
        return None
    # FormIDs in the TSV are 8-char uppercase; normalise just in case.
    key = curve_fid.strip().upper().zfill(8)
    if key not in SPOIL_CURVE_TIME:
        return None
    base_min, curve_name = SPOIL_CURVE_TIME[key]
    tiers = [
        {
            "pct":     pct,
            "label":   label,
            "source":  source,
            "minutes": round(base_min * (1 + pct / 100.0), 2),
        }
        for pct, label, source in SPOIL_REDUCTIONS
    ]
    return {
        "base_minutes": base_min,
        "curve_type":   curve_name,
        "curve_formId": key,
        "tiers":        tiers,
    }


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


def load_cmpo_names(path: Optional[str]) -> Dict[str, str]:
    """EDID -> pretty name for CMPO (Component) records.

    Sourced from KYWD_Export_*_Refs.tsv, which lists every record that
    references a keyword alongside its signature. Filtering on
    RefSignature == "CMPO" gives us the full component list.
    """
    if not path:
        return {}
    out: Dict[str, str] = {}
    for row in read_tsv(path):
        if (row.get("RefSignature") or "").strip() != "CMPO":
            continue
        edid = (row.get("RefEDID") or "").strip()
        name = (row.get("RefName") or "").strip()
        if edid and name and edid not in out:
            out[edid] = name
    return out


def load_misc_names(path: Optional[str]) -> Dict[str, str]:
    """EDID -> FULL name for MISC records."""
    if not path:
        return {}
    out: Dict[str, str] = {}
    for row in read_tsv(path):
        edid = (row.get("EDID") or "").strip()
        name = (row.get("FULL") or "").strip()
        if edid and name and edid not in out:
            out[edid] = name
    return out


def load_weight_backfill(alch_paths: List[str]) -> Dict[str, float]:
    """FormID -> Weight from the first ALCH TSV that has weights populated.

    Works around the Apr 2026 xEdit export losing the Weight column on
    every row — we scan older exports (March 2026, Feb 2026, Dec 2025)
    and keep the first one with non-empty weights.
    """
    for path in alch_paths:
        if not path or not os.path.isfile(path):
            continue
        out: Dict[str, float] = {}
        for r in read_tsv(path):
            fid = (r.get("ALCH_FormID") or "").strip()
            w = safe_float(r.get("Weight", ""))
            if fid and w is not None:
                out[fid] = w
        if out:
            return out
    return {}


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(data_dir: str, outdir: str) -> str:
    alch_path = find_first(data_dir, ALCH_GLOBS)
    cobj_path = find_first(data_dir, COBJ_GLOBS)
    eff_path  = find_first(data_dir, ALCH_EFFECTS_GLOBS)
    kywd_refs_path = find_first(data_dir, KYWD_REFS_GLOBS)
    misc_path = find_first(data_dir, MISC_GLOBS)

    if not alch_path or not cobj_path:
        raise SystemExit(
            f"Missing required TSVs (ALCH={alch_path!r}, COBJ={cobj_path!r}) "
            f"under {data_dir!r}"
        )

    alch = read_tsv(alch_path)
    cobj = read_tsv(cobj_path)
    effects = read_tsv(eff_path) if eff_path else []

    # Ingredient-name lookups for recipe components that aren't in ALCH
    # (e.g. c_Wood -> "Wood", Cannery_Clean_Can -> "Clean Can").
    cmpo_names = load_cmpo_names(kywd_refs_path)
    misc_names = load_misc_names(misc_path)

    # Weight column in Apr 2026 ALCH is empty for all rows; backfill from the
    # newest older export that still has it.
    weight_backfill_paths = [
        os.path.join(data_dir, n) for n in ALCH_GLOBS
        if os.path.join(data_dir, n) != alch_path
    ]
    weight_backfill = load_weight_backfill(weight_backfill_paths)

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
        weight = safe_float(r.get("Weight", ""))
        if weight is None:
            weight = weight_backfill.get(fid)
        # Spoil time is keyed off the HealthCurve — the same FormID is
        # both "HP restored over level" and the spoil-category identifier.
        spoil_time = compute_spoil_time(r.get("ENIT_HealthCurve_FormID", ""))
        return {
            "edid":        r.get("ALCH_EDID", ""),
            "formId":      fid,
            "name":        r.get("FULL") or r.get("ALCH_EDID") or "",
            "type":        pretty_type(kws, r),
            "weight":      weight,
            "value":       safe_int(r.get("Value", "")),
            "spoils_to":   r.get("ENIT_SpoiledItem_FULL", "").strip() or None,
            "spoil_time":  spoil_time,
            "is_canned":   (r.get("ENIT_IsCanned") or "").strip().lower() == "true",
            "canned_base": r.get("ENIT_CannedBase_FULL", "").strip() or None,
            "mutation":    mutation_perk(kws),
            "effects":     eff_by_fid.get(fid, []),
        }

    # Pre-compute the set of cooking-workbench COBJ EDIDs that are just
    # duplicates of a chem sibling (Disease Cure, Healing Salve, etc.).
    cooking_dupes = cooking_duplicates_to_skip(cobj)

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

        # Skip cooking-workbench dupes when a chem sibling exists.
        if recipe_edid in cooking_dupes:
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

        # Regional recipes (e.g. Disease Cure - Ash Heap) get a suffix so
        # the card reads as a distinct recipe rather than "six Disease Cures".
        region = extract_region(recipe_edid)
        display_name = out_name
        if region:
            display_name = f"{out_name} - {region}"

        # Resolve the full ingredient list (for the dish breakdown).
        # Lookup priority: manual override -> ALCH FULL -> CMPO (component)
        # -> MISC FULL -> raw EDID as last-resort.
        all_ings_resolved: List[Dict[str, Any]] = []
        for ing_edid, qty in ings:
            ing_row = alch_by_edid.get(ing_edid)
            ing_name = (
                INGREDIENT_NAME_OVERRIDES.get(ing_edid)
                or (ing_row.get("FULL") if ing_row else None)
                or cmpo_names.get(ing_edid)
                or misc_names.get(ing_edid)
                or ing_edid
            )
            all_ings_resolved.append({
                "edid": ing_edid,
                "name": ing_name,
                "qty":  qty,
            })

        recipe_payload = {
            "recipe_edid":  recipe_edid,
            "recipe_name":  display_name,
            "region":       region,
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
