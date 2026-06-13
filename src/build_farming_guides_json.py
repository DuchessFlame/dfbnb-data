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
    "ALCH_Export_June_2026.tsv",
    "ALCH_Export_Apr_2026.tsv",
    "ALCH_Export_March_2026.tsv",
    "ALCH_Export_Feb_2026.tsv",
    "ALCH_Export_Dec_2025.tsv",
]
ALCH_EFFECTS_GLOBS = [
    "ALCH_Export_June_2026_Effects.tsv",
    "ALCH_Export_Apr_2026_Effects.tsv",
]
COBJ_GLOBS = [
    "COBJ_Export_Jun_2026.tsv",
    "COBJ_Export_Apr_2026.tsv",
    "COBJ_Export_March_2026.tsv",
    "COBJ_Export_Feb_2026.tsv",
    "COBJ_Export_Dec_2025.tsv",
]
# KYWD refs includes CMPO (Component) records — used to resolve ingredient
# EDIDs like "c_Wood" to their pretty names ("Wood").
KYWD_REFS_GLOBS = [
    "KYWD_Export_Jun_2026_Refs.tsv",
    "KYWD_Export_Apr_2026_Refs.tsv",
    "KYWD_Export_March_2026_Refs.tsv",
]
# MISC records cover raw materials / containers used as recipe components
# (e.g. Cannery_Clean_Can -> "Clean Can") that don't appear in ALCH.
MISC_GLOBS = [
    "MISC_Export_Jun_2026.tsv",
    "MISC_Export_Apr_2026.tsv",
    "MISC_Export_Mar_2026.tsv",
]
# CURV points exports. Y at X=1 for each ALCH's HealthCurve equals the
# base spoil time in minutes — so we read this every build instead of
# maintaining a hardcoded table. The April regression was fixed by the
# rewritten ExportCURVToTSV.pas (external-JSON fallback via JASF), which
# now emits *_POINTS.tsv matching the March naming. The old
# *_CurvePoints.tsv (with the double-`.tsv` bug) and the March file are
# kept as fallbacks so previously-committed exports still work.
CURV_POINTS_GLOBS = [
    "CURV_Export_Jun_2026_POINTS.tsv",
    "CURV_Export_Apr_2026_POINTS.tsv",
    "CURV_Export_Apr_2026.tsv_CurvePoints.tsv",
    "CURV_Export_March_2026_POINTS.tsv",
]
# CURV record tables — the source of truth for curve EDIDs and their
# associated JsonFileName (when exported). Used to resolve each curve
# to its raw JSON file on disk when the POINTS TSV doesn't include it
# (common for non-Large food curves whose CURV record has no JASF_Path).
CURV_RECORD_GLOBS = [
    "CURV_Export_Jun_2026_CURV.tsv",
    # Preferred new naming (matches tools\build-curv-points.ps1 output)
    "CURV_Export_Apr_2026_CURV.tsv",
    # Legacy double-extension name from the old xEdit script — kept as
    # fallback so previously-committed exports still build.
    "CURV_Export_Apr_2026.tsv_CURV.tsv",
    "CURV_Export_March_2026.tsv",
    "CURV_Export_Feb_2026.tsv",
    "CURV_Export_Dec_2025.tsv",
]
# GLOB table — holds the storage-activator spoil-rate multipliers
# (refrigerator / freezer / fermenter). GLOB.FLTV is the actual numeric
# value of the global.
GLOB_GLOBS = [
    "GLOB_Export_Jun_2026.tsv",
    "GLOB_Export_Apr_2026.tsv",
    "GLOB_Export_March_2026.tsv",
    "GLOB_Export_Feb_2026.tsv",
    "GLOB_Export_Dec_2025.tsv",
]
# SPEL effects — per-rank magnitudes for perk abilities. Good with Salt
# is encoded as a single SPEL (AbPerkGoodWithSalt) with one effect entry
# per rank (index 0 = Rank 1, index 1 = Rank 2). Magnitudes are stored
# as fractions (0.45 = 45%).
SPEL_EFFECTS_GLOBS = [
    "SPEL_Export_June_2026_EFFECTS.tsv",
    "SPEL_Export_Apr_2026_EFFECTS.tsv",
    "SPEL_Export_March_2026_EFFECTS.tsv",
    "SPEL_Export_Feb_2026_EFFECTS.tsv",
]
# ENCH records — the Refrigerated Backpack mod's effect magnitude lives
# on its ENCH. The Apr export doesn't include ENCH so we fall back to
# the March one (the last one that existed when this was last exported).
ENCH_GLOBS = [
    "ENCH_Export_Jun_2026.tsv",
    "ENCH_Export_Apr_2026.tsv",
    "ENCH_Export_March_2026.tsv",
    "ENCH_Export_Feb_2026.tsv",
    "ENCH_Export_Dec_2025.tsv",
]
# MGEF descriptions — used to parse inline "does not stack" hints and
# other caveats that the game surfaces to players.
MGEF_GLOBS = [
    "MGEF_Export_Jun_2026.tsv",
    "MGEF_Export_Apr_2026.tsv",
    "MGEF_Export_March_2026.tsv",
    "MGEF_Export_Feb_2026.tsv",
    "MGEF_Export_Dec_2025.tsv",
]
# BOOK records — used to resolve plan names ("Recipe: Blight Soup") for the
# Food-That-Can-Be-Canned page. Each canned variant is unlocked by reading
# the BASE food's plan (e.g. learning "Recipe: Blight Soup" also unlocks
# Canned Blight Soup). Some bases are learned by default with no plan;
# canned variants whose COBJ EDID starts with SCORE_S{N}_ are additionally
# gated behind a Scoreboard season.
BOOK_GLOBS = [
    "BOOK_Export_Jun_2026.tsv",
    "BOOK_Export_May_2026.tsv",
    "BOOK_Export_Apr_2026.tsv",
    "BOOK_Export_March_2026.tsv",
]
# Seasons reference table — maps SCORE_S{N} keys to the human-readable
# season name (e.g. "Appalachia Under Siege" for Season 25).
SEASONS_TSV = "fallout76_seasons.tsv"


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

# Emergency fallback for spoil base times, used ONLY when the CURV points
# TSV is missing/empty. The real source is the curve itself — each
# ALCH's HealthCurve has Y=minutes at X=1 (the same curve doubles as the
# HP-per-level scaling for X>1). Reading straight from CURV points means
# Bethesda rebalances propagate to the site the moment you re-export,
# including any new food categories they introduce.
#
# This table is snapshot-from-2022 data; it will drift from the engine
# over time. Keep it small and prefer the live lookup.
SPOIL_BASE_MINUTES_FALLBACK: Dict[str, int] = {
    "CT_FoodHealth_RawMeat":              75,
    "CT_FoodHealth_RawVegetable":         75,
    "CT_FoodHealth_RawFruit":             90,
    "CT_FoodHealth_RawMeatLarge":         90,
    "CT_FoodHealth_RawVegetableLarge":    90,
    "CT_FoodHealth_RawFruitLarge":        108,
    "CT_FoodHealth_CookedMeat":           240,
    "CT_FoodHealth_CookedVegetable":      240,
    "CT_FoodHealth_CookedMeatLarge":      288,
    "CT_FoodHealth_CookedVegetableLarge": 288,
    "CT_FoodHealth_TastyMeat":            300,
    "CT_FoodHealth_TastyVegetable":       300,
    "CT_FoodHealth_CookedFruit":          312,
    "CT_FoodHealth_TastyMeatLarge":       360,
    "CT_FoodHealth_TastyVegetableLarge":  360,
    "CT_FoodHealth_CookedFruitLarge":     374,
    "CT_FoodHealth_TastyFruit":           390,
    "CT_FoodHealth_TastyFruitLarge":      468,
}

# Spoil-time modifier sources. Each entry points at a real record whose
# magnitude gets pulled live from the TSVs at build time. Only the
# label/scope/kind/edid mapping is hardcoded; the numeric magnitudes
# come from the game data so Bethesda rebalances propagate automatically.
#
# kind: what kind of magnitude source to read
#   - "glob_rate"   : GLOB.FLTV interpreted as a spoil-rate multiplier.
#                     time = base / rate. rate 0 = never spoils.
#   - "spel_rank"   : one effect entry on a SPEL. "index" selects the
#                     rank (0-based). Magnitude is "% slower": rate
#                     reduced by magnitude, so time = base / (1 - mag).
#   - "ench_effect" : first effect magnitude on an ENCH. Same math as
#                     spel_rank (rate-reduction model).
#
# scope: which kinds of curve this tier applies to ("food", "brewing",
# or "any"). Keeps fermenter off food rows and fridges off brewing rows.
SPOIL_TIER_SOURCES: List[Dict[str, Any]] = [
    {"label": "Fridge",                    "kind": "glob_rate",  "scope": "food",
     "edid":  "WorkshopStashActivatorGroup_Refrigerator_SpoilRate"},
    {"label": "Freezer",                   "kind": "glob_rate",  "scope": "food",
     "edid":  "WorkshopStashActivatorGroup_Freezer_SpoilRate"},
    {"label": "Fermenter",                 "kind": "glob_rate",  "scope": "brewing",
     "edid":  "WorkshopStashActivatorGroup_Fermenter_SpoilRate"},
    {"label": "Good with Salt Rank 1",     "kind": "spel_rank",  "scope": "food",
     "edid":  "AbPerkGoodWithSalt", "index": 0},
    {"label": "Good with Salt Rank 2",     "kind": "spel_rank",  "scope": "food",
     "edid":  "AbPerkGoodWithSalt", "index": 1},
    {"label": "Refrigerated Backpack mod", "kind": "ench_effect","scope": "food",
     "edid":  "EnchBackpack_Effect_Refrigerated"},
]

# Small override for camelCase-to-pretty conversions that simple
# whitespace insertion can't produce correctly. Variants are otherwise
# discovered automatically from the COBJ EDIDs, so this table should
# almost never need to grow — only for genuine naming oddities.
VARIANT_PRETTY_OVERRIDE: Dict[str, str] = {
    # Healing Salve for The Mire uses "Mire" alone in the EDID, not "TheMire".
    "Mire": "The Mire",
}

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
    # Tolerate phantom files: os.path.isfile() can return True for
    # OneDrive-backed "Files On-Demand" placeholders whose content the
    # kernel can't actually materialize (seen on WSL/sandbox mounts).
    # Callers already skip the row/column work on an empty list, so
    # treating an unreadable file as empty is the right default.
    try:
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except FileNotFoundError:
        return []


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


_CAMEL_SPLIT_RE = re.compile(r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])')


def camel_to_words(s: str) -> str:
    """'CookedMeatLarge' -> 'Cooked Meat Large' (also handles ALLCAPS runs)."""
    return _CAMEL_SPLIT_RE.sub(" ", s).replace("_", " ").strip()


def pretty_curve_name(edid: str) -> str:
    """Derive a nice category name from a HealthCurve EDID.

    Works for the canonical form (CT_FoodHealth_TastyMeat), the Brewing_
    prefix, and prefixed variants (FFZ10_CT_FoodHealth_BioluminescentFluid,
    E08A_CT_FoodHealth_GulperVenom) that Bethesda adds with new content —
    so even curves we don't have a base-minutes value for still render a
    readable category instead of an ugly raw EDID.
    """
    if not edid:
        return ""
    # Strip any prefix up to and including "CT_FoodHealth_"
    m = re.search(r'CT_FoodHealth_(.+)', edid)
    if m:
        return camel_to_words(m.group(1))
    if edid.startswith("Brewing_"):
        return camel_to_words(edid[len("Brewing_"):])
    return camel_to_words(edid)


def _curve_scope(curve_edid: str) -> str:
    """Bucket a curve EDID into 'food' / 'brewing' / 'any' so the tier
    filter knows which storage/perk/mod entries apply to each recipe.
    """
    lc = curve_edid.lower()
    if "brewing" in lc:
        return "brewing"
    if "foodhealth" in lc:
        return "food"
    return "any"


def _resolve_tier(
    spec: Dict[str, Any],
    base_minutes: float,
    globs: Dict[str, float],
    spel_mags: Dict[str, List[float]],
    ench_mags: Dict[str, float],
    warnings: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Turn a SPOIL_TIER_SOURCES spec into a concrete tier entry, or None
    if the referenced record is missing from the TSVs this build.
    """
    kind  = spec["kind"]
    edid  = spec["edid"]
    label = spec["label"]
    note  = spec.get("note")

    if kind == "glob_rate":
        rate = globs.get(edid)
        if rate is None:
            warnings.setdefault("missing_spoil_source", {})[f"GLOB:{edid}"] = (
                warnings.get("missing_spoil_source", {}).get(f"GLOB:{edid}", 0) + 1
            )
            return None
        # rate < 1  ⇒ slower ⇒ longer time. rate == 0 ⇒ never spoils.
        # rate > 1  ⇒ faster (fermenter speeds brewing up).
        if rate == 0:
            mins: Optional[float] = None
            reduction_pct: Optional[int] = 100
        elif rate == 1:
            mins = base_minutes
            reduction_pct = 0
        else:
            mins = round(base_minutes / rate, 2)
            # Negative = faster (rate > 1). Positive = slower (rate < 1).
            reduction_pct = round((1.0 - rate) * 100)
        tier = {
            "kind":     "storage",
            "label":    label,
            "rate_mult": rate,
            "reduction_pct": reduction_pct,
            "minutes":  mins,
            "source":   f"GLOB:{edid}",
        }

    elif kind == "spel_rank":
        idx = spec.get("index", 0)
        mags = spel_mags.get(edid) or []
        if idx >= len(mags):
            warnings.setdefault("missing_spoil_source", {})[f"SPEL:{edid}[{idx}]"] = (
                warnings.get("missing_spoil_source", {}).get(f"SPEL:{edid}[{idx}]", 0) + 1
            )
            return None
        mag = mags[idx]
        mins = None if mag >= 1.0 else round(base_minutes / (1.0 - mag), 2)
        tier = {
            "kind":     "perk",
            "label":    label,
            "magnitude": mag,
            "reduction_pct": round(mag * 100),
            "minutes":  mins,
            "source":   f"SPEL:{edid}[{idx}]",
        }

    elif kind == "ench_effect":
        mag = ench_mags.get(edid)
        if mag is None:
            warnings.setdefault("missing_spoil_source", {})[f"ENCH:{edid}"] = (
                warnings.get("missing_spoil_source", {}).get(f"ENCH:{edid}", 0) + 1
            )
            return None
        mins = None if mag >= 1.0 else round(base_minutes / (1.0 - mag), 2)
        tier = {
            "kind":     "mod",
            "label":    label,
            "magnitude": mag,
            "reduction_pct": round(mag * 100),
            "minutes":  mins,
            "source":   f"ENCH:{edid}",
        }

    else:
        return None

    if note:
        tier["note"] = note
    return tier


def compute_spoil_time(
    curve_edid: str,
    curve_fid: str,
    curve_base_times: Dict[str, float],
    globs: Dict[str, float],
    spel_mags: Dict[str, List[float]],
    ench_mags: Dict[str, float],
    warnings: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build a spoil-time payload keyed off the ALCH's HealthCurve EDID.

    Returns None if the item has no HealthCurve (chems, water, etc. —
    they don't spoil).

    Base minutes resolution order:
      1. Live CURV points data (Y at X=1)
      2. SPOIL_BASE_MINUTES_FALLBACK (2022 snapshot, safety net only)
      3. None — category still renders so the missing curve is visible
         in build warnings.

    Tiers are resolved generatively: each entry in SPOIL_TIER_SOURCES
    names a GLOB / SPEL / ENCH record whose magnitude is pulled from
    the TSV data. Curve scope (food vs brewing) filters out irrelevant
    tiers so fridges don't show on fermentable-brew recipes and vice
    versa.
    """
    if not curve_edid:
        return None  # no curve = doesn't spoil
    curve_type = pretty_curve_name(curve_edid)
    base = curve_base_times.get(curve_edid)
    source: Optional[str] = "curv_points" if base is not None else None
    if base is None:
        fb = SPOIL_BASE_MINUTES_FALLBACK.get(curve_edid)
        if fb is not None:
            base = float(fb)
            source = "fallback_2022"
            warnings.setdefault("using_spoil_fallback", {})[curve_edid] = (
                warnings.get("using_spoil_fallback", {}).get(curve_edid, 0) + 1
            )
    if base is None:
        warnings.setdefault("unknown_spoil_curves", {})[curve_edid] = (
            warnings.get("unknown_spoil_curves", {}).get(curve_edid, 0) + 1
        )
        return {
            "base_minutes": None,
            "curve_type":   curve_type,
            "curve_edid":   curve_edid,
            "curve_formId": (curve_fid or "").strip().upper().zfill(8) or None,
            "source":       None,
            "tiers":        [],
        }

    scope = _curve_scope(curve_edid)
    tiers: List[Dict[str, Any]] = []
    for spec in SPOIL_TIER_SOURCES:
        tier_scope = spec.get("scope", "any")
        if tier_scope != "any" and tier_scope != scope:
            continue
        tier = _resolve_tier(spec, base, globs, spel_mags, ench_mags, warnings)
        if tier is not None:
            tiers.append(tier)

    base_int = int(base) if base == int(base) else round(base, 2)
    return {
        "base_minutes": base_int,
        "curve_type":   curve_type,
        "curve_edid":   curve_edid,
        "curve_formId": (curve_fid or "").strip().upper().zfill(8) or None,
        "source":       source,
        "scope":        scope,
        "tiers":        tiers,
    }


def _common_affix(strings: List[str], reverse: bool = False) -> str:
    """Longest common prefix (or suffix) across a list of strings."""
    if not strings:
        return ""
    items = [s[::-1] for s in strings] if reverse else strings
    shortest = min(items, key=len)
    end = len(shortest)
    for i, ch in enumerate(shortest):
        if any(s[i] != ch for s in items):
            end = i
            break
    aff = shortest[:end]
    return aff[::-1] if reverse else aff


# Recipes in the Fishing/Seasonal/Burn families all follow a "...From{X}"
# naming convention where the part after "From" is the variant (usually a
# fish species). That lets us name these variants cleanly even when the
# common-affix extractor fails because the family uses multiple top-level
# prefixes (Fishing_*, Burn_*, SeasonalFish_*).
_FROM_VARIANT_RE = re.compile(r'From([A-Z][A-Za-z0-9_]*)')


def _extract_variant_token(
    edid: str, common_prefix: str, common_suffix: str,
) -> Optional[str]:
    """Return the best-effort variant-identifier token for an EDID.

    Strategy:
      1. If the EDID contains "From{Capitalised}", prefer that — works
         cleanly for Fish Bits (FromAlpineSawgill) and Summer Filet
         (FromFernskipper) even when top-level prefixes differ.
      2. Otherwise strip the group's common prefix and suffix.
    """
    m = _FROM_VARIANT_RE.search(edid)
    if m and m.group(1):
        return m.group(1)
    token = (
        edid[len(common_prefix):len(edid) - len(common_suffix)]
        if common_suffix else edid[len(common_prefix):]
    )
    token = token.strip("_")
    return token or None


def detect_recipe_variants(
    cobj: List[Dict[str, str]],
    alch_by_fid: Dict[str, Dict[str, str]],
) -> Tuple[set, Dict[str, str]]:
    """Discover duplicate/variant recipes generatively from the COBJ data.

    Groups every recipe by its ALCH output FormID and applies two passes:

    1. Dedup — within a group, recipes that share the exact same
       ingredient set (same items + qty) are collapsed. Chem-workbench
       wins over cooking-workbench; ties fall back to lowest COBJ EDID
       for deterministic output. The losers' EDIDs go into skip_edids.

    2. Variant labeling — among the survivors of a single output, any
       remaining siblings (different ingredient sets producing the same
       item) are variants. The EDID-differentiating token is extracted
       by stripping the longest common prefix & suffix shared across all
       surviving siblings in the group. That token becomes the display
       suffix ("Disease Cure - Ash Heap"), which means new regions or
       other variant axes auto-discover the next time Bethesda ships a
       recipe family without any code change.
    """
    by_out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in cobj:
        edid = r.get("COBJ_EDID", "")
        if not edid or is_cut(edid):
            continue
        if is_cut(r.get("CNAM_EDID", "")):
            continue
        out_fid = r.get("CNAM_FormID", "")
        if not out_fid or out_fid not in alch_by_fid:
            # Only food-output recipes participate (ingredient-search scope).
            continue
        by_out[out_fid].append(r)

    def wb_score(r: Dict[str, str]) -> int:
        b = (r.get("BNAM_EDID", "") or "") + (r.get("BNAM_FULL", "") or "")
        if "Chem" in b:    return 0   # canonical
        if "Cannery" in b: return 1
        if "Brew" in b:    return 2
        if "Cook" in b:    return 3   # usually the duplicate
        return 4

    skip_edids: set = set()
    variant_suffix: Dict[str, str] = {}

    for out_fid, group in by_out.items():
        if len(group) < 2:
            continue

        # Pass 1 — collapse identical ingredient sets
        by_ing: Dict[frozenset, List[Dict[str, str]]] = defaultdict(list)
        for r in group:
            ings = frozenset(parse_fvpa(r.get("FVPA", "")))
            by_ing[ings].append(r)
        for dupes in by_ing.values():
            if len(dupes) < 2:
                continue
            ordered = sorted(dupes, key=lambda x: (wb_score(x), x.get("COBJ_EDID", "")))
            for loser in ordered[1:]:
                skip_edids.add(loser.get("COBJ_EDID", ""))

        # Pass 2 — label surviving siblings with their EDID differentiator
        kept = [r for r in group if r.get("COBJ_EDID", "") not in skip_edids]
        if len(kept) < 2:
            continue
        edids = [r.get("COBJ_EDID", "") for r in kept]
        pref = _common_affix(edids)
        suf  = _common_affix(edids, reverse=True)
        for e in edids:
            token = _extract_variant_token(e, pref, suf)
            if token:
                variant_suffix[e] = token

    return skip_edids, variant_suffix


def pretty_variant_label(token: str) -> str:
    """Turn an EDID differentiator ('AshHeap', 'TheMire', 'SavageDivide')
    into a human label. camelCase gets spaces; known oddities (e.g. 'Mire'
    -> 'The Mire') pick up their override.
    """
    if token in VARIANT_PRETTY_OVERRIDE:
        return VARIANT_PRETTY_OVERRIDE[token]
    return camel_to_words(token)


def parse_fvpa(fvpa: str) -> List[Tuple[str, int]]:
    """Legacy shim. Returns (edid, qty) tuples — qty is the FVPA second
    field as-is (NOT curve-resolved). Used by the duplicate-detection
    pass where the literal token-set matters, not the in-game qty.
    For recipe-output ingredient lists, use parse_fvpa_with_curve +
    resolve_ingredient_qty so that CT_COBJ_Cooking_* curves are applied.
    """
    return [(edid, n) for edid, n, _curve in parse_fvpa_with_curve(fvpa)]


def parse_fvpa_with_curve(fvpa: str) -> List[Tuple[str, int, Optional[str]]]:
    """Parse an FVPA field into (edid, fvpa_value, curve_edid_or_None).

    FVPA format (May 2026 exports):
        c_Wood:3:CT_COBJ_Cooking_Food_5_Wood|...

    - Field 0: ingredient EDID
    - Field 1: integer. For ingredients with a CT_COBJ_Cooking_* curve
      in field 2, this is the **recipe tier** (the X coordinate to
      look up in the curve, NOT the literal qty). For ingredients with
      no curve, or with a COBJ_Workshop_* workshop-component lookup in
      field 2 (which doesn't modify qty), this IS the literal qty.
    - Field 2 (optional): the curve / lookup EDID. CT_COBJ_Cooking_*
      curves modify qty; COBJ_Workshop_* and empty don't.

    Callers that need the real ingredient qty should pass each row
    through resolve_ingredient_qty() with a curves dict from
    load_ingredient_qty_curves().
    """
    out: List[Tuple[str, int, Optional[str]]] = []
    if not fvpa:
        return out
    for token in fvpa.split("|"):
        token = token.strip()
        if not token or ":" not in token:
            continue
        parts = token.split(":")
        edid = parts[0].strip()
        qty_str = parts[1] if len(parts) > 1 else ""
        curve_edid = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
        try:
            n = int(qty_str)
        except ValueError:
            try:
                n = int(float(qty_str))
            except ValueError:
                n = 1
        out.append((edid, n, curve_edid))
    return out


def load_ingredient_qty_curves(
    curvetables_roots: List[str],
) -> Dict[str, Dict[int, int]]:
    """Return {curve_edid: {x: y}} for CT_COBJ_Cooking_* curves.

    These curves drive ingredient quantities for cooking COBJs — the
    integer in FVPA field 1 is the X coordinate (recipe tier 1-4),
    and Y at that X is the real ingredient qty. Without applying the
    curve, every Tasty (tier 3) recipe would emit "3× wood" instead
    of the in-game "1× wood".

    Source: data/curvetables/json/cobj/cooking/*.json — committed JSONs
    exported by ExportCURVToTSV.pas. Filename stem maps to curve EDID
    via PascalCase underscore-split rule
    (food_5_wood.json -> CT_COBJ_Cooking_Food_5_Wood).
    """
    out: Dict[str, Dict[int, int]] = {}
    for root in curvetables_roots:
        if not root or not os.path.isdir(root):
            continue
        cooking_dir = os.path.join(root, "cobj", "cooking")
        if not os.path.isdir(cooking_dir):
            continue
        for fname in os.listdir(cooking_dir):
            if not fname.lower().endswith(".json"):
                continue
            stem = os.path.splitext(fname)[0]
            pascal = "_".join(p.capitalize() for p in stem.split("_"))
            edid = "CT_COBJ_Cooking_" + pascal
            if edid in out:
                continue
            try:
                with open(os.path.join(cooking_dir, fname), encoding="utf-8") as f:
                    jd = json.load(f)
            except (OSError, ValueError):
                continue
            for pt in jd.get("curve", []):
                try:
                    x = int(pt.get("x"))
                    y = int(pt.get("y"))
                except (TypeError, ValueError):
                    continue
                out.setdefault(edid, {})[x] = y
    return out


def resolve_ingredient_qty(
    fvpa_value: int,
    curve_edid: Optional[str],
    qty_curves: Dict[str, Dict[int, int]],
    warnings: Dict[str, Any],
) -> int:
    """Return the real in-game qty for an FVPA ingredient row.

    Most callers should pass the (edid, fvpa_value, curve_edid) tuple
    from parse_fvpa_with_curve() through this. Behaviour:

    - No curve, or a non-cooking lookup like COBJ_Workshop_Steel:
      fvpa_value IS the literal qty -> return as-is.
    - CT_COBJ_Cooking_* curve present: fvpa_value is the X coordinate;
      look up the curve's Y at that X and return it. If the curve isn't
      loaded or doesn't have a point at X, fall back to fvpa_value and
      log a warning so the build surfaces the gap.
    """
    if not curve_edid or not curve_edid.startswith("CT_COBJ_Cooking_"):
        return fvpa_value
    curve = qty_curves.get(curve_edid)
    if not curve:
        warnings.setdefault("missing_qty_curve", {})[curve_edid] = (
            warnings.get("missing_qty_curve", {}).get(curve_edid, 0) + 1
        )
        return fvpa_value
    if fvpa_value in curve:
        return curve[fvpa_value]
    key = curve_edid + "@X=" + str(fvpa_value)
    warnings.setdefault("qty_curve_missing_tier", {})[key] = (
        warnings.get("qty_curve_missing_tier", {}).get(key, 0) + 1
    )
    return fvpa_value


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


def _discover_curvetables_roots(data_dir: str) -> List[str]:
    """Candidate paths for the curvetables JSON root. Tries, in order:

      1. FO76_CURVETABLES env override
      2. In-repo committed copy at <repo>/data/curvetables/json
         (works in CI — no external checkout needed)
      3. A sibling fo76-tools checkout next to the dfbnb-data repo
      4. The OneDrive GitHub layout (Duchess's canonical local path)

    Returns all existing candidates so the loader can merge them, though
    in practice one is enough. Missing paths are tolerated — the loader
    falls through to the POINTS TSV and then to the hardcoded 2022
    snapshot.
    """
    env = os.environ.get("FO76_CURVETABLES")
    repo_root = os.path.abspath(os.path.join(data_dir, os.pardir))
    repo_parent = os.path.abspath(os.path.join(repo_root, os.pardir))
    candidates = [
        env,
        os.path.join(repo_root,   "data", "curvetables", "json"),
        os.path.join(repo_parent, "fo76-tools", "misc", "curvetables", "json"),
        os.path.join(os.path.expanduser("~"), "OneDrive", "GitHub",
                     "fo76-tools", "misc", "curvetables", "json"),
    ]
    return [p for p in candidates if p and os.path.isdir(p)]


def _edid_lookup_keys(edid: str) -> List[str]:
    """Normalised keys to match a curve EDID against a raw JSON filename.

    Handles the three real cases:
      - 'CT_FoodHealth_TastyMeat'        <-> 'foodhealth_tastymeat.json'
      - 'Brewing_ShortFermentation'      <-> 'brewing_shortfermentation.json'
      - 'E08A_CT_FoodHealth_GulperVenom' <-> 'foodhealth_e08a_gulpervenom.json'
        (DLC prefix appears after the stem in the filename, before it in
        the EDID — so we emit both normalisations and let whichever one
        exists win.)
    """
    lc = edid.lower()
    keys = set()
    # Strip a leading CT_ (engine curve prefix, not part of filenames).
    stripped = re.sub(r'^ct_', '', lc)
    keys.add(stripped.replace("_", ""))
    # DLC-prefix swap: "prefix_ct_foodhealth_X" -> "foodhealth_prefix_X"
    m = re.match(r'^([a-z0-9]+)_ct_(foodhealth)_(.+)$', lc)
    if m:
        prefix, stem, rest = m.groups()
        keys.add(f"{stem}{prefix}{rest}".replace("_", ""))
    return list(keys)


def _read_first_curve_point(path: str) -> Optional[float]:
    """Return Y at X=1 from a crafting curve JSON, or None on any failure."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    points = data.get("curve") if isinstance(data, dict) else None
    if not isinstance(points, list) or not points:
        return None
    first = points[0]
    if not isinstance(first, dict):
        return None
    try:
        y = float(first.get("y"))
        x = float(first.get("x", 1.0))
    except (TypeError, ValueError):
        return None
    return y if x == 1.0 else None


def load_curve_base_times(
    curv_points_paths: List[str],
    curv_record_paths: List[str],
    curvetables_roots: List[str],
) -> Dict[str, float]:
    """Return {curve_edid: Y@X=1} by merging every available source.

    Sources (later sources fill gaps but never overwrite):

      1. CURV_Export_*_POINTS.tsv — xEdit-dumped points for curves with a
         JASF_Path in their CURV record. Covers the "Large" food variants
         and some DLC curves.

      2. Raw JSON files in fo76-tools/misc/curvetables/json/crafting —
         used for every CURV record EDID the POINTS export missed (non-
         Large food curves, brewing curves, any future additions). Each
         EDID is matched to a filename via normalised keys, so changes
         to casing or DLC-prefix ordering don't break the lookup.

    Both sources are canonical engine data — rebalances and new curves
    propagate on the next TSV re-export + build. Returns an empty dict
    only if no source is reachable, in which case compute_spoil_time
    falls back to the tiny 2022 hardcoded snapshot.
    """
    out: Dict[str, float] = {}

    # --- Source 1: CURV POINTS TSV (Y@X=1 per curve EDID) -----------
    for path in curv_points_paths:
        if not path or not os.path.isfile(path):
            continue
        hits = 0
        for row in read_tsv(path):
            edid = (row.get("EDID") or "").strip()
            if not edid:
                continue
            try:
                x = float(row.get("X", ""))
                y = float(row.get("Y", ""))
            except (TypeError, ValueError):
                continue
            if x == 1.0 and edid not in out:
                out[edid] = y
                hits += 1
        if hits:
            break  # first populated export wins

    # --- Source 2: raw crafting JSONs, matched via EDID lookup keys -
    # Build a filename index once across every curvetables root we find.
    # key: normalised lower-case filename stem, value: absolute path.
    json_index: Dict[str, str] = {}
    for root in curvetables_roots:
        if not root or not os.path.isdir(root):
            continue
        crafting = os.path.join(root, "crafting")
        if not os.path.isdir(crafting):
            continue
        for fname in os.listdir(crafting):
            if not fname.lower().endswith(".json"):
                continue
            stem = os.path.splitext(fname)[0].lower().replace("_", "")
            json_index.setdefault(stem, os.path.join(crafting, fname))

    if not json_index:
        return out

    # Walk the canonical EDID list from the CURV record TSV and try each
    # missing one against the JSON filename index.
    seen_edids = set()
    for path in curv_record_paths:
        if not path or not os.path.isfile(path):
            continue
        for row in read_tsv(path):
            edid = (
                row.get("CURV_EDID")
                or row.get("EDID")
                or ""
            ).strip()
            if not edid or edid in seen_edids or edid in out:
                continue
            seen_edids.add(edid)
            for key in _edid_lookup_keys(edid):
                fpath = json_index.get(key)
                if not fpath:
                    continue
                y = _read_first_curve_point(fpath)
                if y is not None:
                    out[edid] = y
                    break

    return out


def load_glob_values(paths: List[str]) -> Dict[str, float]:
    """Return {glob_edid: FLTV} from the first available GLOB export."""
    for path in paths:
        if not path or not os.path.isfile(path):
            continue
        out: Dict[str, float] = {}
        for row in read_tsv(path):
            edid = (row.get("EDID") or "").strip()
            fltv = safe_float(row.get("FLTV", ""))
            if edid and fltv is not None:
                out[edid] = fltv
        if out:
            return out
    return {}


def load_spel_effect_magnitudes(paths: List[str]) -> Dict[str, List[float]]:
    """Return {spel_edid: [magnitude_per_effect_index]}.

    Good with Salt's per-rank magnitudes are represented as multiple
    effect entries on a single SPEL (AbPerkGoodWithSalt) — index 0 is
    Rank 1's magnitude, index 1 is Rank 2's. We preserve the order from
    the TSV's "EffectIndex" column so rank numbering stays sane.
    """
    for path in paths:
        if not path or not os.path.isfile(path):
            continue
        tmp: Dict[str, Dict[int, float]] = defaultdict(dict)
        for row in read_tsv(path):
            edid = (row.get("SPEL_EDID") or "").strip()
            if not edid:
                continue
            try:
                idx = int(row.get("EffectIndex", "0"))
            except ValueError:
                continue
            mag = safe_float(row.get("EFIT_Magnitude", ""))
            if mag is None:
                continue
            tmp[edid][idx] = mag
        if tmp:
            return {
                edid: [by_idx[i] for i in sorted(by_idx.keys())]
                for edid, by_idx in tmp.items()
            }
    return {}


# Parses the "Effect_N" cells that ENCH exports use — each is a single
# string of "key=value;..." pairs. We only need Mag out of it.
_ENCH_EFFECT_MAG_RE = re.compile(r'\bMag=([0-9.eE+\-]+)')


def load_ench_effect_magnitudes(paths: List[str]) -> Dict[str, float]:
    """Return {ench_edid: first_effect_magnitude} from the Effect_1 cell."""
    for path in paths:
        if not path or not os.path.isfile(path):
            continue
        out: Dict[str, float] = {}
        for row in read_tsv(path):
            edid = (row.get("ENCH_EDID") or "").strip()
            if not edid:
                continue
            cell = row.get("Effect_1") or row.get("Effects_Flat") or ""
            m = _ENCH_EFFECT_MAG_RE.search(cell)
            if not m:
                continue
            try:
                out[edid] = float(m.group(1))
            except ValueError:
                continue
        if out:
            return out
    return {}


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
# Effect name / duration helpers
# ---------------------------------------------------------------------------

# Suffixes stripped from MGEF_FULL names for display. Order matters — first
# match wins so "Eating: Radiation Damage" is handled before the generic
# " Food" / " Drink" strip.
_EFFECT_RENAME: List[Tuple[re.Pattern, str]] = [
    # Exact matches first (engine-internal names)
    (re.compile(r'^Eating:\s*Radiation Damage$', re.I),             "Rads"),
    (re.compile(r'^Radiation Exposure$', re.I),                     "Rads"),
    (re.compile(r'^SURV_Food_Effect$', re.I),                       "Satisfy Hunger"),
    (re.compile(r'^SURV_Drink_Effect$', re.I),                      "Quench Thirst"),
    (re.compile(r'^SURV_DiseaseVector_Food_Effect$', re.I),         "Disease Chance"),
    (re.compile(r'^SURV_DiseaseVector_Drink_Effect$', re.I),        "Disease Chance"),
    (re.compile(r'^SURV_AddHunger_Potion_Effect$', re.I),           "Increase Hunger"),
    (re.compile(r'^SURV_AddThirst_Potion_Effect_NotChem$', re.I),   "Increase Thirst"),
    (re.compile(r'^SURV_DiseaseCure.*Effect.*$', re.I),             "Cure Disease"),
    (re.compile(r'^SURV_IncreaseDiseaseResistance.*Effect$', re.I), "Disease Resist"),
    (re.compile(r'^SURV_ReduceDiseaseResistance.*Effect$', re.I),   "Reduce Disease Resist"),
    # Prefix strips
    (re.compile(r'^Alcohol:\s*'),                                   ""),
    (re.compile(r'^Chem:\s*'),                                      ""),
    (re.compile(r'^Psycho:\s*'),                                    ""),
    (re.compile(r'^Food:\s*'),                                      ""),
    # Suffix strips
    (re.compile(r'\s+Food$'),                                       ""),
    (re.compile(r'\s+Drink$'),                                      ""),
    (re.compile(r'\s+Eating$'),                                     ""),
]


def _clean_effect_name(raw: str) -> str:
    """Turn an engine MGEF_FULL into a player-friendly display name."""
    s = raw.strip()
    for rx, repl in _EFFECT_RENAME:
        if repl == "":
            s = rx.sub("", s).strip()
        else:
            m = rx.match(s)
            if m:
                return repl
    return s


def _format_effect_duration(seconds: Optional[float]) -> Optional[str]:
    """Format an effect duration in seconds for display."""
    if seconds is None or seconds <= 0:
        return None
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    hours = s // 3600
    mins = (s % 3600) // 60
    parts = []
    if hours == 1:
        parts.append("1 hour")
    elif hours > 1:
        parts.append(f"{hours} hours")
    if mins:
        parts.append(f"{mins} min")
    return " ".join(parts) if parts else f"{s}s"


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Canned-page helpers: plan lookup + season detection
# ---------------------------------------------------------------------------

def load_book_plans_by_full(book_path: Optional[str]) -> Dict[str, Dict[str, str]]:
    """Index BOOK records by their FULL field, so we can look up a plan by
    its in-game name (e.g. "Recipe: Blight Soup"). The first BOOK with that
    FULL wins — duplicates are rare and the FULL string is the same anyway."""
    if not book_path or not os.path.exists(book_path):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    with open(book_path, encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for row in rdr:
            full = (row.get("FULL") or "").strip()
            if not full or full in out:
                continue
            out[full] = row
    return out


def load_seasons_by_key(seasons_path: Optional[str]) -> Dict[str, Dict[str, str]]:
    """Index fallout76_seasons.tsv by SeasonKey (e.g. SCORE_S25 ->
    {SeasonName: "Appalachia Under Siege", SeasonNumber: "25", ...})."""
    if not seasons_path or not os.path.exists(seasons_path):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    with open(seasons_path, encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for row in rdr:
            key = (row.get("SeasonKey") or "").strip()
            if key:
                out[key] = row
    return out


_SEASON_PREFIX_RE = re.compile(r"^SCORE_S?(\d+)_")


def detect_canned_season(
    cobj_edid: Optional[str],
    seasons_by_key: Dict[str, Dict[str, str]],
) -> Tuple[Optional[str], Optional[str]]:
    """Given a canned recipe's COBJ EDID, return (season_number_str,
    season_name) if it's a Scoreboard season reward, else (None, None)."""
    if not cobj_edid:
        return (None, None)
    m = _SEASON_PREFIX_RE.match(cobj_edid)
    if not m:
        return (None, None)
    num = m.group(1)
    # Normalise to SCORE_S{NN} (zero-padded) — the lookup key in the
    # seasons TSV. We try both padded and unpadded forms because older
    # COBJ EDIDs use SCORE_25_ while newer ones use SCORE_S25_.
    padded = f"SCORE_S{int(num):02d}"
    srow = seasons_by_key.get(padded) or seasons_by_key.get(f"SCORE_S{num}")
    season_name = (srow.get("SeasonName") if srow else None) or None
    return (num, season_name)


def lookup_canned_plan(
    base_name: Optional[str],
    book_by_full: Dict[str, Dict[str, str]],
) -> Optional[str]:
    """Return the BOOK FULL (e.g. "Recipe: Blight Soup") for the plan that
    teaches the base food, or None if the base recipe is learned by default
    (no plan exists)."""
    if not base_name:
        return None
    plan = book_by_full.get(f"Recipe: {base_name}")
    if plan:
        return (plan.get("FULL") or "").strip() or None
    return None


# ---------------------------------------------------------------------------
# Verdant Season buff chart (generative)
# ---------------------------------------------------------------------------
# Which farming buff works on which plant, derived from the engine exports:
#   - Verdant Season   -> LVLI list on GLOB SQ_VerdantSeasonExtraHarvestChance
#   - Backwoodsman 4   -> LVLI list on GLOB Backwoodsman04_Chance_Global
#   - Gamma Green Tea  -> flora carrying keyword SSE_PerkHarvestFlowers
#   - Green Thumb / Sam (Mechanic ally "Tune-Up") -> keyword PerkGreenThumbValidFlora
# Two tables: normal plants (from the Verdant/Backwoodsman lists + the Pot o'
# planters) and nuked plants (the FloraRad* forms). Row = [name, verdant,
# gamma_green_tea, backwoodsman, green_thumb, sam].
#
# Naming notes: the game's "Slipper Cactus" is "Prickeye" on-site; the
# "Angler Plant" LVLI points at a never-removed debug flora ("Lure Weed")
# that has one live spawn in the Tunnel of Love, so it stays under that name.
_VS_FN = {
    "AnglerPlant": "Lure Weed", "AshRose": "Ash Rose", "Aster": "Aster",
    "Blackberry": "Blackberry", "Blight": "Blight", "Bloodleaf": "Bloodleaf",
    "Carrot": "Carrot", "Corn": "Corn", "Cranberry": "Cranberry",
    "CranberryDiseased": "Diseased Cranberry", "Fern": "Fern Flower",
    "FeverBlossom": "Fever Blossom", "Firecap": "Firecap",
    "Firecracker": "Firecracker Berry", "FungusBrain": "Brain Fungus",
    "FungusGlowing": "Glowing Fungus", "FungusMegaSloth": "Megasloth Mushroom",
    "Ginseng": "Ginseng Root", "Gourd": "Gourd", "GutShroom01": "Gut Shroom",
    "Kaleidopore": "Kaleidopore", "Melon": "Melon", "Mutfruit": "Mutfruit",
    "PitcherPlant": "Pitcher Plant", "Pumpkin": "Pumpkin",
    "PumpkinVine01": "Pumpkin", "Razorgrain": "Razorgrain",
    "Rhododendron": "Rhododendron", "Sap": "Sap", "Siltbean": "Silt Bean",
    "SlipperCactus_Big": "Prickeye", "SlipperCactus_Medium": "Prickeye",
    "SlipperCactus_Small": "Prickeye", "SnapTail": "Snaptail",
    "SootFlower": "Soot Flower", "StarlightCreeper": "Starlight Creeper",
    "SwampPlant": "Swamp Plant", "SwampPod01": "Swamp Pod",
    "SwampPodFlower01": "Swamp Pod Flower", "Tarberry": "Tarberry",
    "Tato": "Tato", "Thistle": "Thistle", "ToxicSootFlower": "Toxic Soot Flower",
    "WildCarrotFlower": "Wild Carrot Flower", "WildGourdFlower": "Wild Gourd Flower",
    "WildMelonFlower": "Wild Melon Flower", "WildTatoFlower": "Wild Tato Flower",
    "Radlily": "Radlily", "CarnalWeeper": "Carnal Weeper", "CrystalCup": "Crystal Cup",
}

# --- Nuked plant -> original (base flora) name -------------------------------
# Pulled straight from the game data: each nuked plant is a FloraRad* form
# whose EDID stem (minus the FloraRad/Burn_/Trap/UseLPI wrappers + trailing
# digits) names its base flora. We resolve that stem against _VS_FN (the same
# canonical name map used for the normal-plant rows), so the original name
# comes from the files, not a hand-typed list. e.g. FloraRadFlashFern ->
# stem "flashfern" contains "fern" -> _VS_FN["Fern"] = "Fern Flower".
_VS_FN_NORM_CACHE = None


def _vs_fn_norm():
    global _VS_FN_NORM_CACHE
    if _VS_FN_NORM_CACHE is None:
        pairs = []
        for k, v in _VS_FN.items():
            kn = re.sub(r"[^a-z]", "", k.lower())
            if kn:
                pairs.append((kn, v))
        _VS_FN_NORM_CACHE = pairs
    return _VS_FN_NORM_CACHE


def _vs_base_stem(edid):
    e = re.sub(r"^(Burn_|Trap|UseLPI_|DEBUG)", "", edid, flags=re.I)
    e = re.sub(r"florarad", "", e, flags=re.I)
    e = re.sub(r"\d+$", "", e)
    return re.sub(r"[^a-z]", "", e.lower())


def _vs_original_name(edids, nuked_display):
    nd = re.sub(r"[^a-z]", "", nuked_display.lower())
    # Prefer a base token that sits INSIDE the form stem (e.g. "fern" in
    # "flashfern"); only if none is found, fall back to the form stem sitting
    # inside a longer base key (e.g. "slippercactus" in "slippercactusbig").
    # This stops a more-specific key (SwampPodFlower) beating the real base.
    primary = None
    fallback = None
    for ed in edids:
        stem = _vs_base_stem(ed)
        if not stem:
            continue
        for kn, name in _vs_fn_norm():
            if len(kn) < 3:
                continue
            if kn in stem:
                if primary is None or len(kn) > primary[0]:
                    primary = (len(kn), name)
            elif stem in kn:
                if fallback is None or len(kn) > fallback[0]:
                    fallback = (len(kn), name)
    chosen = primary or fallback
    if chosen is None:
        return ""
    name = chosen[1]
    if re.sub(r"[^a-z]", "", name.lower()) == nd:
        return ""
    return name


_VS_POTS = ["Pot o' Radlily", "Pot o' Crystalcup", "Pot o' Carnal Weeper"]

# Radlily, Crystal Cup and Carnal Weeper can ONLY be farmed from the Pot o'
# planters, so they are shown as the single "Pot o' ..." rows above and the
# plain-name rows are suppressed (they are in the Verdant/Backwoodsman LVLI
# lists, which would otherwise create duplicate, half-filled rows). The planter
# flora (SSE_Flora_Pot*) carry the Green Thumb + Gamma keywords, and the
# resource lists are in the Verdant/Backwoodsman GLOBs, so all five buffs apply.
_VS_POT_BASES = {"Radlily", "CrystalCup", "CarnalWeeper"}

# Plants that Gamma Green Tea (SSE_PerkHarvestFlowers) actually works on — the
# "flower" harvest set. Verified against KYWD_Export (June 2026): only these
# canonical plants have flora carrying SSE_PerkHarvestFlowers. The food crops
# (Gourd, Melon, Carrot, Tato, Swamp Pod) do NOT, despite sharing a name stem
# with their wild-flower cousins — the old fuzzy matcher wrongly ticked them.
# "Swamp Plant" is included because its resource list (LL_Flora_SwampPlant)
# harvests the Strangler Bloom (Swamp Pod Flower), which carries the keyword.
_VS_GAMMA_PLANTS = {
    "Ash Rose", "Aster", "Fern Flower", "Fever Blossom", "Kaleidopore",
    "Rhododendron", "Snaptail", "Soot Flower", "Swamp Plant",
    "Swamp Pod Flower", "Thistle", "Toxic Soot Flower",
    "Wild Carrot Flower", "Wild Gourd Flower", "Wild Melon Flower",
    "Wild Tato Flower",
    "Pot o' Radlily", "Pot o' Crystalcup", "Pot o' Carnal Weeper",
}

# Base-plant note for nuked plants whose EDID carries no base token, so the
# stem matcher returns blank. Resolved from the LPI_Flora<Base> harvest list
# that swaps in each FloraRad* form (verified against LVLI_Export June 2026).
# Decay Vine is intentionally absent: it is an orphan form with no harvest
# list, resource, or ingredient in the game data, so no base can be derived.
_VS_NUKED_BASE_OVERRIDE = {
    "Afterwind Vine": "Tato",
    "Blast Berry": "Cranberry",
    "Fission Fruit": "Mutfruit",
    "Wild Fission Fruit": "Mutfruit",
    "Gamma Dogwood": "Bleach Dogwood",
    "Geiger Blossom": "Soot Flower",
    "Glowing Mutshoot Fungus": "Glowing Fungus",
    "Ionized Cackleberry": "Mothman Eggs",
    "Irradiroot": "Carrot",
    "Neutron Pod": "Silt Bean",
    "Quantum Leaf": "Bloodleaf",
}


def _vs_read_lines(path: Optional[str]) -> List[str]:
    if not path or not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read().splitlines()


def build_verdant_buff_chart(data_dir: str) -> Dict[str, Any]:
    glob_lines = _vs_read_lines(find_first(data_dir, GLOB_GLOBS))
    kywd_lines = _vs_read_lines(find_first(data_dir, KYWD_REFS_GLOBS))

    def lvli_bases(pat: str) -> set:
        for line in glob_lines:
            if pat in line:
                bs = set()
                for cell in line.split("\t"):
                    m = re.match(r"^[0-9A-Fa-f]+:(.+):LVLI$", cell)
                    if m:
                        bs.add(re.sub(r"^(SSE_Resources_LL_|LL_Flora_)", "", m.group(1)))
                return bs
        return set()

    verdant = lvli_bases("SQ_VerdantSeasonExtraHarvestChance")
    back = lvli_bases("Backwoodsman04_Chance_Global")

    def kw_pairs(kwid: str) -> List[Tuple[str, str]]:
        out = []
        for line in kywd_lines:
            c = line.split("\t")
            if len(c) >= 7 and c[1] == kwid and c[6].strip():
                if c[4].startswith(("ATX_COMP", "CUT_")):
                    continue
                out.append((c[4], c[6].strip()))
        return out

    gt = kw_pairs("PerkGreenThumbValidFlora")
    gg = kw_pairs("SSE_PerkHarvestFlowers")
    gt_eds = {e for e, _ in gt}
    gg_eds = {e for e, _ in gg}

    def is_nuked(ed: str) -> bool:
        return bool(re.search(r"florarad", ed, re.I))

    def is_pot(ed: str) -> bool:
        return "pot" in ed.lower()

    def normed(ed: str) -> str:
        e = re.sub(r"^(Burn_|Trap|UseLPI_|DEBUG|SSE_Resources_LL_|SSE_Flora_|LL_Flora_)", "", ed, flags=re.I)
        e = re.sub(r"^FloraRad", "", e, flags=re.I)
        e = re.sub(r"^Flora", "", e, flags=re.I)
        return re.sub(r"[^a-z]", "", e.lower())

    # Normal plants. Columns: [name, verdant, gamma, backwoodsman, green_thumb, sam].
    # Verdant / Backwoodsman come straight from the LVLI lists. Gamma uses the
    # verified _VS_GAMMA_PLANTS set (the old fuzzy name-matcher produced both
    # false positives and false negatives). Green Thumb (and Sam's Tune-Up,
    # which shares its flora list) applies to every farmable plant in these
    # lists, so both are set to 1.
    rows: Dict[str, list] = {}
    for base in sorted(verdant | back):
        if base in _VS_POT_BASES:
            continue  # shown only as the Pot o' rows below
        nm = _VS_FN.get(base)
        if not nm:
            warnings.setdefault("verdant_unmapped_plant", {})
            warnings["verdant_unmapped_plant"][base] = warnings["verdant_unmapped_plant"].get(base, 0) + 1
            continue
        V = 1 if base in verdant else 0
        B = 1 if base in back else 0
        G = 1 if nm in _VS_GAMMA_PLANTS else 0
        if nm in rows:
            r = rows[nm]
            r[1] |= V; r[2] |= G; r[3] |= B
        else:
            rows[nm] = [nm, V, G, B, 1, 1]
    # Pot o' planter flowers — single merged row each; all five buffs apply.
    for p in _VS_POTS:
        rows[p] = [p, 1, 1, 1, 1, 1]
    plants = sorted(rows.values(), key=lambda r: r[0].lower())

    # Nuked plants (FloraRad* forms)
    nuk_edids: Dict[str, set] = {}
    for line in kywd_lines:
        c = line.split("\t")
        if len(c) >= 7 and is_nuked(c[4]) and c[6].strip():
            nm = c[6].strip()
            if nm == "Springtime Splendor":
                continue
            nuk_edids.setdefault(nm, set()).add(c[4])
    nuked = []
    for nm in sorted(nuk_edids, key=lambda s: s.lower()):
        eds = nuk_edids[nm]
        G = 1 if (eds & gg_eds) else 0
        T = 1 if (eds & gt_eds) else 0
        orig = _VS_NUKED_BASE_OVERRIDE.get(nm) or _vs_original_name(eds, nm)
        nuked.append([nm, 0, G, 0, T, T, orig])

    return {
        "columns": ["plant", "verdant_season", "gamma_green_tea",
                    "backwoodsman_4", "green_thumb", "sam_camp_ally"],
        "count_plants": len(plants),
        "count_nuked": len(nuked),
        "plants": plants,
        "nuked": nuked,
    }



# ---------------------------------------------------------------------------
# Verdant Season buff stacking — worked examples (generative)
#
# Editorial scenarios showing how the farming buffs combine on a single
# plant. Each buff is either an "add" buff (+N to the count) or a "mult"
# buff (doubles the running total). The maths is:
#       total = (base + sum of adds) * product of mult factors
# Totals are computed here so the displayed rows and the printed total can
# never drift apart. Each row carries a note with the buff's activation
# chance / condition (e.g. Backwoodsman 4 only fires ~50% of the time).
# ---------------------------------------------------------------------------

_VS_EXAMPLE_BUFFS = {
    "backwoodsman": {"label": "Backwoodsman 4",             "kind": "add",  "amount": 1, "note": "50% chance"},
    "verdant":      {"label": "Verdant Season",             "kind": "add",  "amount": 1, "note": "33% chance"},
    "gamma":        {"label": "Gamma Green Tea",            "kind": "add",  "amount": 1, "note": "while active"},
    "sam":          {"label": "Sam Camp Ally Harvest Buff", "kind": "mult", "factor": 2, "note": "while active"},
    "green_thumb":  {"label": "Green Thumb",                "kind": "mult", "factor": 2, "note": "while equipped"},
}
# Canonical display order: add buffs first, then the multipliers.
_VS_EXAMPLE_ORDER = ["backwoodsman", "verdant", "gamma", "sam", "green_thumb"]
# Hand-picked scenarios (each is the set of buff keys that are active).
_VS_EXAMPLE_SCENARIOS = [
    [],
    ["green_thumb"],
    ["backwoodsman", "green_thumb"],
    ["backwoodsman", "gamma", "green_thumb"],
    ["backwoodsman"],
    ["backwoodsman", "verdant"],
    ["backwoodsman", "verdant", "green_thumb"],
    ["backwoodsman", "verdant", "gamma", "green_thumb"],
    ["backwoodsman", "verdant", "gamma", "sam", "green_thumb"],
]
# Known-good totals — the build asserts the computed values match these, so a
# bad edit to the catalog/scenarios fails loudly instead of shipping silently.
_VS_EXAMPLE_EXPECTED = [1, 2, 4, 6, 2, 3, 6, 8, 16]


def build_verdant_buff_examples() -> Dict[str, Any]:
    examples: List[Dict[str, Any]] = []
    for scen in _VS_EXAMPLE_SCENARIOS:
        rows: List[Dict[str, Any]] = [{"text": "Base = 1", "note": ""}]
        adds = 0
        factor = 1
        for key in _VS_EXAMPLE_ORDER:
            if key not in scen:
                continue
            b = _VS_EXAMPLE_BUFFS[key]
            if b["kind"] == "add":
                adds += b["amount"]
                op = "+%d" % b["amount"]
            else:
                factor *= b["factor"]
                op = "×%d" % b["factor"]
            rows.append({"text": "%s %s" % (b["label"], op), "note": b["note"]})
        total = (1 + adds) * factor
        examples.append({"rows": rows, "total": total})

    got = [e["total"] for e in examples]
    if got != _VS_EXAMPLE_EXPECTED:
        raise ValueError(
            "verdant buff-example totals drifted: %r != %r" % (got, _VS_EXAMPLE_EXPECTED))

    return {
        "total_label": "Total possible harvest from one plant",
        "examples": examples,
    }


def build(data_dir: str, outdir: str) -> str:
    alch_path = find_first(data_dir, ALCH_GLOBS)
    cobj_path = find_first(data_dir, COBJ_GLOBS)
    eff_path  = find_first(data_dir, ALCH_EFFECTS_GLOBS)
    kywd_refs_path = find_first(data_dir, KYWD_REFS_GLOBS)
    misc_path = find_first(data_dir, MISC_GLOBS)
    book_path = find_first(data_dir, BOOK_GLOBS)
    seasons_path = os.path.join(data_dir, SEASONS_TSV)
    if not os.path.exists(seasons_path):
        seasons_path = None

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

    # Plan / season lookups for the Food-That-Can-Be-Canned page. BOOK
    # provides the human plan name ("Recipe: Blight Soup"); the seasons
    # TSV provides the marketing name for each Scoreboard season.
    book_by_full = load_book_plans_by_full(book_path)
    seasons_by_key = load_seasons_by_key(seasons_path)
    if not book_path:
        warnings.setdefault("missing_book_tsv", True)

    # Live spoil-time data: {curve_edid: Y@X=1 in minutes}. Merges three
    # sources — the CURV POINTS TSV (xEdit export of curves that had a
    # JASF_Path), the CURV record TSV (canonical list of curve EDIDs),
    # and the raw crafting/*.json files from fo76-tools. All are engine
    # data; rebalances propagate automatically on the next build.
    curv_points_paths = [os.path.join(data_dir, n) for n in CURV_POINTS_GLOBS]
    curv_record_paths = [os.path.join(data_dir, n) for n in CURV_RECORD_GLOBS]
    curvetables_roots = _discover_curvetables_roots(data_dir)
    curve_base_times = load_curve_base_times(
        curv_points_paths, curv_record_paths, curvetables_roots
    )

    # Cooking-ingredient qty curves. Without these, every cooking recipe
    # emits the wrong qty (tier index instead of real qty). Tier 3 Tasty
    # meals are the common case; without curves they'd all say 3x wood
    # when the in-game cost is 1x wood.
    ingredient_qty_curves = load_ingredient_qty_curves(curvetables_roots)

    # Live data for spoil modifiers — storage (GLOB), perks (SPEL), mods
    # (ENCH). Replaces the old hardcoded SPOIL_REDUCTIONS list; every
    # number below is now pulled from the engine TSVs each build.
    glob_values = load_glob_values(
        [os.path.join(data_dir, n) for n in GLOB_GLOBS]
    )
    spel_mags = load_spel_effect_magnitudes(
        [os.path.join(data_dir, n) for n in SPEL_EFFECTS_GLOBS]
    )
    ench_mags = load_ench_effect_magnitudes(
        [os.path.join(data_dir, n) for n in ENCH_GLOBS]
    )

    # Weight column in Apr 2026 ALCH is empty for all rows; backfill from the
    # newest older export that still has it.
    weight_backfill_paths = [
        os.path.join(data_dir, n) for n in ALCH_GLOBS
        if os.path.join(data_dir, n) != alch_path
    ]
    weight_backfill = load_weight_backfill(weight_backfill_paths)

    # Collector for build-time warnings — surfaced at the end of build so
    # coverage gaps (new HealthCurves, unknown workbenches, unresolved
    # ingredient names) fail loudly rather than rotting silently.
    warnings: Dict[str, Any] = {}

    # ALCH lookups
    alch_by_fid: Dict[str, Dict[str, str]] = {r["ALCH_FormID"]: r for r in alch}
    alch_by_edid: Dict[str, Dict[str, str]] = {r["ALCH_EDID"]: r for r in alch}

    # Effects -> by ALCH FormID -> list of structured effect dicts.
    # Each effect resolves its magnitude and duration from GLOB overrides
    # (MAGG_GLOB / DURG_GLOB -> FLTV) with fallback to the raw EFIT columns.
    eff_by_fid: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in effects:
        fid = e.get("ALCH_FormID", "")
        raw_name = (e.get("MGEF_FULL") or e.get("MGEF_EDID") or "").strip()
        mgef_edid = (e.get("MGEF_EDID") or "").strip()
        if not fid or not raw_name:
            continue
        # Skip duplicate effects on the same ALCH (keyed by MGEF_EDID)
        if any(ex.get("edid") == mgef_edid for ex in eff_by_fid[fid]):
            continue

        # Resolve magnitude: GLOB override -> EFIT fallback
        mag_glob_edid = (e.get("MAGG_GLOB_EDID") or "").strip()
        mag = glob_values.get(mag_glob_edid) if mag_glob_edid else None
        if mag is None:
            mag = safe_float(e.get("EFIT_Magnitude", ""))

        # Resolve duration: GLOB override -> EFIT fallback
        dur_glob_edid = (e.get("DURG_GLOB_EDID") or "").strip()
        dur = glob_values.get(dur_glob_edid) if dur_glob_edid else None
        if dur is None:
            dur = safe_float(e.get("EFIT_Duration", ""))

        # Clean the display name
        display = _clean_effect_name(raw_name)

        # Format duration for display
        dur_display = _format_effect_duration(dur)

        eff_by_fid[fid].append({
            "name":      display,
            "edid":      mgef_edid,
            "magnitude": round(mag, 4) if mag is not None else None,
            "duration":  round(dur, 4) if dur is not None else None,
            "dur_display": dur_display,
            "mag_glob":  mag_glob_edid or None,
            "dur_glob":  dur_glob_edid or None,
        })

    # Pre-compute ALCH "lite" record (used both for the ingredient list and
    # as the recipe's `output` block).  _depth prevents infinite recursion
    # when a fermented item itself spoils into a vintage variant.
    def alch_lite(r: Dict[str, str], _depth: int = 0) -> Dict[str, Any]:
        kws = parse_keywords(r.get("Keywords_Flat", ""))
        fid = r.get("ALCH_FormID", "")
        weight = safe_float(r.get("Weight", ""))
        if weight is None:
            weight = weight_backfill.get(fid)
        # Spoil time is keyed off the HealthCurve EDID (stable across
        # ESM resaves); the base minutes come from the live CURV points
        # lookup and the tier magnitudes from GLOB / SPEL / ENCH TSVs.
        spoil_time = compute_spoil_time(
            r.get("ENIT_HealthCurve_EDID", ""),
            r.get("ENIT_HealthCurve_FormID", ""),
            curve_base_times,
            glob_values,
            spel_mags,
            ench_mags,
            warnings,
        )

        # Resolve the spoiled-to ALCH record (e.g. Fermentable → finished
        # alcohol).  Only one level deep to avoid chaining through vintages.
        spoiled_to_output = None
        if _depth == 0:
            spoil_fid = r.get("ENIT_SpoiledItem_FormID", "").strip()
            spoil_row = alch_by_fid.get(spoil_fid) if spoil_fid else None
            if spoil_row and eff_by_fid.get(spoil_fid):
                spoiled_to_output = alch_lite(spoil_row, _depth=1)

        result = {
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
            "addiction_name":   r.get("ENIT_Addiction_FULL", "").strip() or None,
            "addiction_chance":  safe_float(r.get("ENIT_AddictionChance", "")),
            # Perk-card classification flags — used by the frontend to
            # decide which perk sub-rows to render in the effects table.
            "is_alcohol":   "DrinkTypeAlcohol" in kws,
            "is_nuka_cola": "DrinkTypeSoda" in kws,
            "is_chem":      "ObjectTypeChem" in kws,
        }
        if spoiled_to_output is not None:
            result["spoiled_to_output"] = spoiled_to_output
        return result

    # Discover duplicate-workbench pairs and variant recipes from the data
    # itself — no hardcoded region list, no EDID-suffix matching. The
    # returned `skip_edids` are recipes that duplicate a chem sibling with
    # identical ingredients; `variant_suffix` maps a recipe EDID to its
    # EDID-differentiator token (e.g. "AshHeap", "SavageDivide") for use
    # as a display suffix.
    skip_edids, variant_suffix = detect_recipe_variants(cobj, alch_by_fid)

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

        # Skip recipes that lost the dedup pass (same ingredients as a
        # preferred chem sibling).
        if recipe_edid in skip_edids:
            continue

        ings_raw = parse_fvpa_with_curve(r.get("FVPA", ""))
        if not ings_raw:
            continue
        # Curve-resolve every ingredient qty. For cooking recipes the
        # FVPA "qty" is actually the tier index; the real qty comes
        # from the CT_COBJ_Cooking_* curve at X=tier. For non-curve or
        # COBJ_Workshop_* ingredients, the FVPA value is the qty as-is.
        ings = [
            (edid, resolve_ingredient_qty(
                fvpa_val, curve_edid, ingredient_qty_curves, warnings))
            for edid, fvpa_val, curve_edid in ings_raw
        ]

        out_alch = alch_by_fid.get(out_fid)
        # Only interested in recipes whose OUTPUT is an ALCH (= consumable).
        if not out_alch:
            continue

        wb_cat, wb_label = workbench_category(
            r.get("BNAM_EDID", ""), r.get("BNAM_FULL", "")
        )
        if wb_cat == "other":
            bnam_full = r.get("BNAM_FULL", "") or r.get("BNAM_EDID", "") or "(none)"
            warnings.setdefault("unknown_workbench", {})[bnam_full] = (
                warnings.get("unknown_workbench", {}).get(bnam_full, 0) + 1
            )

        # Variant recipes (e.g. Disease Cure - Ash Heap) get an auto-derived
        # suffix so siblings read as distinct cards. The suffix token is
        # pulled from the EDID itself in detect_recipe_variants — any new
        # region Bethesda ships will surface here without a code change.
        variant_token = variant_suffix.get(recipe_edid)
        display_name = out_name
        region = None  # kept for backwards-compat in the JSON payload
        if variant_token:
            region = pretty_variant_label(variant_token)
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
            )
            if not ing_name:
                # Fell all the way through to using the raw EDID — surface
                # this so we can add an override or extend the lookup set.
                warnings.setdefault("unresolved_ingredients", {})[ing_edid] = (
                    warnings.get("unresolved_ingredients", {}).get(ing_edid, 0) + 1
                )
                ing_name = ing_edid

            # Pattern-based overrides — fish meals from any species should
            # display as a generic "Any Fish" since any fish can be used.
            # Covers Fishing_Fish_Meal_*, Burn_Fish_Meal_*, and seasonal.
            # Regular fish (Fishing_Fish_Meal_*, Burn_Fish_Meal_*) can be
            # any species — display generically. Seasonal fish keep their
            # specific names since each fillet needs that season's fish.
            if not re.match(r"SeasonalFish_", ing_edid, re.IGNORECASE) \
               and re.search(r"Fish_Meal_", ing_edid):
                ing_name = "Any Fish"

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

    # Food-that-can-be-canned page list.
    #
    # Each item gets:
    #   - plan_name:   the FULL of the BOOK that teaches the BASE recipe
    #                  (e.g. "Recipe: Blight Soup"). When present, this is
    #                  the plan players must read to unlock both the base
    #                  recipe and its canned variant. None = base recipe
    #                  is learned by default (no plan exists for it).
    #   - season_number / season_name: if the canned recipe's COBJ EDID
    #                  starts with SCORE_S{N}_, this is the Scoreboard
    #                  season that gates the canned variant. The base
    #                  recipe may still be learnable by reading its own
    #                  plan year-round; only the canned variant is the
    #                  season reward. Both fields are None for non-season
    #                  canned items.
    #
    # We need the canned recipe COBJ EDID to detect the season prefix. The
    # ingredients list above doesn't expose canned items as ingredients
    # (canned outputs aren't recipe inputs themselves), so we look it up
    # from the ingredient_uses map — for each base ingredient, one of the
    # recipes that consume it will be the canning recipe. Easier: scan
    # cobj directly for entries whose output (CNAM) matches the canned
    # ALCH FormID.
    canned_recipe_edid_by_output_fid: Dict[str, str] = {}
    for r in cobj:
        out_fid = r.get("CNAM_FormID", "")
        if not out_fid:
            continue
        # Prefer the FIRST recipe found (alphabetical COBJ EDID); skip cuts.
        if is_cut(r.get("COBJ_EDID", "")):
            continue
        if out_fid not in canned_recipe_edid_by_output_fid:
            canned_recipe_edid_by_output_fid[out_fid] = r.get("COBJ_EDID", "")

    # Map every COBJ EDID -> set of its ingredient EDIDs (parsed from FVPA).
    # Used below to detect the canning workflow per item. We only need the
    # ingredient identities here, not quantities/curves, so a light split is
    # enough (FVPA segments are "<edid>:<qty>:<curve?>" joined by "|").
    cobj_ings_by_edid: Dict[str, set] = {}
    for r in cobj:
        edid = r.get("COBJ_EDID", "")
        if not edid:
            continue
        fvpa = r.get("FVPA", "") or ""
        cobj_ings_by_edid[edid] = {
            seg.split(":", 1)[0] for seg in fvpa.split("|") if seg
        }

    canned_items: List[Dict[str, Any]] = []
    for r in alch:
        if (r.get("ENIT_IsCanned") or "").strip().lower() != "true":
            continue
        if is_cut(r.get("ALCH_EDID", "")):
            continue
        alch_fid = r.get("ALCH_FormID", "")
        canned_recipe_edid = canned_recipe_edid_by_output_fid.get(alch_fid)
        base_name = (r.get("ENIT_CannedBase_FULL") or "").strip() or None
        plan_name = lookup_canned_plan(base_name, book_by_full)
        season_number, season_name = detect_canned_season(
            canned_recipe_edid, seasons_by_key
        )
        # Super Duper: True if the item does NOT have BlockSuperDuperPerk.
        kw_flat = r.get("Keywords_Flat") or ""
        super_duper = "BlockSuperDuperPerk" not in kw_flat

        # Canning workflow - generative from the canning recipe's ingredient
        # list. Two ways the game lets you can a food:
        #   "cook_first": the canning recipe consumes the already-cooked base
        #                 food itself (its ALCH EDID appears as an ingredient),
        #                 so you cook the food first, then can the cooked item.
        #   "raw":        the canning recipe consumes raw ingredients (the base
        #                 food's EDID is NOT an ingredient), so it cooks and
        #                 cans in a single step at the Canning Station.
        # Derived purely from data, so if Bethesda changes a recipe the next
        # COBJ export flips this automatically - no code edit needed. None when
        # the canning recipe can't be resolved (don't guess).
        base_edid = (r.get("ENIT_CannedBase_EDID") or "").strip()
        recipe_ings = cobj_ings_by_edid.get(canned_recipe_edid or "")
        if not canned_recipe_edid or not recipe_ings:
            canning_method = None
        elif base_edid and base_edid in recipe_ings:
            canning_method = "cook_first"
        else:
            canning_method = "raw"

        canned_items.append({
            "edid":              r.get("ALCH_EDID", ""),
            "formId":            alch_fid,
            "name":              r.get("FULL") or r.get("ALCH_EDID") or "",
            "canned_base":       base_name,
            "canned_base_edid":  r.get("ENIT_CannedBase_EDID") or None,
            "canned_recipe_edid": canned_recipe_edid or None,
            "plan_name":         plan_name,
            "season_number":     season_number,
            "season_name":       season_name,
            "super_duper":       super_duper,
            "canning_method":    canning_method,
        })
    canned_items.sort(key=lambda i: (i["name"] or "").lower())

    # ---- Clean Can recipe ---------------------------------------------
    # Every canned recipe needs 1x Clean Can in addition to its base food.
    # Clean Cans aren't food (they don't show up in the ingredient-search
    # list), so they don't have a recipe entry in the per-ingredient
    # output above — we look them up directly from the COBJ TSV by EDID
    # and emit a single shared clean_can_recipe block at the page level.
    # The canned page renders this once, alongside the base food recipe.
    clean_can_recipe = None
    for r in cobj:
        if r.get("COBJ_EDID") != "ATX_co_Cannery_Junk_TinCan_Crafted":
            continue
        cc_raw = parse_fvpa_with_curve(r.get("FVPA", ""))
        cc_ings: List[Dict[str, Any]] = []
        for ing_edid, fvpa_val, curve_edid in cc_raw:
            qty = resolve_ingredient_qty(
                fvpa_val, curve_edid, ingredient_qty_curves, warnings
            )
            ing_row = alch_by_edid.get(ing_edid)
            ing_name = (
                INGREDIENT_NAME_OVERRIDES.get(ing_edid)
                or (ing_row.get("FULL") if ing_row else None)
                or cmpo_names.get(ing_edid)
                or misc_names.get(ing_edid)
                or ing_edid
            )
            cc_ings.append({"edid": ing_edid, "name": ing_name, "qty": qty})
        _wb_cat, wb_label = workbench_category(
            r.get("BNAM_EDID", ""), r.get("BNAM_FULL", "")
        )
        clean_can_recipe = {
            "recipe_edid":     r.get("COBJ_EDID", ""),
            "recipe_name":     "Clean Can",
            "workbench":       wb_label,
            "all_ingredients": cc_ings,
        }
        break

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
                "count_items":      len(canned_items),
                "items":            canned_items,
                "clean_can_recipe": clean_can_recipe,
            },
            "verdant-season": {
                "buff_chart": build_verdant_buff_chart(data_dir),
                "buff_examples": build_verdant_buff_examples(),
            },
        },
    }

    # Attach a warnings summary to the payload so downstream consumers
    # can surface it in dashboards / admin pages if they want. Sort the
    # inner maps by hit count (descending) so the most-broken items are
    # listed first.
    warnings_summary: Dict[str, Any] = {}
    for key, hits in warnings.items():
        if isinstance(hits, dict):
            warnings_summary[key] = [
                {"value": v, "count": c}
                for v, c in sorted(hits.items(), key=lambda x: -x[1])
            ]
        else:
            warnings_summary[key] = hits
    payload["warnings"] = warnings_summary

    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, "farming_guides.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"[build_farming_guides_json] wrote {out_path}  "
        f"ingredients={len(ingredients)}  canned={len(canned_items)}"
    )

    # Print warnings last so they're the last thing a human sees when
    # running the build. Each section is compact — only shown when
    # something actually landed in the bucket.
    if warnings_summary:
        print("[build_farming_guides_json] WARNINGS (coverage gaps — "
              "consider updating static lookups):")
        labels = {
            "unknown_spoil_curves":    "HealthCurves with no CURV points AND no fallback (items render 'base unknown')",
            "using_spoil_fallback":    "HealthCurves served from 2022 fallback (CURV export missing these)",
            "missing_spoil_source":    "Spoil-tier source records not found in GLOB/SPEL/ENCH exports",
            "unknown_workbench":       "BNAM workbench refs that fell through to 'Other'",
            "missing_qty_curve":       "CT_COBJ_Cooking_* curve referenced by an FVPA but not loaded from curvetables/json",
            "qty_curve_missing_tier":  "FVPA tier (X) had no Y point in the referenced curve",
            "unresolved_ingredients":  "Ingredient EDIDs with no ALCH/CMPO/MISC name",
        }
        for key, entries in warnings_summary.items():
            header = labels.get(key, key)
            if not entries:
                continue
            print(f"  {header}:")
            for e in entries[:15]:  # cap so logs stay readable
                print(f"    - {e['value']}  ({e['count']}×)")
            if len(entries) > 15:
                print(f"    … and {len(entries) - 15} more")
    else:
        print("[build_farming_guides_json] no coverage warnings — "
              "all curves/workbenches/ingredients resolved cleanly.")

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
