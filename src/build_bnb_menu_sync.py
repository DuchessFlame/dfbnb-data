from __future__ import annotations

"""
build_bnb_menu_sync.py
=======================
Syncs the ALCH export TSVs into BnB_Menu_Items.tsv — the master TSV that drives
the customer menu page, the staff-portal crafting tab, and the order log. This
script is the single entry point for "I dropped a new ALCH / COBJ export into
tsv/, now make everything pick it up".

What it does:
  1. Reads ALCH_Export_*.tsv (base) + ALCH_Export_*_Effects.tsv
  2. Reads dist/cobj-recipes.json (built by build_cobj_recipes_json.py — run
     that first) for ingredient-count driven auto-pricing.
  3. Reads the current BnB_Menu_Items.tsv.
  4. For every ALCH record that matches a menu category (keyword analysis),
     ensures it exists in the menu TSV. New items are:
       * auto-priced using the BnB formula:
            price_xbox = ingredient_count * 5
            price_ps   = price_xbox + 10
            price_pc   = price_xbox + 10
         where ingredient_count comes from the item's COBJ recipe. If no COBJ
         recipe is found (e.g. ALCH-only items like looted consumables) prices
         stay blank and a diagnostic is emitted.
       * order limits (limit_new / limit_existing) are always left blank for
         new items — the chef must set them manually. A "needs_limits"
         diagnostic is written so the staff-portal diagnostics tab surfaces
         them.
     Existing items keep their manually-set prices and limits untouched —
     the sync only refreshes mutation / buff / Pre War / Post War fields.
  5. Populates the "mutation" column (Herbivore / Carnivore / Both / blank)
     from ALCH keywords.
  6. Populates the "buff" column from MGEF effect names in the effects TSV.
  7. Writes the updated BnB_Menu_Items.tsv.
  8. Reports unpriced items and new-items-needing-limits to diagnostics.json
     for the staff portal.

Usage:
    python build_bnb_menu_sync.py
    python build_bnb_menu_sync.py --data-dir tsv --outdir dist
    python build_bnb_menu_sync.py --alch-base ALCH_Export_Apr_2026.tsv
                                  --alch-effects ALCH_Export_Apr_2026_Effects.tsv
    python build_bnb_menu_sync.py --cobj-json dist/cobj-recipes.json
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagnostics import Diagnostics  # noqa: E402
from cut_content import is_cut, purge_cut_rows  # noqa: E402
import tsv_source          # one resolver for every export selection

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MENU_TSV = "BnB_Menu_Items.tsv"

MENU_COLS = [
    "menu category", "name", "edid", "form_id", "build",
    "price_xbox", "price_ps", "price_pc",
    "limit_new", "limit_existing",
    "mutation", "buff",
    "order category", "notes",
    "availability",
]

# ---------------------------------------------------------------------------
# Category detection from ALCH keywords
# ---------------------------------------------------------------------------
# Each rule is (keyword_substring, menu_category, order_category).
# First match wins, so order matters — more specific before general.

KEYWORD_TO_CATEGORY: List[Tuple[str, str, str]] = [
    ("ObjectTypeSerum",            "Serums",                      "serum"),
    ("ObjectTypeSyringerAmmo",     "__skip__",                    ""),      # syringer darts — not menu items
    ("DrinkTypeAlcohol",           "Alcohol",                     "alcohol"),
    ("DrinkTypeLiquor",            "Alcohol",                     "alcohol"),
    ("DrinkTypeSarsaparilla",      "Alcohol",                     "alcohol"),
    ("ObjectTypeNukaCola",         "Soda & Drinks",               "drink"),
    ("DrinkTypeSoda",              "Soda & Drinks",               "drink"),
    ("DrinkTypeSodaIcon",          "Soda & Drinks",               "drink"),
    ("ObjectTypeChem",             "Chems",                       "chem"),
    ("ObjectTypeStimpak",          "Chems",                       "chem"),
    ("ObjectTypeSalve",            "Chems",                       "chem"),
    ("ObjectTypeRadX",             "Chems",                       "chem"),
    ("ObjectTypeBloodPack",        "Chems",                       "chem"),
    ("ObjectTypeAntibiotics",      "Chems",                       "chem"),
    ("ChemTypeRadaway",            "Chems",                       "chem"),
    ("ChemTypeHealing",            "Chems",                       "chem"),
    ("ObjectTypeCandy",            "Pre War Food",                "prewarf"),
    ("DrinkTypeTea",               "Dishes & Teas",               "food"),
    ("DrinkTypeJuice",             "Dishes & Teas",               "food"),
]

# EDID patterns for Fish (these don't have a distinguishing keyword)
FISH_EDID_PATTERNS = [
    re.compile(r"Fish_Meal", re.IGNORECASE),
    re.compile(r"Fish_Fishbits", re.IGNORECASE),
    re.compile(r"Fishing_Fish.*Cooked", re.IGNORECASE),
]

# EDID patterns for Magazines & Bobbleheads
MAGAZINE_BOBBLEHEAD_PATTERNS = [
    re.compile(r"^Magazine_", re.IGNORECASE),
    re.compile(r"^Bobble[Hh]ead_.*_Potion", re.IGNORECASE),
    re.compile(r"^BobbleHead_.*_Potion", re.IGNORECASE),
]

# EDID patterns to always skip (pets, test items, debug, etc.)
SKIP_EDID_PATTERNS = [
    re.compile(r"^PETS_", re.IGNORECASE),
    re.compile(r"^Debug", re.IGNORECASE),
    re.compile(r"^TestQA", re.IGNORECASE),
    re.compile(r"^test[A-Z]", re.IGNORECASE),
    re.compile(r"^zz", re.IGNORECASE),
    re.compile(r"^AC_SQ04_Reopening_", re.IGNORECASE),  # cut content (Nukashine 2 etc.)
    re.compile(r"^SFS01_Brew_", re.IGNORECASE),          # cut content (Tater/Muttberry/Sunday Shine)
    re.compile(r"Firecracker", re.IGNORECASE),            # cut content (Firecracker Whiskey line)
    re.compile(r"^_Disease", re.IGNORECASE),              # game system (disease chance)
    re.compile(r"SteelSkin", re.IGNORECASE),              # game system (Steel Skin Infusion)
    re.compile(r"HealingCloud", re.IGNORECASE),           # game system (Healing Cloud Infusion)
    re.compile(r"FieryInfusion", re.IGNORECASE),          # game system (Fiery Infusion)
    re.compile(r"^PotionOf", re.IGNORECASE),              # game system (Potion of Experience etc.)
    re.compile(r"^Recalibrated", re.IGNORECASE),          # game system (Recalibrated Liberator)
    re.compile(r"^DEPRECATED_", re.IGNORECASE),           # deprecated variants of event items
    re.compile(r"^BS00_Contribution", re.IGNORECASE),     # ATLAS Donor's Provisions
    re.compile(r"TreasureHunt_Chest", re.IGNORECASE),     # Mole Miner Pails (event loot)
    re.compile(r"^E07A_Mothman", re.IGNORECASE),          # Cultist High Priest Pack
    re.compile(r"MutatedEvents_Package", re.IGNORECASE),  # Mutated event packages
    re.compile(r"Challenge_Raids_", re.IGNORECASE),       # Gleaming Depths reward crate
    re.compile(r"^Festive_", re.IGNORECASE),              # Holiday/Waterlogged gifts
    re.compile(r"^v96_Metabolux", re.IGNORECASE),         # Metabolux Syringe
    re.compile(r"^MTR01_Precursor", re.IGNORECASE),       # Precursor serums (game system)
    # ^SCORE_ blanket removed — Season 22/25 added real consumables with
    # SCORE_ prefixed EDIDs. Reward-box items (Lunchbox, Bobblehead Box,
    # Magazine Package, Carry Weight Booster, Perfect Bubblegum, Scout's
    # Banner) are allowed through and marked Not Available via
    # not-available.json instead.
    re.compile(r"^Spooky_TreatBag", re.IGNORECASE),       # Spooky Treat Bag (event loot)
    re.compile(r"^TrackingDart", re.IGNORECASE),          # Tracking Dart
    re.compile(r"^MTNS05_VoxDart", re.IGNORECASE),        # Vox Interpreter Dart
    re.compile(r"^W05_MQS_205P_Jen", re.IGNORECASE),     # Jen's Stealth potion (quest item)
    re.compile(r"^CUT_RefreshingBeverage", re.IGNORECASE), # cut Refreshing Beverage
    re.compile(r"^DLC04_Calmex", re.IGNORECASE),          # Calmex Silk (DLC chem, not sold)
    re.compile(r"^CUT_SFS09_FormulaQ", re.IGNORECASE),   # cut Formula Q
    re.compile(r"^MTNZ03_FormulaB", re.IGNORECASE),      # Formula B (quest reward only)
    re.compile(r"^POST_DetoxingSalve", re.IGNORECASE),    # Detoxing Salve (not sold)
    re.compile(r"^SURV_Innoculation", re.IGNORECASE),     # Scorched Fever Innoculation
    re.compile(r"^ResuscitationKit", re.IGNORECASE),      # Resuscitation Kit (perk item)
    re.compile(r"^CUT_P01E_HeartNougat", re.IGNORECASE), # cut Nougat Heart candies
    re.compile(r"^SwarmMeat", re.IGNORECASE),             # Flying Ant Meat / Crispy Flying Ant Bits
    re.compile(r"^Fishing_.*_Glowing$", re.IGNORECASE),  # Glowing fish dishes (no COBJ)
    re.compile(r"^ZZZ_Fishing_.*_Glowing", re.IGNORECASE), # Canned glowing fish (no COBJ)
    re.compile(r"^PTS_", re.IGNORECASE),                  # prototype-scrapped (Potion of Experience/Exploration/Knowledge)
    re.compile(r"^CUT_", re.IGNORECASE),                  # anything explicitly tagged CUT_ in the game data
    re.compile(r"^SpoiledFood_", re.IGNORECASE),          # test decay items (Spoiled Meat/Vegetables/Fruit)
    re.compile(r"_Spoiled$", re.IGNORECASE),              # spoiled variants (Milk_Brahmin_Spoiled, *FluidPotion_Spoiled)
    re.compile(r"^FloraSpecimenJarYellow$", re.IGNORECASE), # Raw Yellowcake Flux (cut specimen jar)
]

# ---------------------------------------------------------------------------
# Mutation detection from keywords
# ---------------------------------------------------------------------------
# Herbivore doubles plant food; Carnivore doubles meat food.

HERBIVORE_KEYWORDS = {
    "IngredientTypeFruit",
    "IngredientTypeVegetable",
    "IngredientTypeHerb",
    "FoodTypeVegHam",
}

CARNIVORE_KEYWORDS = {
    "IngredientTypeMeat",
    "IngredientTypeEgg",
    "FoodTypeChickenMeat",
    "MealTypeSteak",
}

# ---------------------------------------------------------------------------
# Buff name mapping from MGEF_EDID
# ---------------------------------------------------------------------------
# We parse the MGEF_EDID to extract human-readable buff names.
# Only care about food/alcohol/chem "Fortify*" and "Restore*" effects.

MGEF_BUFF_MAP = {
    # SPECIAL stats
    "FortifyStrength":          "Strength",
    "FortifyPerception":        "Perception",
    "FortifyEndurance":         "Endurance",
    "FortifyCharisma":          "Charisma",
    "FortifyIntelligence":      "Intelligence",
    "FortifyAgility":           "Agility",
    "FortifyLuck":              "Luck",
    # Derived stats
    "FortifyActionPoints":      "Max AP",
    "FortifyActionPointRegen":  "AP Regen",
    "FortifyCarryWeight":       "Carry Weight",
    "FortifyHealth":            "Max HP",
    "FortifyHealRate":          "Heal Rate",
    "FortifyCritDamage":        "Crit Damage",
    "FortifyMeleeDamage":       "Melee Damage",
    "FortifyDamageResist":      "Damage Resist",
    "FortifyResistDamage":      "Damage Resist",
    "FortifyResistEnergy":      "Energy Resist",
    "FortifyResistFire":        "Fire Resist",
    "FortifyResistPoison":      "Poison Resist",
    "FortifyResistRads":        "Rad Resist",
    "FortifyResistRadExposure": "Rad Resist",
    "FortifyResistRadIngestion":"Rad Resist",
    "FortifyXPBonus":           "XP",
    "FortifyBarter":            "Barter",
    "FortifyGunAccuracy":       "Gun Accuracy",
    "ExtraDamage":              "Extra Damage",
    "RestoreActionPoints":      "Restore AP",
    "RestoreHealth":            "Restore HP",
    "Food_FortifyMoveSpeed":    "Move Speed",
    "FortifyMaxBobber":         "Max Bobber",
    "DLC03_FortifyDamageResistFog": "Fog Resist",
}

# Compiled regex: match any key from MGEF_BUFF_MAP at the start of the EDID
# (stripping suffix like Food, Alcohol, ChemEffect, XCell, etc.)
_BUFF_RE = re.compile(
    r"^(" + "|".join(re.escape(k) for k in sorted(MGEF_BUFF_MAP.keys(), key=len, reverse=True)) + r")",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def clean(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def extract_keyword_names(keywords_flat: str) -> Set[str]:
    """Parse Keywords_Flat like 'ObjectTypeFood[00055ECC] | MealTypeRaw[00134858]'
    into a set of keyword names."""
    names = set()
    for chunk in keywords_flat.split("|"):
        chunk = chunk.strip()
        if not chunk:
            continue
        # Format: Name[FormID]:EDID:Type  OR  FormID:Name:Type
        bracket = chunk.find("[")
        if bracket > 0:
            names.add(chunk[:bracket].strip())
        else:
            # Fallback: try colon-separated
            parts = chunk.split(":")
            if len(parts) >= 2:
                names.add(parts[1].strip())
    return names


def detect_category(edid: str, keywords: Set[str], is_canned: bool, meal_keywords: Set[str]) -> Tuple[str, str]:
    """Return (menu_category, order_category) for an ALCH record.
    Returns ('__skip__', '') for items that should NOT be on the menu.
    Returns ('Other', 'other') for items that don't match any rule."""

    # Skip test/debug/pet items — run both filters:
    #   1) the shared cut_content module (primary source of truth)
    #   2) the local SKIP_EDID_PATTERNS (sync-specific extras like PETS_ /
    #      Debug / the bunch of event-crate / dart / syringer patterns that
    #      aren't strictly "cut content" but still shouldn't hit the menu)
    if is_cut(edid):
        return ("__skip__", "")
    for pat in SKIP_EDID_PATTERNS:
        if pat.search(edid):
            return ("__skip__", "")

    # Check Fish by EDID pattern
    for pat in FISH_EDID_PATTERNS:
        if pat.search(edid):
            return ("Fish", "fish")

    # Check Magazines & Bobbleheads by EDID pattern
    for pat in MAGAZINE_BOBBLEHEAD_PATTERNS:
        if pat.search(edid):
            return ("Magazines & Bobbleheads", "magazine")

    # Pre War Clean items by EDID suffix
    if edid.lower().endswith("_prewar_clean"):
        return ("Pre War Food", "prewarf")

    # Check keyword-based rules
    for kw_sub, cat, order in KEYWORD_TO_CATEGORY:
        for kw in keywords:
            if kw_sub.lower() in kw.lower():
                return (cat, order)

    # Canned food
    if is_canned:
        return ("Canned", "canned")

    # Food items by MealType / IngredientType
    has_food = any("ObjectTypeFood" in kw for kw in keywords)
    has_raw = any("MealTypeRaw" in kw for kw in keywords)
    has_cooked = any(kw in meal_keywords for kw in {
        "MealTypeCooked", "MealTypeSoup", "MealTypeTasty",
        "MealTypeGourmet", "MealTypeSteak", "MealTypeBirthdayCake",
    })
    has_packaged = any("MealTypePackaged" in kw for kw in keywords)
    has_ingredient = any(kw.startswith("IngredientType") for kw in keywords)

    if has_food or has_ingredient:
        if has_packaged:
            return ("Pre War Food", "prewarf")
        if has_raw and not has_cooked:
            return ("Ingredients", "ingredient")
        if has_cooked or has_food:
            # Could be Dishes & Teas or Condiments — default to Dishes & Teas
            return ("Dishes & Teas", "food")
        return ("Ingredients", "ingredient")

    # Catch-all: items that don't match any rule go to Other
    # for manual review via the diagnostics page
    return ("Other", "other")


def detect_mutation(keywords: Set[str]) -> str:
    """Return 'Herbivore', 'Carnivore', 'Both', or '' based on keywords."""
    is_herb = bool(keywords & HERBIVORE_KEYWORDS)
    is_carn = bool(keywords & CARNIVORE_KEYWORDS)
    if is_herb and is_carn:
        return "Both"
    if is_herb:
        return "Herbivore"
    if is_carn:
        return "Carnivore"
    return ""


def detect_buffs(effects: List[Dict[str, str]]) -> str:
    """Extract human-readable buff names from a record's effects.
    Returns pipe-separated list like 'Strength | Carry Weight | XP'."""
    buffs: List[str] = []
    seen: Set[str] = set()

    for eff in effects:
        mgef_edid = clean(eff.get("MGEF_EDID", ""))
        if not mgef_edid:
            continue

        m = _BUFF_RE.search(mgef_edid)
        if m:
            key = m.group(1)
            # Case-insensitive lookup
            for map_key, name in MGEF_BUFF_MAP.items():
                if map_key.lower() == key.lower():
                    if name not in seen:
                        seen.add(name)
                        buffs.append(name)
                    break

    return " | ".join(buffs)


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------

# Month-name → number so "Apr_2026" sorts AFTER "March_2026" instead of
# before it (alphabetical: Apr < Dec < Feb < March). xEdit exports embed the
# month as a short English word in the filename, which makes plain sorting
# unsafe — we previously shipped builds against the March TSV even when the
# April export was sitting right next to it.
_MONTH_TO_NUM = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

_DATE_RE = re.compile(r"_([A-Za-z]+)_(\d{4})", re.IGNORECASE)


def _filename_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def find_latest_file(data_dir: str, pattern: str) -> Optional[str]:
    """Find the most recent file matching a glob pattern.

    Sorts by the embedded-date key (via tsv_source.export_key) which already
    gives a total order — (date, time), with undated filenames sorting oldest —
    and never touches mtime, so a fresh git checkout (identical mtimes) still
    resolves correctly. This was previously a plain alphabetical sort and
    silently picked March over April.
    """
    matches = glob.glob(os.path.join(data_dir, pattern))
    if not matches:
        return None
    # _filename_date_key delegates to tsv_source.export_key, returning
    # (datetime.date, time). A single sort is correct; the old code compared
    # that key with `> 0`, which raised once the key became a date, not an int.
    matches.sort(key=_filename_date_key)
    return matches[-1]


def load_alch_base(path: str) -> List[Dict[str, str]]:
    """Load ALCH base export TSV."""
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_alch_effects(path: str) -> Dict[str, List[Dict[str, str]]]:
    """Load ALCH effects TSV, grouped by ALCH_FormID."""
    grouped: Dict[str, List[Dict[str, str]]] = {}
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            fid = clean(row.get("ALCH_FormID", ""))
            if fid:
                grouped.setdefault(fid, []).append(row)
    return grouped


def load_menu_tsv(path: str) -> List[Dict[str, str]]:
    """Load existing BnB_Menu_Items.tsv."""
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_cobj_recipes(path: str) -> Dict[str, Dict[str, Any]]:
    """Load dist/cobj-recipes.json and index ingredient counts by display name
    AND by recipe EDID (some ALCH records match COBJ via edid rather than name).

    Returns {"by_name": {lowercase_name: ingredient_count},
             "by_edid": {lowercase_edid: ingredient_count}}.

    Missing file is NOT fatal — we just can't auto-price. build_cobj_recipes_json
    should have run before this, but during a fresh-clone bootstrap or a partial
    build it may not exist yet. We warn and fall through to blank prices.
    """
    if not os.path.isfile(path):
        return {"by_name": {}, "by_edid": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"WARNING: failed to load {path}: {e}", file=sys.stderr)
        return {"by_name": {}, "by_edid": {}}

    recipes = data.get("recipes", {}) or {}
    meta = data.get("recipe_meta", {}) or {}

    by_name: Dict[str, int] = {}
    by_edid: Dict[str, int] = {}
    for name, ingredients in recipes.items():
        count = len(ingredients) if isinstance(ingredients, dict) else 0
        if not count:
            continue
        by_name[name.lower()] = count
        # cnam_edid is the recipe's produced-item EDID (matches ALCH_EDID for
        # food/chem items); edid is the COBJ record's own EDID (less useful).
        m = meta.get(name, {}) or {}
        cnam = clean(m.get("cnam_edid", ""))
        if cnam:
            by_edid[cnam.lower()] = count

    return {"by_name": by_name, "by_edid": by_edid}


def calc_prices(ingredient_count: int) -> Tuple[str, str, str]:
    """Apply the BnB pricing formula for standard (non-canned) items.

      price_xbox = ingredient_count * 5
      price_ps   = price_xbox + 10
      price_pc   = price_xbox + 10

    Returns ("", "", "") if ingredient_count is 0 or negative (no recipe ⇒
    no auto-price; the row stays blank and the diagnostic will flag it).
    """
    if ingredient_count <= 0:
        return ("", "", "")
    xbox = ingredient_count * 5
    ps_pc = xbox + 10
    return (str(xbox), str(ps_pc), str(ps_pc))


# ---------------------------------------------------------------------------
# Canned-item pricing
# ---------------------------------------------------------------------------
# Canned items are priced as: fresh item's ACTUAL price + 10 caps.
#   price_xbox = fresh_price_xbox + 10
#   price_ps   = fresh_price_ps   + 10
#   price_pc   = fresh_price_pc   + 10
#
# The fresh price comes from looking up the non-canned version in the
# existing menu TSV. Some fresh items use the ingredient formula
# (ingredients × 5), others have static premium prices (e.g. Brain Bombs
# at 100c). This approach respects both.
#
# If no fresh version exists in the menu, we fall back to the ingredient
# formula: (ingredient_count × 5) + 10 for Xbox, + 20 for PS/PC.
#
# The fresh item is found by stripping the _Cannery suffix (and any
# trailing _G1/_G2 for Season 25 variants) and any SCORE_ prefix from
# the EDID, or by stripping "Canned " from the display name.

_CANNERY_SUFFIX_RE = re.compile(r"_Cannery.*$", re.IGNORECASE)
_SCORE_PREFIX_RE = re.compile(r"^SCORE_S?\d+_", re.IGNORECASE)

CANNED_MARKUP = 10


def derive_fresh_edid(canned_edid: str) -> str:
    """Strip _Cannery suffix and SCORE_ prefix to get the fresh item's EDID.

    Examples:
        BrahminMeatCooked_Cannery            → BrahminMeatCooked
        SCORE_25_BrainBombsGourmet_Cannery_G1 → BrainBombsGourmet
        SCORE_S25_BlightVegetableCookedSoup_Cannery_G2 → BlightVegetableCookedSoup
    """
    edid = _CANNERY_SUFFIX_RE.sub("", canned_edid)
    edid = _SCORE_PREFIX_RE.sub("", edid)
    return edid


def calc_canned_from_fresh_prices(
    fresh_xbox: str, fresh_ps: str, fresh_pc: str,
) -> Tuple[str, str, str]:
    """Add the canned markup (+10) to each of the fresh item's actual prices.

    Returns ("", "", "") if any fresh price is blank or non-numeric.
    """
    try:
        px = int(fresh_xbox) + CANNED_MARKUP
        pps = int(fresh_ps) + CANNED_MARKUP
        ppc = int(fresh_pc) + CANNED_MARKUP
    except (ValueError, TypeError):
        return ("", "", "")
    return (str(px), str(pps), str(ppc))


def calc_canned_from_ingredients(ingredient_count: int) -> Tuple[str, str, str]:
    """Fallback: derive canned price from ingredient count when no fresh
    item exists in the menu. Uses the standard ingredient formula + markup.

      price_xbox = (ingredient_count * 5) + 10
      price_ps   = (ingredient_count * 5) + 20
      price_pc   = (ingredient_count * 5) + 20

    Returns ("", "", "") if ingredient_count is 0 or negative.
    """
    if ingredient_count <= 0:
        return ("", "", "")
    xbox = (ingredient_count * 5) + CANNED_MARKUP
    ps_pc = (ingredient_count * 5) + CANNED_MARKUP + 10  # +10 platform markup
    return (str(xbox), str(ps_pc), str(ps_pc))


def save_menu_tsv(path: str, rows: List[Dict[str, str]]) -> None:
    """Write BnB_Menu_Items.tsv preserving column order."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MENU_COLS, delimiter="\t",
                                lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def sync(
    alch_base_path: str,
    alch_effects_path: str,
    menu_path: str,
    cobj_json_path: str,
    diag: Diagnostics,
) -> List[Dict[str, str]]:
    """Run the sync and return updated menu rows."""

    # Load everything
    alch_rows = load_alch_base(alch_base_path)
    effects_by_fid = load_alch_effects(alch_effects_path)
    menu_rows = load_menu_tsv(menu_path)
    cobj_index = load_cobj_recipes(cobj_json_path)

    # ── Self-healing cut-content purge ───────────────────────────────────
    # Any existing menu row whose EDID matches a cut/test/debug pattern is
    # removed before we do any work. This catches legacy rows that slipped
    # in before the cut_content patterns were complete (e.g.
    # zzz_CranberryTastyRelish_Cannery, AC_SQ04_Reopening_* Nukashine 2,
    # _Disease Chance Food 0-4 etc.) and means the master TSV converges to
    # a clean state just by re-running this script.
    menu_rows, purged_rows = purge_cut_rows(menu_rows, edid_col="edid")
    if purged_rows:
        for r in purged_rows:
            diag.info(
                "bnb_menu_sync.purge.cut",
                f"Purged cut-content row: {clean(r.get('name',''))!r} ({clean(r.get('edid',''))})",
                detail=clean(r.get("edid", "")),
                context={"name": clean(r.get("name", "")), "edid": clean(r.get("edid", ""))},
            )
        print(f"Purged {len(purged_rows)} cut-content row(s) from master TSV", file=sys.stderr)

    print(f"ALCH base: {len(alch_rows)} records", file=sys.stderr)
    print(f"ALCH effects: {len(effects_by_fid)} records with effects", file=sys.stderr)
    print(f"Menu: {len(menu_rows)} existing items (after cut purge)", file=sys.stderr)
    print(
        f"COBJ recipe index: {len(cobj_index['by_name'])} by-name, "
        f"{len(cobj_index['by_edid'])} by-edid entries",
        file=sys.stderr,
    )
    if not cobj_index["by_name"] and not cobj_index["by_edid"]:
        diag.warning(
            "bnb_menu_sync.cobj.missing",
            "cobj-recipes.json is empty or missing — new items will be added with blank prices.",
            detail=cobj_json_path,
        )

    # Index existing menu items by edid (lowercase) for fast lookup
    menu_by_edid: Dict[str, Dict[str, str]] = {}
    for row in menu_rows:
        edid = clean(row.get("edid", "")).lower()
        if edid:
            menu_by_edid[edid] = row

    # Also index by form_id for fallback matching
    menu_by_fid: Dict[str, Dict[str, str]] = {}
    for row in menu_rows:
        fid = clean(row.get("form_id", "")).lower()
        if fid:
            menu_by_fid[fid] = row

    # Index by display name (lowercase) for fresh-item price lookups.
    # Used by the canned pricing path to find the fresh version's actual
    # price — respects static premium prices (e.g. Brain Bombs at 100c).
    menu_by_name: Dict[str, Dict[str, str]] = {}
    for row in menu_rows:
        n = clean(row.get("name", "")).lower()
        if n:
            menu_by_name[n] = row

    # Track which menu categories currently exist
    existing_categories = {clean(r.get("menu category", "")).lower() for r in menu_rows}
    existing_categories.discard("")

    # Build a set of EDIDs that have a _PreWar_Clean variant so we can
    # identify their "dirty" (post-war) counterparts.
    prewar_clean_bases: Set[str] = set()
    for alch in alch_rows:
        edid = clean(alch.get("ALCH_EDID", ""))
        if edid.lower().endswith("_prewar_clean"):
            base = edid[:edid.lower().index("_prewar_clean")]
            prewar_clean_bases.add(base.lower())

    # Process each ALCH record
    new_items: List[Dict[str, str]] = []
    updated_count = 0
    skipped_count = 0
    new_count = 0

    for alch in alch_rows:
        fid = clean(alch.get("ALCH_FormID", ""))
        edid = clean(alch.get("ALCH_EDID", ""))
        full_name = clean(alch.get("FULL", ""))
        keywords_flat = clean(alch.get("Keywords_Flat", ""))
        is_canned = clean(alch.get("ENIT_IsCanned", "")).lower() == "true"

        if not edid or not full_name:
            continue

        keywords = extract_keyword_names(keywords_flat)
        meal_keywords = keywords  # same set, detect_category picks out MealType* ones

        # Detect category
        cat, order_cat = detect_category(edid, keywords, is_canned, meal_keywords)

        # Apply Pre War / Post War display-name suffix for packaged food variants
        if edid.lower().endswith("_prewar_clean"):
            if not full_name.endswith("(Pre War)"):
                full_name = full_name + " (Pre War)"
        elif edid.lower() in prewar_clean_bases:
            # This EDID has a _PreWar_Clean counterpart → it's the post-war version
            if not full_name.endswith("(Post War)"):
                full_name = full_name + " (Post War)"

        if cat == "__skip__":
            skipped_count += 1
            continue

        # All valid categories are accepted — new categories are created
        # automatically. Items land in "Other" if no keyword rule matches,
        # and staff can re-categorise via the diagnostics page.

        # Detect mutation
        mutation = detect_mutation(keywords)

        # Detect buffs from effects
        effects = effects_by_fid.get(fid, [])
        buff = detect_buffs(effects)

        # Check if already in menu
        existing = menu_by_edid.get(edid.lower()) or menu_by_fid.get(fid.lower())

        if existing:
            # Always update mutation and buff from game data
            changed = False
            if mutation and existing.get("mutation", "") != mutation:
                existing["mutation"] = mutation
                changed = True
            if buff and existing.get("buff", "") != buff:
                existing["buff"] = buff
                changed = True
            # Only update display name for Pre War/Post War suffix corrections
            # (don't overwrite manual name edits like "Mountain Honey Moonshine")
            old_name = clean(existing.get("name", ""))
            is_prewar_rename = "(Pre War)" in full_name or "(Post War)" in full_name
            if is_prewar_rename and old_name != full_name:
                # Replace (Clean)→(Pre War), (Dirty)→(Post War) in existing name
                new_name = old_name.replace("(Clean)", "(Pre War)").replace("(Dirty)", "(Post War)")
                if new_name != old_name:
                    existing["name"] = new_name
                    changed = True
            if changed:
                updated_count += 1
        else:
            # ─── NEW ITEM ───
            # Canned items use a different pricing path: look up the FRESH
            # version's ACTUAL price in the menu and add +10 canned markup.
            # This respects static premium prices (e.g. Brain Bombs at 100c
            # → Canned Brain Bombs at 110c). Falls back to ingredient
            # formula + markup only if no fresh version exists in the menu.
            if cat == "Canned":
                fresh_name = re.sub(
                    r"^Canned\s+", "", full_name, flags=re.IGNORECASE
                ).lower()
                fresh_row = menu_by_name.get(fresh_name)

                if fresh_row:
                    # Fresh item exists — use its actual prices + markup
                    px, pps, ppc = calc_canned_from_fresh_prices(
                        clean(fresh_row.get("price_xbox", "")),
                        clean(fresh_row.get("price_ps", "")),
                        clean(fresh_row.get("price_pc", "")),
                    )
                    fresh_ing = 0  # not used in this path

                if not fresh_row or not px:
                    # No fresh item, or fresh price is blank/N/A —
                    # fall back to ingredient formula + canned markup
                    fresh_edid = derive_fresh_edid(edid)
                    fresh_ing = (
                        cobj_index["by_edid"].get(fresh_edid.lower())
                        or cobj_index["by_name"].get(fresh_name)
                        or 0
                    )
                    px, pps, ppc = calc_canned_from_ingredients(fresh_ing)
            else:
                # Standard auto-price from COBJ recipe ingredient count.
                # Look up by EDID first (unambiguous) then fall back to
                # display name (handles ALCH renames since recipe was written).
                ing_count = (
                    cobj_index["by_edid"].get(edid.lower())
                    or cobj_index["by_name"].get(full_name.lower())
                    or 0
                )
                px, pps, ppc = calc_prices(ing_count)

            new_row = {col: "" for col in MENU_COLS}
            new_row["menu category"] = cat
            new_row["name"] = full_name
            new_row["edid"] = edid
            new_row["form_id"] = fid
            new_row["build"] = ""
            new_row["price_xbox"] = px
            new_row["price_ps"] = pps
            new_row["price_pc"] = ppc
            # limit_new / limit_existing intentionally left blank — the chef
            # must set order caps manually per the "needs_limits" diagnostic.
            new_row["mutation"] = mutation
            new_row["buff"] = buff
            new_row["order category"] = order_cat
            new_items.append(new_row)
            new_count += 1

            if px:
                if cat == "Canned" and fresh_row:
                    price_note = (
                        f"Canned pricing: fresh {fresh_name!r} is "
                        f"{clean(fresh_row.get('price_xbox',''))} Xbox, +10 markup"
                    )
                elif cat == "Canned":
                    price_note = (
                        f"Canned pricing (ingredient fallback): {fresh_ing} "
                        f"ingredients × 5 + 10 markup"
                    )
                else:
                    price_note = (
                        f"Auto-priced from {ing_count}-ingredient COBJ recipe"
                    )
                msg = (
                    f"New item {full_name!r} -> {cat}. {price_note}: "
                    f"XBOX={px}, PS/PC={pps}. "
                    f"Order limits (new/existing) still need to be set."
                )
                diag.warning(
                    "bnb_menu_sync.new_item.needs_limits",
                    msg,
                    detail=f"edid={edid}, form_id={fid}",
                    context={
                        "name": full_name,
                        "category": cat,
                        "edid": edid,
                        "form_id": fid,
                        "ingredient_count": fresh_ing if cat == "Canned" else ing_count,
                        "price_xbox": px,
                        "price_ps": pps,
                        "price_pc": ppc,
                    },
                )
            else:
                if cat == "Canned":
                    no_recipe_note = (
                        f"No fresh version found in menu (tried "
                        f"{fresh_name!r}) and no COBJ recipe found, so "
                        f"no auto-price could be calculated."
                    )
                else:
                    no_recipe_note = (
                        f"No COBJ recipe found, so no auto-price could be "
                        f"calculated."
                    )
                msg = (
                    f"New item {full_name!r} -> {cat}. {no_recipe_note} "
                    f"Prices and order limits both need to be set manually."
                )
                diag.warning(
                    "bnb_menu_sync.new_item.needs_price_and_limits",
                    msg,
                    detail=f"edid={edid}, form_id={fid}",
                    context={
                        "name": full_name,
                        "category": cat,
                        "edid": edid,
                        "form_id": fid,
                    },
                )

    # Merge new items into existing rows
    all_rows = menu_rows + new_items

    # Sort: by category (A-Z) then name (A-Z)
    all_rows.sort(key=lambda r: (
        clean(r.get("menu category", "")).lower(),
        clean(r.get("name", "")).lower(),
    ))

    # Report unpriced items
    unpriced = []
    for row in all_rows:
        name = clean(row.get("name", ""))
        cat = clean(row.get("menu category", ""))
        px = clean(row.get("price_xbox", ""))
        pps = clean(row.get("price_ps", ""))
        ppc = clean(row.get("price_pc", ""))

        # N/A counts as "priced" (intentionally not for sale)
        def is_priced(v):
            return v != "" and v.upper() != ""

        has_any = px or pps or ppc
        if not has_any:
            unpriced.append((name, cat))

    for name, cat in unpriced:
        diag.error(
            "bnb_menu_sync.unpriced",
            f"No price set: {name!r} ({cat})",
            detail=name,
            context={"name": name, "category": cat},
        )

    print(f"\nSync complete:", file=sys.stderr)
    print(f"  Existing items updated (mutation/buff): {updated_count}", file=sys.stderr)
    print(f"  New items added: {new_count}", file=sys.stderr)
    print(f"  Skipped (no category match / test / pet): {skipped_count}", file=sys.stderr)
    print(f"  Unpriced items: {len(unpriced)}", file=sys.stderr)
    print(f"  Total menu items: {len(all_rows)}", file=sys.stderr)

    diag.info(
        "bnb_menu_sync.summary",
        f"Sync: {len(all_rows)} total, {new_count} new, {updated_count} updated, {len(unpriced)} unpriced.",
        context={
            "total": len(all_rows),
            "new": new_count,
            "updated": updated_count,
            "unpriced": len(unpriced),
            "skipped": skipped_count,
        },
    )

    return all_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Sync ALCH export into BnB Menu TSV")
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Diagnostics output dir")
    parser.add_argument("--alch-base", type=str, default=None,
                        help="ALCH base TSV filename (auto-detects latest if omitted)")
    parser.add_argument("--alch-effects", type=str, default=None,
                        help="ALCH effects TSV filename (auto-detects latest if omitted)")
    parser.add_argument("--cobj-json", type=str, default=None,
                        help="Path to cobj-recipes.json (defaults to <outdir>/cobj-recipes.json). "
                             "Used to auto-price new items via ingredient_count x 5.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write changes, just report what would happen")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="bnb_menu_sync", outdir=args.outdir)

    # Find ALCH files.
    #
    # The base ALCH TSV pattern "ALCH_Export_*.tsv" unfortunately also matches
    # "ALCH_Export_*_Effects.tsv" — we must filter those out BEFORE picking
    # the newest, otherwise find_latest_file might hand back an effects file
    # and we'd end up treating it as the base.
    if args.alch_base:
        base_path = os.path.join(args.data_dir, args.alch_base)
    else:
        candidates = [
            p for p in glob.glob(os.path.join(args.data_dir, "ALCH_Export_*.tsv"))
            if "_Effects" not in os.path.basename(p)
        ]
        if candidates:
            candidates.sort(key=_filename_date_key)
            base_path = candidates[-1]
        else:
            base_path = None

    if args.alch_effects:
        effects_path = os.path.join(args.data_dir, args.alch_effects)
    else:
        effects_path = find_latest_file(args.data_dir, "ALCH_Export_*_Effects.tsv")

    if not base_path or not os.path.isfile(base_path):
        diag.error("bnb_menu_sync.missing_alch_base", "ALCH base TSV not found")
        diag.save()
        sys.exit(1)

    if not effects_path or not os.path.isfile(effects_path):
        diag.error("bnb_menu_sync.missing_alch_effects", "ALCH effects TSV not found")
        diag.save()
        sys.exit(1)

    menu_path = os.path.join(args.data_dir, MENU_TSV)
    if not os.path.isfile(menu_path):
        diag.error("bnb_menu_sync.missing_menu", f"{MENU_TSV} not found")
        diag.save()
        sys.exit(1)

    # COBJ recipes JSON — built by build_cobj_recipes_json.py. Used for
    # auto-pricing new items. If it's missing we carry on with blank prices
    # (warned in diag) rather than aborting.
    cobj_json_path = args.cobj_json or os.path.join(args.outdir, "cobj-recipes.json")

    print(f"ALCH base:    {base_path}", file=sys.stderr)
    print(f"ALCH effects: {effects_path}", file=sys.stderr)
    print(f"Menu TSV:     {menu_path}", file=sys.stderr)
    print(f"COBJ JSON:    {cobj_json_path}", file=sys.stderr)

    rows = sync(base_path, effects_path, menu_path, cobj_json_path, diag)

    if args.dry_run:
        print("\n[DRY RUN] No files written.", file=sys.stderr)
    else:
        save_menu_tsv(menu_path, rows)
        print(f"\nWrote {menu_path} ({len(rows)} rows)", file=sys.stderr)

    diag.save()


if __name__ == "__main__":
    main()
