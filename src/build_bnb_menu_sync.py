from __future__ import annotations

"""
build_bnb_menu_sync.py
=======================
Syncs the ALCH export TSVs into BnB_Menu_Items.tsv.

What it does:
  1. Reads ALCH_Export_*.tsv (base) + ALCH_Export_*_Effects.tsv
  2. Reads the current BnB_Menu_Items.tsv
  3. For every ALCH record that matches an existing menu category
     (based on keyword analysis), ensures it exists in the menu TSV.
     New items are added with blank prices.
  4. Populates the "mutation" column (Herbivore / Carnivore / Both / blank)
     from ALCH keywords.
  5. Populates the "buff" column from MGEF effect names in the effects TSV.
  6. Writes the updated BnB_Menu_Items.tsv.
  7. Reports unpriced items to diagnostics.json for the staff portal.

Usage:
    python build_bnb_menu_sync.py
    python build_bnb_menu_sync.py --data-dir tsv --outdir dist
    python build_bnb_menu_sync.py --alch-base ALCH_Export_Apr_2026.tsv
                                  --alch-effects ALCH_Export_Apr_2026_Effects.tsv
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
    ("ObjectTypeNukaCola",         "Soda",                        "drink"),
    ("DrinkTypeSoda",              "Soda",                        "drink"),
    ("DrinkTypeSodaIcon",          "Soda",                        "drink"),
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

    # Skip test/debug/pet items
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

def find_latest_file(data_dir: str, pattern: str) -> Optional[str]:
    """Find the most recent file matching a glob pattern."""
    matches = sorted(glob.glob(os.path.join(data_dir, pattern)))
    return matches[-1] if matches else None


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
    diag: Diagnostics,
) -> List[Dict[str, str]]:
    """Run the sync and return updated menu rows."""

    # Load everything
    alch_rows = load_alch_base(alch_base_path)
    effects_by_fid = load_alch_effects(alch_effects_path)
    menu_rows = load_menu_tsv(menu_path)

    print(f"ALCH base: {len(alch_rows)} records", file=sys.stderr)
    print(f"ALCH effects: {len(effects_by_fid)} records with effects", file=sys.stderr)
    print(f"Menu: {len(menu_rows)} existing items", file=sys.stderr)

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
            # New item — add with blank prices
            new_row = {col: "" for col in MENU_COLS}
            new_row["menu category"] = cat
            new_row["name"] = full_name
            new_row["edid"] = edid
            new_row["form_id"] = fid
            new_row["mutation"] = mutation
            new_row["buff"] = buff
            new_row["order category"] = order_cat
            new_items.append(new_row)
            new_count += 1

            diag.warning(
                "bnb_menu_sync.new_item",
                f"New item added: {full_name!r} -> {cat}",
                detail=f"edid={edid}, form_id={fid}",
                context={"name": full_name, "category": cat, "edid": edid, "form_id": fid},
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
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write changes, just report what would happen")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="bnb_menu_sync", outdir=args.outdir)

    # Find ALCH files
    if args.alch_base:
        base_path = os.path.join(args.data_dir, args.alch_base)
    else:
        base_path = find_latest_file(args.data_dir, "ALCH_Export_*.tsv")
        # Exclude effects files
        if base_path and "_Effects" in base_path:
            candidates = sorted(glob.glob(os.path.join(args.data_dir, "ALCH_Export_*.tsv")))
            candidates = [c for c in candidates if "_Effects" not in c]
            base_path = candidates[-1] if candidates else None

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

    print(f"ALCH base:    {base_path}", file=sys.stderr)
    print(f"ALCH effects: {effects_path}", file=sys.stderr)
    print(f"Menu TSV:     {menu_path}", file=sys.stderr)

    rows = sync(base_path, effects_path, menu_path, diag)

    if args.dry_run:
        print("\n[DRY RUN] No files written.", file=sys.stderr)
    else:
        save_menu_tsv(menu_path, rows)
        print(f"\nWrote {menu_path} ({len(rows)} rows)", file=sys.stderr)

    diag.save()


if __name__ == "__main__":
    main()
