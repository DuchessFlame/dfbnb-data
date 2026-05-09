from __future__ import annotations

"""
build_mule_master_items_json.py
================================
Generates dist/mule-master-items.json — a comprehensive item list for the
staff/member portal Mule tab's "Master List" feature.

Players use the Master List to search and assign items to their mule
characters. This builder combines data from multiple sources into a
single flat JSON that the portal JS fetches once:

  Category           Source
  ─────────────────  ──────────────────────────────────────────────────
  junk               dist/mule-defaults.json  (hand-curated junk list)
  ingredient         ALCH TSV  (raw ingredients filtered by keyword, excl. fish)
  fish               ALCH TSV  (raw fish: ObjectTypeFish or fish EDID patterns)
  food_tea           dist/bnb-item-categories.json → food (excl condiment/soda)
  alcohol            dist/bnb-item-categories.json → alcohol
  bobblehead         dist/collectables_bobbleheads.json
  magazine           dist/collectables_magazines.json  (individual issues)
  legendary_mod      OMOD TSV  (weapon+armor star 1–4 prefix names)
  headwear           KYWD TSV keyword 0033C76A → ARMO refs (hats/masks)
  clothes            KYWD TSV keyword 0033C76A → ARMO refs (outfits)
  chem               dist/bnb-item-categories.json → chems
  serum              dist/bnb-item-categories.json → serums
  condiment_np       dist/menu-items.json → Condiments & Non-Perishable + Pre War Food
  soda               dist/menu-items.json → Soda & Drinks

Output shape
────────────
{
  "version":   "YYYY-MM-DD",
  "generated": "<ISO-8601 UTC>",
  "categories": {
    "junk":          [ { "name": "Steel" }, ... ],
    "ingredient":    [ { "name": "Corn" }, ... ],
    "fish":          [ { "name": "Walleye" }, ... ],
    "food_tea":      [ { "name": "Cranberry Relish" }, ... ],
    "alcohol":       [ { "name": "Ballistic Bock" }, ... ],
    "bobblehead":    [ { "name": "Bobblehead: Agility" }, ... ],
    "magazine":      [ { "name": "Backwoodsman 1" }, ... ],
    "legendary_mod": [ { "name": "Anti-Armor" }, ... ],
    "headwear":      [ { "name": "Fasnacht Raven Mask" }, ... ],
    "clothes":       [ { "name": "Asylum Worker Uniform Blue" }, ... ],
    "chem":          [ { "name": "Stimpak" }, ... ],
    "serum":         [ { "name": "Adrenal Reaction Serum" }, ... ],
    "condiment_np":  [ { "name": "Boiled Water" }, ... ],
    "soda":          [ { "name": "Nuka-Cola" }, ... ]
  }
}

Usage
─────
  python build_mule_master_items_json.py
  python build_mule_master_items_json.py --data-dir /path/to/tsvs --outdir /path/to/dist
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
import tempfile
from typing import Any, Dict, List, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from cut_content import is_cut  # noqa: E402
except Exception:
    def is_cut(edid: str) -> bool:
        """Fallback cut-content check if module not available."""
        if not edid:
            return True
        prefix = edid.split("_")[0].upper() if "_" in edid else ""
        return prefix in ("CUT", "ZZZ", "ZZZZ", "POST", "DEL", "TEST", "DEBUG")


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def norm_fid(s: str) -> str:
    s = (s or "").strip().upper()
    if s.startswith("0X"):
        s = s[2:]
    return s.zfill(8)[-8:]


def newest_tsv(pattern: str, data_dir: str) -> str | None:
    matches = glob.glob(os.path.join(data_dir, pattern))
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def read_tsv_rows(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rows.append(row)
    return rows


def sorted_unique_names(names: set[str]) -> list[dict]:
    return [{"name": n} for n in sorted(names, key=lambda x: x.lower())]


# ──────────────────────────────────────────────────────────────────────
# Junk from mule-defaults.json
# ──────────────────────────────────────────────────────────────────────

def load_junk(dist_dir: str) -> list[dict]:
    path = os.path.join(dist_dir, "mule-defaults.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — junk list will be empty")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    names: set[str] = set()
    for mule in data.get("mules", []):
        for item in mule.get("items", []):
            if item.get("type", "").strip().lower() == "junk":
                name = item.get("name", "").strip()
                if name:
                    names.add(name)
    print(f"  junk: {len(names)} items from mule-defaults.json")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Fish + Ingredients from ALCH TSV
# ──────────────────────────────────────────────────────────────────────

_FISH_EDID_PREFIXES = ("Fishing_Fish_", "SeasonalFish_", "Burn_Fish_", "Fish_")


def _is_fish(edid: str, kw_flat: str) -> bool:
    """Return True if this ALCH row is a raw fish (not cooked food/tea).

    A raw fish has MealTypeRaw AND either:
      - ObjectTypeFish keyword, OR
      - EDID starts with a known fish prefix
    """
    if "MealTypeRaw" not in kw_flat:
        return False
    if "ObjectTypeFish" in kw_flat:
        return True
    return any(edid.startswith(pfx) for pfx in _FISH_EDID_PREFIXES)


def load_fish(data_dir: str) -> list[dict]:
    """Pull raw fish from the ALCH TSV — items with ObjectTypeFish or fish EDID prefix."""
    alch_path = newest_tsv("ALCH_Export_*.tsv", data_dir)
    if not alch_path:
        print("  WARN: ALCH TSV not found — fish list will be empty")
        return []

    names: set[str] = set()
    with open(alch_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = (row.get("ALCH_EDID") or "").strip()
            name = (row.get("FULL") or "").strip()
            if not name or is_cut(edid):
                continue
            kw_flat = (row.get("Keywords_Flat") or "")
            if _is_fish(edid, kw_flat):
                names.add(name)

    print(f"  fish: {len(names)} items from ALCH TSV")
    return sorted_unique_names(names)


def load_ingredients(data_dir: str, dist_dir: str) -> list[dict]:
    """Pull raw ingredients from the ALCH TSV — items with MealTypeRaw keyword, excluding fish."""
    alch_path = newest_tsv("ALCH_Export_*.tsv", data_dir)
    if not alch_path:
        print("  WARN: ALCH TSV not found — ingredient list will be empty")
        return []

    names: set[str] = set()
    with open(alch_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = (row.get("ALCH_EDID") or "").strip()
            name = (row.get("FULL") or "").strip()
            if not name or is_cut(edid):
                continue
            kw_flat = (row.get("Keywords_Flat") or "")
            # MealTypeRaw marks actual raw ingredients (uncooked)
            # but exclude fish — they go in their own category
            if "MealTypeRaw" in kw_flat and not _is_fish(edid, kw_flat):
                names.add(name)

    print(f"  ingredient: {len(names)} items from ALCH TSV")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Food/Tea, Alcohol, Chems, Serums from bnb-item-categories.json
# ──────────────────────────────────────────────────────────────────────

def load_from_item_categories(dist_dir: str, cat_key: str, label: str) -> list[dict]:
    path = os.path.join(dist_dir, "bnb-item-categories.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — {label} list will be empty")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("categories", {}).get(cat_key, [])
    names: set[str] = set()
    for it in items:
        name = (it.get("name") or "").strip()
        if name:
            names.add(name)
    print(f"  {label}: {len(names)} items from bnb-item-categories.json[{cat_key}]")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Condiments & Non-Perishable, Soda & Drinks from menu-items.json
# ──────────────────────────────────────────────────────────────────────

def load_from_menu_items(dist_dir: str, menu_categories: list[str], label: str) -> list[dict]:
    """Pull items from menu-items.json matching one or more category names."""
    path = os.path.join(dist_dir, "menu-items.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — {label} list will be empty")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("menu_items", [])
    cat_set = set(menu_categories)
    names: set[str] = set()
    for it in items:
        cat = (it.get("category") or "").strip()
        if cat in cat_set:
            name = (it.get("name") or "").strip()
            if name:
                names.add(name)
    print(f"  {label}: {len(names)} items from menu-items.json categories {menu_categories}")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Bobbleheads from collectables_bobbleheads.json
# ──────────────────────────────────────────────────────────────────────

def load_bobbleheads(dist_dir: str) -> list[dict]:
    path = os.path.join(dist_dir, "collectables_bobbleheads.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — bobblehead list will be empty")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    names: set[str] = set()
    # Structure: { groups: [ { name, items: [ {name, isCut, ...} ] } ] }
    for group in data.get("groups", []):
        for it in group.get("items", []):
            name = (it.get("name") or "").strip()
            if name and not it.get("isCut"):
                names.add(name)
    print(f"  bobblehead: {len(names)} items from collectables_bobbleheads.json")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Magazines from collectables_magazines.json (individual issues)
# ──────────────────────────────────────────────────────────────────────

def load_magazines(dist_dir: str) -> list[dict]:
    path = os.path.join(dist_dir, "collectables_magazines.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — magazine list will be empty")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    names: set[str] = set()
    items_list = data if isinstance(data, list) else []
    if isinstance(data, dict):
        for key, val in data.items():
            if isinstance(val, list):
                items_list = val
                break

    for it in items_list:
        name = (it.get("name") or "").strip()
        edid = (it.get("edid") or "").strip()
        if not name or it.get("isCut"):
            continue
        # Skip BACKUP_ variants and Plan: entries
        if edid.startswith("BACKUP_") or name.startswith("Plan:"):
            continue
        names.add(name)

    print(f"  magazine: {len(names)} issues from collectables_magazines.json")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Legendary mods from OMOD TSV
# ──────────────────────────────────────────────────────────────────────

def load_legendary_mods(data_dir: str) -> list[dict]:
    omod_path = newest_tsv("OMOD_Export_*.tsv", data_dir)
    if not omod_path:
        print("  WARN: OMOD TSV not found — legendary mod list will be empty")
        return []

    names: set[str] = set()
    with open(omod_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = (row.get("OMOD_EDID") or row.get("EDID") or "").strip()
            full = (row.get("FULL") or "").strip()
            if not full or not edid:
                continue
            # Only legendary mods
            if "mod_Legendary" not in edid:
                continue
            # Skip cut content, parent/extract/crafting entries
            edid_up = edid.upper()
            if any(x in edid_up for x in ["ZZZ", "CUT", "_PARENT", "_EXTRACT",
                                            "_CRAFTING", "_CRAFTED", "CIRCUITBREAKER"]):
                continue
            # Skip generic/random crafting entries
            if "Random" in edid or "random" in edid:
                continue
            # The FULL name is the legendary prefix (e.g. "Anti-Armor")
            names.add(full)

    print(f"  legendary_mod: {len(names)} unique prefixes from OMOD TSV")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Apparel (headwear + clothes) from KYWD TSV keyword 0033C76A
# ──────────────────────────────────────────────────────────────────────

# Exclusion prefixes / substrings applied to the ARMO EDID
_APPAREL_EXCLUDE_PREFIXES = ("ATX_", "CUT_", "DEL_", "POST_", "SCORE_")
_APPAREL_EXCLUDE_CONTAINS = ("ZZZ", "NONPLAYABLE", "NON_PLAYABLE")

# Headwear classification: EDID contains any of these (case-insensitive)
_HEADWEAR_HINTS = ("HEADWEAR", "HAT", "HELMET", "MASK")


def _is_excluded_apparel(edid: str) -> bool:
    """Return True if the ARMO EDID should be excluded from apparel."""
    up = edid.upper()
    # Prefix checks
    for pfx in _APPAREL_EXCLUDE_PREFIXES:
        if up.startswith(pfx):
            return True
    # Substring checks
    for sub in _APPAREL_EXCLUDE_CONTAINS:
        if sub in up:
            return True
    # Kid / Kids check (word boundary-ish: preceded by _ or start)
    if "KID" in up:
        return True
    return False


def _is_headwear(edid: str) -> bool:
    """Return True if the ARMO EDID indicates headwear."""
    up = edid.upper()
    return any(h in up for h in _HEADWEAR_HINTS)


def load_apparel(data_dir: str) -> tuple[list[dict], list[dict]]:
    """Load apparel from KYWD TSV keyword 0033C76A refs.

    Returns (headwear_items, clothes_items) as sorted unique name lists.
    """
    kywd_path = newest_tsv("KYWD_Export_*.tsv", data_dir)
    if not kywd_path:
        print("  WARN: KYWD TSV not found — apparel lists will be empty")
        return [], []

    # Build the refs path — same name with _Refs suffix
    base, ext = os.path.splitext(kywd_path)
    refs_path = base + "_Refs" + ext
    if not os.path.exists(refs_path):
        print(f"  WARN: KYWD refs file not found at {refs_path} — apparel lists will be empty")
        return [], []

    # Step 1: Find the keyword row for 0033C76A to get its EDID
    target_fid = "0033C76A"
    target_edid = None
    with open(kywd_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            fid = norm_fid(row.get("FormID") or "")
            if fid == target_fid:
                target_edid = (row.get("EDID") or "").strip()
                break

    if not target_edid:
        print(f"  WARN: Keyword {target_fid} not found in KYWD TSV — apparel lists will be empty")
        return [], []

    # Step 2: Read refs file — find ARMO records referencing this keyword
    # Refs columns: KeywordEDID, RefSignature, RefEDID, RefName
    headwear: set[str] = set()
    clothes: set[str] = set()

    with open(refs_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            kw_edid = (row.get("KeywordEDID") or "").strip()
            if kw_edid != target_edid:
                continue

            sig = (row.get("RefSignature") or "").strip().upper()
            if sig != "ARMO":
                continue

            ref_edid = (row.get("RefEDID") or "").strip()
            ref_name = (row.get("RefName") or "").strip()

            if not ref_name or not ref_edid:
                continue
            if _is_excluded_apparel(ref_edid):
                continue

            if _is_headwear(ref_edid):
                headwear.add(ref_name)
            else:
                clothes.add(ref_name)

    print(f"  headwear: {len(headwear)} items from KYWD TSV (keyword {target_fid})")
    print(f"  clothes: {len(clothes)} items from KYWD TSV (keyword {target_fid})")
    return sorted_unique_names(headwear), sorted_unique_names(clothes)


# ──────────────────────────────────────────────────────────────────────
# Main builder
# ──────────────────────────────────────────────────────────────────────

def build(data_dir: str, dist_dir: str) -> dict:
    print("Building mule-master-items.json ...")

    categories = {}
    categories["junk"] = load_junk(dist_dir)
    categories["fish"] = load_fish(data_dir)
    categories["ingredient"] = load_ingredients(data_dir, dist_dir)
    categories["food_tea"] = load_from_item_categories(dist_dir, "food", "food_tea")
    categories["alcohol"] = load_from_item_categories(dist_dir, "alcohol", "alcohol")
    categories["bobblehead"] = load_bobbleheads(dist_dir)
    categories["magazine"] = load_magazines(dist_dir)
    categories["legendary_mod"] = load_legendary_mods(data_dir)
    categories["chem"] = load_from_item_categories(dist_dir, "chems", "chem")
    categories["serum"] = load_from_item_categories(dist_dir, "serums", "serum")
    hw, cl = load_apparel(data_dir)
    categories["headwear"] = hw
    categories["clothes"] = cl

    # Condiments & Non-Perishable (pre-made stockpile items + pre-war food)
    categories["condiment_np"] = load_from_menu_items(
        dist_dir, ["Condiments & Non-Perishable", "Pre War Food"], "condiment_np"
    )

    # Soda & Drinks
    categories["soda"] = load_from_menu_items(
        dist_dir, ["Soda & Drinks"], "soda"
    )

    # Remove any condiment_np / soda items from food_tea to avoid duplicates
    exclude_from_food = set()
    for item in categories["condiment_np"] + categories["soda"]:
        exclude_from_food.add(item["name"])
    if exclude_from_food:
        before = len(categories["food_tea"])
        categories["food_tea"] = [
            it for it in categories["food_tea"] if it["name"] not in exclude_from_food
        ]
        removed = before - len(categories["food_tea"])
        if removed:
            print(f"  food_tea: removed {removed} items now in condiment_np/soda")

    total = sum(len(v) for v in categories.values())
    now = dt.datetime.now(dt.timezone.utc)

    output = {
        "version": now.strftime("%Y-%m-%d"),
        "generated": now.isoformat(),
        "total_items": total,
        "categories": categories,
    }

    print(f"  TOTAL: {total} items across {len(categories)} categories")
    return output


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parser = argparse.ArgumentParser(description="Build mule-master-items.json")
    parser.add_argument("--data-dir", default=os.path.join(repo_root, "tsv"),
                        help="Directory containing xEdit TSV exports")
    parser.add_argument("--outdir", default=os.path.join(repo_root, "dist"),
                        help="Output directory for JSON")
    args = parser.parse_args()

    data = build(args.data_dir, args.outdir)

    out_path = os.path.join(args.outdir, "mule-master-items.json")
    # Atomic write
    fd, tmp = tempfile.mkstemp(dir=args.outdir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp, out_path)
        print(f"  Written: {out_path}")
    except Exception:
        os.unlink(tmp)
        raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
