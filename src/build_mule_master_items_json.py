from __future__ import annotations

"""
build_mule_master_items_json.py
================================
Generates dist/mule-master-items.json — a comprehensive item list for the
staff/member portal Mule tab's "Master List" feature.

Players use the Master List to search and assign items to their mule
characters. This builder pulls from two primary data sources:

  1) menu-items.json  — consumable categories (food, chems, alcohol, etc.)
  2) Supplement files  — categories not covered by the menu

  Category           Source
  ─────────────────  ──────────────────────────────────────────────────
  alcohol            menu-items.json → "Alcohol"
  chem               menu-items.json → "Chems"
  serum              menu-items.json → "Serums"
  food_tea           menu-items.json → "Dishes & Teas"
  fish               menu-items.json → "Fish"
  ingredient         menu-items.json → "Ingredients"
  condiment_np       menu-items.json → "Condiments & Non-Perishable"
                                      + "Pre War Food" + "Canned"
  soda               menu-items.json → "Soda & Drinks"
  bobblehead         dist/collectables_bobbleheads.json
  magazine           dist/collectables_magazines.json  (individual issues)
  legendary_mod      OMOD TSV  (weapon+armor star 1–4 prefix names)
  headwear           KYWD TSV keyword 0033C76A → ARMO refs (hats/masks)
  clothes            KYWD TSV keyword 0033C76A → ARMO refs (outfits)
  junk               dist/mule-defaults.json  (hand-curated junk list)

Output shape
────────────
{
  "version":   "YYYY-MM-DD",
  "generated": "<ISO-8601 UTC>",
  "categories": {
    "junk":          [ { "name": "Steel" }, ... ],
    "ingredient":    [ { "name": "Corn" }, ... ],
    ...
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
import sys
import tempfile
from typing import Any, Dict, List, Set


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
# PRIMARY SOURCE: menu-items.json
# ──────────────────────────────────────────────────────────────────────
# Mapping from mule category key → list of menu-items.json category names.
# Each mule category pulls from one or more menu categories.

MENU_CATEGORY_MAP = {
    "alcohol":      ["Alcohol"],
    "chem":         ["Chems"],
    "serum":        ["Serums"],
    "food_tea":     ["Dishes & Teas"],
    "fish":         ["Fish"],
    "ingredient":   ["Ingredients"],
    "condiment_np": ["Condiments & Non-Perishable", "Pre War Food", "Canned"],
    "soda":         ["Soda & Drinks"],
}


def load_all_menu_categories(dist_dir: str) -> dict[str, list[dict]]:
    """Load all consumable categories from menu-items.json in one pass.

    Returns a dict of mule_category_key → sorted list of {name} dicts.
    """
    path = os.path.join(dist_dir, "menu-items.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — all menu categories will be empty")
        return {k: [] for k in MENU_CATEGORY_MAP}

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("menu_items", [])

    # Build a reverse lookup: menu category name → mule category key
    reverse: dict[str, str] = {}
    for mule_key, menu_cats in MENU_CATEGORY_MAP.items():
        for mc in menu_cats:
            reverse[mc] = mule_key

    # Single pass through all items
    buckets: dict[str, set[str]] = {k: set() for k in MENU_CATEGORY_MAP}

    for it in items:
        cat = (it.get("category") or "").strip()
        mule_key = reverse.get(cat)
        if mule_key is None:
            continue  # skip categories not mapped (e.g. "Other", "Magazines & Bobbleheads")
        name = (it.get("name") or "").strip()
        if name:
            buckets[mule_key].add(name)

    # Convert to sorted lists and print counts
    result: dict[str, list[dict]] = {}
    for mule_key in MENU_CATEGORY_MAP:
        names = buckets[mule_key]
        menu_cats = MENU_CATEGORY_MAP[mule_key]
        result[mule_key] = sorted_unique_names(names)
        print(f"  {mule_key}: {len(names)} items from menu-items.json {menu_cats}")

    return result


# ──────────────────────────────────────────────────────────────────────
# SUPPLEMENT: Junk from mule-defaults.json
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
# SUPPLEMENT: Bobbleheads from collectables_bobbleheads.json
# ──────────────────────────────────────────────────────────────────────

def load_bobbleheads(dist_dir: str) -> list[dict]:
    path = os.path.join(dist_dir, "collectables_bobbleheads.json")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found — bobblehead list will be empty")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    names: set[str] = set()
    for group in data.get("groups", []):
        for it in group.get("items", []):
            name = (it.get("name") or "").strip()
            if name and not it.get("isCut"):
                names.add(name)
    print(f"  bobblehead: {len(names)} items from collectables_bobbleheads.json")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# SUPPLEMENT: Magazines from collectables_magazines.json
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
        if edid.startswith("BACKUP_") or name.startswith("Plan:"):
            continue
        names.add(name)

    print(f"  magazine: {len(names)} issues from collectables_magazines.json")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# SUPPLEMENT: Legendary mods from OMOD TSV
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
            if "mod_Legendary" not in edid:
                continue
            edid_up = edid.upper()
            if any(x in edid_up for x in ["ZZZ", "CUT", "_PARENT", "_EXTRACT",
                                            "_CRAFTING", "_CRAFTED", "CIRCUITBREAKER"]):
                continue
            if "Random" in edid or "random" in edid:
                continue
            names.add(full)

    print(f"  legendary_mod: {len(names)} unique prefixes from OMOD TSV")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# SUPPLEMENT: Apparel (headwear + clothes) from KYWD TSV keyword 0033C76A
# ──────────────────────────────────────────────────────────────────────

_APPAREL_EXCLUDE_PREFIXES = ("ATX_", "CUT_", "DEL_", "POST_", "SCORE_")
_APPAREL_EXCLUDE_CONTAINS = ("ZZZ", "NONPLAYABLE", "NON_PLAYABLE")
_HEADWEAR_HINTS = ("HEADWEAR", "HAT", "HELMET", "MASK")


def _is_excluded_apparel(edid: str) -> bool:
    up = edid.upper()
    for pfx in _APPAREL_EXCLUDE_PREFIXES:
        if up.startswith(pfx):
            return True
    for sub in _APPAREL_EXCLUDE_CONTAINS:
        if sub in up:
            return True
    if "KID" in up:
        return True
    return False


def _is_headwear(edid: str) -> bool:
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

    base, ext = os.path.splitext(kywd_path)
    refs_path = base + "_Refs" + ext
    if not os.path.exists(refs_path):
        print(f"  WARN: KYWD refs file not found at {refs_path} — apparel lists will be empty")
        return [], []

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
    print()

    # ── Primary source: menu-items.json ──
    print("── menu-items.json (primary) ──")
    categories = load_all_menu_categories(dist_dir)
    print()

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
            print()

    # ── Supplement sources ──
    print("── Supplement sources ──")
    categories["junk"] = load_junk(dist_dir)
    categories["bobblehead"] = load_bobbleheads(dist_dir)
    categories["magazine"] = load_magazines(dist_dir)
    categories["legendary_mod"] = load_legendary_mods(data_dir)
    hw, cl = load_apparel(data_dir)
    categories["headwear"] = hw
    categories["clothes"] = cl

    total = sum(len(v) for v in categories.values())
    now = dt.datetime.now(dt.timezone.utc)

    output = {
        "version": now.strftime("%Y-%m-%d"),
        "generated": now.isoformat(),
        "total_items": total,
        "categories": categories,
    }

    print()
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
