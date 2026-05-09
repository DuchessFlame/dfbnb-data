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
  ingredient         ALCH TSV  (raw ingredients filtered by keyword)
  food_tea           dist/bnb-item-categories.json → food
  alcohol            dist/bnb-item-categories.json → alcohol
  bobblehead         dist/collectables_bobbleheads.json
  magazine           dist/collectables_magazines.json  (series only)
  legendary_mod      OMOD TSV  (weapon+armor star 1–4 prefix names)
  apparel            (manual list — hardcoded below, user maintains)
  chem               dist/bnb-item-categories.json → chems
  serum              dist/bnb-item-categories.json → serums

Output shape
────────────
{
  "version":   "YYYY-MM-DD",
  "generated": "<ISO-8601 UTC>",
  "categories": {
    "junk":          [ { "name": "Steel" }, ... ],
    "ingredient":    [ { "name": "Corn" }, ... ],
    "food_tea":      [ { "name": "Cranberry Relish" }, ... ],
    "alcohol":       [ { "name": "Ballistic Bock" }, ... ],
    "bobblehead":    [ { "name": "Bobblehead: Agility" }, ... ],
    "magazine":      [ { "name": "Astoundingly Awesome Tales" }, ... ],
    "legendary_mod": [ { "name": "Anti-Armor" }, ... ],
    "chem":          [ { "name": "Stimpak" }, ... ],
    "serum":         [ { "name": "Adrenal Reaction Serum" }, ... ],
    "apparel":       [ { "name": "Asylum Worker Uniform Blue" }, ... ]
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
# Ingredients from ALCH TSV (raw ingredients excluded from food)
# ──────────────────────────────────────────────────────────────────────

def load_ingredients(data_dir: str, dist_dir: str) -> list[dict]:
    """Pull raw ingredients from the ALCH TSV — items with MealTypeRaw keyword."""
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
            # MealTypeRaw marks actual raw ingredients (uncooked)
            kw_flat = (row.get("Keywords_Flat") or "")
            if "MealTypeRaw" in kw_flat:
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
# Magazines from collectables_magazines.json (series names only)
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
        # Extract series name (strip " 01", " 02" etc issue numbers)
        series = re.sub(r"\s+\d{1,2}$", "", name).strip()
        if series:
            names.add(series)

    print(f"  magazine: {len(names)} series from collectables_magazines.json")
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
# Apparel — manual list (user-maintained, updated in this file)
# ──────────────────────────────────────────────────────────────────────

# This list will be populated by the user. Leave empty until they
# provide the apparel list.
MANUAL_APPAREL: list[str] = [
    # User will provide this list — placeholder for now
]


def load_apparel() -> list[dict]:
    names = set(n.strip() for n in MANUAL_APPAREL if n.strip())
    print(f"  apparel: {len(names)} items (manual list)")
    return sorted_unique_names(names)


# ──────────────────────────────────────────────────────────────────────
# Main builder
# ──────────────────────────────────────────────────────────────────────

def build(data_dir: str, dist_dir: str) -> dict:
    print("Building mule-master-items.json ...")

    categories = {}
    categories["junk"] = load_junk(dist_dir)
    categories["ingredient"] = load_ingredients(data_dir, dist_dir)
    categories["food_tea"] = load_from_item_categories(dist_dir, "food", "food_tea")
    categories["alcohol"] = load_from_item_categories(dist_dir, "alcohol", "alcohol")
    categories["bobblehead"] = load_bobbleheads(dist_dir)
    categories["magazine"] = load_magazines(dist_dir)
    categories["legendary_mod"] = load_legendary_mods(data_dir)
    categories["chem"] = load_from_item_categories(dist_dir, "chems", "chem")
    categories["serum"] = load_from_item_categories(dist_dir, "serums", "serum")
    categories["apparel"] = load_apparel()

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
