#!/usr/bin/env python3
"""
src/build_atom_shop_json.py
============================
Reads src/bundles.json + the newest tsv/ENTM_Export_*.tsv,
fixes image URLs, adds DESC descriptions, validates,
and writes dist/atom_shop.json.
"""

import csv
import glob
import json
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC      = os.path.join(SCRIPT_DIR, "bundles.json")
DIST     = os.path.join(SCRIPT_DIR, "..", "dist", "atom_shop.json")
TSV_ROOT = os.path.join(SCRIPT_DIR, "..", "tsv")

IMAGE_BASE_URL = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/atom-shop/request-item-images/"

OLD_IMAGE_BASES = [
    "https://www.buffsnbrew.com/wp-content/uploads/fo76/storefront/bundles/",
    "https://www.buffsnbrew.com/wp-content/uploads/fo76/storefront/",
    "/wp-content/uploads/fo76/storefront/bundles/",
    "/wp-content/uploads/fo76/storefront/",
]

DESC_STRIP_RE = re.compile(
    r"\s*-\s*(C\.A\.M\.P\. ITEMS APPEAR WHILE IN C\.A\.M\.P\. MODE|"
    r"APPAREL IS CRAFTABLE AT ARMOR WORKBENCHES|"
    r"APPAREL IS CRAFTABLE AT THE ARMOR WORKBENCH|"
    r"WEAPON SKINS ARE CRAFTABLE AT WEAPONS WORKBENCHES|"
    r"POWER ARMOR PAINT JOBS ARE CRAFTABLE AT POWER ARMOR STATIONS|"
    r"CAMP ITEMS APPEAR WHILE IN CAMP MODE"
    r")[^-]*-?\s*$",
    re.IGNORECASE,
)


def clean_desc(raw):
    s = raw.strip()
    for _ in range(5):
        new = DESC_STRIP_RE.sub("", s).strip()
        if new == s:
            break
        s = new
    s = re.sub(r"  +", " ", s)
    return s


def fix_image_url(url):
    if not url:
        return url
    if url.startswith(IMAGE_BASE_URL) and url.lower().endswith(".avif"):
        return url
    for old_base in OLD_IMAGE_BASES:
        if url.startswith(old_base):
            filename = url[len(old_base):].split("/")[-1]
            stem, _ = os.path.splitext(filename)
            return IMAGE_BASE_URL + stem + ".avif"
    bare = url.split("/")[-1]
    stem, _ = os.path.splitext(bare)
    return IMAGE_BASE_URL + stem + ".avif"


def fix_item_images(item):
    if item.get("imageUrl"):
        item["imageUrl"] = fix_image_url(item["imageUrl"])
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            if bi.get("imageUrl"):
                bi["imageUrl"] = fix_image_url(bi["imageUrl"])
    return item


def load_desc_lookup():
    pattern = os.path.join(TSV_ROOT, "ENTM_Export_*.tsv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[atom_shop] WARNING: No ENTM_Export_*.tsv found in {TSV_ROOT}", file=sys.stderr)
        return {}
    tsv_path = files[-1]
    print(f"[atom_shop] Reading DESC from: {os.path.basename(tsv_path)}")
    lookup = {}
    with open(tsv_path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = str(row.get("EDID") or "").strip().upper()
            desc = str(row.get("DESC") or "").strip()
            if edid and desc:
                lookup[edid] = clean_desc(desc)
    print(f"[atom_shop] DESC entries loaded: {len(lookup)}")
    return lookup


def apply_desc(item, lookup):
    edid = str(item.get("edid") or "").strip().upper()
    item["desc"] = lookup.get(edid, "")
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            bi_edid = str(bi.get("edid") or "").strip().upper()
            bi["desc"] = lookup.get(bi_edid, "")
    return item


def main():
    try:
        with open(SRC, "r", encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        print(f"[atom_shop] Cannot read {SRC}", file=sys.stderr)
        sys.exit(1)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[atom_shop] JSON parse error in {SRC}: {e}", file=sys.stderr)
        sys.exit(1)

    items = data.get("items", [])
    if not isinstance(items, list) or not items:
        print("[atom_shop] No items found in src/bundles.json", file=sys.stderr)
        sys.exit(1)

    errors = 0
    for i, item in enumerate(items):
        if not item.get("name"):
            print(f"[atom_shop] Item {i} missing name", file=sys.stderr)
            errors += 1
    if errors:
        print(f"[atom_shop] {errors} validation error(s). Aborting.", file=sys.stderr)
        sys.exit(1)

    desc_lookup = load_desc_lookup()

    fixed_count = 0
    fixed_items = []
    for item in items:
        original_url = item.get("imageUrl", "")
        fixed = fix_item_images(dict(item))
        fixed = apply_desc(fixed, desc_lookup)
        if fixed.get("imageUrl") != original_url:
            fixed_count += 1
        fixed_items.append(fixed)

    data["items"] = fixed_items

    if fixed_count:
        print(f"[atom_shop] Rewrote {fixed_count} image URL(s)")
    else:
        print(f"[atom_shop] All image URLs already correct")

    os.makedirs(os.path.dirname(DIST), exist_ok=True)
    with open(DIST, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"[atom_shop] OK — {len(fixed_items)} items written to dist/atom_shop.json")


if __name__ == "__main__":
    main()
