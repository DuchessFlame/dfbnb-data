#!/usr/bin/env python3
"""
src/build_atom_shop_json.py
============================
Reads src/bundles.json, fixes image URLs, validates, and writes dist/atom_shop.json.

NOTE: build_bundles.py is retired — this script absorbs its job.

IMAGE URL REWRITE
─────────────────
All images are AVIF. Old paths (fo76/storefront/) are rewritten to the
correct WP Engine upload path. Any leftover .webp extensions are also
corrected to .avif.

Old path (dead): /wp-content/uploads/fo76/storefront/
New path:        /wp-content/uploads/guide-images/atom-shop/request-item-images/

Usage:
  python build_atom_shop_json.py
  python build_atom_shop_json.py --tsv-root tsv --outdir dist
"""

import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC  = os.path.join(SCRIPT_DIR, "bundles.json")
DIST = os.path.join(SCRIPT_DIR, "..", "dist", "atom_shop.json")

# ── Image URL config ─────────────────────────────────────────────────────────
# Correct WP Engine path — matches where sync_bundles_to_site.ps1 uploads to
IMAGE_BASE_URL = "https://buffsnbrew.com/wp-content/uploads/guide-images/atom-shop/request-item-images/"

# All old/dead base paths to rewrite
OLD_IMAGE_BASES = [
    "https://buffsnbrew.com/wp-content/uploads/fo76/storefront/bundles/",
    "https://buffsnbrew.com/wp-content/uploads/fo76/storefront/",
    "/wp-content/uploads/fo76/storefront/bundles/",
    "/wp-content/uploads/fo76/storefront/",
]
# ─────────────────────────────────────────────────────────────────────────────


def fix_image_url(url: str) -> str:
    """
    Rewrite any old image base path to the correct current one.
    Also corrects any leftover .webp extensions to .avif.
    """
    if not url:
        return url

    for old_base in OLD_IMAGE_BASES:
        if url.startswith(old_base):
            filename = url[len(old_base):]
            # Strip any accidental sub-path segments — only keep the bare filename
            filename = filename.split("/")[-1]
            # Force .avif extension regardless of what was there before
            stem, _ = os.path.splitext(filename)
            return IMAGE_BASE_URL + stem + ".avif"

    # URL doesn't match any old base — still ensure .avif extension
    stem, ext = os.path.splitext(url)
    if ext.lower() != ".avif":
        return stem + ".avif"

    return url


def fix_item_images(item: dict) -> dict:
    """Recursively fix imageUrl in an item and its bundleItems."""
    if item.get("imageUrl"):
        item["imageUrl"] = fix_image_url(item["imageUrl"])
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            if bi.get("imageUrl"):
                bi["imageUrl"] = fix_image_url(bi["imageUrl"])
    return item


def main():
    # ── Load ─────────────────────────────────────────────────────────────────
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

    # ── Validate ──────────────────────────────────────────────────────────────
    items = data.get("items", [])
    if not isinstance(items, list) or not items:
        print("[atom_shop] No items found in src/bundles.json", file=sys.stderr)
        sys.exit(1)

    errors = 0
    for i, item in enumerate(items):
        if not item.get("name"):
            print(f"[atom_shop] Item {i} missing name", file=sys.stderr)
            errors += 1
        if item.get("isBundle") and not item.get("bundleItems"):
            print(f'[atom_shop] Bundle "{item.get("name", "?")}" has no bundleItems', file=sys.stderr)

    if errors:
        print(f"[atom_shop] {errors} validation error(s). Aborting.", file=sys.stderr)
        sys.exit(1)

    # ── Fix image URLs ────────────────────────────────────────────────────────
    fixed_count = 0
    fixed_items = []
    for item in items:
        original_url = item.get("imageUrl", "")
        fixed = fix_item_images(dict(item))
        if fixed.get("imageUrl") != original_url:
            fixed_count += 1
        fixed_items.append(fixed)

    data["items"] = fixed_items

    if fixed_count:
        print(f"[atom_shop] Rewrote {fixed_count} image URL(s) → {IMAGE_BASE_URL}")
    else:
        print(f"[atom_shop] All image URLs already correct — no rewrites needed")

    # ── Write dist ────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(DIST), exist_ok=True)
    with open(DIST, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"[atom_shop] OK — {len(fixed_items)} items written to dist/atom_shop.json")


if __name__ == "__main__":
    main()
