#!/usr/bin/env python3
"""Build bundles.json by extracting bundle data from atom_shop.json.

Generates the JSON consumed by df-bnb-bundles.js (the "Items Available to
Request" page).  Must run AFTER build_atom_shop_json.py, which enriches
atom_shop.json from the ENTM TSV export.

Bundle composition (which items belong to which bundle) is curated in
atom_shop.json — the ENTM TSV's ECIL columns hold image references,
not child-item links, so we cannot derive composition from TSVs alone.

Usage (local):   python src/build_bundles_json.py
Usage (CI):      python src/build_bundles_json.py --out-dir dist
"""

import argparse
import json
from datetime import date
from pathlib import Path

# ── CLI ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument(
    "--out-dir", default="dist",
    help="Directory containing atom_shop.json and where bundles.json is written",
)
args = parser.parse_args()

DIST_DIR = Path(args.out_dir)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_child(item):
    """Normalise a bundle child item to the renderer's expected shape."""
    return {
        "name":           item.get("name", ""),
        "edid":           item.get("edid", ""),
        "formId":         item.get("formId", item.get("form_id", "")),
        "imageUrl":       item.get("imageUrl", item.get("image", "")),
        "technicalNotes": item.get("technicalNotes", ""),
    }


def _extract_bundle(item):
    """Normalise an Atom-Shop bundle item."""
    return {
        "name":           item.get("name", item.get("displayName", "")),
        "edid":           item.get("edid", ""),
        "formId":         item.get("formId", item.get("form_id", "")),
        "imageUrl":       item.get("imageUrl", item.get("image", "")),
        "isBundle":       True,
        "technicalNotes": item.get("technicalNotes", ""),
        "bundleItems":    [_extract_child(bi)
                           for bi in item.get("bundleItems", [])],
    }


def _extract_ltb(ltb):
    """Normalise an LTB (Limited Time Bundle) entry.

    LTB bundles use ``items`` (not ``bundleItems``) as the child key and
    carry extra metadata (released date, update name, platforms).
    """
    return {
        "name":           ltb.get("name", ""),
        "edid":           ltb.get("id", ""),
        "formId":         "",
        "imageUrl":       ltb.get("imageUrl", ""),
        "isBundle":       True,
        "technicalNotes": "",
        "ltb":            True,
        "released":       ltb.get("released", ""),
        "update":         ltb.get("update", ""),
        "platforms":      ltb.get("platforms", []),
        "bundleItems":    [_extract_child(bi)
                           for bi in ltb.get("items", [])],
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    atom_path = DIST_DIR / "atom_shop.json"
    if not atom_path.exists():
        print(f"ERROR: {atom_path} not found — "
              f"run build_atom_shop_json.py first.")
        return

    with open(atom_path, encoding="utf-8") as fh:
        data = json.load(fh)

    bundles = []

    # 1. Atom Shop bundles  (items with isBundle or bundleItems)
    for item in data.get("items", []):
        if item.get("isBundle") or item.get("bundleItems"):
            bundles.append(_extract_bundle(item))

    atom_count = len(bundles)

    # 2. LTB (Limited Time Bundles)
    for ltb in data.get("ltb", []):
        bundles.append(_extract_ltb(ltb))

    ltb_count = len(bundles) - atom_count

    # --- Write JSON ---
    output = {
        "_meta": {
            "generated":   date.today().isoformat(),
            "source":      "atom_shop.json",
            "description": ("Bundle items extracted from atom_shop.json "
                            "for the Items Available to Request page"),
            "totalItems":  len(bundles),
            "atomShop":    atom_count,
            "ltb":         ltb_count,
        },
        "items": bundles,
    }

    out_path = DIST_DIR / "bundles.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, separators=(",", ":"), ensure_ascii=False)

    print(f"Wrote {len(bundles)} bundles to {out_path} "
          f"({atom_count} Atom Shop + {ltb_count} LTB)")


if __name__ == "__main__":
    main()
