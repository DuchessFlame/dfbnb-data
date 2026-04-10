#!/usr/bin/env python3
"""
build_weapons_json.py
====================
STATUS: SCAFFOLDED — gap filler for the Weapons category.

NOTES FOR NEXT AI
─────────────────
1. This script should generate dist/weapons.json from xEdit TSV exports.

2. REFERENCE SCRIPTS:
   - build_armour_json.py — good template for a straightforward TSV→JSON build
   - build_titles_json.py — good template for complex multi-source builds
   - build_collectables_json.py — good template for categorized item builds

3. EXPECTED INPUT:
   TSV files in the tsv/ directory, exported from xEdit. Common columns:
   - Editor ID (EDID), Form ID, Full Name, Description
   - Category-specific columns TBD based on game data structure

4. EXPECTED OUTPUT:
   dist/weapons.json — consumed by the JS module df-bnb-weapons.js on the website

5. WORKFLOW:
   There is a matching GitHub Actions workflow:
   .github/workflows/build-weapons.yml
   that calls this script on push to tsv/ or src/ paths.

6. The master workflow (dfbnb-patch-build.yml) also calls this script.

Usage:
  python build_weapons_json.py
  python build_weapons_json.py --tsv-root tsv --outdir dist
"""

import json
import os
import sys

from patchlog_utils import write_empty_patchlog_feed

def main():
    print(f"[build_weapons_json.py] Gap filler — real build logic TBD")

    # Ensure output directory exists
    os.makedirs("dist", exist_ok=True)

    # Write empty/placeholder JSON
    out_path = os.path.join("dist", "weapons.json")
    with open(out_path, "w") as f:
        json.dump({"_status": "scaffolded", "_category": "Weapons", "items": []}, f, indent=2)

    print(f"[build_weapons_json.py] Wrote {out_path}")

    # Write empty patchlog feed
    write_empty_patchlog_feed("dist", "patchlog_latest_bnb_weapons.json", current_count=0)

if __name__ == "__main__":
    main()
