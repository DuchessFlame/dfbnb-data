#!/usr/bin/env python3
"""
build_cryptids_json.py
====================
STATUS: SCAFFOLDED — gap filler for the Cryptids category.

NOTES FOR NEXT AI
─────────────────
1. This script should generate dist/cryptids.json from xEdit TSV exports.

2. REFERENCE SCRIPTS:
   - build_armour_json.py — good template for a straightforward TSV→JSON build
   - build_titles_json.py — good template for complex multi-source builds
   - build_collectables_json.py — good template for categorized item builds

3. EXPECTED INPUT:
   TSV files in the tsv/ directory, exported from xEdit. Common columns:
   - Editor ID (EDID), Form ID, Full Name, Description
   - Category-specific columns TBD based on game data structure

4. EXPECTED OUTPUT:
   dist/cryptids.json — consumed by the JS module df-bnb-cryptids.js on the website

5. WORKFLOW:
   There is a matching GitHub Actions workflow:
   .github/workflows/build-cryptids.yml
   that calls this script on push to tsv/ or src/ paths.

6. The master workflow (dfbnb-patch-build.yml) also calls this script.

Usage:
  python build_cryptids_json.py
  python build_cryptids_json.py --tsv-root tsv --outdir dist
"""

import json
import os
import sys

from patchlog_utils import write_empty_patchlog_feed

def main():
    print(f"[build_cryptids_json.py] Gap filler — real build logic TBD")

    # Ensure output directory exists
    os.makedirs("dist", exist_ok=True)

    # Write empty/placeholder JSON
    out_path = os.path.join("dist", "cryptids.json")
    with open(out_path, "w") as f:
        json.dump({"_status": "scaffolded", "_category": "Cryptids", "items": []}, f, indent=2)

    print(f"[build_cryptids_json.py] Wrote {out_path}")

    # Write empty patchlog feed
    write_empty_patchlog_feed("dist", "patchlog_latest_df_cryptids.json", current_count=0)

if __name__ == "__main__":
    main()
