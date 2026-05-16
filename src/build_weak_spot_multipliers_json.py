"""
build_weak_spot_multipliers_json.py
-----------------------------------
Generates dist/calculators/weak_spot_multipliers.json from the TSV source.

Expected TSV format (tab-separated, with header row):
  Enemy\tLimb\tMultiplier

If the TSV doesn't exist yet, falls back to the manually-curated JSON
already in dist/calculators/weak_spot_multipliers.json (no-op).

Usage:
  python src/build_weak_spot_multipliers_json.py
"""

import json
import os
import csv
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV_PATH = os.path.join(ROOT, "tsv", "BPTD_WeakSpot_Export.tsv")
OUT_PATH = os.path.join(ROOT, "dist", "calculators", "weak_spot_multipliers.json")


def build_from_tsv():
    """Parse the TSV and group by enemy name."""
    enemies = {}

    with open(TSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            enemy = row.get("Enemy", "").strip()
            limb = row.get("Limb", "").strip()
            mult_raw = row.get("Multiplier", "1.00").strip()

            if not enemy or not limb:
                continue

            try:
                mult = float(mult_raw)
            except ValueError:
                mult = 1.00

            if enemy not in enemies:
                enemies[enemy] = []

            enemies[enemy].append({
                "limb": limb,
                "multiplier": round(mult, 2)
            })

    # Sort enemies alphabetically, sort limbs by multiplier desc then name
    sorted_enemies = {}
    for name in sorted(enemies.keys()):
        parts = enemies[name]
        parts.sort(key=lambda p: (-p["multiplier"], p["limb"]))
        sorted_enemies[name] = parts

    return sorted_enemies


def main():
    if os.path.isfile(TSV_PATH):
        print(f"[weak_spot] Building from TSV: {TSV_PATH}")
        enemies = build_from_tsv()
    else:
        # No TSV yet — check if output already exists (manual curation)
        if os.path.isfile(OUT_PATH):
            print(f"[weak_spot] No TSV found. Existing JSON preserved: {OUT_PATH}")
            return
        else:
            print(f"[weak_spot] ERROR: No TSV at {TSV_PATH} and no existing JSON.")
            return

    output = {
        "generated": str(date.today()),
        "enemies": enemies
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[weak_spot] Written {len(enemies)} enemies -> {OUT_PATH}")


if __name__ == "__main__":
    main()
