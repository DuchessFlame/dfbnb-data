#!/usr/bin/env python3
"""Contract check for the CAMP-items JSON family.

Every /bnb/camp-items/ and /df/camp/ page is rendered by ONE module
(df-bnb-camp-items.js) with ONE stylesheet, so all eleven JSONs have to agree
on a shape. Before the Aug 2026 unification they didn't: collectrons and the
resource producers carried build info, images and a station block while the
buff stations, allies, pets, fridges and cryos quietly shipped without them,
and the only symptom was a page that looked thinner than the others.

This script fails the build when that starts happening again. It checks the
things the renderer actually reads, and nothing else:

  * the file parses and has a non-empty items array
  * every item has an id (formId) and a displayName
  * per-page REQUIRED_FIELDS are present and non-empty on every item
  * per-page EXPECTED_COVERAGE fields are present on at least N% of items —
    a soft floor that catches "the join broke and 90% lost their images"
    without failing on the handful of records Bethesda genuinely leaves blank

Run with --dist dist (live) or --dist dist/pts (PTS preview).
Exit code 1 on any violation, with every problem printed — not just the first.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Per-page contract
# ---------------------------------------------------------------------------
# required:  must be present and truthy on EVERY item, or the page breaks.
# coverage:  {field: minimum fraction of items that must carry it}. These are
#            deliberately loose — they exist to catch a broken join, not to
#            demand data Bethesda has not shipped.

CONTRACT = {
    "buff-stations.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buffTypes"],
        "coverage": {"imageUrl": 0.90, "buildInfo": 0.80, "outputInfo": 0.90},
        "groups":   True,   # root expand per buff type comes from data.groups
    },
    "allies.json": {
        "required": ["formId", "displayName"],
        "coverage": {"buildInfo": 0.90, "buffsAndEffects": 0.50},
    },
    "collectrons.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buildInfo"],
        "coverage": {"production": 0.90, "station": 0.90, "craftingRequirements": 0.80},
    },
    "resource_producers.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buildInfo", "bucket"],
        "coverage": {"production": 0.90, "station": 0.90},
    },
    "repair-bots.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buildInfo"],
        "coverage": {"imageUrl": 0.90, "craftingRequirements": 0.90},
    },
    "weather_stations.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buildInfo"],
        "coverage": {"imageUrl": 0.90, "craftingRequirements": 0.90},
    },
    "cryos.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buildInfo"],
        "coverage": {"imageUrl": 0.90, "spoilageReduction": 0.90},
    },
    "fridges.json": {
        "required": ["formId", "displayName", "obtainRoutes", "buildInfo"],
        "coverage": {"imageUrl": 0.90, "spoilageReduction": 0.90},
    },
    "pets.json": {
        "required": ["formId", "displayName", "animalType", "buildInfo"],
        "coverage": {"imageUrl": 0.90},
    },
    "pet-furniture.json": {
        "required": ["formId", "displayName", "animalType"],
        "coverage": {"imageUrl": 0.90},
    },
    "pet-apparel.json": {
        "required": ["formId", "displayName", "animalType"],
        "coverage": {"imageUrl": 0.90, "craftingRequirements": 0.80},
    },
}


def check_file(dist: Path, name: str, spec: dict) -> list[str]:
    problems: list[str] = []
    path = dist / name

    if not path.exists():
        return [f"{name}: MISSING"]

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:                      # noqa: BLE001 - report, don't raise
        return [f"{name}: not valid JSON — {exc}"]

    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, list) or not items:
        return [f"{name}: no items array (or it is empty)"]

    if spec.get("groups") and not (isinstance(data, dict) and data.get("groups")):
        problems.append(f"{name}: missing the 'groups' array the root expands are built from")

    for field in spec.get("required", []):
        missing = [
            (it.get("formId") or it.get("edid") or "?")
            for it in items
            if not it.get(field)
        ]
        if missing:
            shown = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
            problems.append(
                f"{name}: {len(missing)}/{len(items)} items missing required '{field}' ({shown})"
            )

    for field, floor in spec.get("coverage", {}).items():
        have = sum(1 for it in items if it.get(field))
        frac = have / len(items)
        if frac < floor:
            problems.append(
                f"{name}: '{field}' present on {have}/{len(items)} "
                f"({frac:.0%}) — below the {floor:.0%} floor"
            )

    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dist", default="dist", help="Folder holding the built JSON")
    ap.add_argument(
        "--allow-missing",
        action="store_true",
        help="Skip files that aren't present (PTS runs that build a subset)",
    )
    args = ap.parse_args()

    dist = Path(args.dist)
    all_problems: list[str] = []
    checked = 0

    for name, spec in CONTRACT.items():
        if args.allow_missing and not (dist / name).exists():
            print(f"  SKIP {name} (not built)")
            continue
        found = check_file(dist, name, spec)
        checked += 1
        if found:
            all_problems.extend(found)
        else:
            print(f"  OK   {name}")

    if all_problems:
        print(f"\nCamp-items JSON contract FAILED ({len(all_problems)} problem(s)):",
              file=sys.stderr)
        for p in all_problems:
            print(f"  - {p}", file=sys.stderr)
        return 1

    print(f"\nCamp-items JSON contract OK ({checked} file(s) checked in {dist}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
