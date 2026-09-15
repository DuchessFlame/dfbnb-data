#!/usr/bin/env python3
"""
plan_fishing_rod.py — fishing-rod equipment on the plan checklists.

Fishing rod gear was scattered across two plan_master buckets that both
misfiled it:

  * rods, bobbers and floats landed in `recipe`, so they sat on the Recipe Plan
    Checklist between the food recipes and the drowned furniture;
  * reels and rod upgrades landed in `weapon`, so they sat on the Weapon Plan
    Checklist alongside receivers and scopes.

Neither is a recipe or a weapon. They all belong on
/df/plan-checklists/fishing-rod/, which guide_index.tsv has carried since the
category was built. This module tags them so the renderer can pull them onto
that page and drop them from the other two.

The bucket itself is left alone on purpose. `type` is the progress-store key
for every row, so moving a plan between buckets would strand whatever ticks
people already have against it. The page a plan appears on is a display
decision; the bucket is storage.

Fields written onto each matching row:

    fishing_rod_role    "rod" | "bobber" | "reel" | "upgrade"
    fishing_rod_group   the group heading it renders under

WHAT COUNTS
-----------
The plan's own EditorID, which every one of these spells the same way:
`..._Recipe_mod_FishingRod_<kind>_<name>`. That is deliberately narrow. Fishing
bait, the fishing food recipes and the whole Drowned furniture set also carry a
`Fishing_` prefix, and they really are recipes — they stay where they are.

    Fishing_Recipe_mod_FishingRod_LineUpgrade_05    -> reel      (Mark 4 Reel)
    zzz_Fishing_Recipe_mod_FishingRod_RodBobber_*   -> bobber
    zzz_Fishing_Workshop_Recipe_CookingStove_*      -> not ours, it is a CAMP plan
    Fishing_Recipe_Food_FishChowder                 -> not ours, it is food

Usage:
    python3 src/plan_fishing_rod.py --report-only src/plan-system/plan_master.json
    python3 src/plan_fishing_rod.py src/plan-system/plan_master.json dist/plan_master.json
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

SCHEMA = 1

# The plan record for a piece of fishing rod gear. The kind sits directly after
# FishingRod_, and LineUpgrade has to be tested before the bare Upgrade or every
# reel reads as a rod upgrade.
# NB: the trailing separator is an underscore, and `_` is a word character, so
# `\b` never fires after "RodBase01" or "Upgrade" — match the underscore itself.
_PLAN = re.compile(r"Recipe_mod_FishingRod_(LineUpgrade|RodBase\d*|RodBobber|Upgrade)_", re.I)

_ROLE = {
    "lineupgrade": "reel",
    "rodbase":     "rod",
    "rodbobber":   "bobber",
    "upgrade":     "upgrade",
}

# Render order on the page: the rod first, then what you hang off it, then the
# two kinds of upgrade you fit to it.
GROUPS = [
    ("rod",     "Rods",              "The rod itself — the skin you cast with."),
    ("bobber",  "Bobbers & Floats",  "What sits on the water. Cosmetic: the bobber does not change what you catch."),
    ("reel",    "Reels",             "Line upgrades, Mark 1 through Mark 4. Each tier raises the line strength you can land a fish with."),
    ("upgrade", "Rod Upgrades",      "The one-off rod mods — drag, handle, bearing, hook and gear ratio."),
]

GROUP_LABEL = {key: label for key, label, _ in GROUPS}


def role(plan_edid):
    """The fishing-rod role this plan EditorID carries, or None."""
    m = _PLAN.search(plan_edid or "")
    if not m:
        return None
    kind = m.group(1).lower()
    if kind.startswith("rodbase"):
        kind = "rodbase"
    return _ROLE.get(kind)


def attach(items):
    """Tag every fishing-rod equipment plan in a plan_master item list."""
    tally = collections.Counter()
    for it in items:
        r = role((it.get("plan_item") or {}).get("edid", ""))
        if not r:
            continue
        it["fishing_rod_role"] = r
        it["fishing_rod_group"] = GROUP_LABEL[r]
        tally[r] += 1
        if it.get("cut"):
            tally["cut"] += 1
    return dict(tally) if tally else {}


def report(stats, where=""):
    if not stats:
        print(f"[plan_fishing_rod] {where}no fishing rod plans found")
        return
    total = sum(v for k, v in stats.items() if k != "cut")
    print(f"[plan_fishing_rod] {where}{total} fishing rod plans tagged"
          + (f" ({stats['cut']} cut)" if stats.get("cut") else ""))
    print("  " + ", ".join(f"{k}={v}" for k, v in sorted(stats.items()) if k != "cut"))


def enrich(path, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [])
    if not stats:
        return doc, {}
    doc["fishing_rod_schema"] = SCHEMA
    if not report_only:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
    return doc, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="plan_master.json file(s) to enrich in place")
    ap.add_argument("--report-only", action="store_true", help="resolve and print, write nothing")
    args = ap.parse_args()

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[plan_fishing_rod] missing: {path}", file=sys.stderr)
            continue
        doc, stats = enrich(path, args.report_only)
        report(stats, f"{path}: ")
        if args.report_only and stats:
            rows = [i for i in doc["items"] if i.get("fishing_rod_role")]
            for key, label, _ in GROUPS:
                mine = [i for i in rows if i["fishing_rod_role"] == key]
                print(f"    {label} ({len(mine)})")
                for i in sorted(mine, key=lambda x: x["name"]):
                    print(f"      {'CUT ' if i.get('cut') else '    '}{i['name']}  [{i['type']}]")


if __name__ == "__main__":
    main()
