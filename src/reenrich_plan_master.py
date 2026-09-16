#!/usr/bin/env python3
"""
reenrich_plan_master.py — re-run every plan_master post-pass, in order, over a
finished file. One command per channel, so the three copies cannot drift.

WHY THIS EXISTS
---------------
There are THREE copies of plan_master and they had drifted:

    src/plan-system/plan_master.json   the source of truth
    dist/plan_master.json              what the live pages fetch
    dist/pts/plan_master.json          what the PTS pages fetch

Every enricher has to run against all of them, and none of them needs the
80-minute rebuild to get there: the enrichers are pure joins against exports
plan_master already lists. Before this script existed they were run by hand,
one file at a time, and the PTS copy ended up with NO `plan_subpages` block and
zero `plan_page` rows — so on PTS the Snow Globes, Fishing Rod and Camera Mod
pages rendered empty and the rows they should have carved out were still
double-rendering on the PTS Recipe and Weapon pages. Nothing errored. That is
the failure mode this script exists to make impossible.

ORDER IS LOAD-BEARING
---------------------
    1. plan_images       writes `image_dir`   — the classification everything routes on
    2. plan_subpages     writes `plan_page`   — reads image_dir
    3. plan_consumables  writes `consumable_*`— reads plan_page ("recipe")
    4. add_weapon_groups writes `weapon_*`    — reads plan_page ("weapon")
    5. add_armour_groups writes `armour_*`    — reads plan_page (both armour pages)

Run out of order and a page silently empties. Each step is also written to
PRUNE: a row that has moved pages loses the fields of the page it left, so
re-running over an already-enriched file converges rather than accumulating.

CHANNELS
--------
The PTS copy is built from tsv/pts and carries ~70 plans the live one does not,
so it must be enriched from the PTS exports — not from tsv/. --channel picks
both the file and the export root together, which is the whole point: they are
never separately chosen and so never separately wrong.

Usage:
    python3 src/reenrich_plan_master.py --all
    python3 src/reenrich_plan_master.py --channel live
    python3 src/reenrich_plan_master.py --channel pts --report-only
    python3 src/reenrich_plan_master.py --all --verify
"""

from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import plan_images
import plan_subpages
import plan_consumables
import add_weapon_groups
import add_armour_groups

# channel -> (plan_master paths, dist dir for published-art lookup, tsv root)
CHANNELS = {
    "live": (["src/plan-system/plan_master.json", "dist/plan_master.json"],
             "dist", "tsv"),
    "pts":  (["dist/pts/plan_master.json"],
             "dist/pts", "tsv/pts"),
}

# The top-level keys the enrichers own. Checked across copies by --verify, and
# they are also the keys normalizeMasterJSON() in the renderer has to name —
# that function strips every top-level key it does not, which is how
# `plan_subpages` was silently dropped once already.
SCHEMA_KEYS = ("plan_subpages_schema", "plan_subpages",
               "consumables_schema", "consumable_groups",
               "weapon_groups_schema", "weapon_groups_sources",
               "armour_groups_schema", "armour_groups_sources")


def enrich_file(path, dist_dir, tsv_dir, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    items = doc.get("items") or []
    print(f"\n[reenrich] {path}  ({len(items)} plans, exports from {tsv_dir})")

    add_weapon_groups.set_tsv_dir(os.path.join(ROOT, tsv_dir))
    plan_consumables.set_tsv_dir(os.path.join(ROOT, tsv_dir))

    # 1. art + classification
    idx, staged = plan_images.load(dist_dir, tsv_dir, verbose=False)
    plan_images.report(plan_images.attach(items, idx, staged))

    # 2. which page each plan renders on
    plan_subpages.report(plan_subpages.attach(items))
    doc["plan_subpages_schema"] = plan_subpages.SCHEMA
    doc["plan_subpages"] = plan_subpages.config()
    lost = plan_subpages.homeless(items)
    if lost:
        print(f"  *** {len(lost)} LIVE ROWS ON NO PAGE ***")
        for i in lost[:20]:
            print(f"      {i['name']}  [{i.get('image_dir')}]")

    # 3. Recipe page grouping
    cstats = plan_consumables.attach(items)
    plan_consumables.report(cstats)
    if cstats:
        doc["consumables_schema"] = plan_consumables.SCHEMA
        doc["consumable_groups"] = plan_consumables.config()

    # 4. Weapon page grouping
    wstats = add_weapon_groups.attach(items)
    add_weapon_groups.report(wstats)
    if wstats:
        doc["weapon_groups_schema"] = add_weapon_groups.SCHEMA
        doc["weapon_groups_sources"] = wstats["sources"]

    # 5. Body / Power Armour page grouping
    astats = add_armour_groups.attach(items)
    add_armour_groups.report(astats)
    if astats:
        doc["armour_groups_schema"] = add_armour_groups.SCHEMA
        doc["armour_groups_sources"] = astats["sources"]

    if not report_only:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
        print(f"  wrote {path}")
    return doc


def verify(docs):
    """Every copy carries the same schema keys and a page for every live row."""
    ok = True
    print("\n[reenrich] verify")
    for path, doc in docs.items():
        missing = [k for k in SCHEMA_KEYS if k not in doc]
        live = [i for i in doc["items"] if not i.get("cut")]
        paged = [i for i in live if i.get("plan_page")]
        # A row a page deliberately skips is not homeless — the skip names the
        # page that does show it (the snow globe display cases are furniture and
        # render on Displays, which has its own dataset).
        lost = plan_subpages.homeless(live)
        counts = {}
        for i in paged:
            counts[i["plan_page"]] = counts.get(i["plan_page"], 0) + 1
        print(f"  {path}")
        print(f"    live {len(live)}  on a page {len(paged)}"
              f"  skipped {len(live) - len(paged) - len(lost)}  homeless {len(lost)}")
        print(f"    pages: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        if missing:
            ok = False
            print(f"    *** MISSING SCHEMA KEYS: {', '.join(missing)}")
        # A plan can carry one page and one only — plan_page is a single value,
        # so the real risk is a row with none. Say so loudly.
        if lost:
            ok = False
            print(f"    *** {len(lost)} live rows render nowhere")
            for i in lost[:20]:
                print(f"        {i['name']} [{i.get('image_dir')}]")
    print("  OK" if ok else "  *** VERIFY FAILED ***")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", choices=sorted(CHANNELS), default="")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if not args.channel and not args.all:
        ap.error("pass --channel live|pts or --all")

    os.chdir(ROOT)
    todo = sorted(CHANNELS) if args.all else [args.channel]
    docs = {}
    for ch in todo:
        paths, dist_dir, tsv_dir = CHANNELS[ch]
        for path in paths:
            if not os.path.exists(path):
                print(f"[reenrich] missing: {path}", file=sys.stderr)
                continue
            docs[path] = enrich_file(path, dist_dir, tsv_dir, args.report_only)
    if args.verify or args.all:
        if not verify(docs):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
