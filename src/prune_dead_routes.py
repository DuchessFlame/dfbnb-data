#!/usr/bin/env python3
"""
prune_dead_routes.py — take routes the game can never pay out OFF the plan pages.

WHY
---
build_plan_obtain_json.py lists every leveled list that holds a plan as a route.
It never asked whether anything in the game actually ROLLS that list, so a list
Bethesda retired without marking it cut still published at full strength:

    Plan: Wasteland Loveseat   Steel Reign - Minerva (Gold Bullion vendor)  100%
    Plan: Ghillie Suit         Steel Reign - Minerva (Gold Bullion vendor)  100%

Both come from BS02_SpecialVendor_Minerva_LLS_GoldVendor_Backlog_02, which has
had 0 references in every export since Feb 2026. Minerva's real stock is the 24
numbered lists her vendor list picks by BS02_SpecialVendor_InventoryIndex.

WHAT COUNTS AS DEAD
-------------------
Exactly what the Current Bugged Plans scanner proves, via
build_bugged_plans_json.route_dead_reasons(), so the two pages can never
disagree:
    cut list                       a zzz_/DEL_/Test list, or one only a cut list reaches
    parked list (cut, not marked)  an orphaned Backlog/TEMP holding pen
    reward pool not hooked up      an orphan whose own quest reward record is visible
                                   and hands out items without it (Tunnel of Love)
    entry can never roll           reached only through a First Match entry that
                                   can never win, or a literal 100% ChanceNone
A list with 0 links and nothing else is KEPT: the export can't see Papyrus, and
Wastelanders gold vendors, A Grand Reopening etc. pay out that way.

WHAT IT DOES TO A ROW
---------------------
Dead routes move from `obtain_routes` to `retired_routes` (with the reason) so
nothing is lost, then `obtain_ledger` is rebuilt (it indexes routes by
position) and the source pill follows on the next step. No rate is computed or
changed here (plan-obtain-pipeline: this pipeline owns no rate maths).

Runs inside build_plan_obtain_json.py after drop conditions and before source
tags. Standalone, over a finished file (seconds, no 80-minute rebuild):
    python src/prune_dead_routes.py dist/plan_master.json --data-dir tsv
    python src/prune_dead_routes.py dist/pts/plan_master.json --data-dir tsv/pts
Then re-run apply_source_pill.py, build_new_plans_json.py and
build_underarmour_json.py so the copied rows pick it up.
"""
import argparse, collections, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import plan_sources                                   # noqa: E402
import build_bugged_plans_json as bbp                 # noqa: E402

_SCAN = {}


def scan_for(tsv_dir):
    key = os.path.abspath(tsv_dir)
    if key not in _SCAN:
        s = bbp.Scan(tsv_dir)
        s.analyse_entries()
        s.analyse_reach()
        _SCAN[key] = s
    return _SCAN[key]


def attach(items, tsv_dir):
    scan = scan_for(tsv_dir)
    stats = collections.Counter()
    examples = []
    for it in items:
        routes = it.get("obtain_routes") or []
        if not routes:
            continue
        keep, gone = [], []
        for r in routes:
            why = bbp.route_dead_reasons(scan, r.get("lvli") or [])
            if why:
                gone.append(dict(r, retired_because=sorted(why)))
            else:
                keep.append(r)
        if not gone:
            continue
        it["obtain_routes"] = keep
        it["retired_routes"] = (it.get("retired_routes") or []) + gone
        it["obtain_ledger"] = plan_sources.obtain_ledger(it)
        stats["rows"] += 1
        stats["routes"] += len(gone)
        if not keep and not (it.get("obtain_unlocks") or []):
            stats["rows_left_with_nothing"] += 1
        for g in gone:
            for w in g["retired_because"]:
                stats["why: " + w] += 1
        if len(examples) < 8:
            examples.append(f"{it.get('name')}: -{', '.join(g.get('route') or '?' for g in gone)}")
    stats["_examples"] = examples
    return stats


def report(stats, stream=None):
    stream = stream or sys.stdout
    ex = stats.pop("_examples", [])
    print(f"  retired {stats.get('routes', 0)} dead route(s) on {stats.get('rows', 0)} plan(s); "
          f"{stats.get('rows_left_with_nothing', 0)} now have no source at all", file=stream)
    for k in sorted(k for k in stats if k.startswith("why: ")):
        print(f"    {k[5:]:32s} {stats[k]}", file=stream)
    for e in ex:
        print(f"    e.g. {e}", file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--data-dir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    a = ap.parse_args(argv)
    for path in a.files:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("items") or []
        print(f"[prune-dead-routes] {path}")
        report(attach(items, a.data_dir))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
