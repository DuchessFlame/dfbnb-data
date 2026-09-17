#!/usr/bin/env python3
r"""
add_drop_conditions.py — attach "Drop Conditions" to a built plan_master.

    python3 src/add_drop_conditions.py dist/plan_master.json
    python3 src/add_drop_conditions.py dist/pts/plan_master.json --tsv-dir tsv/pts

A full plan_master build is ~80 minutes per channel and none of this needs a
rate: conditions are a plain read of the leveled-list entries. This re-derives
which lists produced each route -- by calling `resolve_routes(..., names_only=
True)`, the same function that named them in the first place, with the rng76
resolve switched off -- and then writes `lvli` and `conditions` onto each route.

Run it after add_cobj_link.py, the same as the other post-steps: the own-plan
tests ("stops dropping once you learn it") read the row's linked recipe.
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_plan_obtain_json as bpo
import plan_conditions

import build_farming_used_for as bfu
import plan_sources


def main(argv=None):
    ap = argparse.ArgumentParser(description="drop conditions for plan routes")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--tsv-dir", default="tsv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    tsv = os.path.abspath(args.tsv_dir)
    bpo.TSV = tsv
    t0 = time.time()
    tables = bpo.ssrc.load_tables(tsv)
    cont_names = bfu._load_cont_names(tsv)
    npc_names = {}
    npcf = bpo.newest("NPC_Export_*.tsv", tsv)
    if npcf:
        for r in bpo.read_rows(npcf):
            fid = (r.get("FormID") or "").strip().upper()
            full = (r.get("FULL") or "").strip()
            if fid and full and fid not in npc_names:
                npc_names[fid] = full
    # source_label() reads QUEST_NAMES on every route name, so the index has to
    # exist before the first names_only call -- same order as the builder.
    unlock_idx = plan_sources.UnlockIndex(tsv, lambda pat, root: bpo.newest(pat, root))
    bpo.QUEST_NAMES = unlock_idx.quest_names
    index = plan_conditions.ConditionIndex(tsv, bpo.newest)
    print(f"[conditions] tables loaded in {time.time()-t0:.0f}s "
          f"({len(index.entries)} lists, {index.export})")

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[conditions] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("items") or []
        print(f"[conditions] {path}: {len(items)} rows")
        t1 = time.time()
        for n, it in enumerate(items, 1):
            book = ((it.get("plan_item") or {}).get("formid") or "").upper()
            routes = it.get("obtain_routes") or []
            if not book or not routes:
                continue
            if all(r.get("lvli") for r in routes):
                continue                      # already carries them
            by_name = bpo.resolve_routes(book, tables, None, cont_names,
                                         npc_names, names_only=True)
            for r in routes:
                r["lvli"] = sorted(set(by_name.get(r.get("route"), ())))
            if n % 400 == 0:
                print(f"   ... {n}/{len(items)}  ({time.time()-t1:.0f}s)")
        plan_conditions.report(plan_conditions.attach(items, tsv, bpo.newest,
                                                      index=index))
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
