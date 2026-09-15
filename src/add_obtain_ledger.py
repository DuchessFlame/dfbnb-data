#!/usr/bin/env python3
r"""
add_obtain_ledger.py — put `obtain_ledger` on the rows of an already-built
plan_master.json (and anything that copied rows out of one).

Why this exists: build_plan_obtain_json.py emits the ledger itself now, but a
full plan_master build is ~80 minutes per channel and the ledger resolves
NOTHING new — it is a pure re-sort of obtain_routes + obtain_unlocks, both of
which are already in the file. Re-running rng76 to add a derived field would be
80 minutes spent to arrive at the same percentages.

So this reads the JSON, calls the same plan_sources.obtain_ledger() the builder
calls, and writes the file back. Idempotent: run it twice and the second run
changes nothing. After the next full build it is redundant, which is fine — it
still produces byte-identical rows.

    python3 src/add_obtain_ledger.py dist/plan_master.json dist/pts/plan_master.json

The pages that copy rows verbatim out of plan_master (underarmour.json,
new_plans.json) can be patched with the same call, and that is preferable to
re-running their builders by hand: on this machine there is no tsv_live
snapshot, so a local PTS rebuild would compare the PTS export against itself
and silently blank every NEW pill. The ledger is a pure function of fields
those rows already carry, so patching lands on the identical bytes the next CI
rebuild will produce.
"""
import argparse, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plan_sources


def walk_items(blob):
    """Yield every plan row in a dist JSON, flat list or grouped."""
    if isinstance(blob, dict):
        for it in blob.get("items") or []:
            yield it
        for g in blob.get("groups") or []:
            for it in (g.get("items") or []):
                yield it
    elif isinstance(blob, list):
        for it in blob:
            yield it


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="dist JSON files to patch in place")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[ledger] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            blob = json.load(f)

        n = changed = 0
        for it in walk_items(blob):
            if not isinstance(it, dict) or "obtain_routes" not in it:
                continue
            n += 1
            led = plan_sources.obtain_ledger(it)
            if it.get("obtain_ledger") != led:
                it["obtain_ledger"] = led
                changed += 1

        live = sum(1 for it in walk_items(blob)
                   if isinstance(it, dict) and (it.get("obtain_ledger") or []))
        print(f"[ledger] {path}: {n} rows, {changed} updated, "
              f"{live} with at least one route that applies")
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
