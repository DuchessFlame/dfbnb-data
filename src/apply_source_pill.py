#!/usr/bin/env python3
r"""
apply_source_pill.py — write `source_tag` onto a built plan_master, in seconds.

    python3 src/apply_source_pill.py dist/plan_master.json dist/pts/plan_master.json

Pure string work over routes and unlocks that are already resolved -- no TSV, no
rates, nothing from rng76. The full build calls the same module at its tail.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plan_source_pill


def main(argv=None):
    ap = argparse.ArgumentParser(description="source tag for each plan row")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    for path in args.paths:
        if not os.path.exists(path):
            print(f"[source] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("items") or []
        print(f"[source] {path}: {len(items)} rows")
        plan_source_pill.report(plan_source_pill.attach(items))
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
