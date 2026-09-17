#!/usr/bin/env python3
r"""
apply_display_names.py — re-title plan rows in place, in seconds.

The same shape as add_recipe_unlocks.py and for the same reason: a full
plan_master build is ~80 minutes per channel and nothing here needs a rate. This
is pure string work over the finished document.

    python3 src/apply_display_names.py dist/plan_master.json
    python3 src/apply_display_names.py dist/pts/plan_master.json --tsv-dir tsv/pts

It writes `display_name` only. `name` -- the game's own plan name -- is never
touched, and the renderer prints it as "Plan name" in Technical wherever the two
differ. `--dry-run` prints what would change and writes nothing.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plan_display_names


def main(argv=None):
    ap = argparse.ArgumentParser(description="model-first titles for weapon paints")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--tsv-dir", default="tsv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[titles] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("items") or []
        print(f"[titles] {path}: {len(items)} rows")
        plan_display_names.report(plan_display_names.attach(items, args.tsv_dir))
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
