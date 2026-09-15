#!/usr/bin/env python3
r"""
add_plan_images.py — resolve art for the plan checklist rows and write it into
the dist JSONs.

Same shape as add_obtain_ledger.py: build_plan_obtain_json.py does this itself
on a full build, and this script does it to an already-built file so a change
to the art — a newly staged .avif, a page that just got its own pictures — is
live without an 80-minute rng76 rebuild. Nothing here resolves a route or a
rate; it only decides which picture each row points at.

    # after dropping new .avif files into the staging folders
    python3 src/add_plan_images.py --avif-dir "<...>/.Plan Checklist" \
        dist/plan_master.json dist/underarmour.json dist/new_plans.json

--avif-dir scans the staging root and rewrites data/plan_images.json, which is
committed so CI resolves the same stems. Without it the committed list is used.

underarmour.json is passed --folder underarmour: its rows are copied out of
plan_master and still carry the apparel/armour bucket, but the page has its own
folder on the server.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plan_images


def walk_items(blob):
    """Every plan row in a dist JSON, flat list or grouped."""
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
    ap.add_argument("--avif-dir", default="",
                    help="staging root mirroring the server folders; scans it "
                         "and rewrites data/plan_images.json")
    ap.add_argument("--dist-dir", default="dist")
    ap.add_argument("--tsv-dir", default="tsv")
    ap.add_argument("--config", default=plan_images.CONFIG)
    ap.add_argument("--folder", default="",
                    help="force every row onto one page folder (underarmour)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    if args.avif_dir:
        plan_images.scan_staging(args.avif_dir, args.config)

    idx, staged = plan_images.load(args.dist_dir, args.tsv_dir, args.config)

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[images] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            blob = json.load(f)

        rows = [it for it in walk_items(blob)
                if isinstance(it, dict) and "plan_item" in it]
        folder = args.folder
        if not folder and os.path.basename(path).startswith("underarmour"):
            folder = "underarmour"
        stats = plan_images.attach(rows, idx, staged, folder_override=folder)

        print(f"[images] {path}: {len(rows)} rows")
        plan_images.report(stats, stream=sys.stdout)
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
