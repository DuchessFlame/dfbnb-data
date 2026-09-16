#!/usr/bin/env python3
r"""
add_collectable_challenges.py — stamp the `challenges` block onto committed dist docs.

The join itself lives in collectable_challenges.py and runs inside
build_collectable_spawns_json.build_set(), so a full rebuild already emits the key.
This post-step exists for the same reason split_chance_spawns.py does: the full
rebuild needs the Mappalachia DB (or a geo cache in step with it) and rewrites the
whole document, including the hand-authored photos and directions. Adding one
derived key to what is already published should not require any of that.

Idempotent. Always writes the key — a set with no matching challenges gets an
empty block, so the renderer draws nothing rather than a stale block from a
previous run.

    python src/add_collectable_challenges.py                      # dist/, live exports
    python src/add_collectable_challenges.py --channel pts --dist-dir dist/pts
    python src/add_collectable_challenges.py --set pint-sized-slasher-masks --dry-run

Writes with indent=2, matching build_collectable_spawns_json.py's dist writer.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import collectable_challenges as cchal  # noqa: E402

EMPTY = {"heading": "", "items": []}


# collectable_spawns_manifest.json matches the same glob as the set documents but
# is an index, not a page — it has no regions and must never gain a challenges key.
NOT_A_SET = {"manifest"}


def slug_of(path):
    m = re.match(r"collectable_spawns_(.+)\.json$", os.path.basename(path))
    slug = m.group(1) if m else ""
    return "" if slug in NOT_A_SET else slug


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dist-dir", default=os.environ.get("OUT_DIR", os.path.join(REPO, "dist")),
                    help="directory holding collectable_spawns_*.json (default: dist/)")
    ap.add_argument("--channel", default="live", choices=("live", "pts"),
                    help="which TSV root the CHAL/CNDF exports come from (default: live)")
    ap.add_argument("--set", dest="only", default="", help="limit to one set slug")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    args = ap.parse_args()

    dist_dir = args.dist_dir if os.path.isabs(args.dist_dir) else os.path.join(REPO, args.dist_dir)
    paths = sorted(glob.glob(os.path.join(dist_dir, "collectable_spawns_*.json")))
    if not paths:
        print(f"[challenges] no collectable_spawns_*.json under {dist_dir} — nothing to do.")
        return

    chal_path, cndf_path = cchal.context_paths(args.channel)
    print(f"[challenges] channel={args.channel}")
    print(f"[challenges]   CHAL: {os.path.basename(chal_path) if chal_path else 'MISSING'}")
    print(f"[challenges]   CNDF: {os.path.basename(cndf_path) if cndf_path else 'MISSING'}")
    if not chal_path:
        print("[challenges] no CHAL export for this channel — refusing to blank existing blocks.")
        return

    changed = 0
    for path in paths:
        slug = slug_of(path)
        if not slug or (args.only and slug != args.only):
            continue
        try:
            data = json.load(open(path, encoding="utf-8"))
        except Exception as exc:
            print(f"[challenges] {slug}: unreadable ({exc}) — skipped.")
            continue

        block = cchal.challenges_for_set(slug, channel=args.channel) or EMPTY
        print(f"[challenges] {slug}: {cchal.build_report(block)}")
        if data.get("challenges") == block:
            continue
        data["challenges"] = block
        changed += 1
        if not args.dry_run:
            json.dump(data, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    verb = "would update" if args.dry_run else "updated"
    print(f"[challenges] {verb} {changed} document(s) in {os.path.relpath(dist_dir, REPO)}.")


if __name__ == "__main__":
    main()
