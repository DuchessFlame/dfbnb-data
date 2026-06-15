#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_curvetables.py
Refresh the curve-table JSON tree straight from the Fallout 76 game archive.

WHY THIS EXISTS
---------------
The CURV records in the game only store a *pointer* to a curve-table JSON
file -- they don't carry the X/Y points. The points live in JSON files
packed inside `SeventySix - Startup.ba2` under `misc/curvetables/json/`.

The `json` tree in this repo is just a snapshot of those files. The xEdit
export script and build-curv-points only ever READ that snapshot -- nothing
refreshes it. So every time Bethesda adds a new curve (e.g. the June 2026
World Pets curves) the snapshot is stale and those curves come out empty.

This script is the missing step: it unpacks every `misc/curvetables/json/*`
file from Startup.ba2 into the repo's json root, so the snapshot always
matches the live game. Run it locally (it needs the game files, which are
not in CI), commit the refreshed tree, and GitHub CI regenerates the POINTS
TSV from it automatically.

WORKFLOW
--------
    1. (local) xEdit ExportCURVToTSV.pas      -> CURV_Export_<Month>_CURV.tsv
    2. (local) python tools/extract_curvetables.py   <-- THIS (refresh JSONs)
    3. commit the json tree + the CURV tsv
    4. (CI)   build_curv_points_tsv.py         -> CURV_Export_<Month>_POINTS.tsv

USAGE
-----
    # Defaults: auto-find Startup.ba2 + refresh every json root it can locate
    python tools/extract_curvetables.py

    # Explicit:
    python tools/extract_curvetables.py \
        --ba2 "D:\\steamlibrary\\steamapps\\common\\Fallout 76 Playtest\\Data\\SeventySix - Startup.ba2" \
        --json-root ../dfbnb-data/data/curvetables/json \
        --json-root ../fo76-tools/misc/curvetables/json

    # See what would change without writing anything:
    python tools/extract_curvetables.py --dry-run

No external dependencies -- standard library only (PowerShell not required).
"""

import argparse
import os
import struct
import sys
import zlib
from pathlib import Path

# Path prefix inside the archive that holds the curve-table JSONs.
ARCHIVE_PREFIX = "misc/curvetables/json/"

# Where Startup.ba2 normally lives. Override with --ba2.
DEFAULT_BA2_CANDIDATES = [
    r"D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data\SeventySix - Startup.ba2",
    r"C:\Program Files (x86)\Steam\steamapps\common\Fallout76\Data\SeventySix - Startup.ba2",
]

# json roots to refresh, relative to the repo this script sits in (../sibling).
DEFAULT_JSON_ROOTS = [
    "data/curvetables/json",                 # dfbnb-data (committed; CI reads this)
    "../fo76-tools/misc/curvetables/json",   # fo76-tools (build-curv-points.ps1 reads this)
]


def find_ba2(explicit):
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            sys.exit(f"ERROR: --ba2 not found: {p}")
        return p
    for cand in DEFAULT_BA2_CANDIDATES:
        if Path(cand).is_file():
            return Path(cand)
    sys.exit(
        "ERROR: could not auto-locate Startup.ba2. Pass it explicitly:\n"
        '  --ba2 "D:\\...\\Fallout 76\\Data\\SeventySix - Startup.ba2"'
    )


def read_ba2_index(ba2_path):
    """Parse a BA2 GNRL (general) v1 archive -> list of (name, offset, packed, unpacked)."""
    with open(ba2_path, "rb") as f:
        head = f.read(24)
        if head[0:4] != b"BTDX":
            sys.exit(f"ERROR: {ba2_path.name} is not a BA2 archive (bad magic).")
        version, = struct.unpack("<I", head[4:8])
        btype = head[8:12]
        if btype != b"GNRL":
            sys.exit(
                f"ERROR: {ba2_path.name} is type {btype!r}, expected GNRL. "
                "Curve tables live in the GNRL Startup archive."
            )
        n_files, = struct.unpack("<I", head[12:16])
        name_table_off, = struct.unpack("<Q", head[16:24])

        # File records: 36 bytes each, immediately after the 24-byte header.
        f.seek(24)
        recs = []
        for _ in range(n_files):
            rec = f.read(36)
            offset, packed, unpacked, _align = struct.unpack("<QIII", rec[16:36])
            recs.append((offset, packed, unpacked))

        # Name table: uint16 length + path, one per file, in record order.
        f.seek(name_table_off)
        names = []
        for _ in range(n_files):
            (ln,) = struct.unpack("<H", f.read(2))
            names.append(f.read(ln).decode("latin1"))

    return [
        (names[i].replace("\\", "/"), recs[i][0], recs[i][1], recs[i][2])
        for i in range(n_files)
    ]


def read_member(ba2_path, offset, packed, unpacked):
    with open(ba2_path, "rb") as f:
        f.seek(offset)
        data = f.read(packed if packed else unpacked)
    if packed:
        data = zlib.decompress(data)
    return data


def main(argv=None):
    ap = argparse.ArgumentParser(description="Refresh curve-table JSONs from Startup.ba2")
    ap.add_argument("--ba2", default=None, help="Path to SeventySix - Startup.ba2")
    ap.add_argument("--json-root", action="append", default=None,
                    help="Target json root (repeatable). Defaults to the dfbnb-data "
                         "and fo76-tools trees relative to this repo.")
    ap.add_argument("--clean", action="store_true", default=True,
                    help="Remove existing *.json under each target first so the result "
                         "exactly mirrors the game (default: on).")
    ap.add_argument("--no-clean", dest="clean", action="store_false",
                    help="Overwrite/add only; leave other existing json files in place.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would change; write nothing.")
    args = ap.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    ba2_path = find_ba2(args.ba2)

    roots = args.json_root or DEFAULT_JSON_ROOTS
    targets = []
    for r in roots:
        p = (repo_root / r).resolve() if not os.path.isabs(r) else Path(r)
        # Only refresh roots whose parent dir exists (don't invent stray trees).
        if p.exists() or p.parent.exists():
            targets.append(p)
        else:
            print(f"[skip] json root not found, ignoring: {p}")
    if not targets:
        sys.exit("ERROR: no usable json roots. Pass --json-root <path>.")

    print(f"Archive : {ba2_path}")
    index = read_ba2_index(ba2_path)
    members = [(n, o, p, u) for (n, o, p, u) in index
               if n.lower().startswith(ARCHIVE_PREFIX)]
    print(f"Found   : {len(members)} curve-table JSON files in archive\n")
    if not members:
        sys.exit(f"ERROR: no '{ARCHIVE_PREFIX}*' entries in {ba2_path.name}.")

    for target in targets:
        print(f"=== {target} ===")
        added = changed = same = removed = 0

        # Build the fresh set first (relpath -> bytes), all lowercase like the game.
        fresh = {}
        for name, off, packed, unpacked in members:
            rel = name[len(ARCHIVE_PREFIX):]  # strip the archive prefix
            fresh[rel] = read_member(ba2_path, off, packed, unpacked)

        # Optional clean: delete json files that won't be replaced (true mirror).
        if args.clean and target.exists():
            keep = {(target / r).resolve() for r in fresh}
            for existing in target.rglob("*.json"):
                if existing.resolve() not in keep:
                    removed += 1
                    if not args.dry_run:
                        existing.unlink()
            # also drop now-empty dirs
            if not args.dry_run:
                for d in sorted([p for p in target.rglob("*") if p.is_dir()],
                                key=lambda p: len(p.parts), reverse=True):
                    try:
                        d.rmdir()
                    except OSError:
                        pass

        for rel, data in fresh.items():
            dest = target / rel
            if dest.exists():
                old = dest.read_bytes()
                if old == data:
                    same += 1
                    continue
                changed += 1
            else:
                added += 1
            if not args.dry_run:
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(data)

        verb = "would" if args.dry_run else ""
        print(f"  added   {verb} : {added}")
        print(f"  changed {verb} : {changed}")
        print(f"  removed {verb} : {removed}  (stale files not in current game)")
        print(f"  unchanged     : {same}")
        print()

    print("Done." + ("  (dry run -- nothing written)" if args.dry_run else
          "  Commit the json tree, then CI rebuilds the POINTS TSV."))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
