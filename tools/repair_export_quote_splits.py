#!/usr/bin/env python3
r"""
repair_export_quote_splits.py
-----------------------------
Rewrites the xEdit TSV exports in place, re-joining rows the export split in
half. See src/tsv_repair.py for the mechanism — in one line: the export scripts
round-tripped each row through `TStringList.DelimitedText` without setting
`QuoteChar`, so any value that STARTS with a double quote was treated as a quoted
token and broken into two columns, shifting the rest of the row one place right.

The `.pas` scripts have since been fixed (`QuoteChar := #0` on all 96 sites), so
fresh exports are clean. This tool exists for the exports already committed and
as a guard on anything dropped in before the fixed scripts were used.

WHY REPAIR THE FILES RATHER THAN EVERY READER
=============================================
Seventeen builders read ENTM alone, plus more for BOOK / ACTI / COBJ / MISC /
NOTE, each with its own hand-rolled `csv.DictReader` call. Threading a repair
through all of them is a large, risky diff with no way to be sure none was
missed, and it would not help `tools/`, a notebook, or the next script somebody
writes. Fixing the data once fixes every consumer, forever.

The exports are not sacred: the site already regenerates
`CURV_Export_*_POINTS.tsv` and rewrites `season_rewards.tsv` in the build. This
is the same kind of derived-data maintenance.

MINIMAL DIFF
============
Only the lines that actually change are rewritten. Every other line is copied
through byte for byte, including its original line ending and any encoding
quirk, so `git diff` shows exactly the repaired rows and nothing else.

SAFETY
======
* A row is only touched when the evidence includes a space-led fragment — the
  one shape nothing but a split produces. See tsv_repair._shape_score.
* `--dry-run` reports without writing.
* Idempotent: a second run finds nothing.
* Files are only rewritten if at least one row changed.

USAGE
=====
    python tools/repair_export_quote_splits.py --dry-run
    python tools/repair_export_quote_splits.py
    python tools/repair_export_quote_splits.py --path tsv/pts
    python tools/repair_export_quote_splits.py --check     # CI: fail if dirty
"""

from __future__ import annotations

import glob
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import tsv_repair  # noqa: E402

REPO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
TAG = "[repair-splits]"


def _encoding_of(path: str):
    """The first encoding that decodes the whole file, and whether it has a BOM."""
    raw = open(path, "rb").read()
    bom = raw.startswith(b"\xef\xbb\xbf")
    body = raw[3:] if bom else raw
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            body.decode(enc)
            return enc, bom, raw
        except UnicodeDecodeError:
            continue
    return None, bom, raw


def repair_file(path: str, dry_run: bool = False):
    """Return (rows_repaired, wrote). Only changed lines are rewritten.

    The rewrite happens at the BYTE level, splicing new bytes in place of the
    broken lines. Round-tripping through Python's text layer instead would
    "helpfully" normalise the file: writing with utf-8-sig adds a BOM to a file
    that never had one, which is a three-byte diff on every export and exactly
    the sort of noise that makes a data commit unreviewable.
    """
    header, rows = tsv_repair._read_raw(path)
    fixed, count = tsv_repair.repair_rows(header, list(rows))
    if not count:
        return 0, False

    encoding, bom, raw = _encoding_of(path)
    if encoding is None:                     # pragma: no cover - defensive
        print(f"{TAG} [SKIP] cannot decode {path}")
        return 0, False

    nl = b"\r\n" if b"\r\n" in raw[:raw.find(b"\n") + 1] else b"\n"
    prefix = raw[:3] if bom else b""
    lines = raw[len(prefix):].split(nl)

    # lines[0] is the header; data lines follow in the order the reader
    # produced them, with blank lines skipped on both sides.
    data_idx = 0
    changed = 0
    for i in range(1, len(lines)):
        if not lines[i].strip():
            continue
        if data_idx >= len(rows):
            break
        if rows[data_idx] != fixed[data_idx]:
            lines[i] = "\t".join(fixed[data_idx]).encode(encoding)
            changed += 1
        data_idx += 1

    if dry_run or not changed:
        return count, False

    with open(path, "wb") as f:
        f.write(prefix + nl.join(lines))
    return count, True


def main() -> int:
    args = sys.argv[1:]
    dry = "--dry-run" in args or "--check" in args
    roots = []
    if "--path" in args:
        roots = [os.path.join(REPO, args[args.index("--path") + 1])]
    else:
        roots = [os.path.join(REPO, "tsv"), os.path.join(REPO, "tsv", "pts")]

    paths = []
    for r in roots:
        paths += sorted(glob.glob(os.path.join(r, "*.tsv")))
    paths = [p for p in paths if not p.endswith(".bak")]

    # The *_Refs companions are hundreds of MB of pure FormID references with no
    # name or description column, so they cannot carry a quote-led value and are
    # not worth the minutes it takes to scan them. Raise with --max-mb.
    max_mb = 60
    if "--max-mb" in args:
        max_mb = int(args[args.index("--max-mb") + 1])
    skipped = [p for p in paths if os.path.getsize(p) > max_mb * 1024 * 1024]
    paths = [p for p in paths if os.path.getsize(p) <= max_mb * 1024 * 1024]
    for p in skipped:
        print(f"{TAG} [skip >{max_mb}MB] {os.path.relpath(p, REPO)}")

    total_rows = total_files = 0
    for p in paths:
        try:
            count, wrote = repair_file(p, dry_run=dry)
        except Exception as e:                # pragma: no cover - defensive
            print(f"{TAG} [ERROR] {os.path.basename(p)}: {e}")
            continue
        if count:
            total_rows += count
            total_files += 1
            verb = "would repair" if dry else ("repaired" if wrote else "no-op")
            print(f"{TAG} {verb} {count:4d} row(s) in {os.path.relpath(p, REPO)}")

    if not total_files:
        print(f"{TAG} nothing to repair — every export is clean")
        return 0

    print(f"{TAG} {total_rows} row(s) across {total_files} file(s)")
    if "--check" in args:
        print(f"{TAG} [FAIL] committed exports still contain quote-split rows. "
              f"Run: python tools/repair_export_quote_splits.py")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
