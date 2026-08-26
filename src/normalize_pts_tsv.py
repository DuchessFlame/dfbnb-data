#!/usr/bin/env python3
"""
normalize_pts_tsv.py
====================
Turn a folder of PTS xEdit exports into a clean, build-ready TSV set.

WHY THIS EXISTS
---------------
The PTS export scripts (!!!Wordpress - Export*.pas, PTS channel) name their
files with a unique timestamp so OneDrive never collapses repeat same-month
pulls, e.g.:

    ACTI_Export_PTS_2026-06-21_1430_ACTI.tsv
    ACTI_Export_PTS_2026-06-28_0900_ACTI.tsv   <- a later pull, same month
    LVLI_Export_PTS_2026-06-28_0900_LVLI_Entries.tsv
    ALCH_Export_PTS_2026-06-28_0900.tsv

The existing build scripts pick "the newest TSV" by parsing a *month name*
out of the filename (newest() -> regex _([a-z]+)_(\\d{4})_). PTS timestamp
names don't match that, and there can be several pulls in one month.

This script does the channel-specific work so the builders need ZERO changes:
  1. groups PTS files by (base, suffix)  e.g. (ACTI_Export, _ACTI)
  2. keeps only the NEWEST pull per group (by the embedded timestamp)
  3. writes each chosen file out under a normal live-style name the builders
     already understand:  <base>_<MonthName>_<Year><suffix>.tsv
        ACTI_Export_June_2026_ACTI.tsv
        LVLI_Export_June_2026_LVLI_Entries.tsv
        ALCH_Export_June_2026.tsv

The build then runs exactly as the live build does, just over PTS data.

Usage:
  python normalize_pts_tsv.py --src tsv/pts --dst /tmp/ptsbuild/tsv
  python normalize_pts_tsv.py --src tsv/pts --dst tsv --in-place   # overwrite
"""

import argparse
import calendar
import glob
import os
import re
import shutil
import sys

# ---------------------------------------------------------------------------
# Health checks: a newest pull that is obviously broken must NOT win.
#
# 2026-08-22 shipped a CHAL export produced by a STALE copy of
# '!!!Wordpress - ExportCHALToTSV.pas' (the pre-July-2026 version). It had no
# MNAM/RNAM/HNAM/JASF/ANAM columns at all and CNAM/ENAM were blank on all 5692
# rows, which aborted build_challenges_json_v3.py and killed the whole PTS
# build. Rather than fail the run on a bad export, fall back to the newest
# healthy pull for that group and shout about it in the log.
#
#   (base, suffix) -> {
#       'required_nonempty': columns that must exist AND have >=1 non-blank row
#       'expected_columns' : columns a current-script export always emits
#   }
# ---------------------------------------------------------------------------
HEALTH_CHECKS = {
    ('CHAL_Export', ''): {
        'required_nonempty': ['CNAM', 'ENAM'],
        'expected_columns': ['CNAM', 'ENAM', 'MNAM', 'RNAM', 'HNAM', 'JASF', 'ANAM'],
    },
}


def _read_tsv_header_and_rows(path, max_rows=None):
    """Yield (header list, iterator of split rows). Tolerant of encoding junk."""
    for enc in ('utf-8-sig', 'cp1252'):
        try:
            with open(path, encoding=enc, errors='replace', newline='') as f:
                header = f.readline().replace('\x00', '').rstrip('\r\n').split('\t')
                rows = []
                for i, line in enumerate(f):
                    if max_rows is not None and i >= max_rows:
                        break
                    rows.append(line.replace('\x00', '').rstrip('\r\n').split('\t'))
                return header, rows
        except UnicodeDecodeError:
            continue
    return [], []


def check_health(key, path):
    """Return (ok, list of problem strings) for one candidate export."""
    spec = HEALTH_CHECKS.get(key)
    if not spec:
        return True, []

    header, rows = _read_tsv_header_and_rows(path)
    if not header:
        return False, ['unreadable / empty file']

    problems = []
    index = {name: i for i, name in enumerate(header)}

    missing = [c for c in spec.get('expected_columns', []) if c not in index]
    if missing:
        problems.append('missing column(s): ' + ', '.join(missing))

    for col in spec.get('required_nonempty', []):
        i = index.get(col)
        if i is None:
            continue  # already reported as missing
        if not any(len(r) > i and r[i].strip() for r in rows):
            problems.append(f'{col} is blank on all {len(rows)} rows')

    return (not problems), problems

# ACTI_Export _PTS_ 2026-06-21 _ 1430  _ACTI .tsv
#  ^base            ^date         ^time  ^suffix
PTS_RE = re.compile(
    r'^(?P<base>.+?)_PTS_(?P<date>\d{4}-\d{2}-\d{2})(?:[_-](?P<time>\d{3,6}))?(?P<suffix>.*)\.tsv$',
    re.IGNORECASE,
)


def parse_pts_name(fname):
    """Return (base, suffix, sortkey, month, year) or None if not a PTS export."""
    m = PTS_RE.match(fname)
    if not m:
        return None
    base = m.group('base')
    date = m.group('date')              # 2026-06-21
    time = m.group('time') or '0000'
    suffix = m.group('suffix') or ''    # '_ACTI', '_LVLI_Entries', or ''
    year, month, day = date.split('-')
    sortkey = f"{date}_{time.zfill(4)}"
    month_name = calendar.month_name[int(month)]   # 'June'
    return base, suffix, sortkey, month_name, year


def normalize(src, dst, in_place=False):
    files = sorted(glob.glob(os.path.join(src, '*.tsv')))
    candidates = {}      # (base, suffix) -> [(sortkey, fullpath, month, year), ...]
    skipped = []
    for fp in files:
        fn = os.path.basename(fp)
        parsed = parse_pts_name(fn)
        if not parsed:
            skipped.append(fn)
            continue
        base, suffix, sortkey, month, year = parsed
        candidates.setdefault((base, suffix), []).append((sortkey, fp, month, year))

    # Newest healthy pull per group. Groups with no HEALTH_CHECKS entry always
    # take the newest pull (check_health returns ok), so behaviour is unchanged
    # for everything except the explicitly guarded exports.
    groups = {}
    rejected = []        # (group label, filename, [problems])
    for key, entries in candidates.items():
        entries.sort(reverse=True)          # newest first
        chosen = None
        for entry in entries:
            ok, problems = check_health(key, entry[1])
            if ok:
                chosen = entry
                break
            rejected.append((f'{key[0]}{key[1]}', os.path.basename(entry[1]), problems))
        if chosen is None:
            # Every pull failed — keep the newest so the builder's own guard
            # reports the real problem instead of the file silently vanishing.
            chosen = entries[0]
        groups[key] = chosen

    if not in_place:
        os.makedirs(dst, exist_ok=True)

    written = []
    for (base, suffix), (sortkey, fp, month, year) in sorted(groups.items()):
        out_name = f"{base}_{month}_{year}{suffix}.tsv"
        out_path = os.path.join(dst, out_name)
        shutil.copyfile(fp, out_path)
        written.append((os.path.basename(fp), out_name))

    return written, skipped, len(files), rejected


def main():
    ap = argparse.ArgumentParser(description="Normalize PTS xEdit exports into a build-ready TSV set.")
    ap.add_argument('--src', default='tsv/pts', help='folder of raw PTS exports (default: tsv/pts)')
    ap.add_argument('--dst', required=True, help='destination folder for normalized live-style TSVs')
    ap.add_argument('--in-place', action='store_true', help='write into --dst even if it already exists (no mkdir guard)')
    args = ap.parse_args()

    if not os.path.isdir(args.src):
        print(f"[normalize_pts] src not found: {args.src}", file=sys.stderr)
        sys.exit(1)

    written, skipped, total, rejected = normalize(args.src, args.dst, in_place=args.in_place)

    print(f"[normalize_pts] scanned {total} file(s) in {args.src}")
    for group, fname, problems in rejected:
        print(f"[normalize_pts] *** REJECTED {fname} ({group}): {'; '.join(problems)}")
        print("[normalize_pts]     falling back to the previous healthy pull. "
              "Re-run the xEdit export with the CURRENT script from "
              "'GitHub\\xedit scripts\\' — an old copy in xEdit's Edit Scripts "
              "folder produces exports like this.")
    print(f"[normalize_pts] wrote {len(written)} normalized file(s) to {args.dst}:")
    for src_name, out_name in written:
        print(f"    {src_name}  ->  {out_name}")
    if skipped:
        print(f"[normalize_pts] skipped {len(skipped)} non-PTS file(s): {', '.join(skipped[:10])}"
              + (" ..." if len(skipped) > 10 else ""))
    if not written:
        print("[normalize_pts] WARNING: no PTS files matched — nothing to build.", file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
