#!/usr/bin/env python3
r"""
diff_refr_placements.py — compare two REFR_Placements exports for one or more base
FormIDs and report exactly what Bethesda changed: refs added, removed, or moved.

Why this exists
---------------
The Slasher grave placements silently changed between builds: graves 05/07/09 were
re-placed under NEW reference FormIDs (SDOW__GraveNN, double underscore) at different
markers, and one WV Lumber Co. ref was deleted outright. Nothing in the pipeline
noticed, because `tsv/phantom_grave_sites.tsv` is only as fresh as the newest
committed export, and CI cannot produce an export (xEdit needs the game files).

Run this against every new export so a placement change is a reported fact rather
than something discovered weeks later from photographs that don't match the data.

Bases of interest
-----------------
  008F1672  SDOW_MQ02_Graves_GraveActivator   "Disturbed Grave"
  008E069E  SDOW_SlasherClue                  Pint-Sized Slasher Mask

Usage
-----
  # explicit pair
  python diff_refr_placements.py --old tsv/pts/REFR_Placements_PTS_2026-07-18_0757.tsv \
                                 --new tsv/pts/REFR_Placements_PTS_2026-08-22_1900.tsv

  # newest two exports that contain the base, chosen automatically
  python diff_refr_placements.py --base 008F1672 --base 008E069E --auto

Exit code is 1 when anything changed, so it can gate a workflow step.
"""

import argparse, glob, os, sys, math
from collections import OrderedDict
import tsv_source          # one resolver for every export selection

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

DEFAULT_BASES = OrderedDict([
    ("008F1672", "Disturbed Grave (SDOW_MQ02_Graves_GraveActivator)"),
    ("008E069E", "Pint-Sized Slasher Mask (SDOW_SlasherClue)"),
])

MOVED_TOL = 64.0     # game units; below this a coord change is float noise, not a move


def exports(channel=None):
    """Placement exports for ONE channel, oldest -> newest.

    Defaults to the channel the caller is building. It used to concatenate both
    channels and sort the result lexically, which mixed live and PTS placements in
    one list ordered by filename — the bug that published PTS graves on a live page.
    """
    ch = (channel or os.environ.get("DFBNB_CHANNEL") or "live").strip().lower()
    return tsv_source.all_matching("REFR_Placements_*.tsv", channel=ch)


def read(path, bases):
    """{base: {refFormID: row}} for the requested bases."""
    out = {b: {} for b in bases}
    with open(path, encoding="utf-8", errors="replace") as fh:
        hd = fh.readline().rstrip("\n").split("\t")
        if "BaseFormID" not in hd:
            return out
        i = {h: n for n, h in enumerate(hd)}
        for ln in fh:
            c = ln.rstrip("\n").split("\t")
            if len(c) <= i.get("Z", 0):
                continue
            b = c[i["BaseFormID"]].strip().upper()
            if b not in out:
                continue
            def g(k):
                n = i.get(k)
                return c[n].strip() if n is not None and n < len(c) else ""
            out[b][g("RefFormID").upper()] = {
                "edid": g("RefEDID"), "cell": g("CellEDID"),
                "x": g("X"), "y": g("Y"), "z": g("Z"),
            }
    return out


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def report(base, label, old, new):
    added = [r for r in new if r not in old]
    removed = [r for r in old if r not in new]
    moved = []
    for r in new:
        if r not in old:
            continue
        a, b = old[r], new[r]
        ax, ay, bx, by = _f(a["x"]), _f(a["y"]), _f(b["x"]), _f(b["y"])
        if None in (ax, ay, bx, by):
            continue
        d = math.hypot(bx - ax, by - ay)
        if d > MOVED_TOL:
            moved.append((r, d, a, b))

    changed = bool(added or removed or moved)
    head = f"{base}  {label}"
    print(f"\n{head}\n{'-' * len(head)}")
    print(f"  {len(old)} refs before  ->  {len(new)} refs after")
    if not changed:
        print("  NO CHANGES — every reference is identical.")
        return False

    if removed:
        print(f"\n  REMOVED ({len(removed)}):")
        for r in sorted(removed):
            v = old[r]
            print(f"    {r}  {v['edid'] or '(no EDID)':18} {v['cell'] or '':28} "
                  f"({v['x']}, {v['y']})")
    if added:
        print(f"\n  ADDED ({len(added)}):")
        for r in sorted(added):
            v = new[r]
            print(f"    {r}  {v['edid'] or '(no EDID)':18} {v['cell'] or '':28} "
                  f"({v['x']}, {v['y']})")
    if moved:
        print(f"\n  MOVED ({len(moved)}):")
        for r, d, a, b in sorted(moved, key=lambda t: -t[1]):
            print(f"    {r}  {b['edid'] or '(no EDID)':18} {d:>9.0f}u  "
                  f"({a['x']}, {a['y']}) -> ({b['x']}, {b['y']})")

    # A removal paired with an addition is usually a re-placement, not a deletion.
    if removed and added:
        print("\n  NOTE: removals alongside additions usually mean Bethesda RE-PLACED "
              "\n        the object under a new reference FormID rather than deleting it."
              "\n        Check the added EDIDs for the same grave/mask number before "
              "\n        treating anything as gone.")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old")
    ap.add_argument("--new")
    ap.add_argument("--auto", action="store_true",
                    help="use the newest two exports containing each base")
    ap.add_argument("--base", action="append", dest="bases",
                    help="base FormID (repeatable). Default: graves + masks.")
    args = ap.parse_args()

    bases = OrderedDict((b.upper(), DEFAULT_BASES.get(b.upper(), "")) for b in args.bases) \
        if args.bases else DEFAULT_BASES

    if args.auto or not (args.old and args.new):
        cand = [p for p in exports() if any(read(p, bases)[b] for b in bases)]
        if len(cand) < 2:
            print(f"Need two exports containing those bases; found {len(cand)}.")
            for p in cand:
                print("   ", os.path.relpath(p, REPO))
            return 2
        old_p, new_p = cand[-2], cand[-1]
    else:
        old_p, new_p = args.old, args.new

    print(f"OLD  {os.path.relpath(old_p, REPO)}")
    print(f"NEW  {os.path.relpath(new_p, REPO)}")
    o, n = read(old_p, bases), read(new_p, bases)

    changed = False
    for b, label in bases.items():
        if not o[b] and not n[b]:
            print(f"\n{b}  {label}\n  base not present in either export — skipped")
            continue
        changed |= report(b, label, o[b], n[b])
    print()
    return 1 if changed else 0


if __name__ == "__main__":
    sys.exit(main())
