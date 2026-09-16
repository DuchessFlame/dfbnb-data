#!/usr/bin/env python3
r"""
plan_changes.py — what changed about a plan that was already here.

The New Plans page answers "what did this patch ADD", by diffing the plan
roster: FormIDs in the new BOOK export that were not in the previous one. A
plan that was always here and has since become tradeable, or picked up a second
source, keeps its FormID, so the roster diff can never see it. This module is
the other half: it snapshots the ATTRIBUTES of every plan at build time and
diffs those on the next build.

    python3 src/plan_changes.py dist/plan_master.json          # diff + report
    python3 src/plan_changes.py dist/plan_master.json --write   # …and re-snapshot

What it watches, and nothing else — every one of these is a fact a reader would
want to know had changed, and each is a single field the build already resolves:

    tradeable       can you trade or drop it
    stops_dropping  has it left the loot pools
    cut             is it unobtainable
    name            did the studio rename it
    routes          where it comes from, by route name and ledger bucket

Rates are deliberately NOT watched. A drop rate moves whenever the surrounding
loot pool moves, and a page that announces "the rate changed" every patch for
two thousand plans is noise that buries the six real changes.


THE BASELINE PROBLEM
--------------------
A naive attribute diff lies the first time a missing export comes back. In
September 2026 the BOOK export was current and COBJ was three months stale, so
every Slasher plan resolved no recipe and no source at all. When COBJ is
re-exported those plans will each appear to have "gained" a source they have
had all along — hundreds of false "now also drops from …" lines, in the exact
patch where the page is being trusted for the first time.

So a snapshot records WHICH EXPORT each field was read from, and a field's
change is only published when the export set was COHERENT — when the record
export the field depends on is at least as new as the BOOK export that defined
the roster. A field resolved against a stale export could not have seen what it
did not see, so its absence proves nothing and no claim is made from it. The
change is held, counted, and named by --report, not silently dropped.
"""

import argparse
import glob
import json
import os
import re
import sys

SNAPSHOT = os.path.join("data", "plan_snapshot.json")
VERSION = 1

# The record exports a plan's fields are resolved from. Only the types that
# actually gate a watched field are listed — this is the dependency map the
# coherence rule reads, not an inventory of the tsv folder.
# The prefix is the one the FILES use, not the record signature: the quest
# export is QUEST_Export_*.tsv, and a "QUST" entry here matches nothing, is
# permanently stale, and would suppress every route change forever.
RECORD_TYPES = ("BOOK", "COBJ", "LVLI", "OMOD", "ARMO", "WEAP",
                "ALCH", "ACTI", "MISC", "FURN", "QUEST", "CONT", "NPC")

# field -> the exports it is resolved from. BOOK defines the roster and is the
# yardstick every other export is judged against, so it is not listed as its own
# dependency.
FIELD_DEPENDS = {
    "name":           (),
    "tradeable":      (),
    "stops_dropping": ("LVLI",),
    "cut":            ("COBJ", "LVLI"),
    "routes":         ("COBJ", "LVLI", "QUEST", "CONT", "NPC"),
}

_ISO = re.compile(r"_(\d{4})-(\d{2})-(\d{2})")
_STAMP = re.compile(r"_([A-Za-z]{3,9})_(\d{4})")
MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"), 1)}


def _stamp(path):
    """(year, month, day) from the filename — the same rule the builders use.

    The month in the NAME decides, never the mtime: tsv/ holds several exports
    with identical timestamps because a sync touched them all at once, and an
    mtime sort there silently picks May over July.
    """
    name = os.path.basename(path)
    m = _ISO.search(name)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = _STAMP.search(name)
    if not m:
        return (0, 0, 0)
    return (int(m.group(2)), MONTHS.get(m.group(1)[:3].lower(), 0), 0)


def export_fingerprint(tsv_dir="tsv"):
    """record type -> the newest export of it, as a filename.

    Companion sheets (…_Locations, …_Properties, …_Refs) are folded into their
    parent type: which sheet a build read is not the question, only which BUILD
    of the game the data came from.
    """
    out = {}
    for rtype in RECORD_TYPES:
        hits = glob.glob(os.path.join(tsv_dir, f"{rtype}_Export_*.tsv"))
        if not hits:
            out[rtype] = ""
            continue
        best = sorted(hits, key=lambda h: (_stamp(h), os.path.getmtime(h)),
                      reverse=True)[0]
        out[rtype] = os.path.basename(best)
    return out


def coherence(exports):
    """Which record exports are at least as new as the BOOK export.

    BOOK defines the roster. A record export older than it describes an earlier
    build of the game, so anything resolved through it is answering a question
    about the wrong version and cannot be used as evidence that something was
    absent.
    """
    book = _stamp(exports.get("BOOK") or "")
    out = {}
    for rtype in RECORD_TYPES:
        name = exports.get(rtype) or ""
        out[rtype] = bool(name) and _stamp(name) >= book
    out["BOOK"] = bool(exports.get("BOOK"))
    return out


# ── the snapshot ────────────────────────────────────────────────────────────

def route_key(route):
    """The identity of a source, with the rate deliberately left out."""
    return "{}|{}".format((route.get("route") or "").strip(),
                          (route.get("source_type") or "").strip())


def route_buckets(item):
    """route index -> its ledger label ("Events & Activities", "Caps", …)."""
    out = {}
    for entry in (item.get("obtain_ledger") or []):
        for idx in (entry.get("routes") or []):
            out[idx] = entry.get("label") or ""
    return out


def plan_state(item):
    """The watched fields of one plan, in the shape the snapshot stores."""
    buckets = route_buckets(item)
    routes = {}
    for i, route in enumerate(item.get("obtain_routes") or []):
        routes[route_key(route)] = buckets.get(i, "")
    return {
        "name":           item.get("name") or "",
        "tradeable":      item.get("tradeable"),
        "stops_dropping": item.get("stops_dropping"),
        "cut":            bool(item.get("cut")),
        "routes":         routes,
    }


def snapshot(rows, tsv_dir="tsv", taken=""):
    import datetime
    exports = export_fingerprint(tsv_dir)
    return {
        "version": VERSION,
        "taken": taken or datetime.datetime.now(datetime.timezone.utc)
                                  .isoformat(timespec="seconds"),
        "exports": exports,
        "coherent": coherence(exports),
        "plans": {r["id"]: plan_state(r) for r in rows if r.get("id")},
    }


def read_snapshot(path=SNAPSHOT):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        snap = json.load(f)
    return snap if snap.get("version") == VERSION else None


def write_snapshot(snap, path=SNAPSHOT):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=1, sort_keys=True)
    os.replace(tmp, path)


# ── the diff ────────────────────────────────────────────────────────────────

def _publishable(field, was_coherent, now_coherent,
                 was_exports=None, now_exports=None):
    """Can a change in this field be claimed, given both export sets?

    A dependency clears in either of two ways.

    1. IT DID NOT MOVE. If the snapshot and this build read the byte-identical
       export file, both saw exactly the same data from it, so no difference
       between them can have come from it and it has nothing to hide. This case
       matters more than it sounds: CONT and FURN lag for months at a time, and
       without it a permanently-stale export silences route reporting forever —
       the rule would protect against a false positive that cannot happen while
       suppressing every true one. Duchess, on being shown that: "stale is bad.
       make it generative."

    2. BOTH SIDES ARE COHERENT. The export is at least as new as the BOOK export
       that defined the roster, on both builds, so neither was answering about an
       older build of the game.

    Anything else is held: the dependency moved AND at least one side was reading
    data older than its own roster, which is exactly the case where a source
    "appearing" means the data caught up rather than the game changing.

    Returns the blocking dependency's name, or "" when the field can be claimed.
    """
    was_exports = was_exports or {}
    now_exports = now_exports or {}
    for dep in FIELD_DEPENDS.get(field, ()):
        before, after = was_exports.get(dep), now_exports.get(dep)
        if before and after and before == after:
            continue                       # 1. it did not move
        if was_coherent.get(dep) and now_coherent.get(dep):
            continue                       # 2. both sides coherent
        return dep
    return ""


_SINCE = "the previous build"


def _phrase(field, old, new, bucket=""):
    if field == "tradeable":
        if new is True and old is not True:
            return "Now tradeable — it could not be traded or dropped before."
        if new is False and old is not False:
            return "No longer tradeable — it could be traded or dropped before."
    if field == "stops_dropping":
        if new is True:
            return "No longer drops — it has been taken out of the loot pools."
        if new is False:
            return "Dropping again — it is back in the loot pools."
    if field == "cut":
        return ("Now cut content — it can no longer be obtained." if new
                else "No longer cut — it can be obtained again.")
    if field == "name":
        return f"Renamed — it was called “{old}”."
    if field == "route-added":
        where = f" ({bucket})" if bucket else ""
        return f"Now also comes from {new}{where}."
    if field == "route-removed":
        where = f" ({bucket})" if bucket else ""
        return f"No longer comes from {old}{where}."
    return ""


def diff(rows, snap, tsv_dir="tsv", stats=None):
    """Attach `changes` to every row that changed since `snap`. Returns stats.

    A row with nothing to say gets `changes: []` rather than no key, so the
    front end never has to distinguish "unchanged" from "built before this
    existed" — a page shipped against an older dataset just renders nothing.

    Newly-added sources are ALSO marked on the route itself (`"new": true`), so
    How to Obtain can point at the line that is new instead of making the
    reader match a sentence in Technical against a table above it.
    """
    stats = stats if stats is not None else {}

    def bump(key):
        stats[key] = stats.get(key, 0) + 1

    now_exports = export_fingerprint(tsv_dir)
    now_coherent = coherence(now_exports)
    was_coherent = (snap or {}).get("coherent") or {}
    was_exports = (snap or {}).get("exports") or {}
    prev = (snap or {}).get("plans") or {}
    since = (snap or {}).get("taken") or ""

    for item in rows:
        item["changes"] = []
        for route in (item.get("obtain_routes") or []):
            route.pop("new", None)

        before = prev.get(item.get("id") or "")
        if before is None:
            bump("unseen")                       # new plan, or first snapshot
            continue
        after = plan_state(item)
        changes = []

        for field in ("tradeable", "stops_dropping", "cut", "name"):
            if before.get(field) == after.get(field):
                continue
            held = _publishable(field, was_coherent, now_coherent,
                                was_exports, now_exports)
            if held:
                bump(f"held:{field}:{held}")
                continue
            text = _phrase(field, before.get(field), after.get(field))
            if text:
                changes.append({"field": field, "from": before.get(field),
                                "to": after.get(field), "text": text,
                                "since": since})
                bump(field)

        old_routes = before.get("routes") or {}
        new_routes = after.get("routes") or {}
        if old_routes != new_routes:
            held = _publishable("routes", was_coherent, now_coherent,
                                was_exports, now_exports)
            if held:
                bump(f"held:routes:{held}")
            else:
                for key in sorted(set(new_routes) - set(old_routes)):
                    label = key.split("|")[0]
                    changes.append({"field": "routes", "kind": "added",
                                    "route": label, "bucket": new_routes[key],
                                    "text": _phrase("route-added", "", label,
                                                    new_routes[key]),
                                    "since": since})
                    bump("route-added")
                for key in sorted(set(old_routes) - set(new_routes)):
                    label = key.split("|")[0]
                    changes.append({"field": "routes", "kind": "removed",
                                    "route": label, "bucket": old_routes[key],
                                    "text": _phrase("route-removed", label, "",
                                                    old_routes[key]),
                                    "since": since})
                    bump("route-removed")
                added = set(new_routes) - set(old_routes)
                for route in (item.get("obtain_routes") or []):
                    if route_key(route) in added:
                        route["new"] = True

        item["changes"] = changes
        if changes:
            bump("changed")
    return stats


def report(stats, stream=sys.stderr):
    changed = stats.get("changed", 0)
    held = {k: v for k, v in stats.items() if k.startswith("held:")}
    print(f"  {changed} plans changed"
          f"   (unseen {stats.get('unseen', 0)})", file=stream)
    for key in sorted(k for k in stats if not k.startswith("held:")
                      and k not in ("changed", "unseen")):
        print(f"    {key:16s} {stats[key]}", file=stream)
    if held:
        print("  held back — the export set was not coherent, so the change "
              "cannot be claimed:", file=stream)
        for key in sorted(held):
            _, field, dep = key.split(":", 2)
            print(f"    {field:16s} {held[key]:5d}  ({dep} export is older "
                  f"than BOOK)", file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("paths", nargs="+", help="dist JSON files to patch in place")
    ap.add_argument("--snapshot", default=SNAPSHOT)
    ap.add_argument("--tsv-dir", default="tsv")
    ap.add_argument("--write", action="store_true",
                    help="re-snapshot from the FIRST path after diffing")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    snap = read_snapshot(args.snapshot)
    if snap is None:
        print(f"[changes] no usable snapshot at {args.snapshot} — "
              f"this build becomes the baseline", file=sys.stderr)

    first_rows = None
    for path in args.paths:
        if not os.path.exists(path):
            print(f"[changes] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            blob = json.load(f)
        rows = [it for it in _walk(blob)
                if isinstance(it, dict) and "plan_item" in it]
        if first_rows is None:
            first_rows = rows
        print(f"[changes] {path}: {len(rows)} rows")
        report(diff(rows, snap, args.tsv_dir), stream=sys.stdout)
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    if args.write and first_rows is not None and not args.dry_run:
        write_snapshot(snapshot(first_rows, args.tsv_dir), args.snapshot)
        print(f"[changes] snapshot written: {args.snapshot} "
              f"({len(first_rows)} plans)")
    return 0


def _walk(node):
    if isinstance(node, dict):
        if "plan_item" in node:
            yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


if __name__ == "__main__":
    raise SystemExit(main())
