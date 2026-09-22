#!/usr/bin/env python3
r"""
plan_pins.py — manual "this plan changed" pins with an auto-expiry window.

WHY
---
The New Plans "changed" pass (plan_changes.py) diffs a plan's attributes against
a committed snapshot. Two things routinely make it MISS a real, same-patch
change:

  1. the snapshot is re-taken AFTER the patch build, so the new source is baked
     into the baseline and the diff finds nothing; and
  2. the coherence gate holds a route-add while a dependency export
     (COBJ / LVLI / CONT) is older than the BOOK export.

Either way a plan that genuinely gained a source this patch never gets its
↻ Changed treatment. This module is the sanctioned manual override: a small
declarative TSV (data/new_plans_pins.tsv) lists the plan, the source it gained,
and a start/end window, and this pass FORCE-ATTACHES the same `changes` entry the
diff would have produced — reusing the exact render contract (the ↻ Changed pill,
the "Changed since the last build" Technical block, and the New tag on the route
line). When today passes a pin's EndDate the pin goes inert on its own; no edit,
no dist hand-patch.

WHERE IT RUNS
-------------
reenrich_plan_master.enrich_file() calls apply() immediately AFTER
plan_changes.diff(). Order matters: diff() clears item["changes"] and pops any
route "new" flag every run, so pins must land after it. Because diff() resets
first, re-enriching any number of times converges — pins never accumulate.

DE-DUPE / HAND-OFF TO THE REAL DIFF
-----------------------------------
A pin is de-duped against whatever the diff already found: if the export catches
up and plan_changes detects the same route-add on its own, the pin adds nothing
(it is counted as `deduped`). So a pin can safely be left in place until its end
date even after the data self-heals — it simply stops contributing.

The wording is produced by plan_changes._phrase(), NOT re-implemented here, so a
pinned change reads identically to an organically-detected one.

STANDALONE
----------
    python3 src/plan_pins.py --list                 # every pin + active/inert
    python3 src/plan_pins.py --today 2027-01-01      # as-of a date (expiry check)
    python3 src/plan_pins.py --lint dist/plan_master.json   # routes/ids resolve?
"""
from __future__ import annotations

import argparse
import csv
import datetime
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import plan_changes  # reuse _phrase() so pins read exactly like real changes

PINS_TSV = os.path.join(ROOT, "data", "new_plans_pins.tsv")

# Kind -> (change field, how to build the change dict). route-added/route-removed
# carry a Route + Bucket; the scalar kinds carry neither. Everything routes its
# wording through plan_changes._phrase so a pin cannot drift from an organic change.
_SCALAR = {
    "tradeable":      ("tradeable",      False, True),
    "not-tradeable":  ("tradeable",      True,  False),
    "stops-dropping": ("stops_dropping", False, True),
    "drops-again":    ("stops_dropping", True,  False),
    "cut":            ("cut",            False, True),
    "uncut":          ("cut",            True,  False),
}
_ROUTE_KINDS = {"route-added", "route-removed"}


def _parse_dmy(s):
    """'15/9/2026' (D/M/YYYY, no zero-pad) -> date, or None. Same rule as the
    seasons TSV and build_new_plans_json so every date in the repo parses one way."""
    s = (s or "").strip().strip('"')
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime.date(y, mo, d)
    except ValueError:
        return None


_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _fmt_display(d):
    return f"{d.day} {_MONTH_ABBR[d.month - 1]} {d.year}" if d else ""


def today():
    """date.today(), overridable with DFBNB_PIN_TODAY=YYYY-MM-DD for tests/preview."""
    env = (os.environ.get("DFBNB_PIN_TODAY") or "").strip()
    if env:
        try:
            return datetime.date.fromisoformat(env)
        except ValueError:
            pass
    return datetime.date.today()


_CHANNELS = {"live", "pts", "both"}


def load_pins(path=PINS_TSV):
    """[{plan_id, kind, route, bucket, start(date), end(date), channel, note, line}].

    Comment lines (`#`) and blanks are skipped; the header row is detected by its
    first cell being 'PlanId'. Malformed rows are collected and returned so the
    caller can warn rather than silently drop a pin. A blank Channel defaults to
    'live'. The Channel column is optional so a 7-column file still parses.
    """
    pins, errors = [], []
    if not os.path.exists(path):
        return pins, errors
    with open(path, encoding="utf-8", errors="replace") as f:
        for lineno, raw in enumerate(f, 1):
            if not raw.strip() or raw.lstrip().startswith("#"):
                continue
            cells = raw.rstrip("\n").split("\t")
            if cells and cells[0].strip() == "PlanId":
                continue  # header
            # PlanId, Kind, Route, Bucket, StartDate, EndDate, [Channel,] Note
            cells += [""] * (8 - len(cells))
            plan_id, kind, route, bucket, start_s, end_s, channel, note = \
                (c.strip() for c in cells[:8])
            kind = kind.lower()
            channel = (channel or "live").lower()
            start, end = _parse_dmy(start_s), _parse_dmy(end_s)
            problem = None
            if not plan_id:
                problem = "no PlanId"
            elif kind not in _ROUTE_KINDS and kind not in _SCALAR:
                problem = f"unknown Kind {kind!r}"
            elif kind in _ROUTE_KINDS and not route:
                problem = f"{kind} needs a Route"
            elif channel not in _CHANNELS:
                problem = f"unknown Channel {channel!r} (live|pts|both)"
            elif not start or not end:
                problem = "bad StartDate/EndDate (need D/M/YYYY)"
            elif end < start:
                problem = "EndDate before StartDate"
            if problem:
                errors.append((lineno, problem, raw.strip()))
                continue
            pins.append({"plan_id": plan_id, "kind": kind, "route": route,
                         "bucket": bucket, "start": start, "end": end,
                         "channel": channel, "note": note, "line": lineno})
    return pins, errors


def _channel_of(tsv_dir):
    """'pts' or 'live' from the export root — same rule reenrich uses."""
    if not tsv_dir:
        return "live"
    parts = os.path.normpath(tsv_dir).replace("\\", "/").split("/")
    return "pts" if "pts" in parts else "live"


def is_active(pin, day):
    return pin["start"] <= day <= pin["end"]


def _change_for(pin):
    """The `changes` dict a pin injects — same shape plan_changes.diff emits."""
    since = _fmt_display(pin["start"])
    if pin["kind"] in _ROUTE_KINDS:
        added = pin["kind"] == "route-added"
        text = plan_changes._phrase(
            "route-added" if added else "route-removed",
            "" if added else pin["route"],
            pin["route"] if added else "",
            pin["bucket"])
        return {"field": "routes", "kind": "added" if added else "removed",
                "route": pin["route"], "bucket": pin["bucket"],
                "text": text, "since": since, "pinned": True}
    field, old, new = _SCALAR[pin["kind"]]
    return {"field": field, "from": old, "to": new,
            "text": plan_changes._phrase(field, old, new),
            "since": since, "pinned": True}


def _already_present(changes, change):
    """Has the diff (or an earlier pin) already produced an equivalent change?"""
    for c in changes:
        if c.get("field") != change.get("field"):
            continue
        if change["field"] == "routes":
            if c.get("kind") == change.get("kind") and \
                    (c.get("route") or "") == (change.get("route") or ""):
                return True
        else:
            if c.get("to") == change.get("to"):
                return True
    return False


def apply(items, tsv_dir=None, path=PINS_TSV, day=None, channel=None):
    """Force-attach active pins' changes onto matching plan_master items.

    `channel` ("live"/"pts") gates which pins apply; when omitted it is derived
    from `tsv_dir` (the same live/pts rule reenrich uses). A pin whose Channel is
    "both" applies to either. This keeps a live-game change (e.g. an SDOW plan
    already in the live game) off the PTS "what's coming next" page. Pins are a
    pure data overlay otherwise — they read no export.
    """
    day = day or today()
    channel = channel or _channel_of(tsv_dir)
    pins, errors = load_pins(path)
    by_id = {(it.get("id") or "").upper(): it for it in items}

    stats = {"pins_total": len(pins), "channel": channel, "active": 0,
             "inert": 0, "off_channel": 0, "applied": 0, "deduped": 0,
             "missing_plan": [], "missing_route": [], "errors": errors}

    for pin in pins:
        if pin["channel"] != "both" and pin["channel"] != channel:
            stats["off_channel"] += 1
            continue
        if not is_active(pin, day):
            stats["inert"] += 1
            continue
        stats["active"] += 1
        it = by_id.get(pin["plan_id"].upper())
        if it is None:
            stats["missing_plan"].append(pin["plan_id"])
            continue
        change = _change_for(pin)
        changes = it.setdefault("changes", [])
        if _already_present(changes, change):
            stats["deduped"] += 1
            continue
        changes.append(change)
        stats["applied"] += 1
        # Tag the route line itself so How to Obtain can mark the new source,
        # exactly as plan_changes.diff() does. If the route is not on the item,
        # the pin still shows its Technical sentence but flag nothing, and we warn.
        if pin["kind"] == "route-added":
            hit = False
            for r in (it.get("obtain_routes") or []):
                if (r.get("route") or "").strip() == pin["route"]:
                    r["new"] = True
                    hit = True
            if not hit:
                stats["missing_route"].append((pin["plan_id"], pin["route"]))
    return stats


def report(stats, stream=sys.stderr):
    print(f"  pins[{stats.get('channel','?')}]: {stats['active']} active, "
          f"{stats['inert']} inert, {stats.get('off_channel',0)} off-channel "
          f"(of {stats['pins_total']}) — applied {stats['applied']}, "
          f"deduped {stats['deduped']}", file=stream)
    for lineno, why, raw in stats.get("errors", []):
        print(f"    *** pin line {lineno}: {why}", file=stream)
    for pid in stats.get("missing_plan", []):
        print(f"    *** pin PlanId not in plan_master: {pid}", file=stream)
    for pid, route in stats.get("missing_route", []):
        print(f"    *** pin route not on plan {pid}: {route!r} "
              f"(Technical sentence shown, route line not flagged)", file=stream)


# ── standalone ──────────────────────────────────────────────────────────────

def _cli(argv=None):
    ap = argparse.ArgumentParser(description="preview / lint New-Plans pins")
    ap.add_argument("--path", default=PINS_TSV)
    ap.add_argument("--today", default="", help="as-of date YYYY-MM-DD")
    ap.add_argument("--list", action="store_true", help="list every pin + status")
    ap.add_argument("--lint", default="", help="plan_master.json to resolve ids/routes against")
    args = ap.parse_args(argv)

    day = datetime.date.fromisoformat(args.today) if args.today else today()
    pins, errors = load_pins(args.path)
    for lineno, why, raw in errors:
        print(f"ERROR line {lineno}: {why}\n    {raw}")
    print(f"as of {day.isoformat()}: {len(pins)} pin(s)")
    if args.list:
        for p in pins:
            tag = "ACTIVE" if is_active(p, day) else "inert "
            extra = f" -> {p['route']} ({p['bucket']})" if p["route"] else ""
            print(f"  [{tag}] {p['channel']:4s} {p['plan_id']:16s} {p['kind']:14s}"
                  f" {p['start'].isoformat()}..{p['end'].isoformat()}{extra}")

    if args.lint:
        import json
        doc = json.load(open(args.lint, encoding="utf-8"))
        items = doc.get("items") or []
        by_id = {(it.get("id") or "").upper(): it for it in items}
        bad = 0
        for p in pins:
            it = by_id.get(p["plan_id"].upper())
            if it is None:
                bad += 1
                print(f"  MISSING id  {p['plan_id']}")
                continue
            if p["kind"] == "route-added":
                routes = [(r.get("route") or "") for r in (it.get("obtain_routes") or [])]
                if p["route"] not in routes:
                    bad += 1
                    print(f"  MISSING route on {p['plan_id']}: {p['route']!r}")
                    print(f"      has: {routes}")
        print("  lint OK" if not bad else f"  *** {bad} pin(s) do not resolve")
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
