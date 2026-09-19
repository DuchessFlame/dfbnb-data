#!/usr/bin/env python3
"""
build_make_plan_checklist_json.py
---------------------------------
Data feed for /df/plan-checklists/make-your-own/ — the "Make Your Own Plan
Checklist" builder page. The user searches / filters the whole plan roster,
ticks what they own, and adds rows to a personal list they can print or
download.

WHY THIS PAGE NEEDS ITS OWN (TINY) BUILD
----------------------------------------
The builder page renders a FLAT list of every learnable plan — no root
expands, no sub-expands, no drop-rate maths, no images. It only needs, per
plan:

    name · type · tradeable status · which How-to-Obtain routes apply

All of that already exists, fully resolved, in dist/plan_master.json (the
obtain ledger + the tradeable flag from the plan-obtain pipeline). This build
does NOT re-derive any of it — it copies those resolved values verbatim into a
compact per-plan record so the page ships a small file instead of parsing the
full multi-megabyte plan_master on the client.

INCLUDE-ALL POLICY (deliberate)
-------------------------------
Every plan in plan_master is included — nothing is excluded here:
  * cut / unobtainable plans are kept and flagged `cut: true`
  * ATX-tagged plans (an `ATX_` EditorID on the plan item / COBJ / created
    object) are kept and flagged `atx: true`
  * source_tag="Atom" plans are kept as-is
The page exposes Cut and ATX as filter chips so they can be found and pruned
from the UI later. There is no Atom-Shop-cosmetic set to strip: those (skins,
paints) live in atom_shop.json and are not in plan_master at all.

This is generative — the roster is whatever plan_master contains, resolved by
walking every item. No hardcoded plan list, no FormIDs, no per-item numbers.

Two modes, same shape as every other twin-channel builder:
  (default / live)  reads dist/plan_master.json      -> dist/make_plan_checklist.json
  --pts             reads dist/pts/plan_master.json   -> dist/pts/make_plan_checklist.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the renderer loads the right twin automatically.

Usage:
    python3 src/build_make_plan_checklist_json.py
    python3 src/build_make_plan_checklist_json.py --pts
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(SCRIPT_DIR)
PTS = "--pts" in sys.argv
DIST_DIR = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
PLAN_MASTER = os.path.join(DIST_DIR, "plan_master.json")
OUT = os.path.join(DIST_DIR, "make_plan_checklist.json")

SCHEMA = 1

# Canonical How-to-Obtain route order. This MUST match OBTAIN_LEDGER_ROWS in
# df-bnb-plan-checklists.js / df-bnb-make-plan-checklist.js so the filter chips
# and pills read in the same order everywhere. The renderer only shows chips
# for routes that actually occur, but the ORDER is anchored here.
LEDGER_ROWS = [
    "Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
    "Limited Time Bundle", "Containers", "Scrap to Learn",
    "Events & Activities", "Quests", "Challenges",
]

# Friendly labels for the type filter, in display order.
TYPE_ORDER = ["weapon", "armour", "apparel", "backpack-mod", "recipe"]
TYPE_LABELS = {
    "weapon": "Weapon",
    "armour": "Armour",
    "apparel": "Apparel",
    "backpack-mod": "Backpack Mod",
    "recipe": "Recipe",
}


def applied_routes(item):
    """The ledger labels that actually apply to this plan, in canonical order.

    `applies: false` is the old shape's way of saying N/A; the current builder
    just leaves the row out. We copy this verbatim — no re-derivation."""
    present = set()
    for r in (item.get("obtain_ledger") or []):
        if isinstance(r, dict) and r.get("applies") is not False and r.get("label"):
            present.add(r["label"])
    return [lbl for lbl in LEDGER_ROWS if lbl in present]


def is_atx(item):
    """True when any resolved EditorID on the plan carries the ATX_ prefix.

    ATX_ is Bethesda's atom-content marker. NOTE: several ATX_ plans are now
    obtainable via Caps / Events, so this is a *marker for pruning*, not an
    exclusion — the include-all policy keeps them."""
    for key in ("plan_item", "cobj", "cnam"):
        v = item.get(key)
        if isinstance(v, dict):
            ed = (v.get("edid") or "").upper()
            if ed.startswith("ATX_"):
                return True
    return False


def compact(item):
    name = item.get("display_name") or item.get("name") or item.get("id")
    return {
        "id": item.get("id"),
        "name": name,
        "type": item.get("type") or "recipe",
        # true / false / null — copied straight from the pipeline's plan-wide flag.
        "tradeable": item.get("tradeable"),
        "routes": applied_routes(item),
        "cut": bool(item.get("cut")),
        "atx": is_atx(item),
    }


def build(dist_dir=None):
    """Write <dist_dir>/make_plan_checklist.json from that dir's plan_master.

    `dist_dir` exists so reenrich_plan_master.py can run this per channel right
    after it finishes a plan_master, which is the only way this file cannot go
    stale: every value in it is copied verbatim out of plan_master, so the page
    is wrong the moment plan_master moves and nothing reruns this. It did go
    stale — on 20 Sept 2026 the page was still serving 901 "Unknown" tradeable
    plans that the roster had already resolved. Defaults to the module-level
    DIST_DIR so `python3 src/build_make_plan_checklist_json.py [--pts]` is
    unchanged.
    """
    dist_dir = dist_dir or DIST_DIR
    plan_master = os.path.join(dist_dir, "plan_master.json")
    out = os.path.join(dist_dir, "make_plan_checklist.json")
    with open(plan_master, encoding="utf-8") as fh:
        master = json.load(fh)
    src_items = master.get("items") or []

    items = [compact(it) for it in src_items]
    # Stable, human-friendly default order: name A–Z.
    items.sort(key=lambda r: (r["name"] or "").lower())

    # The subset of routes / types that actually occur, in canonical order, so
    # the page can build its filter chips without shipping empties.
    routes_present = set()
    types_present = set()
    for r in items:
        routes_present.update(r["routes"])
        types_present.add(r["type"])

    filters = {
        "routes": [lbl for lbl in LEDGER_ROWS if lbl in routes_present],
        "types": [
            {"key": t, "label": TYPE_LABELS.get(t, t.title())}
            for t in TYPE_ORDER if t in types_present
        ],
    }

    doc = {
        "schema": SCHEMA,
        "version": master.get("version"),
        "generated": datetime.now(timezone.utc).isoformat(),
        "source": "plan_master.json",
        "count": len(items),
        "cut_count": sum(1 for r in items if r["cut"]),
        "atx_count": sum(1 for r in items if r["atx"]),
        "route_order": LEDGER_ROWS,
        "filters": filters,
        "items": items,
    }

    os.makedirs(dist_dir, exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

    print(f"[make-plan-checklist] wrote {out} — {doc['count']} plans "
          f"({doc['cut_count']} cut, {doc['atx_count']} ATX-tagged); "
          f"routes={filters['routes']}")


if __name__ == "__main__":
    build()
