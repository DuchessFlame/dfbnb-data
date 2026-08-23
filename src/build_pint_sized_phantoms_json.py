#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_pint_sized_phantoms_json.py
=================================
Builds: dist/pint_sized_phantoms.json

A small, standalone build for the single page
    /df/treasure-maps/pint-sized-phantoms/rewards/

Why this exists
---------------
The Pint-Sized Phantoms ("Secrets to the Grave", SDOW_*) reward data used to be
built as one block inside the giant ``build_treasure_maps_json.py`` run. That big
build resolves every treasure-map / U-Mine-It region and pulls huge TSV exports,
which makes it slow and memory-hungry. This script reads only the Slasher content
straight from the **PTS TSV exports** (``tsv/pts/``) and writes a dedicated
``pint_sized_phantoms.json`` for the one page, so it can be regenerated cheaply.

It reuses the already-verified rng76 drop-rate engine and the phantom-building
helpers in ``build_treasure_maps_json.py`` rather than duplicating that logic —
so the numbers stay identical to the main build, with one deliberate fix below.

The Skulls quantity-variant fix
-------------------------------
The Skulls list ``SDOW_LL_SQ01_Junk_Skull`` [008F863A] is a pick-one LVLI with
**five entries that are all the same item** (Skull, 000347E1) but at different
quantities and ChanceNone values:

    qty x5, ChanceNone 10  -> 1/5 * 0.90 = 18%
    qty x4, ChanceNone 20  -> 1/5 * 0.80 = 16%
    qty x3, ChanceNone 30  -> 1/5 * 0.70 = 14%
    qty x2, ChanceNone 40  -> 1/5 * 0.60 = 12%
    qty x1, ChanceNone 50  -> 1/5 * 0.50 = 10%
    (nothing: 30%)

rng76 already resolves those five rates correctly. The main build's
``aggregate_items()`` then collapsed them into a single row keyed only on FormID —
summing to one "Skull, 70%, qty 1-5" line and hiding the per-quantity odds.

The fix lives in ``build_treasure_maps_json._phantom_items``, which now uses
``aggregate_items_split_qty`` — keying on **(FormID, quantity)** so different
quantities of the same item stay as separate rows (genuine duplicates at the
same quantity still merge and sum), and dropping qty-0 blank leaves. The result:
the Skulls pool shows five rows (x5 @ 18%, x4 @ 16%, x3 @ 14%, x2 @ 12%,
x1 @ 10%). This script just drives that build against the PTS TSV — no
monkeypatch, no re-implemented formula, rng76.py untouched.

NOTE (standalone-copy rule): only display aggregation changed, not any drop-rate
formula, so the two engines stay consistent. The Event-Reward standalone build
(``build_activities_rewards_json.py``) uses a FormID-keyed model (compute_lvli)
that carries no quantities and does not build this page, so there is nothing to
mirror there.

Usage:
    python build_pint_sized_phantoms_json.py
"""

import glob
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_SRC = Path(__file__).resolve().parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Reuse the verified engine + phantom-building helpers.
import build_treasure_maps_json as btm
from rng76 import Rng76Data
import tsv_source          # one resolver for every export selection

_REPO_ROOT = _SRC.parent
DIST_DIR = _REPO_ROOT / "dist"
PTS_TSV_DIR = _REPO_ROOT / "tsv" / "pts"


def main():
    print("[build_pint_sized_phantoms_json.py] Starting build...")

    if not PTS_TSV_DIR.is_dir():
        raise FileNotFoundError(f"PTS TSV directory not found: {PTS_TSV_DIR}")

    # Point every btm.newest()/read_tsv lookup at the PTS exports.
    btm.TSV_DIR = PTS_TSV_DIR

    book_path = btm.newest("BOOK_Export_*.tsv", exclude_substrings=["Locations"])
    print(f"  BOOK: {os.path.basename(book_path)}")
    books = btm.BookLookup(btm.read_tsv(book_path))

    print("  Loading rng76 engine from PTS TSV...")
    rng_data = Rng76Data.from_tsv_root(str(PTS_TSV_DIR))
    resolver = rng_data.resolver

    # The PTS export ships CURV headers but omits the CURV *_POINTS* file, so
    # curve interpolation (e.g. the Tier16 XP curve 00876404) would return 0.
    # XP curves are static universal game data, so supplement the point data
    # from the main tsv/ export as a fallback.
    if not rng_data.curvs.points:
        main_points = tsv_source.all_matching(
            str(_REPO_ROOT / "tsv" / "CURV_Export_*_POINTS.tsv"))
        if main_points:
            print(f"  CURV points fallback: {os.path.basename(main_points[-1])}")
            rng_data.curvs.load_points(main_points[-1])

    # The Skulls/Throwing-Knives quantity split is now built into
    # build_treasure_maps_json._phantom_items (via aggregate_items_split_qty),
    # so no monkeypatch is needed here — build_pint_sized_phantoms picks it up.

    print("  Building Pint-Sized Phantoms block...")
    phantoms = btm.build_pint_sized_phantoms(resolver, books)
    if not phantoms:
        raise SystemExit(
            "    Slasher map BOOK (008F15E4) not found in the PTS export — "
            "nothing to build."
        )

    phantoms["meta"] = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generator": "build_pint_sized_phantoms_json.py",
        "source": "PTS TSV exports (tsv/pts/)",
        "source_files": [os.path.basename(book_path)],
        "notes": [
            "Drop rates resolved via rng76 engine (deep LVLI flattening)",
            "Skulls pool split per quantity — SDOW_LL_SQ01_Junk_Skull [008F863A] "
            "is a pick-one of x5/x4/x3/x2/x1 Skull at 18/16/14/12/10%",
            "Same item at different quantities is kept as separate rows "
            "((FormID, quantity) aggregation key); qty-0 blank leaves dropped",
            "GMRW conditions NOT baked in — handled by website JS",
        ],
    }

    os.makedirs(str(DIST_DIR), exist_ok=True)
    out_path = DIST_DIR / "pint_sized_phantoms.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(phantoms, f, indent=2, ensure_ascii=False)

    pools = phantoms["repeatable_rewards"]["pools"]
    total_items = sum(p.get("item_count", 0) for p in pools)
    print(f"\n  Output: {out_path}")
    print(f"  Map: {phantoms['map']['name']}")
    print(f"  XP L50 {phantoms['experience']['xp_level_50']}, "
          f"Caps {phantoms['caps']['amount']}")
    print(f"  Repeatable pools: {len(pools)} ({total_items} items), "
          f"{len(phantoms['quest_related']['branches'])} journal branches")
    for p in pools:
        if p.get("name") == "Skulls":
            rows = ", ".join(f"x{it['qty']} {it['drop_rate']}" for it in p["items"])
            print(f"  Skulls pool ({p['fire_rate_pct']} fire): {rows}")
    print("[build_pint_sized_phantoms_json.py] Done.")


if __name__ == "__main__":
    main()
