#!/usr/bin/env python3
r"""
chem_loot_collapse.py — Fixed Spawn Locations cleanup for the farming spawn docs.

Two jobs, run AFTER build_farming_used_for (so the Containers type list is already
attached by _patch_containers):

1. UNIVERSAL — drop `container` and `loot-list` placements from Fixed Spawn
   Locations on EVERY farming doc. Those container placements are now represented,
   with real rng76 drop rates, in the Containers expand (container TYPE -> rate),
   so listing thousands of individual container map points is redundant and blows
   the page up. Fixed Spawn keeps the genuine, returnable world points: `direct`,
   `static`, `nest`, plus creature spots (`npc`) and any `vendor` / `quest-reward`.

2. CHEMS ONLY — a chem is generic medical loot from creatures too (a stimpak can
   roll from a feral ghoul's junk), not a huntable source, so for chem docs the
   `npc` placements are also folded into a note-only Creatures expand instead of
   thousands of map markers. Meat / egg / non-perishable docs KEEP their `npc`
   spots (there the creature IS the source), so only Containers change for them.

Idempotent. Wired into build_farming_used_for.main() to run after the used_for
join, so a CI rebuild re-applies it automatically. Container display + rates come
entirely from _patch_containers (rng76); nothing here invents a rate.
"""
import json
import os
import sys
from collections import Counter

CONTAINER_TYPES = {"container", "loot-list"}   # now shown as Containers type+rate
CHEM_ALSO_DROP = {"npc"}                        # chems: generic creature loot -> note


def _total(doc):
    return sum(l["count"] for r in doc.get("regions", []) for l in r.get("locations", []))


def _is_chem(doc):
    ft = doc.get("farming_tips") or {}
    return ft.get("object_type") == "Chem"


def collapse(doc):
    """Strip redundant container (+chem creature) placements from Fixed Spawn.
    Returns (before_total, after_total)."""
    before = _total(doc)
    drop_types = set(CONTAINER_TYPES)
    is_chem = _is_chem(doc)
    if is_chem:
        drop_types |= CHEM_ALSO_DROP
    dropped = Counter()
    for region in doc.get("regions", []):
        kept_locs = []
        for loc in region.get("locations", []):
            spawns = loc.get("spawns") or []
            kept = [s for s in spawns if s.get("source_type") not in drop_types]
            for s in spawns:
                st = s.get("source_type")
                if st in drop_types:
                    dropped[st] += 1
            if not kept:
                continue
            loc["spawns"] = kept
            loc["count"] = len(kept)
            loc["sources"] = dict(Counter(s.get("source_type") for s in kept))
            if isinstance(loc.get("breakdown"), list):
                loc["breakdown"] = [b for b in loc["breakdown"]
                                    if (b.get("source_type") or b.get("type")) not in drop_types]
            kept_locs.append(loc)
        region["locations"] = kept_locs
    doc["total"] = _total(doc)

    # Chems: note-only Creatures expand summarising the folded generic creature loot.
    if is_chem:
        dr = doc.get("drop_rates")
        if not isinstance(dr, dict):
            dr = {}
            doc["drop_rates"] = dr
        name = doc.get("name") or "This chem"
        npc_n = dropped.get("npc", 0)
        if npc_n:
            dr["creatures"] = {"note": (
                f"Around {npc_n:,} creatures can drop {name} as generic loot; they are "
                "not listed individually. See the Containers expand for the per-container "
                "drop rates and How to Obtain for the crafting recipe.")}
    return before, doc["total"]


def run(dist_dir="dist"):
    root = os.path.join(dist_dir, "farming_spawns")
    if not os.path.isdir(root):
        print(f"[chem_loot_collapse] no {root}")
        return
    changed = 0
    for fn in sorted(os.listdir(root)):
        if not fn.endswith("_spawns.json"):
            continue
        p = os.path.join(root, fn)
        try:
            doc = json.load(open(p, encoding="utf-8"))
        except Exception:
            continue
        before, after = collapse(doc)
        if after != before:
            json.dump(doc, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
            changed += 1
    print(f"[chem_loot_collapse] stripped container/creature placements from "
          f"{changed} farming docs (dist={dist_dir})")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "dist")
