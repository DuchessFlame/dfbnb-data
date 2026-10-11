#!/usr/bin/env python3
r"""
spawns_engine.instances — one place, one count: instanced "Dungeon" copies.

Some interiors exist twice in the game data: the normal cell and an instanced
copy of it, e.g.

    MorgantownHighSchool01       "Morgantown High School"
    MorgantownHighSchoolDungeon  "Morgantown High School"   (instanceable)

They are the SAME place to a player, just a different instance, so a spawn
that sits in both is ONE spawn, not two (Duchess, 11 Oct 2026). Mappalachia
lists both cells, which used to double every spawn inside them.

The copies are not always exact (Oct 2026 DB): Morgantown's dungeon copy
matches 5,208 of its 5,561 points to the main cell, and adds 353 of its own;
Glassed Cavern's copy is shifted, so none of its coordinates match, but it holds
the same objects. So the rule counts PER OBJECT, not per coordinate:

    inside a twin copy, a placement of object X only counts beyond the number
    of X placements in the main cell.

A twin is a space whose editorID ends "Dungeon" and which shares its display
name with a space whose editorID doesn't — derived from the Space table (or the
committed geo snapshot when the DB is absent), never typed. Pairs that merely
share a name but are different places (Vault 79 Main / Entrance, Watoga
Municipal Center 01 / 02) don't match the "Dungeon" rule and are left alone.
"""
from collections import defaultdict


def twin_map(spaces):
    """{twin spaceFormID: main spaceFormID} from {spaceFormID: (editorID, displayName)}."""
    by_name = defaultdict(list)
    for sid, (eid, name) in (spaces or {}).items():
        if name:
            by_name[name.strip().lower()].append((int(sid), eid or ""))
    out = {}
    for group in by_name.values():
        mains = [s for s, e in group if not e.lower().endswith("dungeon")]
        if not mains:
            continue
        for s, e in group:
            if e.lower().endswith("dungeon"):
                out[s] = mains[0]
    return out


def duplicate_instances(rows, twins):
    """rows: iterable of (inst, space, key). `key` is what makes two placements the
    same object (the placed base FormID, or a stand-in such as the source type).
    Returns the set of instanceFormIDs in twin copies that duplicate a main-cell
    placement and must not be counted."""
    if not twins:
        return set()
    main_n = defaultdict(int)          # (main space, key) -> count
    twin_rows = defaultdict(list)      # (twin space, key) -> [inst]
    for inst, space, key in rows:
        try:
            sp = int(space)
        except (TypeError, ValueError):
            continue
        if sp in twins:
            twin_rows[(sp, key)].append(inst)
        else:
            main_n[(sp, key)] += 1
    drop = set()
    for (sp, key), insts in twin_rows.items():
        n = main_n.get((twins[sp], key), 0)
        drop.update(sorted(insts)[:n])
    return drop
