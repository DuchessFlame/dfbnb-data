#!/usr/bin/env python3
r"""
event_swap_spawns.py - where the seasonal "swap-in" enemies can appear.

Treasure Hunters, Pint-Sized Phantoms and the Mothman Equinox Cultist High
Priest have no spawn points of their own. LChar_MainAll [0051C55B] is a
first-match list whose event entry (LChar_Event_Actors [005A7559]) is taken when
the event's GetRandomPercent roll passes AND the actor is outdoors
(IsInInterior == 0) AND it is in an event region (the EventActor_Regions CNDF)
or EventActor_GlobalSpawn is on. So the pool is every OUTDOOR placed actor whose
NPC_ record templates off LChar_MainAll, inside an event region.

Everything here comes from the game files:
  * the actor bases  - NPC export rows with TPLT_EDID == LChar_MainAll
  * the event regions - CNDF EventActor_Regions -> its region CNDFs -> the
                        LocRegion* keyword each one tests
  * the placements   - Mappalachia Position rows in the Appalachia worldspace

Used by render_event_swap_maps.py (the maps) and build_seasonal_events_json.py
(the Treasure Hunter Farming Map page), so the page's numbers always match the
dots. The page builder reads a committed cache when the Mappalachia DB isn't
there (CI); run the builder locally with the DB to refresh it.
"""
import collections
import csv
import json
import os
import re
import sqlite3

import tsv_source

APPALACHIA_SPACE = 2480661
# File stem of the rendered maps (render_event_swap_maps.py SETS key); the page
# links <stem>.jpg (4K) and <region>-spawn-map.jpg from guide-images/seasonal-events/treasure-hunters/.
MAP_SLUG = "event-swap-treasure-hunters-phantoms-high-priests"
POOL_TEMPLATE = "LChar_MainAll"
EVENT_REGIONS_CNDF = "EventActor_Regions"

# LocRegion* keyword -> the region name the site uses. These are Bethesda's
# internal region keywords; they carry no display name in the KYWD export.
LOCREGION_NAMES = {
    "LocRegionForestFloodlands": "Forest",
    "LocRegionMTR": "Ash Heap",
    "LocRegionMountain": "Savage Divide",
    "LocRegionWhitespring": "Savage Divide",   # the Whitespring sits in the Savage Divide
    "LocRegionSwampForest": "The Mire",
    "LocRegionCranberryBog": "Cranberry Bog",
    "LocRegionToxicValley": "Toxic Valley",
    "LocRegionStorm": "Skyline Valley",
    "LocRegionBurn": "Burning Springs",
}
EXPEDITION_REGIONS = ("Atlantic City", "The Pitt")

# Border markers whose icon polygon disagrees with the in-game map region.
# Everything else is filed under the region its map-marker icon sits in
# (Duchess, 30 Sep 2026: Fort Defiance = Cranberry Bog, Striker Row = Ash Heap).
MARKER_REGION_FIXES = {
    "Garrahan Mining Headquarters": "Ash Heap",   # icon falls just inside the Savage Divide polygon
}

_CNDF_REF_RE = re.compile(r"\|IsTrueForConditionForm\|.*?\|([A-Za-z0-9_]+) \[CNDF:")
_KYWD_RE = re.compile(r"\|EditorLocationHasKeyword\|.*?\|([A-Za-z0-9_]+) \[KYWD:")


def pool_npcs(channel="live"):
    """NPC_ EDIDs that template off LChar_MainAll (LvlMainMelee, LvlMainBoss ...)."""
    path = tsv_source.newest(os.path.join("NPC_Export_*.tsv"), channel=channel,
                             exclude=["_Refs", "_PRPS"])
    out = []
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t")
        head = next(rd)
        ie, it = head.index("EDID"), head.index("TPLT_EDID")
        for row in rd:
            if len(row) > it and row[it] == POOL_TEMPLATE:
                out.append(row[ie])
    return sorted(set(out))


def event_regions(channel="live"):
    """Region names the EventActor_Regions CNDF lets the swap happen in."""
    path = tsv_source.newest("CNDF_Export_*.tsv", channel=channel)
    rows = {}
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t")
        next(rd)
        for row in rd:
            if len(row) > 1:
                rows[row[1]] = row
    top = rows.get(EVENT_REGIONS_CNDF)
    if not top:
        return []
    names = []
    for child in _CNDF_REF_RE.findall("\t".join(top)):
        for kw in _KYWD_RE.findall("\t".join(rows.get(child, []))):
            name = LOCREGION_NAMES.get(kw)
            if name is None:
                print("  [WARN] event_swap_spawns: unknown region keyword {} - add it to "
                      "LOCREGION_NAMES".format(kw))
            elif name not in names:
                names.append(name)
    return names


def pool_points(db, geo, npcs, regions):
    """Outdoor Appalachia placements of the pool actors, inside the event regions."""
    con = sqlite3.connect("file:{}?mode=ro".format(db), uri=True)
    q = ("SELECT p.instanceFormID, p.x, p.y, e.editorID FROM Position p "
         "JOIN Entity e ON e.entityFormID = p.referenceFormID "
         "WHERE p.spaceFormID = ? AND e.signature = 'NPC_' AND e.editorID IN (%s)"
         % ",".join("?" * len(npcs)))
    rows = con.execute(q, [APPALACHIA_SPACE] + list(npcs)).fetchall()
    con.close()
    keep = set(regions)
    out, seen = [], set()
    for inst, x, y, eid in rows:
        if inst in seen:
            continue
        seen.add(inst)
        region, marker, _ = geo.resolve(APPALACHIA_SPACE, x, y)
        if region not in keep:
            continue
        out.append({"ref": "{:08X}".format(inst), "x": x, "y": y, "space": APPALACHIA_SPACE,
                    "region": region or "Unknown", "marker": marker or "Unmarked",
                    "source_type": "event-swap", "base": eid})
    return out


def numbered(points):
    """(region, marker) clusters numbered exactly as render_spawn_maps.cluster_by_marker
    numbers the dots (sort by region, marker, source type; 1-based)."""
    groups = collections.OrderedDict()
    for p in points:
        groups.setdefault((p["region"], p["marker"], p.get("source_type", "")), []).append(p["ref"])
    keys = sorted(groups)
    return [{"n": i, "region": k[0], "marker": k[1], "count": len(groups[k]), "refs": groups[k]}
            for i, k in enumerate(keys, 1)]


def page_regions(clusters, geo, all_regions, regions):
    """The farming-map page's region lists: one entry per region (A-Z), every
    location filed under the region its marker icon sits in, sorted by spawn
    points, carrying the dot number(s) on each region's map."""
    by_marker = collections.defaultdict(list)
    for c in clusters:
        by_marker[c["marker"]].append(c)
    home = {}
    for m, cs in by_marker.items():
        tally = collections.Counter()
        for c in cs:
            tally[c["region"]] += c["count"]
        home[m] = (MARKER_REGION_FIXES.get(m) or geo._region_of_marker_label(m)
                   or tally.most_common(1)[0][0])
    out = []
    for r in sorted(all_regions):
        locs = []
        for m, cs in by_marker.items():
            if home[m] != r:
                continue
            locs.append({"marker": m, "spawns": sum(c["count"] for c in cs),
                         "map": [{"region": c["region"], "n": c["n"]}
                                 for c in sorted(cs, key=lambda c: c["n"])]})
        locs.sort(key=lambda l: (-l["spawns"], l["marker"]))
        entry = {"region": r, "total": sum(l["spawns"] for l in locs), "locations": locs,
                 "tile": (re.sub(r"[^a-z0-9]+", "-", r.lower()).strip("-") + "-spawn-map.jpg")
                 if locs else None}
        if not locs:
            entry["reason"] = "expedition" if r in EXPEDITION_REGIONS else (
                "not-event-region" if r not in regions else "no-spawns")
        out.append(entry)
    return out
