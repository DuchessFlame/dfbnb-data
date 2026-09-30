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


def workshop_markers(db):
    """Map-marker labels that are public workshops (PublicWorkshopMarker icon in
    Mappalachia's MapMarker table). A workshop only spawns its attackers while you
    first clear it, so it is left off the looping routes."""
    con = sqlite3.connect("file:{}?mode=ro".format(db), uri=True)
    rows = con.execute("SELECT label FROM MapMarker WHERE spaceFormID = ? AND icon = ?",
                       [APPALACHIA_SPACE, "PublicWorkshopMarker"]).fetchall()
    con.close()
    return sorted({r[0] for r in rows})


def page_regions(clusters, geo, all_regions, regions, workshops=()):
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
            loc = {"marker": m, "spawns": sum(c["count"] for c in cs),
                   "map": [{"region": c["region"], "n": c["n"]}
                           for c in sorted(cs, key=lambda c: c["n"])]}
            xy = geo.marker_xy.get(m)
            if xy:   # map-marker position - the page's route builder orders by it
                loc["x"], loc["y"] = round(xy[0]), round(xy[1])
            if m in workshops:
                loc["workshop"] = True
            locs.append(loc)
        locs.sort(key=lambda l: (-l["spawns"], l["marker"]))
        entry = {"region": r, "total": sum(l["spawns"] for l in locs), "locations": locs,
                 "tile": (re.sub(r"[^a-z0-9]+", "-", r.lower()).strip("-") + "-spawn-map.jpg")
                 if locs else None}
        if not locs:
            entry["reason"] = "expedition" if r in EXPEDITION_REGIONS else (
                "not-event-region" if r not in regions else "no-spawns")
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Suggested farming route (the farming-map page's "Suggested Route" section and
# the route map render_event_swap_maps.py draws). Every location with at least
# ROUTE_MIN_SPAWNS spawn points (workshops left out), region by region in ROUTE_REGIONS order - a loop
# that starts at the top of the map in the Toxic Valley, runs down the Forest,
# across the Ash Heap, up the east side (Cranberry Bog, The Mire) and back
# through the Savage Divide (Duchess, 30 Sep 2026). Inside a region the stops are
# ordered by distance between map-marker icons (nearest neighbour, then 2-opt),
# entering near the last stop of the previous region and leaving towards the next
# region, so the whole route is one line on the map. Stop numbers run 1..N across
# the whole route and match the route map.
# ---------------------------------------------------------------------------
ROUTE_MIN_SPAWNS = 10
ROUTE_REGIONS = ("Toxic Valley", "Forest", "Ash Heap", "Cranberry Bog", "The Mire", "Savage Divide")
# Line/stop colour per region on the route map; the page uses the same colours
# for the swatch beside each region's heading, so map and list agree.
ROUTE_COLOURS = {
    "Toxic Valley": (198, 255, 0),     # lime
    "Forest": (0, 229, 255),           # cyan
    "Ash Heap": (255, 109, 0),         # orange
    "Cranberry Bog": (255, 64, 129),   # pink
    "The Mire": (179, 136, 255),       # lavender
    "Savage Divide": (255, 234, 0),    # yellow
}


def _dist(a, b):
    return ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2) ** 0.5


def _open_path(stops, start_xy, end_xy):
    """Order stops as a path from start_xy to end_xy (both fixed, not stops)."""
    left = list(stops)
    path, cur = [], start_xy
    while left:
        nxt = min(left, key=lambda s: (_dist(cur, s["xy"]), s["marker"]))
        left.remove(nxt)
        path.append(nxt)
        cur = nxt["xy"]
    pts = [start_xy] + [s["xy"] for s in path] + [end_xy]
    improved = True
    while improved:
        improved = False
        for i in range(1, len(pts) - 2):
            for j in range(i + 1, len(pts) - 1):
                if (_dist(pts[i - 1], pts[j]) + _dist(pts[i], pts[j + 1])
                        < _dist(pts[i - 1], pts[i]) + _dist(pts[j], pts[j + 1]) - 1e-6):
                    pts[i:j + 1] = pts[i:j + 1][::-1]
                    path[i - 1:j] = path[i - 1:j][::-1]
                    improved = True
    return path


def route(page_regions_list, geo, min_spawns=ROUTE_MIN_SPAWNS, order=ROUTE_REGIONS):
    """[{region, stops: [{n, marker, spawns, x, y}]}] - see the block comment above."""
    by = {r["region"]: r for r in page_regions_list}
    legs = []
    for name in order:
        r = by.get(name)
        stops = []
        for l in (r or {}).get("locations") or []:
            if l.get("workshop"):
                continue   # workshops don't respawn once cleared - no use on a loop
            xy = geo.marker_xy.get(l["marker"])
            if l["spawns"] >= min_spawns and xy:
                stops.append({"marker": l["marker"], "spawns": l["spawns"], "xy": xy})
            elif l["spawns"] >= min_spawns:
                print("  [WARN] event_swap_spawns.route: no map marker for {} - left off the route"
                      .format(l["marker"]))
        if stops:
            legs.append((name, stops))

    def centre(stops):
        return (sum(s["xy"][0] for s in stops) / len(stops), sum(s["xy"][1] for s in stops) / len(stops))

    out, n, prev_end = [], 0, None
    first_start = None
    for i, (name, stops) in enumerate(legs):
        if prev_end is None:
            # Start at the top of the map (the northernmost stop of the first region).
            top = max(stops, key=lambda s: s["xy"][1])
            start = (top["xy"][0], top["xy"][1] + 1)
            first_start = top["xy"]
        else:
            start = prev_end
        end = centre(legs[i + 1][1]) if i + 1 < len(legs) else (first_start or centre(stops))
        path = _open_path(stops, start, end)
        rows = []
        for s in path:
            n += 1
            rows.append({"n": n, "marker": s["marker"], "spawns": s["spawns"],
                         "x": round(s["xy"][0], 1), "y": round(s["xy"][1], 1)})
        c = ROUTE_COLOURS.get(name, (255, 255, 255))
        out.append({"region": name, "colour": "#{:02X}{:02X}{:02X}".format(*c), "stops": rows})
        prev_end = path[-1]["xy"]
    return out


# ---------------------------------------------------------------------------
# Recommended N-stop loop (the farming-map page's "Recommended 20-Stop Route").
# Pick the N locations that give the most enemy spawn points for the least
# travel: maximise  spawns - RECOMMENDED_LAMBDA * loop length / 10,000 units,
# workshops excluded. Search: seed with the N stops nearest each busy location
# (weighted towards busy ones), then swap stops in/out while the score improves;
# keep the best seed. The loop is ordered by nearest neighbour + 2-opt and
# starts at its northernmost stop. The stats compare it with simply taking the
# N busiest locations anywhere, so the page can say what the trade-off buys.
# ---------------------------------------------------------------------------
RECOMMENDED_STOPS = 20
RECOMMENDED_LAMBDA = 2.0
_UNIT = 10000.0


def _loop_len_nn(pts):
    cur, left, L = pts[0], list(pts[1:]), 0.0
    while left:
        j = min(left, key=lambda q: _dist(cur, q))
        L += _dist(cur, j)
        cur = j
        left.remove(j)
    return L + _dist(cur, pts[0])


def _loop(stops):
    """Closed loop (NN + 2-opt); returns (length, ordered stops)."""
    left = list(stops[1:])
    path = [stops[0]]
    while left:
        nxt = min(left, key=lambda s: (_dist(path[-1]["xy"], s["xy"]), s["marker"]))
        left.remove(nxt)
        path.append(nxt)
    n = len(path)
    improved = True
    while improved:
        improved = False
        for i in range(1, n - 1):
            for j in range(i + 1, n):
                a, b = path[i - 1]["xy"], path[i]["xy"]
                c, d = path[j]["xy"], path[(j + 1) % n]["xy"]
                if _dist(a, c) + _dist(b, d) < _dist(a, b) + _dist(c, d) - 1e-6:
                    path[i:j + 1] = path[i:j + 1][::-1]
                    improved = True
    L = sum(_dist(path[i]["xy"], path[(i + 1) % n]["xy"]) for i in range(n))
    return L, path


def recommended_route(page_regions_list, n=RECOMMENDED_STOPS, lam=RECOMMENDED_LAMBDA):
    cands = [{"marker": l["marker"], "spawns": l["spawns"], "region": r["region"],
              "xy": (l["x"], l["y"])}
             for r in page_regions_list for l in r.get("locations") or []
             if "x" in l and not l.get("workshop")]
    if len(cands) < n:
        return None
    cands.sort(key=lambda c: (-c["spawns"], c["marker"]))

    def score(S):
        return sum(c["spawns"] for c in S) - lam * _loop_len_nn([c["xy"] for c in S]) / _UNIT

    pool_all = cands[:max(40, n * 2)]
    best = None
    for seed in [c for c in cands if c["spawns"] >= 20]:
        S = sorted(cands, key=lambda c: (_dist(c["xy"], seed["xy"]) - 3000 * c["spawns"], c["marker"]))[:n]
        cur = score(S)
        improved = True
        while improved:
            improved = False
            pool = [c for c in pool_all if c not in S]
            for i in range(n):
                for q in pool:
                    T = S[:i] + [q] + S[i + 1:]
                    v = score(T)
                    if v > cur + 1e-6:
                        S, cur, improved = T, v, True
                        break
                if improved:
                    break
        if best is None or cur > best[0] + 1e-6:
            best = (cur, S)
    S = best[1]
    L, path = _loop(sorted(S, key=lambda c: (-c["xy"][1], c["marker"])))
    top = cands[:n]
    topL, _ = _loop(top)
    stops = [{"n": i, "marker": c["marker"], "spawns": c["spawns"], "region": c["region"],
              "colour": "#{:02X}{:02X}{:02X}".format(*ROUTE_COLOURS.get(c["region"], (255, 255, 255))),
              "x": round(c["xy"][0]), "y": round(c["xy"][1])}
             for i, c in enumerate(path, 1)]
    return {"stops": stops,
            "spawns": sum(c["spawns"] for c in S),
            "loop": round(L / _UNIT, 1),
            "busiest": {"spawns": sum(c["spawns"] for c in top), "loop": round(topL / _UNIT, 1)},
            "regions": sorted({c["region"] for c in S})}


def legs_of(stops):
    """Group a stop list into consecutive same-region legs (for render_route)."""
    legs = []
    for s in stops:
        if not legs or legs[-1]["region"] != s["region"]:
            legs.append({"region": s["region"], "stops": []})
        legs[-1]["stops"].append(s)
    return legs
