#!/usr/bin/env python3
r"""
spawns_engine.build — the shared build core for every "{Item} Spawn Locations"
page: Mappalachia Position lookups, geo-cache IO, hand-authored field
preservation, placement resolution, and region/marker grouping.

Coordinates live only in the local Mappalachia Position table (~480 MB, not in
CI). A LOCAL run with the DB resolves every placement and writes a committed
geo cache; every later run WITHOUT the DB rebuilds straight from that cache plus
the committed TSVs — fully automated.

Each item FAMILY (drinks, farming/eggs) supplies its own thin driver
(spawns_configs.*) that calls get_sources() with its classifier, then these
helpers, then assembles its exact output doc. The heavy lifting lives here once.
"""

import os, json
from collections import defaultdict

SQL_CHUNK = 900  # stay under SQLite's 999-variable limit


def _chunks(seq, n=SQL_CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ── DB queries ───────────────────────────────────────────────────────────────
def pull_by_base(cur, base_ints):
    out = []
    for chunk in _chunks(base_ints):
        q = ("SELECT x, y, z, instanceFormID, spaceFormID, referenceFormID FROM Position "
             "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
        out.extend(cur.execute(q, tuple(chunk)).fetchall())
    return out


def pull_by_instance(cur, inst_ints):
    out = []
    for chunk in _chunks(inst_ints):
        q = ("SELECT x, y, z, instanceFormID, spaceFormID FROM Position "
             "WHERE instanceFormID IN (%s)" % ",".join("?" * len(chunk)))
        out.extend(cur.execute(q, tuple(chunk)).fetchall())
    return out


# ── geo cache ────────────────────────────────────────────────────────────────
def load_cache(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(cache, open(path, "w", encoding="utf-8"), ensure_ascii=False)


# ── hand-authored field preservation ─────────────────────────────────────────
def load_existing(path):
    """Load hand-authored fields from the previous build to preserve on rebuild.

    Marker-level slots are keyed (region, marker); PER-SPAWN slots are keyed by the
    placement's ref inside `spawns`, so a marker holding several spawns keeps each
    one's own photos/directions even as the placement list grows or reorders."""
    keep = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in old.get("regions", []):
        for loc in reg.get("locations", []):
            spawns = {}
            for sp in loc.get("spawns") or []:
                ref = sp.get("ref") or ""
                saved = {k: sp.get(k, "") for k in ("image_top", "directions", "image_bottom")
                         if sp.get(k)}
                if ref and saved:
                    spawns[ref] = saved
            keep[(reg.get("region", ""), loc.get("marker", ""))] = {
                "image_top": loc.get("image_top", ""),
                "directions": loc.get("directions", ""),
                "image_bottom": loc.get("image_bottom", ""),
                "spawns": spawns,
            }
    return keep


# ── placement resolution ─────────────────────────────────────────────────────
def resolve_placements(src, geo, cur, cache, db_ok):
    """Given a get_sources() result, return
        (seen: {instanceFormID: (x, y, region, marker, source_type)}, lists_n).

    DB present -> query Position, resolve via geo, record into cache.
    DB absent  -> reconstruct from the committed cache + the TSV source closure.
    """
    base_type = {int(fid, 16): meta["source_type"] for fid, meta in src["placed_bases"].items()}
    direct_ints = {int(fid, 16) for fid in src["direct_refrs"]}
    lists_n = len(src["lvli_closure"])

    seen = {}
    if db_ok:
        for x, y, z, inst, space, ref in pull_by_base(cur, base_type.keys()):
            if inst in seen:
                continue
            region, marker, how = geo.resolve(space, x, y)
            cache[str(inst)] = {"base": ref, "space": space, "x": round(x, 1),
                                "y": round(y, 1), "region": region, "marker": marker}
            seen[inst] = (x, y, region, marker, base_type.get(ref, "loot-list"))
        for x, y, z, inst, space in pull_by_instance(cur, direct_ints):
            if inst in seen:
                continue
            region, marker, how = geo.resolve(space, x, y)
            cache[str(inst)] = {"base": None, "space": space, "x": round(x, 1),
                                "y": round(y, 1), "region": region, "marker": marker}
            seen[inst] = (x, y, region, marker, "direct")
    else:
        for key, e in cache.items():
            inst = int(key)
            base = e.get("base")
            if base in base_type:
                seen[inst] = (e.get("x"), e.get("y"), e.get("region", ""),
                              e.get("marker", ""), base_type[base])
        for inst in direct_ints:
            e = cache.get(str(inst))
            if e and inst not in seen:
                seen[inst] = (e.get("x"), e.get("y"), e.get("region", ""),
                              e.get("marker", ""), "direct")

    return seen, lists_n


# ── region / marker grouping ─────────────────────────────────────────────────
def group_regions(seen, all_regions, keep):
    """Group resolved placements into the per-region location lists. Returns
        (regions_out, src_totals, unresolved, total, placements)
    with the exact shapes the pre-refactor builds emitted."""
    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None,
                                   "sources": defaultdict(int), "places": []})
    unresolved = defaultdict(int)
    for inst, (x, y, region, marker, stype) in seen.items():
        if region not in all_regions:
            unresolved[marker or f"instance {inst:06X}"] += 1
            continue
        g = grouped[(region, marker)]
        g["count"] += 1
        g["sources"][stype] += 1
        ref = f"{int(inst):06X}"
        g["refs"].append(ref)
        # Every placement is kept individually so a marker with N spawns can render
        # N photo/map slots instead of one shared pair (see `spawns` below).
        g["places"].append({"ref": ref, "source_type": stype,
                            "coords": [x, y] if x is not None and y is not None else []})
        if g["coords"] is None and x is not None and y is not None:
            g["coords"] = [x, y]

    by_region = defaultdict(list)
    for (region, marker), g in grouped.items():
        prev = keep.get((region, marker), {})
        prev_spawns = prev.get("spawns") or {}
        # One `spawns` entry per placement: its own map shot, directions and
        # in-place photo. `label` is left blank here — a family driver may set a
        # nicer one (e.g. "Deathclaw Nest #2"); the renderer falls back to
        # "Spawn N". Slots are preserved across rebuilds by ref.
        places = sorted(g["places"], key=lambda p: (p["source_type"], p["ref"]))
        spawns = []
        for p in places:
            hf = prev_spawns.get(p["ref"], {})
            spawns.append({
                "label": "",
                "ref": p["ref"],
                "source_type": p["source_type"],
                "coords": p["coords"],
                "image_top": hf.get("image_top", ""),
                "directions": hf.get("directions", ""),
                "image_bottom": hf.get("image_bottom", ""),
            })
        by_region[region].append({
            "marker": marker,
            "count": g["count"],
            "sources": dict(sorted(g["sources"].items())),
            "image_top": prev.get("image_top", ""),
            "directions": prev.get("directions", ""),
            "image_bottom": prev.get("image_bottom", ""),
            "refs": sorted(set(g["refs"])),
            "coords": g["coords"] or [],
            "spawns": spawns,
        })

    regions_out = []
    for region in all_regions:
        locs = sorted(by_region.get(region, []), key=lambda l: l["marker"].lower())
        regions_out.append({"region": region, "locations": locs})

    total = sum(len(r["locations"]) for r in regions_out)
    placements = sum(loc["count"] for r in regions_out for loc in r["locations"])
    src_totals = defaultdict(int)
    for r in regions_out:
        for loc in r["locations"]:
            for t, n in loc["sources"].items():
                src_totals[t] += n

    return (regions_out, dict(sorted(src_totals.items())),
            {k: unresolved[k] for k in sorted(unresolved)}, total, placements)
