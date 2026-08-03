#!/usr/bin/env python3
r"""
build_cream_spawns_json.py — generate the spawn JSON for the Cream Spawn Locations
page on buffsnbrew.com.

Modelled on build_nuka_cola_spawns_json.py.  Reads committed game-file exports
(LVLI_Entries / LVLI_Refs / ALCH) and resolves every world placement of Cream via
the Mappalachia Position table.  Coordinates are cached so CI can rebuild without
the 459 MB DB.

MERGE, DON'T CLOBBER: image_top / directions / image_bottom are hand-authored and
preserved for a matching (region, marker) key on every rebuild.

Env:
    MAPPALACHIA_DB   default D:\Mappalachia\data\mappalachia.db

Usage:
    python src/build_cream_spawns_json.py            # live channel
    python src/build_cream_spawns_json.py --pts       # PTS channel
"""

import os, sys, json, sqlite3, datetime, argparse
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from farming_spawns_config import CREAM, ALL_REGIONS
from nuka_cola_spawns_geo import Geo
import farming_spawns_sources as sources

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
GEO_CACHE = os.environ.get("FARMING_GEO_CACHE_CREAM",
                           os.path.join(REPO, "data", "farming_spawns", "geo_cache_cream.json"))
SOURCE_TAG = "Game-file exports (LVLI/ALCH) + Mappalachia Position (cached for CI)"
SQL_CHUNK = 900


def _chunks(seq, n=SQL_CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


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


def load_cache():
    try:
        return json.load(open(GEO_CACHE, encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache):
    os.makedirs(os.path.dirname(GEO_CACHE), exist_ok=True)
    json.dump(cache, open(GEO_CACHE, "w", encoding="utf-8"), ensure_ascii=False)


def load_existing(path):
    """Load hand-authored fields from the previous build to preserve on rebuild."""
    keep = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in old.get("regions", []):
        for loc in reg.get("locations", []):
            keep[(reg.get("region", ""), loc.get("marker", ""))] = {
                "image_top": loc.get("image_top", ""),
                "directions": loc.get("directions", ""),
                "image_bottom": loc.get("image_bottom", ""),
            }
    return keep


def resolve_placements(cfg, tbls, geo, cur, cache, db_ok):
    """Yield {instanceFormID: (x, y, region, marker, source_type)} for all items."""
    src = sources.get_sources(cfg["items"], tbls)
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


def build(cfg, tbls, geo, cur, cache, db_ok, generated, dist_dir):
    slug = cfg["slug"]
    path = os.path.join(dist_dir, f"{slug}_spawns.json")
    keep = load_existing(path)

    seen, lists_n = resolve_placements(cfg, tbls, geo, cur, cache, db_ok)

    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None,
                                   "sources": defaultdict(int)})
    unresolved = defaultdict(int)
    for inst, (x, y, region, marker, stype) in seen.items():
        if region not in ALL_REGIONS:
            unresolved[marker or f"instance {inst:06X}"] += 1
            continue
        g = grouped[(region, marker)]
        g["count"] += 1
        g["sources"][stype] += 1
        g["refs"].append(f"{int(inst):06X}")
        if g["coords"] is None and x is not None and y is not None:
            g["coords"] = [x, y]

    by_region = defaultdict(list)
    for (region, marker), g in grouped.items():
        prev = keep.get((region, marker), {})
        by_region[region].append({
            "marker": marker,
            "count": g["count"],
            "sources": dict(sorted(g["sources"].items())),
            "image_top": prev.get("image_top", ""),
            "directions": prev.get("directions", ""),
            "image_bottom": prev.get("image_bottom", ""),
            "refs": sorted(set(g["refs"])),
            "coords": g["coords"] or [],
        })

    regions_out = []
    for region in ALL_REGIONS:
        locs = sorted(by_region.get(region, []), key=lambda l: l["marker"].lower())
        regions_out.append({"region": region, "locations": locs})

    total = sum(len(r["locations"]) for r in regions_out)
    placements = sum(loc["count"] for r in regions_out for loc in r["locations"])
    src_totals = defaultdict(int)
    for r in regions_out:
        for loc in r["locations"]:
            for t, n in loc["sources"].items():
                src_totals[t] += n

    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "lists_in_closure": lists_n,
                  "source_totals": dict(sorted(src_totals.items())),
                  "unresolved": {k: unresolved[k] for k in sorted(unresolved)}},
        "set": slug,
        "name": cfg["name"],
        "page_title": cfg["page_title"],
        "blurb": cfg["blurb"],
        "regions": regions_out,
    }
    os.makedirs(dist_dir, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    extra = f"  [{sum(unresolved.values())} unresolved]" if unresolved else ""
    print(f"  {os.path.basename(path):<46} {total:>4} locations / "
          f"{placements:>5} placements{extra}")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build Cream spawn locations JSON")
    parser.add_argument("--pts", action="store_true",
                        help="Build from PTS data (tsv/pts/) into dist/pts/farming_spawns/")
    args = parser.parse_args(argv)

    tsv_root = os.path.join(REPO, "tsv", "pts") if args.pts else None
    dist_dir = (os.path.join(REPO, "dist", "pts", "farming_spawns") if args.pts
                else os.path.join(REPO, "dist", "farming_spawns"))

    tbls = sources.load_tables(tsv_root)
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = load_cache()

    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print("[cream-spawns] Mappalachia DB found — resolving placements and refreshing geo cache.")
    elif cache:
        print(f"[cream-spawns] No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print("[cream-spawns] No Mappalachia DB and no geo cache — cannot build. "
              "Run once locally with MAPPALACHIA_DB set to seed "
              "data/farming_spawns/geo_cache_cream.json.")
        return

    generated = datetime.date.today().isoformat()
    build(CREAM, tbls, geo, cur, cache, db_ok, generated, dist_dir)

    if db_ok:
        save_cache(cache)
        print(f"[cream-spawns] geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")
    if con:
        con.close()


if __name__ == "__main__":
    main()
