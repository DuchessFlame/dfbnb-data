#!/usr/bin/env python3
r"""
build_farming_spawns_json.py — generic builder for Farming "{Item} Spawn Locations"
page JSONs on buffsnbrew.com.

Config-driven: every item is defined in farming_spawns_config.py.  This script reads
committed game-file exports (LVLI_Entries / LVLI_Refs / ALCH / MISC) and resolves
every world placement via the Mappalachia Position table.  Coordinates are cached so
CI can rebuild without the 459 MB DB.

MERGE, DON'T CLOBBER: image_top / directions / image_bottom are hand-authored and
preserved for a matching (region, marker) key on every rebuild.

Env:
    MAPPALACHIA_DB   default D:\Mappalachia\data\mappalachia.db

Usage:
    python src/build_farming_spawns_json.py --item cream
    python src/build_farming_spawns_json.py --item deathclaw-egg --pts
    python src/build_farming_spawns_json.py --all          # build every item in config
    python src/build_farming_spawns_json.py --all --pts
"""

import os, sys, json, sqlite3, datetime, argparse
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from farming_spawns_config import ALL_SETS, SETS_BY_SLUG, ALL_REGIONS
from nuka_cola_spawns_geo import Geo
import farming_spawns_sources as sources

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
SQL_CHUNK = 900


# ── helpers ──────────────────────────────────────────────────────────────────

def _slug_key(slug):
    """'deathclaw-egg' → 'deathclaw_egg' (for filenames and env-var suffixes)."""
    return slug.replace("-", "_")


def _geo_cache_path(slug):
    env_key = f"FARMING_GEO_CACHE_{_slug_key(slug).upper()}"
    default = os.path.join(REPO, "data", "farming_spawns", f"geo_cache_{_slug_key(slug)}.json")
    return os.environ.get(env_key, default)


def _source_tag(cfg):
    sigs = sorted({item["sig"] for item in cfg["items"]})
    return f"Game-file exports (LVLI/{'/'.join(sigs)}) + Mappalachia Position (cached for CI)"


def _log_prefix(cfg):
    return f"[{cfg['slug']}-spawns]"


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


# ── core logic ───────────────────────────────────────────────────────────────

def resolve_placements(cfg, tbls, geo, cur, cache, db_ok):
    """Return {instanceFormID: (x, y, region, marker, source_type)} for all items."""
    src = sources.get_sources(cfg["items"], tbls,
                              extra_world_bases=cfg.get("extra_world_bases"))
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


def build_one(cfg, tbls, geo, cur, cache, db_ok, generated, dist_dir):
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
        "_meta": {"generated": generated, "source": _source_tag(cfg),
                  "lists_in_closure": lists_n,
                  "source_totals": dict(sorted(src_totals.items())),
                  "unresolved": {k: unresolved[k] for k in sorted(unresolved)}},
        "set": slug,
        "name": cfg["name"],
        "page_title": cfg["page_title"],
        "blurb": cfg["blurb"],
        "drop_rates": cfg.get("drop_rates"),
        "used_for": cfg.get("used_for"),
        "regions": regions_out,
    }
    os.makedirs(dist_dir, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    extra = f"  [{sum(unresolved.values())} unresolved]" if unresolved else ""
    print(f"  {os.path.basename(path):<46} {total:>4} locations / "
          f"{placements:>5} placements{extra}")
    return path


# ── CLI entry-point ──────────────────────────────────────────────────────────

def run_item(cfg, pts=False):
    """Build one item's spawn JSON.  Manages its own geo cache and DB connection."""
    slug = cfg["slug"]
    prefix = _log_prefix(cfg)
    cache_path = _geo_cache_path(slug)

    tsv_root = os.path.join(REPO, "tsv", "pts") if pts else None
    dist_dir = (os.path.join(REPO, "dist", "pts", "farming_spawns") if pts
                else os.path.join(REPO, "dist", "farming_spawns"))

    tbls = sources.load_tables(tsv_root)
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = load_cache(cache_path)

    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print(f"{prefix} Mappalachia DB found — resolving placements and refreshing geo cache.")
    elif cache:
        print(f"{prefix} No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print(f"{prefix} No Mappalachia DB and no geo cache — cannot build. "
              f"Run once locally with MAPPALACHIA_DB set to seed "
              f"data/farming_spawns/geo_cache_{_slug_key(slug)}.json.")
        return

    generated = datetime.date.today().isoformat()
    build_one(cfg, tbls, geo, cur, cache, db_ok, generated, dist_dir)

    if db_ok:
        save_cache(cache, cache_path)
        print(f"{prefix} geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")
    if con:
        con.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Build Farming Spawn Locations JSON (config-driven)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--item", metavar="SLUG",
                       help="Build one item by slug (e.g. cream, deathclaw-egg)")
    group.add_argument("--all", action="store_true",
                       help="Build every item defined in farming_spawns_config.ALL_SETS")
    parser.add_argument("--pts", action="store_true",
                        help="Build from PTS data (tsv/pts/) into dist/pts/farming_spawns/")
    args = parser.parse_args(argv)

    if args.all:
        for cfg in ALL_SETS:
            run_item(cfg, pts=args.pts)
    else:
        cfg = SETS_BY_SLUG.get(args.item)
        if cfg is None:
            valid = ", ".join(sorted(SETS_BY_SLUG))
            parser.error(f"Unknown item slug '{args.item}'. Valid slugs: {valid}")
        run_item(cfg, pts=args.pts)


if __name__ == "__main__":
    main()
