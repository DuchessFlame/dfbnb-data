#!/usr/bin/env python3
r"""
spawns_configs.farming — the farming/egg family driver for the shared spawns engine.

Owns the item sets (Cream + the egg sets, defined in farming_spawns_config), the
per-slug geo caches, PTS support, and the exact farming output-doc shape
(drop_rates / farming_tips / used_for / additional_expands come straight from the
config). Heavy lifting lives in spawns_engine.

A NEW farming item = one dict appended to farming_spawns_config.ALL_SETS. No new code.
"""

import os, sys, sqlite3, datetime, json, argparse

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from farming_spawns_config import ALL_SETS, SETS_BY_SLUG, ALL_REGIONS
from spawns_engine.geo import Geo
from spawns_engine import sources as esources
from spawns_engine import build as ebuild
from spawns_engine import events as eevents
from spawns_engine.classify import farming_classify

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")


def _slug_key(slug):
    """'deathclaw-egg' -> 'deathclaw_egg' (for filenames and env-var suffixes)."""
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


def _origin_maps(cfg, tbls):
    """Partition an item's DIRECT world REFRs by origin so each fixed-spawn marker
    can be broken down by type. Returns (world_refs, static_item):
      world_refs  : set(int) of REFRs placed via the world_spawns LPI list(s)
                    (loose spawns that roll the world-spawn chance, e.g. raw eggs).
      static_item : {int(refr): item FULL name} for REFRs placed straight from an
                    item's own ALCH/MISC record (guaranteed 100% static spawns,
                    e.g. cracked eggs).
    Keyed by int FormID so they match `seen`'s instance keys."""
    dr = cfg.get("drop_rates") or {}
    ws = dr.get("world_spawns") or {}
    world_ids = set()
    if ws.get("list_id"):
        world_ids.add(ws["list_id"].upper())
    for x in (ws.get("list_ids") or []):
        world_ids.add(str(x).upper())
    world_refs = set()
    for lv in world_ids:
        for rf, _redid, rsig in tbls["lvli_refs"].get(lv, ()):
            if rsig == "REFR":
                world_refs.add(int(rf, 16))
    static_item = {}
    for rec in cfg["items"]:
        fid = rec["formid"].upper()
        refs = (tbls["misc_refs"].get(fid, []) if rec.get("sig") == "MISC"
                else tbls["alch_refs"].get(fid, []))
        for rf, _redid, rsig in refs:
            if rsig == "REFR":
                static_item.setdefault(int(rf, 16), rec["full"])
    return world_refs, static_item


def _attach_breakdowns(cfg, tbls, seen, regions_out):
    """When cfg opts in (per_marker_breakdown), attach a per-marker `breakdown`
    list — [{label, count, rate_key, note}] — so the renderer can show how many of
    each source type sit at a marker and each one's %. rate_key is resolved to a %
    at render time from drop_rates ("static" -> 100%), so the numbers can never
    drift from the computed headline rates. Loose-world before loose-static before
    nests, matching the fixed-spawn reading order."""
    if not cfg.get("per_marker_breakdown"):
        return
    from collections import defaultdict
    world_refs, static_item = _origin_maps(cfg, tbls)
    dr = cfg.get("drop_rates") or {}
    ws = dr.get("world_spawns") or {}
    cn = dr.get("containers") or {}
    world_label = ws.get("marker_label") or (cfg["items"][0]["full"])
    nest_label = cn.get("marker_label") or "Nest"
    nest_note = cn.get("marker_yield") or ""

    # source_type -> label for base-placed items (pull-by-base, 100% static spawns).
    static_type_label = {rec.get("world_source_type"): rec["full"]
                         for rec in cfg["items"] if rec.get("world_source_type")}

    tally = defaultdict(lambda: defaultdict(int))   # (region, marker) -> key -> n
    for inst, (_x, _y, region, marker, stype) in seen.items():
        if stype == "nest":
            key = ("nest",)
        elif stype in static_type_label:            # base-placed item (100% static)
            key = ("static", static_type_label[stype])
        elif stype == "direct":
            if inst in static_item:                 # legacy: cracked via ref column
                key = ("static", static_item[inst])
            else:
                key = ("world",)                    # world LPI point (50%)
        else:
            continue                                # vendors/other aren't fixed-spawn markers
        tally[(region, marker)][key] += 1

    order = {"world": 0, "static": 1, "nest": 2}
    for reg in regions_out:
        for loc in reg["locations"]:
            t = tally.get((reg["region"], loc["marker"]))
            if not t:
                continue
            rows = []
            for key in sorted(t, key=lambda k: (order.get(k[0], 9), k[1] if len(k) > 1 else "")):
                cnt = t[key]
                if key[0] == "world":
                    rows.append({"label": world_label, "count": cnt,
                                 "rate_key": "world_spawns", "note": ""})
                elif key[0] == "static":
                    rows.append({"label": key[1], "count": cnt,
                                 "rate_key": "static", "note": ""})
                elif key[0] == "nest":
                    rows.append({"label": nest_label, "count": cnt,
                                 "rate_key": "containers", "note": nest_note})
            loc["breakdown"] = rows


def build_one(cfg, tbls, geo, cur, cache, db_ok, generated, dist_dir):
    slug = cfg["slug"]
    path = os.path.join(dist_dir, f"{slug}_spawns.json")
    keep = ebuild.load_existing(path)

    src = esources.get_sources(cfg["items"], tbls, farming_classify,
                               extra_world_bases=cfg.get("extra_world_bases"),
                               placed_sigs=esources.PLACED_SIGS_FLORA,
                               place_item_bases=cfg.get("place_item_bases", False))
    seen, lists_n = ebuild.resolve_placements(src, geo, cur, cache, db_ok)
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep)
    _attach_breakdowns(cfg, tbls, seen, regions_out)

    # Events & Activities — event/activity reward ROOTS in the closure (§9k):
    # keyword pass + QUEST reward registry, with nested loot-bag sub-lists
    # collapsed into their outer event/activity root (c2p). Raw {list_id, edid,
    # name, type} here; the chained per-root rate is computed later by
    # build_farming_used_for.py (rng76). Empty = renderer shows the empty-state.
    events_activities = eevents.detect(src["lvli_closure"], tbls["parent_edid"],
                                       c2p=tbls["c2p"])

    doc = {
        "_meta": {"generated": generated, "source": _source_tag(cfg),
                  "lists_in_closure": lists_n,
                  "source_totals": src_totals,
                  "unresolved": unresolved},
        "set": slug,
        "name": cfg["name"],
        "page_title": cfg["page_title"],
        "blurb": cfg["blurb"],
        "drop_rates": cfg.get("drop_rates"),
        "farming_tips": cfg.get("farming_tips"),
        "used_for": cfg.get("used_for"),
        "additional_expands": cfg.get("additional_expands"),
        "info_notes": cfg.get("info_notes"),
        "events_activities": events_activities,
        "regions": regions_out,
    }
    os.makedirs(dist_dir, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    extra = f"  [{sum(unresolved.values())} unresolved]" if unresolved else ""
    print(f"  {os.path.basename(path):<46} {total:>4} locations / "
          f"{placements:>5} placements{extra}")
    return path


def run_item(cfg, pts=False):
    """Build one item's spawn JSON. Manages its own geo cache and DB connection."""
    slug = cfg["slug"]
    prefix = _log_prefix(cfg)
    cache_path = _geo_cache_path(slug)

    tsv_root = os.path.join(REPO, "tsv", "pts") if pts else None
    dist_dir = (os.path.join(REPO, "dist", "pts", "farming_spawns") if pts
                else os.path.join(REPO, "dist", "farming_spawns"))

    tbls = esources.load_tables(tsv_root)
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = ebuild.load_cache(cache_path)

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
        ebuild.save_cache(cache, cache_path)
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
