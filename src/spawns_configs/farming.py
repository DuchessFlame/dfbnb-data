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


def build_one(cfg, tbls, geo, cur, cache, db_ok, generated, dist_dir):
    slug = cfg["slug"]
    path = os.path.join(dist_dir, f"{slug}_spawns.json")
    keep = ebuild.load_existing(path)

    src = esources.get_sources(cfg["items"], tbls, farming_classify,
                               extra_world_bases=cfg.get("extra_world_bases"),
                               placed_sigs=esources.PLACED_SIGS_FLORA)
    seen, lists_n = ebuild.resolve_placements(src, geo, cur, cache, db_ok)
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep)

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
