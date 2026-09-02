#!/usr/bin/env python3
"""
build_chainsaw_spawns_json.py
=============================
Builds the BNB Weapons - Chainsaws category: a hub plus one location guide per
chainsaw, seeded straight from the weapon's WEAP record.

Real logic lives in spawns_configs/chainsaws.py (the shared spawns engine, with a
Weapon Stats block and a Mods & Plans block standing in for Used For / Farming Tips
on a weapon page). Output:

  dist/chainsaws.json             — hub index (one card per chainsaw)
  dist/chainsaws/<slug>.json      — one doc per guide page

Fixed Spawn Locations need a LOCAL Mappalachia DB pass; the result is cached to
data/chainsaw_spawns/geo_cache.json (committed) so CI (no DB) rebuilds from the cache.

Seed the coordinates in TWO steps. rng76 holds the whole LVLI export (~260 MB) and the
Mappalachia DB is a ~459 MB SQLite scan; running both in one process is what makes a
laptop swap. --geo-only loads neither rng76 nor the camp-item / vendor / curve exports.

  $env:MAPPALACHIA_DB = "D:\\Mappalachia\\data\\mappalachia.db"
  python src/build_chainsaw_spawns_json.py --geo-only    # DB pass, writes the cache only
  python src/build_chainsaw_spawns_json.py --pts         # normal build, reads the cache

Usage:
  python src/build_chainsaw_spawns_json.py
  python src/build_chainsaw_spawns_json.py --pts       # build the PTS channel too
  python src/build_chainsaw_spawns_json.py --geo-only  # DB pass only, no page writes
  python src/build_chainsaw_spawns_json.py chainsaw    # a single page
"""
import os, sys, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import chainsaws


def _mirror_to_pts():
    """Chainsaw pages come from live game data + Mappalachia rather than PTS-specific
    TSVs, so the PTS channel serves the same docs. mirror_dir copies AND removes what
    has left the source, so a page deleted here can't linger under dist/pts/."""
    from prune_outputs import mirror_dir
    dist = os.path.join(REPO, "dist")
    pts = os.path.join(dist, "pts")
    os.makedirs(pts, exist_ok=True)
    mirror_dir(os.path.join(dist, "chainsaws"), os.path.join(pts, "chainsaws"),
               tag="[chainsaws->pts]")
    hub = os.path.join(dist, "chainsaws.json")
    if os.path.exists(hub):
        shutil.copyfile(hub, os.path.join(pts, "chainsaws.json"))
    print("[chainsaws] mirrored dist/chainsaws -> dist/pts/chainsaws for PTS.")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    slugs = [a for a in argv if not a.startswith("-")]
    if "--geo-only" in argv:
        chainsaws.run(slugs + ["--geo-only"])   # cache only; nothing to mirror
        return
    chainsaws.run(slugs)
    if "--pts" in argv:
        _mirror_to_pts()


if __name__ == "__main__":
    main()
