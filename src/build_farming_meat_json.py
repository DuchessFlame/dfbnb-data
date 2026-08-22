#!/usr/bin/env python3
"""
build_farming_meat_json.py
==========================
Builds the BNB Farming - Meat category: one creature-seeded page per raw meat.

Real logic lives in spawns_configs/meat.py (reuses the shared creature-seeded
engine, spawns_configs.cryptids.compute_bundle). Output:
  dist/meat.json           — hub index (one card per meat)
  dist/meat/<slug>.json    — one doc per meat page

Fixed Spawn Locations need a LOCAL Mappalachia DB pass; the result is cached to
data/meat_spawns/<slug>.json (committed) so CI (no DB) rebuilds from the cache.

Usage:
  python src/build_farming_meat_json.py
  python src/build_farming_meat_json.py deathclaw wolf   # named meats only
"""
import os, sys, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import meat


def _mirror_to_pts():
    """Meat pages are derived from live game data + Mappalachia (not PTS-specific
    TSVs), so the PTS site serves the same docs — mirror dist/meat* to dist/pts/."""
    dist = os.path.join(REPO, "dist")
    pts = os.path.join(dist, "pts")
    os.makedirs(pts, exist_ok=True)
    src_dir, dst_dir = os.path.join(dist, "meat"), os.path.join(pts, "meat")
    if os.path.isdir(src_dir):
        shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
    src_hub = os.path.join(dist, "meat.json")
    if os.path.exists(src_hub):
        shutil.copyfile(src_hub, os.path.join(pts, "meat.json"))
    print(f"[meat] mirrored dist/meat -> dist/pts/meat for PTS.")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    slugs = [a for a in argv if not a.startswith("-")]
    meat.run(["meat"] + slugs)
    if "--pts" in argv:
        _mirror_to_pts()


if __name__ == "__main__":
    main()
