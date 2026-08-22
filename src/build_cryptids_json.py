#!/usr/bin/env python3
"""
build_cryptids_json.py
======================
Builds the DF Cryptids category:
  * dist/cryptids.json            — hub index (one card per cryptid)
  * dist/cryptids/<slug>.json     — one doc per cryptid page

The real build logic lives in the shared spawns engine driver
`spawns_configs/cryptids.py` (creature-seeded: RACE -> NPC -> death lists +
Mappalachia placements), so this stays a thin entry point that the existing
GitHub Actions workflow (.github/workflows/build-cryptids.yml) and the master
workflow (dfbnb-patch-build.yml) can keep calling unchanged.

Fixed Spawn Locations need a LOCAL Mappalachia DB pass to resolve placements to
regions/markers; the result is cached to data/cryptid_spawns/geo_cache.json,
which is committed so CI (no DB) rebuilds straight from the cache.

Usage:
  python src/build_cryptids_json.py
  python src/build_cryptids_json.py mothman wendigo   # named cryptids only
"""

import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import cryptids


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    slugs = [a for a in argv if not a.startswith("-")]
    cryptids.run(["cryptids"] + slugs)


if __name__ == "__main__":
    main()
