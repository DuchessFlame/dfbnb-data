#!/usr/bin/env python3
r"""
build_spawns.py — one CLI for every DF/BNB "{Item} Spawn Locations" page.

Each item family is a thin config on the shared spawns_engine; adding a new spawn
page is a config entry + seed, never copied files.

Usage:
    python src/build_spawns.py nuka-cola [slug ...]     # all 12 variants, or named slugs
    python src/build_spawns.py farming --all [--pts]    # cream + every egg set
    python src/build_spawns.py farming --item cream [--pts]
    python src/build_spawns.py <farming-slug> [--pts]   # shorthand, e.g. deathclaw-egg
    python src/build_spawns.py bobbleheads              # hub + 10 region pages

Families:
    nuka-cola   -> spawns_configs.nuka_cola   (drinks; computed Used For / Farming Tips)
    farming     -> spawns_configs.farming     (Cream + egg sets; config-supplied blocks)
    bobbleheads -> spawns_configs.bobbleheads (hub page + one page per region)
    cryptids    -> spawns_configs.cryptids    (hub index + one page per cryptid)
"""

import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import nuka_cola, farming, bobbleheads, cryptids, meat, insects
from farming_spawns_config import SETS_BY_SLUG


def _usage(err=""):
    if err:
        print("error:", err)
    print(__doc__.strip())
    sys.exit(2 if err else 0)


def main(argv):
    if len(argv) < 2 or argv[1] in ("-h", "--help", "help"):
        _usage()
    cmd = argv[1]
    rest = argv[2:]
    if cmd == "nuka-cola":
        nuka_cola.run(["nuka-cola"] + rest)          # run() reads argv[1:] as slug filter
    elif cmd == "farming":
        farming.main(rest)                            # farming's argparse (--item/--all/--pts)
    elif cmd in ("bobbleheads", "bobblehead"):
        bobbleheads.run(rest)                         # hub + one page per region
    elif cmd in ("cryptids", "cryptid"):
        cryptids.run(["cryptids"] + rest)             # hub + one page per cryptid
    elif cmd == "meat":
        meat.run(["meat"] + rest)                     # hub + one page per meat
    elif cmd in ("insects", "insect"):
        insects.run(["insects"] + rest)               # hub + one page per insect
    elif cmd == "--all":
        farming.main(["--all"] + rest)
    elif cmd in SETS_BY_SLUG:
        farming.main(["--item", cmd] + rest)
    else:
        _usage(f"unknown family/slug '{cmd}'. "
               f"Use 'nuka-cola', 'farming', 'bobbleheads', or a farming slug: "
               f"{', '.join(sorted(SETS_BY_SLUG))}.")


if __name__ == "__main__":
    main(sys.argv)
