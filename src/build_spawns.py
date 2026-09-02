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
    python src/build_spawns.py chems [--pts] [slug ...] # every chem set (74), or named slugs
    python src/build_spawns.py plants [--pts] [slug ...]# every plant set, or named slugs
    python src/build_spawns.py bobbleheads              # hub + 10 region pages
    python src/build_spawns.py chainsaws [--pts]       # hub + one page per chainsaw (WEAP)

Families:
    nuka-cola   -> spawns_configs.nuka_cola   (drinks; computed Used For / Farming Tips)
    farming     -> spawns_configs.farming     (Cream + egg sets; config-supplied blocks)
    chems       -> spawns_configs.farming     (all chems_spawns_config.CHEM_SETS, one pass)
    plants      -> spawns_configs.plants      (flora; spawnWeight two-tier, no fixed-spawn cap)
    bobbleheads -> spawns_configs.bobbleheads (hub page + one page per region)
    cryptids    -> spawns_configs.cryptids    (hub index + one page per cryptid)
    chainsaws   -> spawns_configs.chainsaws   (WEAP-seeded: Weapon Stats + Mods & Plans
                                               replace Used For / Farming Tips)
"""

import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import (nuka_cola, farming, bobbleheads, cryptids, meat, insects,
                            magazines, consumable_items, plants, chainsaws)
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
    elif cmd in ("magazines", "magazine"):
        magazines.run(rest)                           # hub + one page per region (BOOK-seeded)
    elif cmd in ("consumable-items", "consumable_items"):
        consumable_items.run(rest)                    # single-page reward consumables (Scout's Banner, Lunchbox)
    elif cmd in ("cryptids", "cryptid"):
        cryptids.run(["cryptids"] + rest)             # hub + one page per cryptid
    elif cmd == "meat":
        meat.run(["meat"] + rest)                     # hub + one page per meat
    elif cmd in ("insects", "insect"):
        insects.run(["insects"] + rest)               # hub + one page per insect
    elif cmd in ("chems", "chem"):
        _run_chems(rest)                              # every chem set (farming engine), incl. --pts Ghost Boy
    elif cmd in ("chainsaws", "chainsaw"):
        chainsaws.run(rest)                           # hub + one page per chainsaw (WEAP-seeded)
    elif cmd in ("plants", "plant"):
        plants.run(["plants"] + rest)                 # one page per plant (spawnWeight two-tier)
    elif cmd == "--all":
        farming.main(["--all"] + rest)
    elif cmd in SETS_BY_SLUG:
        farming.main(["--item", cmd] + rest)
    else:
        _usage(f"unknown family/slug '{cmd}'. "
               f"Use 'nuka-cola', 'farming', 'chems', 'plants', 'bobbleheads', or a farming slug: "
               f"{', '.join(sorted(SETS_BY_SLUG))}.")


def _run_chems(rest):
    """Build every Farming - Chems page (all 74 chem sets), or a named subset.

    Chems already live in farming_spawns_config.ALL_SETS (appended from
    chems_spawns_config.CHEM_SETS), so this reuses the exact farming engine +
    geo resolver every other food/egg set uses — it just filters ALL_SETS down
    to the chems family and runs them in one pass. `--pts` builds from tsv/pts/
    into dist/pts/farming_spawns/ (Ghost Boy is SDOW/PTS content)."""
    pts = "--pts" in rest
    slug_filter = {a for a in rest if not a.startswith("-")}
    chem_sets = [s for s in SETS_BY_SLUG.values() if s.get("category") == "chems"]
    if slug_filter:
        chem_sets = [s for s in chem_sets if s["slug"] in slug_filter]
    if not chem_sets:
        _usage(f"no chem sets matched {sorted(slug_filter)}.")
    print(f"[chems] building {len(chem_sets)} chem pages"
          + (" (PTS channel)" if pts else "") + " via the shared farming engine.")
    for cfg in chem_sets:
        farming.run_item(cfg, pts=pts)
    print(f"[chems] done — {len(chem_sets)} pages written to "
          f"dist/{'pts/' if pts else ''}farming_spawns/. Re-run build_farming_used_for.py "
          f"to (re)attach Used For / vendors / producer cards.")


if __name__ == "__main__":
    main(sys.argv)
