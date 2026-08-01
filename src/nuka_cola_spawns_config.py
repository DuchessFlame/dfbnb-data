#!/usr/bin/env python3
r"""
nuka_cola_spawns_config.py — the editable membership map for the Nuka Cola
"{Type} Locations" spawn pages on buffsnbrew.com.

This is intentionally the ONLY file you need to touch when:
  * a new Nuka Cola variant is added to the game, or
  * a variant's leveled-item / dispenser FormID changes, or
  * you want to add extra source FormIDs to an existing variant's page.

The build script (build_nuka_cola_spawns_json.py) reads this map, pulls every
placed instance of the listed FormIDs out of the Mappalachia DB, resolves each
to a region + nearest map marker, and writes one dist/nuka_cola_spawns_<slug>.json
per variant. Directions and photos you write by hand are preserved on rebuild.

HOW THE SOURCE FORMIDS WERE FOUND (decimal FormIDs, Mappalachia Entity table):
  The drinks themselves (ALCH records) are never placed in the world — they are
  dispensed from leveled items (LVLI "LPI_Drink_NukaCola_<flavor>") that ARE
  placed (inside machines/containers). So the spawn source for each flavour is
  its leveled-item FormID, not the drink's ALCH FormID.

  Confirmed placements in the current Mappalachia DB (Appalachia worldspace):
    LPI_Drink_NukaCola            3834214   673   -> base Nuka-Cola
    LPI_Drink_NukaCola_ForMachine  295773  1110   -> base Nuka-Cola (machine feed)
    LPI_Drink_NukaCola_Cherry     4629914    33
    LPI_Drink_NukaCola_Cranberry  5875111    27
    LPI_Drink_NukaCola_Dark       2163089    20
    LPI_Drink_NukaCola_Grape      2162567     7
    LPI_Drink_NukaCola_Orange     2162568     6
    LPI_Drink_NukaCola_Quantum    5417048    96
    LPI_Drink_NukaCola_Wild       2163091    11

  NO auto-source available in this DB (leave source_formids empty -> the page
  renders every region with the "no spawns" note until you either add a FormID
  here or hand-author the region expands):
    Twist               - no LPI_Drink_NukaCola_Twist leveled item exists
    Vaccinated          - quest-only (W05_MQ_101P_VaccinatedActivator), not placed
    Sunset Sarsaparilla - not present in the datamined game data at all
    Nukashine           - only 1-3 dispenser placements; add them if you want:
                            P01A_Nukashine_LL_VintageDispenser   4642683
                            P01A_Nukashine_LL_QuantumDispenser   4648160
                            P01A_Nukashine_LL_NuclearMaterialDispenser 4112839

  ATLANTIC CITY / THE PITT: these are separate instanced worldspaces and are
  NOT in the Mappalachia DB, so nothing auto-populates there. They resolve via
  the hand-maintained data/collectable_spawns/manual_regions.json map (shared
  with the collectables pipeline) — add Nuka entries there, or hand-author those
  region expands after the page is built.
"""

# Appalachia worldspace formID (the only worldspace with markers/regions in the DB).
APPALACHIA_SPACE = 2480661

# slug MUST match the guide_index.tsv / nav.json page slug (…-locations).
# source_formids: decimal LVLI (or dispenser) FormIDs whose placements ARE this
#                 variant's spawns. Empty list = no auto-source (hand-author).
VARIANTS = [
    {
        "slug": "nuka-cola-locations",
        "name": "Nuka Cola",
        "blurb": "Every known world spawn for Nuka-Cola, grouped by region. Directions and photos are added by hand.",
        "source_formids": [3834214, 295773],
    },
    {
        "slug": "nuka-cola-cherry-locations",
        "name": "Nuka Cola Cherry",
        "blurb": "Every known world spawn for Nuka-Cola Cherry, grouped by region.",
        "source_formids": [4629914],
    },
    {
        "slug": "nuka-cola-cranberry-locations",
        "name": "Nuka Cola Cranberry",
        "blurb": "Every known world spawn for Nuka-Cola Cranberry, grouped by region.",
        "source_formids": [5875111],
    },
    {
        "slug": "nuka-cola-dark-locations",
        "name": "Nuka Cola Dark",
        "blurb": "Every known world spawn for Nuka-Cola Dark, grouped by region.",
        "source_formids": [2163089],
    },
    {
        "slug": "nuka-cola-grape-locations",
        "name": "Nuka Cola Grape",
        "blurb": "Every known world spawn for Nuka-Grape, grouped by region.",
        "source_formids": [2162567],
    },
    {
        "slug": "nuka-cola-orange-locations",
        "name": "Nuka Cola Orange",
        "blurb": "Every known world spawn for Nuka-Cola Orange, grouped by region.",
        "source_formids": [2162568],
    },
    {
        "slug": "nuka-cola-quantum-locations",
        "name": "Nuka Cola Quantum",
        "blurb": "Every known world spawn for Nuka-Cola Quantum, grouped by region.",
        "source_formids": [5417048],
    },
    {
        "slug": "nuka-cola-twist-locations",
        "name": "Nuka Cola Twist",
        "blurb": "Every known world spawn for Nuka-Cola Twist, grouped by region.",
        "source_formids": [],  # no leveled item in the DB — hand-author
    },
    {
        "slug": "nuka-cola-vaccinated-locations",
        "name": "Nuka Cola Vaccinated",
        "blurb": "Every known world spawn for Nuka-Cola Vaccinated, grouped by region.",
        "source_formids": [],  # quest-only — hand-author
    },
    {
        "slug": "nuka-cola-wild-locations",
        "name": "Nuka Cola Wild",
        "blurb": "Every known world spawn for Nuka-Cola Wild, grouped by region.",
        "source_formids": [2163091],
    },
    {
        "slug": "nukashine-locations",
        "name": "Nukashine",
        "blurb": "Every known world spawn for Nukashine, grouped by region.",
        "source_formids": [],  # dispensers only; add the P01A_Nukashine_LL_* IDs if wanted
    },
    {
        "slug": "sunset-sarsaparilla-locations",
        "name": "Sunset Sarsaparilla",
        "blurb": "Every known world spawn for Sunset Sarsaparilla, grouped by region.",
        "source_formids": [],  # not in the datamined game data — hand-author
    },
]

# The ten regions every page renders, in A-Z order (empty ones get a "no spawns" note).
ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]
