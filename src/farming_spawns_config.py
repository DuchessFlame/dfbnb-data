#!/usr/bin/env python3
r"""
farming_spawns_config.py — item definitions for the Farming "{Item} Spawn Locations"
pages on buffsnbrew.com (Cream, Deathclaw Eggs).

This is the ONLY file you need to touch when Bethesda adds or changes a farming-item
FormID.  The build scripts (build_cream_spawns_json.py, build_deathclaw_egg_spawns_json.py)
read these definitions, walk the LVLI closure from the committed game-file exports, pull
placements from the Mappalachia DB (or committed geo_cache), and write one JSON per set
into dist/farming_spawns/.

HOW THE SOURCE FORMIDS WERE FOUND (from the July 2026 xEdit exports):
  Cream (ALCH 0012DB3D) is placed via LPI_Drink_Cream (00215C9C, 83 world REFRs)
  and LL_Drink_NonAlcohol_Basic_NoWater (002B85D1).  Also 4 direct world REFRs.
  Vendor pools: LLV_Vendor_Healing_Faction_Raiders, Vendor_Moon_Vera_BlueRidge.

  Deathclaw Egg (ALCH 00046939) is placed via LPI_Food_DeathclawEgg (002118C0,
  37 REFRs) and through deathclaw nest containers (LL_Deathclaw_Nest 001B0084 ->
  Container_Loot_DeathclawNest01 003099CD -> DeathclawNest01_Container 0019B65C /
  SFM04_Organic_DeathclawNest 00386096).  Also in collectron pools.

  Cracked Deathclaw Egg (MISC 0014F6AC) has 12 direct world REFRs and is in
  deathclaw nest containers (LLS_Deathclaw_eggcracked 001B0085 -> LL_Deathclaw_Nest).
  Vendor pool: LLV_Vendor_Junk_Small_Rare (000757BD).
"""

# Appalachia worldspace formID (the only worldspace with markers/regions in the DB).
APPALACHIA_SPACE = 2480661

# ── CREAM ────────────────────────────────────────────────────────────────────────
CREAM = {
    "slug": "cream",
    "name": "Cream",
    "page_title": "Cream Spawn Locations",
    "blurb": "Every known world spawn for Cream, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "0012DB3D",
            "edid": "Cream",
            "full": "Cream",
            "sig": "ALCH",
        },
    ],
}

# ── DEATHCLAW EGGS ───────────────────────────────────────────────────────────────
DEATHCLAW_EGG = {
    "slug": "deathclaw-egg",
    "name": "Deathclaw Egg",
    "page_title": "Deathclaw Egg Spawn Locations",
    "blurb": "Every known world spawn for Deathclaw Eggs (raw and cracked), grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "00046939",
            "edid": "DeathclawEgg",
            "full": "Deathclaw Egg",
            "sig": "ALCH",
        },
        {
            "formid": "0014F6AC",
            "edid": "DeathclawEggCracked",
            "full": "Cracked Deathclaw Egg",
            "sig": "MISC",
        },
    ],
}

# All sets in this family — add new items here, then create a build_<slug>_spawns_json.py.
ALL_SETS = [CREAM, DEATHCLAW_EGG]

# The ten regions every page renders, in A-Z order (empty ones get a "no spawns" note).
ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]
