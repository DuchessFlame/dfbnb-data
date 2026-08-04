#!/usr/bin/env python3
r"""
farming_spawns_config.py — item definitions for the Farming "{Item} Spawn Locations"
pages on buffsnbrew.com.

HOW TO ADD A NEW FOOD ITEM TO THE PIPELINE:
  1. Add a new config dict below (copy CREAM as a template).  Required keys:
       slug        URL-safe identifier, e.g. "honey" or "purified-water"
       name        Display name, e.g. "Honey"
       page_title  Full page heading, e.g. "Honey Spawn Locations"
       blurb       One-liner shown at the top of the page
       items       List of {formid, edid, full, sig} dicts (ALCH/MISC FormIDs)
     Optional:
       drop_rates  Vendor/world-spawn rate data (see CREAM for structure)
  2. Append the dict to ALL_SETS at the bottom of this file.
  3. Seed the geo cache locally:
       set MAPPALACHIA_DB=D:\Mappalachia\data\mappalachia.db
       python src/build_farming_spawns_json.py --item <slug>
     This creates data/farming_spawns/geo_cache_<slug>.json.  Commit it.
  4. Done — the next CI run picks it up automatically via --all.
     The env var FARMING_GEO_CACHE_<SLUG_UPPER> overrides the cache path
     (e.g. FARMING_GEO_CACHE_HONEY), but the default is fine.

The generic build script (build_farming_spawns_json.py) reads these definitions, walks
the LVLI closure from the committed game-file exports, pulls placements from the
Mappalachia DB (or committed geo_cache), and writes one JSON per set into
dist/farming_spawns/.

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
    # ── Drop rates (resolved from LVLI TSVs, July 2026) ─────────────────
    # Traced via the drop-rate-engine skill rules.  Cream (ALCH 0012DB3D)
    # appears in three LVLI chains:
    #   Container:  LPI_Drink_Cream (00215C9C, 83 REFRs)
    #               List-level ChanceNone = 50 (GLOB LPI_Chance_Drink_Common_ECON)
    #   Vendor:     LLV_Vendor_Drink (003C24EA) → LL_Drink_NonAlcohol_Basic_NoWater
    #               (002B85D1, pick-one 3 entries, ForEach qty 2+4) ≈ 86% per reset
    #   Raider:     LLV_Vendor_Healing_Faction_Raiders (003D7F7C) entry 0
    #               ChanceNone 85 → 15% extra for Raider vendors
    #   Vera:       Vendor_Moon_Vera_BlueRidge (00695804) entry 1, CN=0 qty=3
    #               → 100% guaranteed, 3 cream
    #   Resource generators: none produce cream.
    "drop_rates": {
        "world_spawns": {
            "list_edid": "LPI_Drink_Cream",
            "list_id": "00215C9C",
            "chance_none_glob": "LPI_Chance_Drink_Common_ECON",
            "chance_none_value": 50,
            "rate": 0.5,
            "rate_display": "50%",
            "note": "Each world spawn rolls a 50% chance per server hop.",
        },
        "vendors": {
            "general": {
                "list_edid": "LLV_Vendor_Drink",
                "list_id": "003C24EA",
                "mechanism": (
                    "Cream is 1 of 3 items in LL_Drink_NonAlcohol_Basic_NoWater "
                    "(pick-one, 33% per roll). The drink pool rolls this list up to "
                    "6 times across two UseAll entries (qty 2 at 50% + qty 4 guaranteed)."
                ),
                "rate_display": "~86%",
                "note": (
                    "Most vendors have roughly an 86% chance to stock at least one "
                    "cream per reset."
                ),
            },
            "raider": {
                "list_edid": "LLV_Vendor_Healing_Faction_Raiders",
                "list_id": "003D7F7C",
                "mechanism": (
                    "Entry 0 of the Raider healing pool references LL_Drink_Cream "
                    "with ChanceNone 85 (15%). The healing pool itself fires 50% of "
                    "the time via LLV_Faction_Raiders, so effective extra = ~7.5%."
                ),
                "rate_display": "~8%",
                "note": (
                    "Raider vendors have an extra ~8% chance to stock cream through "
                    "the Raider healing pool (ChanceNone 85 at 50% pool fire rate), "
                    "on top of the general drink pool."
                ),
            },
            "vera": {
                "list_edid": "Vendor_Moon_Vera_BlueRidge",
                "list_id": "00695804",
                "rate": 1.0,
                "rate_display": "100%",
                "qty": 3,
                "note": "Vera always stocks 3 cream.",
            },
        },
        "resource_generators": None,
    },
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
    # ── Drop rates (resolved from LVLI TSVs, July 2026) ─────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # Raw egg (ALCH 00046939) placed via LPI_Food_DeathclawEgg (002118C0,
    # 37 world REFRs).  ChanceNone = 50 (GLOB LPI_Chance_Food_FruitVegetables).
    # Also 5 direct ALCH world REFRs (100% fixed spawns).
    #
    # Cracked egg (MISC 0014F6AC) has 12 direct MISC world REFRs (100% fixed).
    # Vendor pool: LLV_Vendor_Junk_Small_Rare (000757BD), pick-one 40 entries → 2.5%.
    #
    # Deathclaw nests: Container_Loot_DeathclawNest01 (003099CD, UseAll max_count=2)
    # → LL_Deathclaw_Nest (001B0084, UseAll) → 1x raw egg + 1x cracked egg.
    # Nest entry ChanceNone = 10 (GLOB ItemTwo_High_ChanceNone_Tier) → 90%.
    # 19 nest container REFRs (12 DeathclawNest01_Container + 7 SFM04).
    #
    # Collectrons: Electronics/Junkyard super-rare pool (CN 95, 1/11 = 0.45%),
    # Sir Loin rare pool (CN 99, 1/8 = 0.125%).
    #
    # Resource generators: collectrons listed under vendors, no dedicated generators.
    "drop_rates": {
        "world_spawns": {
            "list_edid": "LPI_Food_DeathclawEgg",
            "list_id": "002118C0",
            "chance_none_glob": "LPI_Chance_Food_FruitVegetables",
            "chance_none_value": 50,
            "rate": 0.5,
            "rate_display": "50%",
            "note": (
                "37 LPI spawn points for the raw egg roll a 50% chance each "
                "per server hop. 5 raw eggs and 12 cracked eggs are fixed "
                "100% static world spawns."
            ),
        },
        "containers": {
            "container_edid": "Container_Loot_DeathclawNest01",
            "container_id": "003099CD",
            "nest_list_edid": "LL_Deathclaw_Nest",
            "nest_list_id": "001B0084",
            "chance_none_glob": "ItemTwo_High_ChanceNone_Tier",
            "chance_none_value": 10,
            "rate": 0.9,
            "rate_display": "90%",
            "count": 19,
            "note": (
                "19 deathclaw nest containers (12 DeathclawNest01 + 7 SFM04). "
                "Each nest has a 90% chance to contain 1 raw egg + 1 cracked egg "
                "(entry ChanceNone 10 via GLOB ItemTwo_High_ChanceNone_Tier)."
            ),
        },
        "vendors": {
            "general": {
                "list_edid": "LLV_Vendor_Junk_Small_Rare",
                "list_id": "000757BD",
                "mechanism": (
                    "Cracked Deathclaw Egg is 1 of 40 items in the junk vendor "
                    "rare pool (pick-one, 2.5% per roll). Raw eggs are not sold "
                    "by any vendor."
                ),
                "rate_display": "2.5%",
                "note": (
                    "Junk vendors have a 2.5% chance per roll to stock a cracked "
                    "deathclaw egg. Raw eggs are not vendored."
                ),
            },
        },
        "resource_generators": {
            "note": (
                "Collectrons can produce raw deathclaw eggs at very low rates: "
                "Electronics and Junkyard collectrons have a ~0.45% chance per "
                "cycle (ChanceNone 95, 1 of 11 in super-rare pool). "
                "Sir Loin collectron has a ~0.13% chance per cycle "
                "(ChanceNone 99, 1 of 8 in rare pool)."
            ),
        },
    },
}

# All sets in this family — add new items here.
# The generic build script (build_farming_spawns_json.py) picks them up automatically.
ALL_SETS = [CREAM, DEATHCLAW_EGG]

# Slug → config dict, for --item <slug> lookup in the build script.
SETS_BY_SLUG = {s["slug"]: s for s in ALL_SETS}

# The ten regions every page renders, in A-Z order (empty ones get a "no spawns" note).
ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]
