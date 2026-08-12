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
    # ── Drop rates ──────────────────────────────────────────────────────
    # RATES ARE NOT HARDCODED. Only the source list IDs live here; the actual
    # percentages are COMPUTED at build time by build_farming_used_for.py, which
    # calls rng76.pick_rate() on these lists (and, for named vendors, on each
    # vendor's real inventory tree). "Rate" = the SINGLE per-roll pick rate — the
    # chance the item is the pick on one roll of the list, as the rng76 harness
    # reports it (NOT a cumulative "will it show up across many rolls" number).
    #   Container:  LPI_Drink_Cream (00215C9C, 83 REFRs) — list-level ChanceNone
    #               50 via GLOB LPI_Chance_Drink_Common_ECON (world-spawn only).
    #   Vendor:     LLV_Vendor_Drink (003C24EA) → LL_Drink_NonAlcohol_Basic_NoWater
    #               (002B85D1, pick-one of 3 → cream is 1 of 3 = ~33% per roll).
    #   Raider:     LLV_Vendor_Healing_Faction_Raiders (003D7F7C) entry 0, cream at
    #               ChanceNone 85 = 15% per roll of that pool.
    #   Vera:       Vendor_Moon_Vera_BlueRidge (00695804) — guaranteed named stock.
    #   Resource generators: none produce cream.
    "drop_rates": {
        "world_spawns": {
            "list_edid": "LPI_Drink_Cream",
            "list_id": "00215C9C",
            "chance_none_glob": "LPI_Chance_Drink_Common_ECON",
            "note": "Each world spawn rolls a per-server-hop chance (computed).",
        },
        "vendors": {
            "general": {
                "list_edid": "LLV_Vendor_Drink",
                "list_id": "003C24EA",
                "mechanism": (
                    "Cream is 1 of 3 items in LL_Drink_NonAlcohol_Basic_NoWater "
                    "(pick-one), so its per-roll pick rate from the drink pool is "
                    "~33%."
                ),
                "note": (
                    "Cream's per-roll pick rate from a general vendor's drink pool "
                    "(computed)."
                ),
            },
            "raider": {
                "list_edid": "LLV_Vendor_Healing_Faction_Raiders",
                "list_id": "003D7F7C",
                "mechanism": (
                    "In the raider healing pool cream sits at ChanceNone 85, i.e. a "
                    "15% per-roll pick rate."
                ),
                "note": (
                    "Cream's per-roll pick rate in the raider healing pool "
                    "(computed)."
                ),
            },
            "vera": {
                "list_edid": "Vendor_Moon_Vera_BlueRidge",
                "list_id": "00695804",
                "qty": 3,
                "location": "Middle Mountain Pitstop",
                "region": "Savage Divide",
                "note": "Vera always stocks 3 cream.",
            },
        },
        "resource_generators": None,
    },
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.50,
        "object_type": "Drink",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.28},
            {"rank": 2, "reduction": "90%", "weight": 0.05},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.05,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.40},
            {"pieces": 2, "reduction": "40%", "weight": 0.30},
            {"pieces": 3, "reduction": "60%", "weight": 0.20},
            {"pieces": 4, "reduction": "80%", "weight": 0.10},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.05},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
    # ── Used For ────────────────────────────────────────────────────────
    # GENERATED at build time by build_farming_used_for.py (post-step, runs
    # after build_recipe_guide_json.py).  Do NOT hand-write it here: the
    # generator derives consumption effects (ALCH exports), recipes
    # (recipe_guide.json) and challenges (challenges.json) from the item's
    # name + form ID above and injects `used_for` into the spawn JSON.
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
            "note": (
                "37 LPI spawn points for the raw egg roll a per-server-hop "
                "chance each (computed). 5 raw eggs and 12 cracked eggs are "
                "fixed static world spawns."
            ),
        },
        "containers": {
            "container_edid": "Container_Loot_DeathclawNest01",
            "container_id": "003099CD",
            "nest_list_edid": "LL_Deathclaw_Nest",
            "nest_list_id": "001B0084",
            "chance_none_glob": "ItemTwo_High_ChanceNone_Tier",
            "count": 19,
            "note": (
                "19 deathclaw nest containers (12 DeathclawNest01 + 7 SFM04). "
                "Each nest's chance to contain 1 raw egg + 1 cracked egg is "
                "computed from the ItemTwo_High_ChanceNone_Tier GLOB."
            ),
        },
        "vendors": {
            "general": {
                "list_edid": "LLV_Vendor_Junk_Small_Rare",
                "list_id": "000757BD",
                "mechanism": (
                    "Cracked Deathclaw Egg is 1 of 40 items in the junk vendor "
                    "rare pool (pick-one). Raw eggs are not sold by any vendor."
                ),
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
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 00046939: Weight 0.25, ObjectTypeFood, IngredientTypeMeat,
    # IngredientTypeEgg, ObjectTypeCanSpoil → spoils to Spoiled Meat.
    # No ObjectTypeNonPerishable → perishable.
    # IngredientTypeMeat → Butcher's Bounty yield perk applies.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 0.25,
        "object_type": "Food",
        "yield_perk": "butchers_bounty",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": True,
    },
}

# ── FROG EGG ────────────────────────────────────────────────────────────────────
FROG_EGG = {
    "slug": "frog-egg",
    "name": "Frog Egg",
    "page_title": "Frog Egg Spawn Locations",
    "blurb": "Every known world spawn for Frog Eggs, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "004FE51C",
            "edid": "RadFrogEgg",
            "full": "Frog Egg",
            "sig": "ALCH",
        },
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # Frog Egg (ALCH 004FE51C) drops exclusively from Rad Frogs via creature
    # loot list LLD_Creature_Radfrog (003B9761, UseAll, 5 entries):
    #   Entry 1: RadFrogEgg (CN=0, qty=1) → guaranteed 1 egg per kill
    #   Entry 3: RadFrogEgg (CN=50, qty=1) → 50% chance of a second egg
    #
    # No world-placed food spawns (no LPI list).
    # No vendor pools.
    # Fasnacht activator E01F_FasnachtFrogEggCluster (00478DED) is event-only.
    "drop_rates": {
        "world_spawns": None,
        "creature_drops": {
            "list_edid": "LLD_Creature_Radfrog",
            "list_id": "003B9761",
            "mechanism": (
                "UseAll list with 5 entries. Entry 1: RadFrogEgg at CN=0 (guaranteed). "
                "Entry 3: RadFrogEgg at CN=50 (50% chance of a second egg)."
            ),
            "note": (
                "Every rad frog kill guarantees 1 frog egg. There is a 50% chance "
                "of receiving a second egg from the same kill. No world-placed "
                "spawns or vendor sources exist."
            ),
        },
        "vendors": None,
        "resource_generators": None,
    },
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 004FE51C: Weight 0.25, ObjectTypeFood, IngredientTypeMeat,
    # IngredientTypeEgg, ObjectTypeCanSpoil → spoils to Spoiled Meat.
    # IngredientTypeMeat → Butcher's Bounty yield perk applies.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 0.25,
        "object_type": "Food",
        "yield_perk": "butchers_bounty",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": True,
    },
}

# ── MIRELURK EGG ────────────────────────────────────────────────────────────────
MIRELURK_EGG = {
    "slug": "mirelurk-egg",
    "name": "Mirelurk Egg",
    "page_title": "Mirelurk Egg Spawn Locations",
    "blurb": "Every known world spawn for Mirelurk Eggs (harvestable and hatching), grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "0023E9D4",
            "edid": "MirelurkEgg",
            "full": "Mirelurk Egg",
            "sig": "ALCH",
        },
    ],
    # Extra world bases: the harvestable ACTI is found through ALCH refs, but the
    # hatching variant (0016579B) is not referenced by the ALCH record, so we inject
    # it manually for Mappalachia Position lookup.
    "extra_world_bases": [
        {
            "formid": "0016579B",
            "edid": "MirelurkEgg_Hatching",
            "sig": "ACTI",
            "source_type": "harvestable",
        },
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # World spawns: MirelurkEgg_Harvestable (001715CD, ~95 ACTI REFRs) gives
    # the egg directly via MirelurkHarvestableScript.  MirelurkEgg_Hatching
    # (0016579B, ~79 REFRs) primarily spawns mirelurks but is also harvestable.
    # Both are 100% guaranteed on activation.
    #
    # Vendor: LLV_Vendor_Food_Whitespring_Rare (0037D966, UseAll 32 entries)
    #   entry 7: MirelurkEgg (CN=0, qty=1) → guaranteed 1 at Whitespring vendor.
    # Also in LL_Food_Any_Rare (004E0FEE, UseAll 24 entries) as rare food loot.
    #
    # Random qty list: LLS_MirelurkEgg_RndAmount (00434585, pick-one 3 entries)
    #   picks 1, 2, or 3 eggs equally (used by TWZ05 quest reward × qty 3).
    "drop_rates": {
        "world_spawns": {
            "list_edid": "MirelurkEgg_Harvestable",
            "list_id": "001715CD",
            "note": (
                "~95 harvestable egg ACTIs and ~79 hatching egg ACTIs in the world. "
                "Each activation guarantees 1 mirelurk egg. Hatching eggs also "
                "spawn a mirelurk hatchling."
            ),
        },
        "vendors": {
            "whitespring": {
                "list_edid": "LLV_Vendor_Food_Whitespring_Rare",
                "list_id": "0037D966",
                "qty": 1,
                "note": (
                    "Whitespring food vendor can stock 1 mirelurk egg from its "
                    "rare food pool (per-reset chance computed)."
                ),
            },
        },
        "resource_generators": None,
    },
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 0023E9D4: Weight 1.00, ObjectTypeFood, IngredientTypeMeat,
    # IngredientTypeEgg, ObjectTypeCanSpoil → spoils to Spoiled Meat.
    # IngredientTypeMeat → Butcher's Bounty yield perk applies.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 1.00,
        "object_type": "Food",
        "yield_perk": "butchers_bounty",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.55},
            {"rank": 2, "reduction": "90%", "weight": 0.10},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.10,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.80},
            {"pieces": 2, "reduction": "40%", "weight": 0.60},
            {"pieces": 3, "reduction": "60%", "weight": 0.40},
            {"pieces": 4, "reduction": "80%", "weight": 0.20},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.10},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": True,
    },
}

# ── MOTHMAN EGG ─────────────────────────────────────────────────────────────────
MOTHMAN_EGG = {
    "slug": "mothman-egg",
    "name": "Mothman Egg",
    "page_title": "Mothman Egg Spawn Locations",
    "blurb": "Every known world spawn for Mothman Eggs (regular and enlightened flora), grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "0008E922",
            "edid": "MothmanEgg",
            "full": "Mothman Egg",
            "sig": "ALCH",
        },
    ],
    # Extra world bases: the FLOR records (UseLPI_FloraMothmanEggs01-03) are in the
    # ALCH refs but FLOR is now in PLACED_SIGS.  The LPI LVLI wrappers whose REFRs
    # hold the actual world coordinates need injecting — the closure walk from ALCH
    # doesn't reach them (chain is LPI→FLOR→ALCH, not ALCH→LPI).
    # Also add the enlightened mothman egg flora LPIs (E07A event).
    "extra_world_bases": [
        {"formid": "0035D23F", "edid": "LPI_FloraMothmanEggs01", "sig": "LVLI", "source_type": "flora"},
        {"formid": "0035D237", "edid": "LPI_FloraMothmanEggs02", "sig": "LVLI", "source_type": "flora"},
        {"formid": "0035D22B", "edid": "LPI_FloraMothmanEggs03", "sig": "LVLI", "source_type": "flora"},
        {"formid": "006189AC", "edid": "E07A_Mothman_LPI_FloraEnlightenedMothmanEggs01", "sig": "LVLI", "source_type": "enlightened-flora"},
        {"formid": "006189AD", "edid": "E07A_Mothman_LPI_FloraEnlightenedMothmanEggs02", "sig": "LVLI", "source_type": "enlightened-flora"},
        {"formid": "006189AE", "edid": "E07A_Mothman_LPI_FloraEnlightenedMothmanEggs03", "sig": "LVLI", "source_type": "enlightened-flora"},
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # World spawns: mothman egg flora via three LPI lists:
    #   LPI_FloraMothmanEggs01 (0035D23F, 24 REFRs)
    #   LPI_FloraMothmanEggs02 (0035D237, 21 REFRs)
    #   LPI_FloraMothmanEggs03 (0035D22B, 21 REFRs)
    #   Total: 66 regular flora placements.
    # Each uses FirstMatch (bit 6) with 4 entries for nuked/storm/radstorm/normal
    # variants.  The normal flora (UseLPI_FloraMothmanEggs01) is the default
    # when no weather condition is active.  MaxValue = Container_MaxCount_Single_Tier
    # (max_count=1).  Each harvest yields 1 egg, respawns on flora timer.
    #
    # Enlightened mothman egg flora (E07A Mothman Equinox event):
    #   E07A_LPI_FloraEnlightenedMothmanEggs01-03 (23 REFRs total)
    #   These yield Perfect Mothman Egg (006238EE) during the event.
    #
    # Creature drops: NONE.  LLD_Creature_Mothman_Wise (003EC2F4) has MothmanEgg
    #   at entry 2, but the Wise Mothman is a non-hostile NPC — players cannot
    #   kill it, so the death item never fires.  Regular mothman death loot
    #   (LLD_Creature_Mothman 0033AB91) contains only NeurotoxicDust, MothmanWing,
    #   and components — no eggs.
    #
    # Event loot: E07A_Mothman_CultistHighPriestReward_Loot (00635071, UseAll 9)
    #   Entry 6: MothmanEgg (CN=0, qty=1) → guaranteed 1 egg
    #   Entry 7: MothmanEgg (CN=80, qty=2) → 20% chance of 2 more eggs
    #   Entry 5: Perfect Mothman Egg (CN=95, qty=1) → 5% chance
    #   (Quest-assigned loot, not a creature death item.)
    #
    # CAMP collectron: ATX_Resources_MothmanNest_egg (006F7E94, pick-one 1 entry)
    #   → 100% per cycle (dedicated Mothman Nest collectron).
    # Liberated collectron: ATX_Resources_Collectron_Liberated (0084CCA6, pick-one 4)
    #   → 25% per cycle (1 of 4 items).
    "drop_rates": {
        "world_spawns": {
            "list_edid": "LPI_FloraMothmanEggs01/02/03",
            "list_ids": ["0035D23F", "0035D237", "0035D22B"],
            "note": (
                "66 mothman egg flora placements across 3 LPI lists. Each harvest "
                "yields 1 egg; respawns on the standard flora timer. 23 additional "
                "enlightened flora placements (E07A event) yield Perfect Mothman Eggs."
            ),
        },
        "creature_drops": None,  # Wise Mothman is non-hostile (can't be killed); regular mothman drops no eggs.
        "event_loot": {
            "list_edid": "E07A_Mothman_CultistHighPriestReward_Loot",
            "list_id": "00635071",
            "note": (
                "Mothman Equinox High Priest reward (quest-assigned, UseAll 9 entries): "
                "1 guaranteed mothman egg + 20% chance of 2 more + 5% chance of a "
                "Perfect Mothman Egg."
            ),
        },
        "vendors": None,
        "resource_generators": {
            "note": (
                "Mothman Nest collectron produces 1 egg per cycle (100%). "
                "Liberated collectron has a 25% chance per cycle (1 of 4 items)."
            ),
        },
    },
    # ── Additional Expands (root-level sections on the same page) ─────
    # Perfect Mothman Egg is a separate ALCH item (006238EE) obtainable only
    # during the Mothman Equinox event.  It gets its own root-level expand
    # on the Mothman Egg guide page.
    "additional_expands": [
        {
            "title": "Perfect Mothman Eggs",
            "formid": "006238EE",
            "edid": "E07A_Mothman_EnlightenedMothmanEgg",
            "full": "Perfect Mothman Egg",
            "sig": "ALCH",
            "weight": 0.25,
            "value": 15,
            "keywords": [
                "ObjectTypeFood", "MealTypeRaw", "IngredientTypeEgg",
                "IngredientTypeMeat", "FoodTypeMothmanEgg", "PlantTypeMothmanEggs",
                "UnsellableObject", "NonPlayerTradable", "ObjectTypeCanSpoil",
            ],
            "spoils_to": "Spoiled Meat",
            "note": (
                "Perfect Mothman Eggs are an event-exclusive variant obtainable "
                "only during the Mothman Equinox seasonal event. They cannot be "
                "traded between players (NonPlayerTradable) and cannot be sold to "
                "vendors (UnsellableObject)."
            ),
            "how_to_obtain": [
                {
                    "source": "Enlightened Mothman Egg Flora",
                    "source_type": "flora",
                    "details": (
                        "23 enlightened mothman egg flora placements across 3 LPI "
                        "lists (E07A_Mothman_LPI_FloraEnlightenedMothmanEggs01–03, "
                        "8 + 8 + 7 REFRs). Each harvest yields 1 Perfect Mothman Egg. "
                        "Only active during the Mothman Equinox event."
                    ),
                    "list_ids": ["006189AC", "006189AD", "006189AE"],
                    "flora_ids": ["006189AA", "006189A9", "006189A8"],
                },
                {
                    "source": "Cultist High Priest Reward (Mothman Equinox)",
                    "source_type": "event_loot",
                    "details": (
                        "E07A_Mothman_CultistHighPriestReward_Loot (00635071, "
                        "UseAll 9 entries): entry 5 has Perfect Mothman Egg with "
                        "CN=95 → 5% chance of 1 per kill. The same loot list also "
                        "drops regular Mothman Eggs (1 guaranteed + 20% chance of "
                        "2 more)."
                    ),
                    "list_id": "00635071",
                    "chance": "5%",
                },
            ],
        },
    ],
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 0008E922: Weight 0.25, ObjectTypeFood, IngredientTypeMeat,
    # IngredientTypeEgg, FoodTypeMothmanEgg, PlantTypeMothmanEggs,
    # ObjectTypeCanSpoil → spoils to Spoiled Meat.
    # IngredientTypeMeat → Butcher's Bounty yield perk applies.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 0.25,
        "object_type": "Food",
        "yield_perk": "butchers_bounty",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": True,
    },
}

# ── RADSCORPION EGG ─────────────────────────────────────────────────────────────
RADSCORPION_EGG = {
    "slug": "radscorpion-egg",
    "name": "Radscorpion Egg",
    "page_title": "Radscorpion Egg Spawn Locations",
    "blurb": "Every known world spawn for Radscorpion Eggs, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "0004693B",
            "edid": "RadscorpionEgg",
            "full": "Radscorpion Egg",
            "sig": "ALCH",
        },
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # World-placed food: LPI_Food_RadscorpionEgg (0062A883, 24 REFRs — ALL
    # interior in Carleton Mine).  ChanceNone 50 via GLOB
    # LPI_Chance_Food_FruitVegetables (003CA90F).
    #   → LL_Food_Single_RadscorpionEgg (0062A884, 1 entry CN=0) → egg.
    #
    # Creature drop: LLD_Creature_Radscorpion (00074D11, UseAll 5 entries)
    #   Entry 3: RadscorpionEgg (CN=75, qty=1) → 25% chance per kill.
    #   Also zzz_LLD_Creature_Radscorpion_Prime (005C4F17) identical structure.
    #
    # Vendor: LLV_Vendor_Food_Whitespring_Unique (0037D967, UseAll 5 entries)
    #   entry 4: RadscorpionEgg (CN=0, qty=1) → guaranteed 1 at Whitespring.
    #
    # Also in LL_Food_Any_Rare (004E0FEE, entry 4) as rare food loot.
    #
    # Mystery Crate: LL_InsectParts (007AC77E, pick-one 52 entries)
    #   entries 18+44: RadscorpionEgg → ~3.8% per crate (2/52).
    "drop_rates": {
        "world_spawns": {
            "list_edid": "LPI_Food_RadscorpionEgg",
            "list_id": "0062A883",
            "chance_none_glob": "LPI_Chance_Food_FruitVegetables",
            "note": (
                "24 LPI spawn points (all interior — Carleton Mine) roll a "
                "per-server-hop chance each (computed)."
            ),
        },
        "creature_drops": {
            "list_edid": "LLD_Creature_Radscorpion",
            "list_id": "00074D11",
            "note": (
                "Radscorpion kills have a chance to drop 1 egg (entry 3, "
                "ChanceNone 75). Same rate for Prime radscorpions."
            ),
        },
        "vendors": {
            "whitespring": {
                "list_edid": "LLV_Vendor_Food_Whitespring_Unique",
                "list_id": "0037D967",
                "qty": 1,
                "note": (
                    "Whitespring food vendor can stock 1 radscorpion egg from its "
                    "unique food pool (per-reset chance computed)."
                ),
            },
        },
        "resource_generators": None,
    },
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 0004693B: Weight 0.25, ObjectTypeFood, IngredientTypeMeat,
    # IngredientTypeEgg, ObjectTypeCanSpoil → spoils to Spoiled Meat.
    # IngredientTypeMeat → Butcher's Bounty yield perk applies.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 0.25,
        "object_type": "Food",
        "yield_perk": "butchers_bounty",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": True,
    },
}

# ── RADTOAD EGG ─────────────────────────────────────────────────────────────────
RADTOAD_EGG = {
    "slug": "radtoad-egg",
    "name": "Radtoad Egg",
    "page_title": "Radtoad Egg Spawn Locations",
    "blurb": "Every known world spawn for Radtoad Eggs, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "00295BE5",
            "edid": "RadToadEgg",
            "full": "Radtoad Egg",
            "sig": "ALCH",
        },
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # Radtoad Egg (ALCH 00295BE5) drops from Radtoads via creature loot:
    #   LLD_Creature_Radtoad (00047185, UseAll 5 entries):
    #     Entry 0: RadToadEgg (CN=0, qty=1) → guaranteed 1 egg per kill.
    #   Also zzz_LLD_Creature_Radtoad_Prime (005C4F1C) identical structure.
    #
    # Secondary egg chance via sub-list chain:
    #   LLS_Creature_RadToad → LLE_Creature_RadToad (0056093C, UseAll 3 entries)
    #     Entry 2: RadToadEgg (CN=50, qty=1) → 50% chance of a bonus egg.
    #   But LLS_Creature_RadToad has level-scaled ChanceNone (80/70/60/40% at
    #   levels 1/18/28/40+), so the effective bonus egg rate at max level is
    #   60% × 50% = 30%.
    #
    # Fasnacht event: LLQ_E01F_Fasnacht_EggClusters (003EE383, UseAll 8 entries)
    #   5 guaranteed eggs + 3 at 50% each → 5–8 eggs per cluster.
    #   Event-only, not a regular world spawn.
    #
    # No world-placed food spawns (no LPI list).
    # No vendor pools.
    "drop_rates": {
        "world_spawns": None,
        "creature_drops": {
            "list_edid": "LLD_Creature_Radtoad",
            "list_id": "00047185",
            "mechanism": (
                "UseAll list with 5 entries. Entry 0: RadToadEgg at CN=0 "
                "(guaranteed 1 egg). Additional bonus egg via LLE_Creature_RadToad "
                "entry 2 (CN=50) gated by LLS_Creature_RadToad level-scaled "
                "ChanceNone (60% fire rate at level 40+)."
            ),
            "note": (
                "Every radtoad kill guarantees 1 egg. At max level there is "
                "roughly a 30% chance of a bonus egg (60% list fire × 50% entry "
                "chance). No world-placed spawns or vendor sources exist."
            ),
        },
        "vendors": None,
        "resource_generators": None,
    },
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 00295BE5: Weight 0.25, ObjectTypeFood, IngredientTypeMeat,
    # IngredientTypeEgg, ObjectTypeCanSpoil → spoils to Spoiled Meat.
    # IngredientTypeMeat → Butcher's Bounty yield perk applies.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 0.25,
        "object_type": "Food",
        "yield_perk": "butchers_bounty",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": True,
    },
}

# All sets in this family — add new items here.
# The generic build script (build_farming_spawns_json.py) picks them up automatically.
ALL_SETS = [CREAM, DEATHCLAW_EGG, FROG_EGG, MIRELURK_EGG, MOTHMAN_EGG,
            RADSCORPION_EGG, RADTOAD_EGG]

# Slug → config dict, for --item <slug> lookup in the build script.
SETS_BY_SLUG = {s["slug"]: s for s in ALL_SETS}

# The ten regions every page renders, in A-Z order (empty ones get a "no spawns" note).
ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]
