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
  Vendor pool: LLV_Vendor_Junk_Small_Rare (000757BD) — DEAD, debug-only list
  (referenced solely by DebugChallengeMisc_Junk CONT); no vendor stocks it.
"""

# Appalachia worldspace formID (the only worldspace with markers/regions in the DB).
APPALACHIA_SPACE = 2480661

# ── CREAM ────────────────────────────────────────────────────────────────────────
CREAM = {
    "slug": "cream",
    "name": "Cream",
    "page_title": "Cream Spawn Locations",
    "blurb": "Every known world spawn for Cream, grouped by region. Directions and photos are added by hand.",
    # Resolve Position by the item's own base FormID too (like Mappalachia), catching
    # any directly world-placed base the ref columns miss. Needs a local DB reseed.
    "place_item_bases": True,
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
    "blurb": "Every known world spawn for Deathclaw Eggs, grouped by region. Directions and photos are added by hand.",
    # Fixed Spawn markers mix loose raw eggs and nests, so each marker gets a
    # per-type breakdown (count + rate) built by
    # spawns_configs.farming._attach_breakdowns.
    "per_marker_breakdown": True,
    # Also resolve Position by each item's OWN base FormID (like Mappalachia), so a
    # directly world-placed egg the ref columns miss — e.g. the raw egg in Vault 63 —
    # is caught. world_source_type tags those base placements as guaranteed (100%)
    # static spawns. REQUIRES a local geo-cache reseed with the Mappalachia DB.
    "place_item_bases": True,
    "items": [
        {
            "formid": "00046939",
            "edid": "DeathclawEgg",
            "full": "Deathclaw Egg",
            "sig": "ALCH",
            "world_source_type": "static-raw",
        },
        {
            "formid": "0014F6AC",
            "edid": "DeathclawEggCracked",
            "full": "Cracked Deathclaw Egg",
            "sig": "MISC",
            "world_source_type": "static-cracked",
            # Cracked eggs are a nest BY-PRODUCT, not a farmable world point, so
            # their own 12 loose placements are dropped from Fixed Spawn Locations
            # (spawns_configs.farming._drop_excluded). The item stays here so the
            # LVLI closure, nest yield, used_for and vendor data still see it.
            "exclude_from_fixed_spawns": True,
        },
    ],
    # ── Info notes (rendered as their own expand, no spawn data) ────────
    "info_notes": [
        {
            "title": "Healthy Deathclaw Egg",
            "body": (
                "The <b>Healthy Deathclaw Egg</b> is a separate item "
                "(<code>Burn_E01_HealthyDeathclawEgg</code>) tied to the "
                "Burning Springs <b>Rust Kingdom</b> “Don’t Feed the "
                "Deathclaws” event. It is a scripted event item, not a "
                "farmable world spawn — it has no map locations, leveled-list "
                "roll, or vendor source, so it does not appear on the map above."
            ),
        },
    ],
    # ── Random Encounters (hand-authored: RE cells aren't in the exports /
    #    Mappalachia, so these Object REs are typed here) ─────────────────
    # "Object" random encounters place a STATIC Deathclaw nest object at a fixed
    # Appalachia random-encounter spot. They render in their own "Random
    # Encounters" root expand (slotted alphabetically). Rate is the same nest
    # loot as a fixed nest — the encounter just decides WHERE the nest appears.
    "random_encounters_intro": (
        "Object random encounters place a <b>static Deathclaw nest</b> at a fixed "
        "Appalachia random-encounter spot. Each nest rolls the same loot as any "
        "other Deathclaw nest — the encounter just decides where the nest appears."
    ),
    "random_encounters": [
        {
            "name": "Deathclaw Nest vs Super Mutants",
            "type": "Object",
            "edid": "RE_ObjectMP10",
            "formid": "0035EBED",
            "note": (
                "An Object random encounter that stages a static Deathclaw nest "
                "under attack by Super Mutants."
            ),
        },
        {
            "name": "Deathclaw Nest vs Scorched",
            "type": "Object",
            "edid": "RE_ObjectMP09",
            "formid": "0035EC0F",
            "note": (
                "An Object random encounter that stages a static Deathclaw nest "
                "against the Scorched."
            ),
        },
        {
            "name": "Deathclaw vs Merchant Caravan",
            "type": "Object",
            "edid": "RE_ObjectMP06",
            "formid": "0035EBF0",
            "note": (
                "An Object random encounter — a Deathclaw and its nest set against "
                "a merchant caravan."
            ),
        },
    ],
    # ── Activities, Events & Quests — hand-authored extras ──────────────
    # The engine's Events & Activities detection walks LVLI reward pools; the
    # Liebowitz hand-in has NO reward-list edge (it's a dialogue hand-in during
    # the Hunter for Hire questline), so it can't be auto-detected and is typed
    # here. `manual: True` tells spawns_engine.events.resolve_event_rates to keep
    # the row verbatim instead of rng76-resolving/dropping it.
    "events_activities_extra": [
        {
            "manual": True,
            "name": "Hunter for Hire — Liebowitz's Deathclaw Egg",
            "type": "Quest",
            "edid": "W05_MQ_002P_Radical_TylerCounty",
            "rate_display": "One-time hand-in",
            "rate_lines": ["One-time hand-in"],
            "rate_value": None,
            "note": (
                "Optional sub-quest under the main questline Hunter for Hire "
                "(given by Duchess at the Wayward). Liebowitz, found on the "
                "rooftops of Tyler County Fairgrounds, asks you to retrieve a "
                "Deathclaw Egg from Deathclaw Island. You can keep the egg instead "
                "of handing it over by passing the required SPECIAL checks. Not "
                "repeatable."
            ),
        },
    ],
    # ── Marker renames (display only) ──────────────────────────────────
    # Mappalachia names the nest marker at ref 2C089D "Excelsior Model Home",
    # but the DF directions were written from Ella Ames' Bunker (the same spot),
    # so the page keeps the DF naming. Applied after breakdowns in
    # spawns_configs.farming; per-spawn directions survive the rename (they are
    # keyed by ref, and the preservation lookup is aliased to the old name).
    "marker_renames": {
        "Excelsior Model Home": "Ella Ames' Bunker",
    },
    # ── Drop rates (resolved from LVLI TSVs, July 2026) ─────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # Raw egg (ALCH 00046939) placed via LPI_Food_DeathclawEgg (002118C0,
    # 37 world REFRs).  ChanceNone = 50 (GLOB LPI_Chance_Food_FruitVegetables).
    # Also 5 direct ALCH world REFRs (100% fixed spawns).
    #
    # Cracked egg (MISC 0014F6AC) has 12 direct MISC world REFRs. They are excluded
    # from Fixed Spawn Locations (see exclude_from_fixed_spawns above).
    # No vendor pool — see the "vendors" note below.
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
            # marker_label = the loose item these LPI points place (raw egg). Used
            # by the per-marker breakdown (rate_key "world_spawns" -> its %).
            "marker_label": "Deathclaw Egg",
            "note": (
                "Raw Deathclaw Eggs spawn loose at 36 LPI points, each rolling a "
                "per-server-hop chance (computed). Deathclaw nests are listed "
                "below with their own rate."
            ),
        },
        "containers": {
            "container_edid": "Container_Loot_DeathclawNest01",
            "container_id": "003099CD",
            "nest_list_edid": "LL_Deathclaw_Nest",
            "nest_list_id": "001B0084",
            "chance_none_glob": "ItemTwo_High_ChanceNone_Tier",
            "count": 19,
            # Deathclaw nests are fixed, returnable world points (a nest sits at a
            # POI you can come back to), so the nest rate renders inside the Fixed
            # Spawn Locations expand (as a second "per nest" row under the world-
            # spawn banner) and the Containers expand shows its empty state. The
            # 90% rate is still COMPUTED from the GLOB by build_farming_used_for.py;
            # this flag only controls WHERE the block is displayed.
            "as_fixed_spawn": True,
            # Per-marker breakdown label + yield note (rate_key "containers" -> 90%).
            "marker_label": "Deathclaw Nest",
            "marker_yield": "each gives 1 Raw + 1 Cracked Egg",
            "note": (
                "19 deathclaw nest containers (12 DeathclawNest01 + 7 SFM04). "
                "Each nest's chance to contain 1 raw egg + 1 cracked egg is "
                "computed from the ItemTwo_High_ChanceNone_Tier GLOB."
            ),
        },
        # NO VENDOR SELLS DEATHCLAW EGGS — verified Aug 2026 against
        # LVLI_Export_July_2026 + dist/vendors.json (112 vendors):
        #   Raw egg (00046939) is in NO vendor list at all. Its only parents are
        #     LLS_Deathclaw_egg (nest), LL_Food_Single_DeathclawEgg ->
        #     LPI_Food_DeathclawEgg (world spawns), and the 3 collectron pools.
        #   Cracked egg (0014F6AC) sits in LLV_Vendor_Junk_Small_Rare (000757BD),
        #     but that list is DEAD DATA: its only ReferencedBy is
        #     DebugChallengeMisc_Junk (00450D6D, CONT) — a debug container — it has
        #     no parent list, and not one vendor in the master stocks it. The old
        #     "junk vendors have a 2.5% chance" note here was derived from that
        #     unreachable list and was wrong; do not reinstate it.
        #   Cooked Deathclaw Egg (0004693C) IS vendored (May at Highway Town
        #     Interior; Mr Waiter / Ms Waitress at Whitespring Refuge) — different
        #     item, different page.
        # build_farming_used_for.py agrees independently: "vendors: 0".
        "vendors": None,
        # Collectrons (F.E.T.C.H. / Junkyard Dog / Sir Loin) are JOINED from
        # dist/collectrons.json at build time and render as cards in the
        # Collectrons expand — see _patch_camp_producers in
        # build_farming_used_for.py. Only the note below is hand-written.
        #
        # Sir Loin traced by hand, Aug 2026 (LVLI_Export_July_2026):
        #   ATX_Resources_Collectron_SirLoin (008CD162) — LVLF "001" (UseAll)
        #   + LVMV_MaxValue 1 => WATERFALL. Entry order is
        #     e0 SuperCommon (ATX_CNone_..._SuperCommon_ECON = 0)   -> drop 1.00
        #     e1 Uncommon    (ATX_CNone_Collectron_Scrap_Rare = 89) -> drop 0.11
        #     e2 Rare        (ATX_CNone_..._SirLoin_Rare_ECON = 99) -> drop 0.01
        #   The ChanceNone-0 entry sits FIRST, so it always wins the cascade and
        #   cum_fail hits 0 — the Rare pool (which holds DeathclawEgg, 1 of 8)
        #   can never roll. Same degenerate-waterfall data bug as the power
        #   armour mod recipe tiers (drop-rate-engine §13.14). We render what
        #   the data SAYS (0%) — never the number it "should" be. If Bethesda
        #   reorders the entries the join picks up the new rate on the next
        #   build with no code change; nothing here compensates for the bug.
        #
        # F.E.T.C.H. / Junkyard Dog for contrast: ChanceNone-0 SuperCommon is
        # LAST, so SuperRare is reachable — 0.05 x 0.30 x 0.75 = 1.125%, / 11
        # entries = 0.10227% per cycle. (The old "0.45%" note ignored the
        # waterfall cascade and used the bare 5% x 1/11.)
        # No note: the 0% on the Sir Loin card speaks for itself. Keep the
        # explanation here for maintainers, not on the page.
        "resource_generators": None,
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
    "place_item_bases": True,   # pull-by-base (like Mappalachia); needs a local DB reseed
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
    "place_item_bases": True,   # pull-by-base (like Mappalachia); needs a local DB reseed
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
    "place_item_bases": True,   # pull-by-base (like Mappalachia); needs a local DB reseed
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
        # Mothman Nest is a RESOURCE GENERATOR (produces in place) and the
        # Liberated Collectron Station is a COLLECTRON (gathers into its stash) —
        # different sources, different expands. Both are joined from the camp-item
        # exports at build time (_patch_camp_producers), so neither is typed here.
        "resource_generators": {},
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
    "place_item_bases": True,   # pull-by-base (like Mappalachia); needs a local DB reseed
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
    "place_item_bases": True,   # pull-by-base (like Mappalachia); needs a local DB reseed
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

# ── TICK BLOOD ─────────────────────────────────────────────────────────────────
TICK_BLOOD = {
    "slug": "tick-blood",
    "name": "Tick Blood",
    "page_title": "Tick Blood Spawn Locations",
    "blurb": "Every known world spawn for Tick Blood, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "003D7494",
            "edid": "TickBlood",
            "full": "Tick Blood",
            "sig": "ALCH",
        },
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.
    #
    # Tick Blood (ALCH 003D7494) drops from all tick variants via creature
    # death item LLD_Creature_Tick (0008EDE7, UseAll, 3 entries):
    #   Entry 0: TickBlood (CN=65, qty=1) → 35% chance of 1 tick blood per kill
    #   Entry 1: TickBloodSac (CN=0, qty=1) → guaranteed 1 tick blood sac (MISC)
    #   Entry 2: LLE_Creature_Small (CN=0) → generic small creature loot
    #
    # Tick variants: Tick (EncTick01Template), Foul Tick (EncTick02),
    # Wretched Tick (EncTick03), Vile Tick (EncTick04),
    # Glowing Tick (EncTick05_Glowing).  All share LLD_Creature_Tick.
    # LvlTick (002078FF) has 78 world placements.
    # LvlTickScorched (003AFE19) has 4 world placements.
    #
    # Direct world REFRs: 2 placed ALCH REFRs (0043A3AA, 0043A3AB).
    #
    # Resource generators:
    #   ATX_Resources_MorbidWell_Blood (00662E6F, pick-one 7 entries)
    #     → tick blood is 1 of 7 = ~14.3% per cycle (Morbid Well).
    #   LL_InsectParts (007AC77E, pick-one 52 entries)
    #     → tick blood at entries 23 + 49 = 2/52 = ~3.85% per Mystery Crate.
    #
    # No vendor pools stock tick blood.
    "drop_rates": {
        "world_spawns": None,
        "creature_drops": {
            "list_edid": "LLD_Creature_Tick",
            "list_id": "0008EDE7",
            "mechanism": (
                "UseAll list with 3 entries. Entry 0: TickBlood at CN=65 "
                "(35% chance of 1 tick blood). Entry 1: TickBloodSac at CN=0 "
                "(guaranteed 1 tick blood sac). Entry 2: LLE_Creature_Small "
                "(generic small creature loot)."
            ),
            "note": (
                "Every tick kill guarantees 1 tick blood sac. There is a 35% "
                "chance of also receiving 1 tick blood from the same kill. "
                "All tick variants (Tick, Foul Tick, Wretched Tick, Vile Tick, "
                "Glowing Tick) share the same death item list."
            ),
        },
        "vendors": None,
        # Morbid Well Collector cards in automatically from
        # dist/resource_producers.json (_patch_camp_producers). The note keeps
        # only what no station card covers.
        "resource_generators": {
            "note": (
                "Mystery Crate (Mire) has a ~3.85% chance per crate "
                "(2 of 52 entries in LL_InsectParts, pick-one)."
            ),
        },
    },
    # ── Farming Tips (TSV-derived, Aug 2026) ───────────────────────────
    # ALCH 003D7494: Weight 0.75, ObjectTypeDrink, MealTypeRaw,
    # DrinkTypeTeaIcon, IngredientTypeBlood.
    # No ObjectTypeNonPerishable but also no ObjectTypeCanSpoil and no
    # spoil item → does not spoil in practice.
    # Not meat/flora/canned → no yield perk applies.
    # ObjectTypeDrink → Thru-Hiker weight perk, Grocer's backpack.
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.75,
        "object_type": "Drink",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.41},
            {"rank": 2, "reduction": "90%", "weight": 0.08},
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.08,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.60},
            {"pieces": 2, "reduction": "40%", "weight": 0.45},
            {"pieces": 3, "reduction": "60%", "weight": 0.30},
            {"pieces": 4, "reduction": "80%", "weight": 0.15},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.08},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── BLOOD SAC ──────────────────────────────────────────────────────────────────
# Non-perishable junk / crafting component (MISC, not ALCH).  "Blood Sac" is the
# in-game name of BloodbugSacFilledRed (00028A63): weight 0.50, value 12, used to
# craft co_chem_SkeetoSpit.  Being a MISC junk item it has NO consumable ALCH data,
# so it renders NO "Used For → When Consumed" block and NO "Farming Tips" expand
# (per spawn-guide §9g/§9k: those are consumable-only).  build_farming_used_for.py
# still injects a `used_for.recipes` list (what Blood Sac is used to craft) and a
# `vendor_list` from the NPC2 vendor master.
BLOOD_SAC = {
    "slug": "blood-sac",
    "name": "Blood Sac",
    "page_title": "Blood Sac Spawn Locations",
    "blurb": "Every known world source for Blood Sac, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "00028A63",
            "edid": "BloodbugSacFilledRed",
            "full": "Blood Sac",
            "sig": "MISC",
        },
    ],
    # ── Drop rates (resolved from LVLI TSVs, August 2026) ──────────────────
    # Traced via the drop-rate-engine skill rules.  Blood Sac is NOT a dedicated
    # creature death drop and has NO world-placed food/LPI list — it is a member
    # of the shared "small junk (rare)" loot pool, so it can roll from any
    # container / creature that draws that pool, plus a few resource generators.
    #
    # Container / loot pool (chance loot):
    #   LLS_Loot_Junk_Small_Rare (000BF429) → LLS_Loot_Junk_Small_All (004F680D)
    #   / LLS_Loot_Junk_Rare_All (0059C7E4) → ... → LL_Junk_Small (0003678C).
    #   Blood Sac is a rare pick in the small-junk pool, so a very wide set of
    #   junk containers (footlockers, suitcases, crates, backpacks, dressers,
    #   lockers, safes …) can rarely contain it.
    #
    # Vendors (chance):
    #   LLV_Vendor_Junk_Small_Rare (000757BD) — junk vendors' rare pool can roll
    #   Blood Sac.  No named vendor stocks it on a guaranteed list.
    #
    # Creatures (chance):
    #   No dedicated creature list.  Blood Sac reaches generic small-creature junk
    #   loot through the same LL_Junk_Small ancestry (feral ghouls, anglers,
    #   gulpers, brahmin, radtoads, dogs, etc.), so many small creatures can very
    #   rarely drop it as junk.
    #
    # Resource generators (coordless — described here, not on the map):
    #   ATX_Resources_MorbidWell_Blood (00662E6F) — the Morbid Well CAMP generator
    #     produces Blood Sac as 1 of 7 blood items (~14.3% per cycle, pick-one).
    #   SCORE_S26_Resources_CultistWell_Blood (008F10E4) — the Season 26 Cultist
    #     Well generator's blood pool.
    #   LL_InsectParts (007AC77E) — Mystery Crate (Mire) insect-parts pool
    #     (~3.85% per crate, 2 of 52, pick-one).
    #   MILE_LL_Scavenger_Junk_Rare (0079B9C5) — Milepost Zero scavenger rare junk.
    #   Burn_BountyHunt_LL_Junk_Rare (00843A12) — Burning Springs bounty-hunt junk.
    "drop_rates": {
        "world_spawns": None,
        # Note-only Creatures expand (NOT creature_drops — that renders an
        # eggs-only "Creature Drops" card). Blood Sac has no dedicated creature
        # death list; it reaches generic small-creature junk loot through the
        # shared LL_Junk_Small ancestry. The actual kill spots are counted as
        # markers under Fixed Spawn Locations (source_type npc).
        "creatures": {
            "note": (
                "Blood Sac is not a guaranteed drop from any creature. It is a rare "
                "pick in the shared small-junk loot pool (LLS_Loot_Junk_Small_Rare, "
                "part of LL_Junk_Small), so many small creatures - feral ghouls, "
                "anglers, gulpers, brahmin, radtoads and others - can very rarely "
                "drop one as junk loot. Their locations are listed under Fixed Spawn "
                "Locations."
            ),
        },
        "containers": {
            "container_edid": "LLS_Loot_Junk_Small_Rare",
            "container_id": "000BF429",
            "nest_list_edid": "LL_Junk_Small",
            "nest_list_id": "0003678C",
            "note": (
                "A rare pick in the small-junk loot pool, so a wide range of junk "
                "containers (footlockers, suitcases, crates, backpacks, dressers, "
                "lockers and safes) can rarely contain a Blood Sac."
            ),
        },
        "vendors": {
            "general": {
                "list_edid": "LLV_Vendor_Junk_Small_Rare",
                "list_id": "000757BD",
                "mechanism": (
                    "Blood Sac sits in the junk vendors' rare pool "
                    "(LLV_Vendor_Junk_Small_Rare, pick-one). No named vendor "
                    "stocks it on a guaranteed list."
                ),
                "note": (
                    "Junk vendors have a small per-reset chance to stock a Blood "
                    "Sac from their rare junk pool (computed)."
                ),
            },
        },
        # Morbid Well Collector cards in automatically from
        # dist/resource_producers.json (_patch_camp_producers). The note keeps
        # only what no station card covers (the S26 Cultist Well is not in the
        # camp-item export yet).
        "resource_generators": {
            "note": (
                "The Season 26 Cultist Well generator has a matching blood pool. "
                "Blood Sac also appears in the Mire Mystery Crate at ~3.85% per "
                "crate (2 of 52 in LL_InsectParts, pick-one), in the Milepost Zero "
                "scavenger rare-junk pool, and in Burning Springs bounty-hunt "
                "junk rewards."
            ),
        },
    },
    # ── Farming Tips ────────────────────────────────────────────────────
    # OMITTED on purpose. Blood Sac is a MISC junk / crafting component with no
    # consumable ALCH data (no ObjectTypeFood/Drink/Chem), so per spawn-guide §9g
    # the Farming Tips expand does not apply. `farming_tips` is left unset (None).
    #
    # ── Used For ────────────────────────────────────────────────────────
    # GENERATED at build time by build_farming_used_for.py. For Blood Sac the
    # `consumption` block is None (no ALCH), but `recipes` lists what Blood Sac is
    # used to craft, and `vendor_list` is joined from the NPC2 vendor master.
}

# ── HOTDOG ─────────────────────────────────────────────────────────────────────
# Pre-war cooked-meat food (ALCH 00042828). Weight 1.0, value 12, keywords
# ObjectTypeFood + IngredientTypeMeat + MealTypeCooked + ObjectTypeCanSpoil →
# perishable. Mostly a fixed world spawn (174 direct REFRs placed at diners /
# grills / picnic spots) plus a small container presence. As a consumable it
# renders Used For (When Consumed + recipes) and Farming Tips; build_farming_used_for
# populates used_for.consumption from the ALCH record.
HOTDOG = {
    "slug": "hotdog",
    "name": "Hotdog",
    "page_title": "Hotdog Spawn Locations",
    "blurb": "Every known world spawn for Hotdog, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "00042828",
            "edid": "Hotdog",
            "full": "Hotdog",
            "sig": "ALCH",
        },
    ],
    # ── Drop rates ──────────────────────────────────────────────────────
    # Hotdog is placed directly in the world (direct REFRs = guaranteed fixed
    # spawns, routed to Fixed Spawn Locations), plus a small container presence.
    # No dedicated creature death list, no vendor pool, no event/activity pool.
    "drop_rates": {
        "world_spawns": None,     # direct REFRs are fixed 100% spawns, not a chance roll
        "containers": {
            "container_edid": "LL_Food_Any",
            "note": (
                "Hotdog is placed directly in the world at diners, grills and "
                "picnic spots (fixed spawns); a small number of food containers "
                "can also hold one."
            ),
        },
        "vendors": None,
        "resource_generators": None,
    },
    # ── Farming Tips (TSV-derived) ─────────────────────────────────────
    # ALCH 00042828: Weight 1.00, ObjectTypeFood + IngredientTypeMeat +
    # MealTypeCooked + ObjectTypeCanSpoil → perishable cooked food. It is placed
    # in the world (not butchered from a corpse or harvested), so no yield perk
    # applies. Food → Thru-Hiker weight perk / Grocer's backpack.
    "farming_tips": {
        "spoils": True,
        "spoil_duration_hours": None,
        "base_weight": 1.00,
        "object_type": "Food",
        "yield_perk": None,
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
    # ── Used For ────────────────────────────────────────────────────────
    # GENERATED at build time by build_farming_used_for.py — consumption (food
    # hunger effects), recipes and vendor_list are injected from the ALCH record.
}

# ── CANNED MEAT STEW (auto-added Aug 2026) ─────────────────────────────────
CANNED_MEAT_STEW = {
    "slug": "canned-meat-stew",
    "name": "Canned Meat Stew",
    "page_title": "Canned Meat Stew Spawn Locations",
    "blurb": "Every known world spawn for Canned Meat Stew, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "00343776",
            "edid": "FF06_Feed_CannedMeatTastyStew",
            "full": "Canned Meat Stew",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 1.0,
        "object_type": "Food",
        "yield_perk": "can_do",
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.55,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.1,
            },
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.1,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.8,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.6,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.4,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.2,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.1,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── GLOWING BLOOD (auto-added Aug 2026) ─────────────────────────────────
GLOWING_BLOOD = {
    "slug": "glowing-blood",
    "name": "Glowing Blood",
    "page_title": "Glowing Blood Spawn Locations",
    "blurb": "Every known world spawn for Glowing Blood, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "000E2F68",
            "edid": "GlowingOneBlood",
            "full": "Glowing Blood",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.3,
        "object_type": "Chem",
        "yield_perk": None,
        "weight_perk": "traveling_pharmacy",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.17,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.03,
            },
        ],
        "backpack_mod": "chemists",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.24,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.18,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.12,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.06,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.03,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── HONEYCOMB (auto-added Aug 2026) ─────────────────────────────────
HONEYCOMB = {
    "slug": "honeycomb",
    "name": "Honeycomb",
    "page_title": "Honeycomb Spawn Locations",
    "blurb": "Every known world spawn for Honeycomb, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "004E722A",
            "edid": "HoneyComb",
            "full": "Honeycomb",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.25,
        "object_type": "Food",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.14,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.03,
            },
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.03,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.2,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.15,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.1,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.05,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.03,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── PERFECT BUBBLEGUM (auto-added Aug 2026) ─────────────────────────────────
PERFECT_BUBBLEGUM = {
    "slug": "perfect-bubblegum",
    "name": "Perfect Bubblegum",
    "page_title": "Perfect Bubblegum Spawn Locations",
    "blurb": "Every known world spawn for Perfect Bubblegum, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "005EE7EC",
            "edid": "SCORE_Bubblegum_Perfect",
            "full": "Perfect Bubblegum",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.1,
        "object_type": "Food",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.06,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.01,
            },
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.01,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.08,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.06,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.04,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.02,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.01,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── PURIFIED WATER (auto-added Aug 2026) ─────────────────────────────────
PURIFIED_WATER = {
    "slug": "purified-water",
    "name": "Purified Water",
    "page_title": "Purified Water Spawn Locations",
    "blurb": "Every known world spawn for Purified Water, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "000366C0",
            "edid": "WaterPurified",
            "full": "Purified Water",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.5,
        "object_type": "Drink",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.28,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.05,
            },
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.05,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.4,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.3,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.2,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.1,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.05,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── ROYAL JELLY (auto-added Aug 2026) ─────────────────────────────────
ROYAL_JELLY = {
    "slug": "royal-jelly",
    "name": "Royal Jelly",
    "page_title": "Royal Jelly Spawn Locations",
    "blurb": "Every known world spawn for Royal Jelly, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "00329C9E",
            "edid": "HoneyBeastRoyalJelly",
            "full": "Royal Jelly",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.5,
        "object_type": "Food",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.28,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.05,
            },
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.05,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.4,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.3,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.2,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.1,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.05,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── SUGAR BOMBS (auto-added Aug 2026) ─────────────────────────────────
SUGAR_BOMBS = {
    "slug": "sugar-bombs",
    "name": "Sugar Bombs",
    "page_title": "Sugar Bombs Spawn Locations",
    "blurb": "Every known world spawn for Sugar Bombs, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "000330F2",
            "edid": "SugarBombs",
            "full": "Sugar Bombs",
            "sig": "ALCH",
        },
    ],
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.5,
        "object_type": "Food",
        "yield_perk": None,
        "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {
                "rank": 1,
                "reduction": "45%",
                "weight": 0.28,
            },
            {
                "rank": 2,
                "reduction": "90%",
                "weight": 0.05,
            },
        ],
        "backpack_mod": "grocers",
        "backpack_weight": 0.05,
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.2,
        "armour_mod_weights": [
            {
                "pieces": 1,
                "reduction": "20%",
                "weight": 0.4,
            },
            {
                "pieces": 2,
                "reduction": "40%",
                "weight": 0.3,
            },
            {
                "pieces": 3,
                "reduction": "60%",
                "weight": 0.2,
            },
            {
                "pieces": 4,
                "reduction": "80%",
                "weight": 0.1,
            },
            {
                "pieces": 5,
                "reduction": "90% cap",
                "weight": 0.05,
            },
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    },
}

# ── SALT, PEPPER, SUGAR & SPICES (combined 4-item page) ─────────────────────────
# Four cooking-flavour seasonings on ONE page (each its own Used-For expand). All
# ObjectTypeFood + IngredientTypeFlavor, weight 0.25, non-perishable. The engine
# resolves the COMBINED fixed-spawn set across all four; the renderer shows a
# region INDEX (links to the per-region pages) on this page instead of the full
# marker dump. `multi_item` drives the per-item Used For expands; `fixed_spawn_index`
# turns Fixed Spawn Locations into a region index (see df-bnb-farming-non-perishable-guide.js).
SALT_PEPPER_SUGAR_SPICES = {
    "slug": "salt-pepper-spices-sugar",
    "name": "Salt, Pepper, Sugar & Spices",
    "page_title": "Salt, Pepper, Sugar & Spices Spawn Locations",
    "blurb": "Every known world spawn for Salt, Pepper, Sugar and Spices, indexed by region. Per-region location guides are linked below.",
    "items": [
        {"formid": "00118613", "edid": "CookingFlavor_Salt",   "full": "Salt",   "sig": "ALCH"},
        {"formid": "00118617", "edid": "CookingFlavor_Pepper", "full": "Pepper", "sig": "ALCH"},
        {"formid": "00118614", "edid": "CookingFlavor_Sugar",  "full": "Sugar",  "sig": "ALCH"},
        {"formid": "0011863E", "edid": "CookingFlavor_Spices", "full": "Spices", "sig": "ALCH"},
    ],
    # Per-item consumption expands + region-index Fixed Spawn (post-processed by
    # build_salt_pepper_spices_sugar_spawns_json.py after the used_for step).
    "multi_item": True,
    # One shape for every region-page family: {item}/location-guide/{region}/.
    # Matches Bobbleheads and Magazines on DF. The old flat form
    # (".../salt-pepper-spices-sugar-ash-heap/") is redirected, and the renderer
    # still parses it so existing links keep resolving.
    "region_index_base": "/bnb/farming/non-perishable/salt-pepper-spices-sugar/location-guide/",
    "farming_tips": {
        "spoils": False, "spoil_duration_hours": None, "base_weight": 0.25,
        "object_type": "Food", "yield_perk": None, "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers", "backpack_weight": 0.03,
        "armour_mod": "thru_hikers", "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False, "good_with_salt": False,
    },
}

# ── HONEY (004E7229) ────────────────────────────────────────────────────────────
# The single real consumable "Honey" (ObjectTypeFood + MealTypeRaw, weight 0.25,
# non-perishable). Distinct from Mountain Honey (a moonshine drink), Royal Jelly /
# Royal Jelly Taffy, Honeycomb and Blackberry Honey Crisp.
HONEY = {
    "slug": "honey",
    "name": "Honey",
    "page_title": "Honey Spawn Locations",
    "blurb": "Every known world spawn for Honey, grouped by region. Directions and photos are added by hand.",
    "items": [
        {"formid": "004E7229", "edid": "Honey", "full": "Honey", "sig": "ALCH"},
    ],
    "farming_tips": {
        "spoils": False, "spoil_duration_hours": None, "base_weight": 0.25,
        "object_type": "Food", "yield_perk": None, "weight_perk": "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": 0.14},
            {"rank": 2, "reduction": "90%", "weight": 0.03},
        ],
        "backpack_mod": "grocers", "backpack_weight": 0.03,
        "armour_mod": "thru_hikers", "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": 0.20},
            {"pieces": 2, "reduction": "40%", "weight": 0.15},
            {"pieces": 3, "reduction": "60%", "weight": 0.10},
            {"pieces": 4, "reduction": "80%", "weight": 0.05},
            {"pieces": 5, "reduction": "90% cap", "weight": 0.03},
        ],
        "magazines_affect_yield": False, "good_with_salt": False,
    },
}

# ── CANNED COFFEE ──────────────────────────────────────────────────────────
# Packaged pre-war coffee (ALCH 0037E0C4, EDID SurvivalistCoffee). Weight 0.50,
# value 14, keywords ObjectTypeFood + MealTypePackaged (packaged → non-perishable,
# no ObjectTypeCanSpoil). Placed directly in the world (many direct REFRs at
# kitchens / diners / camps) → Fixed Spawn Locations, plus food-container presence.
# It is a member of LL_Perk_CanDo_Items (0059DD1E), so the Can Do! Luck perk finds
# more of it — yield_perk "can_do" (data-confirmed, not from a keyword). It is an
# ingredient in the White Russian recipe (co_Brewing_WhiteRussian), which
# build_farming_used_for surfaces under Used For.
CANNED_COFFEE = {
    "slug": "canned-coffee",
    "name": "Canned Coffee",
    "page_title": "Canned Coffee Spawn Locations",
    "blurb": "Every known world spawn for Canned Coffee, grouped by region. Directions and photos are added by hand.",
    "items": [
        {
            "formid": "0037E0C4",
            "edid": "SurvivalistCoffee",
            "full": "Canned Coffee",
            "sig": "ALCH",
        },
    ],
    # Direct REFRs are fixed 100% world spawns (Fixed Spawn Locations); a small
    # number of food containers can also hold one. No creature/vendor/event pool.
    "drop_rates": {
        "world_spawns": None,
        "containers": {
            "container_edid": "LL_Food_Any",
            "note": (
                "Canned Coffee is placed directly in the world at kitchens, diners "
                "and camps (fixed spawns); a small number of food containers can "
                "also hold one."
            ),
        },
        "vendors": None,
        "resource_generators": None,
    },
    # ALCH 0037E0C4: Weight 0.50, ObjectTypeFood + MealTypePackaged (packaged →
    # non-perishable). Placed in the world (not butchered/harvested) so no yield
    # perk from butchering; but it IS in LL_Perk_CanDo_Items, so Can Do! applies.
    # Food → Thru-Hiker weight perk / Grocer's backpack.
    "farming_tips": {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": 0.50,
        "object_type": "Food",
        "yield_perk": "can_do",
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
    # Used For (consumption + recipes + vendor_list) generated by build_farming_used_for.
}

# All sets in this family — add new items here.
# The generic build script (build_farming_spawns_json.py) picks them up automatically.
ALL_SETS = [CREAM, DEATHCLAW_EGG, FROG_EGG, MIRELURK_EGG, MOTHMAN_EGG,
            RADSCORPION_EGG, RADTOAD_EGG, TICK_BLOOD, BLOOD_SAC, HOTDOG,
            CANNED_MEAT_STEW, CANNED_COFFEE, GLOWING_BLOOD, HONEYCOMB,
            PERFECT_BUBBLEGUM, PURIFIED_WATER, ROYAL_JELLY, SUGAR_BOMBS,
            SALT_PEPPER_SUGAR_SPICES, HONEY]

# ── Farming - Chems ────────────────────────────────────────────────────────────
# 76 chem/serum/disease-cure item sets live in their own generated module
# (chems_spawns_config.CHEM_SETS) to keep this file readable. They use the same
# set schema and are appended to ALL_SETS so the engine + build_farming_used_for
# treat them identically to the food/egg sets. Glowing Blood is intentionally NOT
# here (it already has a Non-Perishable page — GLOWING_BLOOD above).
try:
    from chems_spawns_config import CHEM_SETS
except ImportError:  # pragma: no cover - chems module optional
    CHEM_SETS = []
ALL_SETS = ALL_SETS + list(CHEM_SETS)

# Slug → config dict, for --item <slug> lookup in the build script.
SETS_BY_SLUG = {s["slug"]: s for s in ALL_SETS}

# The ten regions every page renders, in A-Z order (empty ones get a "no spawns" note).
ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]
