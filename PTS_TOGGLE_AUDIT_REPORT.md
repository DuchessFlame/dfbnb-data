# PTS Toggle Pipeline Audit Report

**Generated:** 24 June 2026  
**Scope:** All JS/CSS modules in `dfbnb-child/assets/`, all Python build scripts in `dfbnb-data/src/`, GitHub Actions workflows, PTS TSV exports  
**Purpose:** Determine which JS modules need a PTS toggle pipeline built

---

## How the PTS Toggle Works

The PTS system is powered by `dfbnb-child/df-bnb-pts.js` (loaded in `<head>` before any renderer). It works **transparently** — individual modules don't need to know about it:

1. **Fetch redirect:** Monkey-patches `window.fetch()` to rewrite any URL containing `/dfbnb-data/main/dist/` → `/dfbnb-data/main/dist/pts/`. All JSON fetches are silently rerouted.
2. **Progress freeze:** Overrides `localStorage` so checklist progress keys return `null` on get and silently drop writes — prevents PTS from corrupting live progress.
3. **UI injection:** Adds a floating toggle button, hazard-stripe banner, diagonal watermark, and disables all checkboxes.
4. **API:** Exposes `window.DFBNB_PTS.active` (boolean). Persisted via `localStorage["dfbnb:pts_preview"]`.

**Implication:** Any module that fetches JSON from `dist/` will automatically get PTS data when the toggle is on — *provided a corresponding JSON file exists in `dist/pts/`*. The real question is: does the PTS build pipeline produce the right JSON for each module?

---

## PTS Build Infrastructure

### Existing PTS-Specific Scripts
| Script | Purpose |
|--------|---------|
| `normalize_pts_tsv.py` | Renames PTS export filenames to live-style names so standard builders can consume them |
| `diff_live_pts.py` | Compares `dist/` vs `dist/pts/` JSON, produces changelog |
| `build_axolotl_guide_json.py --pts` | Builds `axolotl_guide_pts.json` from PTS TSVs |

### Existing PTS Workflows
| Workflow | Purpose |
|----------|---------|
| `dfbnb-pts-build.yml` | Full PTS build pipeline — runs `normalize_pts_tsv.py`, clears `dist/`, runs ALL builders, relocates output to `dist/pts/`, then runs `diff_live_pts.py` |
| `build_axolotl_guide_json.yml` | Conditional PTS step if PTS exports exist |

### PTS TSV Exports Currently Available (`tsv/pts/`)
61 files covering these record types: ACTI, ALCH, ARMO, AVIF, BOOK, BPTD, CHAL, CNDF, COBJ, CONT, ECAT, EMOT, ENCH, ENTM, FISH, FLST, FURN, GLOB, GMRW, KEYM, KYWD, LCTN, LSCR, LVLI, MESG_Help, MGEF, MISC, NOTE, NPC, OMOD, PCRD, PERK, PLYT, QUEST, RESO, SPEL, SurvivalTentInteriors, WEAP, WTHR

---

## JS Module Audit

### Legend
- **PTS Status:** Whether the module already has PTS-specific code
- **PTS Needed:** Whether PTS data would be meaningful for this module
- **PTS Ready via Global Toggle:** The `dfbnb-pts-build.yml` workflow already runs ALL builders — so if the JSON output exists in `dist/pts/`, the module works automatically. "Pipeline exists" means the PTS build workflow already covers it.

---

### MODULES WITH REAL CODE (53 files)

#### REWARD / DROP-RATE MODULES (High PTS value — game balance changes)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 1 | df-bnb-events-rewards.js | Event reward pages | `events/events_rewards_by_page.json` | `build_events_rewards_json.py` | N | **Y** — event reward pools, drop rates change on PTS | LVLI, GLOB, CURV, BOOK |
| 2 | df-bnb-public-events.js | Public Events rewards | `events/events_rewards_by_page.json` | `build_events_rewards_json.py` | N | **Y** — shares event rewards JSON | LVLI, GLOB, CURV, BOOK |
| 3 | df-bnb-activities.js | Activities rewards | `activities/activities_rewards_by_page.json` | `build_activities_rewards_json.py` | N | **Y** — activity reward pools change on PTS | LVLI, GLOB, CURV, BOOK |
| 4 | df-bnb-daily-ops.js | Daily Ops rewards | `daily_ops/daily_ops_rewards.json` | `build_daily_ops_json.py` | N | **Y** — DO reward tiers change on PTS | BOOK, ARMO, LVLI |
| 5 | df-bnb-raids.js | Raids (Gleaming Depths) | `reho/reho_rewards_by_page.json` | `build_reho_json.py` | N | **Y** — raid reward pools change on PTS | LVLI, GLOB, CURV |
| 6 | df-bnb-expos.js | Expeditions (AC, Pitt) | `reho/reho_rewards_by_page.json` | `build_reho_json.py` | N | **Y** — expedition rewards change on PTS | LVLI, GLOB, CURV |
| 7 | df-bnb-raid-expo-hunts-ops.js | Combined REHO rewards (8 pages) | `reho/reho_rewards_by_page.json` | `build_reho_json.py` | N | **Y** — combined REHO data | LVLI, GLOB, CURV |
| 8 | df-bnb-seasonal-events.js | 13 seasonal event reward pages | `seasonal_events/seasonal_events_rewards_by_page.json` | `build_events_rewards_json.py` (seasonal path) | N | **Y** — seasonal event rewards change on PTS | LVLI, GLOB, CURV, BOOK |
| 9 | df-bnb-treasure-maps.js | Treasure Maps & Lucky Strike | `treasure_maps.json` | `build_treasure_maps_json.py` | N | **Y** — treasure map loot pools change on PTS | LVLI, BOOK, GLOB, MISC, ALCH |
| 10 | df-bnb-bounty-hunting.js | Bounty Hunting (Grunt/Head Hunt) | `bounty-hunting/bounty_hunting_rewards.json` | `build_bounty_hunting_rewards_json.py` + `build_head_hunt_bosses_json.py` | N | **Y** — bounty reward pools change on PTS | GLOB, CHAL, LVLI, WEAP, ENCH, OMOD |
| 11 | df-bnb-infestations.js | Infestations (HTO) rewards | `infestations/hto_rewards.json` | `build_hto_rewards_json.py` | N | **Y** — infestation rewards change on PTS | GLOB |
| 12 | df-bnb-drop-chances.js | Interactive LVLI calculator | `drop_chances_data.json` | `build_drop_chances_data.py` | N | **Y** — entire LVLI tree changes on PTS | LVLI, BOOK, MISC, KEYM, ARMO, WEAP, ALCH, AMMO, GLOB, CURV |

#### CHECKLIST / PLAN MODULES (High PTS value — new items appear on PTS)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 13 | df-bnb-titles.js | Titles checklist (Camp + Player) | `titles_camp.json`, `titles_player.json`, `titles_manifest.json`, `titles_patchlog.json` | `build_titles_json.py` | N (but builder has PTS-aware obtain text) | **Y** — new titles appear on PTS first | CMPT, PLYT, BOOK, COBJ, GLOB, GMRW, LVLI, CHAL, CNDF, ENTM, CURV |
| 14 | df-bnb-plan-checklists.js | Plan Checklists + Skins + Pennants | `plan_master.json`, `pennants.json` | `build_plan_checklists_json.py` (→ Node.js), `build_pennants_json.py` | N (but pennants builder has PTS mappings) | **Y** — new plans/skins appear on PTS | LVLI, ENTM, BOOK, COBJ |
| 15 | df-bnb-armour.js | Armour Plans & Mods | `armour.json` | `build_armour_json.py` | N | **Y** — new armour mods/plans on PTS | COBJ, BOOK |
| 16 | df-bnb-collectables.js | 7 collectable types (Bobbleheads → Keys) | `collectables_*.json` (8 files) | `build_collectables_json.py` | N | **Y** — new collectables could appear on PTS | ALCH, GLOB, KYWD, MISC, BOOK |
| 17 | df-bnb-fishing-equipment.js | Fishing Equipment checklist | `fishing_equipment.json` | `build_fishing_equipment_json.py` | N | **Y** — new rod skins/bobbers on PTS | OMOD, BOOK, GMRW, CHAL, ENTM |

#### CAMP ITEM MODULES (Medium-High PTS value — new CAMP items on PTS)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 18 | df-bnb-camp-items.js | 11 CAMP item types (Allies → Resource Producers) | Multiple: `allies.json`, `pets.json`, `buff-stations.json`, `collectrons.json`, etc. | `build_allies_pets_weather_json.py`, `build_buff_stations_json.py`, `build_camp_items_json.py` | N | **Y** — new CAMP items appear on PTS | COBJ, ENTM, FURN, FLST, ACTI, RESO, CONT, LVLI, GLOB, BOOK |
| 19 | df-bnb-resource-producers.js | Collectrons & Resource Producers | `collectrons.json`, `resource_producers.json` | `build_camp_items_json.py` | N | **Y** — new collectrons/producers on PTS | RESO, CONT, ENTM, COBJ, BOOK, LVLI, GLOB |

#### CHALLENGE MODULES (Medium PTS value — new challenges on PTS)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 20 | df-bnb-challenges.js | Generic challenges (all types) | REST + page data attrs | `build_challenges_json_v3.py` | N | **Y** — new challenges appear on PTS | CHAL, QUEST, GMRW, ENTM, BOOK, COBJ, KYWD, MISC, FLST |
| 21 | df-bnb-lifetime-challenges.js | Lifetime challenges | REST endpoint | `build_challenges_json_v3.py` | N | **Y** — new lifetime challenges on PTS | CHAL, QUEST, GMRW, ENTM |
| 22 | df-bnb-score-challenges.js | Daily/Weekly SCORE challenges | REST endpoint | `build_challenges_json_v3.py` | N | **Y** — SCORE challenges change on PTS | CHAL, QUEST, GMRW, ENTM |
| 23 | df-bnb-pioneer-scouts.js | Pioneer Scouts badges | REST endpoint | `build_challenges_json_v3.py` | N | **Maybe** — rarely changes but could | CHAL, QUEST |
| 24 | df-bnb-quests.js | Quest checklists | REST endpoint | `build_challenges_json_v3.py` | N | **Maybe** — new quests on PTS | CHAL, QUEST |
| 25 | df-bnb-random-encounters.js | Random Encounter checklists | REST endpoint | `build_challenges_json_v3.py` | N | **Maybe** — rarely changes | CHAL, QUEST |
| 26 | df-bnb-mini-seasons.js | Mini Season challenges | `mini_seasons.json` | Node.js (`deploy-mini-seasons.yml`) | N | **Y** — mini season challenges appear on PTS early | CHAL, ENTM, GMRW |
| 27 | df-bnb-world-pets.js | World Pets (6 sub-pages) | `world_pet_challenges.json`, `world_pet_types.json` | `build_world_pet_challenges_json.py`, `build_world_pet_types_json.py` | N | **Y** — world pet data changes on PTS | CHAL, CNDF, KYWD, ENTM, NPC, ALCH, COBJ, GLOB |

#### FISHING / CALCULATOR MODULES (Medium PTS value)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 28 | df-bnb-fishing.js | Fishing Calculator + Checklist + Axolotl | `fishing.json`, `axolotl_guide.json`, `axolotl_guide_pts.json` | `build_fishing_json.py`, `build_axolotl_guide_json.py` | **Y** (Axolotl sub-page has its own PTS toggle) | **Y** — fish spawn rates, new fish on PTS (Axolotl already done) | FISH, GLOB, LVLI, ALCH, CHAL |
| 29 | df-bnb-legendary-mod-calc.js | Legendary Mod Drop Chances | `legendary_mod_drop_chances.json` | `build_legendary_mod_drop_chances_json.py` | N | **Y** — legendary mod pools change on PTS | OMOD |
| 30 | df-bnb-legendary-crafting-planner.js | Legendary Crafting Planner | `legendary_crafting_planner.json` | *(no dedicated builder found)* | N | **Y** — crafting material requirements could change | OMOD, COBJ |
| 31 | df-bnb-currency.js | Gold Bullion Calculator + Minerva | `currency.json` | `build_currency_json.py` | N | **Y** — bullion prices/plans change on PTS | BOOK, LVLI |
| 32 | df-bnb-minerva.js | Minerva Calculator + Lists | `minerva/minerva_plans.json` | `build_minerva.py` | N | **Y** — Minerva inventory changes on PTS | BOOK, LVLI |
| 33 | df-bnb-calculators.js | DF calculators (5 types) | `calculators/*.json` | Various (season, score, bloom) | N | **Maybe** — score progression changes per season; build/outfit inspiration items change | COBJ, ENTM, BOOK |
| 34 | df-bnb-bnb-calculators.js | Weak Spot Multiplier Calculator | `calculators/weak_spot_multipliers.json` | `build_weak_spot_multipliers_json.py` | N | **Y** — BPTD values change on PTS | BPTD |

#### RECIPE / FARMING MODULES (Medium PTS value)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 35 | df-bnb-menu.js | BNB Menu page | `bnb-menu.json`, `cobj-recipes.json`, `not-available.json` | `build_bnb_menu_json.py`, `build_cobj_recipes_json.py` | N | **Y** — menu items/recipes change on PTS | ALCH, COBJ |
| 36 | df-bnb-farming-planner.js | Farming Planner | `menu-items.json`, `cobj-recipes.json` | `build_menu_items_json.py`, `build_cobj_recipes_json.py` | N | **Y** — ingredients/recipes change on PTS | ALCH, COBJ |
| 37 | df-bnb-farming-guides.js | Farming Guides (4 sub-pages) | `farming_guides.json` | `build_farming_guides_json.py` | N | **Y** — ingredient effects/recipes change on PTS | ALCH, COBJ |

#### DATA REFERENCE MODULES (Medium PTS value)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 38 | df-bnb-curve-tables.js | CURV Table Viewer | `curves/index.json` + chunks | `build_curves_json.py` | N | **Y** — curve values change on PTS | CURV, PCRD, PERK |
| 39 | df-bnb-curves.js | CURV Viewer (duplicate of above) | Same as above | Same as above | N | **Y** — duplicate of curve-tables | CURV, PCRD, PERK |
| 40 | df-bnb-npc-spawns.js | NPC Spawn Locations | `npc_spawns.json` | *(no dedicated builder found — possibly manual JSON)* | N | **Maybe** — spawn locations rarely change | NPC, LCTN |
| 41 | df-bnb-titles-generator.js | Random Title Generator | `titles_camp_generator.json`, `titles_player_generator.json` | `build_titles_generator_json.py` | N | **Y** — new title prefixes/suffixes on PTS | CMPT, PLYT |

#### ATOM SHOP / COSMETIC MODULES (Low-Medium PTS value)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | TSV Types for PTS |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 42 | df-bnb-atom-shop.js | Atom Shop (5 sub-pages) | `atom_shop.json`, `survival_tent_interiors.json`, `seasons.json` | `build_atom_shop_json.py`, `build_survival_tent_interiors_json.py` | N | **Y** — new Atom Shop items datamined on PTS | ENTM, COBJ |
| 43 | df-bnb-bundles.js | Items Available to Request | `bundles.json` | *(no dedicated builder found)* | N | **Maybe** — bundle contents could change | ENTM |
| 44 | df-bnb-seasons.js | Season Scoreboard rewards | `calculators/season_tickets_s*.json`, `all_seasons.json` | `build_season_rewards.py` | N | **Y** — upcoming season rewards appear on PTS | ENTM, BOOK |

#### ALREADY PTS-AWARE MODULES

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | Notes |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 45 | df-bnb-patch-diff.js | Patch Diff Tool | Fetches raw TSVs from GitHub API | N/A (reads TSVs directly) | **Y** (self-contained — fetches `pts` TSV list from GitHub) | Already done | Compares Live vs PTS exports. Independent of global PTS toggle. |
| 46 | df-bnb-fishing.js | (Axolotl sub-page) | `axolotl_guide_pts.json` | `build_axolotl_guide_json.py --pts` | **Y** (own inline PTS toggle for Axolotl) | Axolotl done; main fishing calc still needs global PTS | See row 28 above |

#### MODULES THAT DON'T NEED PTS (community/meta/static data)

| # | Filename | Pages | JSON Source | Build Script | PTS Already? | PTS Needed? | Reason |
|---|----------|-------|-------------|-------------|:---:|---|---|
| 47 | df-bnb-about.js | About pages (DF/BNB) | `about.json` | `build_about_json.py` | N | **N** | Community/donor data, not game data |
| 48 | df-bnb-member-portal.js | Member Portal (4 tabs) | REST API | N/A | N | **N** | User portal (profiles, mules, messages) |
| 49 | df-bnb-staff-portal.js | BNB Staff Portal | REST API | N/A | N | **N** | Staff operations (orders, crafting, delivery) |
| 50 | df-bnb-admin-portal.js | Admin Portal | REST API | N/A | N | **N** | Admin inbox/moderation |
| 51 | df-bnb-member-pins.js | Member Pins | Fetches TSVs | N/A | N | **N** | Member contribution tracking |
| 52 | df-bnb-data-mining.js | Data Mining (Bug Report) | None (JS-built form) | N/A | N | **N** | Static bug report form template |
| 53 | df-bnb-data-miner-patch-notes.js | Patch Notes links | `data-miner-patch-notes.json` | `build_data_miner_patch_notes_json.py` | N | **N** | Links to external patch notes (meta page) |
| 54 | df-bnb-patch-release-dates.js | Patch Release Dates | `patch-release-dates.json` | `build_patches_json.py` | N | **N** | Historical patch dates (static reference) |

---

### SCAFFOLDED / STUB JS FILES (19 files — no real code)

All contain only scaffold instructions, no rendering logic. PTS is N/A until they're built.

| Filename | Intended Page |
|----------|---------------|
| df-bnb-buffs.js | /buffs/ |
| df-bnb-cryptids.js | /cryptids/ |
| df-bnb-cult-of-the-lamb.js | /cult-of-the-lamb/ |
| df-bnb-experience.js | /experience/ |
| df-bnb-farming.js | /farming/ |
| df-bnb-farming-consumables.js | /farming/consumables/ |
| df-bnb-farming-critters.js | /farming/critters/ |
| df-bnb-farming-eggs.js | /farming/eggs/ |
| df-bnb-farming-junk.js | /farming/junk/ |
| df-bnb-farming-meat.js | /farming/meat/ |
| df-bnb-farming-non-perishable.js | /farming/non-perishable/ |
| df-bnb-farming-nuka-cola.js | /farming/nuka-cola/ |
| df-bnb-farming-plants.js | /farming/plants/ |
| df-bnb-legendary-mods.js | /legendary-mods/ |
| df-bnb-perk-cards.js | /perk-cards/ |
| df-bnb-pet-taming.js | /pet-taming/ |
| df-bnb-specials-stats.js | /specials/ or /stats/ |
| df-bnb-starfield.js | /starfield/ |
| df-bnb-weapons.js | /weapons/ |
| df-bnb-vendors.js | /vendors/ |

---

## CSS Module Summary

| Status | Count |
|--------|------:|
| Files with full unique styles | 49 |
| Scaffolded stubs (167-line .cmptTop template only) | 20 |
| **Total CSS files** | **69** |

Largest: `df-bnb-staff-portal.css` (8,599 lines). Note: `df-bnb-curves.css` and `df-bnb-curve-tables.css` are near-duplicates. The REHO triplet (`expos`, `raids`, `raid-expo-hunts-ops`) share identical 163-line stylesheets. The challenge quadruplet (`lifetime-challenges`, `pioneer-scouts`, `quests`, `random-encounters`) share identical 562-line stylesheets.

---

## Build Script Summary

| Status | Count |
|--------|------:|
| Fully functional Python build scripts | ~60 |
| PTS-specific scripts | 2 (`normalize_pts_tsv.py`, `diff_live_pts.py`) |
| Scripts with PTS-aware logic | 3 (`build_axolotl_guide_json.py`, `build_titles_json.py`, `build_pennants_json.py`) |
| Shared libraries | 6 (`rng76.py`, `patchlog_utils.py`, `diagnostics.py`, `cut_content.py`, `mini_seasons_tickets.py`, `build_chal_latest_previous.py`) |
| Scaffolded/placeholder scripts | ~14 |
| Wrapper/delegate scripts | 10 |
| GitHub Actions workflows | 84 |
| PTS-specific workflows | 2 (`dfbnb-pts-build.yml`, `build_axolotl_guide_json.yml` conditional) |

---

## Key Finding: The PTS Pipeline Already Exists

**The `dfbnb-pts-build.yml` workflow already runs ALL builders against PTS TSV exports and outputs everything to `dist/pts/`.** This means:

1. **The global PTS toggle (`df-bnb-pts.js`) already works for every module** that fetches from `dist/` — the fetch redirect transparently serves PTS JSON when the toggle is on.
2. **No individual JS module needs PTS-specific code changes** — the monkey-patched `fetch()` handles it all.
3. **The real gap is operational, not architectural:** The PTS build needs to be triggered (manually or on push to `tsv/pts/`) and the PTS TSV exports need to be current.

### What actually needs work:

| Gap | Details |
|-----|---------|
| **Missing build scripts** | `legendary_crafting_planner.json`, `bundles.json`, and `npc_spawns.json` don't have dedicated Python builders — they may be manually maintained JSON. These won't get PTS versions automatically. |
| **REST-backed challenge modules** | Challenges, Lifetime Challenges, Score Challenges, Pioneer Scouts, Quests, Random Encounters fetch from a REST API endpoint (`/checklists/progress`), not from `dist/` JSON. The PTS fetch redirect won't intercept WordPress REST calls. These modules would need either: (a) a REST endpoint that serves PTS challenge data, or (b) a refactor to fetch from `dist/` JSON instead. |
| **Node.js build scripts** | Plan Checklists (`build-plan-master.mjs`), Mini Seasons, Seasonal Fish, Axolotl Rotations, DF Calculators, and Curves use Node.js tools — verify these are included in `dfbnb-pts-build.yml`. |
| **Duplicate files** | `df-bnb-curves.js` is identical to `df-bnb-curve-tables.js` — clean up or confirm intentional. |
| **Axolotl's inline PTS toggle** | `df-bnb-fishing.js` has its own independent PTS toggle for the Axolotl Guide sub-page. This predates the global toggle and should probably be migrated to use `window.DFBNB_PTS` for consistency. |

### Modules NOT covered by the global PTS toggle (need separate solution):

| Module | Reason | Suggested Fix |
|--------|--------|---------------|
| df-bnb-challenges.js | Fetches from REST API, not `dist/` | Refactor to fetch from `dist/challenges/challenges.json` (already exists) |
| df-bnb-lifetime-challenges.js | Fetches from REST API | Same refactor |
| df-bnb-score-challenges.js | Fetches from REST API | Same refactor |
| df-bnb-pioneer-scouts.js | Fetches from REST API | Same refactor |
| df-bnb-quests.js | Fetches from REST API | Same refactor |
| df-bnb-random-encounters.js | Fetches from REST API | Same refactor |
| df-bnb-curve-tables.js | Fetches from `/wp-content/uploads/curves/`, not `dist/` | Move curve JSON to `dist/curves/` or add WP-path redirect to PTS toggle |
| df-bnb-curves.js | Same as curve-tables | Same fix |
| df-bnb-member-portal.js | REST API | N/A (doesn't need PTS) |
| df-bnb-staff-portal.js | REST API | N/A (doesn't need PTS) |
| df-bnb-admin-portal.js | REST API | N/A (doesn't need PTS) |

---

## Priority Recommendations

### Tier 1 — Already working via global toggle (just ensure PTS build runs)
All modules that fetch from `dist/` — approximately 35 modules. These work automatically when `dfbnb-pts-build.yml` runs and produces `dist/pts/` JSON.

### Tier 2 — Need minor work
- **Curve Tables:** Move curve data from `/wp-content/uploads/curves/` to `dist/curves/` (or extend PTS toggle's URL pattern).
- **Axolotl inline toggle:** Migrate to use global `window.DFBNB_PTS` instead of its own toggle.
- **Missing builders:** Create builders for `legendary_crafting_planner.json`, `bundles.json`, `npc_spawns.json` if PTS data is wanted for those pages.

### Tier 3 — Need architectural decision
- **REST-backed challenge modules (6 modules):** These bypass the fetch redirect because they call WordPress REST endpoints. Options: (a) refactor to fetch `dist/` JSON, (b) add REST endpoint PTS support in PHP, or (c) add REST URL pattern to the PTS toggle's fetch redirect.

### Tier 4 — No PTS needed
- About, Member Portal, Staff Portal, Admin Portal, Member Pins, Data Mining (Bug Report), Patch Notes links, Patch Release Dates — these display community/meta data, not game data.
