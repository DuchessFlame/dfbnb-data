# Spawn Mechanics Research Report

**Date:** 27 June 2026  
**Scope:** Cross-reference the "How Spawns Work" guide with TSV game data exports + BA2 file accessibility

---

## Part 1 — The Guide (theduchessflame.com)

**Source:** [How spawns work and How to respawn resources and items in Fallout 76](https://www.theduchessflame.com/post/how-spawns-work-and-how-to-respawn-resources-and-items-in-fallout-76)  
**Published:** 7 June 2023 | **Updated:** 29 July 2024  
**Also registered on buffsnbrew.com at:** `/df/farming-guides/how-spawns-work/` and `/bnb/farming-guides/how-spawns-work/`

### What the guide covers

The guide documents 7 core spawn rules from the player's perspective:

1. **Two spawn types** — Fixed Spawns (always appear) vs. Fixed Spawn Locations with a Chance to Spawn (probabilistic via leveled lists).
2. **Leveled Lists** — Simple (Bobblehead draws from one pool) and Complex (Blue Cooler chains through 14+ sub-lists). Explains how lists are shared across entities (e.g. Deathclaw Meat in Whitespring vendor list AND gorebag list).
3. **250-Item Pick-up History** — Personal list; items on it won't respawn until cycled out by picking up 250 other items. Summersville book house (289 books) is the reset method. Does NOT reset on time — only by picking up items. Glitches after 3–4 resets.
4. **Player Cell-Activated Spawn** — Server evaluates spawn chances when a player enters a cell. More players = more spawn checks. Force-spawn technique: fast-travel away and back.
5. **World Object Collectibles** — Per-server (once taken by one player, gone for all). Includes Bobbleheads, Magazines, PA, Asylum Dresses. Type is locked per-server once spawned.
6. **Farmable Plants** — 35% spawn chance on cell entry. Cannot be force-spawned. Same for all players on a server.
7. **Container/Vendor Spawns** — Unique leveled list per container type. Server "forgets" your visit after ~8 minutes away, re-rolling inventory.

### Spawn rates from data miners (Thaang/Gilpo)

| Category | Spawn % |
|----------|---------|
| Bobbleheads / Magazines / Stealth Boys / Clothing | 20% |
| Armour Plans | 10% |
| Weapon Plans | 10% |
| Weapon/Armour Mod Plans | 25% |
| Mini Nukes | 15% |
| Fusion Cores / Chems / Common Drinks / Ammo | 50% |
| PA Frame | 25% |
| Loose PA Pieces on Frame | 40–70% |
| Pre-war Food | 25% |
| Nuka Cola/Cherry (vending machine) | 7.5% |
| Nuka Quantum (vending machine) | 3.12% |
| All harvestable flora | 35% |
| Cap Stash (Standard / Medium / High / Jackpot) | 73.26% / 18.31% / 7.42% / 1% |
| Asylum Dresses (Red) | 0.05% |

### What the guide does NOT cover

The guide is entirely player-facing. It does not reference any Creation Engine record types (GLOB, ENCH, ENTM, ACTI, LCTN, etc.), form IDs, or internal data structures. The underlying mechanics were reverse-engineered from observation and attributed to data miners Thaang and Gilpo.

---

## Part 2 — TSV Game Data Cross-Reference

### GLOB — The Core Spawn Timer System

The GLOB exports are the richest spawn data source. The `fTriggeredActorRespawn` family controls creature respawn timers with location-size variants:

| GLOB EDID | Value | Ref Count | Purpose |
|-----------|-------|-----------|---------|
| `fTriggeredActorRespawnResetTimeMinutes` | 30 min | 11 | Default NPC respawn (empty cell) |
| `fTriggeredActorRespawnOccupiedResetTimeMinutes` | 10 min | 12 | Default NPC respawn (player present) |
| `fTriggeredActorRespawnResetTimeMinutes_LargeLocation` | 60 min | 387 | Large location empty respawn |
| `fTriggeredActorRespawnOccupiedResetTimeMinutes_LargeLocation` | 30 min | 387 | Large location occupied respawn |
| `fTriggeredActorRespawnResetTimeMinutes_SmallLocation` | 10 min | 59 | Small location empty respawn |
| `fTriggeredActorRespawnOccupiedResetTimeMinutes_SmallLocation` | 5 min | 58 | Small location occupied respawn |
| `fTriggeredActorRespawnResetTimeMinutes_EventSpace` | 60 min | 4 | Event space respawn |
| `fTriggeredActorRespawnOccupiedResetTimeMinutes_EventSpace` | 60 min | 4 | Event space (same occupied/empty) |
| `fTriggeredActorRespawnResetTimeMinutes_StartingArea` | 5 min | 15 | Forest region fast respawn |
| `fTriggeredActorRespawnMinDistanceFromPlayers` | 5,000 units | 2 | Min distance from players to respawn |
| `fTriggeredActorRespawnMinTimeSinceDeathMinutes` | 5 min | 2 | Cooldown after creature death |

**Key insight:** The guide's "~8 minute server memory" for containers aligns with the 10-minute `OccupiedResetTimeMinutes` default, and the 5-minute Small Location timer explains why some areas seem to reset faster.

### Encounter Wave Spawner (EWS) Globals

| GLOB EDID | Value | Purpose |
|-----------|-------|---------|
| `EWS_Default_SpawnRadiusMin` | 128 units | Minimum spawn distance |
| `EWS_Default_SpawnRadiusMax` | 5,120 units | Maximum spawn distance |
| `EWS_Default_FindPlayerRadius` | 4,096 units | Player detection range |
| `EWS_Default_MinDistanceFromPlayers` | 256 units | Won't spawn this close |
| `EWS_WavePause_TimerMult` | 20 sec | Pause between encounter waves |

### Specific Spawn Chance Globals

| GLOB EDID | Value | Context |
|-----------|-------|---------|
| `ScorchedStatueSpawnChance` | 101% (always) | Scorched Statue event |
| `Festive_ScorchedSpawnChance` | 0% (off) | Holiday Scorched (toggled on during events) |
| `Spooky_ScorchedSpawnChance` | 10% | Spooky Scorched |
| `COMP_AllySpawnChance_Standard` | 10% | Standard ally appearance |
| `COMP_AllySpawnChance_High` | 50% | High-chance ally |
| `COMP_AllySpawnChance_Always` | 100% | Guaranteed ally spawn |
| `TreasureHunt_SpawnChance` | 0% (off) | Treasure Hunter event (toggled) |
| `E02A_Meat_SpawnChance` | 25% | Primal Cuts meat drop |
| `SpawnChance_Cnone_GatlingPlasmaModPlans` | 95% ChanceNone | 5% actual drop chance |
| `RA_PartyCrasherSpawnChance_Bigfoot` | 0.33% | Bigfoot party crasher |

### Fissure / Scorchbeast Spawn System

| GLOB EDID | Value | Purpose |
|-----------|-------|---------|
| `EN07_Fissure_RespawnTimerLength` | 600 sec (10 min) | Fissure Scorchbeast respawn |
| `EN07_Fissure_UnloadTimerLength` | 90 sec | Fissure unload timer |
| `EN07_Blast_RespawnTriggerRestorationTime` | 30 sec | Nuked fissure restoration |
| `SQ_ScorchbeastStartDistanceMax` | 100,000 units | SB engagement range max |
| `SQ_ScorchbeastStartDistanceMin` | 20,000 units | SB engagement range min |

### Quest Respawn Timers

| GLOB EDID | Value | Ref Count |
|-----------|-------|-----------|
| `QuestExpireGlobal_Daily` | 86,400 sec (24h) | 422 |
| `QuestExpireGlobal_Default` | 2,592,000 sec (30d) | 1,369 |
| `QuestExpireGlobal_Event` | 1,800 sec (30 min) | 30 |
| `QuestExpireGlobal_EventLong` | 3,600 sec (1h) | 2 |

### Level Normalization (Renorm) Globals

80+ entries control creature spawn level scaling. Examples:

| Tier | Min Level | Max Level | Ref Count |
|------|-----------|-----------|-----------|
| Tier01 (default) | 1 | 50 | 3,388 / 3,534 |
| Tier03 | 10 | 75 | 182 / 673 |
| Boss_ScorchBeastQueen | 80 | 95 | 1 |
| Boss_WendigoColossus | 80 | 100 | 8 |
| Boss_JerseyDevil | 20 | 100 | 2 |

### PTS-Only Spawn Globals (Not Yet on Live)

6 new globals found on PTS, including new Festive/Spooky Scorched spawn toggles (`HTO_LCP_LimitedTimeToggle_Holiday_*`) and Shadows of the Deep party crasher rates (`LCP_SDOW_*`, `SDOW_BurialSitePartyCrasherSpawnChance = 0.25`).

---

### ACTI — Spawn Infrastructure (5,000 records)

The physical spawn system in the world is built from Activator records:

| ACTI EDID | Placed Refs | Role |
|-----------|-------------|------|
| `DefaultRespawnActorGroupTrigger` | 1,679 | Primary respawn trigger volume |
| `InvisibleSpawnMarker` | 6,284 | Standard spawn point |
| `InvisibleSpawnMarkerGiant` | 115 | Large creature spawn point |
| `InvisibleSpawnMarkerLarge` | 134 | Large spawn point |
| `SpawnExclusionVolume` | 527 | Blocks spawning in an area |
| `DefaultEventRespawnVolume` | 264 | Event-specific respawn trigger |
| `WorkshopNPCSpawner` | — | Workshop NPC spawning |
| `COMP_AllySpawnMarker_All_ChanceDefault` | — | CAMP ally spawn marker |

### FURN — 80 Ambush Spawn Furniture Markers

Defines HOW creatures appear (animations): `AmbushSpawnHole`, `AmbushSpawnBurrow`, `AmbushSpawnWaterSurface`, `AmbushSpawnFakeDoor`, `AmbushSpawnFromSkyToFloor`, `AmbushSpawnCrawlFromUnder`, `AmbushSpawnGenericFabricator`, plus non-spawn variants for ceiling/wall/floor ambient positioning.

### LCTN — Location Spawn Mapping (1,899 locations, 70,546 LCSR refs)

11,699 LCSR entries are spawn-related, mapping spawn markers to specific world coordinates. Key LocRefTypes:

- `SpawnMarkerRefTypeMolerat` — 199 entries (molerat burrow points)
- `EncounterSpawnLocRef` — 9 entries
- `SpawnMax0768/1024/1280` — spawn radius caps per location
- `SpawnFindPlayer1280/1536/1792` — player detection radii per location

### NPC_ — Creature Spawn Variants (6,542 NPCs)

21 NPC records are spawn-specific variants: `EncMirelurkSpawnHatchling`, `EncMirelurkSpawn01`, `EncMirelurkSpawn_Glowing`, `DLC03_EncHermitCrabSpawn`, `EncWendigo_WendigoColossusSpawn`, `TestScorchBeastAmbushSpawn`, `LvlMirelurkSpawnScorched`.

### CNDF — Spawn Condition Functions

6 condition records control spawn behavior:
- `GlowingCreature_Spawn_Condition` — checks LocVariantGlowing keyword + nuke region
- `Radstorm_GlowingCreature_Spawn_Condition` — GetRandomPercent vs Radstorm chance GLOB
- `Radstorm_NukaFlora_Spawn_Condition` — radstorm flora spawn
- `SpawnChance_RadstormWeather_Condition` — GetRandomPercent < 15 + storm keyword

### Other Record Types

- **LVLI** — 10,244 leveled item lists, 49,053 entries. Controls loot pools, not spawn timing. 1 spawn-named list: `LLD_Creature_WendigoColossusSpawn`.
- **FLST** — 19 spawn-related form lists including `DefaultSpawnLocationList` (27 entries), `EncounterSpawnFeralGhoulAmbushes` (4 entries), `EncounterSpawnMoleratAmbushes` (2 entries).
- **QUEST** — 25 spawn-related quests including `EN07_MQ_Fissure_SpawnerQuest`, `SQ_SmallFissureSpawner`, `COMP_RE_Camp_LiteAllySpawn`.
- **AVIF** — Key actor values: `MirelurkQueenActiveSpawn`, `SpawnedLegendaryItem`, `DLC03HermitCrabActiveSpawn`, `EWS_SpawnRadiusMax/Min`, `EWS_FindPlayerRadius`.
- **ENCH** — Object Effects (enchantments), NOT encounters. No spawn data.
- **ENTM** — Atomic Shop Entitlements, NOT Encounter Manager. No spawn data.

### Files NOT in the dataset

No EZNE (Encounter Zone) or LVLN (Leveled NPC List) TSV exports exist.

---

## Part 3 — BA2 Game Files on D: Drive

**Path:** `D:\SteamLibrary\steamapps\common\Fallout76\Data\`

### File Inventory

- **97 BA2 archives** (~100 GB total)
- **2 ESM files:** `SeventySix.esm` (881 MB, main game master) and `NW.esm` (27 MB, legacy Nuclear Winter)
- **2 BTD terrain files** in `Terrain\`
- **4 BK2 videos** in `Video\`
- **1 empty dir:** `FO76Edit Cache\`
- **No loose files** — everything is packed in BA2 archives

### BA2 Format

All BA2s use the BTDX magic header, version 1. Two archive types: GNRL (general assets) and DX10 (textures). The format is well-documented and parseable:

```
Bytes 0–3:   Magic "BTDX"
Bytes 4–7:   Version 1 (uint32 LE)
Bytes 8–11:  Type "GNRL" or "DX10"
Bytes 12–15: File count (uint32 LE)
Bytes 16–23: Name table offset (uint64 LE)
```

The name table at the end stores filenames as length-prefixed strings. Files are fully readable from the workspace.

### Spawn-Relevant Content in BA2s

| Archive | Spawn Content |
|---------|--------------|
| **MiscClient.ba2** | 197 spawn-related compiled Papyrus scripts (.pex): `encountermanagementscript.pex`, `encounterwave.pex`, `clearactiveeffectonrespawn.pex`, creature-specific scripts |
| **Startup.ba2** | 1,701 creature-related JSON curve tables for armour/damage scaling per creature type |
| **Interface.ba2** | `interface/deathrespawn.swf` — the player respawn UI |
| **SeventySix.esm** | Master record file — ALL game records (spawns, NPCs, locations, leveled lists etc.) in binary TES4 format |

### Can Python Read BA2s?

- No `ba2` module on PyPI
- **`bethesda-structs` (0.1.4) IS available** on PyPI for parsing Bethesda file formats
- The BA2 format is simple enough to parse with a custom Python script (header + file records + name table)
- The ESM requires a full TES4/Creation Engine record parser — `bethesda-structs` or FO76Edit/xEdit for that

### Key Limitation

The ESM is the real goldmine for spawn data (it contains all the records that appear in the TSV exports), but it's a complex binary format requiring dedicated parsers. The TSV exports already cover this data in a much more accessible form.

---

## Summary — Guide vs Game Data Alignment

| Guide Concept | Matching Game Data |
|---------------|--------------------|
| "Cells activate spawns" | `DefaultRespawnActorGroupTrigger` (1,679 placements), `InvisibleSpawnMarker` (6,284 placements) |
| "~8 min server memory" | `fTriggeredActorRespawnOccupiedResetTimeMinutes` = 10 min (close match) |
| "250-item pick-up history" | Not in GLOBs — likely hardcoded engine behaviour |
| "35% plant spawn chance" | No direct GLOB match — may be in LVLI ChanceNone or hardcoded |
| "20% Bobblehead/Magazine spawn" | No direct GLOB match — likely in LVLI ChanceNone values |
| "Spawn chances per item type" | `SpawnChance_Cnone_*` GLOBs partially match; full rates likely in LVLI entries |
| "Leveled Lists" | 10,244 LVLI records with 49,053 entries |
| "Per-server world objects" | Engine-level behaviour, not in static data |
| "Force-spawn by fast travel" | Explained by cell-entry trigger activating `DefaultRespawnActorGroupTrigger` |
| Creature respawn timers | Full `fTriggeredActorRespawn` family with location-size variants |
| Fissure Scorchbeast respawn | `EN07_Fissure_RespawnTimerLength` = 10 min |
| Seasonal spawn toggles | `Festive_ScorchedSpawnChance`, `Spooky_ScorchedSpawnChance`, `TreasureHunt_SpawnChance` |

### Notable gaps

- The **250-item pick-up history** is not visible in any TSV — almost certainly hardcoded in the engine
- The guide's **spawn percentages** (20% Bobbleheads, 35% plants, etc.) don't appear as standalone GLOBs — they're likely baked into LVLI ChanceNone values or spawn marker properties within placed references in the ESM
- **No EZNE (Encounter Zone) exports exist** — these would map encounter difficulty scaling to map regions
- **No LVLN (Leveled NPC) exports exist** — these would show the NPC spawn pools per encounter
