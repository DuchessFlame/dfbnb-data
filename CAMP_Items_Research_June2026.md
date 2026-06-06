# Fallout 76 CAMP Items -- Comprehensive Data Mining Report

**Source:** June 2026 TSV Exports (xEdit)
**Date compiled:** 6 June 2026
**Purpose:** Reference data for building guide pages on buffsnbrew.com

---

## TABLE OF CONTENTS

1. [Weather Stations](#1-weather-stations)
2. [Collectrons](#2-collectrons)
3. [Resource Generators](#3-resource-generators)
4. [CAMP Buff Stations](#4-camp-buff-stations)
5. [Pets](#5-pets)
6. [Water Purifiers / Generators](#6-water-purifiers--generators)

---

## 1. WEATHER STATIONS

### System Architecture

The CAMP Weather Station system uses a **three-part parallel FormList architecture**:

| FormList FormID | EDID | Entries | Contains |
|-----------------|------|---------|----------|
| `006EE8F0` | ATX_Weather_FormList_WeatherStations | 19 | ACTI records (placeable stations) |
| `006EE8EF` | ATX_Weather_FormList_Weathers | 19 | WTHR records (weather effects) |
| `006EE8F1` | ATX_Weather_FormList_Keywords | 19 | KYWD records (weather type IDs) |

Indices match 1:1 across all three lists. When a weather station is placed and powered, the game looks up its position in the WeatherStations list, then fetches the matching WTHR record from the Weathers list at the same index. Players interact via the linked native terminal `0072DB96` (nativeATXCAMPWeatherControlNativeTerminal), not by activating the station directly.

**Shared properties (ALL 19 active stations):**
- Power Required: **2**
- Script: `WorkshopObjectScript`
- Keywords: `WorkshopCanBePowered`, `WorkshopPowerConnectionKW`, `BlockPlayerActivation`, `LinkTerminalATXCampWeatherStation`, + unique weather type keyword
- XALG Flags: Premium
- CAMP limit: **1 per CAMP** (GLOB `006EE8F3` WorkshopCount_WeatherStation_Camp = 1), **0 per Workshop** (GLOB `006EE8F2` = 0)
- Crafting cost: **2 Circuitry, 2 Rubber, 4 Steel, 2 Screws** (COBJ `006EE8ED`)

### All 19 Active Weather Stations

| # | ACTI FormID | EDID | Display Name | Weather KW FormID | WTHR FormID | ENTM FormID | Rarity |
|---|-------------|------|-------------|-------------------|-------------|-------------|--------|
| 0 | `006EE8EC` | ATX_Weather_WeatherStation_Standard_Clear | Weather Control Station (Clear) | `006EE8F4` | `006EE8F9` ATX_Weather_Clear | `006EE8EE` | Rare |
| 1 | `006F0692` | ATX_Weather_WeatherStation_Standard_RadStorm | Weather Control Station (Radstorm) | `006F0680` | `006F0681` ATX_Weather_Radstorm | `006F0693` | Nuclear |
| 2 | `007263BB` | ATX_Weather_WeatherStation_Snowman_Snow | Weather Control Station (Snow) | `007263BA` | `007263BD` ATX_Weather_Snow01 | `007263C0` | Superior |
| 3 | `0073ABA5` | ATX_Weather_WeatherStation_XPDACBoardwalk | Weather Control Station (Atlantic City Fog) | `0073ABA7` | `0073ABA8` ATX_Weather_XPD_AC_Boardwalk_Fog | `0073ABA6` (SCORE_S15) | Superior |
| 4 | `00743E6D` | ATX_Weather_WeatherStation_Thunderstorm | Weather Control Station (Thunderstorm) | `00743E6F` | `0076B55A` ATX_Weather_Thunderstorm | `00743E6E` | Superior |
| 5 | `00757615` | ATX_Weather_WeatherStation_Storm_SkylineValley | Weather Control Station (Skyline Valley) | `00757616` | `00782D9A` ATX_Weather_Storm_DeadZone_New | `007586AC` | Superior |
| 6 | `007813E3` | ATX_Weather_WeatherStation_Mothman | Weather Control Station (Mothman) | `00798D5F` | `00798D60` ATX_Weather_MothmanEquinox | `00781421` | Superior |
| 7 | `00787ED0` | ATX_Weather_WeatherStation_Halloween01 | Weather Control Station (Halloween) | `0079C8E1` | `007962BE` ATX_Weather_HalloweenOvercast01 | `00787EE2` | Superior |
| 8 | `0078DB62` | ATX_Weather_WeatherStation_FallFoliage | Weather Control Station (Fall) | `007ACD58` | `007ACD5C` ATX_Weather_FallFoliage | `0078DB68` | Superior |
| 9 | `0079A1B8` | ATX_Weather_WeatherStation_SnowAurora | Weather Control Station (Snow Aurora) | `007B24D0` | `007B24CF` ATX_Weather_SnowAurora01 | `0079A1D2` | Superior |
| 10 | `007990A4` | SCORE_S19_Weather_WeatherStation_NukeZone | Weather Control Station (Nuke Zone) | `007990A5` | `0079B16A` ATX_Weather_RadstormNukeVariation | `007990A2` (SCORE_S19) | Superior |
| 11 | `007B288D` | ATX_Weather_WeatherStation_VerdantPollen | Weather Control Station (Blooming Haze) | `007F182C` | `007F1829` ATX_Weather_VerdantPollen | `007B28BB` | Superior |
| 12 | `007D70CE` | ATX_Weather_WeatherStation_Fireworks | Weather Control Station (Fireworks) | `0081139F` | `00811398` ATX_Weather_Fireworks | `007D70D3` | Superior |
| 13 | `00832AF8` | ATX_Weather_WeatherStation_LightRain | Weather Control Station (Light Rain) | `00832935` | `00832AD7` ATX_Weather_LightRain | `008319F2` | Rare |
| 14 | `0084CCC8` | ATX_Weather_WeatherStation_BurningNight | Weather Station (Burning Night) | `0084CCE2` | `0086C996` ATX_Weather_BurningNight | `0084CCD5` | Superior |
| 15 | `0084CCC9` | ATX_Weather_WeatherStation_BurningSandstorm | Weather Control Station (Burning Sandstorm) | `0084CCE3` | `0085354E` ATX_Weather_BurningSandStorm | `0084CCD6` | Superior |
| 16 | `008998E1` | SCORE_S24_Weather_WeatherStation_Invasion | Weather Control Station (Alien Invasion) | `008AACC0` | `008AACC1` ATX_Weather_Invasion | `008998E2` (SCORE_S24) | Superior |
| 17 | `0085B5F6` | ATX_Weather_WeatherStation_Outwaste | Weather Control Station (Outwaste) | `008ABBCB` | `008ABBCE` ATX_Weather_Outwaste | `0085B5FA` | Superior |
| 18 | `008B0D74` | SCORE_S25_Weather_WeatherStation_Rainbow | Weather Control Station (Rainbow) | `008B5449` | `008B544A` ATX_Weather_RainbowLightRain | `008B0D76` (SCORE_S25) | Superior |

### Deprecated / Cut Weather Stations

| ACTI FormID | EDID | Display Name | Notes |
|-------------|------|-------------|-------|
| `006FB14E` | zzzATX_Utility_ATX_Utility_CampWeather_DeafultWeatherStation | Weather Station | No keywords/properties/scripts. Deprecated. |
| `006FB14D` | zzzATX_Utility_ATX_Utility_CampWeather_SnowWeatherStation | Snow Weather Station | No keywords/properties/scripts. Deprecated. |
| `00787ECF` | zzzATX_Weather_WeatherStation_Fall_Leaves | Weather Control Station (Fall) | Replaced by FallFoliage (`0078DB62`). |
| `007ADC9B` | ZZZ_SCORE_S20_Weather_WeatherStation_Radstorm | Weather Control Station (Radstorm) | Duplicate of `006F0692`. |
| `00832934` | zzzATX_Weather_WeatherStation_Standard_LightRain | Weather Control Station (Light Rain) | Replaced by `00832AF8`. |

### Weather Vanes (Decorative Only -- NOT Functional)

4 cosmetic weather vanes exist as STAT records (no weather effect):
- `005DB34B` Fox Weather Vane | `005DB34C` Goat Weather Vane | `005DB34D` Rooster Weather Vane | `005DB34E` Vault Boy Weather Vane
- COBJ: `005DD4E4` -- 3 Steel
- LVLI: `005DD4F0` ATX_workshop_LL_Decorations_WeatherVanes

### Atlantic City Fog (Gold Vendor Plan)

Separate crafting path for Gold Bullion purchase:
- Plan FormID: `0075FF95` "Plan: Weather Control Station (Atlantic City Fog)" -- 1,250 Gold Bullion (Tier 10)
- Condition: `0075FF98` SCORE_S15_COBJ_WeatherMachine_ACBoardwalk_Condition

---

## 2. COLLECTRONS

### Collection Rate Summary

| Rate (hrs) | Minutes | Collectrons |
|------------|---------|-------------|
| 0.080 | 4.8 | Scavenger, Gold, Silver, Red Rocket, F.E.T.C.H., Junkyard Dog, Raider (Alcohol/MeleeAmmo modes) |
| 0.100 | 6.0 | Raider (base mode) |
| 0.120 | 7.2 | Communist (Revolutionary), Santatron, Fasnacht, Nuka-Cola, BoS, Nuka-Quantron, Auto-Miner, Toxic Bob |
| 0.150 | 9.0 | Free States, Liberated, Peppino, Evidence Collection, Sir Loin |
| 0.170 | 10.2 | Lumberjack, Scoutmaster, Biv Bev, Robo Butler, Cobby, Mr. Farmhand + all Coffee Machines + Tree Sap + Radstag |

### All 26 Collectron Bot Variants

#### 1. Scavenger Collectron (Base Game)
- **NPC:** `000524B6` ATX_Collectron | **Container:** `0000729C` ATX_Collectron_Station "Scavenger Collectron Station"
- **Rate:** 0.080 hrs (4.8 min) | **Race:** Protectron
- **Collects (LVLI `00554178`):** Scrap materials in 5 rarity tiers:
  - VeryRare (CN 95%): Ultracite, Nuclear Material, Coal
  - Rare (CN 89%): Ballistic Fiber, Antiseptic, Asbestos, Black Titanium, Circuitry, Crystal, Fiber Optics, Gold, Oil, Silver
  - Uncommon (CN 82%): Acid, Adhesive, Aluminum, Copper, Fiberglass, Gears, Screws, Springs
  - Common (CN 57%): Mixed common scrap
  - SuperCommon (CN 0%): Bone, Ceramic, Cloth, Cork, Lead, Plastic, Steel, Wood
- **Crafting:** 1 Circuitry, 1 Copper, 1 Gears, 3 Steel
- **Source:** Base game (free)

#### 2. Free States Collectron
- **NPC:** `00536D2A` ATX_Collectron_FreeStates | **Container:** `00554181` "Free States Collectron Station"
- **Rate:** 0.150 hrs (9 min) | **Race:** Protectron
- **Collects (LVLI `0055417C`):** Food, Water (purified CN 85% / boiled CN 65% / dirty), Meds (RadAway, RadX, Stimpak, diluted -- CN 60%), Pre-War Money (CN 5%), Tin Cans
- **Source:** Atom Shop

#### 3. Gold Collectron
- **NPC:** `00536D2B` ATX_Collectron_GoldBot | **Container:** `00536D42` "Scavenging Station"
- **Rate:** 0.080 hrs (4.8 min) | **Race:** Protectron
- **Collects (LVLI `005F0D29`):** Gold Ore, Gold Scrap
- **Source:** Atom Shop

#### 4. Silver Collectron
- **NPC:** `00536D44` ATX_Collectron_SilverBot | **Container:** `00536D43` "Silver Collectron Station"
- **Rate:** 0.080 hrs (4.8 min) | **Race:** Protectron
- **Collects (LVLI `0064A14B`):** Silver Ore, Silver Scrap
- **Source:** Atom Shop / Gold Bullion plan

#### 5. Comrade Collectron (Communist)
- **NPC:** `00564AE6` ATX_Collectron_Communist | **Container:** `00536D41` "Communist Collectron Station"
- **Race:** Protectron | **Has 2 modes:**
- **Proletariat mode** (0.080 hrs): Meds (CN 80%), Food (CN 55%), Water (CN 50%), Tools (CN 30%), Propaganda (always -- note + Red Star Pin)
- **Revolutionary mode** (0.120 hrs): Weapons (Chinese Officer Sword through Flare Gun), Throwables (grenades/mines/molotovs), Blood packs, Propaganda
- **Source:** Atom Shop

#### 6. Ace the Raider (Raider Collectron)
- **NPC:** `00564AE7` ATX_Collectron_Raider "Ace the Raider" | **Container:** `005698B3` "Raider Collectron Station"
- **Race:** Protectron | **Has 2 modes:**
- **Alcohol & Chems mode** (0.080 hrs): Ammo, Melee weapons (CN 80%), Alcohol/Beer, Ranged weapons (CN 80%), Chems (CN 70%), Bones
- **Melee & Ammo mode** (0.080 hrs): Similar split
- **Source:** Atom Shop

#### 7. BoS Collectron
- **NPC:** `00565214` ATX_Collectron_Bos | **Container:** `005C53AB` "BoS Collectron Station"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `005C53AF`):** Scrap (CN via ECON = 5%), Goodies (Common rewards -- CN 60%), Ammo (random fallback -- CN 75%)
- **Source:** Atom Shop

#### 8. Santatron
- **NPC:** `005A0F71` ATX_Collectron_Santa | **Container:** `005A1152` "Santatron Collectron Station"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `005A1159`):** Holiday Gifts (ONLY when Festive_Holiday_Enabled=1 -- Tier 3 CN 85%, Tier 2 CN 60%, Tier 1 CN 0%), Toys (CN 59%), Sweets (CN 65%), Fertilizer (CN 25%), Coal (always)
- **Source:** Atom Shop

#### 9. Fasnacht Collectron
- **NPC:** `005A0CB2` ATX_Collectron_Fasnacht | **Container:** `005ACB21` "Fasnacht Collectron Station"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `005AD5A9`):** Fasnacht Donuts (CN 90%, only during Fasnacht), Fasnacht Sausage (CN 88%, only during Fasnacht), Sweets (CN 50%), Liquor & Wine (CN 50%), Beer (CN 0%)
- **Source:** Atom Shop

#### 10. Nukatron (Nuka-Cola Collectron)
- **NPC:** `005FBC1E` ATX_Collectron_NukaCola "Nukatron" | **Container:** `005FBC18` "Nuka-Cola Collectron Station"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `005FBC1B`):** Nuka Quantum (CN 95%), Nuka Cranberry (CN 90%), Nuka Grape (CN 85%), Nuka Orange (CN 80%), Nuka Cherry (CN 75%), Nuka Wild (CN 70%), Empty bottle (CN 65%), Nuka-Cola (fallback)
- **Source:** Atom Shop / Gold Bullion plan

#### 11. Red Rocket Collectron
- **NPC:** `0060503C` ATX_Collectron_RedRocketRobot | **Container:** `005FD095` "Red Rocket Collectron Station"
- **Rate:** 0.080 hrs (4.8 min) | **Race:** RedRocketRobotRace (unique pump-shaped model)
- **Collects (LVLI `0060DF53`):** Auto parts -- Uncommon (CN 30%: hub caps, enamel bucket, metal bucket, coolant cap), Rare (CN 75%: gas canister, magnet, fuse, oil can, blowtorch, fuel tank), SuperCommon (CN 0%: fuse, screwdriver, lightbulb, wrenches, wonderglue)
- **Source:** Atom Shop / Gold Bullion plan

#### 12. F.E.T.C.H. Collectron
- **NPC:** `00620A43` ATX_Collectron_FETCH | **Container:** `00620F39` "F.E.T.C.H. Collectron Station"
- **Rate:** 0.080 hrs (4.8 min) | **Race:** DogCollectronRace (custom dog robot)
- **Collects (LVLI `00622F7A`):** Electronics -- Uncommon (CN 30%: alarm clock, springs, cameras, toasters), Rare (CN 75%: fuse, magnets, military circuits, sensor modules), SuperRare (CN 95%: nuclear material, deathclaw egg, baseball grenade, clean dog bowl), SuperCommon (CN 0%: fuse, circuitry, gears, screws)
- **Source:** Atom Shop / Gold Bullion plan

#### 13. Junkyard Dog Collectron
- **NPC:** `0065F22F` ATX_Collectron_FETCH_JunkyardDog | **Container:** `0065F22C` "Junkyard Dog Collectron Station"
- **Rate:** 0.080 hrs (4.8 min) | **Race:** DogCollectronRace
- **Collects (LVLI `006667F5`):** Same structure as F.E.T.C.H. with Junkyard-specific sub-lists
- **Source:** Atom Shop

#### 14. Nuka-Quantron
- **NPC:** `00679A7A` SCORE_S11_Collectron_NukaCola_NukaQuantum | **Container:** `00679A76` "Nuka-Cola Quantum Collectron Station"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `0067B2EA`):** Ultracite Scrap (CN 98%), Nuclear Material (CN 95%), Nuka Twist (CN 90%), Nuka Wild (CN 85%), Nuka Cherry (CN 80%), Nuka Quantum (CN 75%), Empty bottle (CN 65%), Nuka-Cola (fallback)
- **Source:** SCORE Season 11 / Gold Bullion

#### 15. Auto-Miner
- **NPC:** `006C6DC6` SCORE_S14_Collectron_AutoMiner | **Container:** `006C6DBF` "Auto-Miner Collectron Station"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `006C6DC3`):** Ultracite Ore (CN 98%), Black Titanium Ore (CN 90%), Aluminum Ore (CN 75%), Lead Ore (CN 60%), Copper Ore (CN 50%), Steel Ore (fallback)
- **Source:** SCORE Season 14 / Gold Bullion

#### 16. Lumberjack Collectron
- **NPC:** `00772A50` ATX_Collectron_Lumberjack | **Container:** `0076A6BC` "Lumberjack Collectron Station"
- **Rate:** 0.170 hrs (10.2 min) | **Race:** Protectron
- **Collects (LVLI `0076A6C2`):** Junk (standard tiers) + Lumberjack-specific wood items (wood scrap, clipboard, pencil, wooden blocks)
- **Crafting:** 5 Wood
- **Source:** Atom Shop

#### 17. Scoutmaster
- **NPC:** `00771DCD` SCORE_S17_Collectron_ScoutMaster | **Container:** `00771DCB` "Scoutmaster Collectron Station"
- **Rate:** 0.170 hrs (10.2 min) | **Race:** Protectron
- **Collects (LVLI `00771DC4`):** Cranberries (CN 98%), S'mores (CN 90%), Board Game (CN 90%), Starlight Berries (CN 90%), Cat Plush (CN 80%), Sugar Bombs (CN 80%), Bowie Knife (CN 80%), Bubblegum (CN 80%), Knife (CN 70%), Fancy Lad Snack Cakes (CN 70%), Tick Blood Sac (CN 70%), Survivalist meals (CN 60-70%), Tomahawk (CN 50%), Pork n Beans (CN 50%), Potato Crisps (CN 50%), Wood Scrap (fallback)
- **Source:** SCORE Season 17 / Gold Bullion

#### 18. Biv Bev Collectron
- **NPC:** `0079A1DC` ATX_Collectron_BivBev | **Container:** `0079A1C9` "Biv Bev Collectron Station"
- **Rate:** 0.170 hrs (10.2 min) | **Race:** Robobrain (unique -- only Robobrain collectron)
- **Collects (LVLI `007C729C`):** Brewing materials (wood scrap, boiled water, razorgrain, corn, mutfruit, sugar, lemonade, brahmin milk, snaptail reed), Beer (Pickaxe Pilsner, Old Possum, Oak Holler Lager, New River Red Ale, Blackwater Brew), Liquor (Wine, Whiskey, Vodka, Rum, Bourbon)
- **Source:** Atom Shop

#### 19. Liberated Collectron
- **NPC:** `0082A6AD` ATX_Collectron_Liberated | **Container:** `0082A6AA` "Liberated Collectron Station"
- **Rate:** 0.150 hrs (9 min) | **Race:** Liberator (unique -- only Liberator collectron)
- **Collects (LVLI `0084CCA6`):** Mothman Wing, Sap Vegetable, Starlight Berries, Mothman Egg
- **Source:** Atom Shop

#### 20. Whitespring Robo Butler
- **NPC:** `0083645B` SCORE_S22_Collectron_RoboButler | **Container:** `00836467` "Whitespring Robo Butler Collectron Station"
- **Rate:** 0.170 hrs (10.2 min) | **Race:** Protectron
- **Collects (LVLI `00836460`):** Abraxo Cleaner (CN 95%), Soap (CN 88%), Toilet Paper (CN 80%), Bubblegum (CN 80%), Blamco Mac and Cheese (CN 70%), Dandy Boy Apples (CN 58%), Yum Yum Deviled Eggs (CN 44%), Cram (CN 29%), Salisbury Steak (CN 12%), Sugar Bombs (CN 8%), Plunger (fallback)
- **Source:** SCORE Season 22 / Gold Bullion

#### 21. Toxic Bob
- **NPC:** `00841316` ATX_Collectron_ToxicBob | **Container:** `00841308` "Toxic Bob"
- **Rate:** 0.120 hrs (7.2 min) | **Race:** Protectron
- **Collects (LVLI `008577EE`):** Toxic Goo, Dirty Water, Glowing Blood Pack
- **Source:** Atom Shop

#### 22. Cobby (Cornbot)
- **NPC:** `0085CE51` SCORE_S23_Collectron_Cornbot "Cobby" | **Container:** `0085CE4C` "Cobby Collectron Station"
- **Rate:** 0.170 hrs (10.2 min) | **Race:** Protectron
- **Collects (LVLI `0085CE50`):** Corn Soup (CN 95%), Tato Salad (CN 90%), Corn Pone (CN 85%), Tasty Tato Stew (CN 80%), Silt Bean (CN 75%), Pumpkin (CN 70%), Melon Juice (CN 65%), Gourd (CN 60%), Melon (CN 55%), Carrot (CN 50%), Bloodleaf (CN 45%), Tato (CN 40%), Glowing Fungus (CN 35%), Fertilizer (CN 30%), Corn (fallback)
- **Source:** SCORE Season 23 / Gold Bullion

#### 23. Peppino
- **NPC:** `00857253` ATX_Collectron_Peppino | **Container:** `00857245` "Peppino Collectron Station"
- **Rate:** 0.150 hrs (9 min) | **Race:** Protectron
- **Collects (LVLI `008A52C7`):** Toy Car, Accordion (Misc), Gumdrops, Pepperoni Roll (Tasty), Rat Poison, Bowling Pin + Legendary Gear sub-lists (CN 95% SuperRare)
- **Source:** Atom Shop

#### 24. Evidence Collection Assistant
- **NPC:** `008599F4` ATX_Collectron_EvidenceCollectionAssistantCollectron_ECT | **Container:** `008599E0`
- **Rate:** 0.150 hrs (9 min) | **Race:** Protectron
- **Collects (LVLI `008AD79B`):** SuperCommon (CN 0%: Fuse, Circuitry), SuperRare (CN 97%: NonPremium_MagazineBookBox)
- **Source:** Atom Shop

#### 25. Sir Loin
- **NPC:** `008B155F` ATX_Collectron_SirLoin | **Container:** `008B155C` "Sir Loin Collectron Station"
- **Rate:** 0.150 hrs (9 min) | **Race:** Protectron
- **Collects (LVLI `008CD162`):** SuperCommon (CN 0%: Radstag/Molerat/Dog/Squirrel Meat + kitchenware), Uncommon (CN 89%: Yao Guai/Stingwing/Radscorpion/Angler/Mirelurk Meat), Rare (CN 99%: Scorchbeast parts, Deathclaw Egg, Megasloth/Mirelurk Queen/Sheepsquatch Meat)
- **Source:** Atom Shop

#### 26. Mr. Farmhand
- **NPC:** `008DE0A4` SCORE_S25_Collectron_MrFarmhand | **Container:** `008B3DC8` "Mr. Farmhand Collectron Station"
- **Rate:** 0.170 hrs (10.2 min) | **Race:** Mr. Handy (unique -- only Mr. Handy collectron)
- **Collects (LVLI `008B3DCC`):** Leather (CN 95%), Fertilizer (CN 90%), Brahmin Milk (CN 95%), Cloth Scrap (CN 85%), Silt Bean (CN 80%), Sugar (CN 75%), Carrot (CN 70%), Razorgrain Flour (CN 65%), Razorgrain (CN 60%), Mutfruit (CN 55%), Tato (fallback)
- **Source:** SCORE Season 25 / Gold Bullion

### Non-Bot Collectors (Stationary Items Using Collectron Resource System)

These share `WorkshopCollectorObject` keyword but are objects, not walking robots:

| FormID | Display Name | Rate (hrs) | Collects |
|--------|-------------|------------|----------|
| `0067522F` | Slocum's Joe Coffee Machine | 0.170 | Survivalist Coffee |
| `0068E882` | Slocum's Flavorful Coffee Machine | 0.170 | Survivalist Coffee |
| `0078140A` | Red Rocket Coffee Machine | 0.170 | Survivalist Coffee |
| `00791C19` | The Sicilian Espresso Machine | 0.170 | Survivalist Coffee |
| `0067F3AB` | Tree Sap Collector | 0.170 | Adhesive Scrap |
| `0068E77F` | Radstag Field Dressing Station | 0.170 | Radstag Meat (+ -50% food spoilage) |

### Unique Model/Race Notes

- **Standard Protectron:** Most collectrons
- **DogCollectronRace:** F.E.T.C.H. and Junkyard Dog
- **RedRocketRobotRace:** Red Rocket Collectron (unique pump-shaped robot)
- **DLC01RoboBrainRace:** Biv Bev (only Robobrain)
- **WorkShopLiberatorRace:** Liberated Collectron (only Liberator)
- **HandyRace:** Mr. Farmhand (only Mr. Handy)

---

## 3. RESOURCE GENERATORS

### Workshop-Only Mineral Extractors (Require Snap to Resource Deposit)

All require 10 Power, have `WorkshopMustBeSnapped` + `WorkshopCollectorHardpoint` keywords.

| FormID | EDID | Display Name | Resource AV | CarryWeight |
|--------|------|-------------|-------------|-------------|
| `00090011` | WorkshopCollectorMineralCopper | Mineral Extractor - Copper | ResourceAV_Copper | 1.0 |
| `000906C3` | WorkshopCollectorMineralAluminum | Mineral Extractor - Aluminum | ResourceAV_Aluminum | 1.5 |
| `000906C6` | WorkshopCollectorMineralGold | Mineral Extractor - Gold | ResourceAV_Gold | 1.5 |
| `000958F8` | WorkshopCollectorMineralCrystal | Mineral Extractor - Crystal | ResourceAV_Crystal | 2.0 |
| `00095975` | WorkshopCollectorMineralLead | Mineral Extractor - Lead | ResourceAV_Lead | 1.5 |
| `00095A36` | WorkshopCollectorMineralNuclear | Mineral Extractor - Nuclear | ResourceAV_Nuclear | 1.0 |
| `00095A39` | WorkshopCollectorMineralSteel | Mineral Extractor - Steel | ResourceAV_Steel | 1.0 |
| `00322CFE` | WorkshopCollectorCoal | Coal Extractor | ResourceAV_Coal | 1.0 |
| `00322D00` | WorkshopCollectorMineralSilver | Mineral Extractor - Silver | ResourceAV_Silver | 1.5 |
| `00322D01` | WorkshopCollectorMineralTitanium | Mineral Extractor - Titanium | ResourceAV_Titanium | 1.0 |
| `00322D02` | WorkshopCollectorAcid | Acid Extractor | ResourceAV_Acid | 0.5 |
| `00322D03` | WorkshopCollectorCrudeOil | Oil Extractor | ResourceAV_Oil | 0.5 |
| `00322D04` | WorkshopCollectorJunkPile | Junk Extractor | ATX_ResourceAV_Collectron_Junk | 2.0 |
| `00322D05` | WorkshopCollectorConcrete | Concrete Extractor | ResourceAV_Concrete | 2.0 |
| `00322D06` | WorkshopCollectorWoodPile | Wood Extractor | ResourceAV_Wood | 2.0 |
| `003C4E0A` | WorkshopCollectorFertilizerPile | Fertilizer Extractor | ResourceAV_Fertilizer | 1.5 |

Workshop-only special extractors (higher power):

| FormID | Display Name | Power | Resource |
|--------|-------------|-------|----------|
| `003B2547` | Ammunition Factory | 20 | 10mm Ammo |
| `003EB3CF` | Fusion Core Processor | 100 | Fusion Cores |
| `003D1014` | Food Packaging Factory | 40 | Packaged Food |
| `0033E203` | Ashforge | 20 | Ash |

Skin variants: Enclave, Vault-Tec, and Garrahan Company skins exist for all extractors (same function, different appearance).

### CAMP-Placeable Fertilizer Producers

All have budget multiplier 9.0 and are limited to 1 per CAMP.

| FormID | EDID | Display Name | Resource AV | Source |
|--------|------|-------------|-------------|--------|
| `0008EDFB` | WorkshopCollectorFertilizer | Fertilizer Collector (Brahmin) | ResourceAV_FertilizerBrahmin | Base Game |
| `005B7614` | SCORE_S1_ATX_CAMP_Collector_ChickenCoop | Chicken Coop | ResourceAV_Fertilizer | Scoreboard S1 |
| `005E7EEA` | SCORE_S1_ATX_CAMP_Collector_ChickenCoop_GoldVendor | Chicken Coop (Gold) | ResourceAV_Fertilizer | Gold Bullion |
| `0068E8C3` | ATX_CAMP_Collector_CagedRabbit | Caged Rabbit | ResourceAV_Fertilizer | Atom Shop |
| `00785027` | MILE_CAMP_Collector_PheasantCoop | Rad-Pheasant Coop | ResourceAV_Fertilizer | Milestone |
| `0058F902` | W05_WorkshopCollectorTurboFertilizer | Turbo-Fert Fertilizer Collector | W05_ResourceAV_TurboFertFertilizer | Quest Reward |

### CAMP-Placeable Food / Drink Producers

| FormID | Display Name | Resource | CarryWt | Power | Spoilage |
|--------|-------------|----------|---------|-------|----------|
| `006F5369` | Mirelurk Steamer | Food | 10.0 | 0 | -- |
| `00804F53` | Raider Mirelurk Boil | Food | 10.0 | 0 | -- |
| `00787E2C` | Deathclaw Slow Roaster | Food | 10.0 | 0 | -- |
| `0068E77F` | Radstag Field Dressing Station | Radstag Meat | 10.0 | 0 | -50% |
| `00773E2C` | Thrasher Field Dressing Station | Rad Turkey | 10.0 | 0 | -50% |
| `007AEE0C` | Critter Cooker | Food | 2.0 | 0 | -- |
| `008308D2` | SoulSoup Server | Food | 2.0 | 0 | -- |
| `0083E779` | Wall-Mounted Oven | Food | 10.0 | 0 | -50% |
| `0083E778` | Pumpkin Pie Stand | Pumpkin Pie | 4.0 | 0 | -- |
| `0089A833` | Pile of Fish | Fish | 1.0 | 0 | -- |
| `0089AA07` | Brahmin Hide Tanning Rack | Leather | 1.0 | 0 | -- |
| `007D6ACA` | Live Bait Barrel | Bait | 1.0 | 0 | -- |
| `007D6B02` | Fish Trap | Fish | 10.0 | 0 | Water |
| `00864523` | Raider Fish Trap | Fish | 10.0 | 0 | Water |
| `0075FBEF` | Weenie Wagon | Canned Dog Food | 6.0 | 3 | -- |
| `006F06CB` | Cookie Jar | Mud Cookie | 1.0 | 0 | -- |
| `006F7E7E` | Birthday Cake | Cake | 2.0 | 0 | -50% |
| `007ACF9C` | Slime Cake | Cake | 2.0 | 0 | -50% |
| `0076D053` | Pemmican Collector | Pemmican | 1.0 | 0 | -50% |
| `008975A1` | Curing Shed | Cured Food | 1.0 | 0 | -- |
| `00848140` | Spice Rack | Spices | 2.0 | 0 | -- |
| `0075C8F4` | Spoiled Apple Tree | Apples | 1.0 | 0 | -- |
| `007461F3` | Water Boiler | Boiled Water | 15.0 | 0 | -- |
| `0089A4D5` | Steam Boiler | Boiled Water | 15.0 | 0 | -- |
| `00691C01` | Brahmin Milk Machine | Brahmin Milk | 6.0 | 1 | -- |
| `0076D046` | Motorized Butter Churn | Cooking Oil | 6.0 | 3 | -- |

### Beverage Producers

| FormID | Display Name | Produces | CarryWt | Source |
|--------|-------------|----------|---------|--------|
| `0067522F` | Slocum's Joe Coffee Machine | Survivalist Coffee | 2.0 | ATX |
| `0068E882` | Slocum's Flavorful Coffee Machine | Survivalist Coffee | 2.0 | ATX |
| `0078140A` | Red Rocket Coffee Machine | Survivalist Coffee | 2.0 | ATX |
| `00791C19` | The Sicilian Espresso Machine | Survivalist Coffee | 2.0 | S18 |
| `006F0686` | Company Tea Machine | Tea | 2.5 | S15 |
| `00787E2E` | Jumpy Juice Company Tea Machine | Tea | 2.5 | ATX |
| `007970FC` | Brewing Vat | Fermentable Beer | 15.0 | S18 |
| `007AE650` | Toxic Gooler Cooler | Toxic Goo | 3.0 | S20 |
| `00841307` | Barrel of Toxic Goo | Toxic Goo | 3.0 | ATX |

### Powered Machines

| FormID | Display Name | Produces | CarryWt | Power | Source |
|--------|-------------|----------|---------|-------|--------|
| `0061E20E` | Popcorn Machine | Popcorn | 6.0 | 3 | ATX |
| `00699AB3` | Hollywood Popcorn Machine | Popcorn | 6.0 | 3 | S13 |
| `007A7E79` | Nuka-Cola Popcorn Machine | Popcorn | 6.0 | 3 | ATX |
| `007D70DA` | Patriot Popper Popcorn Machine | Popcorn | 6.0 | 3 | ATX |
| `0067B398` | Nuka-Cola Candy Machine | Candy | 6.0 | 0 | ATX |
| `0067BBB3` | Nuka-Cola Quantum Candy Machine | Candy | 6.0 | 0 | ATX |
| `008B1CD2` | Nuka Victory Candy Machine | Candy | 6.0 | 0 | ATX |
| `007DC486` | The Whirl Wizard | Paint | 15.0 | 3 | ATX |
| `0085B259` | Shredder | Scrap | 6.0 | 3 | ATX |

### Ammo Producers

| FormID | Display Name | Ammo Type | CarryWt | Source |
|--------|-------------|-----------|---------|--------|
| `005957DE` | ArmCo Ammunition Construction Appliance | 10mm (configurable) | 1.0 | Wastelanders Quest |
| `008A639D` | Rip Daring Ammo Construction Appliance | 10mm (configurable) | 1.0 | S24 |
| `0088854A` | Amm-O-Matic (10mm) | 10mm | 0.3 | ATX |
| `0088854B` | Amm-O-Matic (.308) | .308 | 0.18 | ATX |
| `0088854C` | Amm-O-Matic (.44) | .44 | 0.168 | ATX |
| `0088854D` | Amm-O-Matic (.45) | .45 | 0.18 | ATX |
| `0088854E` | Amm-O-Matic (5.56) | 5.56 | 0.4 | ATX |
| `0088854F` | Amm-O-Matic (Shotgun Shells) | Shotgun Shells | 0.24 | ATX |

### Misc Resource Producers

| FormID | Display Name | Produces | CarryWt | Source |
|--------|-------------|----------|---------|--------|
| `00799F93` | Counterfeit Bottle Cap Press | Caps | 1.0 | ATX |
| `0067F3AB` | Tree Sap Collector | Adhesive | 0.5 | ATX |
| `0078BB4A` | The Nodding Donkey | Oil | 2.0 | ATX |
| `006A90E9` | Budding Apothecary | Flora | 1.0 | ATX |
| `006C5088` | Butterfly Sanctuary | Acid | 0.5 | ATX |
| `0073553D` | Mothman Moth Sanctuary | Acid | 0.5 | ATX |
| `00691C08` | Firewood Pile | Wood | 2.0 | S12 |
| `005FBD09` | Beehive | Honey | 1.0 | S4 |
| `00791C01` | Beehive (Black & Yellow) | Honey | 1.0 | S18 |
| `007B28AA` | Poisoned Earth Well | Poison | 1.0 | ATX |
| `00662E69` | Morbid Well Collector | Morbid items | 1.0 | ATX |
| `006F7E8C` | Mothman Nest | Mothman Egg | 1.0 | ATX |
| `00787E2D` | Pack Brahmin | Mixed | 5.0 | ATX |
| `007B2859` | Doctor's Orders Cigarette Machine | Cigarettes | 5.0 | ATX |
| `007A7D79` | Curio Cabinet | Curiosities | 2.0 | ATX |
| `0089A8B9` | Unknown Origins Curio Cabinet | Curiosities | 2.0 | S24 |
| `0079A1C8` | Enclave Data Center | Data/Scrap | 3.0 | ATX |
| `0082D340` | Whitespring Enclave Data Center | Data/Scrap | 3.0 | ATX |
| `0084CCD1` | Rocker Box | Mixed | 1.0 | ATX |
| `0085D824` | Crashed Cargo Bot | Mixed | 6.0 | S23 |
| `008A706F` | Rip Daring's Survival Cache | Mixed | 6.0 | S24 |
| `008B2F7F` | Camden Park Trash Bin | Junk | 2.0 | S25 |

### Fusion Core / Plasma Core Rechargers

| FormID | Display Name | Power | Source |
|--------|-------------|-------|--------|
| `00651DC7` | Fusion Core Recharger | 20 | ATX |
| `006E74FB` | Modern Home Fusion Core Recharger | 20 | ATX |
| `00773E14` | Vault-Tec Fusion Core Recharger | 20 | ATX |
| `007A7D61` | Industrial Fusion Core Recharger | 20 | ATX |
| `0079A58B` | Plasma Core Recharger | 20 | S19 |

### Key CAMP Limit GLOBs

Most CAMP resource producers are limited to 1 per CAMP via WorkshopCount GLOBs. Key examples:
- Brahmin (fertilizer): 1 per CAMP (`0004BE8D`)
- Chicken Coop: 1 (`005C763E`)
- Milk Machine: 1 (`006D342F`)
- Butter Churn: 1 (`0076D047`)
- Beehive: 1 (`005FE78E`)
- ArmCo: 1 (`005957E1`)
- Amm-O-Matic: 1 (`00854BEB`)
- Fish Trap: 1 (`008082A9`)
- Nodding Donkey: 1 (`0078E472`)
- Pet bowls/houses: 4 (`00411B85`)
- CAMP pets total: 1 (`0079954A`)

---

## 4. CAMP BUFF STATIONS

### Musical Instruments -- "Well Tuned" Buff

**Spell:** `0050CD15` SURV_WellTunedSpell "Well Tuned"
**Effect:** `0050CD14` SURV_WellTunedEffect -- **+25 AP regeneration** for **3600 seconds (60 minutes)**
**Keyword:** `0050CD11` FurnitureTypeInstrument
**Type:** Solo buff (player only)

| FormID | EDID | Display Name | Premium? |
|--------|------|-------------|----------|
| `0000CFA7` | Instrument_AcousticGuitar | Acoustic Guitar | No |
| `0000C22A` | Instrument_Banjo | Banjo | No |
| `00006F48` | Instrument_SteelGuitar | Steel Guitar | No |
| `00044DBC` | Instrument_SnareDrum | Snare Drum | No |
| `0010F30F` | Instrument_Grand_Piano | Grand Piano | No |
| `0010F3BC` | Instrument_Upright_Piano | Upright Piano | No |
| `0034D1F5` | Instrument_Tuba | Tuba | No |
| `0037B7F4` | Instrument_Bass | Bass | No |
| `003B7692` | Instrument_FrameDrum | Frame Drum | No |
| `001199CA` | Instrument_ChemicalBarrel | Chemical Barrel | No |
| `0033AE56` | Instrument_MetalBarrel | Metal Barrel | No |
| `004E0FA9` | Instrument_MouthHarp | Mouth Harp | No |
| `00662764` | Instrument_Nukelele | Nuka-lele | No |
| `00668025` | Instrument_Nukelele_Quantum | Nuka-lele (Quantum) | No |
| `006381B4` | Instrument_Pipe_Organ | Pipe Organ | No |
| `0055A338` | Instrument_Theremin | Theremin | Premium |
| `005D1C15` | ATX_Orgatronic | Orgatronic Deluxe | Premium |
| `005FD994` | ATX_Instrument_DrumSet | Drum Set | Premium |
| `0062C3E2` | ATX_Instrument_Pipe_Organ | Pipe Organ (ATX) | Premium |
| `0064E9E7` | ATX_Instrument_SkullDrumSet | Skull Drum Set | Premium |
| `0065BFF1` | ATX_Instrument_ResonatorGuitar | Resonator Guitar | Premium |
| `00678D16` | ATX_Instrument_ResonatorGuitar_B | Resonator Guitar B | Premium |
| `00664E7D` | ATX_Instrument_Hambone | Hambone Stool | Premium |
| `0066F201` | SCORE_S16_Instrument_HomemadeXylophone | Homemade Xylophone | Premium |
| `00691B73` | SCORE_S12_Instrument_Violin | Violin | Premium |
| `006B8E0E` | EXP17_Instrument_DrumSet | Drum Set (Expedition) | No |
| `00728409` | ATX_Instrument_Xylophone | Xylophone | Premium |
| `007746A5` | ATX_Furniture_Instrument_Accordion | Accordion | Premium |
| `007AE386` | Furniture_Instrument_Accordion_FROMATX | Accordion (Free) | No |
| `0078C78E` | MILE_Workshop_Instrument_Saxophone | Rusty Saxophone | No |
| `007AE566` | SCORE_S20_Instrument_ChemicalBarrelDrum_Blue | Chemical Barrel Drum (Blue) | Premium |
| `007AE563` | SCORE_S20_Instrument_MetalBarrelDrum_Radioactive | Radioactive Barrel Drum | Premium |
| `007AE564` | SCORE_S20_Instrument_MetalBarrelDrum | Metal Barrel Drum | Premium |
| `007AE565` | SCORE_S20_Instrument_ChemicalBarrelDrum | Chemical Barrel Drum | Premium |

### SPECIAL Stat Buff Stations

All SPECIAL buffs use:
- **Duration:** `0065015E` ATX_SPECIAL_BuffDurationGlobal = **1800 seconds (30 minutes)**
- **Magnitude:** `0065015F` ATX_SPECIAL_BuffMagnitudeGlobal = **+2 to SPECIAL stat**
- **Type:** Solo buff

#### Strength (+2 STR, 30 min)
Keyword: `005B359F` ATX_FurnituretypeStrength | Spell: `005B519D` ATX_BuffStrength

| FormID | Display Name | Source |
|--------|-------------|--------|
| `0056744C` | Weight Bench | SCORE S2 (Premium) |
| `005F56CA` | Weight Bench (Gold Vendor) | Gold Bullion |

#### Agility (+2 AGI, 30 min)
Keyword: `005EDEE0` ATX_FurnituretypeAgility | Spell: `005EDEE5` ATX_BuffAgility

| FormID | Display Name | Dual-Stat? | Source |
|--------|-------------|-----------|--------|
| `005D80E0` | Antique Speed Bag | AGI only | SCORE S3 |
| `00609E0A` | Antique Speed Bag (Gold Vendor) | AGI only | Gold Bullion |
| `00804F5B` | Raider Speed Bag | AGI only | ATX |
| `006F459F` | Boardwalk Bonanza Pinball Machine | AGI + PER | ATX |
| `0076E0FF` | Astro Attack Pinball Machine | AGI + PER | ATX |
| `007DB370` | Vault-Tec Pinball Machine | AGI + PER | ATX |
| `008B12E0` | Rip Daring Pinball Machine | AGI + PER | Mini Season 2026 |
| `0076A6C0` | Atomic Roller Machine | AGI + PER | ATX |
| `007CEA86` | Five Finger Filet Table | AGI + LCK | ATX |
| `007CF731` | Cosmic Capture | AGI only | Invaders Event |

#### Endurance (+2 END, 30 min)
Keyword: `00644EA3` ATX_FurnituretypeEndurance | Spell: `00644EA7` ATX_BuffEndurance

| FormID | Display Name | Source |
|--------|-------------|--------|
| `00644EA2` | Exercise Bike | SCORE S8 |
| `00830006` | Ice Bath (ZZZ) | S22 (deprecated) |

#### Charisma (+2 CHA, 30 min)
Keyword: `0065015A` ATX_FurnituretypeCharisma | Spell: `0061F6AC` ATX_BuffCharisma

| FormID | Display Name | Dual-Stat? | Source |
|--------|-------------|-----------|--------|
| `006628DA` | Blue 9 Ball Table | CHA only | SCORE S15 |
| `006628D8` | Red 9 Ball Table | CHA only | Fallout 1st |
| `006628D9` | Wood 9 Ball Table | CHA only | ATX |
| `006628DB` | Green 9 Ball Table | CHA only | ATX |
| `00840E70` | Rustic 9 Ball Table | CHA only | ATX |
| `006B8E0F` | Casino Pool Table | CHA only | Expedition 17 |
| `0078CA7D` | Pool Table (Storm) | CHA only | Storm Quest |
| `00646D80` | Arm Wrestle Machine | CHA only | SCORE S9 |
| `00684BAA` | Arm Wrestle Machine (Vault Girl) | CHA only | ATX |
| `007DBEAA` | Poseidon Arm Wrestle Machine | CHA only | SCORE S21 |
| `006D1DD7` | Arm Wrestle Machine (EXP17) | CHA only | Expedition 17 |
| `006A274D` | Hollywood Vanity | CHA only | SCORE S13 |
| `007267B6` | Hollywood Vanity (Free) | CHA only | From ATX |
| `006E739E` | Shoeshine Machine | CHA only | ATX |
| `006A910B` | Bowling Alley Lane | CHA + LCK | ATX |
| `006EB2BF` | American Bowling Alley Lane | CHA + LCK | ATX |

#### Perception (+2 PER, 30 min)
Keyword: `0065015D` ATX_FurnituretypePerception | Spell: `00650156` ATX_BuffPerception

| FormID | Display Name | Source |
|--------|-------------|--------|
| `006628DF` | Radiation Glove Box | ATX |
| `00766AA3` | Stargazer's Telescope | Milestone |
| *(plus all dual-stat Pinball/Roller machines listed under Agility)* | | |
| *(plus all dual-stat Bowling Arcade machines listed under Luck)* | | |

#### Intelligence (+2 INT, 30 min)
Keyword: `0065015B` ATX_FurnituretypeIntelligence | Spell: `00650155` ATX_BuffIntelligence

| FormID | Display Name | Source |
|--------|-------------|--------|
| `007D6A59` | Summoning Circle | ATX |

#### Luck (+2 LCK, 30 min)
Keyword: `0065015C` ATX_FurnituretypeLuck | Spell: `0060497D` ATX_BuffLuck

| FormID | Display Name | Dual-Stat? | Source |
|--------|-------------|-----------|--------|
| `006C50B8` | Bowling Arcade Machine | LCK + PER | ATX |
| `006FA9F0` | Bowling Arcade Machine (Stars & Strikes) | LCK + PER | ATX |
| `006FCB2E` | Rolling Stars Bowling Arcade Machine | LCK + PER | ATX |
| `008B12DC` | Camden Park Claw Machine | LCK + PER | S25 |
| *(plus Bowling Alley Lanes listed under Charisma as CHA + LCK)* | | |
| *(plus Five Finger Filet Table listed under Agility as AGI + LCK)* | | |

### Sleep / Bed Buffs

| Bed Type | Keyword | Spell | Buff | Duration |
|----------|---------|-------|------|----------|
| Sleeping Bag | `003CD038` BedTypeSleepingBag | `0005C528` SURV_WellRested | Rested: +5% XP | 60 min |
| Mattress | `003CD037` BedTypeMattress | `0005C528` SURV_WellRested | Rested: +5% XP | 60 min |
| Comfy Bed | `003CD036` BedTypeComfy | `003CD033` SURV_WellRested2 | Well Rested: +5% XP, +2 AGI | 2-3 hrs |
| Near non-romanced companion | -- | `0059E3F0` COMP_WellRested3_KindredSpirit | Kindred Spirit: +5% XP, +2 PER | 3 hrs |
| Near romanced companion | -- | `0059E3F1` COMP_WellRested3_LoversEmbrace | Lover's Embrace: +5% XP, +2 CHA | 3 hrs |

### Rested-Type Furniture (Not Beds)

Keyword: `005A4E2B` ATX_FurnituretypeRested -- provides a Rested-type buff

| FormID | Display Name | Source |
|--------|-------------|--------|
| `005A2C4B` | Communal Firepit | ATX |
| `0060212D` | Hot Tub | ATX |
| `0060EC27` | Vault-Tec Spa | ATX |
| `00677B9B` | Cappy Hot Tub | SCORE S11 |
| `0068380D` | Goo-Tub | SCORE S20 |
| `0068DF0A` | Skulls Fire Pitt | ATX |
| `00692563` | Communal Campfire | Moon Event |
| `0079B885` | Cauldron Hot Tub | Mischief Night 2 |

### Special Buff Stations

#### Phoropter -- VATS Accuracy Buff (NEW in S24)
- **FormID:** `0089ADB0` | **Spell:** `0089ADB5` "Accuracy Boost"
- **Effect:** +25% VATS Accuracy for 7200 seconds (2 hours)
- **Source:** SCORE S24 (Premium)

#### Lethal Loveseat -- Chem Duration Buff (Love Hurts Mini Season)
- **FormID:** `00897409` | **Spell:** `00897408` "Rush of Love"
- **Effect:** +25% Chem Duration for 3600 seconds (60 minutes)
- **Source:** Love Hurts Mini Season (Premium)

#### Sharpening Stone -- Melee Blood Pack Chance (NEW)
- **FormID:** `008B1D5A` | **Spell:** `008D1C9C` "Sharpening Stone"
- **Effect:** 5% Blood Pack chance from melee kills for 1800 seconds (30 minutes)
- **Source:** ATX (Premium)

#### Rip Daring Vault Boy Statue -- "Rip's Bounty" (NEW)
- **FormID:** `008B1553` | **Spell:** `008B1D5E` "Rip's Bounty"
- **Effect:** Cryptids may drop extra scraps for 3600 seconds (60 minutes)
- **Source:** Weapons Expert Mini Season 2026 (Premium)

#### Mothman XP Buffs
- **Sacred Mothman Tome** (`00755EB1`): +5% XP for 60 min (Equinox event reward, not craftable)
- **Scarberry's Shrine** (`0068D3D5`): Companion uses it to apply +5% XP for 60 min (team buff)

#### Utility Stations (Not Traditional Buffs)
- **Sympto-matic** (`005D98A5`): Cures all diseases. Keyword `005D98A6` FurnitureTypeDiseaseCure
- **Blood Transfusion Pump** (`007B2841`): Restores health / removes rads (humans) or gives rads (ghouls). 15 min cooldown.

### Key Buff Keyword Reference

| FormID | Keyword EDID | Buff |
|--------|-------------|------|
| `0050CD11` | FurnitureTypeInstrument | Well Tuned (+25 AP regen, 60 min) |
| `005B359F` | ATX_FurnituretypeStrength | +2 STR (30 min) |
| `005EDEE0` | ATX_FurnituretypeAgility | +2 AGI (30 min) |
| `00644EA3` | ATX_FurnituretypeEndurance | +2 END (30 min) |
| `0065015A` | ATX_FurnituretypeCharisma | +2 CHA (30 min) |
| `0065015B` | ATX_FurnituretypeIntelligence | +2 INT (30 min) |
| `0065015C` | ATX_FurnituretypeLuck | +2 LCK (30 min) |
| `0065015D` | ATX_FurnituretypePerception | +2 PER (30 min) |
| `005A4E2B` | ATX_FurnituretypeRested | Rested (varies) |
| `0076B52B` | FurnitureTypeXPBonus | +5% XP (60 min) |
| `0089ADB2` | FurnituretypeAccuracy | +25% VATS Accuracy (2 hrs) |
| `00897407` | LoveHurts_FurnituretypeChemDuration | +25% Chem Duration (60 min) |
| `003CD036` | BedTypeComfy | Well Rested (+5% XP, +2 AGI) |
| `003CD037` | BedTypeMattress | Rested (+5% XP) |
| `003CD038` | BedTypeSleepingBag | Rested (+5% XP) |
| `005D98A6` | FurnitureTypeDiseaseCure | Disease cure |

---

## 5. PETS

### Master Pet List (FLST `0079AF16` CAMPPets_Actors -- 25 entries)

**CAMP limit:** 1 pet total (GLOB `0079954A` CAMPPets_WorkshopCount = 1)
**All pets share factions:** WorkshopNPCFaction, CaptiveFaction, PlayerAllyFaction

### Cats (11)

| # | FormID | EDID | Display Name | Source |
|---|--------|------|-------------|--------|
| 1 | `0077D821` | CAMPPets_Actor_Cat_Tabby | Grey Tabby | Base game (free) |
| 2 | `007A19C7` | SCORE_S19_CAMPPets_Actor_Cat_BlackCat | Bombay Cat | SCORE S19 |
| 3 | `007AE523` | ATX_CAMPPets_Actor_Cat_Sphynx | Sphynx Cat | Atom Shop |
| 4 | `007DC477` | ATX_CAMPPets_Actor_Cat_Wild | Wild Cat | Atom Shop |
| 5 | `00804BB0` | ATX_CAMPPets_Actor_Cat_Farm | Farm Cat | Atom Shop |
| 6 | `0082BCB9` | ATX_CAMPPets_Actor_Cat_RoboPaw | RoboPaw Steel Cat | Atom Shop (unique RoboPaw race) |
| 7 | `0083FF20` | SCORE_S22_CAMPPets_Actor_Cat_Ragdoll | Ragdoll Cat | SCORE S22 |
| 8 | `00853B88` | SCORE_S23_CAMPPets_Actor_Cat_Lykoi | Lykoi Cat | SCORE S23 |
| 9 | `008A5DF8` | SCORE_S24_CAMPPets_Actor_Cat_GlowingCat | Glowing Cat | SCORE S24 |
| 10 | `008AFF68` | SCORE_S25_CAMPPets_Actor_Cat_CyprusCat | Cyprus Cat | SCORE S25 |
| 11 | `008B1209` | ATX_CAMPPets_Actor_Cat_MrPebbles | Mr. Pebbles | Atom Shop |

**Cat stats:** Level 1, CalcHealth 15, CalcActionPts 50, Race: CAMPPets_Cat_TabbyRace (except RoboPaw: CAMPPets_Cat_RoboPawRace)

### Dogs (9)

| # | FormID | EDID | Display Name | Source |
|---|--------|------|-------------|--------|
| 1 | `0077D822` | CAMPPets_Actor_Dog_GermanShepherd | German Shepherd | Base game (free) |
| 2 | `007A19B8` | SCORE_S19_CAMPPets_Actor_Dog_WhiteGermanShepherd | White Shepherd | SCORE S19 |
| 3 | `007B28FA` | ATX_CAMPPets_Actor_Dog_Rottweiler | Rottweiler | Atom Shop |
| 4 | `0082A9A0` | ATX_CAMPPets_Actor_Dog_RoboPaw | RoboPaw Steel Dog | Atom Shop (unique RoboPaw race) |
| 5 | `008335D3` | SCORE_S22_CAMPPets_Actor_Dog_Sable | Sable Shepherd | SCORE S22 |
| 6 | `0085B0CC` | ATX_CAMPPets_Actor_Dog_RoboPawPS | RoboPaw Blue | Atom Shop (PS-themed) |
| 7 | `0084132D` | ATX_CAMPPets_Actor_Dog_Mongrel | Mongrel | Atom Shop |
| 8 | `008AFDFB` | ATX_CAMPPets_Actor_Dog_Goodboy | Goodboy | Atom Shop |
| 9 | `008B1D6F` | SCORE_S25_CAMPPets_Actor_Dog_GlowingDog | Glowing Dog | SCORE S25 |

**Dog stats:** Level 3, CalcHealth 190, CalcActionPts 100, Race: CAMPPets_Dog_GermanShepherdRace (except RoboPaw variants: CAMPPets_Dog_RoboPawRace)

### Radhogs (4)

| # | FormID | EDID | Display Name | Source |
|---|--------|------|-------------|--------|
| 1 | `0084FB8F` | ATX_CAMPPets_Actor_RadHog_Standard | Radhog | Atom Shop |
| 2 | `0089A8C7` | ATX_CAMPPets_Actor_Radhog_Rooter_Copy01 | Rooter | Atom Shop |
| 3 | `008B1552` | ATX_CAMPPets_Actor_Radhog_Gruyere | Gruyere | Atom Shop |
| 4 | `008B1F0D` | SCORE_S25_CAMPPets_Actor_Radhog_GlowingHog | Glowing Radhog | SCORE S25 |

**Radhog stats:** Level 15, CalcHealth 450 (standard) or 190 (variants), CalcActionPts 150/100, Race: CAMPPets_RadHogRace

### Deathclaw (1)

| # | FormID | EDID | Display Name | Source |
|---|--------|------|-------------|--------|
| 1 | `008A52AB` | ATX_CAMPPets_Actor_Deathclaw_DC | Deathclaw | Atom Shop |

**Deathclaw stats:** Level 15, CalcHealth 450, CalcActionPts 150, Race: CAMPPets_DeathclawRace

### Pet Emotes (FLST `0079AF17` -- 13 emotes)

- **Dog:** Sit, Speak, Pet
- **Cat:** Sit, Speak, Pet, High Five
- **Radhog:** Sit, Speak, Pet
- **Deathclaw:** Sit, Speak, Pet

### Pet Apparel (13 items, crafted at Armor Workbench)

| FormID | Display Name | Pet Type |
|--------|-------------|----------|
| `0078B4F7` | Red Bow Collar | Cat |
| `0078B4F6` | Red Bow Collar | Dog |
| `007B2846` | Rusted Chain Collar | Dog |
| `007DBEEE` | Leather Collar | Cat |
| `00840E06` | Responders Bandana | Dog |
| `00852175` | Nose Ring | Radhog |
| `00853B79` | Rusty Nails Collar | Cat |
| `00853B7A` | Rusty Nails Collar | Dog |
| `00868F6F` | Sooie-Heart Ring | Radhog |
| `008A52B0` | Ochre Plating | Deathclaw |
| `008AF6AD` | Steel Plating | Deathclaw |
| `008B0D71` | Edelweiss Ring | Radhog |
| `008B0D81` | Star Collar | Dog |

### Pet Food (6 recipes, crafted at Cooking Station)

| FormID | Display Name | Type |
|--------|-------------|------|
| `0042A519` | Forrage Porridge | Plant |
| `0042A51C` | Veggie Vittles | Plant |
| `0042A51D` | Seed Feed | Plant |
| `0042A51A` | Meat Treats | Meat |
| `0042A51E` | Burger Bits | Meat |
| `0042A51F` | Steak Sticks | Meat |

### Legacy Pet Systems

5 old PETS_ prefix pet NPCs exist (PETS_Cat_01_Grey, PETS_Cat_02_Black, PETS_Cat_03_Orange, PETS_Dog_01_Brown, PETS_Dog_02_Black) using `PETS_PetFriendsToAllPlayers` faction and the old CatRace/RaiderDogRace instead of CAMPPets-specific races. These are from the pre-CAMPPets system.

---

## 6. WATER PURIFIERS / GENERATORS

### Water Types Produced

| FormID | EDID | Display Name | Sellable |
|--------|------|-------------|----------|
| `0000EE1D` | WaterDirty | Dirty Water | No |
| `000366BF` | WaterBoiled | Boiled Water | No |
| `000366C0` | WaterPurified | Purified Water | Yes (20 caps) |

### Production Rates

| Interval GLOB | Value (hrs) | Minutes | Used By |
|---------------|-------------|---------|---------|
| `0045128B` ResourceProductionIntervalHoursWater | 0.0835 | ~5 min | Standard purifiers, hand pump, water well |
| `0075B0CA` ATX_ResourceProductionIntervalHours_Boiler_BoiledWater | 0.1700 | ~10.2 min | Water Boiler, Steam Boiler |
| `00799A9A` ATX_ResourceProductionIntervalHours_Cooler_PurifiedWater | 0.1700 | ~10.2 min | Enclave Water Cooler |
| `007F5556` ATX_ResourceProductionIntervalHours_DirtyWater | 0.1700 | ~10.2 min | Dirty Pond, Rain Water Collector, Railroad Water Tower |

### Base Game Water Producers

| FormID | Display Name | Water Type | Power | Placement | Crafting Cost |
|--------|-------------|-----------|-------|-----------|---------------|
| `00332681` | Water Pump | Dirty (with rads) | None | Ground (dirt) | 1 Gears, 4 Steel, 1 Concrete |
| LVLI `00471384` | Water Purifier - Small | Purified | Yes (8) | Ground | 2 Ceramic, 4 Copper, 4 Oil, 8 Rubber, 15 Steel, 4 Screws |
| LVLI `00464834` | Water Purifier (Medium) | Purified | Yes (4) | In water | 2 Ceramic, 2 Copper, 2 Oil, 5 Rubber, 10 Steel, 2 Cloth |
| LVLI `00464835` | Water Purifier - Industrial | Purified | Yes (10) | In water | 2 Ceramic, 4 Copper, 4 Oil, 10 Rubber, 20 Steel, 6 Screws, 4 Cloth |
| `00582109` | Water Well | Dirty (NO rads) | None | Ground (dirt) | 1 Gears, 4 Steel, 1 Concrete (Gold Bullion plan) |

### Atom Shop / Scoreboard Unique Water Producers

| FormID | Display Name | Water Type | Interval | Source |
|--------|-------------|-----------|----------|--------|
| `007461F3` | Water Boiler | Boiled | 0.1700 hrs | SCORE S16 / Gold Bullion |
| `0089A4D5` | Steam Boiler | Boiled | 0.1700 hrs | Atom Shop |
| `00799A92` | Enclave Water Cooler | Purified | 0.1700 hrs | SCORE S19 |
| `007F555A` | Dirty Pond | Dirty | 0.1700 hrs | SCORE S21 |
| `005C9B1D` | Rain Water Collector | Dirty | 0.1700 hrs | ATX |
| `00694BB6` | Railroad Water Tower | Dirty | 0.1700 hrs | ATX |
| `005FBD08` | Vintage Water Cooler | Purified | 0.0835 hrs | ATX |
| `0068D480` | Deep Well | Purified | 0.0835 hrs | ATX |

### Purifier Skins (Cosmetic Only, Same Stats)

**Small purifier skins:** Corvega (`00422B89`), Clean (`004949E9`), Vault-Tec (`004949EA`), Vault-Tec Prototype (`005F57BE`)
**Medium purifier skin:** Clean (`00464833`)
**Industrial purifier skins:** Corvega (`00405D78`), Clean (`004949ED`)
**Water pump skin:** Corvega (`00405D30`)

### Drinkable Water Fixtures (NOT Passive Producers)

These provide water on activation (drinking) but do NOT passively fill a container:

Fire Hydrants (5 skins), Drinking Fountains (3 skins), Sinks (6+ skins), Junkyard Fountain, Fish Cleaning Table -- all produce Dirty Water when drunk from.

---

## APPENDIX: KEY FORMID QUICK REFERENCE

### Weather Station System
- Master COBJ: `006EE8ED` | LVLI: `006EE8F6`
- FormLists: Stations `006EE8F0` | Weathers `006EE8EF` | Keywords `006EE8F1`
- CAMP limit GLOB: `006EE8F3` (1 per CAMP)
- Terminal: `0072DB96`

### Collectron System
- Master LVLI: `00536D30` ATX_workshop_LL_Collectrons (25 entries)
- Universal keyword: `0052EE0B` WorkshopCollectorObject
- ContainerTakeOnly: `003D019D`

### Pet System
- Master FLST: `0079AF16` CAMPPets_Actors (25 entries)
- Emotes FLST: `0079AF17` CAMPPets_Emotes (13 entries)
- Furniture FLST: `0079AF18` CAMPPets_Furnitures (26 entries)
- Keywords FLST: `0079AF19` CAMPPets_Keywords (29 entries)
- CAMP limit GLOB: `0079954A` (1 pet per CAMP)
- Type keyword: `00797094` ActorTypeCAMPPet
- Gift AV: `0079B107` CAMPPets_PetHasGiftForPlayer01

### Buff System
- SPECIAL duration GLOB: `0065015E` (1800 sec / 30 min)
- SPECIAL magnitude GLOB: `0065015F` (+2)
- Well Tuned spell: `0050CD15` (3600 sec / 60 min)
- Well Rested spell: `0005C528` (3600 sec / 60 min)
- Well Rested 2 spell: `003CD033` (7200-10800 sec / 2-3 hrs)
