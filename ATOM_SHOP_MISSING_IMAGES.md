# Atom Shop — Missing Images Audit

Checked 2026-09-07. Sources: dist/atom_shop.json (534 items + 13 LTB bundles), dist/survival_tent_interiors.json (17 tents).
Every image URL HEAD-checked live against buffsnbrew.com.

## 1. Broken links — imageUrl set in JSON, file 404s on the server (47)

| Item | Expected file |
|---|---|
| Blood Eagle Power Armour Paint | `ATX_Skin_PowerArmor_Paint_BloodEagle.avif` |
| BOS Scouting Tower | `SCORE_S2_CAMP_Structure_BoSTower.avif` |
| Captain Cosmos Power Armour Bundle | `score_s1_skin_powerarmor_paint_captaincosmos.avif` |
| Charleston Fire Station Bundle | `atx_camp_structure_charleston_firestation.avif` |
| Cowboy Hat | `ATX_Apparel_Headwear_Western_Hat_02.avif` |
| Duster | `ATX_Apparel_Outfit_Western_05_Duster.avif` |
| Down Home Wallpaper Set | `ATX_CAMP_WallPaper_Sport_03.avif` |
| Greenhouse Kit Bundle | `score_s2_camp_kit_greenhouse.avif` |
| Hammock | `ATX_CAMP_Bed_Hammock.avif` |
| Haunted House Porch Set | `ATX_CAMP_Kit_HauntedHousePorchSet.avif` |
| Haunted House Staircase | `ATX_CAMP_Structure_Stairs_HauntedHouse.avif` |
| Hubris Comics Wallpaper | `ATX_CAMP_WallPaper_Sport_03.avif` |
| Laser Grid Door | `ATX_CAMP_Door_Metal_LaserGrid.avif` |
| Leprechaun Outfit | `ATX_Apparel_Outfit_VaultBoy_Leprechaun.avif` |
| Matte Black Handmade Paint | `atx_skin_weaponskin_handmadegun_matteblack.avif` |
| Missile Silo Shelter Bundle | `shelters_shelterentrance_nuclearmissilesilo.avif` |
| Moonshine Mama Mask | `ATX_Apparel_Headwear_Fasnacht_Moon.avif` |
| Mothman Bed | `ATX_CAMP_Bed_Twinbed_Mothman.avif` |
| Mothman Pip-Boy Paint | `atx_pipboy_mothman.avif` |
| Mothman Wallpaper Bundle | `atx_camp_wallpaper_sport_03.avif` |
| Neon Palm Reader Sign | `ATX_ENTM_CAMP_Lights_NeonPalmReaderSign.avif` |
| New Year's Icon | `ATX_PlayerIcon_Holiday_15.avif` |
| Nuka-Cola Quantum Sign | `ATX_CAMP_Lights_SignNukaQuantum01.avif` |
| Park Ranger Power Armour Skin | `ATX_Skin_PowerArmor_Paint_ParkRanger.avif` |
| Pumpkin Vault Girl Head | `ATX_Apparel_Headwear_PumpkinVaultGirlMask.avif` |
| Rabbit Plushie | `ATX_CAMP_FloorDecor_Plushie_Rabbit.avif` |
| Red Modular Mainframe | `ATX_CAMP_FloorDecor_ModularMainFrame_Red.avif` |
| Roadtripper Outfit | `ATX_Apparel_Outfit_1950sCasualStylish.avif` |
| RR Ranger Power Armour Paint | `ATX_Skin_PowerArmor_Paint_RRRanger.avif` |
| Rusted Chain link Fence | `ATX_CAMP_Defense_Fence_RustedChainLink.avif` |
| Revolutionary Outfit | `ATX_Apparel_Outfit_FreeStates_Revolutionary.avif` |
| Samurai Outfit | `ATX_Apparel_Outfit_Samurai.avif` |
| Sheepsquatch Plushie | `ATX_CAMP_FloorDecor_Plushie_Sheepsquatch.avif` |
| Skull Lord War Suit | `ATX_Apparel_Outfit_SkullLord.avif` |
| Silver Shroud Plushie | `ATX_CAMP_FloorDecor_Plushie_SilverShroud.avif` |
| Solid Color Wallpaper Set | `ATX_CAMP_WallPaper_Sport_03.avif` |
| Stocking Set | `ATX_CAMP_WallDeco_Stockings.avif` |
| Vault-Tec Grandfather Clock | `ATX_CAMP_FloorDecor_GrandfatherClock_VaultTec.avif` |
| Vault-Tec Locker Bay | `ATX_CAMP_StashBox_SheltersVaultTecLockerBay.avif` |
| Waste Barrel Planter | `ATX_CAMP_FloorDecor_Planter_WasteBarrel.avif` |
| Wasteland Werewolf Outfit | `ATX_Apparel_Outfit_WastelandWerewolf.avif` |
| Welcome Friends Sign | `ATX_CAMP_Lights_SignWelcomeFriends.avif` |
| Western Bar Bundle | `atx_camp_furniture_barset_oldwest.avif` |
| Whitesprings Wallpaper Set | `ATX_CAMP_WallPaper_Sport_03.avif` |
| Wildwood Tavern Bundle | `atx_camp_structure_wildwoodtavern.avif` |
| Wolf Howl Emote | `ATX_CAMP_Lights_WolfHowl.avif` |
| Worm Farm | `ATX_CAMP_FloorDecor_WormFarm.avif` |

## 2. Survival Tent Skins — missing (1)

| Tent | Expected file |
|---|---|
| GNN News Van (Survival Tent) | `GNN News Van Survival Tent.avif` |

## 3. Limited-Time Bundles

- All 13 bundle cover images are empty (`imageUrl: ""`) — by design, JS shows a placeholder.
- `Brotherhood Recruitment Bundle > Brotherhood of Steel Salute` — no imageUrl.
- 13 bundle-item rows point at `/season_images/utility/` files that 404:
  `score_currency_atoms.avif`, `atx_utility_repairkit_basic.avif`, `atx_utility_scrapkit_basic.avif`

## 4. Items with no imageUrl at all (68)

Mostly legacy bundles (deliberate placeholder — the renderer will not borrow a contents image). The genuine gaps are the emotes:

| Emote | EDID |
|---|---|
| Angry Fist Shake | `ATX_ENTM_Emotes_Angry_Fistshake` |
| Communist Salute Emote | `ATX_ENTM_Emotes_Hello_Salute_Communist` |
| Grognak Battlecry | `ATX_ENTM_Emotes_BattleCry_Grognak` |
| Holidays Emote Bundle | `ATX_ENTM_Emotes_Interactions_Holiday2018` |
| Mothman Worship Emote | `ATX_ENTM_Emotes_Misc_MothmanWorship` |
| No Thank You Emote | `ATX_ENTM_Emotes_No_NoThankYou` |
| No Way Emote | `ATX_ENTM_Emotes_No_UniversalNo` |
| Raider Salute | `ATX_ENTM_Emotes_Hello_Salute_Raider` |
| Share the Love Bundle | `ATX_ENTM_Emotes_Love_Holiday2018` |
| Super Angry | `ATX_ENTM_Emotes_Angry_Supermutant` |

## 5. Matches found in `.CAMP Items`

**None.** All 3,164 files (1,231 images) in `OneDrive\Guides and Stuff\.CAMP Items` were indexed and matched by
filename, EDID stem and display-name tokens against all 116 missing entries. Zero real matches.

The folder only holds CAMP buff stations, collectrons, resource producers, weather stations, pets/allies and
their cover banners. The missing Atom Shop entries are apparel, power-armour paints, wallpaper, plushies,
structures and emotes — none of which are in that folder.

Near-misses checked and rejected (same word, different item):

- `score_s11_camp_utility_collectron_nukaquantum_*` — Nuka-Quantum **Collectron**, not the Nuka-Cola Quantum **Sign**
- `atx_camp_utility_weatherstation_mothman_*` — Mothman **weather station**, not the Mothman **Bed**
- `shelters_shelterentrance_gleamingdepths_*` — Gleaming Depths shelter, not the **Missile Silo** shelter
- `score_s19_camp_camppets_cat_blackcat_*` — Black Cat **pet**, not the Black Cat **Bundle** cover
- `atx_entm_camp_defense_fence_corrugatedsteel_*` — corrugated steel fence, not **rusted chain link**

The `tents/` subfolder holds only raw `.dds` game textures — nothing usable for the GNN News Van.
