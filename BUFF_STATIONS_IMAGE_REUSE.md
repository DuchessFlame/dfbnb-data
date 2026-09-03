# Buff Stations — images already in wp-content

Checked 2026-09-03 against the live site. **All 84 image slots on /bnb/camp-items/buff-stations/ currently 404** — the `guide-images/camp-items/buff-stations/` folder is empty.

**24 of 86 items can reuse a file already hosted** — no re-upload needed. Every URL below was HEAD-verified 200.

| Item | EDID | Existing wp-content URL |
|---|---|---|
| Theremin | `Instrument_Theremin` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_CAMP_Furniture_Instrument_Theremin.avif` |
| Communal Firepit | `ATX_Communal_Firepit` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_CAMP_Structure_Communal_Firepit.avif` |
| Orgatronic Deluxe | `ATX_Orgatronic` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/atx_camp_furniture_instrument_orgatronic.avif` |
| Hot Tub | `ATX_HotTub` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_Camp_FloorDecor_HotTub.avif` |
| Vault-Tec Spa | `ATX_VaultTecSpa` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_CAMP_Decoration_VaultTecSpa.avif` |
| Pipe Organ (Atom Shop) | `ATX_Instrument_Pipe_Organ` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_CAMP_Furniture_Instrument_Pipe_Organ.avif` |
| Pipe Organ | `Instrument_Pipe_Organ` | `/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_CAMP_Furniture_Instrument_Pipe_Organ.avif` |
| Weight Bench | `SCORE_S2_Furniture_Weightbench` | `/wp-content/uploads/season_images/season-2/score_s2_utility_weightbench.avif` |
| Exercise Bike | `SCORE_S8_Furniture_ExerciseBike` | `/wp-content/uploads/season_images/season-8/score_s8_camp_utility_exercisebike.avif` |
| Arm Wrestle Machine | `SCORE_S9_ArmWrestleMachine` | `/wp-content/uploads/season_images/season-9/score_s9_camp_utility_armwrestlemachine.avif` |
| Cappy Hot Tub | `SCORE_S11_Furniture_CappyHotTub` | `/wp-content/uploads/season_images/season-11/score_s11_camp_furniture_cappyhottub.avif` |
| Violin | `SCORE_S12_Instrument_Violin` | `/wp-content/uploads/season_images/season-12/score_s12_camp_furniture_instrument_violinchair.avif` |
| Hollywood Vanity | `SCORE_S13_Misc_HollywoodVanity` | `/wp-content/uploads/season_images/season-13/score_s13_camp_utility_hollywoodvanity.avif` |
| Hollywood Vanity (Free) | `SCORE_S13_Misc_HollywoodVanity_FROMATX` | `/wp-content/uploads/season_images/season-13/score_s13_camp_utility_hollywoodvanity.avif` |
| Homemade Xylophone | `SCORE_S16_Instrument_HomemadeXylophone` | `/wp-content/uploads/season_images/season-16/score_s16_camp_furniture_instrument_homemadexylophone.avif` |
| Goo-Tub | `SCORE_S20_Furniture_GooTub` | `/wp-content/uploads/season_images/season-20/score_s20_camp_decoration_gootub.avif` |
| Metal Barrel Drum | `SCORE_S20_Instrument_MetalBarrelDrum` | `/wp-content/uploads/season_images/season-20/score_s20_camp_furniture_instrument_metalbarreldrum.avif` |
| Chemical Barrel Drum | `SCORE_S20_Instrument_ChemicalBarrelDrum` | `/wp-content/uploads/season_images/season-20/score_s20_camp_furniture_instrument_chemicalbarreldrum.avif` |
| Poseidon Arm Wrestle Machine | `SCORE_S21_ArmWrestleMachine_Poseidon` | `/wp-content/uploads/season_images/season-21/score_s21_camp_utility_armwrestlemachine_poseidon.avif` |
| Phoropter | `SCORE_S24_Utility_PlayerBuff_Phoropter` | `/wp-content/uploads/season_images/season-24/score_s24_camp_utility_phoropter.avif` |
| Black Piano | `SCORE_MiniSeason_2026_SockHop_Instrument_BlackPiano` | `/wp-content/uploads/guide-images/mini-seasons/summer-sock-hop/score_miniseason_2026_sockhop_camp_furniture_instrument_blackpiano.avif` |
| Hot Shot Motorcycle | `SCORE_MiniSeason_2026_SockHop_Furniture_Lonecycle` | `/wp-content/uploads/guide-images/mini-seasons/summer-sock-hop/score_miniseason_2026_sockhop_camp_floordecor_lonecycle.avif` |
| Cosmic Capture | `Invaders_AlienWhackAMole` | `/wp-content/uploads/guide-images/seasonal-events/invaders-from-beyond/reward-images/cosmic-capture.avif` |
| Rip Daring Pinball Machine | `SCORE_MiniSeason_2026_Furniture_RipDaringPinballMachine` | `/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_camp_furniture_ripdaringpinballmachine_l.avif` |

## Notes

- **Every URL above is `.avif`.** No `.webp` is being recommended. The `.webp` paths that show up inside `dist/*.json` are legacy stems that `df-bnb-seasons.js` rewrites at render time (`/season_images/score_sN_x.webp` -> `/season_images/season-N/x.avif`); the files on the server are all AVIF. Those stale `.webp` stems in the JSONs are worth cleaning up separately.
- **`/wp-content/uploads/storefront/` is a dead path** - confirmed empty on the server. Same for `/wp-content/uploads/fo76/storefront/`. Both appear in the JSONs; neither resolves. Nothing above points at them.
- **The server is case-sensitive.** `request-item-images` filenames must keep their exact mixed case (e.g. `ATX_Camp_FloorDecor_HotTub.avif`, not lowercase).
- `Pipe Organ` (base game) and `Pipe Organ (Atom Shop)` share one file; same for `Hollywood Vanity` / `Hollywood Vanity (Free)`.
- The renderer sets `img.src = item.imageUrl` verbatim (df-bnb-camp-items.js:1623) - no prefixing, no folder assumption. **An override can point at any path under wp-content**, which is what makes all of the above reusable.

### The four season items - not corrupt, just never uploaded

Corrected from the first pass. The season folders are populated; these are individual files missing from them, and the season scoreboard pages show broken images in the same spots:

| Season | Folder state | Missing file for the buff station |
|---|---|---|
| 3 | 35 of 46 tiles load - 11 missing | `score_s3_camp_floordecor_exerciseequipment_antiquespeedbag.avif` (Antique Speed Bag) |
| 10 | 65 of 69 load - 4 missing | `score_s10_camp_utility_radiation_glove_box.avif` (Radiation Glove Box) |
| 15 | 78 of 80 load - 2 missing | `score_s15_camp_furniture_table_special_9balltable_b_blue.avif` (Blue 9 Ball Table) |
| 25 | 61 of 61 load - folder complete | no tile exists - the Camden Park Claw Machine isn't a scoreboard reward, so there was never a season file to reuse |

Uploading the season-3, -10 and -15 files fixes the buff-stations page **and** the corresponding scoreboard page in one go.

## Still need art (62 items)

| Item | EDID | Expected filename |
|---|---|---|
| Steel Guitar | `Instrument_SteelGuitar` | `instrument_steelguitar.avif` |
| Banjo | `Instrument_Banjo` | `instrument_banjo.avif` |
| Acoustic Guitar | `Instrument_AcousticGuitar` | `instrument_acousticguitar.avif` |
| Snare Drum | `Instrument_SnareDrum` | `instrument_snaredrum.avif` |
| Grand Piano | `Instrument_Grand_Piano` | `instrument_grand_piano.avif` |
| Upright Piano | `Instrument_Upright_Piano` | `instrument_upright_piano.avif` |
| Chemical Barrel | `Instrument_ChemicalBarrel` | `instrument_chemicalbarrel.avif` |
| Metal Barrel | `Instrument_MetalBarrel` | `instrument_metalbarrel.avif` |
| Tuba | `Instrument_Tuba` | `instrument_tuba.avif` |
| Bass | `Instrument_Bass` | `instrument_bass.avif` |
| Frame Drum | `Instrument_FrameDrum` | `instrument_framedrum.avif` |
| Mouth Harp | `Instrument_MouthHarp` | `instrument_mouthharp.avif` |
| Antique Speed Bag | `SCORE_S3_Antique_Speed_Bag` | `atx_camp_floordecor_exerciseequipment_antiquespeedbag.avif` |
| Sympto-matic | `Workshop_Symptomatic` | `workshop_symptomatic.avif` |
| Drum Set | `ATX_Instrument_DrumSet` | `atx_camp_furniture_instrument_drumset.avif` |
| Skull Drum Set | `ATX_Instrument_SkullDrumSet` | `atx_camp_furniture_instrument_drumset_skull.avif` |
| Resonator Guitar | `ATX_Instrument_ResonatorGuitar` | `atx_camp_furniture_instrument_resonatorguitar.avif` |
| Nuka-lele | `Instrument_Nukelele` | `instrument_nukelele.avif` |
| Red 9 Ball Table | `ATX_F1_9BallTable_Red` | `atx_f1_camp_furniture_table_special_9balltable_red.avif` |
| Wood 9 Ball Table | `ATX_9BallTable_D_WoodGreen` | `atx_camp_furniture_table_special_9balltable_d_woodgreen.avif` |
| Blue 9 Ball Table | `SCORE_S15_9BallTable_B_Blue` | `atx_camp_furniture_table_special_9balltable_b_blue.avif` |
| Green 9 Ball Table | `ATX_9BallTable_C_Green` | `atx_camp_furniture_table_special_9balltable_c_green.avif` |
| Radiation Glove Box | `ATX_RadiationGloveBox` | `score_s10_camp_utility_radiation_glove_box.avif` |
| Hambone Stool | `ATX_Instrument_Hambone` | `atx_camp_furniture_instrument_hambonechair.avif` |
| Nuka-lele (Quantum) | `Instrument_Nukelele_Quantum` | `instrument_nukelele_quantum.avif` |
| Resonator Guitar B | `ATX_Instrument_ResonatorGuitar_B` | `atx_camp_furniture_instrument_resonatorguitar.avif` |
| Vault Girl Arm Wrestle Machine | `ATX_ArmWrestleMachine_VaultGirl` | `atx_camp_utility_armwrestlemachine_vaultgirl.avif` |
| Scarberry’s Shrine | `SCORE_S12_CAMP_Scarberry_Shrine_FURN` | `score_s12_camp_scarberry_shrine_furn.avif` |
| Skulls Fire Pitt | `ATX_Furniture_SkullsFirePitt` | `atx_entm_camp_furniture_skullsfirepitt.avif` |
| Bowling Alley Lane | `ATX_Structure_Furniture_BowlingAlleyLane` | `atx_structure_furniture_bowlingalleylane.avif` |
| Bowling Arcade Machine | `ATX_Furniture_BowlingArcadeMachine` | `atx_entm_camp_furniture_bowlingarcademachine.avif` |
| Shoeshine Machine | `ATX_Furniture_ShoeshineMachine` | `atx_entm_camp_furniture_shoeshinemachine.avif` |
| American Bowling Alley Lane | `ATX_Structure_Furniture_BowlingAlleyLane_Americana` | `atx_tp_structure_furniture_bowlingalleylane_americana.avif` |
| Boardwalk Bonanza Pinball Machine | `ATX_Furniture_BoardwalkBonanzaPinballMachine` | `atx_entm_camp_furniture_boardwalkbonanzapinballmachine.avif` |
| Stars and Strikes Bowling Arcade Machine | `ATX_Furniture_BowlingArcadeMachine_StarsAndStrikes` | `atx_camp_furniture_bowlingarcademachine_starsandstrikes.avif` |
| Rolling Stars Bowling Arcade Machine | `ATX_Furniture_BowlingArcadeMachine_RollingStars` | `atx_camp_furniture_bowlingarcademachine_rollingstars.avif` |
| Xylophone | `ATX_Instrument_Xylophone` | `atx_camp_furniture_instrument_xylophone.avif` |
| Sacred Mothman Tome | `E07A_Furniture_SacredMothmanTome` | `e07a_furniture_sacredmothmantome.avif` |
| Stargazer's Telescope | `MILE_StargazerTelescope` | `mile_stargazertelescope.avif` |
| Atomic Roller Machine | `ATX_Furniture_Atomicroller` | `atx_camp_furniture_atomicroller.avif` |
| Astro Attack Pinball Machine | `ATX_Furniture_GameCabinet_AstroAttack` | `atx_camp_furniture_gamecabinet_astroattack.avif` |
| Accordion | `ATX_Furniture_Instrument_Accordion` | `atx_entm_camp_furniture_instrument_accordion.avif` |
| Rusty Saxophone | `MILE_Workshop_Instrument_Saxophone` | `mile_workshop_instrument_saxophone.avif` |
| Cauldron Hot Tub | `MN2_Workshop_Furniture_CauldronHotTub` | `mn2_workshop_furniture_cauldronhottub.avif` |
| Accordion (Free) | `Furniture_Instrument_Accordion_FROMATX` | `atx_entm_camp_furniture_instrument_accordion.avif` |
| Radioactive Barrel Drum | `SCORE_S20_Instrument_MetalBarrelDrum_Radioactive` | `score_s20_instrument_metalbarreldrum_radioactive.avif` |
| Chemical Barrel Drum (Blue) | `SCORE_S20_Instrument_ChemicalBarrelDrum_Blue` | `score_s20_instrument_chemicalbarreldrum_blue.avif` |
| Blood Transfusion Pump | `ATX_FloorDecor_BloodTransfusionPump` | `atx_entm_camp_floordecor_bloodtransfusionpump.avif` |
| Five Finger Filet Table | `ATX_FloorDecor_FiveFingerFiletTable` | `atx_entm_camp_floordecor_fivefingerfilettable.avif` |
| Motorcycle | `ATX_Furniture_Motorcycle` | `atx_camp_floordecor_motorcycle.avif` |
| Summoning Circle | `ATX_Furniture_SummoningCircle` | `atx_entm_camp_floordecor_summoningcircle.avif` |
| Vault-Tec Pinball Machine | `ATX_Furniture_VaultTecPinballMachine` | `atx_camp_furniture_vaulttecpinballmachine.avif` |
| Raider Speed Bag | `ATX_CAMP_FloorDecor_ExerciseEquipment_Speed_Bag_RaiderSpeedBag` | `atx_camp_floordecor_exerciseequipment_raiderspeedbag.avif` |
| Rustic 9 Ball Table | `ATX_9BallTable_Rustic_HandmadePoolTable` | `atx_camp_furniture_table_special_9balltable_rustic_handmadepooltable.avif` |
| Lethal Loveseat | `SCORE_MiniSeason_LoveHurts_Furniture_BuffFurniture_LethalSeat` | `score_miniseason_lovehurts_camp_furniture_chair_lethalseat.avif` |
| Camden Park Claw Machine | `Score_S25_Furniture_CamdenClawMachine` | `score_s25_camp_furniture_clawmachine.avif` |
| Rip Daring Vault Boy Statue | `SCORE_MiniSeason_2026_WeaponsExpert_RipBoyStatue` | `score_miniseason_2026_weaponsexpert_camp_furniture_ripboystatue.avif` |
| Sharpening Stone | `ATX_Utility_Sharpening` | `atx_entm_camp_utility_sharpening.avif` |
| Atomic Hair Dryer | `ATX_Structure_Furniture_AtomicDryer` | `atx__structure_furniture_atomicdryer.avif` |
| Sleeping Bags | `BedTypeSleepingBag` | `sleeping_bags.avif` |
| Mattresses | `BedTypeMattress` | `mattresses.avif` |
| Comfy Beds | `BedTypeComfy` | `comfy_beds.avif` |
