#!/usr/bin/env python3
"""
src/build_atom_shop_json.py
============================
Reads src/atom_shop.json + the newest tsv/ENTM_Export_*.tsv,
fixes image URLs, adds DESC descriptions, validates,
and writes dist/atom_shop.json.

Also writes static LTB (Limited Time Bundles) data as data["ltb"]
in the same output file. LTB bundles are real-money platform DLC
(Steam/PS/Xbox), not Atom Shop items, so they are hardcoded here
rather than sourced from the ENTM export.

Categories (matching the JS front-end):
  Apparel - Headwear, Apparel - Outfits,
  Bundles,
  CAMP - Beds, CAMP - Camp Sets, CAMP - Displays & Weapon Racks, CAMP - Doors,
  CAMP - Floors, CAMP - Garden & Fences, CAMP - Kiddie Rides,
  CAMP - Lamps & Lights, CAMP - Resource Generator, CAMP - Shelters,
  CAMP - Skins, CAMP - Stash Boxes, CAMP - Vending Machines,
  Emotes, Fridges, Nuka Cola, Photomode, Player, Player Icons, Plushies,
  Pre-Fabs and Structures,
  Skins - Armour, Skins - Backpack & Lootbags, Skins - Pip Boy,
  Skins - Power Armour, Skins - Weapons,
  Wallpaper, Other
"""

import csv
import glob
import json
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC      = os.path.join(SCRIPT_DIR, "..", "dist", "atom_shop.json")
DIST     = os.path.join(SCRIPT_DIR, "..", "dist", "atom_shop.json")
TSV_ROOT = os.path.join(SCRIPT_DIR, "..", "tsv")

IMAGE_BASE_URL = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/atom-shop/request-item-images/"
LTB_IMAGE_BASE_URL = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/atom-shop/bundle-images/"

OLD_IMAGE_BASES = [
    "https://www.buffsnbrew.com/wp-content/uploads/fo76/storefront/bundles/",
    "https://www.buffsnbrew.com/wp-content/uploads/fo76/storefront/",
    "/wp-content/uploads/fo76/storefront/bundles/",
    "/wp-content/uploads/fo76/storefront/",
]

# ── Limited Time Bundles (real-money platform DLC) ───────────────────
# Ordered newest-first. status: "active" | "replaced" | "discontinued" | "removed"
# imageUrl fields use filenames only; LTB_IMAGE_BASE_URL is prepended by the JS.
LTB_BUNDLES = [
    {
        "id":        "mojave-bundle",
        "name":      "Mojave Bundle",
        "released":  "2026-01-29",
        "update":    "Burning Springs / Fallout TV Season 2",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Ranger Power Armour Paint",      "desc": "Don the iconic look of the NCR's elite with this rugged, battle-worn Power Armour paint.",      "formId": "00862D94", "edid": "ATX_ENTM_Skin_PowerArmor_Model_65PA",                   "imageUrl": "ATX_Skin_PowerArmor_Model_65PA.avif"},
            {"name": "NCR Flag",                       "desc": "Proudly display the NCR Flag at your C.A.M.P.",                                                 "formId": "0086C9D3", "edid": "ATX_ENTM_CAMP_Decoration_FlagWaving_NCR",               "imageUrl": "ATX_CAMP_Decoration_FlagWaving_NCR.avif"},
            {"name": "New Vegas Neon Sign",            "desc": "Add a touch of vintage Vegas glamour to your settlement.",                                       "formId": "00872E7B", "edid": "ATX_ENTM_CAMP_Lights_NeonSign_NewVegasSign_NV",        "imageUrl": "ATX_CAMP_Lights_NeonSign_NewVegasSign.avif"},
            {"name": "Ad Victoriam (Super Sledge)",    "desc": "A 4★ Legendary Super Sledge — the first 4★ weapon sold via the platform store.",         "formId": "00872E7A", "edid": "ATX_ENTM_Skin_WeaponModel_SuperSledge_AdVictoriam",    "imageUrl": "ATX_Skin_WeaponModel_SuperSledge_AdVictoriam.avif"},
            {"name": "Legion Legate Outfit",           "desc": "Embrace the ruthless efficiency of Caesar's Legion with this imposing outfit.",                  "formId": "00872E7C", "edid": "ATX_ENTM_Apparel_Outfit_LegionLegate",                 "imageUrl": "ATX_Apparel_Outfit_LegionLegate.avif"},
            {"name": 'Player Title: "Ad Victoriam"',  "desc": "Player Title Prefix.",                                                                           "formId": "00890E5D", "edid": "ATX_ENTM_PlayerTitles_Prefix_AdVictoriam",             "imageUrl": "ATX_PlayerTitles_Prefix_AdVictoriam.avif"},
            {"name": 'Player Title: "Tribune"',        "desc": "Player Title Prefix and Suffix.",                                                                "formId": "00872E7F", "edid": "ATX_ENTM_PlayerTitles_Prefix_Suffix_Tribune",          "imageUrl": "ATX_PlayerTitles_Prefix_Suffix_Tribune.avif"},
        ],
    },
    {
        "id":        "atomic-angler-bundle",
        "name":      "Atomic Angler Bundle",
        "released":  "2025-06-03",
        "update":    None,
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Hydro Tech Exo Power Armour",  "desc": "Deep water power armour — or an interesting way to catch fish by hand.",                         "formId": "007D6AD0", "edid": "ATX_ENTM_Skin_PowerArmor_Model_HydroTechExo",          "imageUrl": "ATX_Skin_PowerArmor_Model_HydroTechExo.avif"},
            {"name": "Appalachian Contessa Prefab",  "desc": "Place the Appalachian Contessa in your C.A.M.P. and be the captain of your own ship.",           "formId": "007D6ACC", "edid": "ATX_ENTM_CAMP_Structure_AppalachianContessa",           "imageUrl": "ATX_ENTM_CAMP_Structure_AppalachianContessa.avif"},
            {"name": "Live Bait Barrel",             "desc": "A resource collector. Have a fresh selection of fishing bait available in your C.A.M.P.",        "formId": "007D6ACB", "edid": "ATX_ENTM_CAMP_Collector_LiveBaitBarrel",               "imageUrl": "ATX_CAMP_Collector_LiveBaitBarrel.avif"},
            {"name": "Floating Buoy Set",            "desc": "For those who like to float things around, even in radiated water.",                              "formId": "007D6ACF", "edid": "ATX_ENTM_CAMP_FloorDecor_FishingBuoy",                "imageUrl": "ATX_CAMP_FloorDecor_FishingBuoy.avif"},
            {"name": "Charmcaster Fishing Rod",      "desc": "For those who said fishing is a boring hobby.",                                                   "formId": "00808283", "edid": "ATX_ENTM_Skin_WeaponSkin_FishingRod_Charmcaster",     "imageUrl": "ATX_Skin_WeaponSkin_FishingRod_Charmcaster.avif"},
            {"name": 'Player Title: "Contessa"',    "desc": "Player Title Prefix.",                                                                            "formId": "00816188", "edid": "ATX_ENTM_PlayerTitles_Prefix_Contessa",               "imageUrl": "ATX_PlayerTitles_Prefix_Contessa.avif"},
            {"name": 'Player Title: "Buoy"',        "desc": "Player Title Suffix.",                                                                            "formId": "00816189", "edid": "ATX_ENTM_PlayerTitles_Suffix_Buoy",                  "imageUrl": "ATX_PlayerTitles_Suffix_Buoy.avif"},
        ],
    },
    {
        "id":        "enclave-armory-bundle",
        "name":      "Enclave Armory Bundle",
        "released":  "2024-12-03",
        "update":    "Gleaming Depths",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Enclave Lab Shelter",                   "desc": "What secrets were they hiding in here? Decide for yourself.",                                         "formId": "00799F98", "edid": "Shelters_ENTM_ShelterEntrance_GleamingDepths",              "imageUrl": "Shelters_ShelterEntrance_GleamingDepths.avif"},
            {"name": "Enclave Technician Outfit",             "desc": "Gear up and get ready.",                                                                              "formId": "00799FE4", "edid": "ATX_ENTM_Apparel_Outfit_EnclaveTechnician",                 "imageUrl": "ATX_Apparel_Outfit_RaidRunner.avif"},
            {"name": "Enclave Technician Helmet",             "desc": "Patriotism is in the air, and that's all you'll be breathing in with this helmet.",                    "formId": "007AD3D5", "edid": "ATX_ENTM_Apparel_Headwear_EnclaveTechnician",              "imageUrl": "ATX_Apparel_Headwear_EnclaveTechnician.avif"},
            {"name": "Vertiguard Enclave Power Armour Paint", "desc": "Continue the Enclave's work in the Gleaming Depths with the Vertiguard Enclave Power Armour Paint.",  "formId": "00799C3A", "edid": "ATX_ENTM_Skin_PowerArmor_Model_Vertiguard_Enclave",        "imageUrl": "ATX_Skin_PowerArmor_Model_Vertiguard_Enclave.avif"},
            {"name": "Vertiguard Enclave Jetpack",            "desc": "Fly into the Gleaming Depths!",                                                                      "formId": "00799C77", "edid": "ATX_ENTM_Skin_PowerArmor_Jetpack_Vertiguard_Enclave",     "imageUrl": "ATX_Skin_PowerArmor_Jetpack_Vertiguard_Enclave.avif"},
            {"name": "Enclave Repair Bot",                    "desc": "Trust the Enclave to keep your C.A.M.P. at tip-top shape. Cannot be built inside a Shelter.",         "formId": "007AE546", "edid": "ATX_ENTM_CAMP_Utility_RepairBot_Enclave",                  "imageUrl": "ATX_CAMP_Utility_RepairBot_Enclave.avif"},
            {"name": 'Player Title: "Gleaming"',              "desc": "Player Title Prefix.",                                                                                "formId": "007A8CDC", "edid": "ATX_ENTM_PlayerTitles_Prefix_Gleaming",                   "imageUrl": "ATX_PlayerTitles_Prefix_Gleaming.avif"},
            {"name": 'Player Title: "Technician"',            "desc": "Player Title Suffix.",                                                                                "formId": "007A8CDD", "edid": "ATX_ENTM_PlayerTitles_Suffix_Technician",                 "imageUrl": "ATX_PlayerTitles_Suffix_Technician.avif"},
        ],
    },
    {
        "id":        "skyline-valley-lost-treasures-bundle",
        "name":      "Skyline Valley — Lost Treasures Bundle",
        "released":  "2024-06-12",
        "update":    "Skyline Valley",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Weather Control Station (Skyline Valley)", "desc": "Give your C.A.M.P. an electrical red glow!",                                    "formId": "007586AC", "edid": "ATX_ENTM_CAMP_Utility_WeatherStation_Storm_SkylineValley",  "imageUrl": "ATX_CAMP_Utility_WeatherStation_Storm_SkylineValley.avif"},
            {"name": "V63 Power Armour Paint",                   "desc": "Become a Vault 63 champion with this shiny new Power Armour skin.",             "formId": "0077E754", "edid": "ATX_ENTM_Skin_PowerArmor_Model_V63",                       "imageUrl": "ATX_Skin_PowerArmor_Model_StormVaultPA_L.avif"},
            {"name": "V63 Jetpack Paint",                        "desc": "Matching jetpack skin included with the V63 Power Armour Paint.",               "formId": "0077E754", "edid": "ATX_ENTM_Skin_PowerArmor_Model_V63",                       "imageUrl": "ATX_Skin_PowerArmor_Model_StormVaultPA_C1.avif"},
            {"name": "Vault 63 Door Display",                    "desc": "Bring a piece of Vault 63 to your C.A.M.P.",                                    "formId": "00787127", "edid": "ATX_ENTM_CAMP_Decoration_Vault63",                         "imageUrl": "ATX_CAMP_Decoration_Vault63.avif"},
            {"name": "Moe the Mole Plushie",                     "desc": "Moe the Mole really digs safety.",                                              "formId": "00787128", "edid": "ATX_ENTM_CAMP_FloorDecor_Plushie_MoeTheMole",              "imageUrl": "ATX_CAMP_FloorDecor_Plushie_MoeTheMole.avif"},
            {"name": "V63 Chassis Display Frame",                "desc": "Showcase your Power Armour in Vault 63 style!",                                 "formId": "0075C57E", "edid": "ATX_ENTM_CAMP_Display_ChassisDisplayFrame_V63",            "imageUrl": "ATX_CAMP_Display_ChassisDisplayFrame_V63.avif"},
            {"name": "Lightning Rod Pose",                       "desc": "Harness the power of lightning with this pose.",                                 "formId": "00766AA7", "edid": "ATX_ENTM_Photomode_Pose_LightningRod",                    "imageUrl": "ATX_Photomode_Pose_LightningRod.avif"},
        ],
    },
    {
        "id":        "atlantic-city-high-stakes-bundle",
        "name":      "Atlantic City High Stakes Bundle",
        "released":  "2023-12-05",
        "update":    "Atlantic City — Boardwalk Paradise",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Casino Quarter C.A.M.P. Kit",    "desc": "Build your very own Atlantic City.",                                              "formId": "006F0CD7", "edid": "ATX_Upgrade2023_AC_ENTM_CAMP_Kit_CasinoQuarter",                       "imageUrl": "ATX_Upgrade2023_AC_CAMP_Kit_CasinoQuarter.avif"},
            {"name": "Civic Duty Power Armour Paint",  "desc": "The Munis keep the city running. Fulfil your Civic Duty with this Power Armour!", "formId": "006F5748", "edid": "ATX_Upgrade2023_AC_ENTM_Skin_PowerArmor_Model_CivicDuty",             "imageUrl": "ATX_Upgrade2023_AC_Skin_PowerArmor_Model_CivicDuty.avif"},
            {"name": "Honey Pot 'o Gold Slot Machine", "desc": "With a little bit of sweet luck you'll be over the rainbow.",                     "formId": "006F5197", "edid": "ATX_Upgrade2023_AC_ENTM_Utility_SlotMachine_HoneyPotOGold",           "imageUrl": "ATX_Upgrade2023_AC_Utility_SlotMachine_HoneyPotOGold.avif"},
            {"name": "Aquarium of the Atlantic Door",  "desc": "What treasures await you in the deep seas behind this door?",                     "formId": "006F0CD6", "edid": "ATX_Upgrade2023_AC_ENTM_CAMP_Door_Secret_AquariumoftheAtlantic",      "imageUrl": "ATX_Upgrade2023_AC_CAMP_Door_Secret_AquariumoftheAtlantic.avif"},
            {"name": "Large Overgrown Plushie",        "desc": "The Overgrown will take over your bedroom with this Large Plushie!",              "formId": "006F0D57", "edid": "ATX_Upgrade2023_AC_ENTM_CAMP_FloorDecor_Plushie_LargeOvergrown",     "imageUrl": "ATX_Upgrade2023_AC_CAMP_FloorDecor_Plushie_LargeOvergrown.avif"},
            {"name": "Rig Roll-Up Backpack",           "desc": "Rig, Roll, and Pack it up with this all-purpose Backpack!",                       "formId": "006EFE64", "edid": "ATX_Upgrade2023_AC_ENTM_Skin_Backpack_RigRollUp",                    "imageUrl": "ATX_Upgrade2023_AC_Skin_Backpack_RigRollUp.avif"},
        ],
    },
    {
        "id":        "pitt-recruitment-bundle",
        "name":      "The Pitt Recruitment Bundle",
        "released":  "2022-09-13",
        "update":    "Expeditions: The Pitt",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Pittsburgh Neighbourhood C.A.M.P. Kit", "desc": "Fit right in with the friends from your expeditions with the Pittsburgh Neighbourhood C.A.M.P. Kit.", "formId": "00667A2E", "edid": "ATX_Upgrade2022_ENTM_CAMP_Kit_Pittsburgh_Neighborhood",    "imageUrl": "ATX_Upgrade2022_CAMP_Kit_Pittsburgh_Neighborhood.avif"},
            {"name": "Fanatic Paint (10mm SMG)",               "desc": "You don't want to be caught in The Pitt without the Fanatic Paint for the 10mm SMG.",                "formId": "00664E7B", "edid": "ATX_Upgrade2022_Pitt_ENTM_Skin_WeaponSkin_10mmSMG_Fanatic", "imageUrl": "ATX_Upgrade2022_Pitt_Skin_WeaponSkin_10mmSMG_Fanatic.avif"},
            {"name": "Fanatic Power Armour Paint",             "desc": "Be right at home in the factory with this Fanatic Power Armour Paint. Can be equipped to all Power Armours.", "formId": "0064FBB3", "edid": "ATX_Upgrade2022_Pitt_ENTM_Skin_PowerArmor_Paint_Fanatics", "imageUrl": "ATX_Upgrade2022_Pitt_Skin_PowerArmor_Paint_Fanatics.avif"},
            {"name": "Trog Plushie",                           "desc": "Even Trogs could use a snuggle! Bring this Trog Plushie home to your C.A.M.P.",                      "formId": "00664492", "edid": "ATX_Upgrade2022_Pitt_ENTM_CAMP_FloorDecor_Plushie_Trog",   "imageUrl": "ATX_CAMP_FloorDecor_Plushie_Trog.avif"},
            {"name": "Fusion Core Recharger",                  "desc": "Extend the life of your used Fusion Cores. Cannot be built inside a Shelter.",                        "formId": "00662D03", "edid": "ATX_Upgrade2022_Pitt_ENTM_CAMP_Utility_FusionCoreRecharger", "imageUrl": "ATX_Upgrade2022_Pitt_CAMP_Utility_FusionCoreRecharger.avif"},
        ],
    },
    {
        "id":        "mechanists-persona-bundle",
        "name":      "Mechanist's Persona Bundle",
        "released":  "2022-07-13",
        "update":    None,
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms", "desc": "In-game currency for the Atomic Shop.",           "formId": "", "edid": "", "imageUrl": ""},
            {"name": "Mechanist's Outfit",       "desc": "The Mechanist's signature outfit.",               "formId": "0054ED63", "edid": "ATX_ENTM_Apparel_Outfit_MechanistsOutfit_MP3",    "imageUrl": "ATX_Apparel_Outfit_MechanistOutfit_MP3.avif"},
            {"name": "Mechanist's Helmet",       "desc": "The Mechanist's iconic helmet.",                  "formId": "00559BBC", "edid": "ATX_ENTM_Apparel_Headwear_MechanistsOutfit_MP3",  "imageUrl": "ATX_Apparel_Headwear_MechanistOutfit_MP3.avif"},
            {"name": "6x Repair Kits",           "desc": "Standard Repair Kits.",                           "formId": "", "edid": "", "imageUrl": ""},
        ],
    },
    {
        "id":        "elders-persona-bundle",
        "name":      "Elder's Persona Bundle",
        "released":  "2022-06-13",
        "update":    None,
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms", "desc": "In-game currency for the Atomic Shop.",           "formId": "", "edid": "", "imageUrl": ""},
            {"name": "Elder's Battlecoat",       "desc": "The distinguished coat of a Brotherhood Elder.",  "formId": "00544670", "edid": "ATX_ENTM_Apparel_Outfit_ElderBattlecoat_MP1",     "imageUrl": "ATX_Apparel_Outfit_ElderBattlecoat_MP1.avif"},
            {"name": "6x Repair Kits",           "desc": "Standard Repair Kits.",                           "formId": "", "edid": "", "imageUrl": ""},
        ],
    },
    {
        "id":        "generals-persona-bundle",
        "name":      "General's Persona Bundle",
        "released":  "2022-05-12",
        "update":    None,
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms",          "desc": "In-game currency for the Atomic Shop.",              "formId": "", "edid": "", "imageUrl": ""},
            {"name": "Revolutionary General's Uniform",  "desc": "Suit up in the uniform of a Revolutionary General.", "formId": "0054ED62", "edid": "ATX_ENTM_Apparel_Outfit_GeneralsUniform_MP2",    "imageUrl": "ATX_Apparel_Outfit_GeneralsUniform_MP2.avif"},
            {"name": "3x Repair Kits",                   "desc": "Standard Repair Kits.",                              "formId": "", "edid": "", "imageUrl": ""},
            {"name": "3x Scrap Kits",                    "desc": "Standard Scrap Kits.",                               "formId": "", "edid": "", "imageUrl": ""},
        ],
    },
    {
        "id":        "pint-sized-slashers-persona-bundle",
        "name":      "Pint-Sized Slasher's Persona Bundle",
        "released":  "2022-04-12",
        "update":    None,
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms",    "desc": "In-game currency for the Atomic Shop.",            "formId": "", "edid": "", "imageUrl": ""},
            {"name": "Pint-Sized Slasher Costume", "desc": "The full Pint-Sized Slasher outfit.",              "formId": "0054ED64", "edid": "ATX_ENTM_Apparel_Outfit_PintsizedSlasher_MP4",     "imageUrl": "ATX_Apparel_Outfit_PintsizedSlasher_MP4.avif"},
            {"name": "Pint-Sized Slasher Knife",   "desc": "The Pint-Sized Slasher's signature knife.",        "formId": "00559BBE", "edid": "ATX_ENTM_WeaponModel_Machete_PintSizedSlasher_MP4", "imageUrl": "ATX_WeaponModel_Machete_PintSizedSlasher.avif"},
            {"name": "3x Repair Kits",             "desc": "Standard Repair Kits.",                             "formId": "", "edid": "", "imageUrl": ""},
            {"name": "3x Scrap Kits",              "desc": "Standard Scrap Kits.",                              "formId": "", "edid": "", "imageUrl": ""},
        ],
    },
    {
        "id":        "appalachia-starter-bundle",
        "name":      "Appalachia Starter Bundle",
        "released":  "2020-07-09",
        "update":    "Wastelanders",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "800 (+300 Bonus) Atoms",  "desc": "In-game currency for the Atomic Shop.",                                                       "formId": "",         "edid": "",                                            "imageUrl": ""},
            {"name": "Piper's Press Outfit",    "desc": "Dress as our favourite investigative reporter and uncover the dark secrets of Appalachia!",   "formId": "004FA0A1", "edid": "ATX_ENTM_Apparel_Outfit_Piper_SP1",           "imageUrl": "ATX_Apparel_Outfit_Piper.avif"},
            {"name": "Piper's Press Cap",       "desc": "Dress as our favourite investigative reporter and uncover the dark secrets of Appalachia!",   "formId": "004FA0DC", "edid": "ATX_ENTM_Apparel_Headwear_Piper_Cap_SP1",    "imageUrl": "ATX_Apparel_Headwear_Piper_Cap.avif"},
            {"name": "Piper's Road Goggles",    "desc": "Dress as our favourite investigative reporter and uncover the dark secrets of Appalachia!",   "formId": "004FA0DD", "edid": "ATX_ENTM_Apparel_Headwear_Piper_Goggles_SP1","imageUrl": "ATX_Apparel_Headwear_PiperGoggles.avif"},
            {"name": "5x Repair Kits",           "desc": "Standard Repair Kits.",                                                                       "formId": "",         "edid": "",                                            "imageUrl": ""},
            {"name": "5x Scrap Kits",            "desc": "Standard Scrap Kits.",                                                                        "formId": "",         "edid": "",                                            "imageUrl": ""},
        ],
    },
    {
        "id":        "brotherhood-recruitment-bundle",
        "name":      "Brotherhood Recruitment Bundle",
        "released":  "2020-12-01",
        "update":    "Steel Dawn",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Brotherhood of Steel Scouting Tower",      "desc": "Survey Appalachia for new threats and opportunities from this massive Scouting Tower.", "formId": "005EF0C7", "edid": "ATX_Upgrade2020_BoS_ENTM_CAMP_Structure_BoSScoutingTower",    "imageUrl": "ATX_Upgrade2018_BoS_CAMP_Structure_BoSScoutingTower.avif"},
            {"name": "Brotherhood of Steel Salute",              "desc": "Ad Victoriam, Brothers.",                                                               "formId": "005EDEEE", "edid": "ATX_Upgrade2020_BoS_ENTM_Emotes_BoSSalute",                  "imageUrl": ""},
            {"name": "Brotherhood of Steel Barricade",           "desc": "One more tool to protect against the new dangers of Appalachia.",                       "formId": "005EDF2F", "edid": "ATX_Upgrade2020_BoS_ENTM_CAMP_Defense_BoSBarricade",        "imageUrl": "ATX_Upgrade2020_BoS_CAMP_Defense_BoSBarricade.avif"},
            {"name": "Brotherhood Tactical Field Pack",          "desc": "Never be caught unprepared with the Tactical Field Pack.",                              "formId": "005EF0C6", "edid": "ATX_Upgrade2020_BoS_ENTM_Skin_Backpack_BosTacticalFieldPack", "imageUrl": "ATX_Upgrade2018_BoS_Skin_Backpack_BosTacticalFieldPack.avif"},
            {"name": "Brotherhood Barracks Locker",              "desc": "Keep your valuable finds safe and sound with the Barracks Locker.",                     "formId": "005EF0C3", "edid": "ATX_Upgrade2020_BoS_ENTM_CAMP_StashBox_BoSBarracksLocker",  "imageUrl": "ATX_Upgrade2018_BoS_CAMP_StashBox_BoSBarracksLocker.avif"},
            {"name": "Brotherhood Reclaimed Power Armour Paint", "desc": "Use what you find and reclaim Appalachia!",                                             "formId": "005EF0C5", "edid": "ATX_Upgrade2020_BoS_ENTM_Skin_PowerArmor_Paint_BosReclaimed", "imageUrl": "ATX_Upgrade2018_BoS_Skin_PowerArmor_Paint_BosReclaimed.avif"},
        ],
    },
]

DESC_STRIP_RE = re.compile(
    r"\s*-\s*(C\.A\.M\.P\. ITEMS APPEAR WHILE IN C\.A\.M\.P\. MODE|"
    r"C\.A\.M\.P\. PETS APPEAR ONCE THEIR FURNITURE IS PLACED IN C\.A\.M\.P\. MODE|"
    r"APPAREL IS CRAFTABLE AT ARMOR WORKBENCHES|"
    r"APPAREL IS CRAFTABLE AT THE ARMOR WORKBENCH|"
    r"WEAPON SKINS ARE CRAFTABLE AT WEAPONS WORKBENCHES|"
    r"POWER ARMOR PAINT JOBS ARE CRAFTABLE AT POWER ARMOR STATIONS|"
    r"CAMP ITEMS APPEAR WHILE IN CAMP MODE"
    r")[^-]*-?\s*$",
    re.IGNORECASE,
)

# ── Category constants ───────────────────────────────────────────────
CATEGORY_ORDER = [
    "Apparel - Headwear",
    "Apparel - Outfits",
    "Bundles",
    "CAMP - Beds",
    "CAMP - Buffs",
    "CAMP - Camp Sets",
    "CAMP - Chairs & Tables",
    "CAMP - Decorations",
    "CAMP - Displays & Weapon Racks",
    "CAMP - Doors",
    "CAMP - Floors",
    "CAMP - Garden & Fences",
    "CAMP - Kiddie Rides",
    "CAMP - Lamps & Lights",
    "CAMP - Letters",
    "CAMP - Resource Generator",
    "CAMP - Shelters",
    "CAMP - Skins",
    "CAMP - Stash Boxes",
    "CAMP - Statues",
    "CAMP - Vending Machines",
    "Emotes",
    "Fridges",
    "Nuka Cola",
    "Photomode",
    "Player",
    "Player Icons",
    "Plushies",
    "Pre-Fabs and Structures",
    "Skins - Armour",
    "Skins - Backpack & Lootbags",
    "Skins - Pip Boy",
    "Skins - Power Armour",
    "Skins - Weapons",
    "Skins - Workbenches",
    "Wallpaper",
    "Other",
]


def fix_display_name(name):
    """Normalise US → AU/UK spelling for public-facing display names only.
    EDIDs, image filenames, and formIds are never touched."""
    name = name.replace("Power Armor", "Power Armour")
    name = name.replace(" Armor Paint", " Armour Paint")
    name = name.replace(" Armor Skin", " Armour Skin")
    name = name.replace("Armored", "Armoured")
    name = name.replace("Armory", "Armoury")
    return name


def clean_desc(raw):
    s = raw.strip()
    for _ in range(5):
        new = DESC_STRIP_RE.sub("", s).strip()
        if new == s:
            break
        s = new
    s = re.sub(r"  +", " ", s)
    return s


def fix_image_url(url):
    if not url:
        return url
    if url.startswith(IMAGE_BASE_URL) and url.lower().endswith(".avif"):
        return url
    for old_base in OLD_IMAGE_BASES:
        if url.startswith(old_base):
            filename = url[len(old_base):].split("/")[-1]
            stem, _ = os.path.splitext(filename)
            return IMAGE_BASE_URL + stem + ".avif"
    bare = url.split("/")[-1]
    stem, _ = os.path.splitext(bare)
    return IMAGE_BASE_URL + stem + ".avif"


def fix_item_images(item):
    if item.get("imageUrl"):
        item["imageUrl"] = fix_image_url(item["imageUrl"])
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            if bi.get("imageUrl"):
                bi["imageUrl"] = fix_image_url(bi["imageUrl"])
    return item


def load_desc_lookup():
    """Load DESC and ETDI (image filename) from the newest ENTM export.
    Returns (desc_lookup, etdi_lookup) — both keyed by EDID (upper-cased)."""
    pattern = os.path.join(TSV_ROOT, "ENTM_Export_*.tsv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[atom_shop] WARNING: No ENTM_Export_*.tsv found in {TSV_ROOT}", file=sys.stderr)
        return {}, {}
    tsv_path = files[-1]
    print(f"[atom_shop] Reading DESC + ETDI from: {os.path.basename(tsv_path)}")
    desc_lookup = {}
    etdi_lookup = {}
    with open(tsv_path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = str(row.get("EDID") or "").strip().upper()
            desc = str(row.get("DESC") or "").strip()
            etdi = str(row.get("ETDI") or "").strip()
            if edid and desc:
                desc_lookup[edid] = clean_desc(desc)
            if edid and etdi:
                # ETDI is the DDS filename — strip .dds, add .avif
                img_name = re.sub(r'\.dds$', '.avif', etdi, flags=re.IGNORECASE)
                etdi_lookup[edid] = img_name
    print(f"[atom_shop] DESC entries loaded: {len(desc_lookup)}")
    print(f"[atom_shop] ETDI image entries loaded: {len(etdi_lookup)}")
    return desc_lookup, etdi_lookup


def apply_desc(item, lookup):
    edid = str(item.get("edid") or "").strip().upper()
    item["desc"] = lookup.get(edid, "")
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            bi_edid = str(bi.get("edid") or "").strip().upper()
            bi["desc"] = lookup.get(bi_edid, "")
    return item


def _find_image_in_etdi(edid, etdi_lookup):
    """Look up an EDID in the ETDI lookup.  First tries exact match, then
    falls back to partial matching for non-standard EDIDs (legacy items
    that don't follow the _ENTM_ naming convention)."""
    edid_upper = edid.upper()
    # Exact match
    img = etdi_lookup.get(edid_upper)
    if img:
        return img

    # Build a set of search tails from the EDID, progressively shorter.
    # e.g. ATX_Plushie_BalloonAnimalMrFuzzy_Misc
    #   → PLUSHIE_BALLOONANIMALMRFUZZY_MISC   (strip ATX_)
    #   → BALLOONANIMALMRFUZZY                 (last meaningful segment)
    tails = []
    stripped = edid_upper
    for prefix in ("ATX_", "SCORE_S1_", "SCORE_S2_", "SCORE_",
                   "DLC03WORKSHOPBARNKIT_", "DLC03_", "BABYLON_"):
        if stripped.startswith(prefix):
            stripped = stripped[len(prefix):]
            break
    tails.append(stripped)

    # Also try stripping category prefixes (Clothes_, Plushie_, mod_, F1_, etc.)
    for cat_prefix in ("CLOTHES_", "CLOTHES_UNSTOPPABLES_", "PLUSHIE_",
                       "MOD_MELEE_WARGLAIVE_", "MOD_GROGNAKAXE_WEAPON_MODELSWAP_",
                       "MOD_", "F1_", "ENTM_", "MAP"):
        if stripped.startswith(cat_prefix):
            inner = stripped[len(cat_prefix):]
            # Remove trailing _MISC
            if inner.endswith("_MISC"):
                inner = inner[:-5]
            tails.append(inner)

    # Remove trailing _MISC from full stripped too
    if stripped.endswith("_MISC"):
        tails.append(stripped[:-5])

    for tail in tails:
        if len(tail) < 4:
            continue
        tail_flat = tail.replace("_", "")
        for key, val in etdi_lookup.items():
            if tail in key or tail_flat in key.replace("_", ""):
                return val
    return None


def apply_image_from_entm(item, etdi_lookup):
    """For items missing imageUrl, try to derive it from the ENTM ETDI field."""
    if not item.get("imageUrl"):
        edid = str(item.get("edid") or "").strip()
        if edid:
            img_name = _find_image_in_etdi(edid, etdi_lookup)
            if img_name:
                item["imageUrl"] = IMAGE_BASE_URL + img_name.lower()
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            if not bi.get("imageUrl"):
                bi_edid = str(bi.get("edid") or "").strip()
                if bi_edid:
                    img_name = _find_image_in_etdi(bi_edid, etdi_lookup)
                    if img_name:
                        bi["imageUrl"] = IMAGE_BASE_URL + img_name.lower()
    return item


# ── Categorisation (mirrors the JS categoryFromEdid) ─────────────────

def _is_wallpaper_edid(edid):
    e = str(edid or "").upper()
    return "_CAMP_WALLPAPER_" in e or e.startswith("REUSE_ATX_ENTM_CAMP_WALLPAPER_")


def category_from_edid(edid, is_bundle, name, bundle_items):
    e = str(edid or "").upper()
    n = str(name or "").upper()

    # ── Nuka Cola — HARD RULE (overrides everything) ────
    if "NUKA-COLA" in n or "NUKA COLA" in n or "NUKACOLA" in e or "NUKA_COLA" in e:
        return "Nuka Cola"

    # ── Bundles ─────────────────────────────────────────
    if is_bundle:
        if isinstance(bundle_items, list) and len(bundle_items) > 0:
            if all(_is_wallpaper_edid(bi.get("edid", "")) for bi in bundle_items):
                return "Wallpaper"
        if " SET" not in n:
            return "Bundles"

    # ── Name-based overrides ────────────────────────────
    if "POWER ARMOUR" in n and ("PAINT" in n or "SKIN" in n or "PAINTS" in n):
        return "Skins - Power Armour"

    if "SHELTER" in n:
        return "CAMP - Shelters"

    for kw in ("SCOUTING TOWER", "SCOUT TOWER", "FIREWATCH TOWER",
               "MEGA MANSION", "SEEDY SHED", "OUTHOUSE"):
        if kw in n:
            return "Pre-Fabs and Structures"

    if "COLLECTRON" in n or "COFFEE MACHINE" in n:
        return "CAMP - Resource Generator"

    if "HAUNTED HOUSE STAIRCASE" in n:
        return "CAMP - Camp Sets"

    if " BED" in n or "WATERBED" in n:
        return "CAMP - Beds"

    if "WEAPON RACK" in n:
        return "CAMP - Displays & Weapon Racks"

    if n.endswith(" SIGN") or "NEON SIGN" in n or "BAR SIGN" in n or "SIGNS" in n or "POWER CONNECTOR" in n or "CEILING FAN" in n or "DECK FAN" in n:
        return "CAMP - Lamps & Lights"

    for kw in ("FENCE", "FENCES", "PLANT", "CACTUS", "FLOWER", "FLOWERS", "BRAMBLES",
               "TREE", "TREES", "TIRE", "TYRE", "SUCCULENT", "BARRICADE",
               "RAIN WATER COLLECTOR", "HAY BALE", "WORM FARM"):
        if kw in n:
            return "CAMP - Garden & Fences"

    if "TURRET" in n:
        return "CAMP - Skins"

    # ── Buffs ──────────────────────────────────────────
    for kw in ("HOT TUB", "FIREPIT", "FIRE PIT"):
        if kw in n:
            return "CAMP - Buffs"

    # ── Chairs & Tables ────────────────────────────────
    for kw in ("CHAIR", "TABLE", "SOFA", "BENCH", "STOOL", "DECK CHAIR"):
        if kw in n:
            return "CAMP - Chairs & Tables"

    # ── Letters ────────────────────────────────────────
    if "LETTER" in n and ("KIT" in n or "NEON" in n or "GIANT" in n or "LETTERS" in n):
        return "CAMP - Letters"

    # ── Statues ────────────────────────────────────────
    if "STATUE" in n or "TOTEM" in n:
        return "CAMP - Statues"

    # ── Workbench skins ────────────────────────────────
    if "WORKBENCH" in n or ("STATION" in n and "POWER ARMOUR" not in n):
        return "Skins - Workbenches"

    if "REFRIGERATOR" in n or "FRIDGE" in n:
        return "Fridges"
    if "PLUSHIE" in n:
        return "Plushies"
    if "KIDDIE RIDE" in n or n.endswith(" RIDE"):
        return "CAMP - Kiddie Rides"
    if "VENDING MACHINE" in n:
        return "CAMP - Vending Machines"
    if "STASH BOX" in n or "STASH" in n:
        return "CAMP - Stash Boxes"
    if "PLAYER ICON" in n or "ICON" in n:
        return "Player Icons"
    if "WALLPAPER" in n:
        return "Wallpaper"
    for kw in ("FACE PAINT", "TATTOO", "HAIRSTYLE", "SPIKEHAWK", "MEGATON HAIRSTYLE"):
        if kw in n:
            return "Player"
    for kw in ("EMOTE", "SALUTE", "BATTLECRY", "WOLF HOWL", "SUPER ANGRY",
               "NO THANK YOU", "NO WAY EMOTE", "WORSHIP EMOTE", "FIST SHAKE"):
        if kw in n:
            return "Emotes"
    if "PHOTOFRAME" in n or "PHOTO" in n:
        return "Photomode"

    if "LOOT BAG" in n:
        return "Skins - Backpack & Lootbags"

    if not e:
        return "Other"

    # ── EDID-based ──────────────────────────────────────
    if e.startswith("SHELTERS_"):
        return "CAMP - Shelters"

    if "_CAMP_STRUCTURE_" in e:
        return "Pre-Fabs and Structures"

    if "_SKIN_POWERARMOR_" in e or "_SKIN_POWRARMOR_" in e:
        return "Skins - Power Armour"

    if "_SKIN_BACKPACK_" in e or "_BACKPACK_SKIN_" in e or "_LOOTBAG_" in e:
        return "Skins - Backpack & Lootbags"

    if "_SKIN_PIPBOY_" in e or "_PIPBOY_SKIN_" in e or "_SKIN_PIPB_" in e:
        return "Skins - Pip Boy"

    if "_CAMP_WALLPAPER_" in e or e.startswith("REUSE_ATX_ENTM_CAMP_WALLPAPER_"):
        return "Wallpaper"

    if "_UTILITY_REFRIGERATOR_" in e or "_CAMP_FRIDGE_" in e:
        return "Fridges"

    if "_VENDINGMACHINE_" in e:
        return "CAMP - Vending Machines"

    if "_STASHBOX_" in e:
        return "CAMP - Stash Boxes"

    if "_KIDDIERIDE_" in e:
        return "CAMP - Kiddie Rides"

    if "_UTILITY_COLLECTRON_" in e or "_COLLECTOR_" in e or "_COFFEEMACHINE_" in e or "_SLOCUMSJOE_" in e:
        return "CAMP - Resource Generator"

    if "_CAMP_BED_" in e:
        return "CAMP - Beds"

    if "_MACHINERY_PURIFIER_" in e or "_MACHINERY_GENERATOR_" in e or "_TURRET_" in e:
        return "CAMP - Skins"

    if "_FLOORDECKOR_PLUSHIE_" in e or "_FLOORDECOR_PLUSHIE_" in e:
        return "Plushies"

    if any(k in e for k in ("_CAMP_LIGHT_", "_CAMP_LIGHTS_", "_CAMP_LAMP_", "_CAMP_SIGN_",
                             "_LIGHTING_", "_NEON_", "_POWERCONNECTORS_", "_CEILINGFAN_", "_DECKFAN")):
        return "CAMP - Lamps & Lights"

    if any(k in e for k in ("_CAMP_FLOOR_", "_FLOORING_", "_LAMINATE_", "_ASTROTURF_")):
        return "CAMP - Floors"

    if any(k in e for k in ("_CAMP_DOOR_", "_SECRETDOOR_", "_CURTAINDOOR_")):
        return "CAMP - Doors"

    if any(k in e for k in ("_CAMP_GARDEN_", "_PLANT_", "_PLANTER_", "_SUCCULENT_",
                             "_TOPIARY_", "_CACTUS_", "_BRAMBLES_", "_FENCE_", "_FENCES_",
                             "_WORMFARM_", "_TREE_", "_TIRE_", "_TYRE_", "_BARRICADE_",
                             "_RAINWATER_", "_HAYBALE_")):
        return "CAMP - Garden & Fences"

    if any(k in e for k in ("_HOTTUB_", "_FIREPIT_")):
        return "CAMP - Buffs"

    if any(k in e for k in ("_WEAPONRACK", "_GUNRACKS", "_DISPLAYCASE_", "_DISPLAYRACK_",
                             "_MANNEQUIN_", "_BOBBLEHEAD_")):
        return "CAMP - Displays & Weapon Racks"

    if any(k in e for k in ("_CAMP_KIT_", "_PORCHSET_", "_LOGCABIN_",
                             "_GREENHOUSE_", "_SCAFFOLDKIT_", "_MODULARSOFA_")):
        return "CAMP - Camp Sets"

    if any(k in e for k in ("_PLAYERSTYLE_FACEPAINT_", "_PLAYERSTYLE_TATTOO_",
                             "_PLAYERSTYLE_HAIRSTYLE_", "_PLAYERSTYLE_HAIR_")):
        return "Player"

    if "_PHOTOMODE_FRAME_" in e:
        return "Photomode"

    if "_PLAYERICON_" in e:
        return "Player Icons"

    if "_EMOTES_" in e:
        return "Emotes"

    if any(k in e for k in ("_CAMP_FURNITURE_CHAIR_", "_CAMP_FURNITURE_TABLE_",
                             "_CAMP_FURNITURE_SOFA_", "_CAMP_FURNITURE_BENCH_",
                             "_CAMP_FURNITURE_STOOL_")):
        return "CAMP - Chairs & Tables"

    if "_LETTER_" in e or "_LETTERS_" in e:
        return "CAMP - Letters"

    if "_STATUE_" in e or "_TOTEM_" in e:
        return "CAMP - Statues"

    if "_WORKBENCH_" in e or "_MACHINERY_WORKBENCH_" in e:
        return "Skins - Workbenches"

    if "_CAMP_" in e:
        return "Other"

    if "_APPAREL_" in e or e.startswith("ATX_CLOTHES_"):
        # Underarmor is armour, not apparel
        if "_UNDERARMOR_" in e:
            return "Skins - Armour"
        headwear_kws = ("_HAT_", "_HELMET_", "_MASK_", "_HEADWEAR_", "_BERET_",
                        "_FEZ_", "_BONNET_", "_MONOCLE_", "_GASMASK_", "_HEAD_")
        if any(k in e for k in headwear_kws):
            return "Apparel - Headwear"
        name_hw = ("MASK", "HAT", "HELMET", "HEADBAND", "BERET", "FEZ",
                   "BONNET", "MONOCLE", "GAG GLASSES", "EYE PATCH", "MASCOT HEAD")
        if any(k in n for k in name_hw):
            return "Apparel - Headwear"
        return "Apparel - Outfits"

    if "_SKIN_WEAPON" in e or "_SKIN_WEAP" in e or "_WEAPONSKIN_" in e:
        return "Skins - Weapons"

    if "_SKIN_ARMOR_" in e or "_SKIN_ARMOUR_" in e or "_ARMORSKIN_" in e:
        return "Skins - Armour"

    if "_SKIN_" in e or e.startswith("ATX_MOD_"):
        return "Skins - Weapons"

    if e.startswith("SCORE_"):
        return "Skins - Weapons"

    if e.startswith("ATX_PLUSHIE_"):
        return "Plushies"

    if e.startswith("DLC03WORKSHOP") or e.startswith("ATX_MAP"):
        return "Other"

    return "Other"


# ── Power Armour skin applicability ──────────────────────────
# Reads COBJ (crafting recipe) and ENTM per-type entries from the
# TSV exports to determine EXACTLY which PA chassis each skin
# applies to. No guessing from descriptions or EDID patterns.

# Display order for PA types in the "Applicable to:" string
PA_TYPE_DISPLAY_ORDER = [
    "Excavator", "Raider", "T-45", "T-51", "T-60",
    "T-65", "X-01", "Ultracite", "Hellcat", "Union", "Vulcan",
]

# COBJ/OMOD internal name → display name
_PA_TYPE_DISPLAY = {
    "Excavator": "Excavator", "Raider": "Raider",
    "T45": "T-45", "T51": "T-51", "T60": "T-60", "T65": "T-65",
    "X01": "X-01", "Ultracite": "Ultracite",
    "Hellcat": "Hellcat", "Union": "Union",
    "EnclaveVulcan": "Vulcan",
    # Variant names that appear in some COBJ/OMOD EDIDs
    "Ultracite_Set_V94": "Ultracite",
    "EXC": "Excavator", "ULT": "Ultracite",
    "T51Raider": "Raider",
}

# What "ALL" expands to (every PA chassis that exists in-game)
ALL_PA_TYPES_SET = set(_PA_TYPE_DISPLAY.values())
ALL_PA_TYPES = ", ".join(PA_TYPE_DISPLAY_ORDER)

# Internal PA types used in COBJ EDIDs
_COBJ_PA_TYPES = list(_PA_TYPE_DISPLAY.keys())

# Body-part tokens that appear in COBJ/OMOD EDIDs
_BODY_PARTS_RE = (
    r"(?:Helmet|Torso|ArmLeft|ArmRight|LegLeft|LegRight"
    r"|LeftArm|RightArm|LeftLeg|RightLeg)"
)

# Manual overrides — user-verified corrections that take precedence
# over COBJ data. Key = EDID (case-insensitive), value = display string.
_PA_OVERRIDES = {
    "ATX_ENTM_SKIN_POWERARMOR_PAINT_ENCLAVE": "Applicable to: X-01",
    "ATX_ENTM_SKIN_POWERARMOR_SKIN_SANTASUIT": "Applicable to: T-51",
}

# ENTM skin name → COBJ/OMOD skin name aliases.
# Handles cases where the ENTM EDID uses a different name than the
# COBJ/OMOD crafting entries.
_SKIN_NAME_ALIASES = {
    "BosReclaimed":             "BosCamo",
    "CamoWhite":                "Camo",                # Winter Camo → same as "Camo" paint
    "FreeStates_Revolutionary": "FreeStates02",
    "FreeStates_Survivalist":   "FreeStates01",
    "Camoblack":                "CamoBlack",           # case mismatch in ENTM EDID
    "RedScare":                 "Communist_Red",       # OMOD uses Communist_Red
    "RedShift":                 "Communist",           # OMOD uses Communist
    "WarRider":                 "Warrider",            # case mismatch in OMOD
    "SlocumsJoe":               "Babylon_SlocumsJoe",  # COBJ prefixes with Babylon_
    "Waster":                   "RaiderWaster",        # ENTM: Paint_Raider_Waster → extracts "Waster", COBJ uses "RaiderWaster"
    "Junkyard_Rusted":          "Junkyard_Rusted",     # explicit identity (model skin)
}

# ── Build the COBJ/OMOD skin-name → PA-types lookup at import time ──
_cobj_skin_types = None          # populated by _build_pa_lookup()
_omod_skin_types = None          # populated by _build_pa_lookup()
_entm_skin_families = None       # populated by _build_pa_lookup()
_cobj_edids_by_skin = None       # populated by _build_pa_lookup()  skin_name → sorted list of full COBJ EDIDs


def _build_pa_lookup():
    """Parse COBJ, OMOD, and ENTM TSVs to build skin_name → set-of-PA-types maps."""
    global _cobj_skin_types, _omod_skin_types, _entm_skin_families, _cobj_edids_by_skin
    from collections import defaultdict

    _cobj_skin_types = defaultdict(set)
    _omod_skin_types = defaultdict(set)
    _entm_skin_families = defaultdict(set)
    _cobj_edids_by_skin = defaultdict(list)

    def _parse_paint_lines(path, prefix, target_dict, collect_edids=False):
        """Parse COBJ or OMOD lines for PA paint entries."""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                cols = line.rstrip("\n").split("\t")
                if len(cols) < 2:
                    continue
                edid = cols[1].strip('"')
                if "PowerArmor" not in edid or "Paint" not in edid:
                    continue
                upper = edid.upper()
                if upper.startswith("ZZZ") or upper.startswith("DEL"):
                    continue
                if "Headlamp" in edid or "Lining" in edid or "Misc" in edid:
                    continue

                # Pattern 1: per-type
                # {prefix}_PowerArmor_{TYPE}_{Part}_Material_Paint_{SkinName}
                m = re.search(
                    rf"{prefix}_PowerArmor_(\w+?)_{_BODY_PARTS_RE}_Material_Paint_(\w+)",
                    edid,
                )
                if m:
                    pa_type, skin = m.group(1), m.group(2)
                    if pa_type != "ALL":
                        target_dict[skin].add(pa_type)
                    else:
                        target_dict[skin].add("ALL")
                    if collect_edids:
                        _cobj_edids_by_skin[skin].append(edid)
                    continue

                # Pattern 2: ALL layout (body part AFTER skin name)
                # {prefix}_PowerArmor_ALL_Material_Paint_{SkinName}_{Part}
                m = re.search(
                    rf"{prefix}_PowerArmor_ALL_Material_Paint_(\w+?)_{_BODY_PARTS_RE}$",
                    edid,
                )
                if m:
                    target_dict[m.group(1)].add("ALL")
                    if collect_edids:
                        _cobj_edids_by_skin[m.group(1)].append(edid)

    def _parse_model_lines(path):
        """Parse COBJ lines for 'Model' PA skins — whole-PA replacements that
        use patterns WITHOUT Material_Paint_.  Collects EDIDs only.
        Pattern A: {prefix}_PowerArmor_{SkinName}_{Part}  (skin applies to ALL)
        Pattern B: {prefix}_PowerArmor_{TYPE}_{SkinName}_{Part}  (type-specific model)"""
        _BODY_PART_SET = {
            "Helmet", "Torso", "ArmLeft", "ArmRight", "LegLeft", "LegRight",
            "LeftArm", "RightArm", "LeftLeg", "RightLeg",
        }
        _PA_TYPE_SET = set(_PA_TYPE_DISPLAY.keys())

        _MODEL_PARTS_RE = re.compile(
            r"(?:ATX_)?co(?:_(?:ATX_)?mod)?_PowerArmor_(\w+?)_"
            r"(?:Helmet|Torso|ArmLeft|ArmRight|LegLeft|LegRight"
            r"|LeftArm|RightArm|LeftLeg|RightLeg)$"
        )
        # Pattern B: {prefix}_PowerArmor_{TYPE}_{SkinName}_{Part}
        _MODEL_TYPED_RE = re.compile(
            r"co(?:_(?:ATX_)?mod)?_PowerArmor_("
            + "|".join(re.escape(t) for t in _PA_TYPE_SET)
            + r")_(\w+?)_(?:Helmet|Torso|ArmLeft|ArmRight|LegLeft|LegRight"
            r"|LeftArm|RightArm|LeftLeg|RightLeg)$"
        )
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                cols = line.rstrip("\n").split("\t")
                if len(cols) < 2:
                    continue
                edid = cols[1].strip('"')
                if "PowerArmor" not in edid:
                    continue
                upper = edid.upper()
                if upper.startswith("ZZZ") or upper.startswith("DEL") or upper.startswith("CUT"):
                    continue
                if "Headlamp" in edid or "Lining" in edid or "Misc" in edid:
                    continue
                # Skip entries already caught by _parse_paint_lines (Material_Paint)
                if "Material_Paint" in edid:
                    continue

                # Try Pattern B first (type-specific model): _PowerArmor_{TYPE}_{SkinName}_{Part}
                m = _MODEL_TYPED_RE.search(edid)
                if m:
                    skin = m.group(2)
                    _cobj_edids_by_skin[skin].append(edid)
                    continue

                # Pattern A: _PowerArmor_{SkinName}_{Part}
                m = _MODEL_PARTS_RE.search(edid)
                if m:
                    skin = m.group(1)
                    # Skip if skin is actually a PA type name (base recipe, not a skin)
                    if skin in _PA_TYPE_SET:
                        continue
                    _cobj_edids_by_skin[skin].append(edid)

    # ── COBJ: crafting recipes ──
    cobj_files = sorted(glob.glob(os.path.join(TSV_ROOT, "COBJ_Export_*.tsv")))
    if cobj_files:
        cobj_path = cobj_files[-1]
        print(f"[PA lookup] Reading {os.path.basename(cobj_path)}")
        _parse_paint_lines(cobj_path, "co_mod", _cobj_skin_types, collect_edids=True)
        # Some COBJs use alternate prefixes — parse those too
        _parse_paint_lines(cobj_path, "co_ATX_mod", _cobj_skin_types, collect_edids=True)
        # Also collect EDIDs for model-type skins (whole-PA replacements)
        _parse_model_lines(cobj_path)
        # Sort each skin's COBJ EDIDs for consistent output
        for skin in _cobj_edids_by_skin:
            _cobj_edids_by_skin[skin] = sorted(set(_cobj_edids_by_skin[skin]))
        print(f"[PA lookup] COBJ: {len(_cobj_skin_types)} skin mappings, "
              f"{sum(len(v) for v in _cobj_edids_by_skin.values())} total COBJ EDIDs")

    # ── OMOD: object modifications (covers skins without COBJ recipes) ──
    omod_files = sorted(glob.glob(os.path.join(TSV_ROOT, "OMOD_Export_*.tsv")))
    if omod_files:
        omod_path = omod_files[-1]
        print(f"[PA lookup] Reading {os.path.basename(omod_path)}")
        _parse_paint_lines(omod_path, "mod", _omod_skin_types)
        print(f"[PA lookup] OMOD: {len(_omod_skin_types)} skin mappings")

    # ── ENTM: per-type entries (e.g. Paint_T65_Water) ──
    entm_files = sorted(glob.glob(os.path.join(TSV_ROOT, "ENTM_Export_*.tsv")))
    if entm_files:
        entm_path = entm_files[-1]
        print(f"[PA lookup] Reading {os.path.basename(entm_path)}")
        with open(entm_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter="\t")
            try:
                next(reader)  # skip header
            except StopIteration:
                pass
            for row in reader:
                if len(row) < 2:
                    continue
                edid = row[1]
                if "Skin_PowerArmor" not in edid:
                    continue
                for pa_type in _COBJ_PA_TYPES:
                    marker = f"_Paint_{pa_type}_"
                    if marker in edid:
                        idx = edid.index(marker) + len(marker)
                        skin = edid[idx:]
                        _entm_skin_families[skin].add(pa_type)
                        break

        print(f"[PA lookup] ENTM families: {len(_entm_skin_families)} skin groups")


def _extract_skin_name(edid: str) -> str | None:
    """Pull the 'skin name' suffix out of an ENTM EDID so we can
    match it against the COBJ/ENTM lookup tables."""
    # _Paint_TX_{SkinName}
    m = re.search(r"_Paint_TX_(\w+)", edid)
    if m:
        return m.group(1)
    # _Model_{SkinName}  (may contain & etc. — grab up to non-word)
    m = re.search(r"_Model_(\w+)", edid)
    if m:
        return m.group(1)
    # _Skin_{SkinName}  (but not _Skin_PowerArmor)
    m = re.search(r"_Skin_(?!PowerArmor)(\w+)", edid)
    if m:
        return m.group(1)
    # Per-type: _Paint_{PA_TYPE}_{SkinName}
    for pa_type in _COBJ_PA_TYPES:
        m = re.search(rf"_Paint_{pa_type}_(\w+)", edid)
        if m:
            return m.group(1)
    # Generic paint: Skin_PowerArmor_Paint_{SkinName}
    m = re.search(r"Skin_PowerArmor_Paint_(\w+)", edid)
    if m:
        return m.group(1)
    return None


def _resolve_pa_types(skin_name: str) -> set | None:
    """Look up a skin name in COBJ → OMOD → ENTM families. Returns a set
    of display-name strings like {'T-45', 'T-51', ...}, or None."""
    if _cobj_skin_types is None:
        _build_pa_lookup()

    # Try the direct name, then check aliases
    names_to_try = [skin_name]
    alias = _SKIN_NAME_ALIASES.get(skin_name)
    if alias:
        names_to_try.append(alias)

    raw = None
    for name in names_to_try:
        raw = _cobj_skin_types.get(name)
        if raw:
            break
        raw = _omod_skin_types.get(name)
        if raw:
            break
        raw = _entm_skin_families.get(name)
        if raw:
            break

    if not raw:
        return None

    # Expand "ALL" / "MODEL" to the full set
    if "ALL" in raw or "MODEL" in raw:
        return set(ALL_PA_TYPES_SET)

    # Convert internal names to display names
    display = set()
    for t in raw:
        d = _PA_TYPE_DISPLAY.get(t)
        if d:
            display.add(d)
    return display if display else None


def _format_pa_types(types: set) -> str:
    """Format a set of display PA types into a sorted 'Applicable to:' string."""
    ordered = [t for t in PA_TYPE_DISPLAY_ORDER if t in types]
    return f"Applicable to: {', '.join(ordered)}"


def collect_cobj_edids(edid: str) -> list:
    """Return the sorted list of all COBJ EDIDs related to this PA skin ENTM EDID.
    Returns [] if not a PA skin or no COBJ data found."""
    if _cobj_edids_by_skin is None:
        _build_pa_lookup()

    skin_name = _extract_skin_name(edid)
    if not skin_name:
        return []

    # Try direct name first, then aliases
    names_to_try = [skin_name]
    alias = _SKIN_NAME_ALIASES.get(skin_name)
    if alias:
        names_to_try.append(alias)

    for name in names_to_try:
        edids = _cobj_edids_by_skin.get(name)
        if edids:
            return edids
    return []


def pa_applicability(edid: str, name: str, desc: str) -> str:
    """Return an 'Applicable to: ...' string for Power Armour skins, or ''."""
    e = edid.upper()

    # Check manual overrides first (always win)
    if e in _PA_OVERRIDES:
        return _PA_OVERRIDES[e]

    # Skip non-PA-skin items
    if "_SKIN_POWERARMOR_" not in e and "_SKIN_POWRARMOR_" not in e:
        n = name.upper()
        if not ("POWER ARMOUR" in n and ("PAINT" in n or "SKIN" in n or "PAINTS" in n)):
            return ""

    # Items that are NOT actual PA skins — skip
    for skip in ("_WORKBENCH_", "_STATUE_", "_CHASSIS_", "_JETPACK_", "_JETPACK"):
        if skip in e:
            return ""

    # Headlamp skins — these fit all PA types
    if "_HEADLAMP_" in e:
        return f"Applicable to: {ALL_PA_TYPES} (headlamp)"

    # Helmet-only items
    if "_HELMET_" in e:
        return "Applicable to: All Power Armour (helmet only)"

    # Look up from game data (COBJ + ENTM)
    skin_name = _extract_skin_name(edid)
    if skin_name:
        types = _resolve_pa_types(skin_name)
        if types:
            return _format_pa_types(types)

    # If game data lookup failed, fall back to ALL
    # (this should rarely happen once COBJ data is available)
    print(f"[PA lookup] WARNING: no COBJ/ENTM data for {edid} "
          f"(skin_name={skin_name}), defaulting to ALL")
    return f"Applicable to: {ALL_PA_TYPES}"


def main():
    try:
        with open(SRC, "r", encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        print(f"[atom_shop] Cannot read {SRC}", file=sys.stderr)
        sys.exit(1)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[atom_shop] JSON parse error in {SRC}: {e}", file=sys.stderr)
        sys.exit(1)

    items = data.get("items", [])
    if not isinstance(items, list) or not items:
        print("[atom_shop] No items found in src/atom_shop.json", file=sys.stderr)
        sys.exit(1)

    errors = 0
    for i, item in enumerate(items):
        if not item.get("name"):
            print(f"[atom_shop] Item {i} missing name", file=sys.stderr)
            errors += 1
    if errors:
        print(f"[atom_shop] {errors} validation error(s). Aborting.", file=sys.stderr)
        sys.exit(1)

    desc_lookup, etdi_lookup = load_desc_lookup()

    fixed_count = 0
    img_filled_count = 0
    cat_counts = {}
    fixed_items = []
    for item in items:
        original_url = item.get("imageUrl", "")
        fixed = fix_item_images(dict(item))
        fixed = apply_desc(fixed, desc_lookup)
        # Fill in missing imageUrl from ENTM ETDI data
        had_img_before = bool(fixed.get("imageUrl"))
        fixed = apply_image_from_entm(fixed, etdi_lookup)
        if not had_img_before and fixed.get("imageUrl"):
            img_filled_count += 1
        # AU/UK spelling for display names only (edid/imageUrl untouched)
        fixed["name"] = fix_display_name(fixed.get("name", ""))
        if fixed.get("bundleItems"):
            for bi in fixed["bundleItems"]:
                bi["name"] = fix_display_name(bi.get("name", ""))
        if fixed.get("imageUrl") != original_url:
            fixed_count += 1
        # Compute and store category
        cat = category_from_edid(
            fixed.get("edid", ""),
            bool(fixed.get("isBundle")),
            fixed.get("name", ""),
            fixed.get("bundleItems"),
        )
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        # Auto-populate technicalNotes + relatedEdids for Power Armour skins.
        # Always recalculate from game data (COBJ/OMOD/ENTM) on every build
        # so that updated TSV exports are reflected.
        # Try pa_applicability on EVERY item (not just "Skins - Power Armour"
        # category) because PA skins can appear in Bundles, Nuka Cola, etc.
        pa_note = pa_applicability(
            fixed.get("edid", ""),
            fixed.get("name", ""),
            fixed.get("desc", ""),
        )
        if pa_note:
            fixed["technicalNotes"] = pa_note
        # Collect related COBJ EDIDs for any item that has PA skin data
        cobj_edids = collect_cobj_edids(fixed.get("edid", ""))
        if cobj_edids:
            fixed["relatedEdids"] = cobj_edids

        # Also populate technicalNotes + relatedEdids for PA skins inside bundles
        if fixed.get("bundleItems"):
            for bi in fixed["bundleItems"]:
                pa_note = pa_applicability(
                    bi.get("edid", ""),
                    bi.get("name", ""),
                    bi.get("desc", ""),
                )
                if pa_note:
                    bi["technicalNotes"] = pa_note
                bi_cobj_edids = collect_cobj_edids(bi.get("edid", ""))
                if bi_cobj_edids:
                    bi["relatedEdids"] = bi_cobj_edids

        fixed_items.append(fixed)

    data["items"] = fixed_items

    if fixed_count:
        print(f"[atom_shop] Rewrote {fixed_count} image URL(s)")
    else:
        print(f"[atom_shop] All image URLs already correct")
    if img_filled_count:
        print(f"[atom_shop] Filled {img_filled_count} missing imageUrl(s) from ENTM ETDI")

    # Print category breakdown
    print(f"\n[atom_shop] Category breakdown:")
    for cat in CATEGORY_ORDER:
        count = cat_counts.get(cat, 0)
        if count:
            print(f"  {cat:40s} {count:4d}")
    total = sum(cat_counts.values())
    print(f"  {'TOTAL':40s} {total:4d}")

    # ── Write LTB static data ────────────────────────────────────────
    # LTB imageUrl values are filenames only; the JS prepends LTB_IMAGE_BASE_URL.
    # If an imageUrl is empty ("") the JS renders a placeholder — that's fine.
    # Auto-populate technicalNotes + relatedEdids for PA skins in LTB bundles.
    for ltb in LTB_BUNDLES:
        for li in ltb.get("items", []):
            pa_note = pa_applicability(
                li.get("edid", ""),
                li.get("name", ""),
                li.get("desc", ""),
            )
            if pa_note:
                li["technicalNotes"] = pa_note
            li_cobj_edids = collect_cobj_edids(li.get("edid", ""))
            if li_cobj_edids:
                li["relatedEdids"] = li_cobj_edids
    data["ltb"] = LTB_BUNDLES
    print(f"\n[atom_shop] LTB bundles written: {len(LTB_BUNDLES)}")

    os.makedirs(os.path.dirname(DIST), exist_ok=True)
    with open(DIST, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\n[atom_shop] OK — {len(fixed_items)} items + {len(LTB_BUNDLES)} LTB bundles written to dist/atom_shop.json")


if __name__ == "__main__":
    main()
