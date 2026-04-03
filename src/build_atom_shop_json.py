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
  CAMP - Camp Sets, CAMP - Displays & Weapon Racks, CAMP - Doors,
  CAMP - Floors, CAMP - Garden, CAMP - Kiddie Rides,
  CAMP - Lamps & Lights, CAMP - Skins, CAMP - Stash Boxes,
  CAMP - Vending Machines,
  Emotes, Fridges, Photomode, Player, Player Icons, Plushies,
  Pre-Fabs and Structures,
  Skins - Armour, Skins - Backpack, Skins - Pip Boy,
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
        "price":     "$29.99",
        "status":    "active",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Ranger Power Armour Paint",      "desc": "Don the iconic look of the NCR's elite with this rugged, battle-worn Power Armour paint.",                        "imageUrl": ""},
            {"name": "NCR Flag",                       "desc": "Proudly display the NCR Flag at your C.A.M.P.",                                                                   "imageUrl": ""},
            {"name": "New Vegas Neon Sign",            "desc": "Add a touch of vintage Vegas glamour to your settlement.",                                                         "imageUrl": ""},
            {"name": "Ad Victoriam (Super Sledge)",    "desc": "A 4-Star Legendary Super Sledge — the first 4-star weapon sold via the platform store.",                           "imageUrl": ""},
            {"name": "Legion Legate Outfit",           "desc": "Embrace the ruthless efficiency of Caesar's Legion with this imposing outfit.",                                    "imageUrl": ""},
            {"name": 'Player Title: "Ad Victoriam"',  "desc": "Player Title Prefix.",                                                                                             "imageUrl": ""},
            {"name": 'Player Title: "Tribune"',        "desc": "Player Title Prefix and Suffix.",                                                                                  "imageUrl": ""},
        ],
    },
    {
        "id":        "atomic-angler-bundle",
        "name":      "Atomic Angler Bundle",
        "released":  "2025-06-03",
        "update":    None,
        "price":     "$29.99",
        "status":    "active",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Hydro Tech Exo Power Armour",  "desc": "Deep water power armour — or an interesting way to catch fish by hand.",                                           "imageUrl": ""},
            {"name": "Appalachian Contessa Prefab",  "desc": "Place the Appalachian Contessa in your C.A.M.P. and be the captain of your own ship.",                             "imageUrl": ""},
            {"name": "Live Bait Barrel",             "desc": "A resource collector. Have a fresh selection of fishing bait available in your C.A.M.P.",                          "imageUrl": ""},
            {"name": "Floating Buoy Set",            "desc": "For those who like to float things around, even in radiated water.",                                                "imageUrl": ""},
            {"name": "Charmcaster Fishing Rod",      "desc": "For those who said fishing is a boring hobby.",                                                                     "imageUrl": ""},
            {"name": 'Player Title: "Contessa"',    "desc": "Player Title Prefix.",                                                                                              "imageUrl": ""},
            {"name": 'Player Title: "Buoy"',        "desc": "Player Title Suffix.",                                                                                              "imageUrl": ""},
        ],
    },
    {
        "id":        "enclave-armory-bundle",
        "name":      "Enclave Armory Bundle",
        "released":  "2024-12-03",
        "update":    "Gleaming Depths",
        "price":     "$29.99",
        "status":    "replaced",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Enclave Lab Shelter",                   "desc": "What secrets were they hiding in here? Decide for yourself.",                                              "imageUrl": ""},
            {"name": "Enclave Technician Outfit",             "desc": "Gear up and get ready.",                                                                                  "imageUrl": ""},
            {"name": "Enclave Technician Helmet",             "desc": "Patriotism is in the air, and that's all you'll be breathing in with this helmet.",                        "imageUrl": ""},
            {"name": "Vertiguard Enclave Power Armour Paint", "desc": "Continue the Enclave's work in the Gleaming Depths with the Vertiguard Enclave Power Armour Paint.",      "imageUrl": ""},
            {"name": "Vertiguard Enclave Jetpack",            "desc": "Fly into the Gleaming Depths!",                                                                           "imageUrl": ""},
            {"name": "Enclave Repair Bot",                    "desc": "Trust the Enclave to keep your C.A.M.P. at tip-top shape. Cannot be built inside a Shelter.",             "imageUrl": ""},
            {"name": 'Player Title: "Gleaming"',              "desc": "Player Title Prefix.",                                                                                    "imageUrl": ""},
            {"name": 'Player Title: "Technician"',            "desc": "Player Title Suffix.",                                                                                    "imageUrl": ""},
        ],
    },
    {
        "id":        "skyline-valley-lost-treasures-bundle",
        "name":      "Skyline Valley — Lost Treasures Bundle",
        "released":  "2024-06-12",
        "update":    "Skyline Valley",
        "price":     "$29.99",
        "status":    "discontinued",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Weather Control Station (Skyline Valley)", "desc": "Give your C.A.M.P. an electrical red glow!",                                                           "imageUrl": ""},
            {"name": "V63 Power Armour & Jetpack Paint",         "desc": "Become a Vault 63 champion with this shiny new Power Armour and Jetpack skin.",                        "imageUrl": ""},
            {"name": "Vault 63 Door Display",                    "desc": "Bring a piece of Vault 63 to your C.A.M.P.",                                                           "imageUrl": ""},
            {"name": "Moe the Mole Plushie",                     "desc": "Moe the Mole really digs safety.",                                                                     "imageUrl": ""},
            {"name": "V63 Chassis Display Frame",                "desc": "Showcase your Power Armour in Vault 63 style!",                                                        "imageUrl": ""},
            {"name": "Lightning Rod Pose",                       "desc": "Harness the power of lightning with this pose.",                                                        "imageUrl": ""},
        ],
    },
    {
        "id":        "atlantic-city-high-stakes-bundle",
        "name":      "Atlantic City High Stakes Bundle",
        "released":  "2023-12-05",
        "update":    "Atlantic City — Boardwalk Paradise",
        "price":     "$29.99",
        "status":    "discontinued",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Casino Quarter C.A.M.P. Kit",    "desc": "Build your very own Atlantic City.",                                                                             "imageUrl": ""},
            {"name": "Civic Duty Power Armour Paint",  "desc": "The Munis keep the city running. Fulfil your Civic Duty with this Power Armour!",                                "imageUrl": ""},
            {"name": "Honey Pot 'o Gold Slot Machine", "desc": "With a little bit of sweet luck you'll be over the rainbow.",                                                    "imageUrl": ""},
            {"name": "Aquarium of the Atlantic Door",  "desc": "What treasures await you in the deep seas behind this door?",                                                    "imageUrl": ""},
            {"name": "Large Overgrown Plushie",        "desc": "The Overgrown will take over your bedroom with this Large Plushie!",                                             "imageUrl": ""},
            {"name": "Rig Roll-Up Backpack",           "desc": "Rig, Roll, and Pack it up with this all-purpose Backpack!",                                                      "imageUrl": ""},
        ],
    },
    {
        "id":        "pitt-recruitment-bundle",
        "name":      "The Pitt Recruitment Bundle",
        "released":  "2022-09-13",
        "update":    "Expeditions: The Pitt",
        "price":     "$29.99",
        "status":    "discontinued",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Pittsburgh Neighbourhood C.A.M.P. Kit", "desc": "Fit right in with the friends from your expeditions with the Pittsburgh Neighbourhood C.A.M.P. Kit.",   "imageUrl": ""},
            {"name": "Fanatic Paint (10mm SMG)",               "desc": "You don't want to be caught in The Pitt without the Fanatic Paint for the 10mm SMG.",                  "imageUrl": ""},
            {"name": "Fanatic Power Armour Paint",             "desc": "Be right at home in the factory with this Fanatic Power Armour Paint. Can be equipped to all Power Armours.", "imageUrl": ""},
            {"name": "Trog Plushie",                           "desc": "Even Trogs could use a snuggle! Bring this Trog Plushie home to your C.A.M.P.",                        "imageUrl": ""},
            {"name": "Fusion Core Recharger",                  "desc": "Extend the life of your used Fusion Cores. Cannot be built inside a Shelter.",                          "imageUrl": ""},
        ],
    },
    {
        "id":        "mechanists-persona-bundle",
        "name":      "Mechanist's Persona Bundle",
        "released":  "2022-07-13",
        "update":    None,
        "price":     "$9.99",
        "status":    "removed",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms", "desc": "In-game currency for the Atomic Shop.",         "imageUrl": ""},
            {"name": "Mechanist's Outfit",       "desc": "The Mechanist's signature outfit.",             "imageUrl": ""},
            {"name": "Mechanist's Helmet",       "desc": "The Mechanist's iconic helmet.",                "imageUrl": ""},
            {"name": "6x Repair Kits",           "desc": "Standard Repair Kits.",                         "imageUrl": ""},
        ],
    },
    {
        "id":        "elders-persona-bundle",
        "name":      "Elder's Persona Bundle",
        "released":  "2022-06-13",
        "update":    None,
        "price":     "$9.99",
        "status":    "removed",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms", "desc": "In-game currency for the Atomic Shop.",         "imageUrl": ""},
            {"name": "Elder's Battlecoat",       "desc": "The distinguished coat of a Brotherhood Elder.", "imageUrl": ""},
            {"name": "6x Repair Kits",           "desc": "Standard Repair Kits.",                         "imageUrl": ""},
        ],
    },
    {
        "id":        "generals-persona-bundle",
        "name":      "General's Persona Bundle",
        "released":  "2022-05-12",
        "update":    None,
        "price":     "$9.99",
        "status":    "removed",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms",          "desc": "In-game currency for the Atomic Shop.", "imageUrl": ""},
            {"name": "Revolutionary General's Uniform",  "desc": "Suit up in the uniform of a Revolutionary General.", "imageUrl": ""},
            {"name": "3x Repair Kits",                   "desc": "Standard Repair Kits.",                 "imageUrl": ""},
            {"name": "3x Scrap Kits",                    "desc": "Standard Scrap Kits.",                  "imageUrl": ""},
        ],
    },
    {
        "id":        "pint-sized-slashers-persona-bundle",
        "name":      "Pint-Sized Slasher's Persona Bundle",
        "released":  "2022-04-12",
        "update":    None,
        "price":     "$9.99",
        "status":    "removed",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "1000 (+500 Bonus) Atoms",    "desc": "In-game currency for the Atomic Shop.", "imageUrl": ""},
            {"name": "Pint-Sized Slasher Costume", "desc": "The full Pint-Sized Slasher outfit.",   "imageUrl": ""},
            {"name": "Pint-Sized Slasher Knife",   "desc": "The Pint-Sized Slasher's signature knife.", "imageUrl": ""},
            {"name": "3x Repair Kits",             "desc": "Standard Repair Kits.",                 "imageUrl": ""},
            {"name": "3x Scrap Kits",              "desc": "Standard Scrap Kits.",                  "imageUrl": ""},
        ],
    },
    {
        "id":        "brotherhood-recruitment-bundle",
        "name":      "Brotherhood Recruitment Bundle",
        "released":  "2020-12-01",
        "update":    "Steel Dawn",
        "price":     "$29.99",
        "status":    "removed",
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl":  "",
        "items": [
            {"name": "Brotherhood of Steel Scouting Tower",    "desc": "Survey Appalachia for new threats and opportunities from this massive Scouting Tower.",                 "imageUrl": ""},
            {"name": "Brotherhood of Steel Salute",            "desc": "Ad Victoriam, Brothers.",                                                                               "imageUrl": ""},
            {"name": "Brotherhood of Steel Barricade",         "desc": "One more tool to protect against the new dangers of Appalachia.",                                       "imageUrl": ""},
            {"name": "Brotherhood Tactical Field Pack",        "desc": "Never be caught unprepared with the Tactical Field Pack.",                                              "imageUrl": ""},
            {"name": "Brotherhood Barracks Locker",            "desc": "Keep your valuable finds safe and sound with the Barracks Locker.",                                     "imageUrl": ""},
            {"name": "Brotherhood Reclaimed Power Armour Paint", "desc": "Use what you find and reclaim Appalachia!",                                                           "imageUrl": ""},
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
    "CAMP - Camp Sets",
    "CAMP - Displays & Weapon Racks",
    "CAMP - Doors",
    "CAMP - Floors",
    "CAMP - Garden",
    "CAMP - Kiddie Rides",
    "CAMP - Lamps & Lights",
    "CAMP - Skins",
    "CAMP - Stash Boxes",
    "CAMP - Vending Machines",
    "Emotes",
    "Fridges",
    "Photomode",
    "Player",
    "Player Icons",
    "Plushies",
    "Pre-Fabs and Structures",
    "Skins - Armour",
    "Skins - Backpack",
    "Skins - Pip Boy",
    "Skins - Power Armour",
    "Skins - Weapons",
    "Wallpaper",
    "Other",
]


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
    pattern = os.path.join(TSV_ROOT, "ENTM_Export_*.tsv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[atom_shop] WARNING: No ENTM_Export_*.tsv found in {TSV_ROOT}", file=sys.stderr)
        return {}
    tsv_path = files[-1]
    print(f"[atom_shop] Reading DESC from: {os.path.basename(tsv_path)}")
    lookup = {}
    with open(tsv_path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = str(row.get("EDID") or "").strip().upper()
            desc = str(row.get("DESC") or "").strip()
            if edid and desc:
                lookup[edid] = clean_desc(desc)
    print(f"[atom_shop] DESC entries loaded: {len(lookup)}")
    return lookup


def apply_desc(item, lookup):
    edid = str(item.get("edid") or "").strip().upper()
    item["desc"] = lookup.get(edid, "")
    if item.get("bundleItems"):
        for bi in item["bundleItems"]:
            bi_edid = str(bi.get("edid") or "").strip().upper()
            bi["desc"] = lookup.get(bi_edid, "")
    return item


# ── Categorisation (mirrors the JS categoryFromEdid) ─────────────────

def _is_wallpaper_edid(edid):
    e = str(edid or "").upper()
    return "_CAMP_WALLPAPER_" in e or e.startswith("REUSE_ATX_ENTM_CAMP_WALLPAPER_")


def category_from_edid(edid, is_bundle, name, bundle_items):
    e = str(edid or "").upper()
    n = str(name or "").upper()

    # ── Bundles ─────────────────────────────────────────
    if is_bundle:
        if isinstance(bundle_items, list) and len(bundle_items) > 0:
            if all(_is_wallpaper_edid(bi.get("edid", "")) for bi in bundle_items):
                return "Wallpaper"
        if " SET" not in n:
            return "Bundles"

    # ── Name-based overrides ────────────────────────────
    for kw in ("SCOUTING TOWER", "SCOUT TOWER", "FIREWATCH TOWER",
               "SHELTER", "MEGA MANSION", "SEEDY SHED", "OUTHOUSE"):
        if kw in n:
            return "Pre-Fabs and Structures"

    if "REFRIGERATOR" in n or "FRIDGE" in n:
        return "Fridges"
    if "PLUSHIE" in n:
        return "Plushies"
    if "KIDDIE RIDE" in n:
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

    if not e:
        return "Other"

    # ── EDID-based ──────────────────────────────────────
    if "_CAMP_STRUCTURE_" in e or e.startswith("SHELTERS_"):
        return "Pre-Fabs and Structures"

    if "_SKIN_POWERARMOR_" in e or "_SKIN_POWRARMOR_" in e:
        return "Skins - Power Armour"

    if "_SKIN_BACKPACK_" in e or "_BACKPACK_SKIN_" in e:
        return "Skins - Backpack"

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

    if "_UTILITY_COLLECTRON_" in e or "_COFFEEMACHINE_" in e or "_SLOCUMSJOE_" in e:
        return "CAMP - Skins"

    if "_MACHINERY_PURIFIER_" in e or "_MACHINERY_GENERATOR_" in e:
        return "CAMP - Skins"

    if "_FLOORDECKOR_PLUSHIE_" in e or "_FLOORDECOR_PLUSHIE_" in e:
        return "Plushies"

    if any(k in e for k in ("_CAMP_LIGHT_", "_CAMP_LAMP_", "_LIGHTING_", "_NEON_", "_CEILINGFAN_")):
        return "CAMP - Lamps & Lights"

    if any(k in e for k in ("_CAMP_FLOOR_", "_FLOORING_", "_LAMINATE_", "_ASTROTURF_")):
        return "CAMP - Floors"

    if any(k in e for k in ("_CAMP_DOOR_", "_SECRETDOOR_", "_CURTAINDOOR_")):
        return "CAMP - Doors"

    if any(k in e for k in ("_CAMP_GARDEN_", "_PLANT_", "_PLANTER_", "_SUCCULENT_",
                             "_TOPIARY_", "_CACTUS_", "_BRAMBLES_", "_WORMFARM_")):
        return "CAMP - Garden"

    if any(k in e for k in ("_WEAPONRACK_", "_DISPLAYCASE_", "_DISPLAYRACK_",
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

    if "_CAMP_" in e:
        return "Other"

    if "_APPAREL_" in e or e.startswith("ATX_CLOTHES_"):
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

    desc_lookup = load_desc_lookup()

    fixed_count = 0
    cat_counts = {}
    fixed_items = []
    for item in items:
        original_url = item.get("imageUrl", "")
        fixed = fix_item_images(dict(item))
        fixed = apply_desc(fixed, desc_lookup)
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
        fixed_items.append(fixed)

    data["items"] = fixed_items

    if fixed_count:
        print(f"[atom_shop] Rewrote {fixed_count} image URL(s)")
    else:
        print(f"[atom_shop] All image URLs already correct")

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
    data["ltb"] = LTB_BUNDLES
    print(f"\n[atom_shop] LTB bundles written: {len(LTB_BUNDLES)}")

    os.makedirs(os.path.dirname(DIST), exist_ok=True)
    with open(DIST, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\n[atom_shop] OK — {len(fixed_items)} items + {len(LTB_BUNDLES)} LTB bundles written to dist/atom_shop.json")


if __name__ == "__main__":
    main()
