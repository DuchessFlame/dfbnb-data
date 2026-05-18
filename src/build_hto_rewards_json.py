#!/usr/bin/env python3
"""
build_hto_rewards_json.py
Build HTO (Infestations) reward JSON for buffsnbrew.com.

Reads GLOB values from TSV for numeric accuracy, structures the reward tree
manually based on confirmed xEdit data. Output consumed by df-bnb-infestations.js.

Output: dist/infestations/hto_rewards.json
"""

import json, os, glob, re, csv
from pathlib import Path

# Paths
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_DIR    = _REPO_ROOT / "tsv"
DIST_DIR   = _REPO_ROOT / "dist" / "infestations"


def newest(pattern):
    hits = sorted(glob.glob(str(TSV_DIR / pattern)))
    return hits[-1] if hits else None


def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, encoding=enc, newline="") as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except (UnicodeDecodeError, KeyError):
            continue
    return []


def load_globs():
    path = newest("GLOB_Export_*.tsv")
    globs = {}
    for row in read_tsv(path):
        fid = (row.get("FormID") or "").strip()
        fltv = row.get("FLTV", "0")
        try:
            globs[fid] = float(fltv)
        except (ValueError, TypeError):
            globs[fid] = 0.0
    return globs


# --- Build reward structure ---

def build_hto_rewards():
    globs = load_globs()
    def g(fid, fallback=0.0):
        return globs.get(fid, fallback)

    xp   = int(g("00888541", 5000))
    caps = int(g("00888542", 500))

    # FirstMatch threshold GLOBs
    cn_high   = g("008B7CDE", 25.0)
    cn_medium = g("008B7CDF", 50.0)
    cn_low    = g("008B7CE0", 75.0)

    # 4-star legendary boss drop: GLOB FLTV = % chance the 4-star entry wins.
    # May 2026 TSV stores 100. Old PTS fallback was 50.
    cn_4star  = g("00893F9E", 50.0)

    # Per-pool counts (qty GLOBs)
    scrap_count     = int(g("00893FB3", 10.0))
    stimpak_count   = int(g("00893FB7", 1))
    resources_count = int(g("00893FAD", 1))
    ammo_boss       = int(g("00893F8E", 15))
    ammo_mob        = int(g("00893F8F", 5))
    ammo_support    = int(g("00893F90", 1))

    # List-level ChanceNone (LVCV on the wrapper LVLI)
    serum_list_cn   = 80.0   # 00888532 / 00893F82
    stimpak_list_cn = 80.0   # 00893F83

    # Toggle GLOBs for legendary categories
    leg_toggle_armor  = bool(g("00893FA1", 1.0))
    leg_toggle_pa     = bool(g("00893FA2", 1.0))
    leg_toggle_melee  = bool(g("00893FA3", 1.0))
    leg_toggle_ranged = bool(g("00893FA4", 1.0))

    # === BOSS LOOT ===

    boss_loot = []

    # 1. Legendary - 4 sub-categories each FirstMatch (4-star gated)
    boss_loot.append({
        "label": "Legendary",
        "formid": "0088851D",
        "subtitle": "Each list rolled when you loot the boss · 4 reward lists (Ranged / Melee / PA / Armor)",
        "dropRate": 100,
        "children": [
            {"label": "Weapons (Ranged)", "formid": "00888524", "dropRate": 25,
             "items": [
                {"name": "4-Star Legendary Ranged Weapon", "formid": "0088853C", "sig": "LGDI", "qty": 1, "dropRate": cn_4star, "note": "HTO-exclusive"},
                {"name": "3-Star Legendary Ranged Weapon", "formid": "00417C44", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star, "note": "Standard fallback"},
             ], "mode": "firstmatch"},
            {"label": "Weapons (Melee)", "formid": "00888522", "dropRate": 25,
             "items": [
                {"name": "4-Star Legendary Melee Weapon", "formid": "0088853B", "sig": "LGDI", "qty": 1, "dropRate": cn_4star},
                {"name": "3-Star Legendary Melee Weapon", "formid": "00417C48", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star},
             ], "mode": "firstmatch"},
            {"label": "Power Armor", "formid": "00888520", "dropRate": 25,
             "items": [
                {"name": "4-Star Legendary Power Armor", "formid": "0088853A", "sig": "LGDI", "qty": 1, "dropRate": cn_4star},
                {"name": "3-Star Legendary Power Armor", "formid": "00605FC5", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star},
             ], "mode": "firstmatch"},
            {"label": "Armor", "formid": "0088851E", "dropRate": 25,
             "items": [
                {"name": "4-Star Legendary Armor", "formid": "00888539", "sig": "LGDI", "qty": 1, "dropRate": cn_4star},
                {"name": "3-Star Legendary Armor", "formid": "00417C41", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star},
             ], "mode": "firstmatch"},
        ],
        "mode": "pickone",
    })

    boss_loot.append({
        "label": "Bobbleheads",
        "formid": "00888515",
        "subtitle": "Guaranteed drop · 1 item",
        "dropRate": 100,
        "items": [{"name": "Bobblehead Box", "formid": "008B0D63", "sig": "ALCH", "qty": 1, "dropRate": 100}],
    })

    boss_loot.append({
        "label": "Magazines",
        "formid": "00888526",
        "subtitle": "Guaranteed drop · 1 item",
        "dropRate": 100,
        "items": [{"name": "Magazine Book Box", "formid": "008B0D62", "sig": "ALCH", "qty": 1, "dropRate": 100}],
    })

    boss_loot.append({
        "label": "Treasure Maps",
        "formid": "00888537",
        "subtitle": "Regional loot pool — rewards depend on the infestation's region · 5 regions",
        "dropRate": 100,
        "items": [
            {"name": "Cranberry Bog / Savage Forest Treasure Map", "formid": "003D0CD8", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: Cranberry Bog or The Mire"]},
            {"name": "Forest Treasure Map",                       "formid": "003D0CD5", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: The Forest"]},
            {"name": "Savage Divide Treasure Map",                "formid": "003D0CD9", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: Savage Divide"]},
            {"name": "The Mire Treasure Map",                     "formid": "003D0CD7", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: The Mire"]},
            {"name": "Toxic Valley Treasure Map",                 "formid": "003D0CD6", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: Toxic Valley"]},
        ],
        "mode": "regional",
        "note": "Only the map matching the infestation's region will drop.",
    })

    boss_loot.append({
        "label": "Resources",
        "formid": "00893F7C",
        "subtitle": "Each item rolls independently · 4 items",
        "dropRate": 100,
        "items": [
            {"name": "Scrap-to-Stash",   "formid": "008B0D64", "sig": "UTIL", "qty": resources_count, "dropRate": 100},
            {"name": "Treasury Note",    "formid": "005A5443", "sig": "MISC", "qty": resources_count, "dropRate": 100},
            {"name": "Legendary Module", "formid": "005652F9", "sig": "MISC", "qty": resources_count, "dropRate": 100},
            {"name": "Legendary Tokens", "formid": "003F7410", "sig": "CNCY", "qty": resources_count, "dropRate": 100},
        ],
        "mode": "useall",
    })

    boss_loot.append({
        "label": "Explosives",
        "formid": "0088851C",
        "subtitle": "Each list rolled when you loot the boss · 2 reward lists (grenade + mine)",
        "dropRate": 100,
        "children": [
            {"label": "Grenades", "formid": "00893F79",
             "subtitle": "Pick one · 11 items", "dropRate": 100,
             "items": [
                {"name": "Cryo Grenade",            "formid": "0011002F", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Floater Gnasher Grenade", "formid": "005A70BB", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Floater Flamer Grenade",  "formid": "005A70B6", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Floater Freezer Grenade", "formid": "005A70BC", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Nuka Grenade",            "formid": "001BBCBC", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Nuka Quantum Grenade",    "formid": "0034E210", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Plasma Grenade",          "formid": "0011002D", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Pulse Grenade",           "formid": "00110030", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Molotov Cocktail",        "formid": "00110031", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Baseball Grenade",        "formid": "0034E212", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                {"name": "Frag Grenade",            "formid": "0011002B", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
             ], "mode": "pickone"},
            {"label": "Mines", "formid": "00888527",
             "subtitle": "Pick one · 4 items", "dropRate": 100,
             "items": [
                {"name": "Frag Mine",   "formid": "00110034", "sig": "LVLI", "qty": 1, "dropRate": 25},
                {"name": "Pulse Mine",  "formid": "00110039", "sig": "LVLI", "qty": 1, "dropRate": 25},
                {"name": "Cryo Mine",   "formid": "00110035", "sig": "LVLI", "qty": 1, "dropRate": 25},
                {"name": "Plasma Mine", "formid": "00110038", "sig": "LVLI", "qty": 1, "dropRate": 25},
             ], "mode": "pickone"},
        ],
        "mode": "useall",
    })

    boss_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F76",
        "subtitle": f"Guaranteed drop · {ammo_boss} rounds (matched to equipped weapon)",
        "dropRate": 100,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_boss, "dropRate": 100}],
        "note": f"Drops {ammo_boss} rounds of ammo matching the type your equipped weapon uses.",
    })

    # Serums - pick-one (list-level CN=80, so 20% to drop at all)
    serum_drop = round(100 - serum_list_cn, 1)
    serum_items = [
        ("Adrenal Reaction Serum", "00505BD4"), ("Bird Bones Serum", "0050A5BC"),
        ("Carnivore Serum",        "0050A5BF"), ("Chameleon Serum",  "0050A5C6"),
        ("Eagle Eyes Serum",       "0050A5C9"), ("Egg Head Serum",   "0050A5CB"),
        ("Electrically Charged Serum", "0050A5CD"), ("Empath Serum", "0050A5D2"),
        ("Grounded Serum",         "0050A5D0"), ("Healing Factor Serum", "0050A5D6"),
        ("Herbivore Serum",        "0050A5C4"), ("Herd Mentality Serum",  "0050A5E3"),
        ("Marsupial Serum",        "0050A5E7"), ("Plague Walker Serum",   "0050A5EA"),
        ("Scaly Skin Serum",       "0050A5F0"), ("Speed Demon Serum",     "0050A5F3"),
        ("Talons Serum",           "0050A5F6"), ("Twisted Muscles Serum", "0050A5F9"),
        ("Unstable Isotope Serum", "0050A5FC"),
    ]
    boss_loot.append({
        "label": "Serums",
        "formid": "00888532",
        "subtitle": f"Chance drop of one item · {len(serum_items)} items",
        "dropRate": serum_drop,
        "items": [{"name": n, "formid": f, "sig": "ALCH", "qty": 1,
                   "dropRate": round(serum_drop / len(serum_items), 4)}
                  for n, f in serum_items],
        "mode": "pickone",
        "listChanceNone": serum_list_cn,
    })

    # Chems - FirstMatch ChemsList: high% rare, rest basic
    rare_threshold  = cn_high
    basic_threshold = 100 - rare_threshold
    rare_items_raw = [
        ("Berry Mentats",  "000518BB"), ("Grape Mentats",  "0010129A"),
        ("Orange Mentats", "000518C5"), ("Bufftats",       "00058AA5"),
        ("Psychobuff",     "00058AAC"), ("Psychotats",     "00058AAA"),
        ("Daddy-O",        "00156D0B"), ("Day Tripper",    "00150729"),
        ("Fury",           "000628CA"), ("Calmex",         "00058AA7"),
        ("Overdrive",      "00058AAD"), ("X-Cell",         "001506F4"),
    ]
    boss_loot.append({
        "label": "Chems",
        "formid": "00888517",
        "subtitle": f"FirstMatch tier router · {int(rare_threshold)}% rare / {int(basic_threshold)}% basic",
        "dropRate": 100,
        "children": [
            {"label": "Rare Chems", "formid": "00888519",
             "subtitle": f"Pick one · {len(rare_items_raw)} items", "dropRate": rare_threshold,
             "items": [{"name": n, "formid": f, "sig": "ALCH", "qty": 1,
                        "dropRate": round(rare_threshold / len(rare_items_raw), 4)}
                       for n, f in rare_items_raw],
             "mode": "pickone"},
            {"label": "Basic Chems", "formid": "00888518",
             "subtitle": "Pick one · 4 items", "dropRate": basic_threshold,
             "items": [
                {"name": "Buffout", "formid": "00033778", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                {"name": "Med-X",   "formid": "00033779", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                {"name": "Mentats", "formid": "0003377B", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                {"name": "Psycho",  "formid": "0003377D", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
             ], "mode": "pickone"},
        ],
        "mode": "firstmatch",
    })

    # Stimpaks - wrapper (00893F83) UseAll with list-level CN=80 -> 20% to fire.
    # Inside: StimPaks_Boss (0085A378) FirstMatch with quantity tiers.
    stim_drop = round(100 - stimpak_list_cn, 2)
    stim_3x   = round(stim_drop * cn_high / 100.0, 2)
    stim_2x   = round(stim_drop * (cn_medium - cn_high) / 100.0, 2)
    stim_1x   = round(stim_drop * (100 - cn_medium) / 100.0, 2)
    boss_loot.append({
        "label": "Stimpaks",
        "formid": "00893F83",
        "subtitle": f"Chance drop of one item · 3 quantity tiers (5% × 3 + 5% × 2 + 10% × 1)",
        "dropRate": stim_drop,
        "items": [
            {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 3, "dropRate": stim_3x, "note": f"Top {int(cn_high)}% roll"},
            {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 2, "dropRate": stim_2x, "note": f"Middle {int(cn_medium - cn_high)}% roll"},
            {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 1, "dropRate": stim_1x, "note": f"Bottom {int(100 - cn_medium)}% roll"},
        ],
        "mode": "firstmatch",
        "listChanceNone": stimpak_list_cn,
    })

    boss_loot.append({
        "label": "Rads",
        "formid": "0088852B",
        "subtitle": "Guaranteed drop of one item · 2 items",
        "dropRate": 100,
        "items": [
            {"name": "RadAway", "formid": "002049B7", "sig": "LVLI", "qty": 1, "dropRate": 50},
            {"name": "Rad-X",   "formid": "002049B8", "sig": "LVLI", "qty": 1, "dropRate": 50},
        ],
        "mode": "pickone",
    })

    # Scrap - FirstMatch tiers
    boss_loot.append({
        "label": "Scrap",
        "formid": "0088851A",
        "subtitle": f"FirstMatch tier router · {scrap_count}× from one of 4 rarity tiers",
        "dropRate": 100,
        "scrapCount": scrap_count,
        "children": [
            {"label": "Very Rare Scrap", "formid": "00893F81",
             "subtitle": "Pick one · 5 items", "dropRate": cn_high,
             "items": [
                {"name": "Ballistic Fiber",  "formid": "00432C9A", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                {"name": "Black Titanium",   "formid": "00432C9D", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                {"name": "Nuclear Material", "formid": "00432CAC", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                {"name": "Ultracite",        "formid": "00434513", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                {"name": "Vault Steel",      "formid": "00893FBB", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
             ], "mode": "pickone"},
            {"label": "Rare Scrap", "formid": "00893F7F",
             "subtitle": "Pick one · 7 items", "dropRate": cn_medium - cn_high,
             "items": [
                {"name": "Antiseptic",   "formid": "00432C9C", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                {"name": "Asbestos",     "formid": "00432C9B", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                {"name": "Circuitry",    "formid": "00432CA0", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                {"name": "Fiber Optics", "formid": "00432CA7", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                {"name": "Gold",         "formid": "00432CAA", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                {"name": "Oil",          "formid": "00432CAD", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                {"name": "Silver",       "formid": "00432CB1", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
             ], "mode": "pickone"},
            {"label": "Uncommon Scrap", "formid": "00893F80",
             "subtitle": "Pick one · 9 items", "dropRate": cn_low - cn_medium,
             "items": [
                {"name": "Acid",       "formid": "00432C99", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Adhesive",   "formid": "00432C96", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Aluminum",   "formid": "003D0836", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Copper",     "formid": "00432CA2", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Fiberglass", "formid": "00432CA6", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Gears",      "formid": "00432CA8", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Glass",      "formid": "00432CA9", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Screws",     "formid": "00432CB0", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
                {"name": "Springs",    "formid": "00432CB2", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_low - cn_medium) / 9, 2)},
             ], "mode": "pickone"},
            {"label": "Common Scrap", "formid": "00893F7E",
             "subtitle": "Pick one · 13 items", "dropRate": 100 - cn_low,
             "items": [
                {"name": "Bone",       "formid": "00432C9F", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Ceramic",    "formid": "00432C9E", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Cloth",      "formid": "00432C97", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Coal",       "formid": "00432CA1", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Concrete",   "formid": "00432C98", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Crystal",    "formid": "00432CA4", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Fertilizer", "formid": "00432CA5", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Lead",       "formid": "003D0835", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Leather",    "formid": "00432CAB", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Plastic",    "formid": "00432CAE", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Rubber",     "formid": "00432CAF", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Steel",      "formid": "003D0834", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
                {"name": "Wood",       "formid": "00432CB3", "sig": "LVLI", "qty": scrap_count, "dropRate": round((100 - cn_low) / 13, 2)},
             ], "mode": "pickone"},
        ],
        "mode": "firstmatch",
    })

    # Bespoke (faction-specific) - waterfall, only one fires
    boss_loot.append({
        "label": "Bespoke (Faction-Specific)",
        "formid": "00893F6F",
        "subtitle": "Faction-conditional · only one entry fires (Robot or Scorched bosses)",
        "dropRate": 100,
        "children": [
            {"label": "Robot Boss", "formid": "00893F73",
             "subtitle": "Only drops from Robot faction bosses", "dropRate": 100,
             "conditions": ["Boss is Robot faction"],
             "items": [
                {"name": "Fusion Cells", "formid": "00888530", "sig": "LVLI", "qty": 1, "dropRate": cn_high,             "note": f"Top {int(cn_high)}% roll"},
                {"name": "Components",   "formid": "0088852F", "sig": "LVLI", "qty": 1, "dropRate": cn_medium - cn_high, "note": f"Middle {int(cn_medium - cn_high)}% roll"},
                {"name": "Robot Scrap",  "formid": "00888531", "sig": "LVLI", "qty": 1, "dropRate": 100 - cn_medium,     "note": f"Bottom {int(100 - cn_medium)}% roll"},
             ], "mode": "firstmatch"},
            {"label": "Scorched Boss (Holiday)", "formid": "008B2319",
             "subtitle": "Only drops from Scorched bosses during seasonal events", "dropRate": 100,
             "conditions": ["Boss is Scorched faction", "Holiday toggle active"],
             "items": [
                {"name": "Festive Holiday Gift", "formid": "0059B558", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Festive Scorched enabled"]},
                {"name": "Spooky Holiday Gift",  "formid": "008B2531", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Spooky Scorched enabled"]},
             ], "mode": "conditional"},
        ],
        "mode": "waterfall",
    })

    # === MOB LOOT ===
    # crLLD_Mob (0085CDB6) is UseAll, MaxV=0 -> all 3 child pools fire independently.

    mob_loot = []

    # Mob Legendary - UseAll waterfall max=1 (GLOB 00893F9F = 1).
    # Each entry gated by a Toggle GLOB.
    mob_leg_types = [
        ("Ranged Weapon", "008F2B1C", "00417C44", "Ranged toggle",       leg_toggle_ranged),
        ("Melee Weapon",  "008F2B1A", "00417C48", "Melee toggle",        leg_toggle_melee),
        ("Power Armor",   "008F2B1D", "00605FC5", "Power Armor toggle",  leg_toggle_pa),
        ("Armor",         "008F2B1E", "00417C41", "Armor toggle",        leg_toggle_armor),
    ]
    enabled_count = sum(1 for *_, en in mob_leg_types if en)
    per_type_rate = round(100.0 / enabled_count, 2) if enabled_count else 0.0
    mob_leg_children = [
        {
            "label": label,
            "formid": list_fid,
            "dropRate": per_type_rate if enabled else 0.0,
            "items": [{
                "name": f"3-Star Legendary {label}",
                "formid": lgdi_fid, "sig": "LGDI",
                "qty": 1, "dropRate": 100,
                "conditions": [f"{toggle_name} enabled" if enabled else f"{toggle_name} DISABLED"],
            }],
        }
        for label, list_fid, lgdi_fid, toggle_name, enabled in mob_leg_types
    ]
    mob_loot.append({
        "label": "Legendary",
        "formid": "008F2B1B",
        "subtitle": f"Waterfall by category toggle · {enabled_count} enabled types",
        "dropRate": 100,
        "children": mob_leg_children,
        "mode": "waterfall",
        "note": ("Waterfall picks the first enabled category. Toggles live on GLOBs "
                 "00893FA1-00893FA4 (Armor / PA / Melee / Ranged) and default to enabled."),
    })

    # Mob Bespoke - UseAll waterfall max=1, gated by creature ActorType keyword
    mob_loot.append({
        "label": "Bespoke (Creature-Specific)",
        "formid": "00893F72",
        "subtitle": "Creature-conditional · only one entry fires (5 creature types)",
        "dropRate": 100,
        "children": [
            {
                "label": "Humanoid Mob",
                "formid": "00893F71",
                "subtitle": "Each list rolled when the mob dies · 2 reward lists (grenade + stimpak)",
                "dropRate": 100,
                "conditions": ["Mob is humanoid (BloodEagle, Cultist, PRC Ghoul, MoleMiner, SuperMutant)"],
                "children": [
                    {"label": "Mob Grenades", "formid": "0085A377",
                     "subtitle": "Guaranteed drop of one item · 3 items", "dropRate": 100,
                     "items": [
                        {"name": "Molotov Cocktail", "formid": "00110031", "sig": "LVLI", "qty": 1, "dropRate": round(100/3, 2)},
                        {"name": "Baseball Grenade", "formid": "0034E212", "sig": "LVLI", "qty": 1, "dropRate": round(100/3, 2)},
                        {"name": "Frag Grenade",     "formid": "0011002B", "sig": "LVLI", "qty": 1, "dropRate": round(100/3, 2)},
                     ], "mode": "pickone"},
                    {"label": "Mob Stimpaks", "formid": "00893F84",
                     "subtitle": "Guaranteed drop of one item · 3 quantity tiers (1× / 2× / 3×)", "dropRate": 100,
                     "items": [
                        {"name": "Stimpak", "formid": "00023736", "sig": "ALCH", "qty": 3, "dropRate": round(100/3, 2)},
                        {"name": "Stimpak", "formid": "00023736", "sig": "ALCH", "qty": 2, "dropRate": round(100/3, 2)},
                        {"name": "Stimpak", "formid": "00023736", "sig": "ALCH", "qty": 1, "dropRate": round(100/3, 2)},
                     ], "mode": "pickone"},
                ],
                "mode": "useall",
            },
            {
                "label": "Mothman Hatchling Mob",
                "formid": "0085CDB7",
                "subtitle": "Single reward list · Mothman components",
                "dropRate": 100,
                "conditions": ["Mob has ActorTypeMothmanHatchling keyword"],
                "children": [
                    {"label": "Mothman Components", "formid": "0088851B",
                     "subtitle": "Each item rolls independently · 3 items",
                     "dropRate": 100,
                     "items": [
                        {"name": "Neurotoxic Dust", "formid": "003315B5", "sig": "MISC", "qty": 1, "dropRate": 25, "note": "CN=75"},
                        {"name": "Mothman Wing",    "formid": "003315B6", "sig": "MISC", "qty": 1, "dropRate": 50, "note": "CN=50 (entry #1)"},
                        {"name": "Mothman Wing",    "formid": "003315B6", "sig": "MISC", "qty": 1, "dropRate": 50, "note": "CN=50 (entry #2 · duplicate)"},
                     ], "mode": "useall"},
                ],
            },
            {
                "label": "Floater Mob",
                "formid": "00893F70",
                "subtitle": "Each list rolled when the mob dies · 2 reward lists (PusSac + Components)",
                "dropRate": 100,
                "conditions": ["Mob has ActorTypeFloater keyword"],
                "children": [
                    {"label": "Floater Pus Sac", "formid": "00888534",
                     "subtitle": "Subtype-conditional · 3 outcomes by Floater variant", "dropRate": 100,
                     "items": [
                        {"name": "Flamer Pus Sac",  "formid": "00592EB1", "sig": "MISC", "qty": 1, "dropRate": 100, "conditions": ["ActorTypeFloaterFlamer"]},
                        {"name": "Freezer Pus Sac", "formid": "00592EB0", "sig": "MISC", "qty": 1, "dropRate": 100, "conditions": ["ActorTypeFloaterFreezer"]},
                        {"name": "Gnasher Pus Sac", "formid": "00592EAF", "sig": "MISC", "qty": 1, "dropRate": 100, "conditions": ["ActorTypeFloaterGnasher"]},
                     ], "mode": "conditional"},
                    {"label": "Floater Components", "formid": "00888533",
                     "subtitle": "Guaranteed drop of one item · 10 items (5 scrap × 2 qty tiers)", "dropRate": 100,
                     "items": [
                        {"name": "Adhesive Scrap",         "formid": "001BF72E", "sig": "MISC", "qty": 1, "dropRate": 10},
                        {"name": "Oil Scrap",              "formid": "001BF732", "sig": "MISC", "qty": 1, "dropRate": 10},
                        {"name": "Crystal Scrap",          "formid": "0006907D", "sig": "MISC", "qty": 1, "dropRate": 10},
                        {"name": "Nuclear Material Scrap", "formid": "00069086", "sig": "MISC", "qty": 1, "dropRate": 10},
                        {"name": "Acid Scrap",             "formid": "001BF72D", "sig": "MISC", "qty": 1, "dropRate": 10},
                        {"name": "Adhesive Scrap",         "formid": "001BF72E", "sig": "MISC", "qty": 0, "dropRate": 10, "note": "qty=0 entry"},
                        {"name": "Oil Scrap",              "formid": "001BF732", "sig": "MISC", "qty": 2, "dropRate": 10},
                        {"name": "Crystal Scrap",          "formid": "0006907D", "sig": "MISC", "qty": 2, "dropRate": 10},
                        {"name": "Nuclear Material Scrap", "formid": "00069086", "sig": "MISC", "qty": 2, "dropRate": 10},
                        {"name": "Acid Scrap",             "formid": "001BF72D", "sig": "MISC", "qty": 2, "dropRate": 10},
                     ], "mode": "pickone"},
                ],
                "mode": "useall",
            },
            {
                "label": "Robot Mob",
                "formid": "00893F73",
                "subtitle": "FirstMatch tier router · 3 outcomes (fusion cells / components / scrap)",
                "dropRate": 100,
                "conditions": ["Mob has ActorTypeRobot keyword"],
                "items": [
                    {"name": "Fusion Cells", "formid": "00888530", "sig": "LVLI", "qty": 1, "dropRate": cn_high,             "note": f"Top {int(cn_high)}% roll"},
                    {"name": "Components",   "formid": "0088852F", "sig": "LVLI", "qty": 1, "dropRate": cn_medium - cn_high, "note": f"Middle {int(cn_medium - cn_high)}% roll"},
                    {"name": "Robot Scrap",  "formid": "00888531", "sig": "LVLI", "qty": 1, "dropRate": 100 - cn_medium,     "note": f"Bottom {int(100 - cn_medium)}% roll"},
                ],
                "mode": "firstmatch",
            },
            {
                "label": "Scorched Mob (Holiday)",
                "formid": "008B2319",
                "subtitle": "Holiday-conditional · 2 outcomes (Festive / Spooky)",
                "dropRate": 100,
                "conditions": ["Mob has ActorTypeScorched keyword", "Seasonal holiday active"],
                "items": [
                    {"name": "Festive Holiday Gift", "formid": "0059B558", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Festive Scorched enabled"]},
                    {"name": "Spooky Holiday Gift",  "formid": "008B2531", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Spooky Scorched enabled"]},
                ],
                "mode": "conditional",
            },
        ],
        "mode": "waterfall",
    })

    mob_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F77",
        "subtitle": f"Guaranteed drop · {ammo_mob} rounds (matched to equipped weapon)",
        "dropRate": 100,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_mob, "dropRate": 100}],
        "note": f"Drops {ammo_mob} rounds matching the type your equipped weapon uses.",
    })

    # === SUPPORT LOOT ===
    # crLLD_Support (00893F6E) is UseAll, MaxV=0 -> all 3 pools fire independently.

    support_loot = []

    sup_leg_types = [
        ("Ranged Weapon", "008F2B1F", "00417C44", "Ranged toggle",       leg_toggle_ranged),
        ("Melee Weapon",  "008F2B20", "00417C48", "Melee toggle",        leg_toggle_melee),
        ("Power Armor",   "008F2B19", "00605FC5", "Power Armor toggle",  leg_toggle_pa),
        ("Armor",         "008F2B17", "00417C41", "Armor toggle",        leg_toggle_armor),
    ]
    sup_enabled_count = sum(1 for *_, en in sup_leg_types if en)
    sup_per_type_rate = round(100.0 / sup_enabled_count, 2) if sup_enabled_count else 0.0
    sup_leg_children = [
        {
            "label": label,
            "formid": list_fid,
            "dropRate": sup_per_type_rate if enabled else 0.0,
            "items": [{
                "name": f"3-Star Legendary {label}",
                "formid": lgdi_fid, "sig": "LGDI",
                "qty": 1, "dropRate": 100,
                "conditions": [f"{toggle_name} enabled" if enabled else f"{toggle_name} DISABLED"],
            }],
        }
        for label, list_fid, lgdi_fid, toggle_name, enabled in sup_leg_types
    ]
    support_loot.append({
        "label": "Legendary",
        "formid": "008F2B18",
        "subtitle": f"Waterfall by category toggle · {sup_enabled_count} enabled types",
        "dropRate": 100,
        "children": sup_leg_children,
        "mode": "waterfall",
        "note": "Shares the four toggle GLOBs with the Mob legendary pool.",
    })

    support_loot.append({
        "label": "Bespoke (Creature-Specific)",
        "formid": "00893F74",
        "subtitle": "Creature-conditional · only one entry fires (6 creature types)",
        "dropRate": 100,
        "children": [
            {"label": "Dog", "formid": "00863222",
             "subtitle": f"FirstMatch · {int(cn_medium)}% Bones / {int(cn_low - cn_medium)}% Dogmeat",
             "dropRate": 100, "conditions": ["Support has ActorTypeDog keyword"],
             "items": [
                {"name": "Bones (LVLI)",   "formid": "00070511", "sig": "LVLI", "qty": 1, "dropRate": cn_medium,          "note": f"Top {int(cn_medium)}% roll"},
                {"name": "Dogmeat (LVLI)", "formid": "002165DE", "sig": "LVLI", "qty": 1, "dropRate": cn_low - cn_medium, "note": f"Next {int(cn_low - cn_medium)}% roll"},
             ], "mode": "firstmatch"},
            {"label": "Eye Bot", "formid": "00865AF8",
             "subtitle": f"FirstMatch · {int(cn_medium)}% Components / {int(cn_low - cn_medium)}% Scrap",
             "dropRate": 100, "conditions": ["Support has ActorTypeEyebot keyword"],
             "items": [
                {"name": "Robot Components (LVLI)", "formid": "0088852F", "sig": "LVLI", "qty": 1, "dropRate": cn_medium,          "note": f"Top {int(cn_medium)}% roll"},
                {"name": "Robot Scrap (LVLI)",      "formid": "00888531", "sig": "LVLI", "qty": 1, "dropRate": cn_low - cn_medium, "note": f"Next {int(cn_low - cn_medium)}% roll"},
             ], "mode": "firstmatch"},
            {"label": "Liberator", "formid": "0085B657",
             "subtitle": f"FirstMatch · {int(cn_high)}% Trinkets / {int(cn_medium - cn_high)}% Components",
             "dropRate": 100, "conditions": ["Support has ActorTypeLiberator keyword"],
             "items": [
                {"name": "Liberator Trinkets (LVLI)",   "formid": "0088852A", "sig": "LVLI", "qty": 1, "dropRate": cn_high,             "note": f"Top {int(cn_high)}% roll"},
                {"name": "Liberator Components (LVLI)", "formid": "00888529", "sig": "LVLI", "qty": 1, "dropRate": cn_medium - cn_high, "note": f"Next {int(cn_medium - cn_high)}% roll"},
             ], "mode": "firstmatch"},
            {"label": "Mole Rat", "formid": "0085DC58",
             "subtitle": f"FirstMatch · {int(cn_medium)}% Components / {int(cn_low - cn_medium)}% Meat",
             "dropRate": 100, "conditions": ["Support has ActorTypeMolerat keyword"],
             "items": [
                {"name": "MoleRat Components (LVLI)", "formid": "00888528", "sig": "LVLI", "qty": 1, "dropRate": cn_medium,          "note": f"Top {int(cn_medium)}% roll"},
                {"name": "MoleRat Meat (LVLI)",       "formid": "00213D5F", "sig": "LVLI", "qty": 1, "dropRate": cn_low - cn_medium, "note": f"Next {int(cn_low - cn_medium)}% roll"},
             ], "mode": "firstmatch"},
            {"label": "Mutant Hound", "formid": "0086242D",
             "subtitle": f"FirstMatch · {int(cn_high)}% Bones / {int(cn_medium - cn_high)}% Meat",
             "dropRate": 100, "conditions": ["Support has ActorTypeMutantHound keyword"],
             "items": [
                {"name": "Bones (LVLI)",             "formid": "00070511", "sig": "LVLI", "qty": 1, "dropRate": cn_high,             "note": f"Top {int(cn_high)}% roll"},
                {"name": "Mutant Hound Meat (LVLI)", "formid": "00888540", "sig": "LVLI", "qty": 1, "dropRate": cn_medium - cn_high, "note": f"Next {int(cn_medium - cn_high)}% roll"},
             ], "mode": "firstmatch"},
            {"label": "Wolf", "formid": "00863223",
             "subtitle": f"FirstMatch · {int(cn_high)}% Bones / {int(cn_medium - cn_high)}% Meat",
             "dropRate": 100, "conditions": ["Support has ActorTypeMutatedWolf keyword"],
             "items": [
                {"name": "Bones (LVLI)",     "formid": "00070511", "sig": "LVLI", "qty": 1, "dropRate": cn_high,             "note": f"Top {int(cn_high)}% roll"},
                {"name": "Wolf Meat (LVLI)", "formid": "00211979", "sig": "LVLI", "qty": 1, "dropRate": cn_medium - cn_high, "note": f"Next {int(cn_medium - cn_high)}% roll"},
             ], "mode": "firstmatch"},
        ],
        "mode": "waterfall",
    })

    support_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F78",
        "subtitle": f"Guaranteed drop · {ammo_support} round (matched to equipped weapon)",
        "dropRate": 100,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_support, "dropRate": 100}],
        "note": f"Drops {ammo_support} round matching the type your equipped weapon uses.",
    })

    # === ASSEMBLE FINAL JSON ===

    output = {
        "byPage": {
            "infestations-all-rewards": {
                "name": "Infestations All Rewards",
                "questFormID": "00865FA8",
                "gmrwFormID": "00888543",
                "type": "infestations",
                "description": ("World Activity - not a public event. 7 enemy factions, 51 spawn locations "
                                "across all regions, only 9 active at any time. Clear the infestation and "
                                "defeat the boss to earn rewards."),
                "xp": xp,
                "caps": caps,
                "mobNote": ("Boss loot is the headline drop. Mob and Support enemies also drop loot "
                            "(see the Mob Loot and Support Loot expands) - smaller pools tied to the "
                            "creature's faction and keyword."),
                "bossLoot": boss_loot,
                "mobLoot": mob_loot,
                "supportLoot": support_loot,
                "fourStarMods": {
                    "weapons": [
                        {"name": "Tarnished", "formid": "0085B998", "effect": "Damage increases as weapon durability decreases (up to +120%)"},
                        {"name": "Satiated",  "formid": "0085B996", "effect": "Details in MGEF - exact effect TBD from PTS testing"},
                    ],
                    "armor": [
                        {"name": "Vector",  "formid": "0085B99A", "effect": "Gain 10% Bonus VATS Accuracy Against Distant Targets (up to 50% with 5 pieces)"},
                        {"name": "Raging",  "formid": "0085B997", "effect": "Details in MGEF - exact effect TBD from PTS testing"},
                        {"name": "Haulers", "formid": "0085B99B", "effect": "Details in MGEF - exact effect TBD from PTS testing"},
                    ],
                    "note": "4-star legendary mods are exclusive to Infestations. Vector value is 10% per piece (GLOB 00868BD2).",
                },
                "challenges": {
                    "lifetime": [
                        {"name": "Discover an Infestation",                         "target": 1},
                        {"name": "Complete an Infestation",                         "target": 1},
                        {"name": "Complete 10 Infestations",                        "target": 10},
                        {"name": "Complete 76 Infestations",                        "target": 76},
                        {"name": "Complete 760 Infestations",                       "target": 760},
                        {"name": "Complete an Infestation Involving Every Faction", "target": 7},
                    ],
                    "daily": [
                        {"name": "Kill an Enemy during an Infestation",        "target": 3},
                        {"name": "Kill an Enemy during an Infestation (Team)", "target": 3},
                        {"name": "Complete an Infestation",                    "target": 1},
                        {"name": "Complete an Infestation (Team)",             "target": 1},
                    ],
                    "weekly": [
                        {"name": "Kill an Enemy during an Infestation",        "target": 10},
                        {"name": "Kill an Enemy during an Infestation (Team)", "target": 10},
                        {"name": "Complete 3 Infestations",                    "target": 3},
                        {"name": "Complete 3 Infestations (Team)",             "target": 3},
                    ],
                },
            }
        }
    }

    return output


def main():
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    data = build_hto_rewards()
    out_path = DIST_DIR / "hto_rewards.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    page = data["byPage"]["infestations-all-rewards"]
    print(f"[HTO] Wrote {out_path}")
    print(f"[HTO] XP={page['xp']}, Caps={page['caps']}, "
          f"Boss pools={len(page['bossLoot'])}, "
          f"Mob pools={len(page['mobLoot'])}, "
          f"Support pools={len(page['supportLoot'])}")


if __name__ == "__main__":
    main()
