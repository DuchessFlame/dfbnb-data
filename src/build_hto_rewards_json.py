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

# ── Paths ───────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_DIR    = _REPO_ROOT / "tsv"
DIST_DIR   = _REPO_ROOT / "dist" / "infestations"

def newest(pattern):
    """Pick the newest TSV matching a glob pattern (by filename date, then mtime)."""
    hits = sorted(glob.glob(str(TSV_DIR / pattern)))
    if not hits:
        return None
    # Prefer Apr > March > Feb etc.
    return hits[-1]

def read_tsv(path):
    """Read a TSV file, return list of dicts."""
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
    """Load GLOB FormID → FLTV mapping."""
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

# ── Build reward structure ──────────────────────────────────────────────────

def build_hto_rewards():
    globs = load_globs()

    def g(fid, fallback=0.0):
        return globs.get(fid, fallback)

    xp   = int(g("00888541", 5000))
    caps = int(g("00888542", 500))

    # Scrap ChanceNone thresholds (FirstMatch GLOBs)
    cn_high   = g("008B7CDE", 25.0)   # 25 → top 25%
    cn_medium = g("008B7CDF", 50.0)   # 50 → 25-50%
    cn_low    = g("008B7CE0", 75.0)   # 75 → 50-75%
    cn_4star  = g("00893F9E", 50.0)   # 50% chance of 4-star legendary

    # Scrap count GLOB
    scrap_count = int(g("00893FB3", 10.0))

    # Contextual ammo counts
    ammo_boss    = int(g("00893F8E", 15))
    ammo_mob     = int(g("00893F8F", 5))
    ammo_support = int(g("00893F90", 1))

    # Chem rare ChanceNone
    chem_rare_cn = g("00893F8B", 50.0)  # Not used directly — FirstMatch uses threshold GLOBs

    # Serum list-level ChanceNone = 80 → 20% chance to get a serum at all
    serum_list_cn = 80.0  # From LVCV on 00893F82

    # ── Boss Loot Categories (13 pools, all UseAll independent) ──

    boss_loot = []

    # 1. Legendary — UseAll max_count=1, effectively pick-one of 4 types
    #    Each type: FirstMatch 50% 4-star / 50% 3-star
    boss_loot.append({
        "label": "Legendary",
        "formid": "0088851D",
        "subtitle": f"Pick one of 4 types · {int(cn_4star)}% chance of 4-star, {int(100 - cn_4star)}% chance of 3-star",
        "dropRate": 100,
        "children": [
            {
                "label": "Weapons (Ranged)",
                "formid": "00888524",
                "dropRate": 25,
                "items": [
                    {"name": "4★ Ranged Weapon", "formid": "0088853C", "sig": "LGDI", "qty": 1, "dropRate": cn_4star, "note": "HTO-exclusive 4-star legendary"},
                    {"name": "3★ Ranged Weapon", "formid": "00417C44", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star, "note": "Standard 3-star fallback"},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Weapons (Melee)",
                "formid": "00888522",
                "dropRate": 25,
                "items": [
                    {"name": "4★ Melee Weapon", "formid": "0088853B", "sig": "LGDI", "qty": 1, "dropRate": cn_4star},
                    {"name": "3★ Melee Weapon", "formid": "00417C48", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Power Armor",
                "formid": "00888520",
                "dropRate": 25,
                "items": [
                    {"name": "4★ Power Armor", "formid": "0088853A", "sig": "LGDI", "qty": 1, "dropRate": cn_4star},
                    {"name": "3★ Power Armor", "formid": "00605FC5", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Armor",
                "formid": "0088851E",
                "dropRate": 25,
                "items": [
                    {"name": "4★ Armor", "formid": "00888539", "sig": "LGDI", "qty": 1, "dropRate": cn_4star},
                    {"name": "3★ Armor", "formid": "00417C41", "sig": "LGDI", "qty": 1, "dropRate": 100 - cn_4star},
                ],
                "mode": "firstmatch"
            },
        ],
        "mode": "pickone"
    })

    # 2. Bobbleheads — guaranteed 1x BobbleheadBox
    boss_loot.append({
        "label": "Bobbleheads",
        "formid": "00888515",
        "subtitle": "Guaranteed drop · 1 item",
        "dropRate": 100,
        "items": [{"name": "Bobblehead Box", "formid": "008B0D63", "sig": "ALCH", "qty": 1, "dropRate": 100}]
    })

    # 3. Magazines — guaranteed 1x MagazineBookBox
    boss_loot.append({
        "label": "Magazines",
        "formid": "00888526",
        "subtitle": "Guaranteed drop · 1 item",
        "dropRate": 100,
        "items": [{"name": "Magazine Book Box", "formid": "008B0D62", "sig": "ALCH", "qty": 1, "dropRate": 100}]
    })

    # 4. Treasure Maps — region-conditional, pick-one from region match
    boss_loot.append({
        "label": "Treasure Maps",
        "formid": "00888537",
        "subtitle": "Guaranteed drop · 1 map matching the infestation's region",
        "dropRate": 100,
        "items": [
            {"name": "Cranberry Bog / Savage Forest Treasure Map", "formid": "003D0CD8", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: Cranberry Bog or The Mire"]},
            {"name": "Forest Treasure Map",                       "formid": "003D0CD5", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: The Forest"]},
            {"name": "Savage Divide Treasure Map",                "formid": "003D0CD9", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: Savage Divide"]},
            {"name": "The Mire Treasure Map",                     "formid": "003D0CD7", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: The Mire"]},
            {"name": "Toxic Valley Treasure Map",                 "formid": "003D0CD6", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Region: Toxic Valley"]},
        ],
        "mode": "regional",
        "note": "Only the map matching the infestation's region will drop"
    })

    # 5. Resources — UseAll, all guaranteed
    boss_loot.append({
        "label": "Resources",
        "formid": "00893F7C",
        "subtitle": "Guaranteed drop · 4 items",
        "dropRate": 100,
        "items": [
            {"name": "Scrap-to-Stash",    "formid": "008B0D64", "sig": "UTIL", "qty": 1, "dropRate": 100},
            {"name": "Treasury Note",      "formid": "005A5443", "sig": "MISC", "qty": 1, "dropRate": 100},
            {"name": "Legendary Module",   "formid": "005652F9", "sig": "MISC", "qty": 1, "dropRate": 100},
            {"name": "Legendary Tokens",   "formid": "003F7410", "sig": "CNCY", "qty": 1, "dropRate": 100},
        ],
        "mode": "useall"
    })

    # 6. Explosives — UseAll (1 grenade + 1 mine)
    boss_loot.append({
        "label": "Explosives",
        "formid": "0088851C",
        "subtitle": "Guaranteed drop · 1 grenade + 1 mine",
        "dropRate": 100,
        "children": [
            {
                "label": "Grenades",
                "formid": "00893F79",
                "subtitle": f"Pick one · {11} items",
                "dropRate": 100,
                "items": [
                    {"name": "Cryo Grenade",             "formid": "0011002F", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Floater Gnasher Grenade",  "formid": "005A70BB", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Floater Flamer Grenade",   "formid": "005A70B6", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Floater Freezer Grenade",  "formid": "005A70BC", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Nuka Grenade",             "formid": "001BBCBC", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Nuka Quantum Grenade",     "formid": "0034E210", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Plasma Grenade",           "formid": "0011002D", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Pulse Grenade",            "formid": "00110030", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Molotov Cocktail",         "formid": "00110031", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Baseball Grenade",         "formid": "0034E212", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                    {"name": "Frag Grenade",             "formid": "0011002B", "sig": "LVLI", "qty": 1, "dropRate": round(100/11, 2)},
                ],
                "mode": "pickone"
            },
            {
                "label": "Mines",
                "formid": "00888527",
                "subtitle": "Pick one · 4 items",
                "dropRate": 100,
                "items": [
                    {"name": "Frag Mine",   "formid": "00110034", "sig": "LVLI", "qty": 1, "dropRate": 25},
                    {"name": "Pulse Mine",  "formid": "00110039", "sig": "LVLI", "qty": 1, "dropRate": 25},
                    {"name": "Cryo Mine",   "formid": "00110035", "sig": "LVLI", "qty": 1, "dropRate": 25},
                    {"name": "Plasma Mine", "formid": "00110038", "sig": "LVLI", "qty": 1, "dropRate": 25},
                ],
                "mode": "pickone"
            },
        ],
        "mode": "useall"
    })

    # 7. Contextual Ammo — 15 rounds matching equipped weapon
    boss_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F76",
        "subtitle": f"Guaranteed drop · {ammo_boss} rounds matching your equipped weapon",
        "dropRate": 100,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_boss, "dropRate": 100}],
        "note": f"Drops {ammo_boss} rounds of ammo matching the type your equipped weapon uses"
    })

    # 8. Serums — pick-one from 19, list-level 80% ChanceNone (20% chance to drop)
    serum_drop = round(100 - serum_list_cn, 1)  # 20%
    serum_items = [
        ("Adrenal Reaction Serum", "00505BD4"), ("Bird Bones Serum", "0050A5BC"),
        ("Carnivore Serum", "0050A5BF"), ("Chameleon Serum", "0050A5C6"),
        ("Eagle Eyes Serum", "0050A5C9"), ("Egg Head Serum", "0050A5CB"),
        ("Electrically Charged Serum", "0050A5CD"), ("Empath Serum", "0050A5D2"),
        ("Grounded Serum", "0050A5D0"), ("Healing Factor Serum", "0050A5D6"),
        ("Herbivore Serum", "0050A5C4"), ("Herd Mentality Serum", "0050A5E3"),
        ("Marsupial Serum", "0050A5E7"), ("Plague Walker Serum", "0050A5EA"),
        ("Scaly Skin Serum", "0050A5F0"), ("Speed Demon Serum", "0050A5F3"),
        ("Talons Serum", "0050A5F6"), ("Twisted Muscles Serum", "0050A5F9"),
        ("Unstable Isotope Serum", "0050A5FC"),
    ]
    boss_loot.append({
        "label": "Serums",
        "formid": "00888532",
        "subtitle": f"{serum_drop}% chance to drop · pick one of {len(serum_items)} serums",
        "dropRate": serum_drop,
        "items": [{"name": n, "formid": f, "sig": "ALCH", "qty": 1, "dropRate": round(serum_drop / len(serum_items), 4)} for n, f in serum_items],
        "mode": "pickone",
        "listChanceNone": serum_list_cn
    })

    # 9. Chems — FirstMatch: 25% rare tier, 75% basic tier
    rare_threshold = cn_high   # 25%
    basic_threshold = 100 - rare_threshold  # 75%
    boss_loot.append({
        "label": "Chems",
        "formid": "00888517",
        "subtitle": f"Guaranteed drop · {rare_threshold}% rare / {basic_threshold}% basic",
        "dropRate": 100,
        "children": [
            {
                "label": "Rare Chems",
                "formid": "00888519",
                "subtitle": f"Pick one · 11 items",
                "dropRate": rare_threshold,
                "items": [
                    {"name": "Berry Mentats",   "formid": "000518BB", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Grape Mentats",   "formid": "0010129A", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Orange Mentats",  "formid": "000518C5", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Bufftats",        "formid": "00058AA5", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Psychobuff",      "formid": "00058AAC", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Psychotats",      "formid": "00058AAA", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Daddy-O",         "formid": "00156D0B", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Day Tripper",     "formid": "00150729", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Fury",            "formid": "000628CA", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Calmex",          "formid": "00058AA7", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                    {"name": "Overdrive",       "formid": "00058AAD", "sig": "ALCH", "qty": 1, "dropRate": round(rare_threshold / 11, 2)},
                ],
                "mode": "pickone"
            },
            {
                "label": "Basic Chems",
                "formid": "00888518",
                "subtitle": "Pick one · 4 items",
                "dropRate": basic_threshold,
                "items": [
                    {"name": "Buffout",  "formid": "00033778", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                    {"name": "Med-X",    "formid": "00033779", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                    {"name": "Mentats",  "formid": "0003377B", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                    {"name": "Psycho",   "formid": "0003377D", "sig": "ALCH", "qty": 1, "dropRate": round(basic_threshold / 4, 2)},
                ],
                "mode": "pickone"
            },
        ],
        "mode": "firstmatch"
    })

    # 10. Stimpaks — FirstMatch tiers for quantity
    boss_loot.append({
        "label": "Stimpaks",
        "formid": "00893F83",
        "subtitle": f"Guaranteed drop · 1-3 Super Stimpaks based on RNG tier",
        "dropRate": 100,
        "items": [
            {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 3, "dropRate": cn_high,               "note": f"Top {int(cn_high)}% roll"},
            {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 2, "dropRate": cn_medium - cn_high,    "note": f"Middle {int(cn_medium - cn_high)}% roll"},
            {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 1, "dropRate": 100 - cn_medium,        "note": f"Bottom {int(100 - cn_medium)}% roll"},
        ],
        "mode": "firstmatch"
    })

    # 11. Rads (RadAway / Rad-X) — pick-one from 2
    boss_loot.append({
        "label": "Rads",
        "formid": "0088852B",
        "subtitle": "Guaranteed drop · pick one of 2 items",
        "dropRate": 100,
        "items": [
            {"name": "RadAway",  "formid": "002049B7", "sig": "LVLI", "qty": 1, "dropRate": 50},
            {"name": "Rad-X",    "formid": "002049B8", "sig": "LVLI", "qty": 1, "dropRate": 50},
        ],
        "mode": "pickone"
    })

    # 12. Scrap — FirstMatch tiers, count from GLOB (10)
    boss_loot.append({
        "label": "Scrap",
        "formid": "0088851A",
        "subtitle": f"Guaranteed drop · {scrap_count}x from one of 4 rarity tiers",
        "dropRate": 100,
        "scrapCount": scrap_count,
        "children": [
            {
                "label": "Very Rare Scrap",
                "formid": "00893F81",
                "subtitle": f"Pick one · 5 items",
                "dropRate": cn_high,
                "items": [
                    {"name": "Ballistic Fiber", "formid": "00432C9A", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                    {"name": "Black Titanium",  "formid": "00432C9D", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                    {"name": "Nuclear Material", "formid": "00432CAC", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                    {"name": "Ultracite",       "formid": "00434513", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                    {"name": "Vault Steel",     "formid": "00893FBB", "sig": "LVLI", "qty": scrap_count, "dropRate": round(cn_high / 5, 2)},
                ],
                "mode": "pickone"
            },
            {
                "label": "Rare Scrap",
                "formid": "00893F7F",
                "subtitle": f"Pick one · 7 items",
                "dropRate": cn_medium - cn_high,
                "items": [
                    {"name": "Antiseptic",    "formid": "00432C9C", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                    {"name": "Asbestos",      "formid": "00432C9B", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                    {"name": "Circuitry",     "formid": "00432CA0", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                    {"name": "Fiber Optics",  "formid": "00432CA7", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                    {"name": "Gold",          "formid": "00432CAA", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                    {"name": "Oil",           "formid": "00432CAD", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                    {"name": "Silver",        "formid": "00432CB1", "sig": "LVLI", "qty": scrap_count, "dropRate": round((cn_medium - cn_high) / 7, 2)},
                ],
                "mode": "pickone"
            },
            {
                "label": "Uncommon Scrap",
                "formid": "00893F80",
                "subtitle": f"Pick one · 9 items",
                "dropRate": cn_low - cn_medium,
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
                ],
                "mode": "pickone"
            },
            {
                "label": "Common Scrap",
                "formid": "00893F7E",
                "subtitle": f"Pick one · 13 items",
                "dropRate": 100 - cn_low,
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
                ],
                "mode": "pickone"
            },
        ],
        "mode": "firstmatch"
    })

    # 13. Bespoke (faction-specific) — condition-gated by enemy type
    boss_loot.append({
        "label": "Bespoke (Faction-Specific)",
        "formid": "00893F6F",
        "subtitle": "Conditional drop · depends on boss faction",
        "dropRate": 100,
        "children": [
            {
                "label": "Robot Boss",
                "formid": "00893F73",
                "subtitle": "Only drops from Robot faction bosses",
                "dropRate": 100,
                "conditions": ["Boss is Robot faction"],
                "items": [
                    {"name": "Fusion Cells",  "formid": "00888530", "sig": "LVLI", "qty": 1, "dropRate": 100, "note": f"Top {int(cn_high)}% roll"},
                    {"name": "Components",    "formid": "0088852F", "sig": "LVLI", "qty": 1, "dropRate": 100, "note": f"Top {int(cn_medium)}% roll"},
                    {"name": "Robot Scrap",   "formid": "00888531", "sig": "LVLI", "qty": 1, "dropRate": 100, "note": f"Top {int(cn_low)}% roll"},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Scorched Boss (Holiday)",
                "formid": "008B2319",
                "subtitle": "Only drops from Scorched bosses during seasonal events",
                "dropRate": 100,
                "conditions": ["Boss is Scorched faction", "Holiday toggle active"],
                "items": [
                    {"name": "Festive Holiday Gift", "formid": "0059B558", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Festive Scorched enabled"]},
                    {"name": "Spooky Holiday Gift",  "formid": "008B2531", "sig": "LVLI", "qty": 1, "dropRate": 100, "conditions": ["Spooky Scorched enabled"]},
                ],
                "mode": "conditional"
            },
        ],
        "mode": "conditional"
    })

    # ── Assemble final JSON ─────────────────────────────────────────────────

    output = {
        "byPage": {
            "infestations-all-rewards": {
                "name": "Infestations All Rewards",
                "questFormID": "00865FA8",
                "gmrwFormID": "00888543",
                "type": "infestations",
                "description": "World Activity — not a public event. 7 enemy factions, 51 spawn locations across all regions, only 9 active at any time. Clear the infestation and defeat the boss to earn rewards.",
                "xp": xp,
                "caps": caps,
                "mobNote": f"Mob enemies drop faction-specific bespoke items (grenades, stimpaks, components, or meat depending on creature type) and {ammo_mob} contextual ammo rounds. Support creatures drop creature-specific components (bones, meat, scrap) and {ammo_support} contextual ammo round.",
                "bossLoot": boss_loot,
                "fourStarMods": {
                    "weapons": [
                        {"name": "Tarnished", "formid": "0085B998", "effect": "Damage increases as weapon durability decreases (up to +120%)"},
                        {"name": "Satiated",  "formid": "0085B996", "effect": "Details in MGEF — exact effect TBD from PTS testing"},
                    ],
                    "armor": [
                        {"name": "Vector",  "formid": "0085B99A", "effect": "Gain 10% Bonus VATS Accuracy Against Distant Targets (up to 50% with 5 pieces)"},
                        {"name": "Raging",  "formid": "0085B997", "effect": "Details in MGEF — exact effect TBD from PTS testing"},
                        {"name": "Haulers", "formid": "0085B99B", "effect": "Details in MGEF — exact effect TBD from PTS testing"},
                    ],
                    "note": "4-star legendary mods are exclusive to Infestations. Vector value is 10% per piece (GLOB 00868BD2)."
                },
                "challenges": {
                    "lifetime": [
                        {"name": "Discover an Infestation",                     "target": 1},
                        {"name": "Complete an Infestation",                     "target": 1},
                        {"name": "Complete 10 Infestations",                    "target": 10},
                        {"name": "Complete 76 Infestations",                    "target": 76},
                        {"name": "Complete 760 Infestations",                   "target": 760},
                        {"name": "Complete an Infestation Involving Every Faction", "target": 7},
                    ],
                    "daily": [
                        {"name": "Kill an Enemy during an Infestation",            "target": 3},
                        {"name": "Kill an Enemy during an Infestation (Team)",     "target": 3},
                        {"name": "Complete an Infestation",                        "target": 1},
                        {"name": "Complete an Infestation (Team)",                 "target": 1},
                    ],
                    "weekly": [
                        {"name": "Kill an Enemy during an Infestation",            "target": 10},
                        {"name": "Kill an Enemy during an Infestation (Team)",     "target": 10},
                        {"name": "Complete 3 Infestations",                        "target": 3},
                        {"name": "Complete 3 Infestations (Team)",                 "target": 3},
                    ]
                }
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
    print(f"[HTO] Wrote {out_path}")
    print(f"[HTO] XP={data['byPage']['infestations-all-rewards']['xp']}, "
          f"Caps={data['byPage']['infestations-all-rewards']['caps']}, "
          f"Boss loot pools={len(data['byPage']['infestations-all-rewards']['bossLoot'])}")


if __name__ == "__main__":
    main()
                                                      