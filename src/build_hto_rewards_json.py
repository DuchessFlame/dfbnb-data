#!/usr/bin/env python3
"""
build_hto_rewards_json.py
Build HTO (Infestations) reward JSON for buffsnbrew.com.

Reads GLOB values from TSV for numeric accuracy, structures the reward tree
manually based on confirmed xEdit data. Output consumed by df-bnb-infestations.js.

Two modes:
  (default / live)  reads tsv/      -> dist/infestations/hto_rewards.json
  --pts             reads tsv/pts/  -> dist/pts/infestations/hto_rewards.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the renderer loads the right twin automatically.
"""

import json, os, sys, glob, re, csv
from pathlib import Path

PTS = "--pts" in sys.argv

# ── Paths ───────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_DIR    = _REPO_ROOT / "tsv" / ("pts" if PTS else "")
DIST_DIR   = (_REPO_ROOT / "dist" / "pts" / "infestations") if PTS \
             else (_REPO_ROOT / "dist" / "infestations")

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

TOGGLE_WARNING = "This reward is enabled or disabled at Bethesda's discretion and may not always be active."


def build_hto_rewards():
    globs = load_globs()

    def g(fid, fallback=0.0):
        return globs.get(fid, fallback)

    xp   = int(g("00888541", 5000))
    caps = int(g("00888542", 500))

    # Scrap/Stimpak/Chem FirstMatch threshold GLOBs
    cn_high   = g("008B7CDE", 25.0)   # 25 → top 25%
    cn_medium = g("008B7CDF", 50.0)   # 50 → 25-50%
    cn_low    = g("008B7CE0", 75.0)   # 75 → 50-75%

    # Boss legendary 4-star chance — GetRandomPercent <= this value
    # GLOB 00893F9E: 100.0 in May 2026 TSV = 4-star guaranteed
    cn_4star  = g("00893F9E", 100.0)

    # Scrap count GLOB
    scrap_count = int(g("00893FB3", 10.0))

    # Contextual ammo counts
    ammo_boss    = int(g("00893F8E", 15))
    ammo_mob     = int(g("00893F8F", 5))
    ammo_support = int(g("00893F90", 1))

    # Serum list-level ChanceNone = 80 → 20% chance to get a serum at all
    serum_list_cn = 80.0  # From LVCV on 00893F82

    # Stimpak list-level ChanceNone = 80 → 20% chance to get stimpaks at all
    # From list-level CN on 00893F83
    stim_list_cn = 80.0
    stim_pct = round(100 - stim_list_cn, 1)  # 20%

    # Mob/Support legendary ChanceNone GLOBs (3-star only, no 4-star cascade)
    mob_leg_cn      = g("008F2B25", 80.0)   # 80 → 20% per type
    support_leg_cn  = g("008F2B23", 90.0)   # 90 → 10% per type

    # ── Boss Loot Categories (13 pools, all UseAll independent) ──

    boss_loot = []

    # 1. Legendary — TSV: UseAll (001) + max_count=1 (GLOB 00893F9F=1.0)
    #    Degenerate waterfall (all cn_factor=1.0 → first entry always wins).
    #    Same bug pattern as PA recipe lists (skill section 14).
    #    Display as pick-one (25% each) = intended Bethesda behaviour.
    #    Each type: FirstMatch — GetRandomPercent <= cn_4star, fallback 3-star.
    #    Toggle: 00893FA0 (entry-level) + 008F2B24 (list-level boss toggle).
    leg_4star_pct = cn_4star          # 100 = guaranteed 4-star
    leg_3star_pct = 100 - cn_4star    # 0 = 3-star never drops when 4-star is 100%
    leg_type_pct = 25                 # pick-one of 4 types

    if cn_4star >= 100:
        leg_subtitle = "Guaranteed drop of one item when active · 4★ guaranteed"
    else:
        leg_subtitle = f"Guaranteed drop of one item when active · {int(cn_4star)}% chance of 4★, {int(100 - cn_4star)}% chance of 3★"

    boss_loot.append({
        "label": "Legendary Items",
        "formid": "0088851D",
        "blurb": leg_subtitle,
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            {
                "label": "Weapons (Ranged)",
                "formid": "00888524",
                "dropRate": leg_type_pct,
                "warningNote": TOGGLE_WARNING,
                "items": [
                    {"name": "4★ Legendary Ranged Weapon", "formid": "0088853C", "sig": "LGDI", "qty": 1, "dropRate": leg_4star_pct},
                    {"name": "3★ Legendary Ranged Weapon", "formid": "00417C44", "sig": "LGDI", "qty": 1, "dropRate": leg_3star_pct},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Weapons (Melee)",
                "formid": "00888522",
                "dropRate": leg_type_pct,
                "warningNote": TOGGLE_WARNING,
                "items": [
                    {"name": "4★ Legendary Melee Weapon", "formid": "0088853B", "sig": "LGDI", "qty": 1, "dropRate": leg_4star_pct},
                    {"name": "3★ Legendary Melee Weapon", "formid": "00417C48", "sig": "LGDI", "qty": 1, "dropRate": leg_3star_pct},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Power Armor",
                "formid": "00888520",
                "dropRate": leg_type_pct,
                "warningNote": TOGGLE_WARNING,
                "items": [
                    {"name": "4★ Legendary Power Armor", "formid": "0088853A", "sig": "LGDI", "qty": 1, "dropRate": leg_4star_pct},
                    {"name": "3★ Legendary Power Armor", "formid": "00605FC5", "sig": "LGDI", "qty": 1, "dropRate": leg_3star_pct},
                ],
                "mode": "firstmatch"
            },
            {
                "label": "Armor",
                "formid": "0088851E",
                "dropRate": leg_type_pct,
                "warningNote": TOGGLE_WARNING,
                "items": [
                    {"name": "4★ Legendary Armor", "formid": "00888539", "sig": "LGDI", "qty": 1, "dropRate": leg_4star_pct},
                    {"name": "3★ Legendary Armor", "formid": "00417C41", "sig": "LGDI", "qty": 1, "dropRate": leg_3star_pct},
                ],
                "mode": "firstmatch"
            },
        ],
        "mode": "pickone"
    })

    # 2. Bobbleheads — guaranteed 1x BobbleheadBox. Toggle: 00893F8A
    boss_loot.append({
        "label": "Bobbleheads",
        "formid": "00888515",
        "blurb": "Guaranteed drop · 1 item",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "items": [{"name": "Bobblehead Box", "formid": "008B0D63", "sig": "ALCH", "qty": 1, "dropRate": 100}]
    })

    # 3. Magazines — guaranteed 1x MagazineBookBox. Toggle: 00893FA6
    boss_loot.append({
        "label": "Magazines",
        "formid": "00888526",
        "blurb": "Guaranteed drop · 1 item",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "items": [{"name": "Magazine Book Box", "formid": "008B0D62", "sig": "ALCH", "qty": 1, "dropRate": 100}]
    })

    # 4. Treasure Maps — region-conditional, each pool is a mixed bag of maps.
    #    Condition determines which pool is used, but pools contain maps from
    #    multiple regions (not just the matching one).
    #    CB_SF (003D0CD8): Cranberry Bog OR The Mire → 10 SD + 5 Mire + 4 CB = 19
    #    FF    (003D0CD5): The Forest              → 10 Forest + 10 SD + 4 TV + 2 AH = 26
    #    MTN   (003D0CD9): Savage Divide           → all 35 maps from every region
    #    MTR   (003D0CD7): Ash Heap                → 10 Forest + 10 SD + 2 AH = 22
    #    TV    (003D0CD6): Toxic Valley            → 10 Forest + 10 SD + 4 TV = 24
    # Map number ranges per region (from BOOK export)
    _MAP_COUNTS = {
        "Ash Heap": 2, "Cranberry Bog": 4, "Forest": 10,
        "Mire": 5, "Savage Divide": 10, "Toxic Valley": 4,
    }

    def _map_pool(label, formid, total, regions):
        """Build a treasure map sub-pool listing every individual map."""
        items = []
        for region_name, count in regions:
            pct = round(100 / total, 2)
            for i in range(1, count + 1):
                items.append({"name": f"{region_name} Treasure Map #{i:02d}", "formid": formid, "sig": "LVLI", "qty": 1, "dropRate": pct})
        items.sort(key=lambda x: x["name"])
        return {
            "label": label,
            "formid": formid,
            "blurb": f"Guaranteed drop of one item · {total} items",
            "dropRate": 100,
            "items": items,
            "mode": "pickone"
        }

    boss_loot.append({
        "label": "Treasure Maps",
        "formid": "00888537",
        "blurb": f"Regional loot pool — rewards depend on which region the infestation is active in · 5 regions",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            _map_pool("Infestation in Cranberry Bog or The Mire", "003D0CD8", 19, [
                ("Savage Divide", 10), ("Mire", 5), ("Cranberry Bog", 4),
            ]),
            _map_pool("Infestation in The Forest", "003D0CD5", 26, [
                ("Forest", 10), ("Savage Divide", 10), ("Toxic Valley", 4), ("Ash Heap", 2),
            ]),
            _map_pool("Infestation in Savage Divide", "003D0CD9", 35, [
                ("Savage Divide", 10), ("Forest", 10), ("Mire", 5),
                ("Cranberry Bog", 4), ("Toxic Valley", 4), ("Ash Heap", 2),
            ]),
            _map_pool("Infestation in Ash Heap", "003D0CD7", 22, [
                ("Forest", 10), ("Savage Divide", 10), ("Ash Heap", 2),
            ]),
            _map_pool("Infestation in Toxic Valley", "003D0CD6", 24, [
                ("Forest", 10), ("Savage Divide", 10), ("Toxic Valley", 4),
            ]),
        ],
        "mode": "regional",
    })

    # 5. Resources — UseAll, all guaranteed. Toggle: 00893FAE
    boss_loot.append({
        "label": "Resources",
        "formid": "00893F7C",
        "blurb": "Each item rolls independently · 4 items",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "items": [
            {"name": "Scrap Kit",          "formid": "008B0D64", "sig": "UTIL", "qty": 1, "dropRate": 100},
            {"name": "Treasury Note",      "formid": "005A5443", "sig": "MISC", "qty": 1, "dropRate": 100},
            {"name": "Legendary Module",   "formid": "005652F9", "sig": "MISC", "qty": 1, "dropRate": 100},
            {"name": "Legendary Scrip",    "formid": "003F7410", "sig": "CNCY", "qty": 1, "dropRate": 100},
        ],
        "mode": "useall"
    })

    # 6. Explosives — UseAll (1 grenade + 1 mine). Toggle: 00893F95
    boss_loot.append({
        "label": "Explosives",
        "formid": "0088851C",
        "blurb": "2 reward lists · each rolled on completion",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            {
                "label": "Grenades",
                "formid": "00893F79",
                "blurb": f"Guaranteed drop of one item · {11} items",
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
                "blurb": "Guaranteed drop of one item · 4 items",
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

    # 7. Contextual Ammo — 15 rounds matching equipped weapon. Toggle: 00893F91
    boss_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F76",
        "blurb": f"Guaranteed drop · {ammo_boss} rounds matching your equipped weapon",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_boss, "dropRate": 100}],
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
        "blurb": f"Chance drop of one item · {len(serum_items)} items",
        "dropRate": serum_drop,
        "warningNote": TOGGLE_WARNING,
        "items": [{"name": n, "formid": f, "sig": "ALCH", "qty": 1, "dropRate": round(serum_drop / len(serum_items), 4)} for n, f in serum_items],
        "mode": "pickone",
        "listChanceNone": serum_list_cn
    })

    # 9–11. Aid — combined wrapper for Stimpaks + Rads + Chems.
    #   Chems is FirstMatch: 25% rare tier, 75% basic tier → flattened into 2 sub-expands.
    #   Stimpaks: 80% listChanceNone → 20% chance to drop at all.
    #   Rads: pick-one of RadAway / Rad-X, guaranteed.
    #   Toggle: 00893F8D (chems), 00893FB8 (stimpaks), 00893FA9 (rads)
    rare_threshold = cn_high   # 25%
    basic_threshold = 100 - rare_threshold  # 75%
    boss_loot.append({
        "label": "Aid",
        "formid": "00888517",
        "blurb": "4 reward lists · each rolled on completion",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            {
                "label": "Stimpaks",
                "formid": "00893F83",
                "blurb": f"Chance drop of one item · 3 items",
                "dropRate": stim_pct,
                "items": [
                    {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 3, "dropRate": round(stim_pct * cn_high / 100, 2)},
                    {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 2, "dropRate": round(stim_pct * (cn_medium - cn_high) / 100, 2)},
                    {"name": "Super Stimpak", "formid": "00117DF9", "sig": "ALCH", "qty": 1, "dropRate": round(stim_pct * (100 - cn_medium) / 100, 2)},
                ],
                "mode": "firstmatch",
                "listChanceNone": stim_list_cn
            },
            {
                "label": "Rads",
                "formid": "0088852B",
                "blurb": "Guaranteed drop of one item · 2 items",
                "dropRate": 100,
                "items": [
                    {"name": "RadAway",  "formid": "002049B7", "sig": "LVLI", "qty": 1, "dropRate": 50},
                    {"name": "Rad-X",    "formid": "002049B8", "sig": "LVLI", "qty": 1, "dropRate": 50},
                ],
                "mode": "pickone"
            },
            {
                "label": "Rare Chems",
                "formid": "00888519",
                "blurb": f"Chance drop of one item · 11 items",
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
                "blurb": "Chance drop of one item · 4 items",
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
    })

    # 12. Scrap — FirstMatch tiers, count from GLOB (10). Toggle: 00893FB4
    boss_loot.append({
        "label": "Scrap",
        "formid": "0088851A",
        "blurb": "Guaranteed drop of one item · 4 reward tiers",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "scrapCount": scrap_count,
        "children": [
            {
                "label": "Very Rare Scrap",
                "formid": "00893F81",
                "blurb": f"Chance drop of one item · 5 items",
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
                "blurb": f"Chance drop of one item · 7 items",
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
                "blurb": f"Chance drop of one item · 9 items",
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
                "blurb": f"Chance drop of one item · 13 items",
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

    # 13. Bespoke (faction-specific) — condition-gated by enemy type.
    #     Toggle: 00893F86 (global) + 00893F85 (boss)
    boss_loot.append({
        "label": "Bespoke (Faction-Specific)",
        "formid": "00893F6F",
        "blurb": "Conditional drop · depends on boss faction",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            {
                "label": "Robot Boss",
                "formid": "00893F73",
                "blurb": "Only drops from Robot faction bosses",
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
                "blurb": "Only drops from Scorched bosses during seasonal events",
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

    # ── Mob Loot (3 pools: Legendary + Bespoke + Contextual Ammo) ──────────

    mob_loot = []

    # Mob Legendary — UseAll + max_count=1 (waterfall), 4 types, 3-star only
    # ChanceNone 80 on each sub-list entry = 20% per type
    # Toggle: 00893FA0 (entry-level) + 008F2B22 (list-level mob toggle)
    mob_leg_pct = round(100 - mob_leg_cn, 1)  # 20%
    mob_leg_types = [
        ("3★ Legendary Ranged Weapon", "008F2B1C", "00417C44"),
        ("3★ Legendary Melee Weapon",  "008F2B1A", "00417C48"),
        ("3★ Legendary Power Armor",   "008F2B1D", "00605FC5"),
        ("3★ Legendary Armor",         "008F2B1E", "00417C41"),
    ]
    cum_fail = 1.0
    mob_leg_items = []
    for name, fid, lgdi in mob_leg_types:
        rate = (mob_leg_pct / 100) * cum_fail * 100
        mob_leg_items.append({
            "name": name, "formid": lgdi, "sig": "LGDI", "qty": 1,
            "dropRate": round(rate, 2),
        })
        cum_fail *= (1 - mob_leg_pct / 100)
    mob_leg_total = round(sum(i["dropRate"] for i in mob_leg_items), 2)

    mob_loot.append({
        "label": "Legendary Items",
        "formid": "008F2B1B",
        "blurb": f"Waterfall — at most one drop · {len(mob_leg_items)} items · 3★ only",
        "dropRate": mob_leg_total,
        "warningNote": TOGGLE_WARNING,
        "items": mob_leg_items,
        "mode": "waterfall"
    })

    # Mob Bespoke — conditional on faction. Toggle: 00893F86 + 00893F87
    mob_loot.append({
        "label": "Bespoke (Faction-Specific)",
        "formid": "00893F72",
        "blurb": "Conditional drop · depends on mob faction",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            {
                "label": "Humanoid Mobs",
                "formid": "00893F71",
                "blurb": "Grenades + Stimpaks",
                "dropRate": 100,
                "conditions": ["Mob is humanoid faction (Blood Eagles, Cultists, Ghouls, Scorched, Super Mutants)"],
            },
            {
                "label": "Robot Mobs",
                "formid": "00893F73",
                "blurb": "Fusion Cells + Components + Scrap",
                "dropRate": 100,
                "conditions": ["Mob is Robot faction"],
            },
            {
                "label": "Floater Mobs",
                "formid": "00893F70",
                "blurb": "Pus Sac + Components",
                "dropRate": 100,
                "conditions": ["Mob is Floater"],
            },
            {
                "label": "Mothman Hatchling",
                "formid": "0085CDB7",
                "blurb": "Components",
                "dropRate": 100,
                "conditions": ["Mob is Cultist Mothman Hatchling"],
            },
        ],
        "mode": "conditional"
    })

    # Mob Contextual Ammo — 5 rounds. Toggle: 00893F92
    mob_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F77",
        "blurb": f"Guaranteed drop · {ammo_mob} rounds matching your equipped weapon",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_mob, "dropRate": 100}],
    })

    # ── Support Loot (3 pools: Legendary + Bespoke + Contextual Ammo) ────

    support_loot = []

    # Support Legendary — UseAll + max_count=1 (waterfall), 4 types, 3-star only
    # ChanceNone 90 on each sub-list entry = 10% per type
    # Toggle: 00893FA0 (entry-level) + 008F2B21 (list-level support toggle)
    sup_leg_pct = round(100 - support_leg_cn, 1)  # 10%
    sup_leg_types = [
        ("3★ Legendary Ranged Weapon", "008F2B1F", "00417C44"),
        ("3★ Legendary Melee Weapon",  "008F2B20", "00417C48"),
        ("3★ Legendary Power Armor",   "008F2B19", "00605FC5"),
        ("3★ Legendary Armor",         "008F2B17", "00417C41"),
    ]
    cum_fail = 1.0
    sup_leg_items = []
    for name, fid, lgdi in sup_leg_types:
        rate = (sup_leg_pct / 100) * cum_fail * 100
        sup_leg_items.append({
            "name": name, "formid": lgdi, "sig": "LGDI", "qty": 1,
            "dropRate": round(rate, 2),
        })
        cum_fail *= (1 - sup_leg_pct / 100)
    sup_leg_total = round(sum(i["dropRate"] for i in sup_leg_items), 2)

    support_loot.append({
        "label": "Legendary Items",
        "formid": "008F2B18",
        "blurb": f"Waterfall — at most one drop · {len(sup_leg_items)} items · 3★ only",
        "dropRate": sup_leg_total,
        "warningNote": TOGGLE_WARNING,
        "items": sup_leg_items,
        "mode": "waterfall"
    })

    # Support Bespoke — conditional on creature type. Toggle: 00893F86 + 00893F88
    support_loot.append({
        "label": "Bespoke (Creature-Specific)",
        "formid": "00893F74",
        "blurb": "Conditional drop · depends on creature type",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "children": [
            {"label": "Attack Dog",    "formid": "00863222", "blurb": "Bones + Meat", "dropRate": 100},
            {"label": "EyeBot",        "formid": "00865AF8", "blurb": "Components + Scrap", "dropRate": 100},
            {"label": "Liberator",     "formid": "0085B657", "blurb": "Trinkets + Components", "dropRate": 100},
            {"label": "Mole Rat",      "formid": "0085DC58", "blurb": "Components + Meat", "dropRate": 100},
            {"label": "Mutant Hound",  "formid": "0086242D", "blurb": "Bones + Meat", "dropRate": 100},
            {"label": "Wolf",          "formid": "00863223", "blurb": "Bones + Meat", "dropRate": 100},
        ],
        "mode": "conditional"
    })

    # Support Contextual Ammo — 1 round. Toggle: 00893F93
    support_loot.append({
        "label": "Contextual Ammo",
        "formid": "00893F78",
        "blurb": f"Guaranteed drop · {ammo_support} round matching your equipped weapon",
        "dropRate": 100,
        "warningNote": TOGGLE_WARNING,
        "items": [{"name": "Contextual Ammo", "formid": "0085A375", "sig": "LVLI", "qty": ammo_support, "dropRate": 100}],
    })

    # ── Assemble final JSON ─────────────────────────────────────────────────

    output = {
        "byPage": {
            "infestations-all-rewards": {
                "name": "Infestations All Rewards",
                "questFormID": "00865FA8",
                "gmrwFormID": "00888543",
                "type": "infestations",
                "description": "World Activity — not a public event. 7 enemy factions, 36 spawn locations across all regions, only 5 active at any time. Clear the infestation and defeat the boss to earn rewards.",
                "xp": xp,
                "caps": caps,
                "bossLoot": boss_loot,
                "mobLoot": mob_loot,
                "supportLoot": support_loot,
                "fourStarMods": {
                    "weapons": [
                        {"name": "Tarnished", "formid": "0085B998", "effect": "Damage increases as weapon durability decreases (up to +120%)"},
                        {"name": "Satiated",  "formid": "0085B996", "effect": "Kills Restore Hunger and Thirst"},
                    ],
                    "armor": [
                        {"name": "Vector",  "formid": "0085B99A", "effect": "Gain 10% Bonus V.A.T.S. Accuracy Against Distant Targets (up to 50% with 5 pieces)"},
                        {"name": "Raging",  "formid": "0085B997", "effect": "Upon being hit, deal +5% Damage for 10 seconds per piece (up to +25% with 5 pieces)"},
                        {"name": "Hauler's", "formid": "0085B99B", "effect": "Increases Carry Capacity by 30"},
                    ],
                    "powerArmor": [
                        {"name": "Vector",   "formid": "0085B99D", "effect": "Gain 10% Bonus VATS Accuracy Against Distant Targets (up to 50% with 5 pieces)"},
                        {"name": "Raging",   "formid": "0085B999", "effect": "Upon being hit, deal +5% Damage for 10 seconds per piece (up to +25% with 5 pieces)"},
                        {"name": "Hauler's", "formid": "0085B99C", "effect": "Increases Carrying Capacity by 30"},
                    ],
                    "note": "4-star legendary mods are exclusive to Infestations. Vector value is 10% per piece (GLOB 00868BD2). Satiated: Human = Kills Restore Hunger and Thirst, Ghoul = Kills Restore Feral. Tarnished perk effect still active — old spell system deprecated in May 2026."
                },
                "challenges": {
                    "lifetime": [
                        {"name": "Discover an Infestation",                        "target": 1,   "reward": "Title: Harbinger",    "prereq": "Emerge from Vault 76"},
                        {"name": "Complete an Infestation",                        "target": 1,   "reward": "Title: Eliminator",   "prereq": "Emerge from Vault 76"},
                        {"name": "Complete an Infestation",                        "target": 10,  "reward": "Title: Vanquisher",   "prereq": "Complete an Infestation (×1)"},
                        {"name": "Complete an Infestation",                        "target": 76,  "reward": "Title: Annihilator",  "prereq": "Complete an Infestation (×10)"},
                        {"name": "Complete an Infestation",                        "target": 760, "reward": "Title: Conquerer",    "prereq": "Complete an Infestation (×76)"},
                        {"name": "Complete an Infestation Involving Every Faction", "target": 7,   "reward": "Title: Terminator",
                         "factions": ["Blood Eagles", "Communists", "Cultists", "Mole Miners", "Robots", "Scorched", "Super Mutants"]},
                    ],
                    "daily": [
                        {"name": "Kill an Enemy during an Infestation",            "target": 3,  "reward": "Score"},
                        {"name": "Kill an Enemy during an Infestation while on a Team", "target": 3, "reward": "Score"},
                        {"name": "Complete an Infestation",                        "target": 1,  "reward": "Score"},
                        {"name": "Complete an Infestation while on a Team",        "target": 1,  "reward": "Score"},
                    ],
                    "weekly": [
                        {"name": "Kill an Enemy during an Infestation",            "target": 10, "reward": "Score"},
                        {"name": "Kill an Enemy during an Infestation while on a Team", "target": 10, "reward": "Score"},
                        {"name": "Complete an Infestation",                        "target": 3,  "reward": "Score"},
                        {"name": "Complete an Infestation while on a Team",        "target": 3,  "reward": "Score"},
                    ]
                }
            }
        }
    }

    return output


def main():
    mode = "PTS" if PTS else "LIVE"
    print(f"[HTO] Mode: {mode}  TSV_DIR={TSV_DIR}  DIST_DIR={DIST_DIR}")
    os.makedirs(DIST_DIR, exist_ok=True)
    data = build_hto_rewards()
    if PTS:
        data["isPts"] = True
    out_path = DIST_DIR / "hto_rewards.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[HTO] Wrote {out_path}")
    page = data['byPage']['infestations-all-rewards']
    print(f"[HTO] XP={page['xp']}, Caps={page['caps']}, Boss loot pools={len(page['bossLoot'])}")


if __name__ == "__main__":
    main()  # entry point
