#!/usr/bin/env python3
"""
build_spin_wheel_json.py
========================
Builds the weapon + enemy lists for the Weapon Spin Wheel challenge generator.

Reads from:
  - WEAP_Export_*_Base.tsv   (every weapon in the game)
  - NPC_Export_*.tsv          (every NPC -- filtered to hostile enemies only)

Outputs:
  - dist/spin_wheel/spin_wheel.json

The output JSON has two top-level arrays:
  {
    "weapons": [ { "name": "...", "type": "..." }, ... ],
    "enemies": [ { "name": "...", "race": "..." }, ... ],
    "_meta": { "built": "YYYY-MM-DD", "weaponCount": N, "enemyCount": N }
  }

No external dependencies -- runs on stdlib only.

Usage:  python src/build_spin_wheel_json.py
PTS:    python src/build_spin_wheel_json.py --pts
"""

import csv
import glob as globmod
import json
import os
import re
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PTS = "--pts" in sys.argv
TSV_DIR = os.path.join(ROOT, "tsv", "pts") if PTS else os.path.join(ROOT, "tsv")
OUT_DIR = os.path.join(ROOT, "dist", "pts", "spin_wheel") if PTS else os.path.join(ROOT, "dist", "spin_wheel")

_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def _filename_date_key(path):
    base = os.path.basename(path).lower()
    m = re.search(r'_([a-z]+)_(\d{4})', base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    m2 = re.search(r'_(\d{4})(\d{2})\d{2}_\d+', base)
    if m2:
        return (int(m2.group(1)), int(m2.group(2)))
    return (0, 0)


def find_newest_tsv(pattern):
    files = globmod.glob(os.path.join(TSV_DIR, pattern))
    if not files:
        return None
    return max(files, key=lambda p: (_filename_date_key(p), os.path.getmtime(p)))


def read_tsv(filepath):
    if not filepath or not os.path.exists(filepath):
        return []
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                reader = csv.DictReader(f, delimiter="\t")
                return list(reader)
        except (UnicodeDecodeError, UnicodeError):
            continue
    print(f"  [WARN] Could not read {filepath}", file=sys.stderr)
    return []


_CUT_PATTERNS = [
    re.compile(r"^DEL_?", re.I), re.compile(r"^CUT_?", re.I),
    re.compile(r"^POST_?", re.I), re.compile(r"^ZZZ+_?", re.I),
    re.compile(r"^zz", re.I), re.compile(r"^TEST_?", re.I),
    re.compile(r"^DEBUG_?", re.I), re.compile(r"^test[A-Z]", re.I),
    re.compile(r"^PETS_", re.I), re.compile(r"^DEPRECATED_", re.I),
]


def is_cut(edid):
    if not edid:
        return True
    e = edid.strip()
    return any(p.search(e) for p in _CUT_PATTERNS)


# ===================================================================
#  WEAPONS
# ===================================================================
_WEAPON_TYPE_RULES = [
    ("WeaponTypeGrenade",       "Thrown"),
    ("WeaponTypeMine",          "Thrown"),
    ("WeaponTypeThrown",        "Thrown"),
    ("WeaponTypeTomahawk",      "Thrown"),
    ("WeaponTypeThrowingKnife", "Thrown"),
    ("WeaponTypeUnarmed",       "Unarmed"),
    ("WeaponTypeShotgun",       "Shotgun"),
    ("WeaponTypeHeavyGun",      "Heavy Gun"),
    ("WeaponTypePistol",        "Pistol"),
    ("WeaponTypeRifle",         "Rifle"),
    ("WeaponTypeMelee2H",       "Two-Handed Melee"),
    ("WeaponTypeMelee1H",       "One-Handed Melee"),
    ("WeaponTypeMeleeGeneral",  "Melee"),
]

_WEAP_SKIP_EDID = [
    re.compile(r"^cr[A-Z]"), re.compile(r"NONPLAYABLE", re.I),
    re.compile(r"^Turret", re.I), re.compile(r"^WorkshopTurret", re.I),
    re.compile(r"^HTO_cr", re.I), re.compile(r"^DailyOps_cr", re.I),
    re.compile(r"^SDOW_cr", re.I), re.compile(r"^RD\d+_cr", re.I),
    re.compile(r"^Burn_cr", re.I), re.compile(r"_TESTDMG_", re.I),
    re.compile(r"^debug_balance", re.I), re.compile(r"^Balance\s", re.I),
    re.compile(r"^Audio[Tt]emplate", re.I),
    re.compile(r"^DLC05Workshop[Ff]irework", re.I),
    re.compile(r"^XPD_AC_Race_Firework", re.I),
    re.compile(r"^XPD_AC01_CarryAndThrow", re.I),
    re.compile(r"^XPD_Pitt01_ObjMod_Carry", re.I),
    re.compile(r"^XPD_ObjMod_Carry", re.I),
    # --- Atom-shop weapon skins (cosmetic re-skins of base weapons) ---
    re.compile(r"^ATX_", re.I),
    # --- Fireworks-mine family (cut / Atom batch, not obtainable as weapons) ---
    # Removes the plain-EDID dupes; the ATX_ variants are already caught above.
    re.compile(r"^Firework_Mine_", re.I),
    # --- Cut content ---
    re.compile(r"^PlasmaBundleGrenade", re.I),   # "Plasma Grenade Bundle"
    # --- NPC / non-player junk that isn't a usable weapon ---
    re.compile(r"^PharmaBot", re.I),             # Mr. Handy NPC spray attack
    re.compile(r"^GasTrapDummy", re.I),          # trap dummy
    re.compile(r"^WeaponDecalScorched", re.I),   # cosmetic decal
    re.compile(r"^XPD_AC_MuniTurret", re.I),     # NPC turret ("Turret Machine Gun")
]

_WEAP_SKIP_NAMES = {
    "assets", "junk", "carry object template", "coolant canister",
    "mortar", "unarmed human", "unarmed power armor", "drill progress",
    "deleted", "fusion core ejector",
}

_WEAP_SKIP_KEYWORDS = {
    "WeaponTypeCamera", "WeaponTypeBinoculars",
    "WeaponTypeNoAttack", "WeaponTypeNonOffensive",
    "WeaponTypeFishingRod",
    "HandyWeaponRanged",  # robot (Mr. Handy/Gutsy) NPC weapons, not player-usable
}

# Some player weapons ship with a truncated WEAP_FULL (just the ammo/family word).
# Remap by EDID to the correct player-facing display name.
_WEAP_NAME_REMAP = {
    "RadiumRifle":       "Radium Rifle",
    "PlasmaGun":         "Plasma Gun",
    "LaserGun":          "Laser Gun",
    "RailwayRifle":      "Railway Rifle",
    "PipeGun":           "Pipe Gun",
    "DLC01LightningGun": "Tesla Rifle",
}


def classify_weapon_type(keywords_str):
    if not keywords_str:
        return "Unknown"
    for skip_kw in _WEAP_SKIP_KEYWORDS:
        if skip_kw in keywords_str:
            return None
    for kw_fragment, wtype in _WEAPON_TYPE_RULES:
        if kw_fragment in keywords_str:
            return wtype
    if "WeaponTypeEnergy" in keywords_str or "WeaponTypeLaser" in keywords_str or "WeaponTypePlasma" in keywords_str:
        return "Energy"
    if "WeaponTypeBallistic" in keywords_str:
        return "Ballistic"
    return "Unknown"


def build_weapons():
    tsv_path = find_newest_tsv("WEAP_Export_*_Base.tsv")
    if not tsv_path:
        print("  [WARN] No WEAP_Export_*_Base.tsv found", file=sys.stderr)
        return []
    print(f"  Reading weapons from: {os.path.basename(tsv_path)}")
    rows = read_tsv(tsv_path)
    print(f"  Total WEAP rows: {len(rows)}")
    seen_names = set()
    weapons = []
    for row in rows:
        edid = (row.get("WEAP_EDID") or "").strip()
        full_name = (row.get("WEAP_FULL") or "").strip()
        keywords = row.get("Keywords") or ""
        if not full_name:
            continue
        if is_cut(edid):
            continue
        if any(p.search(edid) for p in _WEAP_SKIP_EDID):
            continue
        if full_name.lower() in _WEAP_SKIP_NAMES:
            continue
        if re.match(r'^\d+\s*(DMG|DR|ER|Rad)', full_name):
            continue
        wtype = classify_weapon_type(keywords)
        if wtype is None:
            continue
        full_name = _WEAP_NAME_REMAP.get(edid, full_name)
        name_key = full_name.lower()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)
        weapons.append({"name": full_name, "type": wtype})
    weapons.sort(key=lambda w: w["name"].lower())
    print(f"  Weapons after filtering: {len(weapons)}")
    return weapons


# ===================================================================
#  ENEMIES
# ===================================================================
_HOSTILE_RACES = {
    "Alien", "Angler", "Bee Swarm", "Behemoth", "Bigfoot",
    "Bloatfly", "Bloodbug", "Blue Devil",
    "Cave Cricket", "Deathclaw", "Dust Devil",
    "FEV Hound", "Feral Ghoul", "Feral Lost", "Firefly",
    "Flatwoods Monster", "Floater", "Fog Crawler",
    "Grafton Monster", "Gulper",
    "Hermit Crab", "Honey Beast",
    "Jersey Devil", "Lesser Devil", "Liberator", "Lost",
    "Megasloth", "Mirelurk", "Mirelurk Hunter", "Mirelurk King",
    "Mirelurk Queen", "Mirelurk Spawn",
    "Mole Miner", "Mole Rat", "Mothman",
    "Ogua", "Overgrown Pollinator", "Overgrown Tank", "Overgrown Thorn",
    "Radant", "RadHog", "Radrat", "Radroach", "Radscorpion",
    "Radtoad", "RadTurkey",
    "Scorchbeast", "Scorched", "Scorchtongue",
    "Sheepsquatch", "Snallygaster", "Stingwing",
    "Storm Goliath", "Super Mutant",
    "Tick", "Trog", "Ultracite Abomination",
    "Wendigo", "Wendigo Colossus",
    "Wild Mongrel", "Wolf", "Yao Guai",
    "Zetan Drone", "Zetan Invader",
}

_MIXED_RACES = {"Assaultron", "Protectron", "Robobrain", "SentryBot"}

# Internal race ids whose player-facing name differs. The renderer uses the
# `race` field as the display name for creatures, so remap on output.
_RACE_DISPLAY_REMAP = {
    "RadTurkey": "Thrasher",
}

_ROBOT_HOSTILE_NAMES = re.compile(
    r"^(Assaultron|Protectron|Robobrain|Sentry Bot|"
    r"Corrupted|Malfunctioning|Overridden|Rogue|Haywire|"
    r"Blood Eagle|Communist|Scorched|Imposter|"
    r"Sheepsquatch Imposterling)", re.I)

_SKIP_RACES = {
    "Human", "Ghoul", "Drifter",
    "Brahmin", "Cat", "Chicken", "Dog", "Attack Dog", "Fox", "Frog",
    "Opossum", "Owl", "Pheasant", "Rabbit", "Squirrel", "Beaver",
    "Cargobot", "Vertibot", "EyeBot",
    "Bubble Turret", "Enclave Turret", "Military Turret",
    "Tripod Turret", "Workshop Turret", "Spotlight",
    "Mr. Handy", "PowerArmor", "ASAM", "SpiderBot", "RoboCat", "RoboDog",
    "Red Rocket Collectron", "Guardian Bot", "Robot", "Test01",
}

_ENEMY_SKIP_NAMES_RE = re.compile(
    r"^(AudioTemplate|AI Data Template|Infestation|Hostile Takeover|"
    r"Daily Ops Template|Lost .* Template|Drill Progress|Drill$|"
    r"DELETED|Sentry$|Mole Miner Boss$|"
    r"Registration Guard|Arena Guard|Audience$|"
    r"BleepA |BleepB |BleepC )", re.I)

_ENEMY_SKIP_TEST = re.compile(r"^\d+DR\s+\d+ER\s+\d+Rad$")
_ENEMY_SKIP_SUBS = {"friendly", "corpse", "dummy", "template", "intercom"}

_HUMAN_HOSTILE_EDID = [
    re.compile(r"Raider", re.I), re.compile(r"BloodEagle", re.I),
    re.compile(r"Cultist", re.I), re.compile(r"Communist", re.I),
    re.compile(r"Fanatic", re.I), re.compile(r"Mobster", re.I),
    re.compile(r"Competitor", re.I), re.compile(r"Showmen", re.I),
    re.compile(r"MunicipalAuditor", re.I),
    re.compile(r"Foreman(?!.*Friendly)", re.I),
]

_FRIENDLY_NAMES = {
    "grahm", "gail", "maul", "injured super mutant",
    "jerry", "tamed deathclaw",
    "polly", "jesse", "vera", "flauresca", "lotus", "aloe",
    "a.t.h.e.n.a.", "artemis", "pandora", "adelaide",
    "whitespring assaultron", "whitespring desk clerk",
    "harvestron", "reprogrammed assaultron",
    "interrogatron", "special agent",
    "arktos pharma assaultron", "campus security assaultron",
    "bounty hunter bot", "elite rapidan assaultron", "rapidan assaultron",
    "hornwright elite security", "vault 96 security assaultron",
    "u.s.s.a. assaultron",
    "private lucky", "rufus", "chompkins", "junkyard dog", "settler guard dog",
    "nightstalker", "camp counselor nia",
    "audrey stolz", "hilda stolz", "hugo stolz",
    "james oberlin", "julio", "laurence", "cassidy",
    "dr. blackburn", "david thorpe", "aldridge", "crane",
    "shadow", "lieutenant kappa",
    "meg", "ra-ra", "rocksy", "weasel", "wren", "molly",
    "knight shin", "surge", "lev", "sheena",
    "sargento", "ex-raider", "former raider", "red",
    "ae-ri", "axel", "barb", "billy", "burke", "caleb fisher",
    "carmen", "cole", "creed", "dillo", "eightball",
    "elder pepper", "eliza", "fast nicky", "fishbones",
    "frank the butcher", "frankie", "gentleman johnny weston",
    "hal gleeson", "hunter", "jack woodhouse", "jessi the hook",
    "kiyomi", "kogan", "line chef larry", "lucas",
    "maximum maddie", "munch", "needles", "nuclear don",
    "patrick", "pierce", "raf", "deathklaus",
    "the claw", "the foreman", "deathclaw handler",
    "purveyor murmrgh",
}


def build_enemies():
    tsv_path = find_newest_tsv("NPC_Export_*.tsv")
    if tsv_path and ("_PRPS" in tsv_path or "_Refs" in tsv_path):
        all_files = globmod.glob(os.path.join(TSV_DIR, "NPC_Export_*.tsv"))
        base_files = [f for f in all_files if "_PRPS" not in f and "_Refs" not in f]
        if base_files:
            tsv_path = max(base_files, key=lambda p: (_filename_date_key(p), os.path.getmtime(p)))
    if not tsv_path:
        print("  [WARN] No NPC_Export_*.tsv found", file=sys.stderr)
        return []
    print(f"  Reading enemies from: {os.path.basename(tsv_path)}")
    rows = read_tsv(tsv_path)
    print(f"  Total NPC rows: {len(rows)}")
    seen_names = set()
    enemies = []
    for row in rows:
        edid = (row.get("EDID") or "").strip()
        full_name = (row.get("FULL") or "").strip()
        race = (row.get("RNAM_Name") or "").strip()
        if not full_name or not race:
            continue
        if is_cut(edid):
            continue
        if edid.startswith("Lvl"):
            continue
        if _ENEMY_SKIP_TEST.match(full_name):
            continue
        if _ENEMY_SKIP_NAMES_RE.match(full_name):
            continue
        name_lower = full_name.lower()
        if any(sub in name_lower for sub in _ENEMY_SKIP_SUBS):
            continue
        if name_lower in _FRIENDLY_NAMES:
            continue
        if race in _SKIP_RACES:
            if race == "Human":
                if not any(p.search(edid) for p in _HUMAN_HOSTILE_EDID):
                    continue
            else:
                continue
        elif race in _MIXED_RACES:
            if not _ROBOT_HOSTILE_NAMES.search(full_name):
                continue
        elif race not in _HOSTILE_RACES:
            continue
        name_key = full_name.lower()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)
        enemies.append({"name": full_name, "race": _RACE_DISPLAY_REMAP.get(race, race)})
    enemies.sort(key=lambda e: e["name"].lower())
    print(f"  Enemies after filtering: {len(enemies)}")
    return enemies


# ===================================================================
#  MAIN
# ===================================================================
def main():
    print("=" * 60)
    print("  Building Weapon Spin Wheel JSON")
    print(f"  Mode: {'PTS' if PTS else 'LIVE'}")
    print("=" * 60)
    weapons = build_weapons()
    enemies = build_enemies()
    output = {
        "weapons": weapons,
        "enemies": enemies,
        "_meta": {
            "built": date.today().isoformat(),
            "weaponCount": len(weapons),
            "enemyCount": len(enemies),
        }
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "spin_wheel.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  Written to: {out_path}")
    print(f"  Weapons: {len(weapons)}, Enemies: {len(enemies)}")
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
