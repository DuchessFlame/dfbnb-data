#!/usr/bin/env python3
"""
build_unique_weapons_json.py
============================
Generates dist/unique_weapons/unique_weapons.json from xEdit TSV exports.

Reads the LL_Weapon_Unique_All leveled list and resolves every unique/named
weapon in Fallout 76, pulling base stats from WEAP, custom mods from the
ObjectTemplate + OMOD chain, enchantments from ENCH, and obtain info from
GMRW/QUEST linkage.

Inputs (from tsv/ in the repo):
  LVLI_Export_*_LVLI_Entries.tsv
  WEAP_Export_*_Base.tsv
  WEAP_Export_*_DNAM.tsv
  WEAP_Export_*_ObjectTemplate.tsv
  OMOD_Export_*.tsv              (base — NOT Properties)
  OMOD_Export_*_Properties.tsv
  ENCH_Export_*.tsv
  GMRW_Export_*.tsv
  QUEST_Export_*.tsv

Output:
  dist/unique_weapons/unique_weapons.json   (LIVE channel)
  dist/pts/unique_weapons/unique_weapons.json   (PTS channel)

Usage:
  python build_unique_weapons_json.py                   # builds both channels
  python build_unique_weapons_json.py --channel live
  python build_unique_weapons_json.py --channel pts
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SRC_DIR   = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
TSV_DIR   = REPO_ROOT / "tsv"
PTS_DIR   = TSV_DIR / "pts"

sys.path.insert(0, str(SRC_DIR))
from patchlog_utils import write_empty_patchlog_feed


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def strip_q(s):
    """Strip surrounding quotes and whitespace."""
    return str(s or "").strip().strip('"').strip()


def newest(pattern, tsv_dir):
    """Return the newest file matching glob pattern by mtime, or None."""
    files = sorted(glob.glob(str(tsv_dir / pattern)), key=os.path.getmtime)
    return files[-1] if files else None


def read_tsv(path):
    """Read a TSV file and return a list of OrderedDicts."""
    with open(path, "r", encoding="latin-1", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def safe_int(v):
    """Parse an int from a string, returning None on failure."""
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def safe_float(v):
    """Parse a float from a string, returning None on failure."""
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def is_cut(edid):
    """Return True if the EDID indicates cut/deleted content."""
    u = strip_q(edid).upper()
    return (u.startswith("ZZZ") or u.startswith("CUT")
            or u.startswith("DEL"))


def is_creature(edid):
    """Return True if the EDID is a creature variant (starts with 'cr')."""
    e = strip_q(edid)
    return e.startswith("cr") and len(e) > 2 and e[2].isupper()


def norm_formid(fid):
    """Normalise a FormID to 8-char uppercase hex."""
    fid = str(fid).strip().upper().replace("0X", "")
    return fid[-8:].zfill(8) if fid else ""


# ---------------------------------------------------------------------------
#  Parse reference strings
# ---------------------------------------------------------------------------

def parse_ref(ref_str):
    """Parse a reference string like '008DB72C:SomeEdid:LVLI' into (formid, edid, sig)."""
    ref_str = strip_q(ref_str)
    if not ref_str:
        return None, None, None
    parts = ref_str.split(":")
    if len(parts) >= 3:
        return norm_formid(parts[0]), parts[1], parts[2]
    elif len(parts) == 2:
        return norm_formid(parts[0]), parts[1], ""
    return norm_formid(parts[0]), "", ""


def parse_omod_ref(include_mod_str):
    """
    Parse an Include_Mod string like:
      mod_Custom_AllRise "All Rise Custom Mod" [OMOD:0047187E]
    Returns (edid, display_name, formid) or (None, None, None).
    """
    s = strip_q(include_mod_str)
    if not s:
        return None, None, None
    # Extract EDID (first token before space or quote)
    edid_match = re.match(r'(\S+)', s)
    edid = edid_match.group(1) if edid_match else None
    # Extract display name in quotes
    name_match = re.search(r'"([^"]*)"', s)
    display_name = name_match.group(1) if name_match else None
    # Extract FormID from [OMOD:XXXXXXXX]
    fid_match = re.search(r'\[OMOD:([0-9A-Fa-f]+)\]', s)
    formid = norm_formid(fid_match.group(1)) if fid_match else None
    return edid, display_name, formid


# ---------------------------------------------------------------------------
#  Ammo name humaniser
# ---------------------------------------------------------------------------

AMMO_NAMES = {
    "Ammo10mm":                "10mm",
    "AmmoFusionCell":          "Fusion Cell",
    "AmmoFusionCore":          "Fusion Core",
    "AmmoPlasmaCartridge":     "Plasma Cartridge",
    "AmmoShotgunShell":        "Shotgun Shell",
    "Ammo308":                 ".308",
    "Ammo50":                  ".50",
    "Ammo556":                 "5.56mm",
    "Ammo44":                  ".44",
    "Ammo45":                  ".45",
    "Ammo5mm":                 "5mm",
    "AmmoMissile":             "Missile",
    "AmmoMiniNuke":            "Mini Nuke",
    "AmmoCrossbow":            "Crossbow Bolt",
    "Ammo2mmEC":               "2mm EC",
    "AmmoCannonball":          "Cannonball",
    "AmmoFlamer":              "Flamer Fuel",
    "AmmoGamma":               "Gamma Round",
    "AmmoSyringer":            "Syringer Ammo",
    "AmmoCryoCell":            "Cryo Cell",
    "AmmoRailway":             "Railway Spike",
    "AmmoHarpoon":             "Harpoon",
    "Ammo40mmGrenade":         "40mm Grenade",
    "AmmoPepperShaker":        "Pepper Shaker Ammo",
    "AmmoUltracite10mm":       "Ultracite 10mm",
    "AmmoUltraciteFusionCell": "Ultracite Fusion Cell",
    "AmmoUltracitePlasmaCartridge": "Ultracite Plasma Cartridge",
    "AmmoUltraciteShotgunShell": "Ultracite Shotgun Shell",
    "AmmoUltracite308":        "Ultracite .308",
    "AmmoUltracite50":         "Ultracite .50",
    "AmmoUltracite556":        "Ultracite 5.56mm",
    "AmmoUltracite44":         "Ultracite .44",
    "AmmoUltracite45":         "Ultracite .45",
    "AmmoUltracite5mm":        "Ultracite 5mm",
    "AmmoUltracite2mmEC":      "Ultracite 2mm EC",
}


def humanise_ammo(dnam_ammo):
    """
    Parse DNAM_Ammo in the xEdit export format:
      AmmoFusionCell "Fusion Cell" [AMMO:000C1897]
    and return the quoted display name ('Fusion Cell').
    Falls back to EDID parsing if the quoted name is missing.

    NOTE: Do NOT strip_q the input — we need the inner quotes intact.
    """
    s = str(dnam_ammo or "").strip()
    if not s or s.startswith("NULL"):
        return None
    # Try to extract the quoted display name first (most reliable)
    quoted = re.search(r'"([^"]+)"', s)
    if quoted:
        return quoted.group(1)
    # Extract the EDID (first token)
    edid = s.split()[0] if s else s
    # Check lookup
    if edid in AMMO_NAMES:
        return AMMO_NAMES[edid]
    # Fallback: strip 'Ammo' prefix and CamelCase-split
    name = re.sub(r'^Ammo', '', edid)
    name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', name)
    name = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', ' ', name)
    return name.strip() if name.strip() else edid


# ---------------------------------------------------------------------------
#  Weapon type classification
# ---------------------------------------------------------------------------

def classify_weapon_type(dnam_row, keywords_str):
    """Classify a weapon using keywords and DNAM_WeaponType."""
    kw = strip_q(keywords_str).lower()
    wt = strip_q(dnam_row.get("DNAM_WeaponType", "")).lower() if dnam_row else ""

    # Keyword-based first (more reliable)
    if "weapontypebow" in kw:
        return "Bow"
    if "weapontypegrenade" in kw or "weapontypemine" in kw:
        return "Explosive"
    if "weapontypeshotgun" in kw:
        return "Shotgun"
    if "weapontypeheavygun" in kw:
        return "Heavy Gun"
    if "weapontypepistol" in kw:
        return "Pistol"
    if "weapontyperifle" in kw:
        return "Rifle"

    # DNAM_WeaponType fallback
    if wt:
        melee_types = [
            "onehandsword", "twohandsword", "onehandaxe", "twohandaxe",
            "onehandmace", "twohandmace", "onehanddagger", "staff",
            "handtohandmelee", "onehandkatana",
        ]
        for mt in melee_types:
            if mt in wt:
                return "Melee"
        if "gun" in wt:
            return "Rifle"

    # Keyword-based melee fallback
    if "weapontypemelee" in kw or "weapontypeunarmed" in kw or "weapontype1h" in kw or "weapontype2h" in kw:
        return "Melee"

    return wt.title() if wt else "Unknown"


# ---------------------------------------------------------------------------
#  Obtain info — hardcoded fallback map
# ---------------------------------------------------------------------------

OBTAIN_FALLBACK = {
    "AllRise": "Quest: Mayor for a Day",
    "PerfectStorm": "Quest: Cold Case",
    "BlackDiamond": "Quest: Flavors of Mayhem",
    "Daisycutter": "Quest: An Organic Solution",
    "TheFixer": "Quest: The Fixer (Encryptid questline)",
    "Fixer": "Quest: The Fixer (Encryptid questline)",
    "VoiceOfSet": "Quest: Prototypical Problems (Order of Mysteries)",
    "BladeOfBastet": "Quest: Forging a Legend (Order of Mysteries)",
    "SomersetSpecial": "Quest: The Wayward",
    "SlugBuster": "Quest: Waste Management (Vault 79)",
    "SoleSurvivor": "Quest: One of Us (Enclave questline)",
    "FinalWord": "Vendor: Beckett (companion quest)",
    "FactFinder": "Vendor: Beckett (companion quest)",
    "BunkerBuster": "Quest: One of Us (Enclave questline)",
    "CircuitBreaker": "Event: Invaders from Beyond",
    "GrandFinale": "Event: Invaders from Beyond",
    "OldGuard": "Event: Nuclear Winter (legacy)",
    "NightLight": "Survival mode reward (legacy)",
    "WhistleInTheDark": "Survival mode reward (legacy)",
    "TheActionHero": "Survival mode reward (legacy)",
    "ResoluteVeteran": "Survival mode reward (legacy)",
    "CommandersCharge": "Survival mode reward (legacy)",
    "CrushingBlow": "Survival mode reward (legacy)",
    "AcceptableOverkill": "Survival mode reward (legacy)",
    "SaltOfTheEarth": "Survival mode reward (legacy)",
    "DisorderlyConduct": "Survival mode reward (legacy)",
    "Kingfisher": "Survival mode reward (legacy)",
    "TheGuarantee": "Survival mode reward (legacy)",
    "UnstoppableMonster": "Survival mode reward (legacy) / Purveyor Murmrgh",
    "TheQuickFix": "Survival mode reward (legacy)",
    "MechanicsBestFriend": "Survival mode reward (legacy)",
    "MedicalMalpractice": "Survival mode reward (legacy)",
    "FaceBreaker": "Survival mode reward (legacy)",
    "TheVATSUnknown": "Quest: Overseer's Mission (Vault 79 raid)",
    "Oathbreaker": "Daily Ops reward",
    "BurningLove": "Treasure Hunter event reward",
    "Pyrolyzer": "Quest: Tracking Unknowns",
    "CivilUnrest": "Quest: Trade Secrets",
    "HolyFire": "Quest: Reformation (Brotherhood of Steel)",
    "EidersMark": "Quest: The Best Defense (Brotherhood of Steel)",
    "Nailer": "World drop: Deathclaw Island / various locations",
    "TheGutter": "World drop: The Burrows / various locations",
    "CamdenWhacker": "Camden Park daily quests",
    "MeteoriteSword": "Quest: Lode Baring",
    "NukaLauncher": "Quest: Overseer, Overseen (Vault 79)",
    "LoveTap": "Treasure Hunter event reward",
    "CrowdControl": "Treasure Hunter event reward",
    "DoctorsOrders": "Treasure Hunter event reward",
    "FoundationsVengeance": "Treasure Hunter event reward",
    "PiratePunch": "Treasure Hunter event reward",
    "ToneDeath": "Treasure Hunter event reward",
    "MindOverMatter": "Treasure Hunter event reward",
    "RatBat": "Treasure Hunter event reward",
    "TheDebilitator": "Treasure Hunter event reward",
    "TheFarmhand": "Treasure Hunter event reward",
    "WesternSpirit": "Treasure Hunter event reward",
    "WhackerSmacker": "Treasure Hunter event reward",
    "GunthersRevolver": "Quest: Gunther's Gold (Atlantic City)",
    "ColdShoulder": "Expedition: Atlantic City",
    "TicketToRevenge": "Expedition: Atlantic City",
    "RedTerror": "Score Season reward",
    "SuperStimpike": "Score Season reward",
    "HeadHunter": "Score Season reward",
    "V63LaserCarbine": "Quest: Skyline Valley",
    "V63GatlingLaser": "Quest: Skyline Valley",
    "CosmicKnife": "Treasure Hunter event reward",
    "IceBreaker": "Score Season reward",
    "PiercingLove": "Score Season reward",
    "ShatteredGrounds": "Event: Burning Springs",
    "Stormcutter": "Quest: Skyline Valley",
    "RelicReaper": "Score Season reward",
    "V63ShockBaton": "Quest: Skyline Valley",
    "SuperSlasher": "Quest: Shadow over Appalachia",
    "BoilingPoint": "Treasure Hunter event reward",
    "LicketySplit": "Treasure Hunter event reward",
    "ResolveBreaker": "Treasure Hunter event reward",
    "StrikeBreaker": "Treasure Hunter event reward",
    "Valkyrie": "Treasure Hunter event reward",
    "Cauterizer": "Quest: Raids (Enclave Expeditions)",
    "DrillFist": "Quest: Raids (Enclave Expeditions)",
    "CryptidJawboneKnife": "Quest: Cryptids of Appalachia",
    "BlueRidgeBrandingIron": "Event: Riding Shotgun",
    "CultistPiercer": "Quest: Cult of the Mothman",
    "LucasSwitchblade": "Quest: Atlantic City",
    "OguaGauntlet": "Quest: Cryptids of Appalachia",
    "UltraciteTerrorSword": "Quest: Ultracite Terror (Brotherhood)",
    "MeadowBreezeSprayer": "Quest: Shadow over Appalachia",
    "PrototypeABX03": "Quest: Shadow over Appalachia",
    "TheKabloom": "Quest: Raids (Enclave Expeditions)",
    "ThePeaceMaker": "Treasure Hunter event reward",
    "EldersMark": "Quest: The Best Defense (Brotherhood of Steel)",
    "Luca": "Quest: Atlantic City",
    "AnchorageAce": "Score Season reward (Anchorage Ace)",
    "SilencedSMG": "Score Season reward (Anchorage Ace)",
    "MutatedEvents": "Mutated public events reward",
    "AutoAxe": "Quest: Shadow over Appalachia",
    "Abraxo": "Quest: Shadow over Appalachia",
}


def match_obtain_fallback(edid):
    """Try to match an EDID against the fallback obtain map (substring match)."""
    e = strip_q(edid)
    for key, val in OBTAIN_FALLBACK.items():
        if key.lower() in e.lower():
            return val
    return None


# ---------------------------------------------------------------------------
#  Base weapon name extraction
# ---------------------------------------------------------------------------

# Common weapon EDID patterns → display names
BASE_WEAPON_NAMES = {
    "10mmSMG":               "10mm Submachine Gun",
    "10mmPistol":            "10mm Pistol",
    "44":                    ".44 Pistol",
    "AssaultRifle":          "Assault Rifle",
    "AutoGrenadeLauncher":   "Auto Grenade Launcher",
    "BaseballBat":           "Baseball Bat",
    "Blade":                 "Blade",
    "BoardWeapon":           "Board",
    "BowCrossbow":           "Crossbow",
    "BowLongbow":            "Bow",
    "BowShortbow":           "Short Bow",
    "Broadsider":            "Broadsider",
    "Chainsaw":              "Chainsaw",
    "CombatKnife":           "Combat Knife",
    "CombatRifle":           "Combat Rifle",
    "CombatShotgun":         "Combat Shotgun",
    "CryolatorWeapon":       "Cryolator",
    "DeathTambo":            "Death Tambo",
    "DoubleBarrelShotgun":   "Double-Barrel Shotgun",
    "DrillWeapon":           "Drill",
    "Fatman":                "Fat Man",
    "Flamer":                "Flamer",
    "GatlingGun":            "Gatling Gun",
    "GatlingLaser":          "Gatling Laser",
    "GatlingPlasma":         "Gatling Plasma",
    "GaussRifle":            "Gauss Rifle",
    "GaussPistol":           "Gauss Pistol",
    "GaussShotgun":          "Gauss Shotgun",
    "GaussMinigun":          "Gauss Minigun",
    "GolfClubWeapon":        "Golf Club",
    "Handmade":              "Handmade Rifle",
    "HarpoonGun":            "Harpoon Gun",
    "HuntingRifle":          "Hunting Rifle",
    "HuntingShotgun":        "Pump-Action Shotgun",
    "LaserGun":              "Laser Gun",
    "LeverActionRifle":      "Lever-Action Rifle",
    "Machete":               "Machete",
    "Minigun":               "Minigun",
    "MissileLauncher":       "Missile Launcher",
    "M79GrenadeLauncher":    "M79 Grenade Launcher",
    "PepperShaker":          "Pepper Shaker",
    "PipeGun":               "Pipe Gun",
    "PipeBoltAction":        "Pipe Bolt-Action",
    "PipeRevolver":          "Pipe Revolver",
    "PlasmaGun":             "Plasma Gun",
    "PowerFist":             "Power Fist",
    "PumpActionShotgun":     "Pump-Action Shotgun",
    "RadiumRifle":           "Radium Rifle",
    "Railway":               "Railway Rifle",
    "RevolutionSword":       "Revolutionary Sword",
    "Ripper":                "Ripper",
    "SledgeHammer":          "Sledgehammer",
    "SingleActionRevolver":  "Single Action Revolver",
    "SkiSword":              "Ski Sword",
    "SMG":                   "Submachine Gun",
    "SniperRifle":           "Sniper Rifle",
    "Spear":                 "Spear",
    "SuperSledge":           "Super Sledge",
    "SwitchBlade":           "Switchblade",
    "Sword":                 "Sword",
    "TeslaRifle":            "Tesla Rifle",
    "TheUltraLight":         "Ultracite Laser",
    "UltraciteLaser":        "Ultracite Laser",
    "WarDrum":               "War Drum",
    "WesternRevolver":       "Western Revolver",
    "NeedleSMG":             "Needle Submachine Gun",
    "V63LaserRifle":         "V63 Laser Carbine",
    "V63GatlingLaser":       "V63 Gatling Laser",
    "V63ShockBaton":         "V63 Shock Baton",
    "NukaLauncher":          "Nuka Launcher",
}


def resolve_base_weapon_name(weap_edid, weap_full):
    """Return the base (generic) weapon name from a WEAP EDID or FULL."""
    e = strip_q(weap_edid)
    # Try direct match on the whole EDID
    if e in BASE_WEAPON_NAMES:
        return BASE_WEAPON_NAMES[e]
    # Try matching each known base name as a substring
    for key, name in BASE_WEAPON_NAMES.items():
        if key in e:
            return name
    # Fallback to WEAP_FULL
    full = strip_q(weap_full)
    return full if full else e


def camelcase_to_display(s):
    """Convert CamelCase to 'Camel Case'."""
    s = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', s)
    s = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', ' ', s)
    s = s.replace('_', ' ')
    return re.sub(r'\s+', ' ', s).strip()


def extract_unique_name_from_lvli(lvli_edid):
    """
    Extract a unique weapon name from an LVLI EDID like:
      LL_Weapon_Ranged_MissileLauncher_BunkerBuster → Bunker Buster
      LL_Weapon_Melee_SuperSledge_AllRise → All Rise
      LL_Weapon_Ranged_EldersMark → Elder's Mark
      RD01_LL_Weapon_Melee_Cauterizer → Cauterizer
      LL_MutatedEvents_Rewards_Weapon_MissileLauncher → None (generic, no unique tail)
    """
    edid = strip_q(lvli_edid)
    if not edid:
        return None

    # Known base weapon EDID fragments (used to detect where the base name ends
    # and the unique name starts)
    base_fragments = sorted(BASE_WEAPON_NAMES.keys(), key=len, reverse=True)

    for frag in base_fragments:
        idx = edid.find(frag)
        if idx >= 0:
            after = edid[idx + len(frag):]
            # Must have an underscore separator followed by unique name
            if after.startswith("_") and len(after) > 1:
                tail = after[1:]  # strip the underscore
                return camelcase_to_display(tail)
            # No unique tail — this is a generic weapon LVLI
            return None

    # No base weapon fragment found — the entire last segment might be the name.
    # Pattern: LL_Weapon_{Range}_{UniqueName}  or  RD01_LL_Weapon_{Range}_{Name}
    parts = edid.split("_")
    # Remove common prefixes
    stripped = [p for p in parts
                if p not in ("LL", "RD01", "BS01", "MQ02", "Invention",
                             "Weapon", "Ranged", "Melee", "MutatedEvents",
                             "Rewards", "P62")]
    if stripped:
        return camelcase_to_display(stripped[-1])
    return None


def extract_unique_name_from_omod(custom_mods_data, omod_props_dict):
    """
    Extract the unique weapon display name from OMOD CustomItemName keyword.
    The OMOD Properties contain rows like:
      PropertyName=Keywords, Value1=CustomItemName_AllRise "..." [KYWD:...]
    Parse the keyword EDID to get "All Rise".
    Also tries the OMOD FULL name (stripping " Custom Mod" suffix).
    """
    for mod in custom_mods_data:
        mod_fid = mod.get("formId")
        if not mod_fid:
            continue
        props = omod_props_dict.get(mod_fid, [])
        for prop in props:
            pname = strip_q(prop.get("PropertyName", ""))
            val1 = strip_q(prop.get("Value1", ""))
            if pname == "Keywords" and "CustomItemName_" in val1:
                # Extract from: CustomItemName_AllRise """" [KYWD:...]
                m = re.match(r'CustomItemName_(\S+)', val1)
                if m:
                    raw = m.group(1).rstrip('"').strip()
                    return camelcase_to_display(raw)
        # Fallback: try the OMOD FULL name (strip "Custom Mod" and "Custom Name")
        mod_name = mod.get("name", "")
        if mod_name:
            cleaned = re.sub(r'\s*Custom\s*(Mod|Name)\s*$', '', mod_name, flags=re.I).strip()
            if cleaned and cleaned != mod_name:
                return cleaned
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  Main build
# ═══════════════════════════════════════════════════════════════════════════

def find_omod_base(tsv_dir):
    """
    Find the OMOD base export (NOT the Properties file).
    Match OMOD_Export_*.tsv but exclude *_Properties.tsv.
    """
    pattern = str(tsv_dir / "OMOD_Export_*.tsv")
    candidates = [f for f in sorted(glob.glob(pattern), key=os.path.getmtime)
                  if not f.endswith("_Properties.tsv")]
    return candidates[-1] if candidates else None


def build_channel(channel, tsv_dir, dist_dir):
    """Build unique_weapons.json for one channel (live or pts)."""
    print(f"\n{'='*60}")
    print(f"  Building unique weapons — channel: {channel}")
    print(f"  TSV dir: {tsv_dir}")
    print(f"{'='*60}")

    # ── Step 1: Find latest TSV files ───────────────────────────────────
    lvli_path = newest("LVLI_Export_*_LVLI_Entries.tsv", tsv_dir)
    weap_base_path = newest("WEAP_Export_*_Base.tsv", tsv_dir)
    weap_dnam_path = newest("WEAP_Export_*_DNAM.tsv", tsv_dir)
    weap_objt_path = newest("WEAP_Export_*_ObjectTemplate.tsv", tsv_dir)
    omod_base_path = find_omod_base(tsv_dir)
    omod_prop_path = newest("OMOD_Export_*_Properties.tsv", tsv_dir)
    ench_path      = newest("ENCH_Export_*.tsv", tsv_dir)
    gmrw_path      = newest("GMRW_Export_*.tsv", tsv_dir)
    quest_path     = newest("QUEST_Export_*.tsv", tsv_dir)

    required = {
        "LVLI Entries": lvli_path,
        "WEAP Base": weap_base_path,
        "WEAP DNAM": weap_dnam_path,
    }
    for label, path in required.items():
        if not path:
            print(f"  ERROR: Could not find {label} TSV in {tsv_dir}. Skipping channel.")
            return None
        print(f"  {label}: {os.path.basename(path)}")

    optional = {
        "WEAP ObjectTemplate": weap_objt_path,
        "OMOD base": omod_base_path,
        "OMOD Properties": omod_prop_path,
        "ENCH": ench_path,
        "GMRW": gmrw_path,
        "QUEST": quest_path,
    }
    for label, path in optional.items():
        if path:
            print(f"  {label}: {os.path.basename(path)}")
        else:
            print(f"  {label}: (not found — will skip related data)")

    # ── Step 2: Read LVLI entries → master weapon list ──────────────────
    print("\n  Reading LVLI entries...")
    lvli_rows = read_tsv(lvli_path)

    # Build full LVLI index: lvli_edid → list of (ref_formid, ref_edid, ref_sig) tuples
    lvli_index = {}
    for row in lvli_rows:
        edid = strip_q(row.get("LVLI_EDID", ""))
        ref_str = strip_q(row.get("LVLO_Reference", ""))
        if edid and ref_str:
            fid, ref_edid, sig = parse_ref(ref_str)
            if fid:
                lvli_index.setdefault(edid, []).append((fid, ref_edid, sig))

    # Get the master list from LL_Weapon_Unique_All
    master_refs = lvli_index.get("LL_Weapon_Unique_All", [])
    if not master_refs:
        print("  ERROR: LL_Weapon_Unique_All not found in LVLI entries. Skipping channel.")
        return None
    print(f"  Found {len(master_refs)} entries in LL_Weapon_Unique_All")

    # ── Step 3: Resolve LVLI refs to WEAP records ───────────────────────
    print("  Resolving LVLI → WEAP references...")
    # For each master ref, resolve to a (weap_formid, lvli_edid_chain) pair
    weapon_refs = []  # list of (weap_formid, weap_edid, source_lvli_edid)
    for fid, ref_edid, sig in master_refs:
        if sig == "WEAP":
            weapon_refs.append((fid, ref_edid, None))
        elif sig == "LVLI":
            # Look up the sub-LVLI to find the WEAP inside
            sub_entries = lvli_index.get(ref_edid, [])
            weap_found = False
            # Prefer non-creature WEAP entries
            for sub_fid, sub_edid, sub_sig in sub_entries:
                if sub_sig == "WEAP" and not is_creature(sub_edid):
                    weapon_refs.append((sub_fid, sub_edid, ref_edid))
                    weap_found = True
                    break
            if not weap_found:
                # Try any WEAP entry (even creature)
                for sub_fid, sub_edid, sub_sig in sub_entries:
                    if sub_sig == "WEAP":
                        weapon_refs.append((sub_fid, sub_edid, ref_edid))
                        weap_found = True
                        break
            if not weap_found:
                # Recurse one more level (LVLI → LVLI → WEAP)
                for sub_fid, sub_edid, sub_sig in sub_entries:
                    if sub_sig == "LVLI":
                        deep_entries = lvli_index.get(sub_edid, [])
                        for d_fid, d_edid, d_sig in deep_entries:
                            if d_sig == "WEAP" and not is_creature(d_edid):
                                weapon_refs.append((d_fid, d_edid, ref_edid))
                                weap_found = True
                                break
                    if weap_found:
                        break
            if not weap_found:
                print(f"    WARN: Could not resolve LVLI {ref_edid} ({fid}) to a WEAP")
                weapon_refs.append((fid, ref_edid, ref_edid))
        else:
            # Unknown sig — include it anyway
            weapon_refs.append((fid, ref_edid, None))

    print(f"  Resolved {len(weapon_refs)} weapon references")

    # ── Step 4: Read all WEAP data ──────────────────────────────────────
    print("  Reading WEAP Base + DNAM...")
    weap_base = {}
    for row in read_tsv(weap_base_path):
        fid = norm_formid(row.get("WEAP_FormID", ""))
        if fid:
            weap_base[fid] = row

    weap_dnam = {}
    for row in read_tsv(weap_dnam_path):
        fid = norm_formid(row.get("WEAP_FormID", ""))
        if fid:
            weap_dnam[fid] = row

    print(f"    WEAP Base: {len(weap_base)} records, DNAM: {len(weap_dnam)} records")

    # ── Step 5: Read ObjectTemplate data ────────────────────────────────
    obj_templates = {}  # weap_fid → {combo_idx → {full, mods: [(edid, name, omod_fid)]}}
    if weap_objt_path:
        print("  Reading WEAP ObjectTemplate...")
        for row in read_tsv(weap_objt_path):
            fid = norm_formid(row.get("WEAP_FormID", ""))
            combo_idx = strip_q(row.get("CombinationIndex", ""))
            mod_str = strip_q(row.get("Include_Mod", ""))
            combo_full = strip_q(row.get("Combination_FULL", ""))
            if fid and mod_str:
                combo = obj_templates.setdefault(fid, {}).setdefault(combo_idx, {
                    "full": combo_full, "mods": []
                })
                if combo_full and not combo["full"]:
                    combo["full"] = combo_full
                mod_edid, mod_name, mod_fid = parse_omod_ref(mod_str)
                if mod_edid:
                    combo["mods"].append((mod_edid, mod_name, mod_fid))
        print(f"    ObjectTemplate: {len(obj_templates)} weapons with template data")

    # ── Step 6: Read OMOD + OMOD Properties ─────────────────────────────
    omod_by_formid = {}
    omod_by_edid = {}
    omod_props = {}  # formid → list of property rows

    if omod_base_path:
        print("  Reading OMOD base...")
        for row in read_tsv(omod_base_path):
            fid = norm_formid(row.get("OMOD_FormID", ""))
            edid = strip_q(row.get("OMOD_EDID", ""))
            if fid:
                omod_by_formid[fid] = row
            if edid:
                omod_by_edid[edid] = row
        print(f"    OMOD base: {len(omod_by_formid)} records")

    if omod_prop_path:
        print("  Reading OMOD Properties...")
        for row in read_tsv(omod_prop_path):
            fid = norm_formid(row.get("OMOD_FormID", ""))
            if fid:
                omod_props.setdefault(fid, []).append(row)
        print(f"    OMOD Properties: {len(omod_props)} mod groups")

    # ── Step 7: Read ENCH ───────────────────────────────────────────────
    ench_by_formid = {}
    if ench_path:
        print("  Reading ENCH...")
        for row in read_tsv(ench_path):
            fid = norm_formid(row.get("ENCH_FormID", ""))
            if fid:
                ench_by_formid[fid] = row
        print(f"    ENCH: {len(ench_by_formid)} records")

    # ── Step 8: Read GMRW / QUEST for obtain info ──────────────────────
    gmrw_items = {}  # gmrw_formid → row
    gmrw_quest_name = {}  # gmrw_formid → quest_display_name
    weapon_to_quest = {}  # weapon_lvli_edid → quest_name

    if gmrw_path:
        print("  Reading GMRW...")
        for row in read_tsv(gmrw_path):
            fid = norm_formid(row.get("FormID", ""))
            if fid:
                gmrw_items[fid] = row
            # Parse RewardedItem to find weapon references
            rewarded = strip_q(row.get("RewardedItem", ""))
            if rewarded:
                r_fid, r_edid, r_sig = parse_ref(rewarded)
                if r_edid and (r_sig in ("LVLI", "WEAP")):
                    # Store mapping: rewarded EDID → GMRW formid
                    gmrw_items.setdefault("_reward_" + r_edid, [])
                    if isinstance(gmrw_items.get("_reward_" + r_edid), list):
                        gmrw_items["_reward_" + r_edid].append(fid)
        print(f"    GMRW: {len([k for k in gmrw_items if not k.startswith('_')])} records")

    if quest_path:
        print("  Reading QUEST...")
        quest_rows = read_tsv(quest_path)
        for row in quest_rows:
            quest_name = strip_q(row.get("FULL - Name", ""))
            # Check GMRWRef columns
            for i in range(10):
                gmrw_ref = strip_q(row.get(f"GMRWRef{i}", ""))
                if gmrw_ref:
                    ref_fid, ref_edid, _ = parse_ref(gmrw_ref)
                    if ref_fid and quest_name:
                        gmrw_quest_name[ref_fid] = quest_name
                    # Also check Reward columns
            for i in range(10):
                reward_ref = strip_q(row.get(f"Reward{i}", ""))
                if reward_ref:
                    ref_fid, ref_edid, _ = parse_ref(reward_ref)
                    if ref_fid and quest_name:
                        gmrw_quest_name[ref_fid] = quest_name
        print(f"    QUEST→GMRW links: {len(gmrw_quest_name)}")

    # ── Step 9: Build weapon objects ────────────────────────────────────
    print("\n  Building weapon objects...")
    weapons = []
    seen_keys = set()

    for weap_fid, weap_edid, source_lvli in weapon_refs:
        # Deduplicate by (formID, source_lvli) — same base weapon can appear
        # as multiple unique weapons (e.g. Missile Launcher → Bunker Buster, BoomStick)
        dedup_key = (weap_fid, source_lvli or "")
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        base_row = weap_base.get(weap_fid, {})
        dnam_row = weap_dnam.get(weap_fid, {})

        weap_full = strip_q(base_row.get("WEAP_FULL", ""))
        edid = strip_q(base_row.get("WEAP_EDID", "")) or strip_q(weap_edid)
        keywords_str = strip_q(base_row.get("Keywords", ""))

        # Skip creature weapons
        if is_creature(edid):
            continue

        # ── Build custom mods list early (needed for name resolution) ──
        # When source_lvli is present, filter custom mods to those matching the
        # LVLI hint (e.g. LL_..._BunkerBuster → only BunkerBuster mods)
        lvli_hint = None
        if source_lvli:
            lvli_hint = extract_unique_name_from_lvli(source_lvli)

        custom_mods_raw = []
        if weap_fid in obj_templates:
            seen_mod_edids = set()
            for cidx, combo_data in obj_templates[weap_fid].items():
                for mod_edid, mod_name, mod_fid in combo_data.get("mods", []):
                    if not mod_edid:
                        continue
                    if mod_edid.lower() in seen_mod_edids:
                        continue
                    # Only include Custom mods (not generic weapon mods)
                    if "custom" not in mod_edid.lower():
                        continue
                    # If we came through an LVLI, use its hint to filter mods.
                    # If the LVLI has a unique name hint (e.g. BunkerBuster),
                    # only keep mods matching that name.
                    # If the LVLI has NO unique hint (generic weapon pool),
                    # skip custom mods entirely for this entry.
                    if source_lvli:
                        if lvli_hint:
                            hint_compact = lvli_hint.replace(" ", "").lower()
                            mod_compact = mod_edid.replace("_", "").lower()
                            if hint_compact not in mod_compact:
                                continue
                        else:
                            # Generic LVLI → no custom mods for this entry
                            continue
                    seen_mod_edids.add(mod_edid.lower())
                    omod_row = omod_by_formid.get(mod_fid, {}) if mod_fid else {}
                    omod_desc = strip_q(omod_row.get("DESC", ""))
                    omod_full = strip_q(omod_row.get("FULL", ""))

                    effects = []
                    if mod_fid and mod_fid in omod_props:
                        for prop_row in omod_props[mod_fid]:
                            prop_name = strip_q(prop_row.get("PropertyName", ""))
                            val1 = strip_q(prop_row.get("Value1", ""))
                            val2 = strip_q(prop_row.get("Value2", ""))
                            func_type = strip_q(prop_row.get("FunctionType", ""))
                            if prop_name:
                                effects.append({
                                    "property": prop_name,
                                    "value": val1,
                                    "value2": val2 if val2 else None,
                                    "function": func_type,
                                })

                    custom_mods_raw.append({
                        "name": omod_full or mod_name or mod_edid,
                        "edid": mod_edid,
                        "formId": mod_fid,
                        "description": omod_desc or None,
                        "effects": effects,
                    })

        # ── Display name resolution ──
        # Priority depends on whether we came through an LVLI (shared base
        # weapon) or have a dedicated WEAP record.
        #
        # When source_lvli is set → the WEAP_FULL is the BASE weapon name
        # (e.g. ".44", "Super Sledge") and is NOT the unique weapon name.
        # Prefer LVLI / OMOD derived names first.
        #
        # When source_lvli is None → we have a dedicated WEAP record whose
        # FULL is the actual unique weapon name (e.g. "The Fixer").

        # Build set of generic weapon names for the "is unique?" check.
        # Include both the full display names AND the raw WEAP_FULL values
        # that we know are generic (short forms like ".44", "Flamer", etc.)
        generic_names = {n.lower() for n in BASE_WEAPON_NAMES.values()}
        # Also include the raw EDID keys (CamelCase→lowercase) and common
        # short forms the game uses as WEAP_FULL for base records:
        for k in BASE_WEAPON_NAMES:
            generic_names.add(k.lower())
        # Extra known short WEAP_FULL values that are base weapons:
        for short in [".44", "laser", "plasma", "flamer", "minigun",
                      "broadsider", "10mm", "missile launcher",
                      "10mm submachine gun", "hunting rifle",
                      "combat rifle", "assault rifle", "auto axe",
                      "pump-action shotgun", "double-barrel shotgun",
                      "combat shotgun", "gatling gun", "gatling laser",
                      "fat man", "power fist", "deathclaw gauntlet",
                      "ski sword", "super sledge", "switchblade",
                      "pipe wrench", "lever-action rifle", "tesla rifle",
                      "harpoon gun", "railway rifle", ".50 cal machine gun",
                      "compound bow", "chinese officer sword", "war glaive",
                      "auto grenade launcher", "baton", "revolutionary sword",
                      "combat knife", "death tambo", "spear",
                      "single action revolver", "baseball bat",
                      "laser gun", "alien blaster", "submachine gun",
                      "cultist blade", "assaultron blade",
                      "commie whacker", "handmade rifle", "shovel",
                      "plasma gun"]:
            generic_names.add(short.lower())

        weap_full_is_unique = False
        if weap_full and not source_lvli:
            # Only trust WEAP_FULL as unique when we have a dedicated WEAP record
            if weap_full.lower() not in generic_names:
                weap_full_is_unique = True

        display_name = None

        # Step 1: If we came through LVLI, try LVLI EDID name first
        if source_lvli:
            display_name = extract_unique_name_from_lvli(source_lvli)

        # Step 2: Try OMOD CustomItemName keyword
        if not display_name:
            display_name = extract_unique_name_from_omod(custom_mods_raw, omod_props)

        # Step 3: Use WEAP_FULL only if it's genuinely unique (dedicated record)
        if not display_name and weap_full_is_unique:
            display_name = weap_full

        # Step 4: Try OMOD FULL name (strip "Custom Mod" suffix)
        if not display_name and custom_mods_raw:
            for mod in custom_mods_raw:
                mname = mod.get("name", "")
                if mname:
                    import re as _re
                    cleaned = _re.sub(r'\s*Custom\s*(Mod|Name)\s*$', '', mname, flags=_re.I).strip()
                    if cleaned and cleaned.lower() not in generic_names:
                        display_name = cleaned
                        break

        # Step 5: Fallback
        if not display_name:
            display_name = weap_full or camelcase_to_display(edid)

        # ── Base weapon name ──
        base_weapon_name = resolve_base_weapon_name(edid, weap_full)

        # ── Stats from DNAM ──
        stats = {
            "damage":     safe_int(dnam_row.get("DNAM_BaseDamage")),
            "fireRate":   safe_float(dnam_row.get("DNAM_Speed")),
            "range":      safe_int(dnam_row.get("DNAM_MaxRange")),
            "accuracy":   safe_int(dnam_row.get("DNAM_AccuracyBonus")),
            "weight":     safe_float(dnam_row.get("DNAM_Weight")),
            "value":      safe_int(dnam_row.get("DNAM_Value")),
            "ammoType":   humanise_ammo(dnam_row.get("DNAM_Ammo")),
            "capacity":   safe_int(dnam_row.get("DNAM_Capacity")),
            "apCost":     safe_int(dnam_row.get("DNAM_ActionPointCost")),
            "stagger":    safe_float(dnam_row.get("DNAM_Stagger")),
            "critMult":   safe_float(dnam_row.get("CRDT_CritDamageMult")),
            "damageCurve": strip_q(dnam_row.get("CVT0_DamageCurve")) or None,
            "level":      safe_int(base_row.get("EILV_Level")),
        }

        # -- Weapon type --
        weapon_type = classify_weapon_type(dnam_row, keywords_str)

        # -- Custom mods (already built above for name resolution) --
        custom_mods = custom_mods_raw

        # -- Enchantments --
        enchantments = []

        # -- How to obtain --
        how_to_obtain = "Unknown"
        quest_name = None

        # Try GMRW/QUEST linkage first
        if source_lvli:
            reward_key = "_reward_" + source_lvli
            gmrw_fids = gmrw_items.get(reward_key, [])
            if isinstance(gmrw_fids, list):
                for gfid in gmrw_fids:
                    qname = gmrw_quest_name.get(gfid)
                    if qname:
                        how_to_obtain = f"Quest: {qname}"
                        quest_name = qname
                        break

        # Try fallback map
        if how_to_obtain == "Unknown":
            fallback = match_obtain_fallback(edid)
            if fallback:
                how_to_obtain = fallback
                if fallback.startswith("Quest:"):
                    quest_name = fallback[len("Quest:"):].strip()

        if how_to_obtain == "Unknown" and source_lvli:
            fallback_lvli = match_obtain_fallback(source_lvli)
            if fallback_lvli:
                how_to_obtain = fallback_lvli
                if fallback_lvli.startswith("Quest:"):
                    quest_name = fallback_lvli[len("Quest:"):].strip()

        # -- Tradeable flag --
        tradeable = True
        kw_lower = keywords_str.lower()
        if "nonplayertradable" in kw_lower or "untradeable" in kw_lower or "notradable" in kw_lower:
            tradeable = False

        # -- Cut content flag --
        is_cut_flag = is_cut(edid)

        weapon = {
            "name": display_name,
            "formId": weap_fid,
            "edid": edid,
            "weaponType": weapon_type,
            "baseWeapon": base_weapon_name,
            "stats": stats,
            "legendaryEffects": [],
            "customMods": custom_mods,
            "enchantments": enchantments,
            "howToObtain": how_to_obtain,
            "questName": quest_name,
            "tradeable": tradeable,
            "isCut": is_cut_flag,
        }
        weapons.append(weapon)

    # Sort alphabetically by name
    weapons.sort(key=lambda w: (w.get('isCut', False), w['name'].lower()))

    # -- Step 10: Output JSON --
    output = {
        "version": 1,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "channel": channel,
        "count": len(weapons),
        "weapons": weapons,
    }

    out_subdir = "unique_weapons"
    if channel == "pts":
        out_path = dist_dir / "pts" / out_subdir / "unique_weapons.json"
    else:
        out_path = dist_dir / out_subdir / "unique_weapons.json"

    os.makedirs(out_path.parent, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\n  Wrote {out_path}")
    print(f"  Total weapons: {len(weapons)}")
    cut_n = sum(1 for w in weapons if w["isCut"])
    mod_n = sum(1 for w in weapons if w["customMods"])
    obt_n = sum(1 for w in weapons if w["howToObtain"] != "Unknown")
    print(f"    Cut: {cut_n}")
    print(f"    With custom mods: {mod_n}")
    print(f"    With obtain info: {obt_n}")

    # Patchlog feed (live only)
    if channel == "live":
        write_empty_patchlog_feed(
            str(dist_dir), "patchlog_latest_bnb_unique_weapons.json",
            current_count=len(weapons),
        )

    return output


def main():
    ap = argparse.ArgumentParser(
        description="Build unique_weapons.json from xEdit TSV exports.")
    ap.add_argument("--channel", choices=["live", "pts", "both"], default="both",
                    help="Which channel to build (default: both)")
    args = ap.parse_args()

    os.chdir(REPO_ROOT)
    dist_dir = REPO_ROOT / "dist"

    if args.channel in ("live", "both"):
        build_channel("live", TSV_DIR, dist_dir)

    if args.channel in ("pts", "both"):
        pts_tsv = PTS_DIR
        if pts_tsv.is_dir():
            build_channel("pts", pts_tsv, dist_dir)
        else:
            print(f"\n  PTS: Skipping -- {pts_tsv} does not exist.")


if __name__ == "__main__":
    main()
# end of file

