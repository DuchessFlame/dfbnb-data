#!/usr/bin/env python3
"""
build_head_hunt_bosses_json.py — Head Hunt Boss Groups
Reads TSV exports and produces:
  dist/bounty-hunting/head_hunt_bosses.json

All boss names, gang names, weapon names, enchantment effects, and ability
descriptions are read from TSVs. Only the gang-to-weapon wiring is structural
(NPC records aren't exported to TSV, so which boss CARRIES which weapon is
defined here; the weapon's actual stats/name/effects still come from TSVs).

When Bethesda patches and new TSVs are exported, this script will pick up
updated boss names, weapon stats, enchantment magnitudes, etc. automatically.
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT       = SCRIPT_DIR.parent
TSV_DIR    = ROOT / "tsv"
OUT_DIR    = ROOT / "dist" / "bounty-hunting"
OUT_FILE   = OUT_DIR / "head_hunt_bosses.json"


def newest(pattern):
    hits = glob.glob(str(TSV_DIR / pattern))
    if not hits:
        return None
    hits.sort(key=lambda p: os.path.getmtime(p))
    return Path(hits[-1])


def read_tsv(path):
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, newline="", encoding=enc) as fh:
                return list(csv.DictReader(fh, delimiter="\t"))
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise RuntimeError(f"Could not decode {path}")


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def slugify(name):
    s = name.lower().strip()
    s = re.sub(r'["\x27]', '', s)
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-')


# Gang-to-weapon structural mapping. NPC records aren't exported to TSV,
# so which boss carries which weapon can't be derived programmatically.
# Key = gang index (position in BURN_Bounty_HeadHunt_Target_FormList).
GANG_WEAPON_MAP = {
    0:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_50CalMachineGun",
         "enchant": "Burn_BountyHunt_EnchBleedChanceAlways",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Poison"},
    6:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_GatlingPlasma",
         "enchant": "Burn_BountyHunt_EnchPoisonChanceAlways",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Poison"},
    10: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_PlasmaCaster",
         "enchant": "Burn_BountyHunt_EnchRadChanceAlways",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Rad"},
    12: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Cremator",
         "enchant": "Burn_BountyHunt_EnchFireChanceAlways",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Fire"},
    1:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_GammaGun",
         "enchant": "Burn_BountyHunt_EnchMoreDamageToHighRADS",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Rad"},
    3:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Cremator_Purifier",
         "mod": "Burn_Bounty_mod_Custom_Purifier",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Fire",
         "spell": "Burn_Bounty_Purifier_SetOnFire"},
    4:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_MedicalMalpractice",
         "enchant": "Burn_BountyHunt_EnchRandomEffect",
         "mod": "Burn_Bounty_mod_custom_DebuffCocktail",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Bug"},
    9:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_HuntingRifle_Nuka",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Rad_NukaCola"},
    26: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_AbraxoGun",
         "mod": "Bounty_Mod_Custom_AbraxoGun",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Hallucigen"},
    2:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_AlienBlaster",
         "enchant": "Burn_BountyHunt_EnchCryoChanceAlways",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Cryo",
         "spell": "Burn_Bounty_AstroEffect"},
    28: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_HandmadeCommunist",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    15: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_GrandFinale",
         "melee": "crLLI_Burn_BountyHunt_Weapon_Melee_GuitarSword",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Baseball"},
    11: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_AutoGrenadeLauncher",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Energy"},
    19: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_V63LaserCarbine",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Energy"},
    8:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_BlackPowderRifle_Dragon",
         "melee": "crLLI_Burn_BountyHunt_Weapon_Melee_WalkingCane",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    13: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_MissileLauncher_Patriot",
         "mod": "Burn_Bounty_mod_Custom_ExtraDamage20Percent",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    20: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_PlasmaGun",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Energy"},
    16: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_CircuitBreaker",
         "mod": "Burn_Bounty_mod_Custom_Mechanist",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Energy",
         "spell": "Burn_Bounty_FrenzySpell"},
    25: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Minigun",
         "mod": "Burn_Bounty_mod_Custom_ExtraDamage25Percent",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    23: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Melee_Knuckles",
         "mod": "Burn_Bounty_ModelSwap_PowerFist",
         "grenade": "crLLI_Burn_BountyHunt_Throwable_ThrowingKnife_Boxers"},
    22: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Revolver",
         "grenade": "crLLI_Burn_BountyHunt_Throwable_ThrowingKnife"},
    24: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_HuntingRifle_Sniper",
         "mod": "Burn_Bounty_mod_Custom_ExtraDamagex5",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    5:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_DoubleBarrelShotgun",
         "enchant": "Burn_BountyHunt_EnchFireChanceAlways",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Fire"},
    17: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_PipeRevolver",
         "enchant": "Burn_BountyHunt_EnchGambling",
         "mod": "Burn_Bounty_mod_Custom_Gambling",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    7:  {"weapon": "crLLI_Burn_BountyHunt_Weapon_Melee_WalkingCane",
         "mod": "Burn_Bounty_mod_Custom_Granny",
         "enchant": "Burn_BountyHunt_EnchMoreDamageToFed",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    29: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Tesla_Rifle_Automatic",
         "mod": "Burn_Bounty_mod_Custom_Electricians_NoStun",
         "melee": "crLLI_Burn_BountyHunt_Weapon_Melee_MeatHook",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Frag"},
    27: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Flatliner",
         "mod": "Burn_Bounty_mod_Custom_ExtraDamagex4",
         "melee": "crLLI_Burn_BountyHunt_Weapon_Melee_SuperSledge",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_Rad"},
    21: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Tesla_Fishing",
         "mod": "Burn_Bounty_mod_Custom_Fisherman",
         "enchant": "Burn_BountyHunt_HookPlayer",
         "grenade": "crLLI_Burn_BountyHunt_Weapon_Ranged_HarpoonGun",
         "spell": "Burn_Bounty_FishermanEffect"},
    18: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_CombatRifle",
         "melee": "crLLI_Burn_BountyHunt_Weapon_Melee_Pickaxe",
         "grenade": "crLLI_Burn_BountyHunt_Grenade_DynamiteBundle"},
    14: {"weapon": "crLLI_Burn_BountyHunt_Weapon_Ranged_Crossbow",
         "melee": "crLLI_Burn_BountyHunt_Throwable_Tomahawk",
         "spell": "Burn_Bounty_ScoutCamo_SP"},
}

_WEAP_FULL_FIXES = {
    "crCremator": "Cremator",
    "crPlasmaGun": "Plasma Gun",
}

_WEAPON_NAME_OVERRIDES = {
    "Flatliner":              "Flatliner",
    "GrandFinale":            "Grand Finale",
    "CircuitBreaker":         "Circuit Breaker",
    "MedicalMalpractice":     "Medical Malpractice",
    "AbraxoGun":              "Meadow Breeze Sprayer",
    "V63LaserCarbine":        "V63 Laser Carbine",
    "Tesla_Fishing":          "Fishing Tesla Rifle",
    "Tesla_Fishing_Lite":     "Fishing Tesla Rifle (Lite)",
    "Tesla_Rifle_Automatic":  "Automatic Tesla Rifle",
    "HuntingRifle_Nuka":      "Nuka Hunting Rifle",
    "HuntingRifle_Sniper":    "Sniper Hunting Rifle",
    "HandmadeCommunist":      "Communist Handmade",
    "Cremator_Purifier":      "Purifier Cremator",
    "BlackPowderRifle_Dragon":"The Dragon",
    "MissileLauncher_Patriot":"Patriot Missile Launcher",
    "Fixer_Patriot":          "Patriot Fixer",
    "50CalMachineGun_Gold":   "Gold .50 Cal Machine Gun",
    "ThirstZapper_Quantum":   "Quantum Thirst Zapper",
    "DoubleBarrelShotgun_Wolf":"Wolf Double-Barrel Shotgun",
    "AlienBlaster":           "Alien Blaster",
}


def load_boss_groups(chal_rows):
    groups = {}
    pat = re.compile(r"CompleteHeadGroups_Group(\d+)_Kill(\w+)_SUB$")
    for row in chal_rows:
        edid = row.get("EDID", "")
        m = pat.search(edid)
        if not m:
            continue
        gnum = int(m.group(1))
        gsuf = m.group(2)
        full = row.get("FULL", "")
        bname = re.sub(r"^Kill the Target\s*[-–—]\s*", "", full).strip() or gsuf
        gidx = -1
        for i in range(1, 30):
            cond = row.get(f"Cond{i}", "")
            gv = re.search(r"([\d.]+)\|GetGlobalValue.*Burn_BountyHunt_RecentHeadhuntGang", cond)
            if gv:
                gidx = int(float(gv.group(1)))
                break
        if gidx < 0:
            for i in range(1, 30):
                cond = row.get(f"Cond{i}", "")
                gv = re.search(r"GetGlobalValue.*RecentHeadhuntGang.*?\|([\d.]+)", cond)
                if gv:
                    gidx = int(float(gv.group(1)))
                    break
        groups.setdefault(gnum, []).append({"bossName": bname, "gangIndex": gidx, "chalEdid": edid, "gangSuffix": gsuf})
    for g in groups.values():
        g.sort(key=lambda b: b["bossName"])
    return dict(sorted(groups.items()))


def load_gang_keywords(flst_rows):
    gangs = {}
    for row in flst_rows:
        if row.get("FLST_EDID") != "BURN_Bounty_HeadHunt_Target_FormList":
            continue
        idx = safe_int(row.get("EntryIndex"), -1)
        if idx < 0:
            continue
        gangs[idx] = {"edid": row.get("Entry_EDID", ""), "name": row.get("Entry_FULL", "").replace(" Gang", ""), "formId": row.get("Entry_FormID", "")}
    return gangs


def load_weapon_lvlis(lvli_rows):
    weapons = {}
    for row in lvli_rows:
        edid = row.get("LVLI_EDID", "")
        if not (edid.startswith("crLLI_Burn_BountyHunt_Weapon_") or edid.startswith("crLLI_Burn_BountyHunt_Grenade_") or edid.startswith("crLLI_Burn_BountyHunt_Throwable_")):
            continue
        ref = row.get("LVLO_Reference", "")
        parts = ref.split(":")
        wfid = parts[0] if len(parts) > 0 else ""
        weid = parts[1] if len(parts) > 1 else ""
        weapons.setdefault(edid, {"lvliFormId": row.get("LVLI_FormID", ""), "weapRefs": []})
        if weid:
            weapons[edid]["weapRefs"].append({"formId": wfid, "edid": weid})
    return weapons


def load_weapon_names(weap_rows):
    names = {r.get("WEAP_EDID", ""): r.get("WEAP_FULL", "") for r in weap_rows if r.get("WEAP_EDID") and r.get("WEAP_FULL")}
    for edid, fix in _WEAP_FULL_FIXES.items():
        names[edid] = fix
    return names


def load_weapon_stats(dnam_rows):
    stats = {}
    for r in dnam_rows:
        eid = r.get("WEAP_EDID", "")
        if not eid:
            continue
        stats[eid] = {"speed": safe_float(r.get("DNAM_Speed")), "reloadSpeed": safe_float(r.get("DNAM_ReloadSpeed")), "minRange": safe_float(r.get("DNAM_MinRange")), "maxRange": safe_float(r.get("DNAM_MaxRange")), "weaponType": r.get("DNAM_WeaponType", ""), "ammo": r.get("DNAM_Ammo", ""), "capacity": safe_int(r.get("DNAM_Capacity"))}
    return stats


def load_enchantments(ench_rows):
    enchants = {}
    for row in ench_rows:
        edid = row.get("ENCH_EDID", "")
        if not edid.startswith("Burn_BountyHunt_"):
            continue
        effects = []
        count = safe_int(row.get("Effects_Count"), 0)
        for i in range(1, min(count + 1, 31)):
            meid = row.get(f"Effect_{i}_MGEF_EID", "")
            if not meid:
                continue
            effects.append({"mgefEdid": meid, "magnitude": safe_float(row.get(f"Effect_{i}_Magnitude")), "area": safe_int(row.get(f"Effect_{i}_Area")), "duration": safe_int(row.get(f"Effect_{i}_Duration"))})
        enchants[edid] = {"name": row.get("ENCH_FULL", "") or edid, "formId": row.get("ENCH_FormID", ""), "effects": effects}
    return enchants


def load_magic_effects(mgef_rows):
    return {r.get("EDID", ""): {"name": r.get("FULL", "") or r.get("EDID", ""), "description": r.get("DNAM_MagicItemDescription", "")} for r in mgef_rows if r.get("EDID")}


def _clean_mod_name(edid):
    s = edid
    for pre in ("Burn_Bounty_mod_Custom_", "Burn_Bounty_mod_custom_", "Bounty_Mod_Custom_", "Burn_Bounty_mod_", "Burn_Bounty_ModelSwap_"):
        if s.startswith(pre):
            s = s[len(pre):]
            break
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)
    s = re.sub(r'_', ' ', s)
    return s.strip() or edid


def load_weapon_mods(omod_rows):
    mods = {}
    for row in omod_rows:
        edid = row.get("OMOD_EDID", "")
        if not (edid.startswith("Burn_Bounty_mod_") or edid.startswith("Bounty_Mod_") or edid.startswith("mod_Custom_Plasma_Abraxo")):
            continue
        raw_name = row.get("FULL", "") or row.get("DESC", "") or ""
        if not raw_name:
            raw_name = _clean_mod_name(edid)
        mods[edid] = {"name": raw_name, "desc": row.get("DESC", ""), "formId": row.get("OMOD_FormID", "")}
    return mods


def load_spells(spel_rows):
    spells = {}
    for row in spel_rows:
        edid = row.get("SPEL_EDID", "") or row.get("EDID", "")
        if not edid.startswith("Burn_Bounty_"):
            continue
        name = row.get("SPEL_FULL", "") or row.get("FULL", "") or edid
        fid = row.get("SPEL_FormID", "") or row.get("FormID", "")
        spells[edid] = {"name": name, "formId": fid}
    return spells


def derive_weapon_name(lvli_edid, base_weap_name):
    suffix = lvli_edid
    for prefix in ("crLLI_Burn_BountyHunt_Weapon_Ranged_", "crLLI_Burn_BountyHunt_Weapon_Melee_", "crLLI_Burn_BountyHunt_Grenade_", "crLLI_Burn_BountyHunt_Throwable_"):
        if suffix.startswith(prefix):
            suffix = suffix[len(prefix):]
            break
    if suffix in _WEAPON_NAME_OVERRIDES:
        return _WEAPON_NAME_OVERRIDES[suffix]
    if base_weap_name:
        return base_weap_name
    return re.sub(r'([A-Z])', r' \1', suffix).strip()


def describe_enchantment(ench_data, mgef_lookup):
    results = []
    for eff in ench_data.get("effects", []):
        mgef = mgef_lookup.get(eff["mgefEdid"], {})
        name = mgef.get("name", eff["mgefEdid"]).replace("DVMF_", "").replace("Burn_ME_", "")
        desc = {"effect": name}
        if eff["magnitude"] and eff["magnitude"] != 0:
            desc["magnitude"] = eff["magnitude"]
        if eff["duration"] and eff["duration"] != 0:
            desc["duration"] = eff["duration"]
        results.append(desc)
    return results


def resolve_weapon(lvli_edid, weapon_lvlis, weapon_names, weapon_stats):
    data = weapon_lvlis.get(lvli_edid)
    if not data or not data.get("weapRefs"):
        return None
    ref = data["weapRefs"][0]
    base_name = weapon_names.get(ref["edid"], "")
    friendly = derive_weapon_name(lvli_edid, base_name)
    stats = weapon_stats.get(ref["edid"], {})
    return {"name": friendly, "baseName": base_name, "baseEdid": ref["edid"], "lvliEdid": lvli_edid, "lvliFormId": data["lvliFormId"], "weaponType": stats.get("weaponType", ""), "speed": stats.get("speed", 0), "maxRange": stats.get("maxRange", 0)}


def assemble_boss(boss_info, gang_data, weapon_lvlis, weapon_names, weapon_stats, enchantments, mgef_lookup, weapon_mods, spells):
    gidx = boss_info["gangIndex"]
    wmap = GANG_WEAPON_MAP.get(gidx, {})
    boss = {"name": boss_info["bossName"], "gangIndex": gidx, "gangName": gang_data.get("name", "") if gang_data else "", "gangEdid": gang_data.get("edid", "") if gang_data else "", "imageSlug": slugify(boss_info["bossName"])}
    if wmap.get("weapon"):
        w = resolve_weapon(wmap["weapon"], weapon_lvlis, weapon_names, weapon_stats)
        if w:
            boss["weapon"] = w
    if wmap.get("enchant"):
        e = enchantments.get(wmap["enchant"])
        if e:
            boss["enchantment"] = {"name": e["name"], "edid": wmap["enchant"], "effects": describe_enchantment(e, mgef_lookup)}
    if wmap.get("mod"):
        m = weapon_mods.get(wmap["mod"])
        if m:
            boss["weaponMod"] = {"name": m["name"], "desc": m["desc"], "edid": wmap["mod"]}
    if wmap.get("grenade"):
        g = resolve_weapon(wmap["grenade"], weapon_lvlis, weapon_names, weapon_stats)
        if g:
            boss["grenade"] = g
    if wmap.get("melee"):
        ml = resolve_weapon(wmap["melee"], weapon_lvlis, weapon_names, weapon_stats)
        if ml:
            boss["meleeWeapon"] = ml
    if wmap.get("spell"):
        sp = spells.get(wmap["spell"])
        if sp:
            boss["specialAbility"] = {"name": sp["name"], "edid": wmap["spell"]}
    return boss


def load_sidekick_info(kywd_rows):
    for row in kywd_rows:
        if row.get("EDID") == "Burn_Bounty_HeadHuntWave_SupportRustRaiders":
            return {"name": row.get("FULL", "Rust Raiders"), "edid": row.get("EDID", "")}
    return {"name": "Rust Raiders", "edid": ""}


def main():
    print("[Head Hunt Bosses] Loading TSVs...")
    chal_path = newest("CHAL_Export_*.tsv")
    flst_path = newest("FLST_Export_*_Entries.tsv") or newest("FLST_Export_List_*_Entries.tsv")
    kywd_path = newest("KYWD_Export_*.tsv")
    lvli_path = newest("LVLI_Export_*_Entries.tsv") or newest("LVLI_Export_Full_*_LVLI_Entries.tsv")
    weap_base = newest("WEAP_Export_*_Base.tsv")
    weap_dnam = newest("WEAP_Export_*_DNAM.tsv")
    ench_path = newest("ENCH_Export_*.tsv")
    mgef_path = newest("MGEF_Export_*.tsv")
    omod_path = newest("OMOD_Export_*.tsv")
    spel_path = newest("SPEL_Export_*_HEADER.tsv") or newest("SPEL_Export_*.tsv")

    missing = []
    for name, path in [("CHAL", chal_path), ("FLST_Entries", flst_path), ("KYWD", kywd_path), ("LVLI_Entries", lvli_path), ("WEAP_Base", weap_base), ("WEAP_DNAM", weap_dnam), ("ENCH", ench_path), ("MGEF", mgef_path)]:
        if not path:
            missing.append(name)
    if missing:
        print(f"[Head Hunt Bosses] FATAL: Missing TSVs: {', '.join(missing)}")
        sys.exit(1)

    chal_rows = read_tsv(chal_path)
    flst_rows = read_tsv(flst_path)
    kywd_rows = read_tsv(kywd_path)
    lvli_rows = read_tsv(lvli_path)
    weap_base_rows = read_tsv(weap_base)
    weap_dnam_rows = read_tsv(weap_dnam)
    ench_rows = read_tsv(ench_path)
    mgef_rows = read_tsv(mgef_path)
    omod_rows = read_tsv(omod_path) if omod_path else []
    spel_rows = read_tsv(spel_path) if spel_path else []

    print(f"  CHAL: {len(chal_rows)}  |  FLST: {len(flst_rows)}  |  MGEF: {len(mgef_rows)}")

    boss_groups = load_boss_groups(chal_rows)
    gang_keywords = load_gang_keywords(flst_rows)
    weapon_lvlis = load_weapon_lvlis(lvli_rows)
    weapon_names = load_weapon_names(weap_base_rows)
    weapon_stats = load_weapon_stats(weap_dnam_rows)
    enchantments = load_enchantments(ench_rows)
    mgef_lookup = load_magic_effects(mgef_rows)
    weapon_mods = load_weapon_mods(omod_rows)
    spell_data = load_spells(spel_rows)
    sidekick_info = load_sidekick_info(kywd_rows)

    print(f"  Groups: {len(boss_groups)}  |  Gangs: {len(gang_keywords)}  |  Weapon LVLIs: {len(weapon_lvlis)}")

    output_groups = []
    for group_num, bosses in boss_groups.items():
        group_bosses = []
        for bi in bosses:
            gd = gang_keywords.get(bi["gangIndex"])
            bo = assemble_boss(bi, gd, weapon_lvlis, weapon_names, weapon_stats, enchantments, mgef_lookup, weapon_mods, spell_data)
            bo["sidekick"] = {"name": sidekick_info["name"], "description": "Support wave enemies that spawn alongside the boss. They carry standard weapons and add pressure during the fight."}
            group_bosses.append(bo)
        output_groups.append({"groupNumber": group_num, "bossCount": len(group_bosses), "bosses": group_bosses})

    result = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "groupCount": len(output_groups),
        "totalBosses": sum(g["bossCount"] for g in output_groups),
        "sidekickType": sidekick_info["name"],
        "groups": output_groups,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n[Head Hunt Bosses] Written -> {OUT_FILE}")
    print(f"  {result['groupCount']} groups, {result['totalBosses']} bosses")
    for g in output_groups:
        names = ", ".join(b["name"] for b in g["bosses"])
        print(f"  Group {g['groupNumber']}: {g['bossCount']} bosses - {names}")


if __name__ == "__main__":
    main()
