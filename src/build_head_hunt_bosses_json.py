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
import tsv_source          # one resolver for every export selection

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT       = SCRIPT_DIR.parent
TSV_DIR    = ROOT / "tsv"
OUT_DIR    = ROOT / "dist" / "bounty-hunting"
OUT_FILE   = OUT_DIR / "head_hunt_bosses.json"


def newest(pattern):
    hits = glob.glob(str(TSV_DIR / pattern))
    if not hits:
        return None
    hits.sort(key=tsv_source.export_key)
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
        groups.setdefault(gnum, []).append({"bossName": bname, "gangIndex": gidx, "chalEdid": edid, "chalFull": full, "chalFormId": row.get("FormID", ""), "gangSuffix": gsuf})
    for g in groups.values():
        g.sort(key=lambda b: b["bossName"])
    return dict(sorted(groups.items()))


def load_head_hunt_metas(chal_rows):
    """Per-group META + overall lifetime challenges (auto-tick targets)."""
    metas = {}
    all_meta = None
    pat = re.compile(r"CompleteHeadGroups_Group(\d+)_META$")
    for row in chal_rows:
        edid = row.get("EDID", "")
        m = pat.search(edid)
        if m:
            metas[int(m.group(1))] = {"edid": edid, "formId": row.get("FormID", ""), "name": row.get("FULL", "")}
        elif edid.endswith("CompleteHeadGroups_All_META"):
            all_meta = {"edid": edid, "formId": row.get("FormID", ""), "name": row.get("FULL", "")}
    return metas, all_meta


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
    # Column quirk of the ENCH export:
    #   Effect_i_MGEF_FID = "FormID:EDID"   (the MGEF reference)
    #   Effect_i_MGEF_EID = magnitude       (the numeric value lives here)
    #   Effect_i_Area     = duration (sec)  (NOT the Duration column, which is blank)
    enchants = {}
    for row in ench_rows:
        edid = row.get("ENCH_EDID", "")
        if not (edid.startswith("Burn_BountyHunt_") or edid.startswith("Burn_Bounty_")
                or edid.startswith("SDOW_")):
            continue
        effects = []
        count = safe_int(row.get("Effects_Count"), 0)
        for i in range(1, min(count + 1, 31)):
            fid_field = row.get(f"Effect_{i}_MGEF_FID", "") or ""
            mgef_edid = fid_field.split(":", 1)[1] if ":" in fid_field else fid_field
            if not mgef_edid:
                continue
            effects.append({
                "mgefEdid": mgef_edid,
                "magnitude": safe_float(row.get(f"Effect_{i}_MGEF_EID")),
                "duration": safe_int(row.get(f"Effect_{i}_Area")),
            })
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


def load_omod_properties(prop_rows):
    """OMOD_FormID -> [{name, value, fn}] from the OMOD *_Properties export."""
    props = {}
    for row in prop_rows:
        fid = row.get("OMOD_FormID", "")
        pname = row.get("PropertyName", "")
        if not fid or not pname:
            continue
        props.setdefault(fid, []).append({
            "name": pname,
            "value": (row.get("Value1", "") or "").strip(),
            "fn": row.get("FunctionType", ""),
        })
    return props


def load_omod_all(omod_rows):
    """Every OMOD by EDID -> {desc, full, formId}. Used to resolve custom-mod
    effect text (DESC / attached enchantment / damage bonus), which object
    templates reference but don't describe."""
    out = {}
    for row in omod_rows:
        edid = row.get("OMOD_EDID", "")
        if not edid:
            continue
        out[edid] = {
            "desc": row.get("DESC", "") or "",
            "full": row.get("FULL", "") or "",
            "formId": row.get("OMOD_FormID", ""),
        }
    return out


def load_weapon_mods(omod_rows, omod_props=None):
    omod_props = omod_props or {}
    mods = {}
    for row in omod_rows:
        edid = row.get("OMOD_EDID", "")
        if not (edid.startswith("Burn_Bounty_mod_") or edid.startswith("Bounty_Mod_")
                or edid.startswith("mod_Custom_Plasma_Abraxo") or edid.startswith("SDOW_")):
            continue
        raw_name = row.get("FULL", "") or row.get("DESC", "") or ""
        if not raw_name:
            raw_name = _clean_mod_name(edid)
        fid = row.get("OMOD_FormID", "")
        entry = {"name": raw_name, "desc": row.get("DESC", ""), "formId": fid}
        pr = omod_props.get(fid, [])
        if pr:
            entry["properties"] = pr
            for p in pr:
                if p["name"] == "DamageBonusMult" and p["value"]:
                    entry["damageMult"] = p["value"]
        mods[edid] = entry
    return mods


# ── Object templates: legendary + custom mod attachment ──────────────────────

_INCLUDE_RE  = re.compile(r'^(\S+)\s+"([^"]*)"\s+\[OMOD:([0-9A-Fa-f]+)\]')
_INCLUDE_RE2 = re.compile(r'^(\S+)\s+\[OMOD:([0-9A-Fa-f]+)\]')
_STD_MOD_SKIP = re.compile(
    r'(_Standard|_Null|_None|Barrel_Standard|Grip_Standard|Grip_StockFull|Mag_Null|'
    r'_Paint|Appearance|CustomName|SpecialEffect|Weapon_Paint|Scope_Sights|_SightsIron)',
    re.I)


def _parse_include_mod(cell):
    cell = (cell or "").strip()
    if not cell:
        return None
    m = _INCLUDE_RE.match(cell)
    if m:
        return {"edid": m.group(1), "name": m.group(2), "omodFormId": m.group(3)}
    m = _INCLUDE_RE2.match(cell)
    if m:
        return {"edid": m.group(1), "name": "", "omodFormId": m.group(2)}
    return None


def load_object_templates(otft_rows):
    """WEAP_EDID -> {combo_index -> {"name": combo_name, "mods": [include, ...]}}"""
    templates = {}
    for row in otft_rows:
        weid = row.get("WEAP_EDID", "")
        if not weid:
            continue
        ci = row.get("CombinationIndex", "")
        combos = templates.setdefault(weid, {})
        combo = combos.setdefault(ci, {"name": row.get("Combination_FULL", ""), "mods": []})
        if not combo["name"] and row.get("Combination_FULL"):
            combo["name"] = row.get("Combination_FULL", "")
        inc = _parse_include_mod(row.get("Include_Mod", ""))
        if inc:
            combo["mods"].append(inc)
    return templates


# Combination names that represent the enemy/bounty-spawned build.
_BOUNTY_COMBO_NAMES = ("bountyenchanted", "super slasher")


def resolve_template_mods(weap_edid, object_templates):
    """Pick the bounty combination for a weapon and split mods into
    legendary (mod_Legendary_WeaponN) and custom (signature) buckets."""
    combos = object_templates.get(weap_edid)
    if not combos:
        return [], []
    chosen = None
    for combo in combos.values():
        if (combo.get("name") or "").strip().lower() in _BOUNTY_COMBO_NAMES:
            chosen = combo
            break
    if chosen is None:
        for combo in combos.values():
            if (combo.get("name") or "").strip().lower() == "default":
                chosen = combo
                break
    if chosen is None:
        chosen = next(iter(combos.values()))

    legendary, custom = [], []
    for m in chosen["mods"]:
        edid = m["edid"]
        lm = re.search(r'mod_Legendary_Weapon(\d+)', edid, re.I)
        if lm:
            legendary.append({
                "star": int(lm.group(1)),
                "name": m["name"] or _clean_mod_name(edid),
                "edid": edid,
                "formId": m["omodFormId"],
            })
        elif _STD_MOD_SKIP.search(edid):
            continue
        elif re.search(r'(crmod_Custom|_mod_Custom|_mod_custom|Bounty_mod|SDOW_mod|SDOW_Mod_Custom)', edid, re.I):
            custom.append({"name": m["name"] or _clean_mod_name(edid),
                           "edid": edid, "formId": m["omodFormId"]})
    legendary.sort(key=lambda x: x["star"])
    return legendary, custom


def load_spell_effects(spel_eff_rows, mgef_lookup):
    """SPEL_EDID -> [{name, magnitude, duration, description}]"""
    out = {}
    for row in spel_eff_rows:
        edid = row.get("SPEL_EDID", "")
        if not edid:
            continue
        meid = row.get("EFID_MGEF_EDID", "")
        mfull = row.get("EFID_MGEF_FULL", "") or (mgef_lookup.get(meid, {}) or {}).get("name", "") or meid
        mag = safe_float(row.get("EFIT_Magnitude"))
        dur = safe_int(row.get("EFIT_Duration"))
        mdesc = (mgef_lookup.get(meid, {}) or {}).get("description", "")
        if mdesc:
            mdesc = mdesc.replace("<dur>", str(dur or ""))
        entry = {"name": mfull}
        if mag:
            entry["magnitude"] = mag
        if dur:
            entry["duration"] = dur
        if mdesc:
            entry["description"] = mdesc
        out.setdefault(edid, []).append(entry)
    return out


def load_spells(spel_rows):
    spells = {}
    for row in spel_rows:
        edid = row.get("SPEL_EDID", "") or row.get("EDID", "")
        if not edid.startswith("Burn_Bounty_") and not edid.startswith("SDOW_"):
            continue
        name = row.get("SPEL_FULL", "") or row.get("FULL", "") or edid
        fid = row.get("SPEL_FormID", "") or row.get("FormID", "")
        desc = row.get("SPEL_DESC", "") or row.get("DESC", "")
        spells[edid] = {"name": name, "formId": fid, "desc": desc}
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


def _fmt_num(n):
    f = float(n)
    return str(int(f)) if f.is_integer() else str(round(f, 3))


def describe_enchantment(ench_data, mgef_lookup):
    results = []
    for eff in ench_data.get("effects", []):
        mgef = mgef_lookup.get(eff["mgefEdid"], {})
        name = (mgef.get("name") or eff["mgefEdid"]).replace("DVMF_", "").replace("Burn_ME_", "")
        name = re.sub(r"^Damage Type\s+", "", name)
        mag = eff.get("magnitude") or 0
        dur = eff.get("duration") or 0
        desc = {"effect": name}
        if mag:
            desc["magnitude"] = mag
        if dur:
            desc["duration"] = dur
        if mgef.get("description"):
            desc["description"] = mgef["description"].replace("<dur>", str(dur or ""))
        # Human-readable line for a damage-over-time style effect.
        if mag and dur:
            desc["text"] = f"{_fmt_num(mag)} damage per second for {dur} seconds"
        elif mag:
            desc["text"] = f"Magnitude {_fmt_num(mag)}"
        results.append(desc)
    return results


def custom_mod_effect(edid, omod_all, omod_props, enchantments, mgef_lookup):
    """Human-readable effect for a weapon custom mod, resolved from TSVs:
      1) an attached Enchantment property  -> DoT / effect text (e.g. bleed)
      2) the OMOD DESC field               -> e.g. "Increases Damage by 10%..."
      3) a DamageBonusMult property        -> "+N% damage"
    Returns None if nothing is derivable."""
    info = omod_all.get(edid)
    if not info:
        return None
    props = omod_props.get(info.get("formId", ""), [])
    # 1) Enchantment property (Value1 = "ENCH_EDID ""Name"" [ENCH:xxxx]")
    for p in props:
        if p.get("name") == "Enchantments" and p.get("value"):
            ench_edid = p["value"].split(" ", 1)[0].strip()
            ench = enchantments.get(ench_edid)
            if ench:
                texts = []
                for e in describe_enchantment(ench, mgef_lookup):
                    t = e.get("text") or e.get("effect")
                    if t:
                        texts.append(t)
                if texts:
                    return " · ".join(texts)
    # 2) DESC
    desc = (info.get("desc") or "").strip()
    if desc:
        return desc
    # 3) DamageBonusMult
    for p in props:
        if p.get("name") == "DamageBonusMult" and p.get("value"):
            try:
                pct = round(float(p["value"]) * 100)
                if pct:
                    return f"+{pct}% damage"
            except (ValueError, TypeError):
                pass
    return None


def enrich_custom_mod_effects(weapon, omod_all, omod_props, enchantments, mgef_lookup):
    """Attach an 'effect' string to each custom mod on a weapon, where one is
    derivable from the OMOD TSVs."""
    if not weapon:
        return
    for cm in weapon.get("customMods", []) or []:
        eff = custom_mod_effect(cm.get("edid", ""), omod_all, omod_props,
                                enchantments, mgef_lookup)
        if eff:
            cm["effect"] = eff


def _weapon_stats_block(stats):
    return {
        "weaponType": stats.get("weaponType", ""),
        "speed": stats.get("speed", 0),
        "maxRange": stats.get("maxRange", 0),
        "ammo": stats.get("ammo", ""),
        "capacity": stats.get("capacity", 0),
    }


def resolve_weapon(lvli_edid, weapon_lvlis, weapon_names, weapon_stats, object_templates=None):
    data = weapon_lvlis.get(lvli_edid)
    if not data or not data.get("weapRefs"):
        return None
    ref = data["weapRefs"][0]
    base_name = weapon_names.get(ref["edid"], "")
    friendly = derive_weapon_name(lvli_edid, base_name)
    weapon = {"name": friendly, "baseName": base_name, "baseEdid": ref["edid"],
              "lvliEdid": lvli_edid, "lvliFormId": data["lvliFormId"]}
    weapon.update(_weapon_stats_block(weapon_stats.get(ref["edid"], {})))
    if object_templates:
        legendary, custom = resolve_template_mods(ref["edid"], object_templates)
        if legendary:
            weapon["legendaryMods"] = legendary
        if custom:
            weapon["customMods"] = custom
    return weapon


def resolve_weapon_by_edid(weap_edid, weapon_names, weapon_stats, object_templates=None):
    """Resolve a weapon directly by its WEAP EDID (for NPC weapons not wired
    through a bounty LVLI, e.g. the Slasher party crasher's Bowie Knife)."""
    weapon = {"name": weapon_names.get(weap_edid, "") or _clean_mod_name(weap_edid),
              "baseName": weapon_names.get(weap_edid, ""), "baseEdid": weap_edid}
    weapon.update(_weapon_stats_block(weapon_stats.get(weap_edid, {})))
    if object_templates:
        legendary, custom = resolve_template_mods(weap_edid, object_templates)
        if legendary:
            weapon["legendaryMods"] = legendary
        if custom:
            weapon["customMods"] = custom
    return weapon


def assemble_boss(boss_info, gang_data, weapon_lvlis, weapon_names, weapon_stats,
                  enchantments, mgef_lookup, weapon_mods, spells,
                  object_templates=None, spell_effects=None):
    spell_effects = spell_effects or {}
    gidx = boss_info["gangIndex"]
    wmap = GANG_WEAPON_MAP.get(gidx, {})
    boss = {"name": boss_info["bossName"], "gangIndex": gidx, "gangName": gang_data.get("name", "") if gang_data else "", "gangEdid": gang_data.get("edid", "") if gang_data else "", "imageSlug": slugify(boss_info["bossName"])}
    if wmap.get("weapon"):
        w = resolve_weapon(wmap["weapon"], weapon_lvlis, weapon_names, weapon_stats, object_templates)
        if w:
            boss["weapon"] = w
    if wmap.get("enchant"):
        e = enchantments.get(wmap["enchant"])
        if e:
            boss["enchantment"] = {"name": e["name"], "edid": wmap["enchant"], "effects": describe_enchantment(e, mgef_lookup)}
    if wmap.get("mod"):
        m = weapon_mods.get(wmap["mod"])
        if m:
            wm = {"name": m["name"], "desc": m["desc"], "edid": wmap["mod"]}
            if m.get("damageMult"):
                wm["damageMult"] = m["damageMult"]
            if m.get("properties"):
                wm["properties"] = m["properties"]
            boss["weaponMod"] = wm
    if wmap.get("grenade"):
        g = resolve_weapon(wmap["grenade"], weapon_lvlis, weapon_names, weapon_stats, object_templates)
        if g:
            boss["grenade"] = g
    if wmap.get("melee"):
        ml = resolve_weapon(wmap["melee"], weapon_lvlis, weapon_names, weapon_stats, object_templates)
        if ml:
            boss["meleeWeapon"] = ml
    if wmap.get("spell"):
        sp = spells.get(wmap["spell"])
        if sp:
            ability = {"name": sp["name"], "edid": wmap["spell"]}
            if sp.get("desc"):
                ability["description"] = sp["desc"]
            effs = spell_effects.get(wmap["spell"])
            if effs:
                ability["effects"] = effs
            boss["specialAbility"] = ability
    return boss


def load_sidekick_info(kywd_rows):
    for row in kywd_rows:
        if row.get("EDID") == "Burn_Bounty_HeadHuntWave_SupportRustRaiders":
            return {"name": row.get("FULL", "Rust Raiders"), "edid": row.get("EDID", "")}
    return {"name": "Rust Raiders", "edid": ""}


def load_globs():
    path = newest("GLOB_Export_*.tsv")
    globs = {}
    if not path:
        return globs
    for row in read_tsv(path):
        fid = (row.get("FormID") or "").strip()
        try:
            globs[fid] = float(row.get("FLTV", "0"))
        except (ValueError, TypeError):
            globs[fid] = 0.0
    return globs


def build_slasher_group(weapon_names, weapon_stats, object_templates, sidekick_info, group_number):
    """The Reborn Pint-Sized Slasher — SDOW seasonal Head Hunt party crasher.
    NPC records aren't exported to TSV, so the boss identity is structural
    (from the Slasher content catalog); the Bowie Knife stats/mods still
    resolve from the WEAP export."""
    knife = resolve_weapon_by_edid(
        "SDOW_crBowieKnife_SlasherBoss", weapon_names, weapon_stats, object_templates)
    if not knife.get("name") or knife["name"].startswith("SDOW"):
        knife["name"] = "Bowie Knife"
        knife["baseName"] = "Bowie Knife"
    if not knife.get("weaponType"):
        knife["weaponType"] = "1H Melee"
    knife["note"] = "Silent · damage Tier 30"
    boss = {
        "name": "The Reborn Pint-Sized Slasher",
        "gangName": "Pint-Sized Phantoms",
        "gangEdid": "SDOW_PintSizedSlasherFaction",
        "race": "Ghoul",
        "imageSlug": "the-reborn-pint-sized-slasher",
        "boss3Star": True,
        "note": "3-star BIG bounty target that can crash an active Head Hunt.",
        "weapon": knife,
        "sidekick": {
            "name": sidekick_info["name"],
            "description": "The party crasher joins the existing Head Hunt adds — "
                           "the standard support wave fights alongside it.",
        },
    }
    return {
        "groupNumber": group_number,
        "seasonal": True,
        "channel": "pts",
        "label": "Slasher Party Crasher",
        "blurb": "Seasonal Shadows of the Dead of Winter (SDOW) party crasher. "
                 "Appears on the live site automatically once the Head Hunt toggle goes live.",
        "spawnGlob": "008FADB5",
        "bossCount": 1,
        "bosses": [boss],
    }


def main():
    print("[Head Hunt Bosses] Loading TSVs...")
    chal_path = newest("CHAL_Export_*.tsv")
    flst_path = newest("FLST_Export_*_Entries.tsv") or newest("FLST_Export_List_*_Entries.tsv")
    kywd_path = newest("KYWD_Export_*.tsv")
    lvli_path = newest("LVLI_Export_*_Entries.tsv") or newest("LVLI_Export_Full_*_LVLI_Entries.tsv")
    weap_base = newest("WEAP_Export_*_Base.tsv")
    weap_dnam = newest("WEAP_Export_*_DNAM.tsv")
    weap_otft = newest("WEAP_Export_*_ObjectTemplate.tsv")
    ench_path = newest("ENCH_Export_*.tsv")
    mgef_path = newest("MGEF_Export_*.tsv")
    _omod = tsv_source.newest(str(TSV_DIR / "OMOD_Export_*.tsv"),
                              exclude="Properties", required=False)
    omod_path = Path(_omod) if _omod else None
    omod_prop_path = newest("OMOD_Export_*_Properties.tsv")
    spel_path = newest("SPEL_Export_*_HEADER.tsv") or newest("SPEL_Export_*.tsv")
    spel_eff_path = newest("SPEL_Export_*_EFFECTS.tsv")

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
    otft_rows = read_tsv(weap_otft) if weap_otft else []
    ench_rows = read_tsv(ench_path)
    mgef_rows = read_tsv(mgef_path)
    omod_rows = read_tsv(omod_path) if omod_path else []
    omod_prop_rows = read_tsv(omod_prop_path) if omod_prop_path else []
    spel_rows = read_tsv(spel_path) if spel_path else []
    spel_eff_rows = read_tsv(spel_eff_path) if spel_eff_path else []

    print(f"  CHAL: {len(chal_rows)}  |  FLST: {len(flst_rows)}  |  MGEF: {len(mgef_rows)}  |  OTFT: {len(otft_rows)}")

    boss_groups = load_boss_groups(chal_rows)
    gang_keywords = load_gang_keywords(flst_rows)
    weapon_lvlis = load_weapon_lvlis(lvli_rows)
    weapon_names = load_weapon_names(weap_base_rows)
    weapon_stats = load_weapon_stats(weap_dnam_rows)
    object_templates = load_object_templates(otft_rows)
    enchantments = load_enchantments(ench_rows)
    mgef_lookup = load_magic_effects(mgef_rows)
    omod_props = load_omod_properties(omod_prop_rows)
    omod_all = load_omod_all(omod_rows)
    weapon_mods = load_weapon_mods(omod_rows, omod_props)
    spell_data = load_spells(spel_rows)
    spell_effects = load_spell_effects(spel_eff_rows, mgef_lookup)
    sidekick_info = load_sidekick_info(kywd_rows)
    head_metas, all_meta = load_head_hunt_metas(chal_rows)
    globs = load_globs()

    print(f"  Groups: {len(boss_groups)}  |  Gangs: {len(gang_keywords)}  |  Weapon LVLIs: {len(weapon_lvlis)}  |  ObjTemplates: {len(object_templates)}")

    output_groups = []
    for group_num, bosses in boss_groups.items():
        group_bosses = []
        for bi in bosses:
            gd = gang_keywords.get(bi["gangIndex"])
            bo = assemble_boss(bi, gd, weapon_lvlis, weapon_names, weapon_stats, enchantments, mgef_lookup, weapon_mods, spell_data, object_templates, spell_effects)
            # Custom-mod effect text (bleed DoT, +% damage, etc.) from OMOD TSVs.
            enrich_custom_mod_effects(bo.get("weapon"), omod_all, omod_props, enchantments, mgef_lookup)
            enrich_custom_mod_effects(bo.get("meleeWeapon"), omod_all, omod_props, enchantments, mgef_lookup)
            enrich_custom_mod_effects(bo.get("grenade"), omod_all, omod_props, enchantments, mgef_lookup)
            bo["sidekick"] = {"name": sidekick_info["name"], "description": "Support wave enemies that spawn alongside the boss. They carry standard weapons and add pressure during the fight."}
            if bi.get("chalEdid"):
                bo["challenge"] = {"edid": bi["chalEdid"], "formId": bi.get("chalFormId", ""), "name": bi.get("chalFull", "")}
            group_bosses.append(bo)
        grp = {"groupNumber": group_num, "bossCount": len(group_bosses), "bosses": group_bosses}
        meta = head_metas.get(group_num)
        if meta:
            grp["metaChallenge"] = meta
        output_groups.append(grp)

    numbered_count = len(output_groups)
    numbered_bosses = sum(g["bossCount"] for g in output_groups)

    # ── SDOW / Slasher party crasher (seasonal, PTS-gated) ───────────────────
    # Gate on channel or the live Head Hunt toggle — NEVER on record presence
    # (the SDOW records ship dormant in the live TSVs).
    is_pts_channel = os.environ.get("DFBNB_CHANNEL", "").strip().lower() == "pts"
    slasher_toggle_on = globs.get("008E0671", 0.0) >= 1.0     # LCP_SDOW_LTC_HeadHuntsToggle
    show_slasher = is_pts_channel or slasher_toggle_on
    if show_slasher:
        output_groups.append(
            build_slasher_group(weapon_names, weapon_stats, object_templates, sidekick_info, numbered_count + 1))

    result = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "channel": "pts" if is_pts_channel else "live",
        "groupCount": numbered_count,
        "totalBosses": numbered_bosses,
        "sidekickType": sidekick_info["name"],
        "slasherPartyCrasher": show_slasher,
        "allChallenge": all_meta,
        "groups": output_groups,
    }

    out_file = Path(os.environ["DFBNB_BOSSES_OUT"]) if os.environ.get("DFBNB_BOSSES_OUT") else OUT_FILE
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n[Head Hunt Bosses] Written -> {out_file}")
    print(f"  {result['groupCount']} groups, {result['totalBosses']} bosses  |  Slasher: {show_slasher}")
    for g in output_groups:
        label = g.get("label") or f"Group {g['groupNumber']}"
        names = ", ".join(b["name"] for b in g["bosses"])
        print(f"  {label}: {g['bossCount']} bosses - {names}")


if __name__ == "__main__":
    main()
