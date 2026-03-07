#!/usr/bin/env python3
"""
build_armour_json.py
====================
Generates dist/armour.json from xEdit TSV exports.

Input files (place in data/ folder or pass via --data-dir):
  COBJ_Export_March_2026.tsv
  BOOK_Export_March_2026.tsv

Output:
  dist/armour.json   — combined power-armour + body-armour + underarmour

Usage:
  python build_armour_json.py
  python build_armour_json.py --data-dir /path/to/tsvs --out dist/armour.json
"""

import csv
import json
import os
import re
import sys
import argparse
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
#  LVLI → human-readable obtain source
# ─────────────────────────────────────────────────────────────────────────────
LVLI_MAP = {
    # Power Armour mod pools
    "LLS_Recipes_Mods_PowerArmor_T45_Tier":         "World drop / vendor (T-45 mod pool)",
    "LLS_Recipes_Mods_PowerArmor_T51_Tier":         "World drop / vendor (T-51b mod pool)",
    "LLS_Recipes_Mods_PowerArmor_T60_Tier":         "World drop / vendor (T-60 mod pool)",
    "LLS_Recipes_Mods_PowerArmor_Raider_Tier":      "World drop / vendor (Raider PA mod pool)",
    "LLS_Recipes_Mods_PowerArmor_Excavator_Tier":   "World drop / vendor (Excavator mod pool)",
    "LLS_Recipes_Mods_PowerArmor_Ultracite_Tier":   "World drop — Ultracite mod pool (no vendor)",
    "LLS_Recipes_Mods_PowerArmor_All_JetPacks":     "World drop / vendor — all PA jet pack plans",
    # Gold bullion vendors
    "W05_LLV_GoldVendor_Recipes_Mods_Armor_PowerArmor_T65": "Purveyor Murmrgh (gold bullion)",
    "W05_LLV_GoldVendor_Recipes_Mods_Armor_SecretService":  "Reginald Stone / Purveyor (gold bullion)",
    "W05_LLV_GoldVendor_Recipes_Armor_SecretService":       "Reginald Stone / Purveyor (gold bullion)",
    "W05_LLV_GoldVendor_SecretService_Reginald":            "Reginald Stone (gold bullion)",
    "W05_LLV_GoldVendor_Settler_Samuel":                    "Samuel (gold bullion vendor)",
    "W05_LLV_GoldVendor_Raider_Mortimer":                   "Mortimer (gold bullion vendor)",
    "LLV_GoldVendor_BOSInfantry_Mods":                      "Gold bullion vendor — Brotherhood Recon mods",
    "LLV_GoldVendor_Hellcat_Mods":                          "Gold bullion vendor — Hellcat mods",
    "LLV_GoldVendor":                                       "Gold bullion vendor",
    # Minerva
    "BS02_SpecialVendor_Minerva":                    "Minerva (travelling vendor)",
    # Expeditions / Raids
    "RD01_LLS_Raids_Rewards":                        "Enclave Expeditions (raids)",
    "XPD_Pitt_LL_Mission":                           "The Pitt expedition",
    "XPD_AC_LL_Mission":                             "Atlantic City expedition",
    "XPD_LLV_ExpeditionVendor":                      "Atlantic City expedition vendor",
    "P62_LLS_Drifter_Rewards_XPDAtlanticCity":       "Atlantic City (Drifter rewards)",
    "P62_LLS_Drifter_Rewards_XPDThePitt":            "The Pitt (Drifter rewards)",
    # Atlantic City quests
    "AC_MQ":                                         "Atlantic City main quest reward",
    "AC_SQ":                                         "Atlantic City side quest reward",
    # Events
    "E01B_Encryptid":                                "Encryptid event",
    "E03A_SpookyScorched":                           "Spooky Scorched / seasonal event",
    "E06_Colossus":                                  "Colossal Problem event",
    "E08A_Moonshine":                                "Moonshine Jamboree event",
    "MOON_":                                         "Blue Ridge / Moonshine Jamboree event",
    "BS02_E01_Metal":                                "Metal Dome event",
    "LLS_MutatedEvents":                             "Mutated public events",
    "LLS_TreasureHunt":                              "Treasure Hunter event",
    "LLS_Festive":                                   "Seasonal / holiday event",
    "E09A_Launcher":                                 "Invaders from Beyond event",
    "E07A_Mothman":                                  "Mothman Equinox event",
    "Storm_RegionBoss":                              "STORM regional boss drop",
    # Faction vendors
    "LLV_Faction_BoS":                               "Brotherhood of Steel faction vendor",
    "LLV_Faction_Raiders":                           "Raiders faction vendor",
    "LLV_Faction_FreeStates":                        "Free States faction vendor",
    "ENB_Vendor_ProductionCenter_Faction_Enclave":   "Enclave vendor",
    "LLE_OrbitalDrop_Enclave":                       "Enclave Orbital Drop event",
    "LLV_Vendor_Recipes_Underarmor_Faction_Raider":  "Raiders faction vendor",
    "LLV_Vendor_Recipes_Base_Faction_BoS":           "Brotherhood of Steel faction vendor",
    "LLV_Vendor_Recipes_Faction_Enclave":            "Enclave faction vendor",
    "LL_BoS_Recipes":                                "Brotherhood of Steel vendor",
    "LL_Vendor_Trainstations":                       "Train station vendor",
    # Daily Ops
    "LL_DailyOps":                                   "Daily Ops reward",
    # Regional / world drops
    "LLS_Recipes_Mods_Armor_Region":                 "World drop / vendor (regional mod pool)",
    "LLS_Recipes_Armor_RegionCranberryBog":          "World drop — Cranberry Bog region",
    "LLS_Recipes_Armor_RegionMire":                  "World drop — The Mire region",
    "LLS_Recipes_Armor_RegionSavageDivide":          "World drop — Savage Divide region",
    "LLS_Recipes_Armor_RegionAshHeap":               "World drop — Ash Heap region",
    "LLS_Recipes_Armor_RegionBurningSprings":        "World drop — Burning Springs region",
    "LLS_Recipes_Armor_RegionForest":                "World drop — The Forest region",
    "LLS_Recipes_Armor_RegionToxicValley":           "World drop — Toxic Valley region",
    "LLS_Recipes_Armor_RegionSkylineValley":         "World drop — Skyline Valley region",
    "LLS_Recipes_Armor_Region":                      "World drop (regional armour pool)",
    "QuestReward_LLS_Schematic_Armor":               "Schematic quest reward",
    # Body armour world pools
    "LLS_Recipes_Armor_Combat":                      "World drop / vendor (Combat Armor pool)",
    "LLS_Recipes_Armor_Leather":                     "World drop / vendor (Leather Armor pool)",
    "LLS_Recipes_Armor_Metal":                       "World drop / vendor (Metal Armor pool)",
    "LLS_Recipes_Armor_Raider":                      "World drop / vendor (Raider Armor pool)",
    "LLS_Recipes_Armor_Robot":                       "World drop / vendor (Robot Armor pool)",
    "LL_Recipes_Armor_Marine":                       "World drop (Marine Armor pool)",
    "LL_Recipes_Armor_Trapper":                      "World drop (Trapper Armor pool)",
    "LL_Recipes_Armor_Set_V94_Acid":                 "Strangler Heart — Savage Divide loot",
    "LL_Recipes_Armor_Set_V94_Bleed":                "Thorn Armor — Savage Divide loot",
    "LL_Recipes_Armor_Set_V94_Solar":                "Solar Armor — Savage Divide loot",
    "LL_Recipes_Armor_Set_V94":                      "Savage Divide special loot",
    # Underarmour lining pools
    "LLS_Recipe_Mod_UnderArmor":                     "World drop (underarmour lining pool)",
    # Special
    "LLS_Systemic_Rewards_Armor_Mods":               "World drop / vendor (armour mod pool)",
    "LLS_Systemic_Rewards_Armor_Plans":              "World drop / vendor (armour plan pool)",
    "LLS_Systemic_Rewards":                          "Seasonal reward / world drop",
    "LLS_Creature_ScorchbeastQueen":                 "Scorchbeast Queen drop",
    "LPI_Recipes_PowerArmor_Mods_Paint_NukaCola":    "Nuka-World on Tour vendor",
    "LPI_Recipes_PowerArmor_Mods_Paint_NukaQuantum": "Nuka-World on Tour vendor",
    # Quest rewards
    "BoSr01_LL_Quest_Reward":                        "Brotherhood of Steel quest reward",
    "BS02_MQ04":                                     "Steel Reign quest reward",
    "FS01_MQ":                                       "Free States main quest reward",
    "SFS09_Habitat":                                 "Skyline Valley quest reward",
    "Burn_MQ":                                       "Burning Springs main quest reward",
    "Burn_LL_GeneralVendor":                         "Burning Springs vendor",
    "ATX_LL_COMP_Vendor_Scarberry":                  "Atom Shop / Cult vendor",
    "LLS_XPD_AC_LL":                                 "Atlantic City reward",
    "LLS_Generic_Recipes":                           "World drop (generic loot pool)",
}

def resolve_lvli(edid):
    edid = edid.strip('"').strip()
    for prefix, label in LVLI_MAP.items():
        if edid.startswith(prefix):
            return label
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  FILTERING
# ─────────────────────────────────────────────────────────────────────────────
SKIP_SUFFIXES = ['NONPLAYABLE', '_NotPlayable', 'NPCONLY', 'REPAIRONLY',
                 'UNPLAYABLE', 'DEPRECATED']
SKIP_PREFIXES = ['DEL', 'CUT', 'POST_', 'zzz', 'ZZZ', 'zzzz', 'ZZZZ',
                 'DEBUG', 'BOUNTY', 'Fishing', 'Meat']

def should_skip(edid):
    e = str(edid or '').strip('"')
    for p in SKIP_PREFIXES:
        if e.startswith(p):
            return True
    for s in SKIP_SUFFIXES:
        if s in e:
            return True
    return False

def is_atx(edid):
    e = str(edid or '').strip('"')
    return e.startswith('ATX_') or e.startswith('SCORE_')


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 1 — build plan_map from BOOK TSV
#  plan_map[cobj_form_id] = { planName, planEdid, planFormId, obtainSources }
# ─────────────────────────────────────────────────────────────────────────────
def build_plan_map(book_path):
    plan_map = {}
    with open(book_path, encoding='latin-1') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # skip header
        for row in reader:
            if len(row) < 3:
                continue
            book_fid  = row[0].strip('"')
            book_edid = row[1].strip('"')
            book_full = row[2].strip('"')
            lvlis = []
            cobj_fids = []
            for cell in row[17:]:
                cell = cell.strip('"').strip()
                if not cell:
                    continue
                parts = cell.split(':')
                if len(parts) < 3:
                    continue
                fid, eid, sig = parts[0], parts[1], parts[2]
                if sig == 'LVLI':
                    r = resolve_lvli(eid)
                    if r and r not in lvlis:
                        lvlis.append(r)
                elif sig == 'COBJ':
                    cobj_fids.append(fid)
            for cf in cobj_fids:
                plan_map[cf] = {
                    'planName':     book_full,
                    'planEdid':     book_edid,
                    'planFormId':   book_fid,
                    'obtainSources': lvlis,
                }
    return plan_map


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 2 — load all relevant COBJ records
# ─────────────────────────────────────────────────────────────────────────────
ARMOUR_KEYWORDS = [
    'PowerArmor', 'UnderArmor', 'mod_armor', 'SecretService',
    'BOSInfantry', 'Botsmith', 'Trapper', 'Marine', 'Scout',
    'Combat', 'Robot', 'Leather', 'Metal', 'Wood', 'Raider',
    'Civil', 'XPD_AC', 'RD01', 'Vulcan', 'T65', 'Hellcat',
    'Union', 'STORM',
]

def load_cobj_records(cobj_path, plan_map):
    records = []
    with open(cobj_path, encoding='latin-1') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row in reader:
            if len(row) < 9:
                continue
            cobj_fid  = row[0].strip('"')
            cobj_edid = row[1].strip('"')
            cnam_fid  = row[2].strip('"')
            cnam_edid = row[3].strip('"')
            cnam_full = row[4].strip('"')
            gnam_edid = row[6].strip('"')
            fnam_kw   = row[8].strip('"')
            fvpa      = row[9].strip('"')

            if should_skip(cobj_edid):
                continue
            if not any(kw in cobj_edid for kw in ARMOUR_KEYWORDS):
                continue

            pm = plan_map.get(cobj_fid, {})
            records.append({
                'cobjFormId':    cobj_fid,
                'cobjEdid':      cobj_edid,
                'modEdid':       cnam_edid,
                'modFull':       cnam_full,
                'workbenchEdid': gnam_edid,
                'keywords':      fnam_kw,
                'materials':     fvpa,
                'isATX':         is_atx(cobj_edid),
                'planName':      pm.get('planName', ''),
                'planEdid':      pm.get('planEdid', ''),
                'planFormId':    pm.get('planFormId', ''),
                'obtainSources': pm.get('obtainSources', []),
            })
    return records


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY BUILDER  (shared by all three pages)
# ─────────────────────────────────────────────────────────────────────────────
NO_MOD_LABELS = {
    'Modifications':  'No Modification',
    'Headlamp':       'No Headlamp',
    'Jet Pack':       'No Jet Pack',
    'Material':       'Standard (No Upgrade)',
    'Material/Paint': 'No Paint',
    'Misc / Lining':  'No Lining',
    'Paint':          'No Paint',
    'Base Garment':   'No Base Garment',
    'Lining':         'No Lining',
    'Style':          'No Style',
}

def make_no_mod(slot, id_suffix):
    return {
        'id':           f'nomod_{id_suffix}',
        'name':         NO_MOD_LABELS.get(slot, 'No Mod'),
        'isNoMod':      True,
        'hasPlan':      False,
        'isATX':        False,
        'obtainText':   'Default — no mod applied',
        'technicalText':'No modification slot state',
    }

def make_entry(r):
    has_plan = bool(r['planName'])
    atx      = r['isATX']
    srcs     = r['obtainSources']

    if not has_plan and not atx:
        obtain = 'No plan required — crafted by default at workbench'
    elif atx:
        obtain = 'Atom Shop / S.C.O.R.E. cosmetic — no plan required'
        if has_plan:
            obtain = f"{r['planName']}\nAtom Shop / S.C.O.R.E. cosmetic"
    else:
        src_str = '\n'.join(srcs) if srcs else 'Source unknown'
        obtain  = f"{r['planName']}\n{src_str}"

    tech = f"EDID: {r['cobjEdid']}\nFormID: {r['cobjFormId']}"
    if r['planFormId']:
        tech += f"\nPlan FormID: {r['planFormId']}"

    return {
        'id':           r['cobjFormId'],
        'edid':         r['cobjEdid'],
        'name':         r['modFull'] or r['modEdid'],
        'hasPlan':      has_plan,
        'isATX':        atx,
        'planName':     r['planName'],
        'obtainText':   obtain,
        'technicalText':tech,
    }

def sort_entries(entries):
    return sorted(entries, key=lambda e: (
        2 if e.get('isATX') else (1 if e.get('hasPlan') else 0)
    ))

def with_no_mod(slot, entries, id_suffix):
    return [make_no_mod(slot, id_suffix)] + sort_entries(entries)


# ─────────────────────────────────────────────────────────────────────────────
#  POWER ARMOUR
# ─────────────────────────────────────────────────────────────────────────────
PA_SET_NAMES = {
    'T45': 'T-45',  'T51': 'T-51b', 't51': 'T-51b', 'T60': 'T-60',
    'X01': 'X-01',  'Raider': 'Raider', 'Excavator': 'Excavator',
    'Ultracite': 'Ultracite', 'T65': 'T-65', 'Hellcat': 'Hellcat',
    'Union': 'Union', 'Vulcan': 'Vulcan', 'EnclaveVulcan': 'Vulcan',
    'STORM': 'S.T.O.R.M.', 'Set': 'Strangler Heart',
}
PA_SET_ORDER = ['T45','T51','T60','X01','Raider','Excavator','Ultracite',
                'T65','Hellcat','Union','Vulcan','Set']
PA_PIECE_ORDER = ['Helmet','Left Arm','Right Arm','Left Leg','Right Leg','Torso']
PA_SLOT_ORDER  = ['Modifications','Headlamp','Jet Pack','Material/Paint']

def _canonical_pa_set(sk):
    if sk == 't51':          return 'T51'
    if sk == 'EnclaveVulcan': return 'Vulcan'
    return sk

def _pa_slot(rest):
    r = rest.lower()
    if r.startswith('misc_jetpack') or '_jetpack_' in r or r == 'misc_jetpack':
        return 'Jet Pack'
    if r.startswith('misc_'):
        return 'Modifications'
    if r.startswith('headlamp'):
        return 'Headlamp'
    return 'Material/Paint'

def _classify_pa(edid):
    """Returns list of (set_key, piece, slot)."""
    e = re.sub(r'^(ATX_|SCORE_\S+?_|RD01_)', '', edid)
    m = re.match(
        r'co_mod_PowerArmor_(\w+)'
        r'_(Helmet|Head|ArmLeft|ArmRight|LegLeft|LegRight|Arm|Leg|Torso)_(.*)',
        e)
    if not m:
        return []
    sk   = m.group(1)
    p_raw = m.group(2)
    rest = m.group(3)
    if sk not in PA_SET_NAMES:
        return []
    slot = _pa_slot(rest)
    piece_map = {
        'Helmet':   ['Helmet'],     'Head':     ['Helmet'],
        'ArmLeft':  ['Left Arm'],   'ArmRight': ['Right Arm'],
        'LegLeft':  ['Left Leg'],   'LegRight': ['Right Leg'],
        'Arm':      ['Left Arm','Right Arm'],
        'Leg':      ['Left Leg','Right Leg'],
        'Torso':    ['Torso'],
    }
    return [(_canonical_pa_set(sk), piece, slot)
            for piece in piece_map.get(p_raw, [p_raw])]

def build_power_armour(records):
    pa_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    seen = set()
    for r in records:
        for set_key, piece, slot in _classify_pa(r['cobjEdid']):
            key = (r['cobjFormId'], set_key, piece, slot)
            if key in seen: continue
            seen.add(key)
            pa_data[set_key][piece][slot].append(make_entry(r))

    output_sets = []
    added_keys  = set()
    for sk in PA_SET_ORDER:
        canon = _canonical_pa_set(sk)
        if canon not in pa_data or canon in added_keys:
            continue
        added_keys.add(canon)
        pieces_out = []
        for piece in PA_PIECE_ORDER:
            if piece not in pa_data[canon]: continue
            slots_out = []
            for slot in PA_SLOT_ORDER:
                if slot not in pa_data[canon][piece]: continue
                mods = with_no_mod(
                    slot,
                    pa_data[canon][piece][slot],
                    f'{canon}_{piece.replace(" ","_")}_{slot.replace("/","_").replace(" ","_")}',
                )
                slots_out.append({'slot': slot, 'mods': mods})
            if slots_out:
                pieces_out.append({'piece': piece, 'slots': slots_out})
        if pieces_out:
            output_sets.append({
                'setKey':  canon,
                'setName': PA_SET_NAMES[sk],
                'pieces':  pieces_out,
            })
    return {'pageTitle': 'Power Armour Plans & Mods', 'sets': output_sets}


# ─────────────────────────────────────────────────────────────────────────────
#  BODY ARMOUR
# ─────────────────────────────────────────────────────────────────────────────
BA_SET_CONFIG = [
    {'key':'Wood',          'name':'Wood Armor',              'pattern': r'^co_mod_armor_Wood_'},
    {'key':'Leather',       'name':'Leather Armor',           'pattern': r'^co_mod_armor_Leather_'},
    {'key':'Metal',         'name':'Metal Armor',             'pattern': r'^co_mod_armor_Metal_'},
    {'key':'RaiderMod',     'name':'Raider Armor',            'pattern': r'^co_mod_armor_RaiderMod_'},
    {'key':'Combat',        'name':'Combat Armor',            'pattern': r'^co_mod_armor_Combat_'},
    {'key':'Robot',         'name':'Robot Armor',             'pattern': r'^co_DLC01_mod_armor_Robot_'},
    {'key':'Marine',        'name':'Marine Armor',            'pattern': r'^co_DLC03_mod_armor_Marine_'},
    {'key':'Trapper',       'name':'Trapper Armor',           'pattern': r'^co_DLC03_mod_armor_Trapper_'},
    {'key':'SecretService', 'name':'Secret Service Armor',    'pattern': r'^(co_mod_armor_SecretService_|co_mod_SecretService_Jetpack)'},
    {'key':'BOSInfantry',   'name':'Brotherhood Recon Armor', 'pattern': r'^(co_mod_armor_BOSInfantry_|co_BOSInfantry_)'},
    {'key':'CivilEngineer', 'name':'Civil Engineer Armor',    'pattern': r'^co_XPD_AC_mod_armor_(Muni|BOSInfantry)'},
    {'key':'Botsmith',      'name':'Botsmith Armor',          'pattern': r'^co_mod_Botsmith_'},
]
BA_PIECE_ORDER = ['Helmet','Left Arm','Right Arm','Left Leg','Right Leg','Torso']
BA_SLOT_ORDER  = ['Material','Misc / Lining','Jet Pack','Paint']

def _classify_ba(edid):
    """Returns (piece_label, slot)."""
    e_low = edid.lower()
    # Piece
    if '_helmet_' in e_low or e_low.endswith('_helmet') or '_head_' in e_low:
        piece = 'Helmet'
    elif '_torso_' in e_low or e_low.endswith('_torso'):
        piece = 'Torso'
    elif '_limbarm_' in e_low or '_arm_' in e_low:
        piece = 'Arm'
    elif '_limbleg_' in e_low or '_leg_' in e_low:
        piece = 'Leg'
    elif '_limb_' in e_low:
        piece = 'Limb'
    else:
        piece = 'Torso'  # fallback

    # Slot
    if '_jetpack' in e_low:
        slot = 'Jet Pack'
    elif re.search(r'_material_\d', e_low) or '_material_' in e_low:
        slot = 'Material'
    elif '_lining_' in e_low:
        slot = 'Misc / Lining'
    elif '_paint_' in e_low:
        slot = 'Paint'
    else:
        slot = 'Misc / Lining'

    return (piece, slot)

def _expand_ba_piece(piece):
    if piece == 'Arm':    return ['Left Arm','Right Arm']
    if piece == 'Leg':    return ['Left Leg','Right Leg']
    if piece == 'Limb':   return ['Left Arm','Right Arm','Left Leg','Right Leg']
    return [piece]

def build_body_armour(records):
    output_sets = []
    for cfg in BA_SET_CONFIG:
        pat  = re.compile(cfg['pattern'])
        recs = [r for r in records if pat.match(r['cobjEdid'])]
        if not recs:
            continue

        piece_slot_mods = defaultdict(lambda: defaultdict(list))
        seen = set()
        for r in recs:
            p_raw, slot = _classify_ba(r['cobjEdid'])
            for piece in _expand_ba_piece(p_raw):
                key = (r['cobjFormId'], piece, slot)
                if key in seen: continue
                seen.add(key)
                piece_slot_mods[piece][slot].append(make_entry(r))

        pieces_out = []
        for piece in BA_PIECE_ORDER:
            if piece not in piece_slot_mods: continue
            slots_out = []
            for slot in BA_SLOT_ORDER:
                if slot not in piece_slot_mods[piece]: continue
                mods = with_no_mod(
                    slot,
                    piece_slot_mods[piece][slot],
                    f"{cfg['key']}_{piece.replace(' ','_')}_{slot.replace(' ','_').replace('/','_')}",
                )
                slots_out.append({'slot': slot, 'mods': mods})
            if slots_out:
                pieces_out.append({'piece': piece, 'slots': slots_out})

        if pieces_out:
            output_sets.append({
                'setKey':  cfg['key'],
                'setName': cfg['name'],
                'pieces':  pieces_out,
            })
    return {'pageTitle': 'Body Armour Plans & Mods', 'sets': output_sets}


# ─────────────────────────────────────────────────────────────────────────────
#  UNDERARMOUR
# ─────────────────────────────────────────────────────────────────────────────
LINING_NAMES = {
    1: 'Standard Lining', 2: 'Treated Lining', 3: 'Resistant Lining',
    4: 'Protective Lining', 5: 'Shielded Lining',
}

UA_TYPES_CONFIG = [
    {
        'typeKey': 'Casual', 'typeName': 'Casual',
        'baseGarments': [
            {'name': 'Undershirt & Jeans',       'planEdid': 'recipe_Armor_Casual_Underarmor_ShirtJeans',   'nocraft': False},
            {'name': 'Flannel Shirt and Jeans',  'planEdid': 'recipe_Armor_Casual_Underarmor_FlannelJeans', 'nocraft': False},
            {'name': 'Military Fatigues',         'planEdid': '',                                            'nocraft': True, 'note': 'World drop — no plan'},
        ],
        'liningPrefix': 'co_mod_UnderArmor_Casual_MK',
        'styleEdid':    'co_mod_UnderArmor_style_Casual',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_Casual',
    },
    {
        'typeKey': 'Raider', 'typeName': 'Raider',
        'baseGarments': [
            {'name': 'Long Johns',    'planEdid': 'recipe_Armor_Raider_Underarmor_LongJohns',      'nocraft': False},
            {'name': 'Harness',       'planEdid': 'recipe_Armor_Raider_Underarmor_Harness',        'nocraft': False},
            {'name': 'Road Leathers', 'planEdid': 'recipe_Armor_Raider_Underarmor_RoadLeathers',   'nocraft': False},
            {'name': 'Raider Leathers','planEdid':'recipe_Armor_Raider_Underarmor_RaiderLeathers', 'nocraft': False},
        ],
        'liningPrefix': 'co_mod_UnderArmor_Raider_MK',
        'styleEdid':    'co_mod_UnderArmor_style_Raider',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_Raider',
    },
    {
        'typeKey': 'BoS', 'typeName': 'Brotherhood of Steel',
        'baseGarments': [
            {'name': 'Brotherhood Soldier Suit', 'planEdid': 'recipe_Armor_BoS_Soldier_Underarmor', 'nocraft': False},
            {'name': 'Brotherhood Knight Suit',  'planEdid': 'recipe_Armor_BoS_Knight_Underarmor',  'nocraft': False},
            {'name': 'Brotherhood Officer Suit', 'planEdid': 'recipe_Armor_BoS_Officer_Underarmor', 'nocraft': False},
            {'name': 'Blue Ridge Infantry Uniform','planEdid':'',                                   'nocraft': True, 'note': 'World drop — no plan'},
        ],
        'liningPrefix': 'co_mod_UnderArmor_BoS_MK',
        'styleEdid':    'co_mod_UnderArmor_style_BoS',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_BoS',
    },
    {
        'typeKey': 'Enclave', 'typeName': 'Enclave (Operative)',
        'baseGarments': [
            {'name': 'Forest Operative Underarmor',          'planEdid': '', 'nocraft': True,  'note': 'Cannot be crafted — world / event drop'},
            {'name': 'Urban Operative Underarmor',           'planEdid': '', 'nocraft': True,  'note': 'Cannot be crafted — world / event drop'},
            {'name': 'Enclave Secret Operative Underarmor',  'planEdid': 'RD01_Recipe_Armor_EnclaveUnderArmorUniform_SecretOperat', 'nocraft': False},
        ],
        'liningPrefix': 'co_mod_UnderArmor_Enclave_MK',
        'styleEdid':    'co_mod_UnderArmor_style_Enclave',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_Enclave',
    },
    {
        'typeKey': 'Marine', 'typeName': 'Marine',
        'baseGarments': [
            {'name': 'Marine Wetsuit',       'planEdid': 'recipe_Armor_Marine_Underarmor_Wetsuit',  'nocraft': False},
            {'name': 'Marine Tactical Helmet','planEdid':'recipe_Armor_Marine_Underarmor_Helmet',   'nocraft': False, 'note': 'Separate head item — not underarmour slot'},
        ],
        'liningPrefix': 'co_mod_UnderArmor_MarineWetsuit_MK',
        'styleEdid':    'co_mod_UnderArmor_style_MarineWetsuit',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_Marine',
    },
    {
        'typeKey': 'VaultSuit', 'typeName': 'Vault Suit',
        'baseGarments': [
            {'name': 'Vault 76 Jumpsuit',          'planEdid': 'recipe_Armor_VaultSuit76_Underarmor_Clean', 'nocraft': False},
            {'name': 'Vault Tec University Jumpsuit','planEdid':'recipe_Armor_VaultSuitVT_Underarmor',       'nocraft': False},
            {'name': 'Vault 94 Jumpsuit',           'planEdid': 'Recipe_Armor_VaultSuit94_Underarmor',      'nocraft': False},
        ],
        'liningPrefix': 'co_mod_UnderArmor_VaultSuit_MK',
        'styleEdid':    'co_mod_UnderArmor_style_VaultSuit',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_VaultSuit',
        'liningNote':   'Vault Suit linings are learned via Overseer\'s Logs — not dropped plans',
    },
    {
        'typeKey': 'SecretService', 'typeName': 'Secret Service',
        'baseGarments': [
            {'name': 'Secret Service Underarmor', 'planEdid': 'W05_Recipe_Armor_Underarmor_SecretService_Uniform_GoldV', 'nocraft': False},
        ],
        'liningPrefix': 'co_mod_UnderArmor_SecretService_MK',
        'styleEdid':    'co_mod_UnderArmor_style_SecretService',
        'stylePlanEdid':'recipe_mod_armor_UnderArmor_style_SecretService',
    },
    {
        'typeKey': 'CivilEngineer', 'typeName': 'Civil Engineer',
        'baseGarments': [
            {'name': 'Civil Engineer Underarmor', 'planEdid': 'Recipe_XPD_AC_Armor_Muni_Underarmor', 'nocraft': False},
        ],
        'liningPrefix': 'co_XPD_AC_mod_UnderArmor_Muni_MK',
        'styleEdid':    'co_XPD_AC_mod_UnderArmor_style_Muni',
        'stylePlanEdid':'Recipe_XPD_AC_mod_armor_UnderArmor_style_Muni',
    },
]

def build_underarmour(records, plan_map):
    # Index COBJ by EDID and plan_map by planEdid
    cobj_by_edid  = {r['cobjEdid']: r for r in records}
    plan_by_edid  = {}
    for fid, data in plan_map.items():
        plan_by_edid[data['planEdid']] = data

    def garment_entry(t_key, gmt):
        pm = plan_by_edid.get(gmt['planEdid'], {})
        has_plan = bool(pm) and not gmt['nocraft']
        sources  = pm.get('obtainSources', [])
        note     = gmt.get('note', '')
        if gmt['nocraft']:
            obtain = note or 'Cannot be crafted — obtain in world'
        elif has_plan:
            src_str = '\n'.join(sources) if sources else 'Source unknown'
            obtain  = f"{pm.get('planName','')}\n{src_str}"
        else:
            obtain = 'Source unknown'
        return {
            'id':           gmt['planEdid'] or gmt['name'].replace(' ','_'),
            'name':         gmt['name'],
            'hasPlan':      has_plan,
            'isATX':        False,
            'nocraft':      gmt['nocraft'],
            'planName':     pm.get('planName',''),
            'obtainText':   obtain,
            'technicalText': f"Plan EDID: {gmt['planEdid']}" if gmt['planEdid'] else 'Not craftable',
            'note':         note,
        }

    def lining_entries(prefix, lining_note=None):
        entries = []
        for mk in range(1, 6):
            edid = f"{prefix}{mk}"
            r = cobj_by_edid.get(edid)
            if not r:
                continue
            pm      = plan_map.get(r['cobjFormId'], {})
            has_plan = bool(pm.get('planName'))
            sources  = pm.get('obtainSources', [])
            name     = LINING_NAMES[mk]
            if lining_note and mk == 1:
                obtain = f'No plan — {lining_note}'
            elif not has_plan:
                obtain = 'No plan required — crafted by default at Armour Workbench'
            else:
                src_str = '\n'.join(sources) if sources else 'Source unknown'
                obtain  = f"{pm.get('planName','')}\n{src_str}"
            entries.append({
                'id':           r['cobjFormId'],
                'edid':         edid,
                'name':         name,
                'hasPlan':      has_plan,
                'isATX':        False,
                'planName':     pm.get('planName',''),
                'obtainText':   obtain,
                'technicalText':f"EDID: {edid}\nFormID: {r['cobjFormId']}",
            })
        return entries

    def style_entry(style_edid, style_plan_edid):
        r = cobj_by_edid.get(style_edid)
        if not r:
            return None
        pm      = plan_by_edid.get(style_plan_edid) or plan_map.get(r['cobjFormId'], {})
        has_plan = bool(pm.get('planName'))
        sources  = pm.get('obtainSources', [])
        src_str  = '\n'.join(sources) if sources else 'Source unknown'
        return {
            'id':           r['cobjFormId'],
            'edid':         style_edid,
            'name':         r['modFull'] or r['modEdid'],
            'hasPlan':      has_plan,
            'isATX':        False,
            'planName':     pm.get('planName',''),
            'obtainText':   f"{pm.get('planName','')}\n{src_str}" if has_plan else 'No plan required — crafted by default',
            'technicalText':f"EDID: {style_edid}\nFormID: {r['cobjFormId']}",
        }

    output_types = []
    for t in UA_TYPES_CONFIG:
        garments = [make_no_mod('Base Garment', f"nomod_garment_{t['typeKey']}")] + \
                   [garment_entry(t['typeKey'], g) for g in t['baseGarments']]

        linings  = [make_no_mod('Lining', f"nomod_lining_{t['typeKey']}")] + \
                   lining_entries(t['liningPrefix'], t.get('liningNote'))

        se       = style_entry(t['styleEdid'], t.get('stylePlanEdid',''))
        styles   = [make_no_mod('Style', f"nomod_style_{t['typeKey']}")] + \
                   ([se] if se else [])

        output_types.append({
            'typeKey':  t['typeKey'],
            'typeName': t['typeName'],
            'slots': [
                {'slot': 'Base Garment', 'mods': garments},
                {'slot': 'Lining',       'mods': linings},
                {'slot': 'Style',        'mods': styles},
            ],
        })
    return {'pageTitle': 'Underarmour Plans & Mods', 'types': output_types}


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description='Generate armour.json from TSV exports')
    parser.add_argument('--data-dir', default='data',    help='Folder containing TSV files')
    parser.add_argument('--out',      default='dist/armour.json', help='Output JSON path')
    parser.add_argument('--cobj',     default=None,      help='Override COBJ TSV path')
    parser.add_argument('--book',     default=None,      help='Override BOOK TSV path')
    args = parser.parse_args()

    # Resolve file paths — support wildcard-style matching for dated filenames
    def find_tsv(directory, keyword):
        for fname in sorted(os.listdir(directory), reverse=True):
            if keyword.lower() in fname.lower() and fname.endswith('.tsv'):
                return os.path.join(directory, fname)
        raise FileNotFoundError(f'No TSV matching "{keyword}" in {directory}')

    cobj_path = args.cobj or find_tsv(args.data_dir, 'COBJ')
    book_path = args.book or find_tsv(args.data_dir, 'BOOK')

    print(f'COBJ: {cobj_path}')
    print(f'BOOK: {book_path}')

    print('Building plan map from BOOK…')
    plan_map = build_plan_map(book_path)
    print(f'  {len(plan_map)} plan entries')

    print('Loading COBJ records…')
    records = load_cobj_records(cobj_path, plan_map)
    print(f'  {len(records)} armour COBJ records')

    print('Building power armour data…')
    power = build_power_armour(records)
    pa_count = sum(len(sl["mods"]) for s in power["sets"] for p in s["pieces"] for sl in p["slots"])
    print(f'  {len(power["sets"])} sets, {pa_count} entries')

    print('Building body armour data…')
    body = build_body_armour(records)
    ba_count = sum(len(sl["mods"]) for s in body["sets"] for p in s["pieces"] for sl in p["slots"])
    print(f'  {len(body["sets"])} sets, {ba_count} entries')

    print('Building underarmour data…')
    under = build_underarmour(records, plan_map)
    ua_count = sum(len(sl["mods"]) for t in under["types"] for sl in t["slots"])
    print(f'  {len(under["types"])} types, {ua_count} entries')

    combined = {
        'version':   '1.0.0',
        'generated': __import__('datetime').date.today().isoformat(),
        'pages': {
            'power-armour': power,
            'body-armour':  body,
            'underarmour':  under,
        },
    }

    os.makedirs(os.path.dirname(args.out) or '.', exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(combined, f, separators=(',', ':'), ensure_ascii=False)

    size_kb = os.path.getsize(args.out) / 1024
    print(f'\nSaved → {args.out}  ({size_kb:.0f} KB)')


if __name__ == '__main__':
    main()
