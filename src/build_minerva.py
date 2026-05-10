#!/usr/bin/env python3
"""
build_minerva.py
Parses the FO76 BOOK export TSV and outputs minerva_plans.json
Usage: python3 build_minerva.py BOOK_Export_March_2026.tsv > minerva_plans.json
"""

import csv, json, math, re, sys
from pathlib import Path

from patchlog_utils import write_patchlog_feed

# Econ_GoldVendor_Tier_XX GLOB IDs -> gold bullion price
TIER_PRICE = {
    "005A5044": 50,   # Tier 01
    "005A5045": 100,  # Tier 02
    "005A5046": 150,  # Tier 03
    "005A5047": 200,  # Tier 04
    "005A5048": 250,  # Tier 05
    "005A5049": 350,  # Tier 06
    "005A504A": 500,  # Tier 07
    "005A504B": 750,  # Tier 08
    "005A504C": 1000, # Tier 09
    "005A504D": 1250, # Tier 10
    "005A504E": 1650, # Tier 11
    "005A504F": 2000, # Tier 12
    "005A5050": 4000, # Tier 13
}

MINERVA_DISCOUNT = 0.75  # 25% off

# LVLI lists that are cut / inactive content — references to these do NOT
# count as a valid Minerva list ref for inclusion purposes.
CUT_LIST_SUBSTRINGS = [
    'Minerva_LLS_GoldVendor_Backlog',       # 00618512
    'Minerva_LLS_GoldVendor_Backlog_2',      # 0063619B
    'Minerva_LLS_GoldVendor_Backlog_02',     # 0063671E
    'Minerva_LLS_LegendaryCrafting',         # 006032BF
]

# Big Sale lists are merge-lists: each includes the 3 preceding emporium lists.
BIG_SALE_MAP = {
    1: 4,  2: 4,  3: 4,
    5: 8,  6: 8,  7: 8,
    9: 12, 10: 12, 11: 12,
    13: 16, 14: 16, 15: 16,
    17: 20, 18: 20, 19: 20,
    21: 24, 22: 24, 23: 24,
}

# ── Gold bullion vendor LVLI FormIDs ──────────────────────────────────
# Derived from LVLI_Export — only LVLIs with refs > 0 (actually connected
# in the vendor chain).  Orphaned LVLIs (refs=0) are excluded even if
# they have "GoldVendor" in the EDID — they are not sold by any NPC.
#
# Removed orphans:
#   00589627  W05_LLV_GoldVendor_Recipes_Weapons_All        (routing-only, 0 refs)
#   00589628  W05_LLV_GoldVendor_Recipes_Armor_SecretService_All (0 refs)
#   005A1F42  W05_LLV_GoldVendor_Recipes_Mods_Armor_SecretService (0 refs)
#   005A1F43  W05_LLV_GoldVendor_Recipes_Mods_Armor_PowerArmor_T65 (0 refs)
#   0062FEB1  LLV_GoldVendor_AlienRifle_Mods                (0 refs)
#   00630C76  LLV_GoldVendor_ElectroEnforcer_Mods            (0 refs)

SAMUEL_LVLIS = {
    '0059672D',  # W05_LLV_GoldVendor_Settler_Samuel
    '005A0AC3',  # W05_LLV_GoldVendor_Settler_Samuel_6_Ally
    '005A0AC4',  # W05_LLV_GoldVendor_Settler_Samuel_3_Cooperative
    '005A0AC5',  # W05_LLV_GoldVendor_Settler_Samuel_1_Cautious
    '005A0EE3',  # W05_LLV_GoldVendor_Settler_Samuel_4_Friendly
    '005A0EE9',  # W05_LLV_GoldVendor_Settler_Samuel_5_Neighborly
}

MORTIMER_LVLIS = {
    '00596E98',  # W05_LLV_GoldVendor_Raider_Mortimer
    '005A0EE5',  # W05_LLV_GoldVendor_Raider_Mortimer_1_Cautious
    '005A0EEB',  # W05_LLV_GoldVendor_Raider_Mortimer_6_Ally
    '005A0EF4',  # W05_LLV_GoldVendor_Raider_Mortimer_5_Neighborly
    '005A0EFA',  # W05_LLV_GoldVendor_Raider_Mortimer_4_Friendly
    '005A0F00',  # W05_LLV_GoldVendor_Raider_Mortimer_3_Cooperative
    '005A32F9',  # W05_LLV_GoldVendor_Raider_Molly_5_Neighborly
}

REGS_LVLIS = {
    '0057CE6E',  # W05_LLV_GoldVendor_SecretService_Reginald
    '0057CE71',  # W05_LLV_GoldVendor_Recipes_Weapons_Ranged_All
    '00589629',  # W05_LLV_GoldVendor_Recipes_Weapons_Grenades_All
    '0058962A',  # W05_LLV_GoldVendor_Recipes_Weapons_Melee_All
    '005A0AC1',  # W05_LLV_GoldVendor_Recipes_Mods_Weapons_Melee_All
    '005A0AC2',  # W05_LLV_GoldVendor_Recipes_Mods_Weapons_Ranged_All
    '005EB042',  # LLV_GoldVendor_BOSInfantry_Mods
    '005EC6DF',  # LLV_GoldVendor_BOSPistol_Mods
    '005EC6E0',  # LLV_GoldVendor_BOSRocketLauncher_Mods
    '005EC6E1',  # LLV_GoldVendor_WarGlaive_Mods
    '005ECD92',  # LLV_GoldVendor_PlasmaSword_Mods
    '0060DB15',  # LLV_GoldVendor_Hellcat_Mods
    '00611A67',  # LLV_GoldVendor_PepperShaker_Mods
}

# Union of all NPC gold vendor LVLIs — for quick "is this a gold vendor plan?" check
ALL_VENDOR_LVLIS = SAMUEL_LVLIS | MORTIMER_LVLIS | REGS_LVLIS

# Daily Ops reward LVLI substrings
DAILY_OPS_SUBS = ['LL_DailyOps_']

# BoS Recipes LVLI (Regs sells these)
BOS_RECIPES_SUB = 'LL_BoS_Recipes'


def _refs_text(row):
    """Join all field values into one string for substring searching."""
    return ' '.join(str(v) for v in row.values() if v)


def _has_vendor_lvli(refs):
    """True if refs contain any gold vendor NPC LVLI FormID."""
    for fid in ALL_VENDOR_LVLIS:
        if fid in refs:
            return True
    if BOS_RECIPES_SUB in refs:
        return True
    return False


def _has_minerva_list(row):
    """True if row references a non-cut Minerva rotation list."""
    for v in row.values():
        if v and 'Minerva_LLS_GoldVendor' in str(v):
            v_str = str(v)
            if not any(cut in v_str for cut in CUT_LIST_SUBSTRINGS):
                return True
    return False


def _has_daily_ops(refs):
    """True if refs contain a Daily Ops reward LVLI."""
    return any(sub in refs for sub in DAILY_OPS_SUBS)


def detect_vendor(row, refs):
    """Return the vendor label for a gold-bullion plan.

    Priority: NPC vendor (Samuel/Mortimer/Regs) > Daily Ops/Minerva > Minerva.
    """
    # Check NPC vendor LVLIs
    is_samuel   = any(fid in refs for fid in SAMUEL_LVLIS)
    is_mortimer = any(fid in refs for fid in MORTIMER_LVLIS)
    is_regs     = any(fid in refs for fid in REGS_LVLIS) or BOS_RECIPES_SUB in refs

    if is_samuel:
        return 'Samuel'
    if is_mortimer:
        return 'Mortimer'
    if is_regs:
        return 'Regs'

    # No NPC vendor — check Daily Ops + Minerva
    has_do = _has_daily_ops(refs)
    has_m  = _has_minerva_list(row)

    if has_do and has_m:
        return 'Daily Ops/Minerva'
    if has_m:
        return 'Minerva'
    if has_do:
        return 'Daily Ops'

    # SCORE plans with GoldVendor in EDID but no LVLI ref — sold by Samuel
    edid = row.get('EDID', '')
    if 'SCORE' in edid and 'GoldVendor' in edid:
        return 'Samuel'

    return 'Gold Vendor'


def parse_gold_price(row):
    bvgo = row.get('BVGO', '')
    for glob_id, price in TIER_PRICE.items():
        if glob_id.upper() in bvgo.upper():
            return price
    try:
        return int(float(row.get('DATA_Value', 0)))
    except (ValueError, TypeError):
        return 0


def get_minerva_lists(row):
    lists = set()
    for val in row.values():
        if not val:
            continue
        for num in re.findall(r'BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)', val):
            lists.add(int(num))
    for emporium_list in list(lists):
        big_sale = BIG_SALE_MAP.get(emporium_list)
        if big_sale:
            lists.add(big_sale)
    return sorted(lists)


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else 'BOOK_Export_March_2026.tsv'

    minerva_plans = []
    all_plans = []

    with open(src, encoding='latin-1') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            edid = row.get('EDID', '')
            name = row.get('FULL', '').strip()

            if not name.startswith('Plan:'):
                continue

            # ── Exclusions ───────────────────────────────────────
            # Deprecated / cut copies (zzz_ prefix)
            edid_lower = edid.lower()
            if edid_lower.startswith('zzz'):
                continue
            # Milepost Zero stamp-currency items — not gold bullion
            if edid.startswith('MILE_'):
                continue

            # ── Inclusion criteria ───────────────────────────────
            # A plan qualifies as a gold-bullion plan if ANY of:
            #   1. "GoldVendor" in EDID (standard naming convention)
            #   2. References an NPC gold vendor LVLI (Samuel/Mortimer/Regs)
            #   3. References a non-cut Minerva rotation list
            refs = _refs_text(row)
            has_gv_edid       = 'GoldVendor' in edid
            has_vendor_lvli   = _has_vendor_lvli(refs)
            has_minerva_ref   = _has_minerva_list(row)

            if not (has_gv_edid or has_vendor_lvli or has_minerva_ref):
                continue

            gold = parse_gold_price(row)
            vendor = detect_vendor(row, refs)
            entry = {
                "formid": row.get('FormID', '').strip(),
                "name": name,
                "gold": gold,
                "vendor": vendor,
            }
            all_plans.append(entry)

            lists = get_minerva_lists(row)
            if lists:
                minerva_plans.append({
                    **entry,
                    "minerva_price": math.floor(gold * MINERVA_DISCOUNT + 0.5),
                    "lists": lists
                })

    minerva_plans.sort(key=lambda x: x['name'])
    all_plans.sort(key=lambda x: x['name'])

    output = {
        "meta": {
            "source": src,
            "total_gold_vendor_plans": len(all_plans),
            "total_minerva_plans": len(minerva_plans),
            "minerva_discount": MINERVA_DISCOUNT,
            "daily_gold_cap": 200,
            "smiley_options": [0, 50, 100, 150, 200, 250, 300],
        },
        "minerva": minerva_plans,
        "all_plans": all_plans,
    }

    print(json.dumps(output, indent=2))
    print(f"\n// Built: {len(all_plans)} gold vendor plans, {len(minerva_plans)} on Minerva lists", file=sys.stderr)

    # Generate patchlog feed to dist/ (minerva_plans.json output location)
    dist_dir = Path(__file__).parent.parent / "dist"
    write_patchlog_feed(
        dist_dir=str(dist_dir),
        feed_name="patchlog_latest_df_minerva.json",
        current_items=minerva_plans,
        key_field="formid",
        name_field="name",
        compare_fields=["name", "gold", "lists"],
        prev_json_path="dist/minerva_plans.json",
        items_extractor=lambda d: d.get("minerva", []),
    )

if __name__ == '__main__':
    main()
