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

# Big Sale lists are merge-lists: each includes the 3 preceding emporium lists.
# A plan in any of those emporium lists also appears at the Big Sale event.
# Derived from LVLI: 04_M->{1,2,3}, 08_M->{5,6,7}, 12_M->{9,10,11},
#                    16_M->{13,14,15}, 20_M->{17,18,19}, 24_M->{21,22,23}
BIG_SALE_MAP = {
    1: 4,  2: 4,  3: 4,
    5: 8,  6: 8,  7: 8,
    9: 12, 10: 12, 11: 12,
    13: 16, 14: 16, 15: 16,
    17: 20, 18: 20, 19: 20,
    21: 24, 22: 24, 23: 24,
}

def parse_gold_price(row):
    # BVGO holds the "Base Value GLOB Override" — the Econ_GoldVendor_Tier_XX GLOB
    # reference that encodes the gold bullion price.  DNAM_Flags is a flags field
    # (always "000001") and does NOT contain the GLOB ID.
    bvgo = row.get('BVGO', '')
    for glob_id, price in TIER_PRICE.items():
        if glob_id.upper() in bvgo.upper():
            return price
    # Fallback for items that don't use the tier GLOB system (e.g. Brotherhood
    # Recon Armor base pieces store their gold price directly in DATA_Value).
    try:
        return int(float(row.get('DATA_Value', 0)))
    except (ValueError, TypeError):
        return 0

def get_minerva_lists(row):
    lists = set()
    for val in row.values():
        if not val:
            continue
        # findall catches ALL list refs in a single field (search only finds the first).
        # The regex matches plain numbered lists (e.g. GoldVendor_03) but NOT the Big
        # Sale merge-lists (GoldVendor_04_M) — those are LVLI-only containers that no
        # BOOK record references directly.
        for num in re.findall(r'BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)', val):
            lists.add(int(num))
    # Expand: if a plan is in an emporium list that's merged into a Big Sale list,
    # it also belongs to that Big Sale list.
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
            # Three signals that a BOOK record is a gold-bullion plan:
            #  1. "GoldVendor" in EDID — the standard naming convention.
            #  2. Non-empty BVGO — an explicit Econ_GoldVendor_Tier_XX GLOB is set,
            #     meaning the game assigned a gold bullion price (e.g. BOS Recon mods).
            #  3. References a Minerva-specific LLS list directly (e.g. Brotherhood
            #     Recon Armor base pieces, Arctic Marine Armor — items sold at Minerva
            #     that use DATA_Value for their gold price instead of the tier GLOB).
            bvgo = row.get('BVGO', '').strip()
            has_gold_vendor = (
                'GoldVendor' in edid
                or bool(bvgo)
                or any('Minerva_LLS_GoldVendor' in str(v) for v in row.values() if v)
            )
            if not has_gold_vendor:
                continue

            gold = parse_gold_price(row)
            entry = {
                "formid": row.get('FormID', '').strip(),
                "name": name,
                "gold": gold,
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
