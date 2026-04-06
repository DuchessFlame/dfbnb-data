#!/usr/bin/env python3
"""
build_minerva.py
Parses the FO76 BOOK export TSV and outputs minerva_plans.json
Usage: python3 build_minerva.py BOOK_Export_March_2026.tsv > minerva_plans.json
"""

import csv, json, re, sys

# Econ_GoldVendor_Tier_XX GLOB IDs -> gold bullion price
TIER_PRICE = {
    "005A5044": 5,   # Tier 01
    "005A5045": 10,  # Tier 02
    "005A5046": 15,  # Tier 03
    "005A5047": 20,  # Tier 04
    "005A5048": 25,  # Tier 05
    "005A5049": 35,  # Tier 06
    "005A504A": 50,  # Tier 07
    "005A504B": 75,  # Tier 08
    "005A504C": 100, # Tier 09
    "005A504D": 125, # Tier 10
    "005A504E": 165, # Tier 11
    "005A504F": 200, # Tier 12
    "005A5050": 400, # Tier 13
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
    dnam = row.get('DNAM_Flags', '')
    for glob_id, price in TIER_PRICE.items():
        if glob_id.upper() in dnam.upper():
            return price
    # fallback: DATA_Value field often equals the gold price
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

            if 'GoldVendor' not in edid:
                continue
            if not name.startswith('Plan:'):
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
                    "minerva_price": round(gold * MINERVA_DISCOUNT),
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

if __name__ == '__main__':
    main()
