#!/usr/bin/env python3
"""
build_currency_json.py
======================
Builds dist/currency.json for the Currency module (Gold Bullion Calculator + Lists).

Reads directly from game export TSVs — no dependency on minerva_plans.json.

Inputs:
  - tsv/BOOK_Export_*.tsv             (plan names, gold prices, list membership)
  - tsv/LVLI_Export_*_LVLI_Entries.tsv  (big sale sub-list composition, optional)

Output:
  - dist/currency.json

Usage:
  python build_currency_json.py
  python build_currency_json.py --book-tsv tsv/BOOK_Export_March_2026.tsv
  python build_currency_json.py --book-tsv tsv/BOOK_Export_March_2026.tsv \
                                --lvli-entries tsv/LVLI_Export_March_2026_LVLI_Entries.tsv \
                                --outdir dist
"""

import argparse
import csv
import glob
import json
import os
import re
import sys

# ---------------------------------------------------------------------------
# Gold tier pricing — Econ_GoldVendor_Tier_XX GLOB IDs -> gold bullion price
# ---------------------------------------------------------------------------
TIER_PRICE = {
    "005A5044": 50,    # Tier 01
    "005A5045": 100,   # Tier 02
    "005A5046": 150,   # Tier 03
    "005A5047": 200,   # Tier 04
    "005A5048": 250,   # Tier 05
    "005A5049": 350,   # Tier 06
    "005A504A": 500,   # Tier 07
    "005A504B": 750,   # Tier 08
    "005A504C": 1000,  # Tier 09
    "005A504D": 1250,  # Tier 10
    "005A504E": 1650,  # Tier 11
    "005A504F": 2000,  # Tier 12
    "005A5050": 4000,  # Tier 13
}

MINERVA_DISCOUNT = 0.75
DAILY_GOLD_CAP   = 400
SMILEY_OPTIONS   = [0, 50, 100, 150, 200, 250, 300]

# Hardcoded big sale fallback (matches March 2026 game data)
BIG_SALE_SLOTS_FALLBACK = {
    4:  [1, 2, 3],
    8:  [5, 6, 7],
    12: [9, 10, 11],
    16: [13, 14, 15],
    20: [17, 18, 19],
    24: [21, 22, 23],
}

SLOT_ORDER = list(range(1, 25))


def parse_gold_price(row):
    # BVGO holds the "Base Value GLOB Override" — the Econ_GoldVendor_Tier_XX GLOB
    # reference that encodes the gold bullion price.  DNAM_Flags is a flags field
    # (always "000001") and does NOT contain the GLOB ID.
    bvgo = row.get("BVGO", "")
    for glob_id, price in TIER_PRICE.items():
        if glob_id.upper() in bvgo.upper():
            return price
    # Fallback for items that don't use the tier GLOB system (e.g. Brotherhood
    # Recon Armor base pieces store their gold price directly in DATA_Value).
    try:
        return int(float(row.get("DATA_Value", 0)))
    except (ValueError, TypeError):
        return 0


def get_minerva_lists(row):
    lists = set()
    for val in row.values():
        if not val:
            continue
        for num in re.findall(r"BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)", val):
            lists.add(int(num))
    return sorted(lists)


def parse_book_tsv(book_tsv):
    all_plans     = []
    minerva_plans = []

    with open(book_tsv, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid = row.get("EDID", "")
            name = row.get("FULL", "").strip()

            if not name.startswith("Plan:"):
                continue
            # Three signals that a BOOK record is a gold-bullion plan:
            #  1. "GoldVendor" in EDID — the standard naming convention.
            #  2. Non-empty BVGO — an explicit Econ_GoldVendor_Tier_XX GLOB is set,
            #     meaning the game assigned a gold bullion price (e.g. BOS Recon mods).
            #  3. References a Minerva-specific LLS list directly (e.g. Brotherhood
            #     Recon Armor base pieces, Arctic Marine Armor — items sold at Minerva
            #     that use DATA_Value for their gold price instead of the tier GLOB).
            bvgo = row.get("BVGO", "").strip()
            has_gold_vendor = (
                "GoldVendor" in edid
                or bool(bvgo)
                or any("Minerva_LLS_GoldVendor" in str(v) for v in row.values() if v)
            )
            if not has_gold_vendor:
                continue

            gold  = parse_gold_price(row)
            entry = {
                "formid": row.get("FormID", "").strip(),
                "name":   name,
                "gold":   gold,
            }
            all_plans.append(entry)

            lists = get_minerva_lists(row)
            if lists:
                minerva_plans.append({
                    **entry,
                    "minerva_price": round(gold * MINERVA_DISCOUNT),
                    "lists":         lists,
                })

    all_plans.sort(key=lambda x: x["name"])
    minerva_plans.sort(key=lambda x: x["name"])
    return all_plans, minerva_plans


def parse_big_sale_from_lvli(entries_path):
    if not entries_path or not os.path.exists(entries_path):
        print("[build_currency_json] LVLI Entries TSV not found — using hardcoded big sale data.",
              file=sys.stderr)
        return BIG_SALE_SLOTS_FALLBACK.copy()

    big_sales   = {}
    m_pattern   = re.compile(r"BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)_M$")
    sub_pattern = re.compile(r"BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)(?:_M)?")

    with open(entries_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            edid  = row.get("LVLI_EDID", "")
            m     = m_pattern.fullmatch(edid)
            if not m:
                continue
            slot_num = int(m.group(1))
            ref      = row.get("LVLO_Reference", "")
            sub_m    = sub_pattern.search(ref)
            # Exclude references to Big Sale merge-lists (end in _M:LVLI).
            # Cannot use '"_M" not in ref' because "_Minerva" also contains "_M".
            if sub_m and "_M:LVLI" not in ref:
                sub_num = int(sub_m.group(1))
                big_sales.setdefault(slot_num, [])
                if sub_num not in big_sales[slot_num]:
                    big_sales[slot_num].append(sub_num)

    if not big_sales:
        print("[build_currency_json] No big sale entries in LVLI TSV — using hardcoded data.",
              file=sys.stderr)
        return BIG_SALE_SLOTS_FALLBACK.copy()

    for slot in big_sales:
        big_sales[slot].sort()

    print(f"[build_currency_json] Parsed {len(big_sales)} big sale slots from LVLI Entries.",
          file=sys.stderr)
    return big_sales


def build_slots(big_sales, plans_by_list):
    slots = []
    for slot in SLOT_ORDER:
        if slot in big_sales:
            sub   = big_sales[slot]
            parts = [str(n) for n in sub]
            label = ("Big Sale \u2014 Lists " + ", ".join(parts[:-1]) + " & " + parts[-1]
                     if len(parts) > 1 else "Big Sale \u2014 List " + parts[0])
            seen  = set()
            count = 0
            for n in sub:
                for p in plans_by_list.get(n, []):
                    if p["formid"] not in seen:
                        seen.add(p["formid"])
                        count += 1
            slots.append({
                "slot":       slot,
                "label":      label,
                "big_sale":   True,
                "sub_lists":  sub,
                "plan_count": count,
            })
        else:
            slots.append({
                "slot":       slot,
                "label":      f"List {slot}",
                "big_sale":   False,
                "sub_lists":  [],
                "plan_count": len(plans_by_list.get(slot, [])),
            })
    return slots


def find_latest(pattern):
    files = sorted(glob.glob(pattern), reverse=True)
    return files[0] if files else None


def main():
    parser = argparse.ArgumentParser(description="Build currency.json for the Gold Bullion module.")
    parser.add_argument("--book-tsv",     default=None,
                        help="Path to BOOK_Export_*.tsv (auto-detected from tsv/ if omitted)")
    parser.add_argument("--lvli-entries", default=None,
                        help="Path to LVLI_Export_*_LVLI_Entries.tsv (optional)")
    parser.add_argument("--outdir",       default="dist",
                        help="Output directory (default: dist)")
    args = parser.parse_args()

    book_tsv = args.book_tsv
    if not book_tsv:
        book_tsv = find_latest("tsv/BOOK_Export_*.tsv")
    if not book_tsv or not os.path.exists(book_tsv):
        print("[build_currency_json] ERROR: No BOOK_Export_*.tsv found. "
              "Pass --book-tsv or run from the repo root.", file=sys.stderr)
        sys.exit(1)
    print(f"[build_currency_json] Using BOOK TSV: {book_tsv}", file=sys.stderr)

    lvli_entries = args.lvli_entries
    if not lvli_entries:
        lvli_entries = find_latest("tsv/LVLI_Export_*_LVLI_Entries.tsv")

    all_plans, minerva_plans = parse_book_tsv(book_tsv)
    print(f"[build_currency_json] {len(all_plans)} gold vendor plans, "
          f"{len(minerva_plans)} on Minerva lists.", file=sys.stderr)

    plans_by_list = {}
    for plan in minerva_plans:
        for n in plan.get("lists", []):
            plans_by_list.setdefault(n, [])
            plans_by_list[n].append(plan)

    big_sales = parse_big_sale_from_lvli(lvli_entries)
    slots     = build_slots(big_sales, plans_by_list)

    output = {
        "meta": {
            "source":                  os.path.basename(book_tsv),
            "built_by":                "build_currency_json.py",
            "total_gold_vendor_plans": len(all_plans),
            "total_minerva_plans":     len(minerva_plans),
            "total_slots":             len(slots),
            "total_big_sale_slots":    len(big_sales),
            "minerva_discount":        MINERVA_DISCOUNT,
            "daily_gold_cap":          DAILY_GOLD_CAP,
            "smiley_options":          SMILEY_OPTIONS,
        },
        "slots":     slots,
        "minerva":   minerva_plans,
        "all_plans": all_plans,
    }

    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, "currency.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[build_currency_json] Wrote {out_path} — "
          f"{len(slots)} slots, {sum(s['plan_count'] for s in slots)} total slot plans.",
          file=sys.stderr)


if __name__ == "__main__":
    main()
