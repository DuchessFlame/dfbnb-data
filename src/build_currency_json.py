#!/usr/bin/env python3
"""
build_currency_json.py
======================
Builds dist/currency.json for the Currency module (Gold Bullion Calculator).

Reads:
  - dist/minerva_plans.json      (plan names, gold prices, list membership)
  - tsv/LVLI_Export_*_List.tsv   (list metadata, optional — for future enrichment)
  - tsv/LVLI_Export_*_Entries.tsv (big sale sub-list composition)

Outputs:
  - dist/currency.json

Usage:
  python build_currency_json.py
  python build_currency_json.py --minerva-json dist/minerva_plans.json \\
                                --lvli-entries tsv/LVLI_Export_March_2026_LVLI_Entries.tsv \\
                                --outdir dist
"""

import argparse
import csv
import json
import os
import re
import sys


# ---------------------------------------------------------------------------
# Big sale structure derived from game data (LVLI exports).
# Each big sale slot merges its three preceding emporium lists.
# Pattern: slot N_M  contains sub-lists [N-3, N-2, N-1]
# ---------------------------------------------------------------------------
BIG_SALE_SLOTS = {
    4:  [1, 2, 3],
    8:  [5, 6, 7],
    12: [9, 10, 11],
    16: [13, 14, 15],
    20: [17, 18, 19],
    24: [21, 22, 23],
}

# Full 24-slot rotation order (matches BS02_SpecialVendor_Minerva_LLV_GoldVendor)
SLOT_ORDER = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
              13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]


def parse_big_sale_from_lvli(entries_path):
    """
    Parse LVLI_Entries TSV to extract big sale sub-list composition.
    Returns a dict: {slot_num: [sub_list_nums]} for _M lists.
    Falls back to hardcoded BIG_SALE_SLOTS if file not found.
    """
    if not entries_path or not os.path.exists(entries_path):
        print(f"[build_currency_json] LVLI Entries TSV not found — using hardcoded big sale data.",
              file=sys.stderr)
        return BIG_SALE_SLOTS.copy()

    big_sales = {}
    m_pattern   = re.compile(r'BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)_M')
    sub_pattern = re.compile(r'BS02_SpecialVendor_Minerva_LLS_GoldVendor_(\d+)(?:_M)?')

    with open(entries_path, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            edid = row.get('LVLI_EDID', '')
            m = m_pattern.fullmatch(edid)
            if not m:
                continue
            slot_num = int(m.group(1))
            ref = row.get('LVLO_Reference', '')
            sub_m = sub_pattern.search(ref)
            if sub_m and '_M' not in ref:
                # Only count non-_M sub-lists (the actual emporium lists)
                sub_num = int(sub_m.group(1))
                big_sales.setdefault(slot_num, [])
                if sub_num not in big_sales[slot_num]:
                    big_sales[slot_num].append(sub_num)

    if not big_sales:
        print("[build_currency_json] No big sale entries found in TSV — using hardcoded data.",
              file=sys.stderr)
        return BIG_SALE_SLOTS.copy()

    # Sort sub-lists for consistency
    for slot in big_sales:
        big_sales[slot].sort()

    print(f"[build_currency_json] Parsed {len(big_sales)} big sale slots from LVLI Entries.",
          file=sys.stderr)
    return big_sales


def build_slots(big_sales):
    """Build the 24-slot rotation metadata list."""
    slots = []
    for slot in SLOT_ORDER:
        if slot in big_sales:
            sub = big_sales[slot]
            # e.g. "Big Sale — Lists 1, 2 & 3"
            parts = [str(n) for n in sub]
            if len(parts) > 1:
                label = "Big Sale — Lists " + ", ".join(parts[:-1]) + " & " + parts[-1]
            else:
                label = "Big Sale — List " + parts[0]
            slots.append({
                "slot":      slot,
                "label":     label,
                "big_sale":  True,
                "sub_lists": sub,
            })
        else:
            slots.append({
                "slot":      slot,
                "label":     f"List {slot}",
                "big_sale":  False,
                "sub_lists": [],
            })
    return slots


def main():
    parser = argparse.ArgumentParser(description="Build currency.json for the Currency module.")
    parser.add_argument("--minerva-json",  default="dist/minerva_plans.json",
                        help="Path to minerva_plans.json (default: dist/minerva_plans.json)")
    parser.add_argument("--lvli-entries",  default=None,
                        help="Path to LVLI_Entries TSV (optional; overrides hardcoded big sale data)")
    parser.add_argument("--outdir",        default="dist",
                        help="Output directory (default: dist)")
    args = parser.parse_args()

    # --- Load source plan data ---
    if not os.path.exists(args.minerva_json):
        print(f"[build_currency_json] ERROR: minerva_plans.json not found at {args.minerva_json}",
              file=sys.stderr)
        sys.exit(1)

    with open(args.minerva_json, encoding='utf-8') as f:
        source = json.load(f)

    all_plans    = source.get("all_plans", [])
    minerva_plans = source.get("minerva", [])
    source_meta  = source.get("meta", {})

    print(f"[build_currency_json] Loaded {len(all_plans)} all_plans, "
          f"{len(minerva_plans)} minerva plans.", file=sys.stderr)

    # --- Parse big sale structure ---
    big_sales = parse_big_sale_from_lvli(args.lvli_entries)

    # --- Build slot metadata ---
    slots = build_slots(big_sales)

    # --- Build per-list plan index ---
    # plans_by_list[n] = list of plan objects
    plans_by_list = {}
    for plan in minerva_plans:
        for list_num in plan.get("lists", []):
            plans_by_list.setdefault(list_num, [])
            if plan not in plans_by_list[list_num]:
                plans_by_list[list_num].append(plan)

    # Inject plan_count into each slot for convenience
    for slot_obj in slots:
        if slot_obj["big_sale"]:
            seen = set()
            count = 0
            for sub in slot_obj["sub_lists"]:
                for p in plans_by_list.get(sub, []):
                    if p["formid"] not in seen:
                        seen.add(p["formid"])
                        count += 1
            slot_obj["plan_count"] = count
        else:
            slot_obj["plan_count"] = len(plans_by_list.get(slot_obj["slot"], []))

    # --- Assemble output ---
    output = {
        "meta": {
            "source":               source_meta.get("source", "minerva_plans.json"),
            "built_by":             "build_currency_json.py",
            "total_gold_vendor_plans": len(all_plans),
            "total_minerva_plans":  len(minerva_plans),
            "total_slots":          len(slots),
            "total_big_sale_slots": len(big_sales),
            "minerva_discount":     source_meta.get("minerva_discount", 0.75),
            "daily_gold_cap":       source_meta.get("daily_gold_cap", 200),
            "smiley_options":       source_meta.get("smiley_options", [0, 50, 100, 150, 200, 250, 300]),
        },
        "slots":     slots,
        "minerva":   minerva_plans,
        "all_plans": all_plans,
    }

    # --- Write output ---
    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, "currency.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    plan_counts = [s["plan_count"] for s in slots]
    print(f"[build_currency_json] Wrote {out_path}", file=sys.stderr)
    print(f"[build_currency_json] Slots: {len(slots)} total, "
          f"{len(big_sales)} big sale | "
          f"Total slot plans: {sum(plan_counts)}", file=sys.stderr)


if __name__ == "__main__":
    main()
