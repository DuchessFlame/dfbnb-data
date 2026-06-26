#!/usr/bin/env python3
"""
build_linda_lee_rewards_json.py
===============================
Generates dist/linda_lee_rewards.json — the legendary reward drop rates for
Linda-Lee's Chum Trough (the /df/fishing/linda-lee-chum-trough-rewards/ guide),
resolved generatively from the live LVLI xEdit exports.

Source of truth (tsv/):
    LVLI_Export_<MONTH>_LVLI_List.tsv      (list flags + entry counts)
    LVLI_Export_<MONTH>_LVLI_Entries.tsv   (per-list ordered entries)

Reward tree (root: Fishing_LL_ChumTroughReward [007C62AF]):
    Fishing_LL_ChumTroughReward            pick-one, 2 entries
      ├─ Fishing_LLS_ChumTrough_Weapon     pick-one, 6 entries (melee/ranged ×3)
      └─ Fishing_LLS_ChumTrough_Armor      pick-one, 4 entries (armour ×3 + PA list)
            └─ Fishing_LLS_ChumTrough_PowerArmor  pick-one, 3 entries

All four lists are pick-one (LVLF flags "11" = Level Filter + For Each, no Use All,
no First Match) with ChanceNone 0 on every entry, so the rate of each leaf is the
product of 1/N down the tree. The resolver follows the drop-rate-engine decision
tree, so if Bethesda ever changes the flags/ChanceNone, re-running this script picks
up the new numbers (and emits a warning if a list stops being pick-one).

Output:
    dist/linda_lee_rewards.json
        { generated, dataSource, rootList, totalChance, rewards[], warnings[] }

Usage:
    python3 build_linda_lee_rewards_json.py
    python3 build_linda_lee_rewards_json.py --tsv-dir tsv --dist-dir dist
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
from datetime import date

ROOT_FORMID = "007C62AF"  # Fishing_LL_ChumTroughReward

# Display ordering for the final table.
CATEGORY_ORDER = {
    "Melee Weapon": 0,
    "Ranged Weapon": 1,
    "Weapon": 2,
    "Armour": 3,
    "Power Armour": 4,
}


def parse_flags(flag_str: str) -> set[int]:
    """LVLF flags are a positional bit string, left-to-right, 0-indexed."""
    return {i for i, c in enumerate(flag_str or "") if c == "1"}


def newest_export(tsv_dir: str, suffix: str) -> str:
    """Pick the most recently modified LVLI export matching *suffix*."""
    matches = glob.glob(os.path.join(tsv_dir, f"LVLI_Export_*_{suffix}.tsv"))
    if not matches:
        raise FileNotFoundError(f"No LVLI_Export_*_{suffix}.tsv in {tsv_dir}")
    return max(matches, key=os.path.getmtime)


def month_label(path: str) -> str:
    m = re.search(r"LVLI_Export_(.+?)_LVLI_", os.path.basename(path))
    return (m.group(1).replace("_", " ") if m else "unknown")


def read_tsv(path: str):
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        header = f.readline().rstrip("\n").split("\t")
        idx = {h: i for i, h in enumerate(header)}
        for line in f:
            cols = line.rstrip("\n").split("\t")
            yield idx, cols


def load_lists(path: str) -> dict:
    lists = {}
    for idx, cols in read_tsv(path):
        fid = cols[idx["LVLI_FormID"]]
        lists[fid] = {
            "edid": cols[idx["LVLI_EDID"]],
            "flags": cols[idx["LVLF_Flags"]],
        }
    return lists


def load_entries(path: str) -> dict:
    entries: dict = {}
    for idx, cols in read_tsv(path):
        fid = cols[idx["LVLI_FormID"]]
        cn_raw = cols[idx["LVOV_ChanceNoneValue"]] if idx["LVOV_ChanceNoneValue"] < len(cols) else "0"
        try:
            cn = float(cn_raw or 0)
        except ValueError:
            cn = 0.0
        entries.setdefault(fid, []).append({
            "index": int(cols[idx["EntryIndex"]] or 0),
            "ref": cols[idx["LVLO_Reference"]],
            "chanceNone": cn,
        })
    for fid in entries:
        entries[fid].sort(key=lambda e: e["index"])
    return entries


def label_from_edid(edid: str):
    """LegendaryItems_Weapons_Melee_Rank1 -> (1, 'Melee Weapon')."""
    m = re.search(r"Rank(\d)", edid)
    star = int(m.group(1)) if m else 0
    low = edid.lower()
    if "powerarmor" in low:
        typ = "Power Armour"
    elif "armor" in low:
        typ = "Armour"
    elif "melee" in low:
        typ = "Melee Weapon"
    elif "ranged" in low:
        typ = "Ranged Weapon"
    elif "weapon" in low:
        typ = "Weapon"
    else:
        typ = edid
    return star, typ


def resolve(formid, rate, lists, entries, leaves, warnings, depth=0):
    if depth > 50:
        warnings.append(f"Depth limit hit at {formid}")
        return
    ents = entries.get(formid, [])
    if not ents:
        return
    lst = lists.get(formid, {})
    flags = parse_flags(lst.get("flags", ""))
    if 2 in flags or 6 in flags:
        warnings.append(
            f"{formid} ({lst.get('edid', '?')}) has flags '{lst.get('flags')}' "
            f"(Use All / First Match) — resolver assumes pick-one; review rates."
        )
    n = len(ents)
    for e in ents:
        cn_factor = 1.0 - (e["chanceNone"] / 100.0)
        r = rate * (1.0 / n) * cn_factor
        parts = e["ref"].split(":")
        ref_fid = parts[0] if parts else ""
        ref_edid = parts[1] if len(parts) > 1 else ""
        ref_type = parts[2] if len(parts) > 2 else ""
        if ref_type == "LVLI":
            resolve(ref_fid, r, lists, entries, leaves, warnings, depth + 1)
        elif ref_type == "LGDI":
            leaves.append({"edid": ref_edid, "rate": r})
        # BOOK / other reference types are not legendary rewards — ignore.


def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv-dir", default=os.path.join(base, "tsv"))
    ap.add_argument("--dist-dir", default=os.path.join(base, "dist"))
    args = ap.parse_args()

    list_path = newest_export(args.tsv_dir, "LVLI_List")
    entries_path = newest_export(args.tsv_dir, "LVLI_Entries")

    lists = load_lists(list_path)
    entries = load_entries(entries_path)

    if ROOT_FORMID not in entries:
        raise SystemExit(f"Root list {ROOT_FORMID} not found in {entries_path}")

    leaves: list = []
    warnings: list = []
    resolve(ROOT_FORMID, 1.0, lists, entries, leaves, warnings)

    rewards = []
    for lf in leaves:
        star, typ = label_from_edid(lf["edid"])
        rewards.append({
            "edid": lf["edid"],
            "label": f"{star}★ Legendary {typ}",
            "category": typ,
            "star": star,
            "chance": round(lf["rate"], 6),
            "chancePct": round(lf["rate"] * 100, 3),
        })
    rewards.sort(key=lambda r: (CATEGORY_ORDER.get(r["category"], 9), r["star"]))

    total = round(sum(lf["rate"] for lf in leaves), 6)
    if abs(total - 1.0) > 1e-4:
        warnings.append(f"Rewards total {total:.6f}, expected 1.0")

    output = {
        "generated": str(date.today()),
        "dataSource": f"xEdit LVLI Export {month_label(list_path)}",
        "rootList": lists.get(ROOT_FORMID, {}).get("edid", ROOT_FORMID),
        "totalChance": total,
        "rewards": rewards,
        "warnings": warnings,
    }

    os.makedirs(args.dist_dir, exist_ok=True)
    out_path = os.path.join(args.dist_dir, "linda_lee_rewards.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"Wrote {out_path}")
    print(f"  rewards: {len(rewards)}  total: {total:.6f}  warnings: {len(warnings)}")
    for w in warnings:
        print("  ! " + w)


if __name__ == "__main__":
    main()
