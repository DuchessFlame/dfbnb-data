#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import glob
import os
from collections import defaultdict
from pathlib import Path

DIST_DIR = Path("dist/events")
PATCHLOG_DIR = Path("dist/patchlogs")

def newest(pattern):
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(pattern)
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]

def read_tsv(path):
    # Try UTF-8 (with BOM), then fall back to Windows-1252 for “é”/smart quotes etc.
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))

def pct(x):
    return round(float(x) * 100, 6)

# --------------------------------------------------
# Load TSVs (REAL exported column names)
# --------------------------------------------------

QUEST = read_tsv(newest("tsv/QUEST_Export_*.tsv"))
GMRW = read_tsv(newest("tsv/GMRW_Export_*.tsv"))
LVLI_LIST = read_tsv(newest("tsv/LVLI_Export_*_LVLI_List.tsv"))
LVLI_ENTRIES = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Entries.tsv"))
LVLI_MATH = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Math.tsv"))
BOOK = read_tsv(newest("tsv/BOOK_Export_*.tsv"))
ARMO = read_tsv(newest("tsv/ARMO_Export_*.tsv"))
GLOB = read_tsv(newest("tsv/GLOB_Export_*.tsv"))
GUIDE = read_tsv(newest("tsv/guide_index.tsv"))

# --------------------------------------------------
# Indexing
# --------------------------------------------------

glob_vals = {r["FormID"]: float(r["FLTV"]) for r in GLOB if r.get("FLTV")}

book_names = {r["FormID"]: r["FULL"] for r in BOOK}
armo_names = {r["FormID"]: r["FULL"] for r in ARMO}

gmrw_by_id = {r["FormID"]: r for r in GMRW}

lvli_math_by_entry = {}
for r in LVLI_MATH:
    key = (r["LVLI_FormID"], r["EntryIndex"])
    lvli_math_by_entry[key] = r

lvli_entries_by_list = defaultdict(list)
for r in LVLI_ENTRIES:
    lvli_entries_by_list[r["LVLI_FormID"]].append(r)

# --------------------------------------------------
# LVLI Probability Engine (using resolved math)
# --------------------------------------------------

def compute_lvli(list_id):
    results = {}
    entries = lvli_entries_by_list.get(list_id, [])
    for e in entries:
        idx = e["EntryIndex"]
        math = lvli_math_by_entry.get((list_id, idx))
        if not math:
            continue

        sub = math["SubLVLI_FormID"]
        list_presence = float(math["ListPresenceChance"] or 1)
        list_none = float(math["ListChanceNoneResolved"] or 0)
        entry_presence = float(math["EntryPresenceChance"] or 1)
        entry_none = float(math["EntryChanceNoneResolved"] or 0)
        cond_rand = float(math["EntryCondChance_RandomPercent"] or 1)

        chance = (
            list_presence *
            (1 - list_none) *
            entry_presence *
            (1 - entry_none) *
            cond_rand
        )

        if sub:
            sub_results = compute_lvli(sub)
            for k, v in sub_results.items():
                results[k] = results.get(k, 0) + v * chance
        else:
            ref = e["LVLO_Reference"]
            if ":" in ref:
                formid = ref.split(":")[0]
                results[formid] = results.get(formid, 0) + chance

    return results

# --------------------------------------------------
# Event Builder
# --------------------------------------------------

events = []
by_page = {}

for q in QUEST:
    qid = q["FormID"]
    name = q["FULL - Name"] or q["EDID"]

    event = {
        "questFormID": qid,
        "name": name,
        "baseRewards": [],
        "rewards": {
            "default": [],
            "headwear": {"common": [], "rare": [], "uncommon": []},
            "plans": {"count": 0, "poolChance": 100, "perItemChance": None, "items": []}
        },
        "banners": [],
        "scenarios": []
    }

    # --------------------
    # Party Crashers
    # --------------------
    count = int(q.get("PartyCrasherCount") or 0)
    for i in range(count):
        npc = q.get(f"PartyCrasher_NPC_{i}")
        glob = q.get(f"PartyCrasher_GLOB_{i}")
        if npc and glob in glob_vals:
            chance = pct(glob_vals[glob])
            event["banners"].append({
                "type": "notice",
                "style": "party-crasher",
                "lines": [f"{chance}% chance for {npc} to spawn at the end of the event."]
            })

    # --------------------
    # Base Rewards (GMRW)
    # --------------------
    for i in range(10):
        ref = q.get(f"GMRWRef{i}")
        if not ref:
            continue
        g = gmrw_by_id.get(ref)
        if not g:
            continue

        if g.get("NAM7_XPGlobal") in glob_vals:
            event["baseRewards"].append({
                "label": "XP",
                "value": glob_vals[g["NAM7_XPGlobal"]]
            })

        if g.get("NAM8_CapsGlobal") in glob_vals:
            event["baseRewards"].append({
                "label": "Caps",
                "value": glob_vals[g["NAM8_CapsGlobal"]]
            })

        root = g.get("RewardedItem")
        if root and ":LVLI" in root:
            root_id = root.split(":")[0]
            probs = compute_lvli(root_id)

            for fid, chance in probs.items():
                chance_pct = pct(chance)

                name = (
                    book_names.get(fid) or
                    armo_names.get(fid) or
                    fid
                )

                row = {
                    "formid": fid,
                    "name": name,
                    "dropRate": chance_pct
                }

                if name.startswith("Plan:") or name.startswith("Recipe:"):
                    event["rewards"]["plans"]["items"].append(row)
                elif "Mask" in name or "Hat" in name:
                    event["rewards"]["headwear"]["common"].append(row)
                else:
                    event["rewards"]["default"].append(row)

    # --------------------
    # Plan UI Rule Override
    # --------------------
    plans = event["rewards"]["plans"]["items"]
    if plans:
        n = len(plans)
        event["rewards"]["plans"]["count"] = n
        event["rewards"]["plans"]["perItemChance"] = round(100 / n, 6)
        for p in plans:
            p["dropRate"] = event["rewards"]["plans"]["perItemChance"]

    events.append(event)

# --------------------------------------------------
# Write Output
# --------------------------------------------------

DIST_DIR.mkdir(parents=True, exist_ok=True)
PATCHLOG_DIR.mkdir(parents=True, exist_ok=True)

with open(DIST_DIR / "events_rewards.json", "w", encoding="utf-8") as f:
    json.dump({"events": events}, f, indent=2)

with open(DIST_DIR / "events_rewards_by_page.json", "w", encoding="utf-8") as f:
    json.dump({"byPage": by_page}, f, indent=2)

with open(PATCHLOG_DIR / "patchlog_latest_df_events.json", "w", encoding="utf-8") as f:
    json.dump({"built": True}, f, indent=2)

print("Events Rewards build complete.")