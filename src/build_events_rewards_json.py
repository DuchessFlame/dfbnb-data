#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_events_rewards_json.py

Builds:
- dist/events/events_rewards.json
- dist/events/events_rewards_by_page.json
- dist/patchlogs/patchlog_latest_df_events.json

Key goals:
- Deterministic output from newest TSV exports in /tsv
- events_rewards_by_page.json MUST contain keys for both:
    - guide slug (e.g. "a-real-blast-reward-checklist")
    - guide url path without trailing slash (e.g. "/df/activities/a-real-blast/a-real-blast-reward-checklist")
  so the frontend can resolve even if the template does not provide a slug dataset attribute.
"""

import csv
import glob
import json
import os
import re
from collections import defaultdict
from pathlib import Path

DIST_DIR = Path("dist/events")
PATCHLOG_DIR = Path("dist/patchlogs")

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def newest(pattern: str) -> str:
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(pattern)
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]

def read_tsv(path: str):
    # UTF-8 (with BOM) then CP1252 fallback for windows exports/smart quotes
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))

def pick(row, *keys, default=""):
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return v
    return default

def pct(x) -> float:
    return round(float(x) * 100, 6)

def norm_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\(.*?\)", "", s)            # drop parenthetical
    s = re.sub(r"[^a-z0-9]+", "", s)         # alnum only
    return s.strip()

def strip_trailing_slash(p: str) -> str:
    p = (p or "").strip()
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p

# --------------------------------------------------
# Load TSVs (newest exports)
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

# Optional: Party Crasher creature name resolution (FormID -> CREA_FULL)
# Safe fallback if CREA export is not present.
try:
    CREA = read_tsv(newest("tsv/CREA_Export_*.tsv"))
except FileNotFoundError:
    CREA = []

# --------------------------------------------------
# Indexing: GLOB / BOOK / ARMO / CREA / GMRW / LVLI
# --------------------------------------------------

glob_vals = {}
for r in GLOB:
    fid = pick(r, "GLOB_FormID", "FormID")
    fltv = pick(r, "GLOB_FLTV", "FLTV")
    if fid and fltv:
        try:
            glob_vals[fid] = float(fltv)
        except ValueError:
            pass

book_names = {}
for r in BOOK:
    fid = pick(r, "BOOK_FormID", "FormID")
    full = pick(r, "BOOK_FULL", "FULL")
    if fid and full:
        book_names[fid] = full

armo_names = {}
for r in ARMO:
    fid = pick(r, "ARMO_FormID", "FormID")
    full = pick(r, "ARMO_FULL", "FULL")
    if fid and full:
        armo_names[fid] = full

crea_names = {}
for r in CREA:
    fid = pick(r, "CREA_FormID", "FormID")
    full = pick(r, "CREA_FULL", "FULL")
    if fid and full:
        crea_names[fid] = full

def humanize_party_crasher_name(raw: str) -> str:
    """
    raw is usually 'FORMID:EDID' or EDID. Prefer CREA_FULL via FORMID.
    Fall back to de-EDID'ing common patterns into readable names.
    """
    s = (raw or "").strip()
    if not s:
        return "Party Crasher"

    formid = s.split(":")[0] if ":" in s else ""
    if formid and formid in crea_names:
        return crea_names[formid].strip()

    edid = s.split(":", 1)[1] if ":" in s else s

    # Common cleanups
    edid = re.sub(r"^Lvl", "", edid)
    edid = re.sub(r"_?PartyCrasher$", "", edid)
    edid = re.sub(r"_", " ", edid).strip()

    # Insert spaces before capitals (very conservative)
    edid = re.sub(r"(?<!^)(?=[A-Z])", " ", edid).strip()

    # Fix some known FO76-ish casing
    edid = edid.replace("Scorch Beast", "Scorchbeast")
    edid = edid.replace("Mirelurk Queen", "Mirelurk Queen")
    edid = edid.replace("Wendigo Colossus", "Wendigo Colossus")
    edid = edid.replace("Deathclaw", "Deathclaw")
    edid = edid.replace("Bigfoot", "Bigfoot")

    return edid if edid else "Party Crasher"

gmrw_by_id = {}
for r in GMRW:
    fid = pick(r, "GMRW_FormID", "FormID")
    if fid:
        gmrw_by_id[fid] = r

lvli_math_by_entry = {}
for r in LVLI_MATH:
    # (list_id, entry_index) -> resolved math row
    try:
        key = (r["LVLI_FormID"], r["EntryIndex"])
    except KeyError:
        continue
    lvli_math_by_entry[key] = r

lvli_entries_by_list = defaultdict(list)
for r in LVLI_ENTRIES:
    if "LVLI_FormID" in r:
        lvli_entries_by_list[r["LVLI_FormID"]].append(r)

# --------------------------------------------------
# LVLI probability engine (uses resolved math TSV)
# --------------------------------------------------

_lvli_cache = {}

def compute_lvli(list_id: str):
    if not list_id:
        return {}
    if list_id in _lvli_cache:
        return _lvli_cache[list_id]

    results = {}
    entries = lvli_entries_by_list.get(list_id, [])

    for e in entries:
        idx = e.get("EntryIndex")
        if idx is None:
            continue

        math = lvli_math_by_entry.get((list_id, idx))
        if not math:
            continue

        sub = (math.get("SubLVLI_FormID") or "").strip()
        list_presence = float(math.get("ListPresenceChance") or 1)
        list_none = float(math.get("ListChanceNoneResolved") or 0)
        entry_presence = float(math.get("EntryPresenceChance") or 1)
        entry_none = float(math.get("EntryChanceNoneResolved") or 0)
        cond_rand = float(math.get("EntryCondChance_RandomPercent") or 1)

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
            ref = (e.get("LVLO_Reference") or "").strip()
            if ":" in ref:
                formid = ref.split(":")[0]
                results[formid] = results.get(formid, 0) + chance

    _lvli_cache[list_id] = results
    return results

# --------------------------------------------------
# Guide mapping: reward checklist pages -> quest name
# --------------------------------------------------

reward_pages = []
for r in GUIDE:
    slug = (r.get("slug") or "").strip()
    if not slug.endswith("-reward-checklist"):
        continue
    url = strip_trailing_slash(r.get("url") or "")
    title = (r.get("title") or "").strip()

    # event title is usually "X Reward Checklist"
    base_title = title
    if base_title.lower().endswith(" reward checklist"):
        base_title = base_title[: -len(" reward checklist")].strip()

    reward_pages.append({
        "slug": slug,
        "url": url,
        "title": title,
        "eventTitle": base_title,
        "eventKey": norm_name(base_title),
    })

reward_pages_by_key = defaultdict(list)
for p in reward_pages:
    if p["eventKey"]:
        reward_pages_by_key[p["eventKey"]].append(p)

# --------------------------------------------------
# Quest indexing (by normalized name)
# --------------------------------------------------

quest_by_key = defaultdict(list)
for q in QUEST:
    qid = pick(q, "QUEST_FormID", "FormID")
    name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name", "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)
    quest_by_key[norm_name(name)].append(q)

# --------------------------------------------------
# Event builder
# --------------------------------------------------

def resolve_name_for_formid(formid: str) -> str:
    return (
        book_names.get(formid)
        or armo_names.get(formid)
        or formid
    )

def classify_reward(name: str) -> str:
    # Very rough classification, can be upgraded later.
    if name.startswith("Plan:") or name.startswith("Recipe:"):
        return "plan"
    if "mask" in name.lower() or "hat" in name.lower() or "hood" in name.lower():
        return "headwear"
    return "default"

events = []
by_page = {}

# Only build events that actually have a reward-checklist page.
# This makes the dist deterministic AND keeps the frontend from trying to show 2,000 irrelevant quests.
for key, pages in sorted(reward_pages_by_key.items(), key=lambda kv: kv[0]):
    # Find a matching QUEST row
    candidates = quest_by_key.get(key, [])
    if not candidates:
        # Still emit a stub so the page doesn't go blank
        event = {
            "questFormID": "",
            "name": pages[0]["eventTitle"] or "Event",
            "baseRewards": [],
            "rewards": {
                "default": [],
                "headwear": {"common": [], "rare": [], "uncommon": []},
                "plans": {"count": 0, "poolChance": 0, "perItemChance": None, "items": []}
            },
            "banners": [],
            "scenarios": [],
            "warnings": [{
                "title": "Missing QUEST match",
                "message": f"No QUEST row matched guide title '{pages[0]['eventTitle']}'. Check guide_index.tsv title vs QUEST FULL name."
            }]
        }
    else:
        # If multiple candidates, take first deterministically by FormID
        candidates.sort(key=lambda r: pick(r, "QUEST_FormID", "FormID"))
        q = candidates[0]
        qid = pick(q, "QUEST_FormID", "FormID")
        name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name", "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)

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
        # Party Crashers (from QUEST export)
        # --------------------
        count = int(q.get("PartyCrasherCount") or 0)
        for i in range(count):
            npc_raw = q.get(f"PartyCrasher_NPC_{i}")
            glob_raw = q.get(f"PartyCrasher_GLOB_{i}")

            if not npc_raw or not glob_raw:
                continue

            # GLOB may be exported as "FORMID:GLOB_EDID" in some TSVs
            glob_fid = glob_raw.split(":")[0] if ":" in str(glob_raw) else str(glob_raw)

            if glob_fid not in glob_vals:
                continue

            chance = pct(glob_vals[glob_fid])
            npc_name = humanize_party_crasher_name(npc_raw)

            event["banners"].append({
                "type": "notice",
                "style": "party-crasher",
                "lines": [
                    f"Party Crasher: {npc_name}",
                    f"{chance}% chance to spawn at the end of the event."
                ]
            })

        # --------------------
        # Base Rewards + Item Pools (GMRW)
        # --------------------
        for i in range(10):
            ref = q.get(f"GMRWRef{i}")
            if not ref:
                continue
            g = gmrw_by_id.get(ref)
            if not g:
                continue

            # XP/Caps globals (optional)
            xp_glob = g.get("NAM7_XPGlobal")
            if xp_glob in glob_vals:
                event["baseRewards"].append({"label": "XP", "value": glob_vals[xp_glob]})

            caps_glob = g.get("NAM8_CapsGlobal")
            if caps_glob in glob_vals:
                event["baseRewards"].append({"label": "Caps", "value": glob_vals[caps_glob]})

            # Root rewarded item LVLI
            root = (g.get("RewardedItem") or "").strip()
            if root and ":LVLI" in root:
                root_id = root.split(":")[0]
                probs = compute_lvli(root_id)

                for fid, chance in probs.items():
                    chance_pct = pct(chance)
                    nm = resolve_name_for_formid(fid)

                    row = {"formid": fid, "name": nm, "dropRate": chance_pct}

                    kind = classify_reward(nm)
                    if kind == "plan":
                        event["rewards"]["plans"]["items"].append(row)
                    elif kind == "headwear":
                        event["rewards"]["headwear"]["common"].append(row)
                    else:
                        event["rewards"]["default"].append(row)

        # --------------------
        # Plans UI rule: if the pool is "1 plan guaranteed", show per-item % cleanly.
        # NOTE: Right now we do NOT know poolChance deterministically from these TSVs,
        # so we keep poolChance at 100 and derive per item.
        # When you add poolChance later, set per item = poolChance / n instead.
        # --------------------
        plans = event["rewards"]["plans"]["items"]
        if plans:
            n = len(plans)
            event["rewards"]["plans"]["count"] = n
            per = round(100 / n, 6)
            event["rewards"]["plans"]["perItemChance"] = per
            for p in plans:
                p["dropRate"] = per

    # Attach to by_page for every guide page pointing at this event
    for p in pages:
        slug = p["slug"]
        url = p["url"]
        if slug:
            by_page[slug] = event
        if url:
            by_page[url] = event
            by_page[strip_trailing_slash(url)] = event

    events.append(event)

# --------------------------------------------------
# Write output
# --------------------------------------------------

DIST_DIR.mkdir(parents=True, exist_ok=True)
PATCHLOG_DIR.mkdir(parents=True, exist_ok=True)

with open(DIST_DIR / "events_rewards.json", "w", encoding="utf-8") as f:
    json.dump({"events": events}, f, indent=2)

with open(DIST_DIR / "events_rewards_by_page.json", "w", encoding="utf-8") as f:
    json.dump({"byPage": by_page}, f, indent=2)

with open(PATCHLOG_DIR / "patchlog_latest_df_events.json", "w", encoding="utf-8") as f:
    json.dump({"built": True}, f, indent=2)

print(f"Events Rewards build complete. events={len(events)} byPage={len(by_page)}")
