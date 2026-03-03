#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_events_rewards_json.py (UPDATED mockup schema)

Builds:
- dist/events/events_rewards.json
- dist/events/events_rewards_by_page.json
- dist/patchlogs/patchlog_latest_df_events.json

Key goals:
- Deterministic output from newest TSV exports in /tsv
- events_rewards_by_page.json MUST contain keys for both:
    - guide slug (e.g. "a-real-blast-reward-checklist")
    - guide url path without trailing slash (e.g. "/df/activities/a-real-blast/a-real-blast-reward-checklist")

Schema goals (mockup phase):
- Keep front-facing event name from guide_index.tsv
- Store real in-game quest name in "gameName"
- Replace fixed "Default/Headwear/Plans" with dynamic "pools" built from GMRW Rewarded Items:
    - Anything RewardedItem that is LVLI becomes its own expand/pool
    - Anything NOT LVLI becomes a "free reward" line above the expands
- Leave hooks for CURV and LVLI linkage:
    - include xpCurveTableFormID on free rewards when present
    - include lvliFormID on pools always
    - include lvliEdid when available (from LVLI list TSV), else blank
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
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"<.*?>", "", s)               # drop alias fragments like <Alias=...>
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()

def strip_trailing_slash(p: str) -> str:
    p = (p or "").strip()
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p

def parse_ref(ref: str):
    """
    "003D7FAB:LVLI" -> ("003D7FAB","LVLI")
    "00012345" -> ("00012345","")
    """
    s = (ref or "").strip()
    if not s:
        return ("", "")
    if ":" in s:
        a, b = s.split(":", 1)
        return (a.strip(), b.strip())
    return (s, "")

def title_case_words(s: str) -> str:
    return " ".join(w.capitalize() if w else w for w in s.split())

def prettify_lvli_label(edid: str) -> str:
    """
    Very lightweight "gap filler" naming:
    - strips common prefixes
    - turns underscores into spaces
    - title-cases
    - a couple targeted swaps
    """
    t = (edid or "").strip()
    if not t:
        return ""

    # common prefixes to strip
    t = re.sub(r"^(LLS?|RA_LL|RA_LLS|RA|LL|QuestReward|Quest_Reward|Rewards)_+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^LL_", "", t, flags=re.IGNORECASE)
    t = t.replace("__", "_")
    t = t.replace("_", " ").strip()
    t = re.sub(r"\s+", " ", t)

    # a few common-sense renames
    t = re.sub(r"\bPublic Events\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bPublic Event Rewards Rewards\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bQuest Reward\b", "Event Rewards", t, flags=re.IGNORECASE)

    t = title_case_words(t)

    # final tidy
    t = t.replace(" Ll ", " LL ")
    return t.strip()


def parse_randompercent_multiplier(conditions_text: str) -> float:
    """
    Extract simple RNG conditions like:
      Subject.GetRandomPercent <= 10
    Returns multiplier in [0,1]. If none found, returns 1.
    If multiple found, multiplies them.
    """
    s = (conditions_text or "")
    mult = 1.0

    for m in re.finditer(r"GetRandomPercent\s*<=\s*(\d+)", s, flags=re.IGNORECASE):
        try:
            n = int(m.group(1))
            n = max(0, min(100, n))
            mult *= (n / 100.0)
        except ValueError:
            pass

    return mult

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

# Optional: Party Crasher creature name resolution
try:
    CREA = read_tsv(newest("tsv/CREA_Export_*.tsv"))
except FileNotFoundError:
    CREA = []

# Optional: CURV exports (placeholder wiring)
try:
    CURV = read_tsv(newest("tsv/CURV_Export_*.tsv"))
except FileNotFoundError:
    CURV = []
try:
    CURV_POINTS = read_tsv(newest("tsv/CURV_Export_*_POINTS.tsv"))
except FileNotFoundError:
    CURV_POINTS = []

# --------------------------------------------------
# Indexing: GLOB / BOOK / ARMO / CREA / LVLI list / CURV
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

lvli_edid_by_formid = {}
for r in LVLI_LIST:
    fid = pick(r, "LVLI_FormID", "FormID")
    edid = pick(r, "LVLI_EDID", "EDID")
    if fid and edid:
        lvli_edid_by_formid[fid] = edid

curv_by_formid = {}
for r in CURV:
    fid = pick(r, "CURV_FormID", "FormID")
    edid = pick(r, "CURV_EDID", "EDID")
    if fid:
        curv_by_formid[fid] = {"formid": fid, "edid": edid}

curv_points_by_formid = defaultdict(list)
for r in CURV_POINTS:
    fid = pick(r, "CURV_FormID", "FormID")
    if fid:
        curv_points_by_formid[fid].append(r)

def humanize_party_crasher_name(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return "Party Crasher"

    formid = s.split(":")[0] if ":" in s else ""
    if formid and formid in crea_names:
        return crea_names[formid].strip()

    edid = s.split(":", 1)[1] if ":" in s else s
    edid = re.sub(r"^Lvl", "", edid)
    edid = re.sub(r"_?PartyCrasher$", "", edid)
    edid = re.sub(r"_", " ", edid).strip()
    edid = re.sub(r"(?<!^)(?=[A-Z])", " ", edid).strip()
    return edid if edid else "Party Crasher"

def resolve_name_for_formid(formid: str) -> str:
    return book_names.get(formid) or armo_names.get(formid) or formid

# --------------------------------------------------
# GMRW indexing (IMPORTANT: many rows per FormID)
# --------------------------------------------------

gmrw_rows_by_id = defaultdict(list)
for r in GMRW:
    fid = pick(r, "FormID", "GMRW_FormID")
    if fid:
        gmrw_rows_by_id[fid].append(r)

# --------------------------------------------------
# LVLI probability engine (uses resolved math TSV)
# --------------------------------------------------

lvli_math_by_entry = {}
for r in LVLI_MATH:
    try:
        key = (r["LVLI_FormID"], r["EntryIndex"])
    except KeyError:
        continue
    lvli_math_by_entry[key] = r

lvli_entries_by_list = defaultdict(list)
for r in LVLI_ENTRIES:
    if "LVLI_FormID" in r:
        lvli_entries_by_list[r["LVLI_FormID"]].append(r)

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
        list_none = float(math.get("ListChanceNoneResolved") or 0)
        entry_presence = float(math.get("EntryPresenceChance") or 1)
        entry_none = float(math.get("EntryChanceNoneResolved") or 0)
        cond_rand = float(math.get("EntryCondChance_RandomPercent") or 1)

        # Weighting within the list (your math export already resolves this)
        apriori = float(math.get("EntryAprioriChance_NoSublist") or 1)

        chance = (
            (1 - list_none) *
            entry_presence *
            (1 - entry_none) *
            cond_rand *
            apriori
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
# Quest indexing + aliases
# --------------------------------------------------

quest_by_key = defaultdict(list)
for q in QUEST:
    qid = pick(q, "QUEST_FormID", "FormID")
    name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name", "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)
    quest_by_key[norm_name(name)].append(q)

EVENT_KEY_ALIASES = {
    "arealblast": ["enclaveactivityarealblast"],
    "botsonparade": ["enclaveactivitybotsonparade"],
    "droppedconnection": ["enclaveactivitydroppedconnection"],
}

def find_quest_candidates_for_key(event_key: str):
    event_key = (event_key or "").strip()
    if not event_key:
        return []

    c = list(quest_by_key.get(event_key, []))
    if c:
        return c

    alias_prefixes = EVENT_KEY_ALIASES.get(event_key, [])
    if alias_prefixes:
        matches = []
        for qkey, rows in quest_by_key.items():
            for pref in alias_prefixes:
                if qkey.startswith(pref):
                    matches.extend(rows)
                    break
        if matches:
            return matches

    matches = []
    for qkey, rows in quest_by_key.items():
        if event_key in qkey:
            matches.extend(rows)
    return matches

# --------------------------------------------------
# Event builder (dynamic GMRW pools)
# --------------------------------------------------

def add_free(free, label, value, meta=None):
    if value is None:
        return
    if isinstance(value, str) and value.strip() == "":
        return
    row = {"label": label, "value": value}
    if meta:
        row["meta"] = meta
    free.append(row)

def merge_conditions(*conds):
    parts = []
    for c in conds:
        s = (c or "").strip()
        if s:
            parts.append(s)
    # de-dupe, keep order
    out = []
    seen = set()
    for p in parts:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out

events = []
by_page = {}

for key, pages in sorted(reward_pages_by_key.items(), key=lambda kv: kv[0]):
    candidates = find_quest_candidates_for_key(key)

    if not candidates:
        event = {
            "questFormID": "",
            "name": pages[0]["eventTitle"] or "Event",
            "gameName": "",
            "freeRewards": [],
            "pools": [],
            "banners": [],
            "scenarios": [],
            "warnings": [{
                "title": "Missing QUEST match",
                "message": f"No QUEST row matched guide title '{pages[0]['eventTitle']}'. Check guide_index.tsv title vs QUEST FULL name."
            }]
        }
    else:
        candidates.sort(key=lambda r: pick(r, "QUEST_FormID", "FormID"))
        q = candidates[0]

        qid = pick(q, "QUEST_FormID", "FormID")
        game_name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name", "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)

        event = {
            "questFormID": qid,
            "name": pages[0]["eventTitle"] or game_name,
            "gameName": game_name,
            "freeRewards": [],
            "pools": [],
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
        # GMRW (dynamic pools per LVLI)
        # --------------------
        pool_seen = set()

        for i in range(10):
            ref = q.get(f"GMRWRef{i}")
            if not ref:
                continue

            rows = gmrw_rows_by_id.get(ref, [])
            if not rows:
                continue

            # "free rewards" are pulled from the FIRST row we see for that GMRW FormID
            r0 = rows[0]

            xp_glob = (r0.get("NAM7_XPGlobal") or "").strip()
            if xp_glob in glob_vals:
                add_free(event["freeRewards"], "XP", glob_vals[xp_glob], meta={"source": "GMRW", "gmrwFormID": ref, "globFormID": xp_glob})

            caps_glob = (r0.get("NAM8_CapsGlobal") or "").strip()
            if caps_glob in glob_vals:
                add_free(event["freeRewards"], "Caps", glob_vals[caps_glob], meta={"source": "GMRW", "gmrwFormID": ref, "globFormID": caps_glob})

            # Curve hook (placeholder: store the CURV formid + EDID if present)
            xpct = (r0.get("XPCT_XPCurveTable") or "").strip()
            if xpct:
                add_free(event["freeRewards"], "XP Curve Table", xpct, meta={"source": "GMRW", "gmrwFormID": ref, "curve": curv_by_formid.get(xpct) or {"formid": xpct}})

            # Legendary hook (placeholder)
            qrlr = (r0.get("QRLR_LegendaryItemRewardRank") or "").strip()
            if qrlr:
                add_free(event["freeRewards"], "Legendary Reward Rank", qrlr, meta={"source": "GMRW", "gmrwFormID": ref})

            # Now build pools per RewardedItem LVLI
            for rr in rows:
                rewarded = (rr.get("RewardedItem") or "").strip()
                if not rewarded:
                    continue

                formid, kind = parse_ref(rewarded)
                count = (rr.get("RewardedItemCount") or rr.get("RewardedItemCount".lower()) or "").strip() or "1"

                conds = merge_conditions(rr.get("Conditions"), rr.get("ConditionGlobs"))

                if kind.upper() == "LVLI":
                    lvli_edid = lvli_edid_by_formid.get(formid, "")
                    label = prettify_lvli_label(lvli_edid) or prettify_lvli_label(rewarded.replace(":", "_"))

                    pool_key = (ref, formid, rr.get("RewardIndex") or "", rr.get("RewardedItemIndex") or "")
                    if pool_key in pool_seen:
                        continue
                    pool_seen.add(pool_key)

                    # Pool-level chance from GMRW row conditions (ex: GetRandomPercent <= 10)
                    cond_text = " | ".join(conds)
                    cond_mult = parse_randompercent_multiplier(cond_text)  # 0..1
                    pool_chance_pct = pct(cond_mult)

                    probs = compute_lvli(formid)
                    items = []
                    for fid, ch in probs.items():
                        # Final drop rate includes the pool conditional
                        final = ch * cond_mult
                        items.append({
                            "formid": fid,
                            "name": resolve_name_for_formid(fid),
                            "dropRate": pct(final)
                        })
                    items.sort(key=lambda x: (x["name"] or "", x["formid"] or ""))

                    event["pools"].append({
                        "title": label or "Reward Pool",
                        "lvliFormID": formid,
                        "lvliEdid": lvli_edid,
                        "sourceGmrwFormID": ref,
                        "rewardIndex": rr.get("RewardIndex"),
                        "rewardedItemIndex": rr.get("RewardedItemIndex"),
                        "count": count,
                        "conditions": conds,
                        "poolChance": pool_chance_pct,
                        "items": items,
                    })
                else:
                    # non-LVLI rewarded items: show as "free" lines (mockup)
                    nm = resolve_name_for_formid(formid) if formid else rewarded
                    add_free(event["freeRewards"], "Guaranteed Reward", f"{nm} x{count}", meta={"source": "GMRW", "gmrwFormID": ref, "rewardedItem": rewarded, "conditions": conds})

        # Sort pools for deterministic output
        event["pools"].sort(key=lambda p: (p.get("title") or "", p.get("lvliFormID") or ""))

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
