#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_events_rewards_json.py

Builds:
  dist/events/events_rewards.json
  dist/events/events_rewards_by_page.json
  dist/patchlogs/patchlog_latest_df_events.json

baseRewards schema (new):
  {
    "tiers": [
      {
        "tier":          ""           # "" | "gold" | "silver" | "bronze" | "mutated"
        "xp":            471,         # XP at level 50, or null
        "xpFormID":      "...",
        "caps":          320,         # caps value from GLOB, or null
        "capsFormID":    "...",
        "legendaryRank": 1,           # int or null
        "lvliFormID":    "...",
        "poolTypes":     [{"type":"caps","label":"Caps"}, ...],
        "titles":        [{"title":"Shepherd","kind":"player","isPrefix":true,"isSuffix":true}]
      }
    ]
  }
"""

import csv, glob, json, os, re
from collections import defaultdict
from pathlib import Path

DIST_DIR     = Path("dist/events")
PATCHLOG_DIR = Path("dist/patchlogs")

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def newest(pattern):
    files = glob.glob(pattern)
    if not files: raise FileNotFoundError(pattern)
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]

def read_tsv(path):
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

def pct(x):
    return round(float(x) * 100, 6)

def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"<.*?>", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()

def strip_trailing_slash(p):
    p = (p or "").strip()
    if p != "/" and p.endswith("/"): p = p[:-1]
    return p

def parse_ref(ref):
    s = (ref or "").strip()
    if not s: return ("", "")
    if ":" in s:
        parts = s.split(":")
        return (parts[0].strip(), parts[-1].strip())
    return (s, "")

def title_case_words(s):
    return " ".join(w.capitalize() if w else w for w in s.split())

LVLI_LABEL_OVERRIDES = {
    # key: lowercase substring match → display label
    "enclave_plasmagun": "Enclave Plasma Gun Mod Boxes",
    "enclaveplasmagun":  "Enclave Plasma Gun Mod Boxes",
    "plasmagun_all":     "Enclave Plasma Gun Mod Boxes",
    "rewards_activit":   "Activity Rewards",
    "rewards_enclave":   "Enclave Activity Rewards",
}

def prettify_lvli_label(edid):
    t = (edid or "").strip()
    if not t: return ""
    tl = t.lower()
    for substr, label in LVLI_LABEL_OVERRIDES.items():
        if substr in tl: return label
    t = re.sub(r"^(LLS?|RA_LL|RA_LLS|RA|LL|QuestReward|Quest_Reward|Rewards)_+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^LL_", "", t, flags=re.IGNORECASE)
    t = t.replace("__", "_").replace("_", " ").strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\bPublic Events\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bPublic Event Rewards Rewards\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bQuest Reward\b", "Event Rewards", t, flags=re.IGNORECASE)
    t = title_case_words(t).replace(" Ll ", " LL ")
    return t.strip()

def parse_randompercent_multiplier(conditions_text):
    mult = 1.0
    for m in re.finditer(r"GetRandomPercent\s*<=\s*(\d+)", conditions_text or "", flags=re.IGNORECASE):
        try:
            n = max(0, min(100, int(m.group(1))))
            mult *= n / 100.0
        except ValueError:
            pass
    return mult

# --------------------------------------------------
# Load TSVs
# --------------------------------------------------

QUEST        = read_tsv(newest("tsv/QUEST_Export_*.tsv"))
GMRW         = read_tsv(newest("tsv/GMRW_Export_*.tsv"))
LVLI_LIST    = read_tsv(newest("tsv/LVLI_Export_*_LVLI_List.tsv"))
LVLI_ENTRIES = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Entries.tsv"))
LVLI_MATH    = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Math.tsv"))
BOOK         = read_tsv(newest("tsv/BOOK_Export_*.tsv"))
ARMO         = read_tsv(newest("tsv/ARMO_Export_*.tsv"))
GLOB         = read_tsv(newest("tsv/GLOB_Export_*.tsv"))
GUIDE        = read_tsv(newest("tsv/guide_index.tsv"))

try:    MISC = read_tsv(newest("tsv/MISC_Export_*.tsv"))
except FileNotFoundError: MISC = []
try:    WEAP = read_tsv(newest("tsv/WEAP_Export_*.tsv"))
except FileNotFoundError: WEAP = []
try:    ALCH = read_tsv(newest("tsv/ALCH_Export_*.tsv"))
except FileNotFoundError: ALCH = []
try:    AMMO = read_tsv(newest("tsv/AMMO_Export_*.tsv"))
except FileNotFoundError: AMMO = []
try:    CREA = read_tsv(newest("tsv/CREA_Export_*.tsv"))
except FileNotFoundError: CREA = []
try:    CURV = read_tsv(newest("tsv/CURV_Export_*.tsv"))
except FileNotFoundError: CURV = []
try:    CURV_POINTS = read_tsv(newest("tsv/CURV_Export_*_POINTS.tsv"))
except FileNotFoundError: CURV_POINTS = []
try:    PLYT = read_tsv(newest("tsv/PLYT_Export_*.tsv"))
except FileNotFoundError: PLYT = []
try:    CMPT = read_tsv(newest("tsv/CMPT_Export_*.tsv"))
except FileNotFoundError: CMPT = []

# --------------------------------------------------
# Index: GLOB
# --------------------------------------------------

glob_vals = {}
for r in GLOB:
    fid  = pick(r, "GLOB_FormID", "FormID")
    fltv = pick(r, "GLOB_FLTV", "FLTV")
    if fid and fltv:
        try: glob_vals[fid] = float(fltv)
        except ValueError: pass

# --------------------------------------------------
# Index: item names
# --------------------------------------------------

book_names = {}
for r in BOOK:
    fid = pick(r, "BOOK_FormID", "FormID")
    full = pick(r, "BOOK_FULL", "FULL")
    if fid and full: book_names[fid] = full

armo_names = {}
for r in ARMO:
    fid = pick(r, "ARMO_FormID", "FormID")
    full = pick(r, "ARMO_FULL", "FULL")
    if fid and full: armo_names[fid] = full

misc_names = {}
for r in MISC:
    # MISC TSVs use various column name conventions across exports
    fid  = pick(r, "MISC_FormID", "FormID", "FormId")
    full = pick(r, "MISC_FULL", "FULL - Name", "FULL", "Name")
    if fid and full: misc_names[fid] = full

weap_names = {}
for r in WEAP:
    fid  = pick(r, "WEAP_FormID", "FormID")
    full = pick(r, "WEAP_FULL", "FULL - Name", "FULL")
    if fid and full: weap_names[fid] = full

alch_names = {}
for r in ALCH:
    fid  = pick(r, "ALCH_FormID", "FormID")
    full = pick(r, "ALCH_FULL", "FULL - Name", "FULL")
    if fid and full: alch_names[fid] = full

ammo_names = {}
for r in AMMO:
    fid  = pick(r, "AMMO_FormID", "FormID")
    full = pick(r, "AMMO_FULL", "FULL - Name", "FULL")
    if fid and full: ammo_names[fid] = full

crea_names = {}
for r in CREA:
    fid  = pick(r, "CREA_FormID", "FormID")
    full = pick(r, "CREA_FULL", "FULL")
    if fid and full: crea_names[fid] = full

# Friendly display names for hardcoded known FormIDs
KNOWN_FID_NAMES = {
    "0000000F": "Caps",
    "005652F9": "Legendary Module",
    "005A5443": "Treasury Note",
    "007FDC33": "Improved Bait",
    "003F7410": "Legendary Scrip",
    "0072D4FC": "Bobblehead Crate",
}

def resolve_name_for_formid(formid):
    if not formid: return formid
    return (KNOWN_FID_NAMES.get(formid)
         or book_names.get(formid)
         or armo_names.get(formid)
         or misc_names.get(formid)
         or weap_names.get(formid)
         or alch_names.get(formid)
         or ammo_names.get(formid)
         or formid)

# --------------------------------------------------
# Index: LVLI
# --------------------------------------------------

lvli_edid_by_formid = {}
for r in LVLI_LIST:
    fid = pick(r, "LVLI_FormID", "FormID")
    edid = pick(r, "LVLI_EDID", "EDID")
    if fid and edid: lvli_edid_by_formid[fid] = edid

lvli_math_by_entry = {}
for r in LVLI_MATH:
    try: key = (r["LVLI_FormID"], r["EntryIndex"])
    except KeyError: continue
    lvli_math_by_entry[key] = r

lvli_entries_by_list = defaultdict(list)
for r in LVLI_ENTRIES:
    if "LVLI_FormID" in r:
        lvli_entries_by_list[r["LVLI_FormID"]].append(r)

_lvli_cache = {}

def compute_lvli(list_id):
    if not list_id: return {}
    if list_id in _lvli_cache: return _lvli_cache[list_id]
    results = {}
    for e in lvli_entries_by_list.get(list_id, []):
        idx = e.get("EntryIndex")
        if idx is None: continue
        math = lvli_math_by_entry.get((list_id, idx))
        if not math: continue
        sub        = (math.get("SubLVLI_FormID") or "").strip()
        list_none  = float(math.get("ListChanceNoneResolved") or 0)
        entry_pres = float(math.get("EntryPresenceChance") or 1)
        entry_none = float(math.get("EntryChanceNoneResolved") or 0)
        cond_rand  = float(math.get("EntryCondChance_RandomPercent") or 1)
        apriori    = float(math.get("EntryAprioriChance_NoSublist") or 1)
        chance = (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori
        if sub:
            for k, v in compute_lvli(sub).items():
                results[k] = results.get(k, 0) + v * chance
        else:
            ref = (e.get("LVLO_Reference") or "").strip()
            qty_raw = (e.get("LVLO_Count") or e.get("Count") or "1").strip()
            try: qty = int(float(qty_raw))
            except (ValueError, TypeError): qty = 1
            if ":" in ref:
                fid = ref.split(":")[0]
                results[fid] = results.get(fid, 0) + chance
    _lvli_cache[list_id] = results
    return results

# --------------------------------------------------
# Index: CURV / XP at level 50
# --------------------------------------------------

curv_by_formid = {}
for r in CURV:
    fid = pick(r, "CURV_FormID", "FormID")
    if fid: curv_by_formid[fid] = {"formid": fid, "edid": pick(r, "CURV_EDID", "EDID")}

_curv_pts = defaultdict(list)
for r in CURV_POINTS:
    fid = pick(r, "CURV_FormID", "FormID")
    try: _curv_pts[fid].append((float(r.get("X") or 0), float(r.get("Y") or 0)))
    except (ValueError, TypeError): pass

def xp_at_level(curv_ref, level=50):
    fid = curv_ref.split(":")[0] if ":" in curv_ref else curv_ref
    pts = _curv_pts.get(fid)
    if not pts: return None
    return int(sorted(pts, key=lambda p: abs(p[0] - level))[0][1])

# --------------------------------------------------
# Index: PLYT / CMPT
# --------------------------------------------------

plyt_by_edid = {}
for r in PLYT:
    edid = pick(r, "EDID - Editor ID", "EDID")
    if edid:
        plyt_by_edid[edid] = {
            "title":    pick(r, "ANAM - Male Title", "ANAM"),
            "isPrefix": str(pick(r, "PTPR - Is Prefix", "PTPR")).lower() == "true",
            "isSuffix": str(pick(r, "PTSU - Is Suffix", "PTSU")).lower() == "true",
        }

cmpt_by_edid = {}
for r in CMPT:
    edid = pick(r, "EDID")
    if edid:
        cmpt_by_edid[edid] = {
            "title":    pick(r, "ANAM - Title", "ANAM"),
            "isPrefix": str(pick(r, "PTPR - Is Prefix", "PTPR")).lower() == "true",
            "isSuffix": str(pick(r, "PTSU - Is Suffix", "PTSU")).lower() == "true",
        }

def book_edid_to_title(book_edid):
    s = book_edid.replace("_Recipe_", "_").replace("Title_", "Titles_")
    if s in plyt_by_edid: return ("player", plyt_by_edid[s])
    s2 = book_edid.replace("_Recipe_", "_").replace("CAMPTitle_", "CAMPTitles_")
    if s2 in cmpt_by_edid: return ("camp", cmpt_by_edid[s2])
    return None

# --------------------------------------------------
# LVLI pool classification
# --------------------------------------------------

KNOWN_FIDS = {
    "0000000F": ("Caps",             "caps"),
    "005652F9": ("Legendary Module", "legendary_modules"),
    "005A5443": ("Treasury Note",    "treasury_notes"),
    "007FDC33": ("Improved Bait",    "improved_bait"),
    "003F7410": ("Legendary Scrip",  "legendary_scrip"),
    "0072D4FC": ("Bobblehead Crate", "bobblehead"),
}
FLUX_EDIDS = {
    "c_NukeFlora_Blue_scrap", "c_NukeFlora_Orange_scrap",
    "c_NukeFlora_Purple_scrap", "c_NukeFlora_Red_scrap", "c_NukeFlora_Yellow_scrap",
}
POOL_TYPE_ORDER = [
    "caps", "chems", "improved_bait", "legendary_item", "legendary_modules",
    "legendary_scrip", "treasury_notes", "bobblehead", "treasure_maps",
    "flux", "player_title", "camp_title",
]

def classify_item_ref(ref_str):
    if not ref_str: return None
    parts = ref_str.split(":")
    fid   = parts[0]
    edid  = parts[1] if len(parts) > 1 else ""
    sig   = parts[-1] if len(parts) > 2 else ""
    if fid in KNOWN_FIDS: return KNOWN_FIDS[fid]
    if sig.upper() == "LGDI": return ("Legendary Item", "legendary_item")
    if edid in FLUX_EDIDS: return ("Flux", "flux")
    if "TreasuryNote" in edid or "Treasury_Note" in edid: return ("Treasury Note", "treasury_notes")
    if "LegendaryModule" in edid: return ("Legendary Module", "legendary_modules")
    if "Stimpak" in edid or edid.lower().startswith("ll_chems"): return ("Chems", "chems")
    if "Map" in edid and ("Lucky" in edid or "mtrz" in edid.lower()): return ("Treasure Map", "treasure_maps")
    if "PlayerTitle" in edid and "Recipe" in edid:
        result = book_edid_to_title(edid)
        if result: return (result[1]["title"], "player_title")
    if "CAMPTitle" in edid and "Recipe" in edid:
        result = book_edid_to_title(edid)
        if result: return (result[1]["title"], "camp_title")
    return None

def classify_pool(lvli_fid):
    types_seen  = {}
    titles      = []
    titles_seen = set()

    def _walk(fid, depth=0, seen=None):
        if seen is None: seen = set()
        if fid in seen or depth > 6: return
        seen.add(fid)
        for entry in lvli_entries_by_list.get(fid, []):
            ref = (entry.get("LVLO_Reference") or "").strip()
            if not ref: continue
            parts    = ref.split(":")
            ref_fid  = parts[0]
            ref_type = parts[-1].upper() if len(parts) > 1 else ""
            if ref_type == "LVLI":
                _walk(ref_fid, depth + 1, seen.copy())
            else:
                c = classify_item_ref(ref)
                if not c: continue
                label, tkey = c
                if tkey not in types_seen:
                    types_seen[tkey] = "Player Title" if tkey == "player_title" else \
                                       "Camp Title"   if tkey == "camp_title"   else label
                if tkey in ("player_title", "camp_title"):
                    edid = parts[1] if len(parts) > 1 else ""
                    if edid and edid not in titles_seen:
                        result = book_edid_to_title(edid)
                        if result:
                            titles_seen.add(edid)
                            kind, td = result
                            titles.append({
                                "title":    td["title"],
                                "kind":     kind,
                                "isPrefix": td["isPrefix"],
                                "isSuffix": td["isSuffix"],
                            })

    _walk(lvli_fid)
    pool_types = []
    for key in POOL_TYPE_ORDER:
        if key in types_seen:
            pool_types.append({"type": key, "label": types_seen[key]})
    for key, label in types_seen.items():
        if not any(p["type"] == key for p in pool_types):
            pool_types.append({"type": key, "label": label})
    return pool_types, titles

# --------------------------------------------------
# Index: GMRW
# --------------------------------------------------

gmrw_rows_by_id     = defaultdict(list)
gmrw_rows_by_parent = defaultdict(list)

for r in GMRW:
    gmrw_fid   = pick(r, "FormID", "GMRW_FormID")
    parent_ref = (r.get("ParentQuestLink") or "").strip()
    parent_fid = parent_ref.split(":")[0] if ":" in parent_ref else parent_ref
    if gmrw_fid: gmrw_rows_by_id[gmrw_fid].append(r)
    if parent_fid: gmrw_rows_by_parent[parent_fid].append(r)

def get_gmrw_rows_for_quest(q):
    qid  = pick(q, "QUEST_FormID", "FormID")
    rows = gmrw_rows_by_parent.get(qid, [])
    if rows: return rows
    # Legacy fallback: GMRWRef0-9 on QUEST row
    seen, all_rows = set(), []
    for i in range(10):
        ref = q.get(f"GMRWRef{i}")
        if not ref: continue
        gmrw_fid = ref.split(":")[0] if ":" in ref else ref
        if gmrw_fid in seen: continue
        seen.add(gmrw_fid)
        all_rows.extend(gmrw_rows_by_id.get(gmrw_fid, []))
    return all_rows

# --------------------------------------------------
# Base Rewards builder
# --------------------------------------------------

TIER_ORDER = ("", "gold", "silver", "bronze", "mutated")

def build_base_rewards(gmrw_rows):
    by_tier = defaultdict(list)
    for r in gmrw_rows:
        by_tier[(r.get("TierLabel") or "").strip()].append(r)

    tiers = []
    for tier_key in TIER_ORDER:
        if tier_key not in by_tier: continue
        rows = by_tier[tier_key]

        # XP at level 50
        xp_val = xp_formid = None
        for r in rows:
            xpct = (r.get("XPCT_XPCurveTable") or "").strip()
            if xpct:
                fid = xpct.split(":")[0]
                xp_val = xp_at_level(fid)
                xp_formid = fid
                break

        # Caps
        caps_val = caps_formid = None
        for r in rows:
            caps_ref = (r.get("NAM8_CapsGlobal") or "").strip()
            if caps_ref:
                fid = caps_ref.split(":")[0]
                if fid in glob_vals:
                    caps_val    = int(glob_vals[fid])
                    caps_formid = fid
                break

        # Legendary rank
        leg_rank = None
        for r in rows:
            qrlr = (r.get("QRLR_LegendaryItemRewardRank") or "").strip()
            if qrlr and qrlr not in ("0", ""):
                try: leg_rank = int(qrlr)
                except ValueError: pass
                break

        # LVLI pool
        lvli_fid = pool_types = titles = None
        for r in rows:
            item_ref = (r.get("RewardedItem") or "").strip()
            if not item_ref: continue
            fid, sig = parse_ref(item_ref)
            if sig.upper() == "LVLI" and fid:
                lvli_fid = fid
                pool_types, titles = classify_pool(fid)
                break
        if pool_types is None: pool_types = []
        if titles     is None: titles     = []

        tiers.append({
            "tier":          tier_key,
            "xp":            xp_val,
            "xpFormID":      xp_formid,
            "caps":          caps_val,
            "capsFormID":    caps_formid,
            "legendaryRank": leg_rank,
            "lvliFormID":    lvli_fid,
            "poolTypes":     pool_types,
            "titles":        titles,
            "conditions":    [c for r in rows for c in
                              (r.get("TierConditions") or r.get("TierConditionFunc")
                               or r.get("Conditions") or "").split("|")
                              if c.strip()],
        })
    return {"tiers": tiers}

# --------------------------------------------------
# Misc helpers
# --------------------------------------------------

def add_free(free, label, value, meta=None):
    if value is None: return
    if isinstance(value, str) and value.strip() == "": return
    row = {"label": label, "value": value}
    if meta: row["meta"] = meta
    free.append(row)

def merge_conditions(*conds):
    out, seen = [], set()
    for c in conds:
        s = (c or "").strip()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def humanize_party_crasher_name(raw):
    s = (raw or "").strip()
    if not s: return "Party Crasher"
    formid = s.split(":")[0] if ":" in s else ""
    if formid and formid in crea_names: return crea_names[formid].strip()
    edid = s.split(":", 1)[1] if ":" in s else s
    edid = re.sub(r"^Lvl", "", edid)
    edid = re.sub(r"_?PartyCrasher$", "", edid)
    edid = re.sub(r"_", " ", edid).strip()
    edid = re.sub(r"(?<!^)(?=[A-Z])", " ", edid).strip()
    return edid if edid else "Party Crasher"

# --------------------------------------------------
# Guide mapping
# --------------------------------------------------

reward_pages = []
for r in GUIDE:
    slug = (r.get("slug") or "").strip()
    if not slug.endswith("-reward-checklist"): continue
    url   = strip_trailing_slash(r.get("url") or "")
    title = (r.get("title") or "").strip()
    base_title = title
    if base_title.lower().endswith(" reward checklist"):
        base_title = base_title[:-len(" reward checklist")].strip()
    reward_pages.append({"slug": slug, "url": url, "title": title,
                          "eventTitle": base_title, "eventKey": norm_name(base_title)})

reward_pages_by_key = defaultdict(list)
for p in reward_pages:
    if p["eventKey"]: reward_pages_by_key[p["eventKey"]].append(p)

# --------------------------------------------------
# Quest indexing
# --------------------------------------------------

quest_by_key = defaultdict(list)
for q in QUEST:
    qid  = pick(q, "QUEST_FormID", "FormID")
    name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name",
                "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)
    quest_by_key[norm_name(name)].append(q)

EVENT_KEY_ALIASES = {
    "arealblast":        ["enclaveactivityarealblast"],
    "botsonparade":      ["enclaveactivitybotsonparade"],
    "droppedconnection": ["enclaveactivitydroppedconnection"],
}

# Enclave activity quest FormIDs — used to detect enclave events and inject
# the shared activities LVLI (008A9106) when it isn't already in GMRW rewards.
ENCLAVE_QUEST_FIDS = set()  # populated from alias keys at runtime
ENCLAVE_ACTIVITIES_LVLI = "008A9106"

def find_quest_candidates_for_key(event_key):
    event_key = (event_key or "").strip()
    if not event_key: return []
    c = list(quest_by_key.get(event_key, []))
    if c: return c
    for pref in EVENT_KEY_ALIASES.get(event_key, []):
        for qkey, rows in quest_by_key.items():
            if qkey.startswith(pref): c.extend(rows)
    if c: return c
    for qkey, rows in quest_by_key.items():
        if event_key in qkey: c.extend(rows)
    return c

# --------------------------------------------------
# Event builder
# --------------------------------------------------

events  = []
by_page = {}

for key, pages in sorted(reward_pages_by_key.items()):
    candidates = find_quest_candidates_for_key(key)

    if not candidates:
        event = {
            "questFormID": "", "name": pages[0]["eventTitle"] or "Event",
            "gameName": "", "freeRewards": [], "baseRewards": {"tiers": []},
            "pools": [], "banners": [], "scenarios": [],
            "warnings": [{"title": "Missing QUEST match",
                          "message": f"No QUEST row matched guide title '{pages[0]['eventTitle']}'."}]
        }
    else:
        candidates.sort(key=lambda r: pick(r, "QUEST_FormID", "FormID"))
        q         = candidates[0]
        qid       = pick(q, "QUEST_FormID", "FormID")
        game_name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name",
                         "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)

        is_public = str(q.get("IsPublicEvent") or q.get("PublicEvent") or "0").strip() == "1"

        event = {
            "questFormID": qid, "name": pages[0]["eventTitle"] or game_name,
            "gameName": game_name, "isPublicEvent": is_public,
            "description": pick(q, "DESC - Description", "DESC", default=""),
            "freeRewards": [], "baseRewards": {"tiers": []},
            "pools": [], "banners": [], "scenarios": [],
        }

        # Invaders flag
        if str(q.get("InvadersTakeOver") or "0").strip() == "1":
            event["banners"].append({
                "type": "notice", "style": "invaders",
                "lines": ["Invaders Event Take Over",
                          "This event can be taken over by Invaders from Beyond."]
            })

        # Party Crashers
        pc_count = int(q.get("PartyCrasherCount") or 0)
        for i in range(pc_count):
            npc_raw  = q.get(f"PartyCrasher_NPC_{i}")
            glob_raw = q.get(f"PartyCrasher_GLOB_{i}")
            if not npc_raw or not glob_raw: continue
            glob_fid = glob_raw.split(":")[0] if ":" in str(glob_raw) else str(glob_raw)
            if glob_fid not in glob_vals: continue
            event["banners"].append({
                "type": "notice", "style": "party-crasher",
                "lines": [f"Party Crasher: {humanize_party_crasher_name(npc_raw)}",
                          f"{pct(glob_vals[glob_fid])}% chance to spawn at the end of the event."]
            })

        # Flag enclave activities based on quest key aliases
        is_enclave_activity = any(
            key in norm_name(game_name or "")
            for key in ["enclaveactivity", "enclave_activity"]
        ) or any(
            "enclave" in alias
            for aliases in EVENT_KEY_ALIASES.values()
            for alias in aliases
            if norm_name(game_name or "") in alias or alias in norm_name(game_name or "")
        )
        if is_enclave_activity:
            event["isEnclaveActivity"] = True

        # GMRW
        gmrw_rows = get_gmrw_rows_for_quest(q)
        if gmrw_rows:
            event["baseRewards"] = build_base_rewards(gmrw_rows)

        # freeRewards (legacy / base tier only, for backward compat)
        pool_seen = set()
        for rr in gmrw_rows:
            tier_label = (rr.get("TierLabel") or "").strip()

            if tier_label == "":
                xpct = (rr.get("XPCT_XPCurveTable") or "").strip()
                if xpct:
                    xpv = xp_at_level(xpct.split(":")[0])
                    if xpv is not None:
                        add_free(event["freeRewards"], "XP", xpv,
                                 meta={"source": "GMRW", "curveFormID": xpct.split(":")[0]})
                caps_ref = (rr.get("NAM8_CapsGlobal") or "").strip()
                if caps_ref:
                    fid = caps_ref.split(":")[0]
                    if fid in glob_vals:
                        add_free(event["freeRewards"], "Caps", int(glob_vals[fid]),
                                 meta={"source": "GMRW", "globFormID": fid})
                qrlr = (rr.get("QRLR_LegendaryItemRewardRank") or "").strip()
                if qrlr and qrlr not in ("0", ""):
                    add_free(event["freeRewards"], "Legendary Reward Rank", qrlr,
                             meta={"source": "GMRW"})

            # Pools (all tiers)
            rewarded = (rr.get("RewardedItem") or "").strip()
            if not rewarded: continue
            formid, kind = parse_ref(rewarded)
            count = (rr.get("RewardedItemCount") or "").strip() or "1"
            conds = merge_conditions(
                rr.get("Conditions"),
                rr.get("TierConditionFunc"),
                rr.get("ConditionGlobs"),
            )

            if kind.upper() == "LVLI":
                pool_key = (formid, rr.get("RewardIndex") or "")
                if pool_key in pool_seen: continue
                pool_seen.add(pool_key)
                lvli_edid = lvli_edid_by_formid.get(formid, "")
                label     = prettify_lvli_label(lvli_edid) or prettify_lvli_label(rewarded.replace(":", "_"))
                cond_mult = parse_randompercent_multiplier(" | ".join(conds))
                probs     = compute_lvli(formid)
                items     = sorted([
                    {
                        "formid": fid,
                        "name": resolve_name_for_formid(fid),
                        "dropRate": pct(ch * cond_mult),
                        "qty": 1,
                        "isPlan": any(
                            n.startswith(("Plan:", "Recipe:"))
                            for n in [resolve_name_for_formid(fid)]
                            if n
                        ),
                    }
                    for fid, ch in probs.items()
                ], key=lambda x: (x["name"] or "", x["formid"] or ""))
                pt, ttl   = classify_pool(formid)
                event["pools"].append({
                    "title": label or "Reward Pool", "lvliFormID": formid, "lvliEdid": lvli_edid,
                    "tier": tier_label, "count": count, "conditions": conds,
                    "poolChance": pct(cond_mult), "poolTypes": pt, "items": items,
                    "itemCount": len(items),
                })
            else:
                nm = resolve_name_for_formid(formid) if formid else rewarded
                is_plan = nm.startswith(("Plan:", "Recipe:")) if nm else False
                add_free(event["freeRewards"], "Guaranteed Reward", f"{nm} x{count}",
                         meta={"source": "GMRW", "rewardedItem": rewarded, "conditions": conds,
                               "name": nm, "qty": count, "isPlan": is_plan, "isUnique": not is_plan})

        event["pools"].sort(key=lambda p: (p.get("title") or "", p.get("lvliFormID") or ""))

        # For enclave activities: ensure the shared activities LVLI 008A9106 is present
        if event.get("isEnclaveActivity"):
            has_act = any(p["lvliFormID"] == ENCLAVE_ACTIVITIES_LVLI for p in event["pools"])
            if not has_act:
                act_edid  = lvli_edid_by_formid.get(ENCLAVE_ACTIVITIES_LVLI, "")
                act_probs = compute_lvli(ENCLAVE_ACTIVITIES_LVLI)
                act_items = sorted([
                    {
                        "formid": fid,
                        "name": resolve_name_for_formid(fid),
                        "dropRate": pct(ch),
                        "qty": 1,
                        "isPlan": resolve_name_for_formid(fid).startswith(("Plan:", "Recipe:")),
                    }
                    for fid, ch in act_probs.items()
                ], key=lambda x: (x["name"] or "", x["formid"] or ""))
                pt, ttl = classify_pool(ENCLAVE_ACTIVITIES_LVLI)
                event["pools"].append({
                    "title": "Enclave Activity Rewards",
                    "lvliFormID": ENCLAVE_ACTIVITIES_LVLI,
                    "lvliEdid": act_edid,
                    "tier": "", "count": "1", "conditions": [],
                    "poolChance": 100.0, "poolTypes": pt, "items": act_items,
                    "itemCount": len(act_items),
                    "isEnclaveActivities": True,
                })

    for p in pages:
        if p["slug"]: by_page[p["slug"]] = event
        if p["url"]:
            by_page[p["url"]] = event
            by_page[strip_trailing_slash(p["url"])] = event
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
