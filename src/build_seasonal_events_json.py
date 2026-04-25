#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_seasonal_events_json.py — Seasonal Events Rewards

Builds:
  dist/seasonal_events/seasonal_events_rewards.json
  dist/seasonal_events/seasonal_events_rewards_by_page.json
  dist/patchlogs/patchlog_latest_df_seasonal_events.json

Separate pipeline from build_events_rewards_json.py.
Imports the shared drop-rate engine (rng76.py) for LVLI resolution.

Output schema per event:
  {
    "questFormID":    "0049886E",
    "name":           "Fasnacht Day Parade",
    "gameName":       "Event: Fasnacht Day",
    "description":    "Join the fun during the Fasnacht Day parade...",
    "slug":           "fasnacht-day-parade-all-rewards",
    "url":            "/df/seasonal-events/fasnacht-day-parade/...",
    "isContainerLoot": false,
    "containerLootDescription": "",
    "baseRewards": {
      "tiers": [
        {
          "tier":          "",
          "xp":            null,
          "xpFormID":      null,
          "caps":          null,
          "capsFormID":    null,
          "legendaryRank": null,
          "lvliFormID":    null,
          "poolTypes":     [{"type":"caps","label":"Caps"}, ...],
          "titles":        []
        }
      ]
    },
    "pools":    [ { "title", "lvliFormID", "lvliEdid", "tier", "count",
                    "conditions", "poolChance", "poolTypes", "items",
                    "itemCount" } ],
    "banners":  [ { "type", "style", "lines" } ],
    "warnings": [ { "title", "message" } ],
    "freeRewards":        [],
    "conditionalRewards": []
  }

Usage: python build_seasonal_events_json.py
"""

import csv, glob, json, os, re, sys
from collections import defaultdict
from pathlib import Path

from patchlog_utils import write_patchlog_feed

# ---------------------------------------------------------------------------
# Import shared drop-rate engine (rng76.py in same directory)
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
for _p in [_this_dir, _this_dir / "src", _this_dir.parent / "src"]:
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from rng76 import (
    Rng76Resolver, LvliIndex, GlobIndex, CurvIndex, ItemNameIndex,
    safe_float, safe_int,
    parse_lvlf_flags, parse_randompercent_multiplier,
    humanize_edid, glob_formid_from_lvli_field,
    fmt_pct, REGION_BY_SUBLVLI_EDID,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_REPO_ROOT   = Path(__file__).resolve().parent.parent
DIST_DIR     = _REPO_ROOT / "dist" / "seasonal_events"
PATCHLOG_DIR = _REPO_ROOT / "dist" / "patchlogs"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _filename_date_key(path):
    base = os.path.basename(path).lower()
    m = re.search(r'_([a-z]+)_(\d{4})', base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    return (0, 0)

def newest(pattern, exclude_substrings=None):
    full_pattern = str(_REPO_ROOT / pattern)
    files = glob.glob(full_pattern)
    if exclude_substrings:
        files = [f for f in files
                 if not any(s in os.path.basename(f) for s in exclude_substrings)]
    if not files:
        raise FileNotFoundError(pattern)
    files.sort(key=lambda x: (_filename_date_key(x), os.path.getmtime(x)))
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
    return round(max(0.0, float(x)) * 100, 6)

def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"<.*?>", "", s)
    return re.sub(r"[^a-z0-9]", "", s)

def strip_trailing_slash(p):
    s = str(p or "").strip()
    return s.rstrip("/") if len(s) > 1 else s

# ---------------------------------------------------------------------------
# Load TSV data
# ---------------------------------------------------------------------------
print("[build_seasonal_events] Loading TSVs...")

QUEST = read_tsv(newest("tsv/QUEST_Export_*.tsv"))
GMRW  = read_tsv(newest("tsv/GMRW_Export_*.tsv"))
GUIDE = read_tsv(newest("tsv/guide_index.tsv"))

# LVLI data (three files)
LVLI_LIST    = read_tsv(newest("tsv/LVLI_Export_*_LVLI_List.tsv"))
LVLI_ENTRIES = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Entries.tsv"))
LVLI_MATH    = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Math.tsv"))

GLOB = read_tsv(newest("tsv/GLOB_Export_*.tsv"))

# Item name lookups
BOOK = read_tsv(newest("tsv/BOOK_Export_*.tsv"))

_armo_files = glob.glob(str(_REPO_ROOT / "tsv" / "ARMO_Export_*.tsv"))
_armo_files = [f for f in _armo_files if "ObjectTemplate" not in os.path.basename(f)]
_armo_files.sort(key=lambda x: os.path.getmtime(x))
ARMO = read_tsv(_armo_files[-1]) if _armo_files else []

try:    MISC = read_tsv(newest("tsv/MISC_Export_*.tsv"))
except FileNotFoundError: MISC = []
try:    WEAP = read_tsv(newest("tsv/WEAP_Export_*.tsv"))
except FileNotFoundError: WEAP = []
try:    ALCH = read_tsv(newest("tsv/ALCH_Export_*.tsv"))
except FileNotFoundError: ALCH = []
try:    AMMO = read_tsv(newest("tsv/AMMO_Export_*.tsv"))
except FileNotFoundError: AMMO = []

# Curve tables (optional)
try:
    CURV = read_tsv(newest("tsv/CURV_Export_*.tsv", exclude_substrings=["_POINTS"]))
    CURV_PTS = read_tsv(newest("tsv/CURV_Export_*_POINTS.tsv"))
except FileNotFoundError:
    CURV, CURV_PTS = [], []

# Player titles (optional)
try:    PLYT = read_tsv(newest("tsv/PLYT_Export_*.tsv"))
except FileNotFoundError: PLYT = []
try:    CMPT = read_tsv(newest("tsv/CMPT_Export_*.tsv"))
except FileNotFoundError: CMPT = []

print(f"[build_seasonal_events] Loaded: {len(QUEST)} quests, {len(GMRW)} GMRW, "
      f"{len(LVLI_LIST)} LVLI lists, {len(LVLI_ENTRIES)} LVLI entries, "
      f"{len(GLOB)} GLOBs, {len(GUIDE)} guide rows")

# ---------------------------------------------------------------------------
# Build indices using rng76 engine
# ---------------------------------------------------------------------------
print("[build_seasonal_events] Building indices...")

lvli_idx = LvliIndex(LVLI_LIST, LVLI_ENTRIES, LVLI_MATH)
glob_idx = GlobIndex(GLOB)

curv_idx = None
if CURV and CURV_PTS:
    curv_idx = CurvIndex(CURV, CURV_PTS)

name_idx = ItemNameIndex()
name_idx.load_book(BOOK)
name_idx.load_armo(ARMO)
if MISC: name_idx.load_misc(MISC)
if WEAP: name_idx.load_weap(WEAP)
if ALCH: name_idx.load_alch(ALCH)
if AMMO: name_idx.load_ammo(AMMO)

resolver = Rng76Resolver(lvli_idx, glob_idx, name_idx, curvs=curv_idx)

# Player/camp title index
title_by_formid = {}
for t in PLYT:
    fid = pick(t, "FormID", "PLYT_FormID")
    if fid:
        title_by_formid[fid] = {
            "title": pick(t, "FULL - Name", "FULL", default=""),
            "kind": "player",
            "isPrefix": pick(t, "IsPrefix", default="") == "1",
            "isSuffix": pick(t, "IsSuffix", default="") == "1",
        }
for t in CMPT:
    fid = pick(t, "FormID", "CMPT_FormID")
    if fid:
        title_by_formid[fid] = {
            "title": pick(t, "FULL - Name", "FULL", default=""),
            "kind": "camp",
            "isPrefix": False,
            "isSuffix": False,
        }

# LVLI EDID lookup
lvli_edid_by_formid = {}
for row in LVLI_LIST:
    fid = pick(row, "FormID", "LVLI_FormID")
    edid = pick(row, "EDID", "LVLI_EDID")
    if fid and edid:
        lvli_edid_by_formid[fid] = edid

# GLOB value lookup
glob_vals = {}
for row in GLOB:
    fid = pick(row, "FormID", "GLOB_FormID")
    val = safe_float(pick(row, "FLTV", "FLTV - Value", default="0"))
    if fid:
        glob_vals[fid] = val

# GMRW indices
_GMRW_SKIP_RE = re.compile(r"^(zzz_|CUT_|POST_|DEL_|P62_)", re.IGNORECASE)

gmrw_rows_by_parent = defaultdict(list)
for r in GMRW:
    edid = pick(r, "EDID", "GMRW_EDID", default="")
    if _GMRW_SKIP_RE.match(edid):
        continue
    parent_raw = pick(r, "ParentQuestLink", "Parent Quest", default="")
    parent_fid = parent_raw.split(":")[0].strip() if parent_raw else ""
    if parent_fid:
        gmrw_rows_by_parent[parent_fid].append(r)

# ---------------------------------------------------------------------------
# Seasonal event definitions
# ---------------------------------------------------------------------------

# Map event key (norm_name of guide title) → quest EDID aliases
EVENT_KEY_ALIASES = {
    "fasnachtdayparade": ["fasnachtday", "eventfasnachtday", "e01ffasnacht"],
    "meatweek":          ["eventgrahmsmeatcook", "e02ameatbbq", "meatcook"],
    "mothmanequinox":    ["eventmothmanequinox", "e07amothman"],
    "invadersfrombeyond":["eventinvadersfrombeyond", "e07binvaders"],
    "treasurehunter":    ["seasonaltreasurehunter", "e04treasurehunter"],
    "holidayscorched":   ["spotlightholiday2018"],
    "halloweenscorched": ["halloweenscorched", "spookyscorched"],
    "mischiefnight":     ["eventmischiefnight", "e03amischief"],
    "thebigbloom":       ["thebigbloom", "ssebigbloom"],
}

# Container-based seasonal events: rewards come from opening containers,
# not from GMRW quest rewards.
CONTAINER_LOOT_EVENTS = {
    "treasurehunter": {
        "description": "Rewards from Mole Miner Pails (found/crafted). Tiers represent pail quality.",
        "pools": [
            {"title": "Ornate Mole Miner Pail (Found)",   "lvliFormID": "005D805A", "tier": ""},
            {"title": "Regular Mole Miner Pail (Found)",   "lvliFormID": "005D8054", "tier": ""},
            {"title": "Dusty Mole Miner Pail (Found)",     "lvliFormID": "005D8056", "tier": ""},
            {"title": "Ornate Mole Miner Pail (Crafted)",  "lvliFormID": "005D8053", "tier": ""},
            {"title": "Regular Mole Miner Pail (Crafted)", "lvliFormID": "005D8059", "tier": ""},
            {"title": "Dusty Mole Miner Pail (Crafted)",   "lvliFormID": "005D8055", "tier": ""},
        ],
    },
    "holidayscorched": {
        "description": "Rewards from Holiday Gifts dropped by Holiday Scorched. Tiers represent gift quality.",
        "pools": [
            {"title": "Large Holiday Gift (Found)",   "lvliFormID": "005DCA88", "tier": ""},
            {"title": "Medium Holiday Gift (Found)",  "lvliFormID": "005DCA8A", "tier": ""},
            {"title": "Small Holiday Gift (Found)",   "lvliFormID": "005DCA89", "tier": ""},
            {"title": "Large Holiday Gift (Crafted)",  "lvliFormID": "005DCA85", "tier": ""},
            {"title": "Medium Holiday Gift (Crafted)", "lvliFormID": "005DCA87", "tier": ""},
            {"title": "Small Holiday Gift (Crafted)",  "lvliFormID": "005DCA86", "tier": ""},
        ],
    },
    "halloweenscorched": {
        "description": "Rewards from Spooky Treat Bags dropped by Spooky Scorched.",
        "pools": [
            {"title": "Spooky Treat Bag", "lvliFormID": "0062038D", "tier": ""},
        ],
    },
}

# Quest descriptions from xEdit DESC field (fallback if not in TSV)
EVENT_DESCRIPTIONS = {
    "fasnachtdayparade":  "Join the fun during the Fasnacht Day parade and earn a chance at a festive mask!",
    "meatweek":           "Help Grahm prepare for the cookout! Cook meat, collect prime cuts, and earn unique rewards.",
    "mothmanequinox":     "Participate in the Mothman Equinox event and earn unique Cultist-themed rewards.",
    "invadersfrombeyond": "Defend against the Zetan invasion and earn unique alien-themed rewards.",
    "treasurehunter":     "Hunt down Mole Miner Treasure Hunters and open their pails for rare rewards.",
    "holidayscorched":    "Defeat Holiday Scorched enemies to collect Holiday Gifts containing rare plans and outfits.",
    "halloweenscorched":  "Take down Spooky Scorched to earn Spooky Treat Bags filled with Halloween-themed rewards.",
    "mischiefnight":      "A night of mischief and mayhem in the Whitespring.",
    "thebigbloom":        "Investigate the strange blooming phenomenon and earn unique rewards.",
}

# ---------------------------------------------------------------------------
# LVLI resolution using rng76 engine
# ---------------------------------------------------------------------------

_lvli_cache = {}

def compute_lvli(formid):
    """Resolve a leveled list into {leafFormID: probability} using rng76."""
    if formid in _lvli_cache:
        return _lvli_cache[formid]
    try:
        result = resolver.resolve_simple(formid)
        _lvli_cache[formid] = result
        return result
    except Exception as e:
        print(f"  [WARN] LVLI resolve failed for {formid}: {e}")
        _lvli_cache[formid] = {}
        return {}

def resolve_name(formid):
    """Get human-readable name for an item FormID."""
    return name_idx.resolve(formid) or humanize_edid(lvli_edid_by_formid.get(formid, formid))

def classify_pool(formid):
    """Classify a pool LVLI and return (poolTypes, title)."""
    edid = lvli_edid_by_formid.get(formid, "").lower()
    pool_types = []
    title = humanize_edid(lvli_edid_by_formid.get(formid, ""))

    if "plan" in edid or "recipe" in edid:
        pool_types.append({"type": "plan", "label": "Plans/Recipes"})
    if "apparel" in edid or "outfit" in edid or "headwear" in edid or "hat" in edid:
        pool_types.append({"type": "apparel", "label": "Apparel"})
    if "weapon" in edid:
        pool_types.append({"type": "weapon", "label": "Weapons"})
    if "armor" in edid or "armour" in edid:
        pool_types.append({"type": "armor", "label": "Armor"})
    if "mod" in edid:
        pool_types.append({"type": "mod", "label": "Mods"})
    if "camp" in edid or "workshop" in edid or "furniture" in edid:
        pool_types.append({"type": "camp", "label": "C.A.M.P."})
    if "stein" in edid:
        pool_types.append({"type": "stein", "label": "Steins"})

    if not pool_types:
        pool_types.append({"type": "misc", "label": "Misc"})

    return pool_types, title

# ---------------------------------------------------------------------------
# Quest indexing
# ---------------------------------------------------------------------------

quest_by_key = defaultdict(list)
quest_by_formid = {}
for q in QUEST:
    qid  = pick(q, "QUEST_FormID", "FormID")
    name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name",
                "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)
    quest_by_key[norm_name(name)].append(q)
    edid = pick(q, "QUEST_EDID", "EDID", default="")
    edid_key = norm_name(edid)
    if edid_key and edid_key != norm_name(name):
        quest_by_key[edid_key].append(q)
    if qid:
        quest_by_formid[qid] = q

def find_quest_candidates(key):
    """Find quest rows matching a guide event key, including aliases."""
    candidates = list(quest_by_key.get(key, []))
    for alias in EVENT_KEY_ALIASES.get(key, []):
        candidates.extend(quest_by_key.get(norm_name(alias), []))
    # Deduplicate by FormID
    seen = set()
    unique = []
    for q in candidates:
        fid = pick(q, "QUEST_FormID", "FormID")
        if fid not in seen:
            seen.add(fid)
            unique.append(q)
    return unique

# ---------------------------------------------------------------------------
# Guide mapping — find seasonal event reward pages
# ---------------------------------------------------------------------------
print("[build_seasonal_events] Mapping guide pages...")

reward_pages = []
for r in GUIDE:
    slug = (r.get("slug") or "").strip()
    url  = (r.get("url") or "").strip()
    # Only process seasonal event reward checklist pages
    if not slug.endswith("-all-rewards"):
        continue
    if "/seasonal-events/" not in url and "/seasonal-events/" not in slug:
        continue
    title = (r.get("title") or "").strip()
    base_title = title
    if base_title.lower().endswith(" all rewards"):
        base_title = base_title[:-len(" all rewards")].strip()
    reward_pages.append({
        "slug": slug, "url": url, "title": title,
        "eventTitle": base_title, "eventKey": norm_name(base_title),
    })

reward_pages_by_key = defaultdict(list)
for p in reward_pages:
    if p["eventKey"]:
        reward_pages_by_key[p["eventKey"]].append(p)

print(f"[build_seasonal_events] Found {len(reward_pages)} seasonal event reward pages")
for key in sorted(reward_pages_by_key):
    pages = reward_pages_by_key[key]
    print(f"  {key}: {pages[0]['eventTitle']} → {pages[0]['url']}")

# ---------------------------------------------------------------------------
# GMRW reward resolution helpers
# ---------------------------------------------------------------------------

def get_gmrw_rows_for_quest(q):
    """Get GMRW reward rows linked to a quest."""
    qid = pick(q, "QUEST_FormID", "FormID")
    rows = list(gmrw_rows_by_parent.get(qid, []))

    # Fallback: check GMRWRef columns on the quest row itself
    if not rows:
        for i in range(10):
            ref = pick(q, f"GMRWRef{i}", f"GMRW_Ref{i}", default="")
            if ref:
                ref_fid = ref.split(":")[0].strip()
                for r in GMRW:
                    gmrw_fid = pick(r, "FormID", "GMRW_FormID")
                    if gmrw_fid == ref_fid:
                        rows.append(r)
    return rows

def extract_tier_from_gmrw(gmrw_row):
    """Determine the reward tier from GMRW conditions/EDID."""
    edid = pick(gmrw_row, "EDID", "GMRW_EDID", default="").lower()
    if "gold" in edid or "tier1" in edid or "tier01" in edid:
        return "gold"
    if "silver" in edid or "tier2" in edid or "tier02" in edid:
        return "silver"
    if "bronze" in edid or "tier3" in edid or "tier03" in edid:
        return "bronze"
    if "mutated" in edid:
        return "mutated"
    return ""

def extract_gmrw_condition_chance(gmrw_row):
    """Extract GetRandomPercent condition from GMRW row."""
    for i in range(5):
        cond = pick(gmrw_row, f"Condition{i}", f"CTDA_Condition{i}", default="")
        if "GetRandomPercent" in cond:
            m = re.search(r'(\d+(?:\.\d+)?)', cond)
            if m:
                return float(m.group(1))
    return 100.0

def simplify_condition(cond_str):
    """Convert verbose xEdit condition string to human-readable text."""
    s = str(cond_str or "").strip()
    if not s:
        return ""
    # GetRandomPercent <= X → "X% chance"
    m = re.match(r'.*GetRandomPercent\s*<=?\s*(\d+(?:\.\d+)?)', s, re.IGNORECASE)
    if m:
        return f"{m.group(1)}% chance"
    # GetPublicEventHasMutation → "Mutated event"
    if "GetPublicEventHasMutation" in s:
        return "Mutated event"
    return s

# ---------------------------------------------------------------------------
# Build base rewards tier structure from GMRW
# ---------------------------------------------------------------------------

def build_base_rewards(gmrw_rows, quest_row):
    """Construct the tiered base rewards structure from GMRW data."""
    tiers = []
    tier_keys_seen = set()

    for r in gmrw_rows:
        tier_key = extract_tier_from_gmrw(r)
        if tier_key in tier_keys_seen:
            continue

        # Check for XP, Caps, and other base reward types
        edid = pick(r, "EDID", "GMRW_EDID", default="").lower()

        # Skip GMRW rows that are pure LVLI pool references (handled separately)
        lvli_ref = pick(r, "RewardLVLI", "Reward_LVLI", "LVLI", default="")
        lvli_fid = lvli_ref.split(":")[0].strip() if lvli_ref else ""

        tier_data = {
            "tier": tier_key,
            "xp": None, "xpFormID": None,
            "caps": None, "capsFormID": None,
            "legendaryRank": None,
            "lvliFormID": lvli_fid or None,
            "poolTypes": [],
            "titles": [],
        }

        # Extract XP from the GMRW row
        xp_val = safe_float(pick(r, "XP", "RewardXP", "XP_Value", default=""))
        if xp_val and xp_val > 0:
            tier_data["xp"] = int(xp_val)
            tier_data["xpFormID"] = pick(r, "XP_FormID", "XPFormID", default=None)

        # Extract Caps
        caps_val = safe_float(pick(r, "Caps", "RewardCaps", "Caps_Value", default=""))
        if caps_val and caps_val > 0:
            tier_data["caps"] = int(caps_val)
            tier_data["capsFormID"] = pick(r, "Caps_FormID", "CapsFormID", default=None)

        # Check LVLI pool types
        if lvli_fid:
            pool_types, _ = classify_pool(lvli_fid)
            tier_data["poolTypes"] = pool_types

        tier_keys_seen.add(tier_key)
        tiers.append(tier_data)

    # Sort tiers: "" (base) first, then gold, silver, bronze, mutated
    tier_order = {"": 0, "gold": 1, "silver": 2, "bronze": 3, "mutated": 4}
    tiers.sort(key=lambda t: tier_order.get(t["tier"], 99))

    return {"tiers": tiers}

# ---------------------------------------------------------------------------
# Build reward pools from GMRW LVLI references
# ---------------------------------------------------------------------------

def build_reward_pools(gmrw_rows):
    """Extract and resolve LVLI reward pools from GMRW rows."""
    pools = []
    seen_lvli = set()

    for r in gmrw_rows:
        edid = pick(r, "EDID", "GMRW_EDID", default="")

        # Check each Ref column for LVLI references
        for ref_key in ["RewardLVLI", "Reward_LVLI", "LVLI",
                        "Ref1", "Ref2", "Ref3", "Ref4", "Ref5"]:
            ref_raw = pick(r, ref_key, default="")
            if not ref_raw:
                continue
            lvli_fid = ref_raw.split(":")[0].strip()
            if not lvli_fid or lvli_fid in seen_lvli:
                continue

            # Check it's actually an LVLI
            if lvli_fid not in lvli_edid_by_formid:
                continue

            seen_lvli.add(lvli_fid)
            lvli_edid = lvli_edid_by_formid.get(lvli_fid, "")

            # Skip known non-reward LVLIs
            if any(skip in lvli_edid.lower() for skip in
                   ["lvlcreature", "lvlenemy", "lvlnpc", "lvlencounter"]):
                continue

            # Resolve the LVLI pool
            probs = compute_lvli(lvli_fid)
            if not probs:
                continue

            items = sorted([
                {
                    "formid": fid,
                    "name": resolve_name(fid),
                    "dropRate": pct(ch),
                    "qty": 1,
                    "isPlan": resolve_name(fid).startswith(("Plan:", "Recipe:")),
                }
                for fid, ch in probs.items()
            ], key=lambda x: (x["name"] or "", x["formid"] or ""))

            pool_types, pool_title = classify_pool(lvli_fid)
            tier = extract_tier_from_gmrw(r)
            pool_chance = extract_gmrw_condition_chance(r)

            # Collect conditions
            conditions = []
            for i in range(5):
                cond = pick(r, f"Condition{i}", f"CTDA_Condition{i}", default="")
                if cond:
                    simplified = simplify_condition(cond)
                    if simplified:
                        conditions.append(simplified)

            pools.append({
                "title": pool_title,
                "lvliFormID": lvli_fid,
                "lvliEdid": lvli_edid,
                "tier": tier,
                "count": "1",
                "conditions": conditions,
                "poolChance": pool_chance,
                "poolTypes": pool_types,
                "items": items,
                "itemCount": len(items),
            })

    pools.sort(key=lambda p: (p.get("title") or "", p.get("lvliFormID") or ""))
    return pools

# ---------------------------------------------------------------------------
# Build banners (Party Crasher / Mutated / Invaders — all "No" for seasonal)
# ---------------------------------------------------------------------------

def build_banners(quest_row):
    """Build notice banners. For seasonal events, all three are 'No'."""
    banners = []
    # Party Crashers, Mutated, Invaders are all "No" for seasonal events
    # (confirmed by TSV research April 2026). No banners to add.
    return banners

# ---------------------------------------------------------------------------
# Main build loop
# ---------------------------------------------------------------------------
print("[build_seasonal_events] Building event data...")

events  = []
by_page = {}

for key, pages in sorted(reward_pages_by_key.items()):
    candidates = find_quest_candidates(key)
    event_title = pages[0]["eventTitle"]
    print(f"\n  Processing: {event_title} (key={key})")

    # Build event object
    event = {
        "questFormID": "",
        "name": event_title,
        "gameName": "",
        "description": EVENT_DESCRIPTIONS.get(key, ""),
        "slug": pages[0].get("slug", ""),
        "url": pages[0].get("url", ""),
        "isContainerLoot": False,
        "containerLootDescription": "",
        "baseRewards": {"tiers": []},
        "pools": [],
        "banners": build_banners(None),
        "warnings": [],
        "freeRewards": [],
        "conditionalRewards": [],
    }

    if not candidates:
        print(f"    [WARN] No quest match for key '{key}'")
        event["warnings"].append({
            "title": "Missing QUEST match",
            "message": f"No quest found matching '{event_title}'. "
                       f"Checked key '{key}' and aliases {EVENT_KEY_ALIASES.get(key, [])}."
        })
    else:
        q = candidates[0]  # Use first match
        qid = pick(q, "QUEST_FormID", "FormID")
        event["questFormID"] = qid
        event["gameName"] = pick(q, "FULL - Name", "QUEST_FULL - Name", "FULL", default="")

        # Get description from quest data if not hardcoded
        if not event["description"]:
            event["description"] = pick(q, "DESC - Description", "DESC", "Description", default="")

        print(f"    Quest: {event['gameName']} ({qid})")

        # Get GMRW reward rows
        gmrw_rows = get_gmrw_rows_for_quest(q)
        print(f"    GMRW rows: {len(gmrw_rows)}")

        if gmrw_rows:
            # Build base rewards tiers
            event["baseRewards"] = build_base_rewards(gmrw_rows, q)

            # Build reward pools
            event["pools"] = build_reward_pools(gmrw_rows)
            print(f"    Pools: {len(event['pools'])}")

    # Container-based seasonal events: inject LVLI pools when GMRW yields none
    if key in CONTAINER_LOOT_EVENTS and not event["pools"]:
        cle = CONTAINER_LOOT_EVENTS[key]
        if cle.get("description"):
            event["containerLootDescription"] = cle["description"]
        event["isContainerLoot"] = True
        print(f"    Container loot event: injecting {len(cle['pools'])} pool(s)")

        for pool_def in cle.get("pools", []):
            formid = pool_def["lvliFormID"]
            lvli_edid = lvli_edid_by_formid.get(formid, "")
            probs = compute_lvli(formid)

            # Normalise probabilities if they don't sum to 1
            _total = sum(probs.values())
            if _total > 0 and abs(_total - 1.0) > 0.0001:
                probs = {k: v / _total for k, v in probs.items()}

            items = sorted([
                {
                    "formid": fid,
                    "name": resolve_name(fid),
                    "dropRate": pct(ch),
                    "qty": 1,
                    "isPlan": resolve_name(fid).startswith(("Plan:", "Recipe:")),
                }
                for fid, ch in probs.items()
            ], key=lambda x: (x["name"] or "", x["formid"] or ""))

            pool_types, pool_title = classify_pool(formid)
            event["pools"].append({
                "title": pool_def["title"],
                "lvliFormID": formid,
                "lvliEdid": lvli_edid,
                "tier": pool_def.get("tier", ""),
                "count": "1",
                "conditions": [],
                "poolChance": 100.0,
                "poolTypes": pool_types,
                "items": items,
                "itemCount": len(items),
                "isContainerLoot": True,
            })

        # Remove "Missing QUEST match" warning since we now have data
        event["warnings"] = [w for w in event.get("warnings", [])
                             if w.get("title") != "Missing QUEST match"]

    # Index by page slug and URL
    for p in pages:
        if p["slug"]:
            by_page[p["slug"]] = event
        if p["url"]:
            by_page[p["url"]] = event
            by_page[strip_trailing_slash(p["url"])] = event

    events.append(event)
    total_items = sum(p.get("itemCount", 0) for p in event["pools"])
    print(f"    Total items across all pools: {total_items}")

# ---------------------------------------------------------------------------
# Write output
# ---------------------------------------------------------------------------
print(f"\n[build_seasonal_events] Writing {len(events)} events to {DIST_DIR}/")

DIST_DIR.mkdir(parents=True, exist_ok=True)
PATCHLOG_DIR.mkdir(parents=True, exist_ok=True)

with open(DIST_DIR / "seasonal_events_rewards.json", "w", encoding="utf-8") as f:
    json.dump({"events": events}, f, separators=(",", ":"))

with open(DIST_DIR / "seasonal_events_rewards_by_page.json", "w", encoding="utf-8") as f:
    json.dump({"byPage": by_page}, f, separators=(",", ":"))

write_patchlog_feed(
    dist_dir=str(_REPO_ROOT / "dist"),
    feed_name="patchlog_latest_df_seasonal_events.json",
    current_items=events,
    key_field="questFormID",
    name_field="name,gameName",
    compare_fields=["name", "gameName", "pools"],
    prev_json_path="dist/seasonal_events/seasonal_events_rewards.json",
    items_extractor=lambda d: d.get("events", []),
)

print(f"[build_seasonal_events] Done. {len(events)} events, {len(by_page)} page mappings.")
