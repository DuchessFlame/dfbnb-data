#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_treasure_maps_json.py
===========================
Builds: dist/treasure_maps.json

Reads xEdit TSV exports and resolves the Treasure Map + U Mine It LVLI
hierarchies into structured JSON with drop rates.

Uses rng76 engine for deep LVLI resolution — flattens nested leveled lists
to leaf items with calculated drop rates per the drop-rate-engine skill rules.

Key LVLIs:
  LL_TreasureMap (0038B471)           - FirstMatch by region (5 entries)
  LLS_TreasureMap_{FF,TV,MTN,MTR,CB_SF} - pick-one map pools per region
  LL_TreasureMap_Reward (001A7220)    - UseAll reward chain (shared pools)
  LLS_TreasureMap_Reward_Base (0050CC2C) - region-specific reward branch
  MTRz05_LL_01_MinerMine (0032AD5F)   - UseAll Independent (max_count=0)
  MTRz05_LL_02_ProspectorMine (0032AD60)
  MTRz05_LL_03_ExcavatorMine (0032AD61)
  MTRZ05_Vendor_LuckyMaps (0086A8AF)  - pick-one lucky map vendor pool

Usage:
  python build_treasure_maps_json.py
"""

import csv, glob, json, os, re, sys
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

# ── rng76 engine import ──────────────────────────────────────────────────
_SRC = Path(__file__).resolve().parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from rng76 import (
    Rng76Data, safe_float, humanize_edid, fmt_pct,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR   = _REPO_ROOT / "dist"
TSV_DIR    = _REPO_ROOT / "tsv"

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
    full_pattern = str(TSV_DIR / pattern)
    files = glob.glob(full_pattern)
    if exclude_substrings:
        files = [f for f in files
                 if not any(s in os.path.basename(f) for s in exclude_substrings)]
    if not files:
        raise FileNotFoundError(f"No files matching {pattern} in {TSV_DIR}")
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


# ============================================================
# Region definitions
# ============================================================

# Map-name prefixes → region key + location URL
MAP_PREFIX_TO_REGION = OrderedDict([
    ("Forest",        {"key": "forest",        "name": "Forest",        "url": "/df/treasure-maps/forest/"}),
    ("Toxic Valley",  {"key": "toxic_valley",  "name": "Toxic Valley",  "url": "/df/treasure-maps/toxic-valley/"}),
    ("Ash Heap",      {"key": "ash_heap",      "name": "Ash Heap",      "url": "/df/treasure-maps/ash-heap/"}),
    ("Cranberry Bog", {"key": "cranberry_bog", "name": "Cranberry Bog", "url": "/df/treasure-maps/cranberry-bog/"}),
    ("Mire",          {"key": "the_mire",      "name": "The Mire",      "url": "/df/treasure-maps/the-mire/"}),
    ("Savage Divide", {"key": "savage_divide", "name": "Savage Divide", "url": "/df/treasure-maps/savage-divide/"}),
])

# Reward pool FormIDs per region (from LLS_TreasureMap_Reward_Base)
# Note: Cranberry Bog and The Mire share the same reward pool (0050CC2D) but
# are displayed as separate regions for clarity.
REWARD_REGION_FORMIDS = {
    "forest":       "0050CC2E",
    "toxic_valley": "0050CC31",
    "ash_heap":     "0050CC30",
    "cranberry_bog":"0050CC2D",
    "the_mire":     "0050CC2D",
    "savage_divide":"0050CC2F",
}

# Shared reward pool FormIDs (from LL_TreasureMap_Reward [001A7220])
SHARED_POOLS = OrderedDict([
    ("caps",         {"name": "Caps",             "formid": "0050CC2A"}),
    ("recipes",      {"name": "Recipes",          "formid": "004F6DAD"}),
    ("weapon_mods",  {"name": "Weapon Mod Plans",  "formid": "003D73D8"}),
    ("armour_mods",  {"name": "Armour Mod Plans",  "formid": "000673A7"}),
])

# Region display order (alphabetical; "The Mire" sorts under T)
REGION_ORDER = [
    "ash_heap", "burning_springs", "cranberry_bog", "forest",
    "savage_divide", "skyline_valley", "the_mire", "toxic_valley",
]

# Map drop region LVLIs (for extracting all unique maps)
# Note: cranberry_bog and the_mire share the same drop pool (LLS_TreasureMap_CB_SF)
# in the game, but the maps within it are displayed under separate region groups
# based on their name prefix.
MAP_POOL_FORMIDS = OrderedDict([
    ("forest",       "003D0CD5"),
    ("toxic_valley", "003D0CD6"),
    ("ash_heap",     "003D0CD7"),
    ("cranberry_bog","003D0CD8"),
    ("the_mire",     "003D0CD8"),
    ("savage_divide","003D0CD9"),
])

# ============================================================
# U Mine It definitions (unchanged)
# ============================================================

UMINE_TIERS = OrderedDict([
    ("miner",      {"formid": "0032AD5F", "edid": "MTRz05_LL_01_MinerMine",      "name": "Miner Map"}),
    ("prospector", {"formid": "0032AD60", "edid": "MTRz05_LL_02_ProspectorMine",  "name": "Prospector Map"}),
    ("excavator",  {"formid": "0032AD61", "edid": "MTRz05_LL_03_ExcavatorMine",   "name": "Excavator Map"}),
])

# ── Lucky Strike shared quest reward pools (fire on every quest completion) ──
# QuestReward_LLS_Aid_All [LVLI:0043934D] is a pick-one of 6 regional aid pools.
# Each entry's "ChanceNone" slot is actually a MinLvl GLOB (xEdit export trap —
# see drop-rate-engine skill section 5 "MinLvl GLOBs"). At Lv50 every region is
# unlocked, so the pick-one resolves to 1/6 = 16.667% per region pool.
#
# Each LL_Aid_<Region> LVLI is then resolved deep via rng76 to flatten its
# items, and the per-region rates are scaled by the parent 1/6 pick.
UMINE_AID_PARENT_FORMID = "0043934D"  # QuestReward_LLS_Aid_All
UMINE_AID_REGIONS = OrderedDict([
    ("forest",         {"formid": "003CC4A3", "edid": "QuestReward_LLS_Aid_Forest",        "name": "Forest",        "min_lvl": 1}),
    ("toxic_valley",   {"formid": "003CC4A5", "edid": "QuestReward_LLS_Aid_ToxicValley",   "name": "Toxic Valley",  "min_lvl": 10}),
    ("savage_divide",  {"formid": "003CC4A1", "edid": "QuestReward_LLS_Aid_SavageDivide",  "name": "Savage Divide", "min_lvl": 15}),
    ("ash_heap",       {"formid": "003CC4A0", "edid": "QuestReward_LLS_Aid_AshHeap",       "name": "Ash Heap",      "min_lvl": 25}),
    ("the_mire",       {"formid": "003CC4A4", "edid": "QuestReward_LLS_Aid_Mire",          "name": "The Mire",      "min_lvl": 30}),
    ("cranberry_bog",  {"formid": "003CC4A2", "edid": "QuestReward_LLS_Aid_CranberryBog",  "name": "Cranberry Bog", "min_lvl": 35}),
])

# LL_Scrap_Acid [LVLI:007AC791] is the second shared pool — pick-one of 3
# acid quantity variants (×1, ×2, ×3) at 33.333% each. resolve_deep handles
# the quantity variants automatically.
UMINE_ACID_FORMID = "007AC791"  # LL_Scrap_Acid


# ============================================================
# Helpers
# ============================================================

def simplify_condition(cond_str):
    """Convert verbose xEdit condition strings to human-readable summaries.

    Subset of the activities builder's simplifier — just the conditions that
    actually appear on treasure-map / U-Mine-It LVLIs (Ghoul gating, FO1st,
    learned-recipe). Other functions return "" so they're filtered out.
    """
    s = (cond_str or "").strip()
    if not s:
        return ""

    # GetIsPlayerGhoul → Ghoul / Human character restriction.
    # Trailing "0.000000" = comparison value 0 (must NOT be ghoul → Human only).
    # Trailing "1.000000" = comparison value 1 (must be ghoul → Ghoul only).
    if "GetIsPlayerGhoul" in s:
        if re.search(r'0\.0+\s*$', s):
            return "Human character only"
        return "Ghoul character only"

    # IsPlayerFO1Member → Fallout 1st membership check
    if "IsPlayerFO1Member" in s:
        return "Requires Fallout 1st membership"

    # HasLearnedRecipe → check comparison value
    if "HasLearnedRecipe" in s:
        if re.search(r'0\.0+\s*$', s):
            return "Won’t drop if you’ve already learned this plan"
        return "Requires the base plan to be learned"

    # GetRandomPercent / GetLevel are baked into rates — hide
    if "GetRandomPercent" in s or "GetLevel" in s:
        return ""

    # Internal/engine-only checks — hide
    for _fn in ("GetGlobalValue", "GetInCurrentLocation", "GetInCell",
                "LocationAliasIsLocation", "GetIsAliasRef", "HasEntitlement",
                "GetItemCount", "GetValue", "GetNumTimesCompletedQuest",
                "IsActivePlayer", "GetVMQuestVariable", "GetStageDone",
                "HasKeyword", "GetRemainingQuestTimeSeconds"):
        if _fn in s:
            return ""

    # Raw GLOB / QUST refs without a known wrapper — hide
    if re.match(r'^[0-9A-Fa-f]{8}:', s) and (":GLOB" in s or ":QUST" in s):
        return ""

    # Fallback: strip raw numeric flags at end and clean up
    s = re.sub(r'\s+[01]{8}\s+[\d.]+$', '', s)
    s = re.sub(r'^Subject\.', '', s)
    s = re.sub(r'\(00 00 00.*?\)', '()', s)
    return s.strip() if s.strip() else ""


def simplify_conditions(conditions):
    """Simplify a list of condition strings, dropping empty/internal results."""
    result = []
    for c in (conditions or []):
        s = simplify_condition(c)
        if s and s not in result:
            result.append(s)
    return result


def aggregate_items(items):
    """Aggregate resolved leaf items by formid, sum drop rates, track qty range."""
    agg = {}
    for it in items:
        fid = it["formid"]
        dr = it["dropRate"]
        if dr < 0.000001:
            continue
        if fid not in agg:
            agg[fid] = {
                "formid": fid,
                "name": it["name"],
                "edid": it.get("edid", ""),
                "sig": it.get("sig", ""),
                "drop_rate_raw": 0.0,
                "qty_min": it["qty"],
                "qty_max": it["qty"],
                "conditions": it.get("conditions", []),
            }
        agg[fid]["drop_rate_raw"] += dr
        agg[fid]["qty_min"] = min(agg[fid]["qty_min"], it["qty"])
        agg[fid]["qty_max"] = max(agg[fid]["qty_max"], it["qty"])
    # Cap individual rates at 1.0
    for v in agg.values():
        if v["drop_rate_raw"] > 1.0:
            v["drop_rate_raw"] = 1.0
    return sorted(agg.values(), key=lambda x: (-x["drop_rate_raw"], x["name"]))


def format_item(agg_item):
    """Format an aggregated item for JSON output."""
    qty_min = agg_item["qty_min"]
    qty_max = agg_item["qty_max"]
    qty_str = f"{qty_min}-{qty_max}" if qty_min != qty_max else str(qty_min)

    # Determine tradeable status from raw conditions BEFORE simplification
    # (NonPlayerTradable etc. are stripped by simplify_condition)
    name = agg_item.get("name", "")
    sig = agg_item.get("sig", "")
    raw_conditions = agg_item.get("conditions", [])
    tradeable = True
    for c in raw_conditions:
        if "NonPlayerTrad" in c or "Untradab" in c or "Untradea" in c:
            tradeable = False
            break

    return {
        "name": name,
        "form_id": agg_item["formid"],
        "edid": agg_item["edid"],
        "sig": sig,
        "drop_rate": fmt_pct(agg_item["drop_rate_raw"] * 100),
        "drop_rate_raw": round(agg_item["drop_rate_raw"], 6),
        "qty": qty_str,
        "conditions": simplify_conditions(raw_conditions),
        "tradeable": tradeable,
    }


def has_toggle_condition(entry):
    for c in entry.get("conditions", []):
        if "GetGlobalValue" in c and "Toggle" in c and "1.000000" in c:
            return True
    return False

def has_learned_recipe_condition(entry):
    for c in entry.get("conditions", []):
        if "HasLearnedRecipe" in c:
            return True
    return False

def get_toggle_glob_info(entry):
    for c in entry.get("conditions", []):
        m = re.search(r'(\w+_Toggle)\s*\[GLOB:([0-9A-Fa-f]{8})\]', c)
        if m:
            return m.group(1), m.group(2)
    return None, None

def get_quest_condition(entry):
    for c in entry.get("conditions", []):
        if "PlayerHasQuest" in c:
            m = re.search(r'"([^"]+)"\s*\[QUST:', c)
            if m:
                return f"Requires quest: {m.group(1)}"
            m2 = re.search(r'(\w+)\s*\[QUST:', c)
            if m2:
                return f"Requires quest: {humanize_edid(m2.group(1))}"
    return None


# ============================================================
# Build: Treasure Map Names (grouped by dig-location region)
# ============================================================

def build_map_names(entries_idx, books):
    """Collect all unique treasure maps from all region pools,
    group by dig-location region (name prefix), add location URLs."""

    # Gather all unique maps from every pool
    all_maps = {}
    for rkey, fid in MAP_POOL_FORMIDS.items():
        for e in entries_idx.entries(fid):
            mfid = e["ref_formid"]
            if mfid not in all_maps:
                all_maps[mfid] = {
                    "name": books.name(mfid),
                    "form_id": mfid,
                    "edid": books.edid(mfid),
                }

    # Group by name prefix → region
    region_maps = defaultdict(list)
    for m in sorted(all_maps.values(), key=lambda x: x["name"]):
        name = m["name"]
        parts = name.split(" Treasure Map ")
        prefix = parts[0] if len(parts) == 2 else "Unknown"
        info = MAP_PREFIX_TO_REGION.get(prefix)
        if info:
            m["location_url"] = info["url"]
            region_maps[info["key"]].append(m)

    return region_maps


# ============================================================
# Build: Flattened Shared Reward Pools (rng76 deep resolve)
# ============================================================

def build_shared_rewards(resolver):
    """Resolve each shared reward pool to flattened leaf items."""
    pools = []
    for pool_key, pool_info in SHARED_POOLS.items():
        raw_items = resolver.resolve_deep(pool_info["formid"])
        agg = aggregate_items(raw_items)
        pools.append({
            "pool_key": pool_key,
            "pool_name": pool_info["name"],
            "pool_formid": pool_info["formid"],
            "item_count": len(agg),
            "items": [format_item(it) for it in agg],
        })
    return pools


# ============================================================
# Build: Flattened Region-Specific Rewards (rng76 deep resolve)
# ============================================================

def build_region_rewards(resolver):
    """Resolve each region's specific reward pool to flattened leaf items."""
    region_items = {}
    for rkey, fid in REWARD_REGION_FORMIDS.items():
        raw_items = resolver.resolve_deep(fid)
        agg = aggregate_items(raw_items)
        region_items[rkey] = [format_item(it) for it in agg]
    return region_items


# ============================================================
# Build: Per-Region Mod Boxes (weapon + armour MISC mod items)
# ============================================================
#
# The treasure-map drop tree for weapon and armour mods passes through
# a FirstMatch-by-region LVLI which routes to a region-specific subtree.
# rng76's resolve_deep follows ONE branch (the first entry, Forest), so
# the JSON only ever shows Forest's mod items. The other regions' mod
# subtrees are never visited.
#
# Fix: walk each region's branch directly, then multiply rates by the
# probability of REACHING the FirstMatch parent in the first place. That
# probability is identical for every region (it's the path from the
# treasure map reward down to the FirstMatch list — region-independent).
#
# The path coefficient is computed empirically by comparing Forest's
# items in the full resolve to the same items resolved directly from
# Forest's branch entry. Since Forest is the FirstMatch's first entry
# (cum_fail = 1.0), the ratio is exactly the path coefficient.

PER_REGION_MOD_BRANCHES = OrderedDict([
    # branch_key -> (top_pools, {region_key: child_entry_formid})
    # top_pools: list of LVLI form IDs that lead to this branch's FirstMatch.
    #   The 4 entries inside LL_TreasureMap_Reward fire INDEPENDENTLY (UseAll
    #   with max_count > 4) so an item that's reachable via more than one of
    #   those entries gets multiple independent chances per dig. Mod boxes
    #   appear via two of those entries:
    #     - 003D73D8  weapon-mods-recipes pool   (cn-gated waterfall)
    #     - 004F6DAD  the all-recipes pool       (pick-one of 4 sub-pools)
    #   Both ultimately land in 003D73DA → 003136F5 (FirstMatch by region),
    #   and the per-dig rate is the SUM of contributions from each path.
    ("weapon_mods", {
        "top_pools": ["003D73D8", "004F6DAD"],
        "branches": OrderedDict([
            ("forest",         "003136C0"),  # LL_Recipes_Mods_Weapons_Any_RegionForest
            ("toxic_valley",   "00313696"),  # LL_Recipes_Mods_Weapons_Any_RegionToxicValley
            ("ash_heap",       "003136E6"),  # LL_Recipes_Mods_Weapons_Any_RegionAshHeap
            ("cranberry_bog",  "003136E7"),  # LL_Recipes_Mods_Weapons_Any_RegionCranberryBog
            ("the_mire",       "003136C1"),  # LL_Recipes_Mods_Weapons_Any_RegionMire
            ("savage_divide",  "00313695"),  # LL_Recipes_Mods_Weapons_Any_RegionSavageDivide
        ]),
    }),
    ("armour_mods", {
        # Armour mods sit behind 000673A7 directly AND behind 004F6DAD via
        # its armour-recipes sub-pool 003D73D9 — same dual-path setup.
        "top_pools": ["000673A7", "004F6DAD"],
        "branches": OrderedDict([
            ("forest",         "004F6816"),
            ("toxic_valley",   "004F682D"),
            ("ash_heap",       "004F6829"),
            ("cranberry_bog",  "004F682A"),
            ("the_mire",       "004F682B"),
            ("savage_divide",  "004F682C"),
        ]),
    }),
])


_MOD_BOX_EDID_RE = re.compile(r'^(?:dlc\d+_)?miscmod_mod', re.IGNORECASE)

# More specific patterns for dig-reward categorisation. Armour patterns are
# checked BEFORE the generic weapon pattern so that, e.g.,
# `recipe_mod_armor_RaiderMod_*` lands in armour_mod_plans, not weapon_mod_plans.
_ARMOUR_PLAN_EDID_RE   = re.compile(r'^recipe_mod_(armor|powerarmor|underarmor)', re.IGNORECASE)
_WEAPON_PLAN_EDID_RE   = re.compile(r'^recipe_mod_', re.IGNORECASE)
_ARMOUR_MODBOX_EDID_RE = re.compile(r'^(?:dlc\d+_)?miscmod_mod_(armor|powerarmor|underarmor)', re.IGNORECASE)

def _is_mod_box(item_dict):
    """A mod box is a MISC item whose EDID starts with 'miscmod_mod'
    (optionally prefixed with 'DLC<n>_' for Wastelanders/NW content).
    Excludes test/debug/cut prefixes like 'zzz_', 'CUT_', 'POST_', 'test_'."""
    edid = (item_dict.get("edid", "") or "")
    return bool(_MOD_BOX_EDID_RE.match(edid))


def _is_branch_mod_leaf(item_dict):
    """Items the per-region mod walker is responsible for: MISC mod boxes
    AND BOOK 'recipe_mod_*' plans. Both kinds of leaf live inside the
    per-region weapon/armour mod sub-trees. Other leaves (random WEAP/ARMO
    that may sit alongside) are not handled here — they flow through the
    shared-pool path or the region-specific reward path instead."""
    edid = (item_dict.get("edid", "") or "")
    sig  = (item_dict.get("sig", "") or "").upper()
    if _MOD_BOX_EDID_RE.match(edid):
        return True
    if sig == "BOOK" and _WEAPON_PLAN_EDID_RE.match(edid):
        return True
    return False


def categorize_dig_item(item):
    """Categorise a flattened dig-reward item into one of:
        weapon_mod_plan  - weapon mod plans (BOOK) and weapon mod boxes (MISC)
        armour_mod_plan  - armour / PA / underarmour mod plans + matching mod boxes
        recipe_plan      - general recipes & plans (workshop, food, whole-armour, etc.)
        region_bonus     - non-plan loot themed to the region (WEAP, ARMO, ALCH, AMMO, junk MISC)

    The buckets map directly to the user-facing Dig Rewards sub-expands."""
    edid = (item.get("edid", "") or "")
    sig = (item.get("sig", "") or "").upper()

    # Loose mod boxes (MISC items the player can apply directly to gear).
    if _MOD_BOX_EDID_RE.match(edid):
        if _ARMOUR_MODBOX_EDID_RE.match(edid):
            return "armour_mod_plan"
        return "weapon_mod_plan"

    # BOOK = plans and recipes.
    if sig == "BOOK":
        if _ARMOUR_PLAN_EDID_RE.match(edid):
            return "armour_mod_plan"
        if _WEAPON_PLAN_EDID_RE.match(edid):
            # Every recipe_mod_* that isn't armour-prefixed is a weapon mod plan.
            return "weapon_mod_plan"
        return "recipe_plan"

    # Everything else (WEAP / ARMO / ALCH / AMMO / generic MISC) is the
    # region-themed bonus loot pulled from LLS_TreasureMap_Reward_Region<X>.
    return "region_bonus"


def _compute_path_coeff(resolver, top_pools, forest_entry_id):
    """Empirically derive the total path-to-FirstMatch coefficient.

    Forest is the first entry in the FirstMatch list, so its cum_fail
    multiplier is 1.0 within each top pool's resolve. Forest items in
    resolve_deep(top_pool_id) have rate = path_coeff_for_that_pool ×
    within_forest_rate. Dividing by within_forest_rate (from a direct
    resolve of Forest's branch entry) gives path_coeff per pool.

    LL_TreasureMap_Reward is UseAll independent (max_count > entry count),
    so multiple top pools that route to the same FirstMatch contribute
    INDEPENDENTLY — the total per-dig coefficient is the SUM of each
    top pool's coefficient.

    Uses median ratio per pool to absorb floating-point noise.
    """
    forest_items = resolver.resolve_deep(forest_entry_id)
    forest_by_id = {it["formid"]: it["dropRate"] for it in forest_items}

    total_coeff = 0.0
    for top_pool_id in top_pools:
        full_items = resolver.resolve_deep(top_pool_id)
        ratios = []
        for it in full_items:
            fid = it["formid"]
            ftr = forest_by_id.get(fid)
            if ftr is not None and ftr > 0 and it["dropRate"] > 0:
                ratios.append(it["dropRate"] / ftr)
        if ratios:
            ratios.sort()
            total_coeff += ratios[len(ratios) // 2]
    return total_coeff


def build_per_region_mod_items(resolver):
    """For each region, return the list of mod-box items (raw rng76 format)
    with rates corrected to per-dig probability."""
    per_region_raw = defaultdict(list)
    for branch_name, cfg in PER_REGION_MOD_BRANCHES.items():
        top_pools     = cfg["top_pools"]
        branches      = cfg["branches"]
        forest_entry  = branches["forest"]
        path_coeff    = _compute_path_coeff(resolver, top_pools, forest_entry)
        if path_coeff <= 0:
            continue
        for region_key, entry_fid in branches.items():
            items = resolver.resolve_deep(entry_fid)
            for it in items:
                # Keep mod-box (MISC) and recipe_mod_* plan (BOOK) leaves —
                # both live inside the per-region branch and need the path
                # coefficient applied. Forest's are also handled here: they
                # are stripped from the shared pools by
                # filter_branch_mod_leaves_from_shared so they aren't
                # double-counted. Other leaves (any WEAP/ARMO that might
                # sit alongside) are left to the shared-pool path.
                if not _is_branch_mod_leaf(it):
                    continue
                scaled = dict(it)
                scaled["dropRate"] = it["dropRate"] * path_coeff
                per_region_raw[region_key].append(scaled)
    return per_region_raw


def filter_branch_mod_leaves_from_shared(shared_pools):
    """Strip per-region branch mod leaves from shared reward pools — both
    MISC mod boxes ('miscmod_mod*') and BOOK 'recipe_mod_*' plans are served
    per-region by build_per_region_mod_items, so leaving them in shared
    pools would cause Forest's items to be counted twice (once via the
    shared pool's resolve_deep walk of Forest's branch, once via the
    per-region collector). The per-region collector applies the empirically
    derived path coefficient that already sums both top_pool contributions,
    so it produces the correct per-dig rate for every region (including
    Forest)."""
    for pool in shared_pools:
        kept = [it for it in pool["items"] if not _is_branch_mod_leaf(it)]
        pool["items"] = kept
        pool["item_count"] = len(kept)
    return shared_pools


# ============================================================
# Per-Region Dig Rewards (categorised + caps)
# ============================================================
#
# Mirrors the JS `collectItemsForRegion` + `categorizeDigItems` logic but runs
# at build time so the JSON consumer just renders pre-bucketed lists.
#
# Categories (each bucket is a sub-expand on the Treasure Maps page):
#   recipes_plans     - general workshop / food / whole-weapon / whole-armour plans
#   weapon_mod_plans  - "Plan: <weapon> <mod>" BOOKs + loose weapon mod boxes
#   armour_mod_plans  - armour / PA / underarmour mod plans
#   region_bonus      - region-themed bonus loot (food, water, ammo, etc.)
#
# Plus the only other dig reward we have data for:
#   caps              - sourced from LL_Caps_TreasureMap (LLS_Caps_High, qty 3-7)
#
# NOTE on XP: there is NO treasure-map XP record in any current TSV export
# (QUEST, GMRW, GLOB, CURV, AVIF). If/when an XP source is found in the data
# (e.g. a curve referenced from a script, a GMRW entry tied to the mound
# activator, etc.) add it here. Until then, do not surface XP — no data, no
# claim.

# Region keyword -> our region key. Mirrors REGION_TOKENS in the JS so the
# Python categoriser sees the same regions the JS does.
_REGION_TOKENS = {
    "Forest":            "forest",
    "ForestFloodlands":  "forest",
    "ToxicValley":       "toxic_valley",
    "AshHeap":           "ash_heap",
    "Mountains":         "savage_divide",
    "CranberryBog":      "cranberry_bog",
    "Mire":              "the_mire",
    "SwampForest":       "the_mire",
    "BurningSprings":    "burning_springs",
    "SkylineValley":     "skyline_valley",
}
_DROP_REGIONS = ["forest", "toxic_valley", "ash_heap",
                 "cranberry_bog", "the_mire", "savage_divide"]
_REGION_LOC_RE = re.compile(r'Region(\w+?)Location')


def _regions_for_item(item):
    """Return the list of region keys an item is gated to via
    `Subject.GetInCurrentLocation(...Region<X>Location...)` conditions.
    Items with no region condition are treated as available in all regions."""
    hits = []
    for c in (item.get("conditions") or []):
        if "GetInCurrentLocation" not in c:
            continue
        m = _REGION_LOC_RE.search(c)
        if m and m.group(1) in _REGION_TOKENS:
            hits.append(_REGION_TOKENS[m.group(1)])
    return hits if hits else list(_DROP_REGIONS)


def _strip_region_conditions(conds):
    """Drop GetInCurrentLocation conditions — already implied by the region
    this list is rendered under, so they'd be noise in the output."""
    return [c for c in (conds or []) if "GetInCurrentLocation" not in c]


def build_dig_rewards(region_key, region, shared_pools):
    """Build the categorised dig-rewards payload for a single region.

    Returns a dict with caps metadata and four flat item lists:
    recipes_plans, weapon_mod_plans, armour_mod_plans, region_bonus."""
    by_id = {}
    caps_item = None

    def add(it):
        fid = it["form_id"]
        if fid not in by_id:
            by_id[fid] = {
                "name":          it["name"],
                "form_id":       fid,
                "edid":          it.get("edid", ""),
                "sig":           it.get("sig", ""),
                "qty":           it.get("qty", "1"),
                "drop_rate_raw": 0.0,
                "drop_rate":     "",
                "conditions":    _strip_region_conditions(it.get("conditions", [])),
                "tradeable":     it.get("tradeable", True) is not False,
            }
        by_id[fid]["drop_rate_raw"] += float(it.get("drop_rate_raw", 0) or 0)

    # 1) Items from shared reward pools that match this region. The "caps"
    #    pool is special-cased and lifted into its own field.
    for pool in shared_pools or []:
        if pool.get("pool_key") == "caps":
            if pool.get("items"):
                caps_item = pool["items"][0]
            continue
        for it in pool.get("items", []):
            if region_key in _regions_for_item(it):
                add(it)

    # 2) Region-specific reward items (region bonus loot + per-region mod boxes)
    for it in (region.get("region_reward_items") or []):
        add(it)

    # Cap rates at 1.0 and format as percent strings
    items = []
    for it in by_id.values():
        rate = min(it["drop_rate_raw"], 1.0)
        it["drop_rate_raw"] = round(rate, 6)
        it["drop_rate"]     = fmt_pct(rate * 100)
        items.append(it)

    # Categorise into the four buckets
    buckets = {
        "recipes_plans":    [],
        "weapon_mod_plans": [],
        "armour_mod_plans": [],
        "region_bonus":     [],
    }
    cat_to_bucket = {
        "recipe_plan":     "recipes_plans",
        "weapon_mod_plan": "weapon_mod_plans",
        "armour_mod_plan": "armour_mod_plans",
        "region_bonus":    "region_bonus",
    }
    for it in items:
        buckets[cat_to_bucket[categorize_dig_item(it)]].append(it)

    # Sort each bucket alphabetically by display name
    for k in buckets:
        buckets[k].sort(key=lambda x: (x.get("name") or "").lower())

    caps_payload = None
    if caps_item:
        caps_payload = {
            "name":          caps_item.get("name", "Caps"),
            "form_id":       caps_item.get("form_id", "0000000F"),
            "edid":          caps_item.get("edid", "Caps001"),
            "qty":           caps_item.get("qty", "3-7"),
            "drop_rate":     caps_item.get("drop_rate", "100%"),
            "drop_rate_raw": caps_item.get("drop_rate_raw", 1.0),
            "scales_with_buffs": False,
            "note":          "Does not scale with player level or other buffs",
        }

    return {
        "caps":             caps_payload,
        "recipes_plans":    buckets["recipes_plans"],
        "weapon_mod_plans": buckets["weapon_mod_plans"],
        "armour_mod_plans": buckets["armour_mod_plans"],
        "region_bonus":     buckets["region_bonus"],
    }


# ============================================================
# Build: Combined Region Output
# ============================================================

REGION_NAMES = OrderedDict([
    ("forest",          "Forest"),
    ("toxic_valley",    "Toxic Valley"),
    ("ash_heap",        "Ash Heap"),
    ("cranberry_bog",   "Cranberry Bog"),
    ("the_mire",        "The Mire"),
    ("savage_divide",   "Savage Divide"),
    ("burning_springs", "Burning Springs"),
    ("skyline_valley",  "Skyline Valley"),
])

REGION_LOCATION_URLS = {
    "forest":          "/df/treasure-maps/forest/",
    "toxic_valley":    "/df/treasure-maps/toxic-valley/",
    "ash_heap":        "/df/treasure-maps/ash-heap/",
    "cranberry_bog":   "/df/treasure-maps/cranberry-bog/",
    "the_mire":        "/df/treasure-maps/the-mire/",
    "savage_divide":   "/df/treasure-maps/savage-divide/",
    "burning_springs": "/df/treasure-maps/burning-springs/",
    "skyline_valley":  "/df/treasure-maps/skyline-valley/",
}


def build_regions(entries_idx, books, resolver, shared_pools):
    """Build the full region data: maps + region-specific rewards + mod boxes
    + categorised dig_rewards (caps / recipes / weapon mods / armour mods /
    region bonus)."""
    map_groups = build_map_names(entries_idx, books)
    region_rewards = build_region_rewards(resolver)
    per_region_mods = build_per_region_mod_items(resolver)

    regions = OrderedDict()
    for rkey in REGION_ORDER:
        maps = map_groups.get(rkey, [])
        rewards = region_rewards.get(rkey, [])
        # Append per-region mod-box items (aggregated + formatted) to this
        # region's reward items. Items without a region condition are added
        # so the JS picks them up under this region only.
        mod_raw = per_region_mods.get(rkey, [])
        if mod_raw:
            mod_agg = aggregate_items(mod_raw)
            rewards = list(rewards) + [format_item(it) for it in mod_agg]
        region_data = {
            "name": REGION_NAMES[rkey],
            "location_url": REGION_LOCATION_URLS.get(rkey, ""),
            "map_count": len(maps),
            "maps": maps,
            "region_reward_items": rewards,
        }
        # dig_rewards is the structured per-region payload the JS renders:
        # caps + categorised plan/recipe/mod buckets.
        region_data["dig_rewards"] = build_dig_rewards(rkey, region_data, shared_pools)
        regions[rkey] = region_data
    return regions


# ============================================================
# Build: U Mine It (uses manual resolution — not rng76)
# ============================================================

class LvliListIndex:
    def __init__(self, rows):
        self._d = {}
        for r in rows:
            fid = pick(r, "LVLI_FormID", default="").strip()
            if not fid:
                continue
            self._d[fid] = {
                "edid": pick(r, "LVLI_EDID", default=""),
                "flags": pick(r, "LVLF_Flags", default=""),
                "count": int(safe_float(pick(r, "EntryCount", "LLCT_Count", default="0"))),
                "maxValue": safe_float(pick(r, "LVMV_MaxValue", default="0")),
                "maxGlob": pick(r, "LVMG_MaxGlobal", default=""),
            }

    def get(self, fid):
        return self._d.get(fid.strip())

    def parse_flags(self, flags_str):
        f = flags_str or ""
        return {
            "useAll": len(f) > 2 and f[2] == '1',
            "firstMatch": len(f) > 6 and f[6] == '1',
        }

    def max_count(self, fid, globs):
        info = self.get(fid)
        if not info:
            return 0
        mg = info["maxGlob"]
        if mg:
            mg_fid = mg.split(":")[0] if ":" in mg else mg
            val = globs.fltv(mg_fid)
            if val is not None:
                return int(val)
        mv = info["maxValue"]
        if mv:
            return int(mv)
        return 0


class LvliEntriesIndex:
    def __init__(self, rows):
        self._d = defaultdict(list)
        for r in rows:
            pfid = pick(r, "LVLI_FormID", default="").strip()
            ref_raw = pick(r, "LVLO_Reference", default="")
            if not pfid or not ref_raw:
                continue
            ref_fid = ref_raw.split(":")[0] if ":" in ref_raw else ref_raw
            ref_edid = ref_raw.split(":")[1] if ref_raw.count(":") >= 1 else ""
            ref_sig = ref_raw.split(":")[2] if ref_raw.count(":") >= 2 else ""
            conds = []
            for ci in range(1, 11):
                c = pick(r, f"Cond{ci}", default="")
                if c:
                    conds.append(c)
            self._d[pfid].append({
                "index": int(safe_float(pick(r, "EntryIndex", default="0"))),
                "ref_formid": ref_fid, "ref_edid": ref_edid, "ref_sig": ref_sig,
                "chanceNoneValue": safe_float(pick(r, "LVOV_ChanceNoneValue", default="0")),
                "chanceNoneGlob": pick(r, "LVOG_ChanceNoneGlobal", default=""),
                "quantity": safe_float(pick(r, "LVIV_Quantity", default="1")),
                "condCount": int(safe_float(pick(r, "CondCount", default="0"))),
                "conditions": conds,
            })
        for fid in self._d:
            self._d[fid].sort(key=lambda e: e["index"])

    def entries(self, fid):
        return self._d.get(fid.strip(), [])


class GlobLookup:
    FALLBACKS = {"0043B770": 75.0, "0089EA90": 75.0}

    def __init__(self, rows):
        self._by_formid = {}
        fltv_key = None
        if rows:
            keys = list(rows[0].keys())
            for c in ("FLTV", "DATA"):
                if c in keys:
                    fltv_key = c
                    break
            if fltv_key is None and len(keys) >= 3:
                fltv_key = keys[2]
        for r in rows:
            fid = pick(r, "FormID", "GLOB_FormID", default="").strip()
            fltv = safe_float(r.get(fltv_key, "0") if fltv_key else "0", 0.0)
            edid = pick(r, "EDID", default="")
            if fid:
                self._by_formid[fid] = {"fltv": fltv, "edid": edid}

    def fltv(self, formid):
        formid = formid.strip()
        if formid in self._by_formid:
            return self._by_formid[formid]["fltv"]
        if formid in self.FALLBACKS:
            return self.FALLBACKS[formid]
        return None

    def edid(self, formid):
        formid = formid.strip()
        return self._by_formid.get(formid, {}).get("edid", "")


class BookLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid:
                self._by_formid[fid] = {
                    "full": pick(r, "FULL", default=""),
                    "edid": pick(r, "EDID", default=""),
                }

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]:
            return e["full"]
        if e and e["edid"]:
            return humanize_edid(e["edid"])
        return formid

    def edid(self, formid):
        e = self._by_formid.get(formid.strip())
        return e["edid"] if e else ""


class MiscLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid:
                self._by_formid[fid] = {
                    "full": pick(r, "FULL", default=""),
                    "edid": pick(r, "EDID", default=""),
                }

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]:
            return e["full"]
        if e and e["edid"]:
            return humanize_edid(e["edid"])
        return formid


class AlchLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid:
                self._by_formid[fid] = {
                    "full": pick(r, "FULL", default=""),
                    "edid": pick(r, "EDID", default=""),
                }

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]:
            return e["full"]
        if e and e["edid"]:
            return humanize_edid(e["edid"])
        return formid


def resolve_chance_none(entry, globs):
    cn_glob = entry.get("chanceNoneGlob", "")
    if cn_glob:
        glob_fid = cn_glob.split(":")[0] if ":" in cn_glob else cn_glob
        glob_edid = globs.edid(glob_fid)
        if "MinLvl" in glob_edid:
            return 0.0
        val = globs.fltv(glob_fid)
        if val is not None:
            return val
    cn_val = entry.get("chanceNoneValue", 0.0)
    if cn_val:
        return cn_val
    return 0.0


# ============================================================
# Build: U Mine It tiers (manual resolution for toggle conditions)
# ============================================================

def build_u_mine_it(list_idx, entries_idx, globs, books, misc, alch):
    def resolve_name(entry):
        fid, sig, edid = entry["ref_formid"], entry["ref_sig"], entry["ref_edid"]
        if sig == "BOOK":
            return books.name(fid)
        if sig == "MISC":
            return misc.name(fid)
        if sig == "ALCH":
            return alch.name(fid)
        return humanize_edid(edid) if edid else fid

    tiers = OrderedDict()
    for tier_key, tier_info in UMINE_TIERS.items():
        fid = tier_info["formid"]
        lvli_info = list_idx.get(fid)
        entries = entries_idx.entries(fid)
        max_c = list_idx.max_count(fid, globs) if lvli_info else 0

        rewards = []
        for e in entries:
            cn = resolve_chance_none(e, globs)
            rate = 1.0 - (cn / 100.0)

            toggle_name, toggle_fid = get_toggle_glob_info(e)
            conditions, notes = [], []

            if has_toggle_condition(e) and toggle_name:
                conditions.append(f"Requires {humanize_edid(toggle_name)} = ON")
                if toggle_fid:
                    notes.append(f"Toggle GLOB: {toggle_name} [{toggle_fid}]")
            if has_learned_recipe_condition(e):
                conditions.append("Won't drop if you've already learned this recipe")
            qc = get_quest_condition(e)
            if qc:
                conditions.append(qc)
            if cn > 0:
                notes.append(f"ChanceNone: {cn}")
                cn_glob_str = e.get("chanceNoneGlob", "")
                if cn_glob_str:
                    gfid = cn_glob_str.split(":")[0]
                    gedid = cn_glob_str.split(":")[1] if ":" in cn_glob_str else ""
                    notes.append(f"ChanceNone via GLOB {gedid} [{gfid}]")

            tradeable = True
            if "PlayerTitle" in e["ref_edid"] or "CampTitle" in e["ref_edid"]:
                tradeable = False

            rewards.append({
                "name": resolve_name(e),
                "edid": e["ref_edid"],
                "form_id": e["ref_formid"],
                "sig": e["ref_sig"],
                "quantity": int(e["quantity"]),
                "drop_rate": fmt_pct(rate * 100),
                "drop_rate_raw": round(rate, 6),
                "tradeable": tradeable,
                "conditions": conditions,
                "notes": notes,
            })

        tiers[tier_key] = {
            "name": tier_info["name"],
            "edid": tier_info["edid"],
            "form_id": fid,
            "list_type": "UseAll Independent (max_count=0)",
            "entry_count": len(entries),
            "rewards": rewards,
            "notes": [
                f"UseAll list, max_count={max_c} -> Independent",
                "Each entry fires independently at its own rate",
                "rate = 1 - (ChanceNone / 100)",
            ],
        }
    return tiers


# ============================================================
# Build: U Mine It shared quest reward pools (Aid + Acid + Junk)
# ============================================================
#
# These pools fire alongside the tier-specific mining LVLI on every Lucky
# Strike quest completion (regardless of tier). Each is resolved deep via
# rng76 so the JSON carries flattened leaf items the website can render
# directly under each region sub-expand without any additional lookups.
#
# Aid: parent LVLI 0043934D (QuestReward_LLS_Aid_All) is a pick-one across
# 6 regional sub-LVLIs gated by LVLV_MinimumLevel (Forest 1, Toxic Valley
# 10, Savage Divide 15, Ash Heap 25, Mire 30, Cranberry Bog 35). The list
# has the LVLF "Calculate from all levels" flag set, so at any player
# level it picks uniformly from every unlocked regional pool. We surface
# each tier as its own sub-expand labelled by player-level range, since
# regions are an implementation detail and confuse readers expecting a
# "your region" gate.
#
# MISC items inside the regional Aid LVLIs (Waste Acid etc.) are scrap
# components — they get hoisted out of the Aid pool into a separate
# Junk & Scrap shared pool so the Aid expand only carries true ALCH
# consumables.
#
# Acid: pick-one of 3 quantity variants on LL_Scrap_Acid. resolve_deep
# handles the qty variants automatically.
def build_u_mine_it_shared_pools(resolver):
    """Resolve the shared Aid + Acid + Junk pools fired on every Lucky Strike completion."""

    # ── Aid: tier-labelled sub-pools (one per region) ────────────────────
    # Build the level-range label from each region's min_lvl + the next
    # region's min_lvl - 1. Last region gets "X+" since there's no upper bound.
    # Items are split: ALCH stays in Aid, MISC is collected for the Junk pool.
    region_keys = list(UMINE_AID_REGIONS.keys())
    aid_regions = []
    junk_items_raw = []  # accumulator for MISC items pulled out of regional Aid LVLIs

    for i, rkey in enumerate(region_keys):
        rinfo = UMINE_AID_REGIONS[rkey]
        min_lvl = rinfo["min_lvl"]
        # Tier label: "Player Level 25 to 29" or "Player Level 35+" for the last tier
        if i + 1 < len(region_keys):
            next_min = UMINE_AID_REGIONS[region_keys[i + 1]]["min_lvl"]
            tier_label = f"Player Level {min_lvl} to {next_min - 1}"
        else:
            tier_label = f"Player Level {min_lvl}+"

        raw = resolver.resolve_deep(rinfo["formid"])
        # Split ALCH (Aid) vs MISC (Junk & Scrap). Anything else stays in Aid
        # by default — no surprises if a new sig appears.
        alch_raw = []
        for it in raw:
            sig = (it.get("sig") or "").upper()
            if sig == "MISC":
                junk_items_raw.append(it)
            else:
                alch_raw.append(it)

        agg = aggregate_items(alch_raw)
        aid_regions.append({
            "region_key": rkey,
            "name": rinfo["name"],         # kept for legacy/debug, not displayed
            "tier_label": tier_label,       # e.g. "Player Level 25 to 29"
            "edid": rinfo["edid"],
            "form_id": rinfo["formid"],
            "min_lvl": min_lvl,
            "tier_drop_rate": "100%",       # one tier always rolls per quest
            "tier_drop_rate_raw": 1.0,
            "item_count": len(agg),
            "items": [format_item(it) for it in agg],
        })

    aid_pool = {
        "key": "aid",
        "name": "Aid Items",
        "form_id": UMINE_AID_PARENT_FORMID,
        "edid": "QuestReward_LLS_Aid_All",
        "list_type": "Pick-one of 6 regional LVLIs (MinLvl-gated)",
        "drop_rate": "100%",
        "drop_rate_raw": 1.0,
        "blurb": f"Regional loot pool · {len(region_keys)} player level tiers · one tier always rolls",
        "regions": aid_regions,
        "notes": [
            "Parent LVLI 0043934D is a pick-one across 6 LL_Aid_<Region> sub-pools",
            "LVLF flag 0x01 (Level Filter) set → picks uniformly from every unlocked tier",
            "Each tier is shown as 100% because exactly one tier is always rolled",
            "MISC items (Waste Acid etc.) are pulled out into the Junk & Scrap pool",
        ],
    }

    # ── Junk & Scrap: MISC scrap components, including Waste Acid ───────
    # Combines two sources into a single expand:
    #   1. LL_Scrap_Acid (007AC791) — historically rendered as a separate
    #      "Acid" pool, but the only item it resolves to is Waste Acid
    #      (001BF72D), a MISC scrap component. It belongs under Junk.
    #   2. Any MISC items that appeared inside the regional Aid sub-LVLIs
    #      (currently none, but the filter is in place so a future xEdit
    #      export that adds MISC entries to those pools is handled
    #      automatically without another code change).
    # Items are aggregated by formid so Waste Acid would still appear once
    # if the same FormID showed up in multiple sources.
    acid_raw = resolver.resolve_deep(UMINE_ACID_FORMID)
    junk_combined = list(acid_raw) + list(junk_items_raw)
    junk_agg = aggregate_items(junk_combined)
    junk_pool = {
        "key": "junk_scrap",
        "name": "Junk & Scrap",
        "form_id": UMINE_ACID_FORMID,  # primary source FormID
        "edid": "LL_Scrap_Acid + LL_Aid_<Region> MISC subset",
        "list_type": "MISC scrap components rolled on every Lucky Strike completion",
        "drop_rate": "100%",
        "drop_rate_raw": 1.0,
        "blurb": "Guaranteed scrap drop · always rolled",
        "item_count": len(junk_agg),
        "items": [format_item(it) for it in junk_agg],
        "notes": [
            "Combines LL_Scrap_Acid (Waste Acid x1-3) with any MISC items from "
            "the regional Aid sub-LVLIs",
            "Waste Acid was historically displayed under a separate 'Acid' "
            "expand — now consolidated into Junk & Scrap to match its "
            "MISC signature (it is a scrap component, not a consumable)",
        ],
    }

    # Order: Aid → Junk & Scrap. The standalone "Acid" pool is gone; its
    # single item (Waste Acid) now lives under Junk & Scrap.
    return [aid_pool, junk_pool]


# ============================================================
# Build: Lucky Maps (unchanged)
# ============================================================

def build_lucky_maps(entries_idx, books):
    vendor_fid = "0086A8AF"
    vendor_entries = entries_idx.entries(vendor_fid)
    count = len(vendor_entries)
    items = []
    for e in vendor_entries:
        fid = e["ref_formid"]
        qc = get_quest_condition(e)
        items.append({
            "name": books.name(fid),
            "edid": books.edid(fid),
            "form_id": fid,
            "drop_rate": fmt_pct((1.0 / count) * 100) if count > 0 else "0%",
            "conditions": [qc] if qc else [],
        })
    drop_sources = [
        {"source": "Vendor (Purveyor Murmrgh)", "list": "MTRZ05_Vendor_LuckyMaps",
         "notes": "Pick-one from 3 maps, requires Lucky Strike quest not active"},
        {"source": "Activity Rewards", "list": "RA_LL_Rewards_Activities_UMineItMaps",
         "notes": "ChanceNone via GLOB LTT_RA_Rewards_Activities_UMineItMap_DropRate (runtime toggle)"},
        {"source": "Public Event Rewards", "list": "RA_LLS_Loot_UmineItMaps",
         "notes": "Requires Lucky Strike quest not active"},
        {"source": "Safe Containers", "list": "Various LLE_Safe_* lists",
         "notes": "Excavator Map only; ChanceNone 70-95 depending on safe difficulty"},
        {"source": "Motherlode Container (Rare)", "list": "MTR05_LLI_MotherlodeContainer_Rare",
         "notes": "Excavator Map; ChanceNone 70"},
    ]
    return {
        "vendor_list": "MTRZ05_Vendor_LuckyMaps",
        "vendor_formid": vendor_fid,
        "items": items,
        "drop_sources": drop_sources,
        "notes": [
            "Lucky Maps start the U Mine It questline (Lucky Strike)",
            "Three tiers: Miner Map -> Prospector Map -> Excavator Map",
            "Each tier leads to progressively better mining rewards",
        ],
    }


def build_teammate_reward(entries_idx):
    fid = "001A721E"
    entries = entries_idx.entries(fid)
    count = len(entries)
    items = []
    for e in entries:
        items.append({
            "name": humanize_edid(e["ref_edid"]),
            "edid": e["ref_edid"],
            "form_id": e["ref_formid"],
            "sig": e["ref_sig"],
            "drop_rate": fmt_pct((1.0 / count) * 100) if count > 0 else "0%",
        })
    return {
        "list_edid": "LL_TreasureMap_TeammateReward",
        "list_formid": fid,
        "list_type": "Pick-one",
        "items": items,
        "notes": [
            "Teammates get a random reward when a team member digs a treasure mound",
            f"Pick-one from {count} entries (equal chance each)",
        ],
    }


def main():
    print("[build_treasure_maps_json.py] Starting build...")
    lvli_list_path = newest("LVLI_Export_*_LVLI_List.tsv")
    lvli_entries_path = newest("LVLI_Export_*_LVLI_Entries.tsv")
    book_path = newest("BOOK_Export_*.tsv", exclude_substrings=["Locations"])
    glob_path = newest("GLOB_Export_*.tsv")
    source_files = [os.path.basename(p) for p in [lvli_list_path, lvli_entries_path, book_path, glob_path]]
    for label, p in [("LVLI List", lvli_list_path), ("LVLI Entries", lvli_entries_path),
                     ("BOOK", book_path), ("GLOB", glob_path)]:
        print(f"  {label}: {os.path.basename(p)}")
    list_idx = LvliListIndex(read_tsv(lvli_list_path))
    entries_idx = LvliEntriesIndex(read_tsv(lvli_entries_path))
    globs = GlobLookup(read_tsv(glob_path))
    books = BookLookup(read_tsv(book_path))
    misc, alch = MiscLookup([]), AlchLookup([])
    try:
        misc_path = newest("MISC_Export_*.tsv")
        misc = MiscLookup(read_tsv(misc_path))
        source_files.append(os.path.basename(misc_path))
    except FileNotFoundError:
        pass
    try:
        alch_path = newest("ALCH_Export_*.tsv")
        alch = AlchLookup(read_tsv(alch_path))
        source_files.append(os.path.basename(alch_path))
    except FileNotFoundError:
        pass

    print("  Loading rng76 engine...")
    rng_data = Rng76Data.from_tsv_root(str(TSV_DIR))
    resolver = rng_data.resolver

    # Shared pools are built first because build_regions now needs them
    # (per-region dig_rewards walk shared pools to attach the matching items).
    print("  Building shared reward pools...")
    shared_pools = build_shared_rewards(resolver)
    # Mod-box items AND BOOK recipe_mod_* plans are served per-region (see
    # build_per_region_mod_items); remove them from shared pools to avoid
    # double-counting Forest's leaves on the website (the shared-pool
    # resolve_deep only walks Forest's FirstMatch branch).
    shared_pools = filter_branch_mod_leaves_from_shared(shared_pools)
    print("  Building regions (deep LVLI resolution)...")
    regions = build_regions(entries_idx, books, resolver, shared_pools)
    print("  Building U Mine It...")
    tiers = build_u_mine_it(list_idx, entries_idx, globs, books, misc, alch)
    print("  Building U Mine It shared pools (Aid + Acid)...")
    umine_shared = build_u_mine_it_shared_pools(resolver)
    print("  Building Lucky Maps...")
    lucky = build_lucky_maps(entries_idx, books)
    print("  Building teammate reward...")
    teammate = build_teammate_reward(entries_idx)

    total_maps = sum(r["map_count"] for r in regions.values())
    total_shared = sum(p["item_count"] for p in shared_pools)
    total_region_items = sum(len(r["region_reward_items"]) for r in regions.values())

    output = {
        "shared_reward_pools": shared_pools,
        "regions": regions,
        "teammate_reward": teammate,
        "u_mine_it": {"tiers": tiers, "shared_pools": umine_shared, "lucky_maps": lucky},
        "meta": {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "generator": "build_treasure_maps_json.py",
            "source_files": source_files,
            "notes": [
                "Drop rates resolved via rng76 engine (deep LVLI flattening)",
                "Shared reward pools apply to ALL treasure map digs",
                "Region-specific rewards vary by dig location",
                "Mod boxes (miscmod_mod*) are computed per region, not via shared pools",
                "GMRW conditions NOT baked in - handled by website JS",
                "Burning Springs and Skyline Valley are empty placeholders",
                "Per-region dig_rewards: caps + categorised plan/mod buckets",
                "Caps sourced from LL_Caps_TreasureMap (LLS_Caps_High, qty 3-7, 100%)",
                "No XP field — there is no treasure-map XP record in any current TSV export",
            ],
        },
    }

    os.makedirs(str(DIST_DIR), exist_ok=True)
    out_path = DIST_DIR / "treasure_maps.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n  Output: {out_path}")
    print(f"  Regions: {len(regions)}, Maps: {total_maps}")
    print(f"  Shared pools: {len(shared_pools)}, Shared items: {total_shared}")
    print(f"  Region-specific items: {total_region_items}")
    aid_tier_count = sum(len(p.get("regions", [])) for p in umine_shared if p["key"] == "aid")
    aid_item_count = sum(
        sum(r["item_count"] for r in p["regions"])
        for p in umine_shared if p["key"] == "aid"
    )
    junk_item_count = sum(p["item_count"] for p in umine_shared if p["key"] == "junk_scrap")
    print(f"  U Mine It tiers: {len(tiers)}, Lucky Maps: {len(lucky['items'])}")
    print(f"  U Mine It shared pools: Aid ({aid_tier_count} tiers, {aid_item_count} items), Junk & Scrap ({junk_item_count} items)")
    print("[build_treasure_maps_json.py] Done.")

    patchlog_dir = DIST_DIR / "patchlogs"
    os.makedirs(str(patchlog_dir), exist_ok=True)
    write_empty_patchlog_feed(str(DIST_DIR), "patchlog_latest_df_treasure_maps.json", current_count=total_maps)


if __name__ == "__main__":
    main()
