#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_daily_ops_rewards_json.py - Daily Ops All Rewards (Activities-shaped)

Writes (one JSON per page, so one bad file can never blank another page):
    dist/daily_ops/daily_ops_all_rewards.json        All Rewards page tree
                                                     (df-bnb-daily-ops-rewards.js)
    dist/daily_ops/daily_ops_reward_checklist.json   flat pools for the Reward
                                                     Checklist + Guide pages
                                                     (df-bnb-daily-ops.js) and the
                                                     Unique Weapons & Armour lookup
    dist/daily_ops/patchlog_latest_df_daily_ops_rewards.json

This is the ONLY Daily Ops rewards builder. It replaced build_daily_ops_json.py
(-> daily_ops_rewards.json, retired 26 Sept 2026). It reads the xEdit exports
directly and does NOT depend on dist/events/events_rewards.json, so a broken
events build cannot take Daily Ops down. Each JSON is built and written on its
own (atomic write, parse-checked); if one fails the other still ships and the
old copy of the failed one stays live.

ROOT EXPANDS (one per way of earning rewards, built from the GMRW rows):
    Elder / Paladin / Knight  first clear of the day at that rank. CUMULATIVE -
                              the rank rows gate on remaining time <= 480/720/960,
                              so Elder also passes Paladin + Knight, Paladin also
                              passes Knight (confirmed in-game 26 Sept 2026).
    Every Clear               GMRW row with no conditions -> every completion.
    Double Mutation           GetGlobalValue == 1 + Elder timer, every Elder clear.
    Slasher Daily Ops         SDOW_ lists: first Elder clear 100%, repeats 25%.

WHY THIS FILE IMPORTS build_events_rewards_json
-----------------------------------------------
The LVLI tree walker (build_lvli_tree_node / resolve_lvli_items_deep) needs a
closure of 20 helper functions and 29 TSV-derived module globals. Copying that
would mean a third hand-rolled copy of the formula layer - the single most
common source of bugs in this project (drop-rate-engine S12). Instead
build_events_rewards_json.py was made import-safe (its build loop sits under an
`if __name__ == "__main__"` guard) and we reuse its walker directly. One engine,
one set of formulas.

THE TIER ROLL FIX
-----------------
Each chase tier LVLI carries a LIST-LEVEL condition (drop-rate-engine S6,
listSelfChance) - not an entry condition:

    Subject.GetRandomPercent <= DailyOps_RareRoll_Tier_<High|Medium|Low>

        DailyOps_RareRoll_Tier_High   [GLOB:005CB979]  FLTV 100  -> Elder   100%
        DailyOps_RareRoll_Tier_Medium [GLOB:005CB97A]  FLTV  10  -> Paladin  10%
        DailyOps_RareRoll_Tier_Low    [GLOB:005CB97B]  FLTV   5  -> Knight    5%

build_lvli_tree_node attaches list-level conditions for DISPLAY only and never
turns them into a rate, so the tier roots come back with entryRate = None. The
renderer's cascade (flattenTreeItems, multiplying entryRate/100 down the tree)
therefore starts at 1.0 and every rate under Paladin ends up 10x too high and
every rate under Knight 20x too high.

The fix is to stamp the resolved list-level chance onto the tier root as
entryRate. The renderer already multiplies by entryRate/100, so the existing
cascade picks it up with no JS change. Never read the GLOB FLTV as a percentage
by hand - `_extract_grp_chance` is operator-aware ( <= 40 is a 40% gate,
>= 40 is a 60% gate ) and resolves the GLOB through the shared resolver.

NOTE: this fixes Daily Ops only. The same list-level rate is ignored for every
other page that goes through build_lvli_tree_node (events, activities). That is
a separate, site-wide call - see HANDOFF notes.

Usage:
    python3 src/build_daily_ops_rewards_json.py
    (PTS twin: dfbnb-pts-build.yml runs it normally, then relocates dist/.)
"""

import copy
import csv
import json
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

import build_events_rewards_json as ev      # import-safe: writes nothing
import tsv_source
from events_page_pools import process_pool, merge_duplicate_pools
from rng76 import parse_randompercent_multiplier
from patchlog_utils import write_patchlog_feed, _git_show_json

# NO --pts MODE, ON PURPOSE.
#
# dfbnb-pts-build.yml normalises tsv/pts/ over tsv/, runs every builder WITHOUT
# --pts, and only then relocates the whole dist/ tree to dist/pts/. So the PTS
# twin of this file is produced by running this script normally inside that
# workflow - there is nothing for a --pts flag to do.
#
# A flag that only changed the OUTPUT path would be actively dangerous here:
# this builder imports build_events_rewards_json, which globs tsv/ directly and
# has no channel switch, so it would read LIVE exports and write them into
# dist/pts/ - live data wearing a PTS label. That is the channel-crossing bug
# tsv_source was written to stop (its docstring: "Silently crossing channels is
# what put PTS grave placements on a live page for five weeks"). Refuse instead.
if "--pts" in sys.argv:
    raise SystemExit(
        "build_daily_ops_rewards_json.py has no --pts mode.\n"
        "The PTS twin is built by dfbnb-pts-build.yml, which normalises\n"
        "tsv/pts/ over tsv/, runs this script normally, then relocates\n"
        "dist/ -> dist/pts/. Running --pts here would write LIVE data into\n"
        "the PTS path."
    )

OUT_DIR = _REPO_ROOT / "dist/daily_ops"

# ---------------------------------------------------------------------------
# Daily Ops constants - all verified against the September 2026 exports.
# ---------------------------------------------------------------------------

QUESTS = {
    "005A77D4": "Uplink",
    "005F0897": "Decryption",
}

# GMRW ConditionGlobs -> timer tier. EDIDs are High/Medium/Low; the player-facing
# names are Elder/Paladin/Knight. Keep both straight.
TIER_BY_GLOB = {
    "005CB976": "elder",     # DailyOps_Timer_Tier_High    FLTV 480
    "005CB977": "paladin",   # DailyOps_Timer_Tier_Medium  FLTV 720
    "005CB978": "knight",    # DailyOps_Timer_Tier_Low     FLTV 960
}

TIER_META = {
    "elder":   {"label": "Elder",   "seconds": 480, "timeLabel": "8:00",  "order": 0},
    "paladin": {"label": "Paladin", "seconds": 720, "timeLabel": "12:00", "order": 1},
    "knight":  {"label": "Knight",  "seconds": 960, "timeLabel": "16:00", "order": 2},
}
TIER_ORDER = ["elder", "paladin", "knight"]

# Root expands, in page order. Rank roots are cumulative (see build_rank_root):
# the GMRW rank rows gate on GetRemainingQuestTimeSeconds <= DailyOps_Timer_Tier_*,
# and 480 <= 720 <= 960, so an Elder clear passes all three rank rows, a Paladin
# clear passes Paladin + Knight, and a Knight clear passes Knight only.
# Confirmed in-game by Duchess, 26 Sept 2026.
ROOT_META = {
    "elder": {
        "label": "Elder",
        "subtitle": "First Elder clear of the day · Elder + Paladin + Knight rewards",
    },
    "paladin": {
        "label": "Paladin",
        "subtitle": "First Paladin clear of the day · Paladin + Knight rewards",
    },
    "knight": {
        "label": "Knight",
        "subtitle": "First Knight clear of the day · Knight rewards",
    },
    "everyClear": {
        "label": "Repeatable",
        "subtitle": "Every clear at any rank, including your first of the day · no plans",
        "note": ("Paid on every clear. Your first clear of the day gets this as well "
                 "as your rank rewards; after that, repeat runs only get this."),
    },
    "doubleMutation": {
        "label": "Double Mutation",
        "subtitle": "Double mutation ops only · Elder clears, every run",
        "note": ("Double mutation ops cleared in 8:00 or less. The extra "
                 "currency only pays on your first Elder clear of the day; the "
                 "double mutation bonus pays on every Elder clear."),
    },
    "slasher": {
        "label": "Slasher Daily Ops",
        "subtitle": "Slasher ops only · during the Pint-Sized Slasher event",
        "note": ("Only fires while the Pint-Sized Slasher event is running and the "
                 "op's enemy is the Slasher encounter. The first Elder clear of the "
                 "day always rolls this list; repeat clears roll it 1 time in 4."),
    },
}
ROOT_ORDER = ["elder", "paladin", "knight", "everyClear", "doubleMutation", "slasher"]

POOL_TITLES = {
    "005DBD0D": "Elder Rewards",
    "005DBD0F": "Paladin Rewards",
    "005CB9FF": "Knight Rewards",
    "005DBD10": "Currency",
    "00612800": "Legendary Modules",
    "005CBA06": "Repeatable Rewards",
    "0061F72A": "Double Mutation Bonus",
    "00935CEA": "First Elder Clear of the Day",
    "008FCEA5": "Repeat Clears",
}

# Intro card copy. Lives in the data, not the renderer, so it can be corrected
# without an FTP push. Every claim here is traceable to a record:
#   tier times          GLOB 005CB976/7/8  (480 / 720 / 960)
#   chase roll 100/10/5 GLOB 005CB979/A/B via each tier list's ListCond1
#   ranks stack         GMRW rank rows use <= on nested timers (confirmed in-game)
#   level split         005DBD0D entry 0 GetLevel >= 50 + First Match fallthrough
#   every clear         GMRW 006311EA index 3 has no conditions
#   double mutation     GMRW index 4: GetGlobalValue == 1 + Tier_High, no GetValue
#   slasher             GMRW index 7/8 + list gates 005CB979 (100) / 00935CE9 (25)
DESCRIPTION = (
    "Daily Ops rewards, pulled straight from the game files. Your clear time "
    "decides your rank, and your rank decides how many reward rolls you get."
)

INFO_CARD = [
    "There are two kinds of op - Uplink and Decryption. They drop exactly the "
    "same rewards, so everything below applies to both.",

    "Finish an op and the game checks your clear time. Under 8 minutes is Elder, "
    "under 12 is Paladin, under 16 is Knight.",

    "Ranks stack. An Elder clear also counts as a Paladin and a Knight clear, so "
    "it rolls all three reward lists: Elder at 100%, Paladin at 10% and Knight at "
    "5%. A Paladin clear rolls Paladin and Knight. A Knight clear rolls Knight only.",

    "Each rank's rewards pay out once per day. After that, repeat runs only get "
    "the Repeatable rewards - currency, aid, ammo, up to three grenades and a 3★ "
    "Legendary Item. There are no plans or recipes in it at all.",

    "Each reward roll is one item. The game picks a list, then a list inside that, "
    "then one thing out of it.",

    "Your level picks which reward list you are on. At level 50 and up you roll "
    "the high-level list. At 49 and under you roll the low-level one. There is no "
    "crossover in either direction.",

    "Double mutation ops add a bonus pool on Elder clears only - and unlike the "
    "rank rewards, it pays on every Elder clear, not just the first.",

    "During the Pint-Sized Slasher event, Slasher ops add their own list: "
    "guaranteed on your first Elder clear of the day, 1 in 4 on repeat clears.",

    "The Rare (Untradable) plans - the War Glaive, Plasma Cutter, "
    "Crusader Pistol, Face Breaker, Hellstorm Missile Launcher, the "
    "Brotherhood Recon, Covert Scout and Arctic Marine armour, the Deep-Space "
    "Alien power armour paints and a few others - stop dropping once you've "
    "learned them, and so does Player Title: Pint-Sized. Everything else keeps "
    "rolling after you learn it, which is why the same plan keeps coming back.",
]

DAILY_OPS_LOCATION_FLST = "005C65E0"
DAILY_OPS_LOCATION_LARGE_FLST = "00602815"
DAILY_OPS_LOCATION_SMALL_FLST = "00606278"

LOCATION_IMAGE_BASE = "/wp-content/uploads/guide-images/daily-ops/locations/"


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


def read_tsv_rows(pattern):
    path = tsv_source.newest(pattern)
    if not path:
        return []
    csv.field_size_limit(10 ** 9)
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))


# ---------------------------------------------------------------------------
# THE FIX - resolve a list's own GetRandomPercent gate into a 0-100 rate.
# ---------------------------------------------------------------------------

def list_self_chance_pct(list_formid):
    """
    List-level GetRandomPercent gate for an LVLI, as a 0-100 percentage.

    Returns None when the list has no list-level GRP condition (most lists).
    Operator-aware and GLOB-resolving - never read the GLOB FLTV directly.
    """
    raw = ev.LVLI_LIST_CONDITIONS.get(list_formid) or []
    if not raw:
        return None
    chance = ev._extract_grp_chance(raw)
    if chance is None:
        return None
    return round(chance * 100.0, 6)


def stamp_list_self_chance(node):
    """
    Apply a tree node's own list-level gate as its entryRate.

    The renderer cascades by multiplying entryRate/100 down the tree, so putting
    the gate here makes every descendant rate correct with no JS change. Only
    set it when the node has no entryRate of its own - an entryRate that is
    already present came from the PARENT's entry for this node and must win,
    otherwise the two gates would be conflated.
    """
    if not isinstance(node, dict):
        return node
    pct = list_self_chance_pct(node.get("formid") or "")
    if pct is None:
        return node
    node["listSelfChance"] = pct
    if node.get("entryRate") is None:
        node["entryRate"] = pct
    return node


# ---------------------------------------------------------------------------
# Level lock - the chase list split
# ---------------------------------------------------------------------------
# Verified in xEdit on LL_DailyOps_Rewards_Chase_Tier03 (005DBD0D):
#
#   LVLF includes "Use first object that matches all conditions (Evaluate as
#   Stack)" -> First Match (drop-rate-engine S3f).
#
#   Entry 0 -> LLS_DailyOps_Rewards_Chase_HighLVL [005D9CBB]
#              Conditions: Subject.GetLevel >= 50
#   Entry 1 -> LLS_DailyOps_Rewards_Chase_LowLVL  [005D9CBD]
#              Conditions: (none)
#
# First Match takes the FIRST entry whose conditions pass and stops. So at 50+
# entry 0 wins outright and entry 1 is never reached; under 50 entry 0 fails and
# falls through to entry 1, which always matches. The two lists are mutually
# exclusive - a player only ever rolls one of them.
#
# The >= 50 side is real data and the tree walker already surfaces it as the
# condition "Requires player level 50+". The UNDER-50 side is NOT in the data -
# the low list carries no cap of its own (LVLV Minimum Level is 1 on every entry
# in both lists, so the "Calculate from all levels <= player's level" flag gates
# nothing here). Its exclusivity is purely a consequence of the First Match
# fallthrough, so it is stamped deliberately below rather than read off a record.

CHASE_HIGH_LVL_LIST = "005D9CBB"
CHASE_LOW_LVL_LIST = "005D9CBD"
CHASE_LEVEL_SPLIT = 50


def stamp_level_lock(node):
    """Mark the two chase branches with the level band that can roll them."""
    if not isinstance(node, dict):
        return node
    fid = (node.get("formid") or "").upper()
    if fid == CHASE_HIGH_LVL_LIST:
        node["levelLock"] = {"min": CHASE_LEVEL_SPLIT, "max": None,
                             "label": "Level %d+ only" % CHASE_LEVEL_SPLIT}
        node["label"] = "Level %d+" % CHASE_LEVEL_SPLIT
        # the title already says it - don't repeat it as a drop condition
        node["conditions"] = [c for c in (node.get("conditions") or [])
                              if c != "Requires player level %d+" % CHASE_LEVEL_SPLIT]
    elif fid == CHASE_LOW_LVL_LIST:
        node["levelLock"] = {"min": None, "max": CHASE_LEVEL_SPLIT - 1,
                             "label": "Level %d and under only" % (CHASE_LEVEL_SPLIT - 1)}
        node["label"] = "Level %d and Under" % (CHASE_LEVEL_SPLIT - 1)
    for child in (node.get("children") or []):
        stamp_level_lock(child)
    return node


# ---------------------------------------------------------------------------
# GMRW -> tier assignment
# ---------------------------------------------------------------------------

def gmrw_rows_for_daily_ops():
    rows = read_tsv_rows("GMRW_Export_*.tsv")
    out = {}
    for r in rows:
        qlink = (r.get("ParentQuestLink") or "").split(":")[0]
        if qlink in QUESTS:
            out.setdefault(qlink, []).append(r)
    return out


def classify_row(row):
    """
    One GMRW reward row -> a reward slot.

    kind:
      rank           first clear of the day at a rank (GetValue == 0 + a timer GLOB)
      everyClear     no conditions at all -> every completion
      doubleMutation GetGlobalValue == 1 + Elder timer, no GetValue (every Elder clear)
      slasher        rewards an SDOW_ list (Pint-Sized Slasher event)
    Rows that reward no LVLI and no legendary item (XP / caps only) return None.
    """
    conds = (row.get("Conditions") or "").strip()
    ref = (row.get("RewardedItem") or "").strip()
    fid = edid = ""
    if ":LVLI" in ref:
        fid, edid = ref.split(":")[0], ref.split(":")[1]
    lgdi = (row.get("QRLI_LegendaryItemRewardList") or "").strip()
    if not fid and not lgdi:
        return None

    tier = None
    globs = row.get("ConditionGlobs") or ""
    for gfid, t in TIER_BY_GLOB.items():
        if gfid in globs:
            tier = t

    if edid.upper().startswith("SDOW_"):
        kind = "slasher"
    elif "GetGlobalValue" in conds:
        kind = "doubleMutation"
    elif not conds:
        kind = "everyClear"
    elif tier:
        kind = "rank"
    else:
        return None

    return {
        "kind": kind,
        "tier": tier,
        "formid": fid,
        "edid": edid,
        "lgdi": lgdi.split(":")[0] if lgdi else "",
        "lgdiRank": (row.get("QRLR_LegendaryItemRewardRank") or "").strip(),
        "index": (int(row.get("RewardIndex") or 0), int(row.get("RewardedItemIndex") or 0)),
    }


def collect_slots(rows):
    slots, seen = [], set()
    for row in sorted(rows, key=lambda r: (int(r.get("RewardIndex") or 0),
                                           int(r.get("RewardedItemIndex") or 0))):
        s = classify_row(row)
        if not s:
            continue
        key = (s["kind"], s["tier"], s["formid"], s["lgdi"], s["index"][0])
        if key in seen:
            continue
        seen.add(key)
        slots.append(s)
    return slots


def lvli_node(formid):
    node = ev.build_lvli_tree_node(formid)
    if not node:
        return None
    node = copy.deepcopy(node)
    node = stamp_list_self_chance(node)
    node = stamp_level_lock(node)
    node["label"] = POOL_TITLES.get(formid) or ev.prettify_lvli_label(node.get("edid") or "")
    return node


# ---------------------------------------------------------------------------
# GMRW legendary item (QRLI -> LGDI -> its ApplicableItemList LVLI)
# ---------------------------------------------------------------------------
# The GMRW's own legendary reward points at an LGDI record (e.g.
# LegendaryItems_Special_AllItems 005380CF). The LGDI's ApplicableItemList is an
# ordinary LVLI, so the breakdown (armour / melee / ranged / power armour, then
# set, piece and weight) comes from the same tree walker as everything else.

_LEG_TOP = {
    "Armor_All": "Armour",
    "Weapons_Melee_All": "Melee Weapons",
    "Weapons_Ranged_All": "Ranged Weapons",
    "PowerArmor_All": "Power Armour",
}
_ARMOUR_SET = {
    "Leather": "Leather", "Wood": "Wood", "EnclaveScoutUniform_Covert": "Covert Scout",
    "Botsmith": "Botsmith", "Muni": "Civil Engineer", "RaiderMod": "Raider",
    "Metal": "Metal", "Robot": "Robot", "Trapper": "Trapper", "Combat": "Combat",
    "SecretService": "Secret Service", "EnclaveScoutUniform_Any_Forest": "Forest Scout",
    "EnclaveScoutUniform_Any_Urban": "Urban Scout", "BosInfantry": "Brotherhood Recon",
    "Marine_Arctic": "Arctic Marine", "Marine": "Marine",
    "EnclaveScoutUniform_Solar": "Solar", "EnclaveScoutUniform_Thorn": "Thorn",
}
_PA_SET = {"T45": "T-45", "T51": "T-51b", "T60": "T-60", "T65": "T-65", "X01": "X-01"}
_PIECE = {"ArmLeft": "Left Arm", "ArmRight": "Right Arm", "LegLeft": "Left Leg",
          "LegRight": "Right Leg", "Torso": "Chest Piece"}


def _leg_label(edid):
    e = edid or ""
    for k, v in _LEG_TOP.items():
        if e.endswith("LegendaryItems_LL_" + k):
            return v
    m = re.match(r"^Legendary_LL_Armor_(.+?)_(ArmLeft|ArmRight|LegLeft|LegRight|Torso)$", e)
    if m:
        return _PIECE[m.group(2)]
    m = re.match(r"^Legendary_LL_Armor_(.+?)(?:_Any)?$", e)
    if m:
        key = m.group(1)
        for k in sorted(_ARMOUR_SET, key=len, reverse=True):
            if key == k or key == k + "_Any" or key.startswith(k):
                return _ARMOUR_SET[k] + " Armour"
    m = re.match(r"^Legendary_LL_PowerArmor_(.+)$", e)
    if m:
        return _PA_SET.get(m.group(1), m.group(1)) + " Power Armour"
    return None


def _flatten_single_item_children(node):
    """A piece list whose children are one-item weight lists (Light / Sturdy /
    Heavy) reads better as one table: fold each child's single item up, carrying
    the child's entryRate as the item's rate."""
    kids = node.get("children") or []
    if kids and all(not k.get("children") and len(k.get("items") or []) == 1 for k in kids):
        items = []
        for k in kids:
            it = dict(k["items"][0])
            own = 100.0 if k.get("entryRate") is None else float(k["entryRate"])
            it["dropRate"] = round(own * float(it.get("dropRate", 100.0)) / 100.0, 6)
            merged = list(it.get("conditions") or [])
            for c in k.get("conditions") or []:
                if c not in merged:
                    merged.append(c)
            if merged:
                it["conditions"] = merged
            items.append(it)
        node["items"] = (node.get("items") or []) + items
        node["children"] = []
    for k in node.get("children") or []:
        _flatten_single_item_children(k)


def _relabel_legendary(node):
    lbl = _leg_label(node.get("edid"))
    if lbl:
        node["label"] = lbl
    for k in node.get("children") or []:
        _relabel_legendary(k)


def _dedupe_node_conditions(node):
    """The tree walker also rolls item conditions up onto their list. When every
    one of a list's conditions is already shown under the item it belongs to,
    drop the rolled-up copy so each condition appears once, under its item.
    List-only conditions (a toggle on the list itself) stay on the list."""
    conds = node.get("conditions") or []
    if conds and node.get("items"):
        on_items = {c for it in node["items"] for c in (it.get("conditions") or [])}
        if all(c in on_items for c in conds):
            node.pop("conditions", None)
    for k in node.get("children") or []:
        _dedupe_node_conditions(k)


def _stamp_min_level(node):
    """A MinLvl_* GLOB sitting in an entry's ChanceNone slot is a minimum player
    level, not a chance of nothing (drop-rate-engine S5). The walker correctly
    treats it as 100%, but the level gate itself is lost - put it back as a
    drop condition on the child it gates, in the site's standard wording."""
    fid = (node.get("formid") or "").upper()
    kids = {(k.get("formid") or "").upper(): k for k in node.get("children") or []}
    for it in node.get("items") or []:
        kids.setdefault((it.get("formid") or "").upper(), it)
    for e in ev.lvli_entries_by_list.get(fid, []):
        ref = (e.get("LVLO_Reference") or "").split(":")[0].upper()
        glob = (e.get("LVOG_ChanceNoneGlobal") or "")
        if ref in kids and "MinLvl" in glob:
            gfid = glob.split(":")[0].upper()
            lvl = ev.glob_vals.get(gfid)
            if lvl and float(lvl) > 1:        # level 1 gates nothing
                text = "Requires player level %d+" % int(float(lvl))
                conds = kids[ref].setdefault("conditions", [])
                if text not in conds:
                    conds.append(text)
    for k in node.get("children") or []:
        _stamp_min_level(k)


_LGDI_CACHE = None


def _lgdi_item_list(lgdi_fid):
    global _LGDI_CACHE
    if _LGDI_CACHE is None:
        _LGDI_CACHE = {(r.get("LGDI_FormID") or "").strip().upper():
                       (r.get("ApplicableItemList_FormID") or "").strip().upper()
                       for r in read_tsv_rows("LGDI_Export_*.tsv")}
    return _LGDI_CACHE.get((lgdi_fid or "").upper(), "")


def legendary_node(slot):
    """The GMRW's own legendary-item reward (QRLI + QRLR), broken down by type."""
    rank = slot.get("lgdiRank") or ""
    name = ("%s★ Legendary Item" % rank) if rank else "Legendary Item"
    list_fid = _lgdi_item_list(slot["lgdi"])
    tree = ev.build_lvli_tree_node(list_fid) if list_fid else None
    if not tree:
        return {
            "type": "gmrwLegendary", "formid": slot["lgdi"], "label": name, "useAll": False,
            "items": [{"formid": slot["lgdi"], "name": name, "qty": 1, "dropRate": 100.0}],
        }
    tree = copy.deepcopy(tree)
    _relabel_legendary(tree)
    _dedupe_node_conditions(tree)
    _stamp_min_level(tree)
    _flatten_single_item_children(tree)
    tree["type"] = "gmrwLegendary"
    tree["lgdi"] = slot["lgdi"]
    tree["label"] = name
    tree.pop("entryRate", None)
    tree["note"] = (
        "One item, picked evenly from armour, melee weapons, ranged weapons and "
        "power armour, then down to a single piece. Anything with a drop "
        "condition you don't meet is skipped, which pushes the odds up for the rest."
    )
    return tree


# ---------------------------------------------------------------------------
# Possible legendary effects (reference list, not a reward)
# ---------------------------------------------------------------------------
# Generative from the LGDI mods export: each GMRW legendary reward's LGDI names
# one modcol per type and star (modcol_Legendary_Crafting_Armor1 ...Weapon3), and
# ModPool_Refs lists every mod_Legendary_* in that collection. Names and effect
# text come from the OMOD export. Nothing here is hand-listed, so a new effect
# added by a patch shows up on the next build.

_EFFECT_TYPES = [("Armor", "Armour"), ("PowerArmor", "Power Armour"),
                 ("WeaponMelee", "Melee Weapons"), ("WeaponRanged", "Ranged Weapons")]


def build_legendary_effects(lgdi_fids):
    lgdi_fids = {f.upper() for f in lgdi_fids if f}
    if not lgdi_fids:
        return None
    omod = {}
    for r in read_tsv_rows("OMOD_Export_*.tsv"):
        e = (r.get("OMOD_EDID") or "").strip()
        if e.startswith("mod_Legendary_"):
            omod[e] = ((r.get("FULL") or "").strip(), (r.get("DESC") or "").strip())

    # {type_key: {star: {name: desc}}}
    pools = {}
    for r in read_tsv_rows("LGDI_Export_*_Mods.tsv"):
        if (r.get("LGDI_FormID") or "").strip().upper() not in lgdi_fids:
            continue
        m = re.match(r"^modcol_Legendary_\w*?(Armor|PowerArmor|Weapon)(\d)$",
                     (r.get("OMOD_EDID") or "").strip())
        if not m:
            continue
        kind, star = m.group(1), int(m.group(2))
        for edid in re.findall(r"(mod_Legendary_\w+) \[OMOD:", r.get("ModPool_Refs") or ""):
            name, desc = omod.get(edid, ("", ""))
            if not name:
                continue
            if kind == "Weapon":
                keys = (["WeaponRanged"] if "_Guns_" in edid else
                        ["WeaponMelee"] if "_Melee_" in edid else
                        ["WeaponMelee", "WeaponRanged"])
            else:
                keys = [kind]
            for k in keys:
                pools.setdefault(k, {}).setdefault(star, {}).setdefault(name, desc)

    children = []
    for key, label in _EFFECT_TYPES:
        stars = pools.get(key) or {}
        if not stars:
            continue
        star_nodes = []
        for star in sorted(stars):
            effects = sorted(stars[star].items(), key=lambda kv: kv[0].lower())
            star_nodes.append({
                "type": "effectList",
                "label": "%d★ Effects" % star,
                "star": star,
                "listOnly": True,
                "items": [{"name": n, "desc": d} for n, d in effects],
            })
        children.append({"type": "effectGroup", "label": label, "listOnly": True,
                         "children": star_nodes})
    if not children:
        return None
    max_star = max(max(s) for s in pools.values())
    return {
        "type": "tier",
        "tier": "legendaryEffects",
        "isReference": True,
        "label": "Possible Legendary Effects",
        "subtitle": "Reference list · the effects a Daily Ops legendary can roll, by star",
        "note": ("Not an extra reward. A %d★ item gets one effect from each star "
                 "tier up to %d★. Listed A-Z." % (max_star, max_star)),
        "timeLabel": None,
        "seconds": None,
        "children": children,
    }


# ---------------------------------------------------------------------------
# Plain-English labels and conditions
# ---------------------------------------------------------------------------
# The tree walker names lists from their EDIDs ("Systemic Rewards Currency
# Caps"). Players don't care what the list is called inside the game, only what
# drops, so these rules rename lists to what they give. Rules, not a hand list
# of every node: an unknown EDID falls through to the walker's own label.

_PRETTY_EDID = {
    "LLS_Systemic_Rewards_Currency_Caps": "Caps",
    "LLS_Systemic_Rewards_Currency_Scrip": "Scrip",
    "LL_Gold_Treasury_Note_02": "Treasury Notes",
    "LLS_DailyOps_Rewards_Currency": "Currency",
    "LLS_DailyOps_Rewards_Aid": "Aid",
    "LL_Stimpak_1_3": "Stimpaks",
    "LL_RadAway_1_3": "RadAway",
    "LLS_Loot_Weapon_Grenades_All": "Grenade",
    "LLS_DailyOps_Contextual_AmmoType_Quest_Repeatable": "Ammo for Your Equipped Weapon",
    "LLS_DailyOps_Contextual_AmmoType_Fallback": "Fallback Ammo",
    "LL_DailyOps_Rewards_HighLVL_Chase_Rare": "Rare",
    "LL_DailyOps_Rewards_HighLVL_Chase_RareUntradable": "Rare (Untradable)",
    "LL_DailyOps_Rewards_HighLVL_Chase_Uncommon": "Uncommon",
    "LL_DailyOps_Rewards_LowLVL_Chase_Rare": "Rare",
    "LL_DailyOps_Rewards_LowLVL_Chase_Uncommon": "Uncommon",
    "LLS_DailyOps_Rewards_Recipes_Rare": "Rare Recipes",
    "LLS_DailyOps_Rewards_Recipes_UnCommon": "Uncommon Recipes",
    "RESTRICTED_LL_LegendaryModule_Single": "Legendary Module",
    "RESTRICTED_LL_LegendaryModule_1-3": "Legendary Modules",
    "SDOW_DailyOps_LL_Rewards_Rare": "Slasher Rewards",
}
_AMMO = {
    "10mm": "10mm", "2mmEC": "2mm EC", "308Caliber": ".308", "38Caliber": ".38",
    "44": ".44", "45Caliber": ".45", "50Cal": ".50 Cal", "50CaliberBall": ".50 Ball",
    "556": "5.56", "5mm": "5mm", "AlienBlaster": "Alien Blaster Rounds",
    "Arrow_Broadhead": "Broadhead Arrows", "Cannonball": "Cannonballs",
    "CrossbowBolt": "Crossbow Bolts", "CryoCell": "Cryo Cells", "FlamerFuel": "Flamer Fuel",
    "FusionCell": "Fusion Cells", "FusionCore": "Fusion Cores", "GammaCell": "Gamma Rounds",
    "GrenadeLauncher": "40mm Grenades", "Harpoon": "Harpoons", "Melee_StimpakDiluted": "Diluted Stimpaks (melee)",
    "MiniNuke": "Mini Nukes", "Missile": "Missiles", "PlasmaCartridge": "Plasma Cartridges",
    "PlasmaCore": "Plasma Cores", "RRSpike": "Railway Spikes", "ShotgunShell": "Shotgun Shells",
}
_GRENADE = {"Molotov": "Molotov Cocktails"}
_UNIQUE_FIX = {"Mechanics Best Friend": "Mechanic's Best Friend",
               "Whistle In The Dark": "Whistle in the Dark"}


def _camel_words(s):
    return re.sub(r"(?<=[a-z])(?=[A-Z])|_", " ", s).strip()


def _pretty_label(node):
    e = node.get("edid") or ""
    if e in _PRETTY_EDID:
        return _PRETTY_EDID[e]
    m = re.match(r"^LLS_Contextual_Ammo_(.+?)(_AntiScorchBeast)?(_Guaranteed)?$", e)
    if m:
        base = _AMMO.get(m.group(1), _camel_words(m.group(1)))
        if base[0] in ".0123456789" and "Rounds" not in base and "Grenades" not in base:
            base += " Ammo"
        return base + (" (Anti-Scorchbeast)" if m.group(2) else "")
    m = re.match(r"^LLS_Grenades_(.+)$", e)
    if m:
        return _GRENADE.get(m.group(1), _camel_words(m.group(1)) + " Grenades")
    m = re.match(r"^(?:DailyOps_)?LL_Weapon_(?:Melee|Ranged)_(?:[A-Za-z0-9]+?_)?([A-Z][A-Za-z]+)$", e)
    if m:
        uniq = _camel_words(m.group(1))
        return _UNIQUE_FIX.get(uniq, uniq)
    return None


_PRETTY_COND = {
    "Toggle: Daily Ops Mutation Mode Index": "Double mutation ops only",
}


def prettify_tree(node, _in_legendary=False):
    if node.get("type") in ("gmrwLegendary", "effectGroup", "effectList", "tier"):
        _in_legendary = _in_legendary or node.get("type") != "tier"
    if not _in_legendary:
        lbl = _pretty_label(node)
        if lbl:
            node["label"] = lbl
            # Named unique weapons: the item inside is just the base weapon
            # (".44", "Pipe Wrench") - carry the unique name onto it.
            # Contextual ammo items are raw EDID-ish names ("Ammo10mm") that
            # only differ by quantity - name them after their list.
            if (node.get("edid") or "").startswith("LLS_Contextual_Ammo_"):
                for it in node.get("items") or []:
                    it["name"] = lbl
            if re.search(r"LL_Weapon_(Melee|Ranged)_", node.get("edid") or ""):
                for it in node.get("items") or []:
                    if lbl.lower() not in (it.get("name") or "").lower():
                        it["name"] = "%s (%s)" % (lbl, it.get("name"))
    for it in node.get("items") or []:
        nm = it.get("name") or ""
        if nm[:1].islower():                    # "frag Grenade" -> "Frag Grenade"
            it["name"] = nm[:1].upper() + nm[1:]
    if node.get("conditions"):
        node["conditions"] = [_PRETTY_COND.get(c, c) for c in node["conditions"]]
    for k in node.get("children") or []:
        prettify_tree(k, _in_legendary)


def _stamp_value_conditions(node):
    """Entry conditions the tree walker drops: GetValue on the Daily Ops
    'reward received today' actor values. == 1 means you have already had that
    rank's reward today (a repeat clear); == 0 means you haven't."""
    fid = (node.get("formid") or "").upper()
    kids = node.get("children") or []
    entries = ev.lvli_entries_by_list.get(fid, [])
    if kids and len(entries) == len(kids):
        for e, k in zip(sorted(entries, key=lambda r: int(r.get("EntryIndex") or 0)), kids):
            blob = " ".join(str(e.get("Cond%d" % i) or "") for i in range(1, 11))
            m = re.search(r"DailyOpsRewardReceivedTier(High|Medium|Low).*?\) \d+ ([01])\.0", blob)
            if m:
                rank = {"High": "Elder", "Medium": "Paladin", "Low": "Knight"}[m.group(1)]
                text = ("Repeat %s clears only - after today's first %s reward" % (rank, rank)
                        if m.group(2) == "1" else "First %s clear of the day only" % rank)
                conds = k.setdefault("conditions", [])
                if text not in conds:
                    conds.append(text)
                # the "rolls twice" note is wrong once one copy is conditional
                node.pop("duplicateRollNote", None)
    for k in kids:
        _stamp_value_conditions(k)


def split_double_mutation_currency(roots):
    """The Elder Bonus Currency list rolls currency once, plus a second time on
    double mutation ops. Move that second roll into the Double Mutation root so
    everything double-mutation lives in one place, and relabel both halves."""
    by = {r["tier"]: r for r in roots}
    elder, dm = by.get("elder"), by.get("doubleMutation")
    if not elder:
        return
    for node in elder["children"]:
        if node.get("edid") != "LL_DailyOps_Rewards_AdditionalCurrency_Tier03":
            continue
        kids = node.get("children") or []
        dm_kids = [k for k in kids if "Double mutation ops only" in (k.get("conditions") or [])]
        base = [k for k in kids if k not in dm_kids]
        if not dm_kids or not dm or len(base) != 1:
            return
        # Elder keeps one plain currency roll, shown as "Bonus Currency".
        keep = base[0]
        keep["label"] = "Currency"
        keep.pop("entryRate", None)
        elder["children"][elder["children"].index(node)] = keep
        # Double Mutation gets the extra roll, first.
        for k in dm_kids:
            k["label"] = "Extra Currency"
            k["conditions"] = ["First Elder clear of the day only"]
            k.pop("entryRate", None)
        dm["children"] = dm_kids + dm["children"]
        return


# ---------------------------------------------------------------------------
# Experience (XP) - flat NAM7 XP on the GMRW reward rows
# ---------------------------------------------------------------------------
# Daily Ops XP is flat (NAM7 GLOBs, no XPCT curve), so it does not scale with
# player level. Each GMRW reward index pays its XP once when its conditions
# pass, so the rows below stack the same way the rank rewards do.

def build_xp_root(rows):
    glob = {}
    for r in read_tsv_rows("GLOB_Export_*.tsv"):
        glob[(r.get("FormID") or "").strip().upper()] = r.get("FLTV")
    seen, out = set(), []
    for row in sorted(rows, key=lambda r: int(r.get("RewardIndex") or 0)):
        idx = int(row.get("RewardIndex") or 0)
        ref = (row.get("NAM7_XPGlobal") or "").strip()
        if not ref or idx in seen:
            continue
        seen.add(idx)
        try:
            amount = int(round(float(glob.get(ref.split(":")[0].upper()) or 0)))
        except ValueError:
            amount = 0
        if amount <= 0:
            continue
        conds = row.get("Conditions") or ""
        globs = row.get("ConditionGlobs") or ""
        tier = next((t for g, t in TIER_BY_GLOB.items() if g in globs), None)
        rank = TIER_META[tier]["label"] if tier else None
        first = "GetValue 10000000 0.000000" in conds
        secs = TIER_META[tier]["timeLabel"] if tier else None
        # Labels never name a rank: the 300 is the base payout for any clear
        # fast enough, the 500 is the Elder-speed bonus on top.
        is_bonus = tier == "elder"
        if "GetGlobalValue" in conds:
            label = "Double Mutation Bonus XP" if is_bonus else "Double Mutation Base XP"
            note = ("Every clear in %s or less" % secs) if not first else \
                   ("First clear of the day in %s or less" % secs)
        elif rank:
            label = "Bonus Elder XP" if is_bonus else "Base XP"
            note = "First clear of the day in %s or less" % secs
        else:
            label, note = "Completion XP", "Every clear"
        out.append({"label": label, "amount": amount, "note": note,
                    "kind": "doubleMutation" if "GetGlobalValue" in conds else ("rank" if rank else "everyClear"),
                    "tier": tier,
                    "order": (1 if "GetGlobalValue" in conds else 0,
                              0 if not is_bonus else 1)})
    out.sort(key=lambda x: x["order"])
    return out


def _xp_node(rows):
    return {
        "type": "xp",
        "label": "Experience (XP)",
        "subtitle": "Awarded on completion · does not scale with player level",
        "xpRows": [{"label": r["label"], "amount": r["amount"], "note": r["note"]} for r in rows],
        "xpNote": "XP does not scale with player level. XP buffs still apply.",
    }


_NO_XP_NOTE = {
    "knight": "No completion XP for a Knight clear - the game's reward record for this rank has none. You still get XP from kills.",
    "everyClear": "This pool gives no completion XP of its own. Any XP comes from your rank and from kills.",
    "slasher": "No extra XP from the Slasher rewards. Any XP comes from your rank and from kills.",
}


def add_xp_to_roots(roots, xp_rows):
    """Put each root's XP in its own Experience (XP) sub-expand, first in the
    root. Rank XP stacks the same way the rank rewards do (an Elder clear also
    passes the Paladin XP check)."""
    for r in roots:
        key = r.get("tier")
        if key in TIER_META:
            mine = [x for x in xp_rows if x["kind"] == "rank"
                    and TIER_META[x["tier"]]["order"] >= TIER_META[key]["order"]]
        else:
            mine = [x for x in xp_rows if x["kind"] == key]
        if mine:
            node = _xp_node(mine)
            if key == "elder" and len(mine) > 1:
                # make the stacking explicit: an Elder clear also passes the
                # Paladin XP check, so both rows pay
                total = sum(x["amount"] for x in mine)
                node["xpNote"] = ("An Elder clear gets both: %d XP in total. "
                                  "XP does not scale with player level. XP buffs still apply." % total)
            r["children"].insert(0, node)
        elif key in _NO_XP_NOTE:
            node = _xp_node([])
            node["xpRows"] = [{"label": "Completion XP", "amount": 0, "note": ""}]
            node["xpNote"] = _NO_XP_NOTE[key]
            r["children"].insert(0, node)


# ---------------------------------------------------------------------------
# Plans & Apparel checklist (the bottom section of the All Rewards page)
# ---------------------------------------------------------------------------
# The Elder, Paladin and Knight reward lists all roll the SAME Level 50+ list -
# only the gate differs (100% / 10% / 5%) - so the checklist lists each plan or
# apparel piece ONCE, with its rate at each rank. Level 49 and Under is left
# out on purpose (generic crafting recipes nobody tracks). Slasher Daily Ops
# items sit in their own group at the bottom, split off by a solid line.
#
# NEW pill (season rule, same as collectables): an item is NEW when it first
# appeared in the Daily Ops lists in an export dated in the CURRENT season -
# fallout76_seasons.tsv, latest season whose start is on or before today. Read
# straight from the dated LVLI exports, so there is no persistence file to keep.

_CHECK_SIGS = ("BOOK", "ARMO")
_PLAN_MASTER = _REPO_ROOT / "dist" / "plan_master.json"
_SEASONS_TSV = _REPO_ROOT / "tsv" / "fallout76_seasons.tsv"


def _current_season():
    import datetime as _dt
    today = _dt.date.today()
    best = None
    try:
        with open(_SEASONS_TSV, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", (r.get("StartDate") or "").strip())
                if not m:
                    continue
                start = _dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                if start <= today and (best is None or start > best[0]):
                    best = (start, (r.get("SeasonName") or "").strip(),
                            (r.get("SeasonNumber") or "").strip())
    except OSError:
        return None
    return best


def _first_seen_months(list_fids, item_fids):
    """{item formid: first export date it appears in any of list_fids}."""
    list_fids = {f.upper() for f in list_fids}
    want = {f.upper() for f in item_fids}
    seen = {}
    csv.field_size_limit(10 ** 9)
    for path in tsv_source.all_matching("LVLI_Export_*_LVLI_Entries.tsv"):
        d = tsv_source.export_date(path)
        try:
            with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    if (r.get("LVLI_FormID") or "").upper() not in list_fids:
                        continue
                    ref = (r.get("LVLO_Reference") or "").split(":")[0].upper()
                    if ref in want and ref not in seen:
                        seen[ref] = d
        except OSError:
            continue
    return seen


def _plan_master_index():
    try:
        with open(_PLAN_MASTER, encoding="utf-8") as f:
            items = json.load(f).get("items") or []
    except (OSError, ValueError):
        return {}
    out = {}
    for it in items:
        fid = (it.get("id") or "").split("_")[-1].upper()
        if fid:
            out.setdefault(fid, it)
    return out


def _walk_items(node, rate, lists, out, path):
    own = 100.0 if node.get("entryRate") is None else float(node["entryRate"])
    r = rate * own / 100.0
    fid = (node.get("formid") or "").upper()
    if fid:
        lists.add(fid)
    for it in node.get("items") or []:
        if it.get("sig") not in _CHECK_SIGS:
            continue
        ifid = (it.get("formid") or "").upper()
        dr = 100.0 if it.get("dropRate") is None else float(it["dropRate"])
        rec = out.setdefault(ifid, {"item": it, "rate": 0.0, "lists": []})
        rec["rate"] += r * dr / 100.0
        if node.get("label") and node["label"] not in rec["lists"]:
            rec["lists"].append(node["label"])
    for k in node.get("children") or []:
        _walk_items(k, r, lists, out, path + [k.get("label")])


def build_checklist_section(reward_tree, tradeable):
    by = {r.get("tier"): r for r in reward_tree}

    def rank_node(root_key, label):
        root = by.get(root_key) or {}
        for c in root.get("children") or []:
            if c.get("label") == label:
                for k in c.get("children") or []:
                    if (k.get("formid") or "").upper() == CHASE_HIGH_LVL_LIST:
                        return c, k
        return None, None

    per_rank, list_fids = {}, set()
    main_items = {}
    for key, label in (("elder", "Elder Rewards"), ("paladin", "Paladin Rewards"),
                       ("knight", "Knight Rewards")):
        parent, hi = rank_node(key, label)
        if not hi:
            continue
        gate = 100.0 if parent.get("entryRate") is None else float(parent["entryRate"])
        found = {}
        _walk_items(hi, gate, list_fids, found, [])
        per_rank[key] = found
        for fid, rec in found.items():
            main_items.setdefault(fid, rec)

    slasher_items, slasher_rates = {}, {}
    sl = by.get("slasher")
    for c in (sl or {}).get("children") or []:
        if c.get("type") == "xp":
            continue
        found = {}
        _walk_items(c, 100.0, list_fids, found, [])
        slasher_rates[c.get("label")] = found
        for fid, rec in found.items():
            slasher_items.setdefault(fid, rec)

    season = _current_season()
    first = _first_seen_months(list_fids, list(main_items) + list(slasher_items))
    pm = _plan_master_index()

    def make(fid, rec, rates):
        it = rec["item"]
        name = it.get("name") or fid
        sig = it.get("sig")
        kind = ("Apparel" if sig == "ARMO" else
                "Title" if name.startswith(("Player Title", "Camp Title")) else
                "Recipe" if name.startswith("Recipe:") else "Plan")
        conds = it.get("conditions") or []
        p = pm.get(fid) or {}
        if sig == "ARMO":
            images, image_dir = [(it.get("edid") or "").lower()], "apparel"
        else:
            images, image_dir = list(p.get("images") or []), p.get("image_dir") or p.get("type") or ""
        fs = first.get(fid)
        is_new = bool(season and fs and (fs.year, fs.month) >= (season[0].year, season[0].month))
        return {
            "id": fid,
            "name": name,
            "kind": kind,
            "edid": it.get("edid") or "",
            # plan_master is the plan checklists' source of truth for trade;
            # the keyword scan only fills in what it doesn't cover (apparel).
            "tradeable": p["tradeable"] if isinstance(p.get("tradeable"), bool) else tradeable.get(fid),
            "stopsDropping": any("already learned" in c for c in conds),
            "conditions": conds,
            "images": [i for i in images if i],
            "image_dir": image_dir,
            "rates": rates,
            "lists": rec["lists"],
            "firstSeen": fs.strftime("%b %Y") if fs else "",
            "isNew": is_new,
        }

    def rank_rates(fid):
        out = []
        for key in ("elder", "paladin", "knight"):
            rec = (per_rank.get(key) or {}).get(fid)
            if rec:
                out.append({"label": TIER_META[key]["label"] + " clear", "rate": round(rec["rate"], 6)})
        return out

    def slasher_rate(fid):
        out = []
        for lbl, found in slasher_rates.items():
            if fid in found:
                out.append({"label": lbl, "rate": round(found[fid]["rate"], 6)})
        return out

    # A-Z on the item itself, not the "Plan:" / "Recipe:" / "Player Title:"
    # prefix, so apparel and plans interleave the way a reader looks for them.
    def az(x):
        return re.sub(r"^(Plan|Recipe|Player Title|Camp Title):\s*", "", x["name"]).lower()

    main = sorted((make(f, r, rank_rates(f)) for f, r in main_items.items()), key=az)
    slash = sorted((make(f, r, slasher_rate(f)) for f, r in slasher_items.items()), key=az)
    groups = [{"id": "dailyops", "label": "Daily Ops Plans & Apparel",
               "sub": "Level 50+ reward list · the same list at every rank", "items": main}]
    if slash:
        groups.append({"id": "slasher", "label": "Slasher Daily Ops",
                       "sub": "Pint-Sized Slasher event only", "divider": True, "items": slash})
    return {
        "title": "Plans & Apparel Checklist",
        "season": {"name": season[1], "number": season[2],
                   "start": season[0].isoformat()} if season else None,
        "groups": groups,
        "count": sum(len(g["items"]) for g in groups),
    }


def slot_nodes(slot):
    out = []
    if slot["formid"]:
        n = lvli_node(slot["formid"])
        if n:
            out.append(n)
    if slot["lgdi"]:
        out.append(legendary_node(slot))
    return out


def build_roots(slots):
    roots = []
    for key in ROOT_ORDER:
        meta = ROOT_META[key]
        if key in TIER_META:
            # Cumulative: this rank plus every slower rank, own rank first.
            my_order = TIER_META[key]["order"]
            picked = [s for s in slots if s["kind"] == "rank"
                      and TIER_META[s["tier"]]["order"] >= my_order]
            picked.sort(key=lambda s: (TIER_META[s["tier"]]["order"], s["index"]))
            time_label = TIER_META[key]["timeLabel"]
            seconds = TIER_META[key]["seconds"]
        else:
            picked = [s for s in slots if s["kind"] == key]
            tiers = [s["tier"] for s in picked if s["tier"]]
            time_label = TIER_META[tiers[0]]["timeLabel"] if key == "doubleMutation" and tiers else None
            seconds = TIER_META[tiers[0]]["seconds"] if key == "doubleMutation" and tiers else None

        children, seen_lgdi = [], set()
        for s in picked:
            for n in slot_nodes(s):
                if n.get("type") == "gmrwLegendary":
                    lkey = n.get("lgdi") or n.get("formid")
                    if lkey in seen_lgdi:
                        continue
                    seen_lgdi.add(lkey)
                children.append(n)
        if not children:
            continue
        roots.append({
            "type": "tier",
            "tier": key,
            "label": meta["label"],
            "subtitle": meta["subtitle"],
            "note": meta.get("note"),
            "timeLabel": time_label,
            "seconds": seconds,
            "isTierWrapper": True,
            "children": children,
        })
    return roots


# ---------------------------------------------------------------------------
# Locations (Bethesda's own FormList - never inferred from names)
# ---------------------------------------------------------------------------

def build_locations():
    flst = read_tsv_rows("FLST_Export_*_List.tsv")
    by_fid = {r.get("FLST_FormID"): r for r in flst}

    def entries(fid):
        row = by_fid.get(fid)
        if not row:
            return []
        n = int(row.get("EntryCount") or 0)
        out = []
        for i in range(1, n + 1):
            v = (row.get("Entry_%d" % i) or "").strip()
            if v:
                out.append(v.split(":")[0])
        return out

    master = entries(DAILY_OPS_LOCATION_FLST)
    large = set(entries(DAILY_OPS_LOCATION_LARGE_FLST))
    small = set(entries(DAILY_OPS_LOCATION_SMALL_FLST))

    lctn = {r.get("LCTN_FormID"): (r.get("LCTN_FULL") or "").strip()
            for r in read_tsv_rows("LCTN_Export_*_LCTN.tsv")}

    out = []
    for fid in master:
        name = lctn.get(fid, "")
        if not name:
            continue
        s = slugify(name)
        out.append({
            "formid": fid,
            "name": name,
            "slug": s,
            "size": "Large" if fid in large else ("Small" if fid in small else ""),
            "map": LOCATION_IMAGE_BASE + s + "-map.avif",
        })
    out.sort(key=lambda x: x["name"])
    return out


# ---------------------------------------------------------------------------
# Checklist pools (folded in from the retired build_daily_ops_json.py)
# ---------------------------------------------------------------------------
# The Daily Ops Reward Checklist page and the Daily Ops Guide page
# (df-bnb-daily-ops.js) read a flat `pools` list with per-item dropRate /
# isPlan / tradeable. They get it from their OWN file,
# daily_ops_reward_checklist.json, so a problem with the All Rewards tree can
# never blank the checklist and vice versa.
#
# These pools are built straight from the GMRW + LVLI exports. They used to be
# lifted out of dist/events/events_rewards.json, which meant a bad events build
# took Daily Ops down with it. The maths is the events builder's own
# (compute_lvli -> rng76, same normalisation, same cond_mult), so the numbers
# are identical - verified field-for-field against the old file on 26 Sept 2026.

CHECKLIST_CONFIG = {
    "name": "Daily Ops",
    "questFormID": "005A77D4",
    "pageType": "dailyops",
    "timerGlobs": {
        "elder": ("005CB976", 480),
        "paladin": ("005CB977", 720),
        "knight": ("005CB978", 960),
    },
}


def _is_non_tradeable(row):
    blob = " ".join(str(v) for v in row.values() if v).lower()
    return ("nonplayertradeable" in blob or "nonplayertradable" in blob
            or "unsellableobject" in blob)


def build_tradeable_map():
    """FormID -> tradeable, from the newest BOOK (plans) then ARMO (apparel) export."""
    tradeable = {}
    for row in read_tsv_rows("BOOK_Export_*.tsv"):
        fid = (row.get("FormID") or "").strip().upper()
        if fid:
            tradeable[fid] = not _is_non_tradeable(row)
    for row in read_tsv_rows("ARMO_Export_*.tsv"):
        fid = (row.get("FormID") or row.get("ARMO_FormID") or "").strip().upper()
        if fid and fid not in tradeable:
            tradeable[fid] = not _is_non_tradeable(row)
    return tradeable


def _raw_pool(row):
    """One GMRW LVLI row -> the raw pool dict events_page_pools.process_pool expects."""
    rewarded = (row.get("RewardedItem") or "").strip()
    if ":LVLI" not in rewarded:
        return None
    formid = rewarded.split(":")[0]
    conds = ev.merge_conditions(row.get("Conditions"), row.get("TierConditionFunc"),
                                row.get("ConditionGlobs"))
    tier_func = (row.get("TierConditionFunc") or "").strip()
    tier_val = (row.get("TierConditionValue") or "").strip()
    if tier_func.lower() == "getrandompercent" and tier_val:
        cond_mult = max(0.0, min(1.0, float(tier_val) / 100.0))
        synth = "GetRandomPercent <= %.6f" % float(tier_val)
        if synth not in conds:
            conds = list(conds) + [synth]
    else:
        raw = " | ".join(c for c in [row.get("Conditions"), row.get("ConditionGlobs")]
                         if (c or "").strip())
        cond_mult = parse_randompercent_multiplier(raw)

    lvli_edid = ev.lvli_edid_by_formid.get(formid, "")
    label = ev.prettify_lvli_label(lvli_edid) or ev.prettify_lvli_label(rewarded.replace(":", "_"))

    probs = ev.compute_lvli(formid)
    total = sum(probs.values())
    if total > 0 and abs(total - 1.0) > 0.0001:
        probs = {k: v / total for k, v in probs.items()}
    items = []
    for fid, ch in probs.items():
        nm = ev.resolve_name_for_formid(fid)
        items.append({
            "formid": fid,
            "name": nm,
            "dropRate": ev.pct(ch * cond_mult),
            "qty": 1,
            "isPlan": bool(nm) and nm.startswith(("Plan:", "Recipe:")),
        })
    items.sort(key=lambda x: (x["name"] or "", x["formid"] or ""))
    return {
        "title": label or "Reward Pool", "lvliFormID": formid, "lvliEdid": lvli_edid,
        "conditions": conds, "poolChance": ev.pct(cond_mult), "items": items,
    }


def build_checklist_pools(rows):
    pools, seen = [], set()
    for row in sorted(rows, key=lambda r: (int(r.get("RewardIndex") or 0),
                                           int(r.get("RewardedItemIndex") or 0))):
        raw = _raw_pool(row)
        if not raw:
            continue
        key = (raw["lvliFormID"], row.get("RewardIndex") or "")
        if key in seen:
            continue
        seen.add(key)
        pools.append(process_pool(raw, CHECKLIST_CONFIG))
    pools = merge_duplicate_pools(pools)
    pools.sort(key=lambda p: p.get("title") or "")   # same order the old file had

    tmap = build_tradeable_map()
    for pool in pools:
        for item in pool.get("items", []):
            fid = (item.get("formid") or "").strip().upper()
            if fid in tmap:
                item["tradeable"] = tmap[fid]
    return pools


def _checklist_items(data):
    items = []
    for pool in (data or {}).get("pools", []):
        items.extend(pool.get("items", []))
    return items


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build():
    per_quest = gmrw_rows_for_daily_ops()
    if not per_quest:
        raise SystemExit("No Daily Ops GMRW rows found - check the GMRW export.")

    # Uplink and Decryption were verified identical (every reward entry, every
    # non-identity column). Build from Uplink and say so on the page; if they ever
    # diverge the check below fails loudly rather than silently showing one.
    def sig(rows):
        skip = {"FormID", "EDID", "ParentQuestDisplay", "ParentQuestLink", "ReferencedByCount"}
        return sorted(
            tuple(sorted((k, (v or "").strip()) for k, v in r.items()
                         if k not in skip and not k.startswith("Ref") and (v or "").strip()))
            for r in rows
        )

    quest_ids = list(per_quest.keys())
    identical = len(quest_ids) > 1 and all(
        sig(per_quest[quest_ids[0]]) == sig(per_quest[q]) for q in quest_ids[1:]
    )
    if len(quest_ids) > 1 and not identical:
        raise SystemExit(
            "Uplink and Decryption reward records DIVERGED. The page assumes they "
            "are identical - rebuild the tree per op type before shipping."
        )

    rows = per_quest.get("005A77D4") or per_quest[quest_ids[0]]
    timer_tiers = {
        k: {"seconds": v["seconds"], "label": "%s (≤ %s)" % (v["label"], v["timeLabel"])}
        for k, v in TIER_META.items()
    }
    return rows, quest_ids, identical, timer_tiers


def build_all_rewards(rows, quest_ids, identical, timer_tiers):
    """Page 1: /df/daily-ops/daily-ops-all-rewards/ (df-bnb-daily-ops-rewards.js)."""
    slots = collect_slots(rows)
    data = {
        "slug": "daily-ops-all-rewards",
        "pageType": "dailyops",
        "name": "Daily Ops",
        "description": DESCRIPTION,
        "infoCard": INFO_CARD,
        "opTypes": [QUESTS[q] for q in quest_ids],
        "opRewardsIdentical": bool(identical),
        "meta": {"timerTiers": timer_tiers},
        "locations": build_locations(),
        "gallery": [],
        "rewardTree": build_roots(slots),
        "warnings": [],
    }
    for r in data["rewardTree"]:
        for c in r["children"]:
            _stamp_value_conditions(c)
        prettify_tree(r)
    split_double_mutation_currency(data["rewardTree"])
    add_xp_to_roots(data["rewardTree"], build_xp_root(rows))
    # Page order inside each rank root: XP, then the guaranteed extras, then the
    # rank reward lists Elder > Paladin > Knight at the bottom (each with its %).
    rank_lists = {"005DBD0D": 0, "005DBD0F": 1, "005CB9FF": 2}
    for r in data["rewardTree"]:
        kids = r.get("children") or []
        top = [k for k in kids if (k.get("formid") or "").upper() not in rank_lists]
        ranks = sorted([k for k in kids if (k.get("formid") or "").upper() in rank_lists],
                       key=lambda k: rank_lists[k["formid"].upper()])
        for k in ranks:
            k["showRate"] = True
        r["children"] = top + ranks
    data["checklist"] = build_checklist_section(data["rewardTree"], build_tradeable_map())
    effects = build_legendary_effects([s["lgdi"] for s in slots if s.get("lgdi")])
    if effects:
        data["rewardTree"].append(effects)
    if len(data["rewardTree"]) < 3:
        raise SystemExit("All Rewards tree has only %d roots - refusing to write it." % len(data["rewardTree"]))
    return data, slots


def build_checklist(rows, timer_tiers):
    """Page 2 + 3: Reward Checklist and Guide (df-bnb-daily-ops.js)."""
    data = {
        "slug": "daily-ops-reward-checklist",
        "pageType": "dailyops",
        "name": "Daily Ops",
        "meta": {"timerTiers": timer_tiers},
        "pools": build_checklist_pools(rows),
        "baseRewards": {"xp": "varies", "caps": "varies"},
    }
    if not data["pools"] or not any(p.get("items") for p in data["pools"]):
        raise SystemExit("Checklist pools came back empty - refusing to write them.")
    return data


def write_json_atomic(path, data):
    """Write to a temp file and swap it in, so a crash mid-write never leaves a
    half-written JSON on the site."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)
    with open(tmp, encoding="utf-8") as f:
        json.load(f)                      # must parse before it replaces the live file
    tmp.replace(path)


ALL_REWARDS_FILE = "daily_ops_all_rewards.json"
CHECKLIST_FILE = "daily_ops_reward_checklist.json"


def write_checklist_patchlog(data):
    """Patchlog feed for the checklist items. Same feed name the retired builder
    wrote, so the patchlog manifest keeps working. The previous version comes
    from git; if there is none yet (first build of the split file), skip rather
    than report every item as "added"."""
    rel = "dist/daily_ops/" + CHECKLIST_FILE
    prev = _git_show_json("HEAD^", rel)
    if not (isinstance(prev, dict) and prev.get("pools")):
        print("patchlog  : skipped (no previous checklist build to diff against)")
        return
    write_patchlog_feed(
        dist_dir=str(OUT_DIR),
        feed_name="patchlog_latest_df_daily_ops_rewards.json",
        current_items=_checklist_items(data),
        key_field="formId",
        name_field="name",
        compare_fields=["name", "category", "rarity"],
        prev_json_path=rel,
        items_extractor=_checklist_items,
    )


if __name__ == "__main__":
    # Each page's JSON is built and written on its own. If one fails, the other
    # still ships and the old copy of the failed one stays live untouched; the
    # script then exits non-zero so the workflow log shows what broke.
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows, quest_ids, identical, timer_tiers = build()
    failed = []

    try:
        tree, slots = build_all_rewards(rows, quest_ids, identical, timer_tiers)
        write_json_atomic(OUT_DIR / ALL_REWARDS_FILE, tree)
        print("GMRW reward slots")
        for s in slots:
            print("  %-15s %-8s %-9s %-45s lgdi=%s" % (s["kind"], s["tier"] or "-", s["formid"] or "-",
                                                        s["edid"] or "-", s["lgdi"] or "-"))
        print("Root expands")
        for r in tree["rewardTree"]:
            print("  %-18s %s" % (r["label"], ", ".join(
                "%s (%s)" % (c.get("label"), c.get("entryRate")) for c in r["children"])))
        print("written   : %s  (%d locations)" % (ALL_REWARDS_FILE, len(tree["locations"])))
    except BaseException as e:            # SystemExit included - keep going
        failed.append(ALL_REWARDS_FILE)
        print("FAILED    : %s - %s" % (ALL_REWARDS_FILE, e))

    try:
        checklist = build_checklist(rows, timer_tiers)
        write_json_atomic(OUT_DIR / CHECKLIST_FILE, checklist)
        write_checklist_patchlog(checklist)
        print("written   : %s  (%d pools)" % (CHECKLIST_FILE, len(checklist["pools"])))
    except BaseException as e:
        failed.append(CHECKLIST_FILE)
        print("FAILED    : %s - %s" % (CHECKLIST_FILE, e))

    if failed:
        raise SystemExit("Daily Ops build failed for: " + ", ".join(failed))
