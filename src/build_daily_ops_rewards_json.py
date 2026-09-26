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
        "subtitle": "First Elder clear of the day · rolls the Elder, Paladin and Knight rewards",
        "note": ("An Elder clear also counts as a Paladin and a Knight clear, so it "
                 "rolls all three chase lists - Elder at 100%, Paladin at 10% and "
                 "Knight at 5% - on top of the Elder-only bonus currency and "
                 "Legendary Modules. Each rank pays out once per day."),
    },
    "paladin": {
        "label": "Paladin",
        "subtitle": "First Paladin clear of the day · rolls the Paladin and Knight rewards",
        "note": ("A Paladin clear also counts as a Knight clear, so it rolls the "
                 "Paladin chase list at 10% and the Knight chase list at 5%. Each "
                 "rank pays out once per day."),
    },
    "knight": {
        "label": "Knight",
        "subtitle": "First Knight clear of the day · Knight rewards only",
        "note": "A Knight clear rolls the Knight chase list at 5%. Each rank pays out once per day.",
    },
    "everyClear": {
        "label": "Every Clear",
        "subtitle": "Every completion, any rank, first run or repeat · no plans in this pool",
        "note": ("This pool has no rank or first-run condition, so it pays out on "
                 "every completion - including your first of the day, alongside the "
                 "rank rewards above. Repeat runs only ever get this pool."),
    },
    "doubleMutation": {
        "label": "Double Mutation",
        "subtitle": "Double mutation ops only · Elder clears, every run",
        "note": ("Needs the double-mutation flag on and a clear inside 8:00. It is "
                 "not limited to your first run - every Elder clear of a double "
                 "mutation op pays it."),
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
    "005DBD0D": "Elder Chase Roll",
    "005DBD0F": "Paladin Chase Roll",
    "005CB9FF": "Knight Chase Roll",
    "005DBD10": "Bonus Currency",
    "00612800": "Legendary Modules",
    "005CBA06": "Every Clear Rewards",
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
    "decides your rank, and your rank decides how many chase rolls you get."
)

INFO_CARD = [
    "There are two kinds of op - Uplink and Decryption. They drop exactly the "
    "same rewards, so everything below applies to both.",

    "Finish an op and the game checks your clear time. Under 8 minutes is Elder, "
    "under 12 is Paladin, under 16 is Knight.",

    "Ranks stack. An Elder clear also counts as a Paladin and a Knight clear, so "
    "it rolls all three chase lists: Elder at 100%, Paladin at 10% and Knight at "
    "5%. A Paladin clear rolls Paladin and Knight. A Knight clear rolls Knight only.",

    "Each rank's rewards pay out once per day. After that, repeat runs only get "
    "the Every Clear pool - currency, aid, ammo, up to three grenades and a 3★ "
    "Legendary Item. There are no plans or recipes in it at all.",

    "Each chase roll is one item. The game picks a list, then a list inside that, "
    "then one thing out of it.",

    "Your level picks which chase list you are on. At level 50 and up you roll "
    "the high-level list. At 49 and under you roll the low-level one. There is no "
    "crossover in either direction.",

    "Double mutation ops add a bonus pool on Elder clears only - and unlike the "
    "rank rewards, it pays on every Elder clear, not just the first.",

    "During the Pint-Sized Slasher event, Slasher ops add their own list: "
    "guaranteed on your first Elder clear of the day, 1 in 4 on repeat clears.",

    "Almost nothing stops dropping once you learn it. Exactly one reward is "
    "removed once you own it: Player Title: Pint-Sized. Everything else keeps "
    "rolling forever. That is why the same plan keeps coming back.",

    "A few work backwards. Mod and paint plans for the War Glaive, Plasma "
    "Cutter, Crusader Pistol, Face Breaker, Hellstorm Missile Launcher, the "
    "Brotherhood Recon, Covert Scout and Arctic Marine armour, the Deep-Space "
    "Alien power armour and a handful of others only drop once you already know "
    "the base plan. Learn the base plan first or they cannot drop at all.",
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
        node["label"] = "Level %d+ Chase List" % CHASE_LEVEL_SPLIT
    elif fid == CHASE_LOW_LVL_LIST:
        node["levelLock"] = {"min": None, "max": CHASE_LEVEL_SPLIT - 1,
                             "label": "Level %d and under only" % (CHASE_LEVEL_SPLIT - 1)}
        node["label"] = "Level %d and Under Chase List" % (CHASE_LEVEL_SPLIT - 1)
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


def legendary_node(slot):
    """The GMRW's own legendary-item reward (QRLI + QRLR), shown as a one-item list."""
    rank = slot.get("lgdiRank") or ""
    name = ("%s★ Legendary Item" % rank) if rank else "Legendary Item"
    return {
        "type": "gmrwLegendary",
        "formid": slot["lgdi"],
        "label": name,
        "useAll": False,
        "items": [{"formid": slot["lgdi"], "name": name, "qty": 1, "dropRate": 100.0}],
    }


def slot_nodes(slot):
    out = []
    if slot["formid"]:
        n = lvli_node(slot["formid"])
        if n:
            out.append(n)
    if slot["lgdi"] and not any(o.get("type") == "gmrwLegendary" and o["formid"] == slot["lgdi"]
                                for o in out):
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
                    if n["formid"] in seen_lgdi:
                        continue
                    seen_lgdi.add(n["formid"])
                children.append(n)
        if not children:
            continue
        roots.append({
            "type": "tier",
            "tier": key,
            "label": meta["label"],
            "subtitle": meta["subtitle"],
            "note": meta["note"],
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
