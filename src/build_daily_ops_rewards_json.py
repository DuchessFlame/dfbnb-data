#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_daily_ops_rewards_json.py - Daily Ops All Rewards (Activities-shaped)

Writes:
    dist/daily_ops/daily_ops_all_rewards.json          (live)
    dist/pts/daily_ops/daily_ops_all_rewards.json      (--pts)

This is a NEW output path. The legacy builder (build_daily_ops_json.py ->
daily_ops_rewards.json) and the legacy renderer (df-bnb-daily-ops.js) are left
alone so the live page keeps working until the new renderer is wired up.

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
    python3 src/build_daily_ops_rewards_json.py            # live
    python3 src/build_daily_ops_rewards_json.py --pts      # PTS twin
"""

import csv
import json
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

import build_events_rewards_json as ev      # import-safe: writes nothing
import tsv_source

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
    "repeat":  {"label": "Repeat Runs", "seconds": None, "timeLabel": None, "order": 3},
}

POOL_TITLES = {
    "005DBD0D": "Chase Rewards",
    "005DBD0F": "Chase Rewards",
    "005CB9FF": "Chase Rewards",
    "005DBD10": "Bonus Currency",
    "00612800": "Legendary Modules",
    "005CBA06": "Repeat Run Rewards",
    "0061F72A": "Double Mutation Bonus",
    "00935CEA": "Seasonal Event Rewards",
    "008FCEA5": "Seasonal Event Rewards",
}

# Intro card copy. Lives in the data, not the renderer, so it can be corrected
# without an FTP push. Every claim here is traceable to a record:
#   tier times          GLOB 005CB976/7/8  (480 / 720 / 960)
#   chase roll 100/10/5 GLOB 005CB979/A/B via each tier list's ListCond1
#   level split         005DBD0D entry 0 GetLevel >= 50 + First Match fallthrough
#   no plans on repeats 005CBA06 walked: 333 items, 0 plans/recipes
#   only title removed  "already learned" condition appears once, on Pint-Sized
DESCRIPTION = (
    "Daily Ops rewards, pulled straight from the game files. Your clear time "
    "decides your rank, and your rank decides whether you get a plan at all."
)

INFO_CARD = [
    "There are two kinds of op - Uplink and Decryption. They drop exactly the "
    "same rewards, so everything below applies to both.",

    "Finish an op and the game checks your clear time. Under 8 minutes is Elder, "
    "under 12 is Paladin, under 16 is Knight.",

    "Your rank decides whether you get a plan at all. At Elder the chase reward "
    "is guaranteed - it fires every run. At Paladin it fires 1 run in 10. At "
    "Knight, 1 in 20. The list of possible plans is the same either way. Speed "
    "does not change what you can win, it changes whether you get to roll.",

    "Plans only drop on your first op of the day. Repeat runs move to a "
    "different pool with no plans or recipes in it at all - caps, scrip, aid, "
    "ammo and up to three grenades. If you are farming plans, the first run is "
    "the one that counts.",

    "The chase reward is one roll, one item. The game picks a list, then a list "
    "inside that, then one thing out of it.",

    "Your level picks which chase list you are on, and you only ever get one. At "
    "level 50 and up you roll the high-level list. At 49 and under you roll the "
    "low-level one. There is no crossover in either direction.",

    "Double mutation ops only pay out at Elder. The bonus needs the "
    "double-mutation flag on and a clear inside 8 minutes. Paladin and Knight "
    "get nothing extra from it.",

    "Almost nothing stops dropping once you learn it. Across all nine reward "
    "lists - 1,374 items, 907 of them plans and recipes - exactly one is removed "
    "once you own it: Player Title: Pint-Sized. Everything else keeps rolling "
    "forever. That is why the same plan keeps coming back.",

    "A few work backwards. The War Glaive, Plasma Cutter, Crusader Pistol, Face "
    "Breaker, and the Brotherhood Recon, Covert Scout and Arctic Marine armour "
    "pieces drop mods that only appear once you already know the base plan. "
    "Learn the plan first or they cannot drop at all.",
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
    elif fid == CHASE_LOW_LVL_LIST:
        node["levelLock"] = {"min": None, "max": CHASE_LEVEL_SPLIT - 1,
                             "label": "Level %d and under only" % (CHASE_LEVEL_SPLIT - 1)}
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


def tier_for_row(row):
    globs = (row.get("ConditionGlobs") or "")
    for fid, tier in TIER_BY_GLOB.items():
        if fid in globs:
            return tier
    return "repeat"


def collect_reward_lists(rows):
    """{tier: [ {formid, edid, title} ]} preserving GMRW reward order."""
    by_tier = {}
    seen = set()
    for row in sorted(rows, key=lambda r: int(r.get("RewardIndex") or 0)):
        ref = (row.get("RewardedItem") or "").strip()
        if not ref or ":LVLI" not in ref:
            continue
        fid, edid = ref.split(":")[0], ref.split(":")[1]
        tier = tier_for_row(row)
        key = (tier, fid)
        if key in seen:
            continue
        seen.add(key)
        by_tier.setdefault(tier, []).append({
            "formid": fid,
            "edid": edid,
            "title": POOL_TITLES.get(fid) or ev.prettify_lvli_label(edid),
        })
    return by_tier


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
# Build
# ---------------------------------------------------------------------------

def build():
    per_quest = gmrw_rows_for_daily_ops()
    if not per_quest:
        raise SystemExit("No Daily Ops GMRW rows found - check the GMRW export.")

    # Uplink and Decryption were verified identical (all 11 reward entries, every
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
    by_tier = collect_reward_lists(rows)

    reward_tree = []
    audit = []
    for tier in sorted(by_tier, key=lambda t: TIER_META.get(t, {}).get("order", 99)):
        meta = TIER_META.get(tier, {"label": tier.title(), "timeLabel": None})
        children = []
        for pool in by_tier[tier]:
            node = ev.build_lvli_tree_node(pool["formid"])
            if not node:
                continue
            before = node.get("entryRate")
            node = stamp_list_self_chance(node)
            node = stamp_level_lock(node)
            node["label"] = pool["title"]
            after = node.get("entryRate")
            audit.append((tier, pool["title"], pool["formid"], before, after))
            children.append(node)
        if not children:
            continue
        reward_tree.append({
            "type": "tier",
            "tier": tier,
            "label": meta["label"],
            "timeLabel": meta.get("timeLabel"),
            "seconds": meta.get("seconds"),
            "isTierWrapper": True,
            "children": children,
        })

    data = {
        "slug": "daily-ops-all-rewards",
        "pageType": "dailyops",
        "name": "Daily Ops",
        "description": DESCRIPTION,
        "infoCard": INFO_CARD,
        "opTypes": [QUESTS[q] for q in quest_ids],
        "opRewardsIdentical": bool(identical),
        "meta": {
            "timerTiers": {
                k: {"seconds": v["seconds"], "label": "%s (<= %s)" % (v["label"], v["timeLabel"])}
                for k, v in TIER_META.items() if v.get("seconds")
            }
        },
        "locations": build_locations(),
        "gallery": [],
        "rewardTree": reward_tree,
        "warnings": [],
    }
    return data, audit


if __name__ == "__main__":
    data, audit = build()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "daily_ops_all_rewards.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))

    print("TIER ROLL FIX - entryRate stamped on each reward-list root")
    print("%-9s %-24s %-9s %-10s %-10s" % ("tier", "pool", "formid", "before", "after"))
    for tier, title, fid, before, after in audit:
        print("%-9s %-24s %-9s %-10s %-10s" % (tier, title[:24], fid, before, after))
    print()
    print("locations : %d" % len(data["locations"]))
    print("tier roots: %d" % len(data["rewardTree"]))
    print("written   : %s" % out_path)
