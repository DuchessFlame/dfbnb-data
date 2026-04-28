#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# REWRITE_VERSION: 2026-04-28-v3
"""
build_seasonal_events_json.py - Seasonal Events Rewards (Tree Rewrite April 2026)

Builds:
  dist/seasonal_events/seasonal_events_rewards_by_page.json

Uses the shared rng76 engine (Rng76Data.from_tsv_root) for LVLI resolution.

Output schema per event page (abbreviated):
  {
    "name":           "Fasnacht Day Parade",
    "slug":           "fasnacht-day-parade-all-rewards",
    "eventSlug":      "fasnacht-day-parade",
    "isContainerLoot": false,
    "xp":   { "value": int, "scalesWithLevel": bool, "breakdown": [...] } | null,
    "caps": { "value": int, "breakdown": [...] } | null,
    "eventRewardTree": [ { type, label, items[{name, qty, dropRate, tiers?[]}] } ],
    "rewards":  [ ... legacy flat list, used by Unique Event Rewards expand ... ],
    "groups":   null | [ ... ],
    "gallery":  []
  }

Tier semantics:
  - Container events (Halloween, Holiday Scorched, Treasure Hunter): each
    container becomes a tier label. An item appearing in N containers will
    have N tier rows; if all rows have identical qty + rate they collapse.
  - Quest events (Fasnacht, Invaders, Meat Week, Mischief, Mothman, Big Bloom,
    Radtoads): same-stem LVLIs (e.g. Mothman _Tier01/02/03, Meat Week
    _Best/Good/Bad) merge into one tier-aware node.

Excludes:
  - Drifter Activation Card and any EDID starting with zzz_, CUT_, POST_, DEL_, P62_
  - GLOBs containing "IgnoreMe" / "XPNone" sentinels

Usage: python build_seasonal_events_json.py
"""

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Import shared drop-rate engine
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
for _p in [_this_dir, _this_dir / "src", _this_dir.parent / "src"]:
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from rng76 import (
    Rng76Data, Rng76Resolver,
    humanize_edid, fmt_pct, pick, read_tsv, newest, safe_float,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_ROOT   = str(_REPO_ROOT / "tsv")
DIST_DIR   = _REPO_ROOT / "dist" / "seasonal_events"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_EXCLUDE_RE = re.compile(r"^(zzz_|CUT_|POST_|DEL_|P62_)", re.IGNORECASE)
_DRIFTER_EDID = "P62_LLS_Rewards_TheDrifter_ActivationKeyCard"

IMAGE_BASE = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/seasonal-events/"

XP_REFERENCE_LEVEL = 50
MIN_RATE_DECIMAL = 0.0001  # 0.01% as decimal

# ---------------------------------------------------------------------------
# Event Definitions
# ---------------------------------------------------------------------------

EVENTS = {
    "fasnacht-day-parade-all-rewards": {
        "name": "Fasnacht Day Parade",
        "eventSlug": "fasnacht-day-parade",
        "description": "Join the fun during the Fasnacht Day parade and earn a chance at a festive mask!",
        "isContainerLoot": False,
        "questFormIDs": ["0049886E"],
    },
    "halloween-scorched-all-rewards": {
        "name": "Halloween Scorched",
        "eventSlug": "halloween-scorched",
        "description": "Take down Spooky Scorched to earn Spooky Treat Bags filled with Halloween-themed rewards.",
        "isContainerLoot": True,
        "containers": [
            {"title": "Spooky Treat Bag", "lvliFormID": "0062038D"},
        ],
    },
    "holiday-scorched-all-rewards": {
        "name": "Holiday Scorched",
        "eventSlug": "holiday-scorched",
        "description": "Defeat Holiday Scorched enemies to collect Holiday Gifts containing rare plans and outfits.",
        "isContainerLoot": True,
        "containers": [
            {"title": "Large Holiday Gift (Found)",   "lvliFormID": "005DCA88"},
            {"title": "Medium Holiday Gift (Found)",  "lvliFormID": "005DCA8A"},
            {"title": "Small Holiday Gift (Found)",   "lvliFormID": "005DCA89"},
            {"title": "Large Holiday Gift (Crafted)",  "lvliFormID": "005DCA85"},
            {"title": "Medium Holiday Gift (Crafted)", "lvliFormID": "005DCA87"},
            {"title": "Small Holiday Gift (Crafted)",  "lvliFormID": "005DCA86"},
        ],
    },
    "invaders-from-beyond-all-rewards": {
        "name": "Invaders from Beyond",
        "eventSlug": "invaders-from-beyond",
        "description": "Defend against the Zetan invasion and earn unique alien-themed rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["00620F7B"],
    },
    "meat-week-all-rewards": {
        "name": "Meat Week",
        "eventSlug": "meat-week",
        "description": "Help Grahm prepare for the cookout! Cook meat, collect prime cuts, and earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["0054B3FA", "0054B3F3"],
        "groups": [
            {"key": "cook", "label": "Grahm's Meat-Cook Rewards", "questFormID": "0054B3FA"},
            {"key": "hunt", "label": "Primal Cuts Rewards", "questFormID": "0054B3F3"},
        ],
    },
    "mischief-night-all-rewards": {
        "name": "Mischief Night",
        "eventSlug": "mischief-night",
        "description": "A night of mischief and mayhem at the Whitespring.",
        "isContainerLoot": False,
        "questFormIDs": ["005600A9", "0077FA14"],
    },
    "mothman-equinox-all-rewards": {
        "name": "Mothman Equinox",
        "eventSlug": "mothman-equinox",
        "description": "Participate in the Mothman Equinox event and earn unique Cultist-themed rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["006173A1"],
    },
    "the-big-bloom-all-rewards": {
        "name": "The Big Bloom",
        "eventSlug": "the-big-bloom",
        "description": "Investigate the strange blooming phenomenon and earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["0079AA0F"],
    },
    "treasure-hunter-all-rewards": {
        "name": "Hunt for the Treasure Hunter",
        "eventSlug": "hunt-for-the-treasure-hunter",
        "description": "Hunt down Mole Miner Treasure Hunters and open their pails for rare rewards.",
        "isContainerLoot": True,
        "containers": [
            {"title": "Ornate Mole Miner Pail (Found)",   "lvliFormID": "005D805A"},
            {"title": "Regular Mole Miner Pail (Found)",   "lvliFormID": "005D8054"},
            {"title": "Dusty Mole Miner Pail (Found)",     "lvliFormID": "005D8056"},
            {"title": "Ornate Mole Miner Pail (Crafted)",  "lvliFormID": "005D8053"},
            {"title": "Regular Mole Miner Pail (Crafted)", "lvliFormID": "005D8059"},
            {"title": "Dusty Mole Miner Pail (Crafted)",   "lvliFormID": "005D8055"},
        ],
    },
    "night-of-the-radtoads-all-rewards": {
        "name": "Night of the Radtoads",
        "eventSlug": "night-of-the-radtoads",
        "description": "Survive the Night of the Radtoads and earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": [],
    },
}

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def is_excluded(edid):
    if not edid:
        return False
    if _EXCLUDE_RE.match(edid):
        return True
    if _DRIFTER_EDID.lower() in edid.lower():
        return True
    return False


def is_trackable(name):
    lower = (name or "").lower()
    return lower.startswith("plan:") or lower.startswith("recipe:")


def slugify_item(name):
    s = (name or "").lower()
    s = re.sub(r"^(plan|recipe):\s*", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_image_url(event_slug, item_name):
    return IMAGE_BASE + event_slug + "/" + slugify_item(item_name) + ".avif"


def parse_ref(ref):
    parts = (ref or "").split(":")
    fid  = parts[0].strip() if len(parts) > 0 else ""
    edid = parts[1].strip() if len(parts) > 1 else ""
    sig  = parts[2].strip().upper() if len(parts) > 2 else ""
    return fid, edid, sig


# ---------------------------------------------------------------------------
# Label cleanup helpers
# ---------------------------------------------------------------------------

_TIER_SUFFIX_RE = re.compile(
    r"_+(Tier\d+|Best|Good|Bad|Bronze|Silver|Gold|"
    r"Easy|Medium|Hard|Common|Uncommon|Rare|Epic|Legendary|"
    r"Small|Large)$",
    re.IGNORECASE,
)

_LVLI_LABEL_OVERRIDES = {
    "ra_ll_rewards_publicevents":         "Public Event Rewards",
    "ra_ll_rewards_activities":           "Activity Rewards",
    "fishing_ll_rewards_improvedbait":    "Improved Bait",
    "ll_questreward_goldbullion":         "Gold Bullion",
    "legendaryitems_special_allitems":    "Legendary Items",
}


def _strip_event_prefix(edid):
    if not edid:
        return edid
    edid = re.sub(r"^E\d+[A-Z]?_", "", edid, flags=re.IGNORECASE)
    edid = re.sub(r"^[A-Z]{2,4}\d*_", "", edid)
    return edid


def _clean_pool_label(edid):
    if not edid:
        return ""
    tl = edid.lower()
    for substr, label in _LVLI_LABEL_OVERRIDES.items():
        if substr in tl:
            return label

    t = _strip_event_prefix(edid)

    glue = ["RA_LL", "RA_LLS", "LLS", "LL", "QuestReward", "Quest_Reward",
            "Quest_Rewards", "QuestRewards", "Event_Reward", "Event_Rewards",
            "EventReward", "EventRewards", "Quest", "Rewards", "Reward"]
    for token in glue:
        t = re.sub(r"(?:^|_)" + token + r"(?:_|$)", "_", t, flags=re.IGNORECASE)
    t = re.sub(r"^_+|_+$", "", t)
    t = re.sub(r"_+", "_", t)

    t = t.replace("_", " ")
    t = re.sub(r"([a-z])([A-Z])", r"\1 \2", t)
    t = re.sub(r"([A-Za-z])(\d)", r"\1 \2", t)
    t = re.sub(r"(\d)([A-Za-z])", r"\1 \2", t)
    t = re.sub(r"\s+", " ", t).strip()

    if not t:
        return _strip_event_prefix(edid).replace("_", " ").strip()

    words = t.split()
    out = " ".join(w.capitalize() if w.islower() else w for w in words)
    out = re.sub(r"\bTier\s*0?(\d+)\b", r"Tier \1", out, flags=re.IGNORECASE)
    return out.strip()


def _extract_tier_suffix(edid):
    if not edid:
        return edid, None
    m = _TIER_SUFFIX_RE.search(edid)
    if not m:
        return edid, None
    stem = edid[: m.start()].rstrip("_")
    tier = m.group(1)
    tier = re.sub(r"Tier\s*0?(\d+)", r"Tier \1", tier, flags=re.IGNORECASE)
    return stem, tier


def _clean_xp_caps_label(glob_ref, fallback="Event Completion"):
    if not glob_ref:
        return fallback
    edid = glob_ref.split(":")[1] if ":" in glob_ref else glob_ref
    if not edid:
        return fallback
    edid = re.sub(r"^(XP|Caps)_?", "", edid)
    edid = _strip_event_prefix(edid)
    edid = re.sub(r"_?Reward(s)?$", "", edid, flags=re.IGNORECASE)
    edid = re.sub(r"_+", "_", edid).strip("_")
    if not edid:
        return fallback
    parts = re.sub(r"_", " ", edid).strip()
    parts = re.sub(r"([a-z])([A-Z])", r"\1 \2", parts)
    parts = re.sub(r"([A-Za-z])(\d)", r"\1 \2", parts)
    parts = re.sub(r"(\d)([A-Za-z])", r"\1 \2", parts)
    parts = re.sub(r"\s+", " ", parts).strip()
    if not parts:
        return fallback
    words = parts.split()
    return " ".join(w.capitalize() if w.islower() else w for w in words)


# ---------------------------------------------------------------------------
# GMRW field resolvers
# ---------------------------------------------------------------------------

def _resolve_xp_value(xpct_ref, xp_glob_ref, data):
    """Resolve XP value at level XP_REFERENCE_LEVEL.
    Returns (xp_int, scales_bool, fid_or_None, label_or_None) or (None, ...)."""
    if xpct_ref:
        fid = xpct_ref.split(":")[0]
        y = data.curvs.interpolate(fid, XP_REFERENCE_LEVEL)
        if y is not None:
            label = _clean_xp_caps_label(xp_glob_ref) if xp_glob_ref else "Event Completion"
            return int(round(y)), True, fid, label
    if xp_glob_ref:
        fid = xp_glob_ref.split(":")[0]
        edid = xp_glob_ref.split(":")[1] if ":" in xp_glob_ref else ""
        if "IgnoreMe" in edid or "XPNone" in edid:
            return None, False, None, None
        v = data.globs.value(fid)
        if v is not None:
            return int(round(v)), False, fid, _clean_xp_caps_label(xp_glob_ref)
    return None, False, None, None


def _resolve_caps_value(caps_glob_ref, data):
    if not caps_glob_ref:
        return None, None, None
    fid = caps_glob_ref.split(":")[0]
    edid = caps_glob_ref.split(":")[1] if ":" in caps_glob_ref else ""
    if "IgnoreMe" in edid:
        return None, None, None
    v = data.globs.value(fid)
    if v is None:
        return None, None, None
    return int(round(v)), fid, _clean_xp_caps_label(caps_glob_ref)


def _legendary_rank_label(qrlr_str, qrri_str):
    qrlr = (qrlr_str or "").strip()
    qrri = (qrri_str or "").strip()
    min_rank = None
    max_rank = None
    try:
        if qrlr:
            min_rank = int(qrlr)
        if qrri:
            max_rank = int(qrri)
    except (ValueError, TypeError):
        pass
    if min_rank is None and max_rank is None:
        return "Legendary Items", None, None
    if min_rank is not None and max_rank is not None and min_rank != max_rank:
        return "Legendary Items ({}–{}★)".format(min_rank, max_rank), min_rank, max_rank
    rank = max_rank if max_rank is not None else min_rank
    if rank == 1:
        return "Legendary Items (1★)", rank, rank
    if rank is not None:
        return "Legendary Items (1–{}★)".format(rank), 1, rank
    return "Legendary Items", None, None


# ---------------------------------------------------------------------------
# GMRW row processing
# ---------------------------------------------------------------------------

def _gmrw_rows_for_quests(quest_fids, gmrw_rows):
    if not quest_fids:
        return []
    qset = {q.upper() for q in quest_fids}
    out = []
    for row in gmrw_rows:
        parent_raw = pick(row, "ParentQuestLink", "Parent Quest", default="")
        parent_fid = parent_raw.split(":")[0].strip().upper() if parent_raw else ""
        if parent_fid in qset:
            out.append(row)
    return out


def _gmrw_extract_xp_caps_per_ri(gmrw_rows, data):
    seen = set()
    xp_entries = []
    caps_entries = []
    ri_to_tier = {}

    for row in gmrw_rows:
        gmrw_fid = (row.get("FormID") or "").strip()
        ri_raw = row.get("RewardIndex") or "0"
        try:
            ri = int(ri_raw)
        except (ValueError, TypeError):
            ri = 0
        key = (gmrw_fid, ri)
        if key in seen:
            continue
        seen.add(key)

        edid = (row.get("EDID") or "").strip()
        if is_excluded(edid):
            continue

        xpct_ref    = (row.get("XPCT_XPCurveTable") or "").strip()
        xp_glob_ref = (row.get("NAM7_XPGlobal") or "").strip()
        caps_ref    = (row.get("NAM8_CapsGlobal") or "").strip()

        cond_text = (row.get("Conditions") or "").strip()
        tier_fn   = (row.get("TierConditionFunc") or "").strip()
        is_conditional = bool(cond_text) or bool(tier_fn)

        xp_val, xp_scales, xp_fid, xp_label = _resolve_xp_value(xpct_ref, xp_glob_ref, data)
        if xp_val and xp_val > 0:
            label = xp_label or "Event Completion"
            xp_entries.append({
                "label":           label,
                "xp":              xp_val,
                "xpFormID":        xp_fid,
                "scalesWithLevel": xp_scales,
                "condition":       is_conditional,
            })
            ri_to_tier.setdefault(ri, label)

        caps_val, caps_fid, caps_label = _resolve_caps_value(caps_ref, data)
        if caps_val and caps_val > 0:
            label = caps_label or "Event Completion"
            caps_entries.append({
                "label":     label,
                "caps":      caps_val,
                "capsFormID": caps_fid,
                "condition": is_conditional,
            })
            ri_to_tier.setdefault(ri, label)

    return xp_entries, caps_entries, ri_to_tier


def _gmrw_iter_lvli_sources(gmrw_rows):
    """Yield (title, lvli_fid, lvli_edid, ri, parent_quest_fid)."""
    seen = set()
    for row in gmrw_rows:
        edid = pick(row, "EDID", "GMRW_EDID", default="")
        if is_excluded(edid):
            continue

        rewarded = pick(row, "RewardedItem", default="")
        if not rewarded:
            continue
        fid, ref_edid, sig = parse_ref(rewarded)
        if sig != "LVLI" or not fid:
            continue
        if is_excluded(ref_edid):
            continue
        if fid in seen:
            continue
        seen.add(fid)

        ri_raw = row.get("RewardIndex") or "0"
        try:
            ri = int(ri_raw)
        except (ValueError, TypeError):
            ri = 0

        parent_fid = pick(row, "ParentQuestLink", default="").split(":")[0].strip()

        title = _clean_pool_label(ref_edid) if ref_edid else _clean_pool_label(edid)
        yield title, fid, ref_edid, ri, parent_fid


def _gmrw_iter_legendary_sources(gmrw_rows):
    """Yield (title, lvli_fid, lvli_edid, ri) for each QRLI legendary list."""
    seen = set()
    for row in gmrw_rows:
        edid = pick(row, "EDID", "GMRW_EDID", default="")
        if is_excluded(edid):
            continue
        qrli = pick(row, "QRLI_LegendaryItemRewardList", default="")
        if not qrli:
            continue
        fid, ref_edid, sig = parse_ref(qrli)
        if not fid or is_excluded(ref_edid):
            continue
        if fid in seen:
            continue
        seen.add(fid)

        ri_raw = row.get("RewardIndex") or "0"
        try:
            ri = int(ri_raw)
        except (ValueError, TypeError):
            ri = 0

        rank_label, _min, _max = _legendary_rank_label(
            row.get("QRLR_LegendaryItemRewardRank"),
            row.get("QRRI_LegendaryRankRandom"),
        )
        yield rank_label, fid, ref_edid, ri


# ---------------------------------------------------------------------------
# Tree node construction
# ---------------------------------------------------------------------------

def _build_lvli_node(title, lvli_fid, lvli_edid, resolver, ev_slug,
                     tier_label_fn, group_key=None):
    """Resolve an LVLI deep and build a tree node (or None)."""
    try:
        items = resolver.resolve_deep(lvli_fid)
    except Exception as e:
        print("    [ERROR] resolve_deep({}): {}".format(lvli_fid, e))
        return None

    out_items = []
    seen_fids = set()
    for it in items:
        fid  = it.get("formid", "")
        edid = it.get("edid", "")
        name = it.get("name", "")
        rate = it.get("dropRate", 0.0)
        qty  = it.get("qty", 1) or 1
        sig  = (it.get("sig") or "").upper()

        if not fid or not name:
            continue
        if is_excluded(edid):
            continue
        if rate < MIN_RATE_DECIMAL:
            continue
        if fid in seen_fids:
            continue
        seen_fids.add(fid)

        out_items.append({
            "name":     name,
            "formid":   fid,
            "edid":     edid,
            "sig":      sig,
            "qty":      qty,
            "dropRate": round(rate * 100, 6),
            "tiers":    [{"tier": tier_label_fn({"formid": fid, "name": name}),
                          "qty":  qty,
                          "rate": round(rate * 100, 6)}],
        })

    if not out_items:
        return None

    out_items.sort(key=lambda x: x["name"].lower())

    node = {
        "type":         "lvli",
        "formid":       lvli_fid,
        "edid":         lvli_edid,
        "label":        title,
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items":        out_items,
    }
    if group_key:
        node["group"] = group_key
    return node


def _merge_tier_nodes(nodes_per_tier):
    """Merge nodes-per-tier into a single node where items repeated across
    tiers get a stacked tier-row breakdown."""
    if not nodes_per_tier:
        return None

    merged_items = {}
    first_node = None

    for tier_label, node in nodes_per_tier:
        if not node:
            continue
        if first_node is None:
            first_node = node
        for it in node["items"]:
            fid = it["formid"]
            if fid not in merged_items:
                merged_items[fid] = {
                    "name":     it["name"],
                    "formid":   fid,
                    "edid":     it["edid"],
                    "sig":      it.get("sig", ""),
                    "qty":      it["qty"],
                    "dropRate": it["dropRate"],
                    "tiers":    [],
                }
            for src_tier in it["tiers"]:
                merged_items[fid]["tiers"].append({
                    "tier": tier_label,
                    "qty":  src_tier.get("qty", it["qty"]),
                    "rate": src_tier.get("rate", it["dropRate"]),
                })

    if not merged_items:
        return None

    out_items = sorted(merged_items.values(), key=lambda x: x["name"].lower())

    for it in out_items:
        rates = [t["rate"] for t in it["tiers"]]
        qtys  = [t["qty"]  for t in it["tiers"]]
        it["dropRate"] = max(rates) if rates else it["dropRate"]
        it["qty"]      = max(qtys)  if qtys  else it["qty"]

    return {
        "type":         "lvli",
        "formid":       first_node["formid"],
        "edid":         first_node["edid"],
        "label":        first_node["label"],
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items":        out_items,
    }


def _build_caps_node(caps_breakdown):
    if not caps_breakdown:
        return None
    items = []
    for entry in caps_breakdown:
        label = entry.get("label") or "Event Completion"
        val = int(entry.get("caps") or 0)
        if val <= 0:
            continue
        items.append({
            "name":     "Caps",
            "formid":   "0000000F",
            "edid":     "Caps001",
            "sig":      "CNCY",
            "qty":      val,
            "dropRate": 100.0,
            "tiers":    [{"tier": label, "qty": val, "rate": 100.0}],
        })
    if not items:
        return None
    if len(items) > 1:
        merged_tiers = []
        for it in items:
            merged_tiers.extend(it["tiers"])
        canonical_qty = max(t["qty"] for t in merged_tiers)
        items = [{
            "name":     "Caps",
            "formid":   "0000000F",
            "edid":     "Caps001",
            "sig":      "CNCY",
            "qty":      canonical_qty,
            "dropRate": 100.0,
            "tiers":    merged_tiers,
        }]
    return {
        "type":         "synthetic",
        "formid":       "",
        "edid":         "_synthetic_caps",
        "label":        "Caps",
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items":        items,
    }


def _build_legendary_node(title, lvli_fid, lvli_edid):
    """Synthetic info-only legendary node (LGDI lists aren't in the LVLI index)."""
    return {
        "type":         "synthetic",
        "formid":       lvli_fid,
        "edid":         lvli_edid,
        "label":        title,
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items": [{
            "name":     title,
            "formid":   lvli_fid,
            "edid":     lvli_edid,
            "sig":      "LGDI",
            "qty":      1,
            "dropRate": 100.0,
            "tiers":    [{"tier": title, "qty": 1, "rate": 100.0}],
        }],
    }


def _collapse_redundant_tiers(node):
    """Drop tiers[] when every tier has identical qty + rate so the JS shows one row."""
    if not node or not node.get("items"):
        return node
    for it in node["items"]:
        tiers = it.get("tiers") or []
        if len(tiers) <= 1:
            it.pop("tiers", None)
            continue
        first_qty = tiers[0].get("qty")
        first_rate = tiers[0].get("rate")
        if all(t.get("qty") == first_qty and t.get("rate") == first_rate for t in tiers):
            it.pop("tiers", None)
    return node


# ---------------------------------------------------------------------------
# Per-event processing
# ---------------------------------------------------------------------------

def _process_quest_event(event_def, slug, resolver, data, gmrw_rows):
    quest_fids = event_def.get("questFormIDs") or []
    rows = _gmrw_rows_for_quests(quest_fids, gmrw_rows)
    print("  Found {} GMRW rows for quest(s) {}".format(len(rows), quest_fids))

    xp_breakdown, caps_breakdown, ri_to_tier = _gmrw_extract_xp_caps_per_ri(rows, data)
    xp_block   = _build_xp_block(xp_breakdown)
    caps_block = _build_caps_block_summary(caps_breakdown)

    tree = []

    caps_node = _build_caps_node(caps_breakdown)
    if caps_node:
        tree.append(caps_node)

    groups = event_def.get("groups")
    quest_to_group = {}
    if groups:
        for g in groups:
            quest_to_group[g["questFormID"]] = g["key"]

    sources = list(_gmrw_iter_lvli_sources(rows))
    by_stem = {}
    stem_order = []
    for title, lvli_fid, lvli_edid, ri, parent_fid in sources:
        stem, tier = _extract_tier_suffix(lvli_edid or "")
        if stem not in by_stem:
            by_stem[stem] = []
            stem_order.append(stem)
        by_stem[stem].append((title, lvli_fid, lvli_edid, ri, parent_fid, tier))

    for stem in stem_order:
        entries = by_stem[stem]

        group_key = None
        if quest_to_group and entries:
            group_key = quest_to_group.get(entries[0][4])

        if len(entries) == 1:
            title, lvli_fid, lvli_edid, ri, _parent_fid, tier_suffix = entries[0]
            tier_label_for_items = tier_suffix or title
            node = _build_lvli_node(
                title, lvli_fid, lvli_edid, resolver, event_def["eventSlug"],
                tier_label_fn=lambda _it, _t=tier_label_for_items: _t,
                group_key=group_key,
            )
            if node:
                if ri in ri_to_tier:
                    node["tierLabel"] = ri_to_tier[ri]
                _collapse_redundant_tiers(node)
                tree.append(node)
        else:
            nodes_per_tier = []
            for title, lvli_fid, lvli_edid, ri, _parent_fid, tier_suffix in entries:
                tlabel = tier_suffix or title
                sub_node = _build_lvli_node(
                    title, lvli_fid, lvli_edid, resolver, event_def["eventSlug"],
                    tier_label_fn=lambda _it, _t=tlabel: _t,
                )
                if sub_node:
                    nodes_per_tier.append((tlabel, sub_node))

            merged = _merge_tier_nodes(nodes_per_tier)
            if merged:
                merged["label"] = _clean_pool_label(stem)
                if group_key:
                    merged["group"] = group_key
                _collapse_redundant_tiers(merged)
                tree.append(merged)

    for title, lvli_fid, lvli_edid, ri in _gmrw_iter_legendary_sources(rows):
        node = _build_legendary_node(title, lvli_fid, lvli_edid)
        if node:
            if ri in ri_to_tier:
                node["tierLabel"] = ri_to_tier[ri]
            tree.append(node)

    flat_rewards = _build_flat_rewards_from_tree(tree, event_def, groups)

    return {
        "xp":              xp_block,
        "caps":            caps_block,
        "eventRewardTree": tree,
        "rewards":         flat_rewards,
        "groups":          groups,
    }


def _process_container_event(event_def, slug, resolver, data):
    containers = event_def.get("containers") or []
    print("  Resolving {} container LVLIs".format(len(containers)))

    nodes_per_tier = []
    for c in containers:
        title = c.get("title", "")
        lvli_fid = c.get("lvliFormID", "")
        if not lvli_fid:
            continue
        node = _build_lvli_node(
            title, lvli_fid, "", resolver, event_def["eventSlug"],
            tier_label_fn=lambda _it, _t=title: _t,
        )
        if node:
            nodes_per_tier.append((title, node))

    merged = _merge_tier_nodes(nodes_per_tier)
    tree = []
    if merged:
        merged["label"] = event_def["name"] + " Rewards"
        _collapse_redundant_tiers(merged)
        tree.append(merged)

    flat_rewards = _build_flat_rewards_from_tree(tree, event_def, None)

    return {
        "xp":              None,
        "caps":            None,
        "eventRewardTree": tree,
        "rewards":         flat_rewards,
        "groups":          event_def.get("groups"),
    }


# ---------------------------------------------------------------------------
# Output builders
# ---------------------------------------------------------------------------

def _build_xp_block(xp_breakdown):
    visible = [e for e in xp_breakdown if e.get("xp", 0) > 0]
    if not visible:
        return None
    total = sum(e["xp"] for e in visible)
    scales = any(e.get("scalesWithLevel") for e in visible)
    return {
        "value":           total,
        "scalesWithLevel": scales,
        "breakdown":       visible,
    }


def _build_caps_block_summary(caps_breakdown):
    visible = [e for e in caps_breakdown if e.get("caps", 0) > 0]
    if not visible:
        return None
    total = sum(e["caps"] for e in visible)
    return {
        "value":     total,
        "breakdown": visible,
    }


def _build_flat_rewards_from_tree(tree, event_def, groups):
    """Produce the legacy flat rewards[] list from the event reward tree."""
    ev_slug = event_def["eventSlug"]
    by_fid = {}
    for node in tree:
        node_label = node.get("label", "")
        for it in node.get("items", []):
            fid = it["formid"]
            name = it["name"]
            edid = it["edid"]
            if fid not in by_fid:
                by_fid[fid] = {
                    "name":         name,
                    "formId":       fid,
                    "edid":         edid,
                    "imageUrl":     build_image_url(ev_slug, name),
                    "releaseYear":  None,
                    "tradeable":    None,
                    "isTrackable":  is_trackable(name),
                    "howToObtain":  "<strong>Source:</strong> " + node_label,
                    "group":        node.get("group"),
                    "dropRates":    [],
                }
            for tier in it.get("tiers") or [{"tier": node_label, "rate": it["dropRate"]}]:
                by_fid[fid]["dropRates"].append({
                    "tier": tier.get("tier") or node_label,
                    "rate": fmt_pct(tier.get("rate", it["dropRate"])),
                })
            if node.get("group") and not by_fid[fid].get("group"):
                by_fid[fid]["group"] = node.get("group")

    return sorted(by_fid.values(), key=lambda r: r["name"].lower())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("[build_seasonal_events] Loading rng76 engine...")
    data = Rng76Data.from_tsv_root(TSV_ROOT)
    resolver = data.resolver

    gmrw_path = newest(str(_REPO_ROOT / "tsv" / "GMRW_Export_*.tsv"))
    gmrw_rows = read_tsv(gmrw_path)
    print("[build_seasonal_events] Loaded {} GMRW rows from {}".format(
        len(gmrw_rows), os.path.basename(gmrw_path)))

    output = {"byPage": {}}

    for slug, event_def in EVENTS.items():
        ev_name = event_def["name"]
        ev_slug = event_def["eventSlug"]
        print("\n[build_seasonal_events] Processing: {} ({})".format(ev_name, slug))

        page_data = {
            "name":            ev_name,
            "description":     event_def["description"],
            "slug":            slug,
            "eventSlug":       ev_slug,
            "isContainerLoot": event_def["isContainerLoot"],
            "xp":              None,
            "caps":            None,
            "eventRewardTree": [],
            "rewards":         [],
            "groups":          event_def.get("groups"),
            "gallery":         [],
        }

        try:
            if event_def["isContainerLoot"]:
                ev = _process_container_event(event_def, slug, resolver, data)
            else:
                ev = _process_quest_event(event_def, slug, resolver, data, gmrw_rows)
        except Exception as e:
            print("  [ERROR] processing failed: {}".format(e))
            ev = {"xp": None, "caps": None, "eventRewardTree": [], "rewards": [],
                  "groups": event_def.get("groups")}

        page_data["xp"]              = ev.get("xp")
        page_data["caps"]            = ev.get("caps")
        page_data["eventRewardTree"] = ev.get("eventRewardTree") or []
        page_data["rewards"]         = ev.get("rewards") or []
        page_data["groups"]          = ev.get("groups")

        tree_len = len(page_data["eventRewardTree"])
        rewards_len = len(page_data["rewards"])
        xp_summary = "{} XP".format(page_data["xp"]["value"]) if page_data["xp"] else "no XP"
        caps_summary = "{} caps".format(page_data["caps"]["value"]) if page_data["caps"] else "no caps"
        print("  -> {} tree nodes, {} flat rewards, {}, {}".format(
            tree_len, rewards_len, xp_summary, caps_summary))

        output["byPage"][slug] = page_data
        url_path = "/df/seasonal-events/" + ev_slug + "/" + slug + "/"
        output["byPage"][url_path] = page_data
        output["byPage"][url_path.rstrip("/")] = page_data

    DIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DIST_DIR / "seasonal_events_rewards_by_page.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print("\n[build_seasonal_events] Written: {}".format(out_path))
    print("[build_seasonal_events] File size: {} bytes".format(out_path.stat().st_size))


if __name__ == "__main__":
    main()
