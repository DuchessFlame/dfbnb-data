#!/usr/bin/env python3
"""
events_page_pools.py — shared reward-pool extraction for event-based pages.

Neutral-named helper (no page-specific coupling) imported by the per-category
build scripts:
    build_raids_json.py       -> dist/raids/raids_rewards.json
    build_expos_json.py       -> dist/expos/expos_rewards.json
    build_daily_ops_json.py   -> dist/daily_ops/daily_ops_rewards.json

It contains the events-based pool assembly that previously lived once inside
build_reho_json.py (_build_from_events + _process_pool + title cleanup + pool
merge + condition summaries). Each build script owns its own PAGE_MAPPINGS
subset, its own dist output, and its own workflow — only the maths lives here,
mirroring how build_activities_rewards_json.py imports rng76.

Source of truth: dist/events/events_rewards.json (built by
build_events_rewards_json.py). No dependency on the retired dist/reho tree.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


# Pool title replacements for human-readable names (ported verbatim from the
# retired build_reho_json.py so output is unchanged).
POOL_TITLE_REPLACEMENTS = {
    "Dailyops Repeatable Doublemutation Rewards": "Double Mutation Bonus",
    "Dailyops Repeatable Quest Rewards": "Repeatable Quest Rewards",
    "Dailyops Rewards Additionalcurrency Tier03": "Bonus Currency: Elder Tier",
    "Dailyops Rewards Additionalcurrency Tier02": "Bonus Currency: Paladin Tier",
    "Dailyops Rewards Additionalcurrency Tier01": "Bonus Currency: Knight Tier",
    "Dailyops Rewards Chase Tier03": "Chase Rewards: Elder Tier",
    "Dailyops Rewards Chase Tier02": "Chase Rewards: Paladin Tier",
    "Dailyops Rewards Chase Tier01": "Chase Rewards: Knight Tier",
    "Restricted LL Legendarymodule 1-3": "Legendary Modules: Elder Tier",
    "P62 Lls Rewards Thedrifter Activationkeycard": "Activation Keycard",
    "Rd01 LL Raids Rewards Enc01": "Stage 1 Rewards",
    "Rd01 LL Raids Rewards Enc02": "Stage 2 Rewards",
    "Rd01 LL Raids Rewards Enc03": "Stage 3 Rewards",
    "Rd01 LL Raids Rewards Enc04": "Stage 4 Rewards",
    "Rd01 LL Raids Rewards Enc05": "Stage 5 Rewards",
    "Rd01 Lls Raids Rewards Enc01 Trophies": "Speed Run Trophy: Stage 1",
    "Rd01 Lls Raids Rewards Enc02 Trophies": "Speed Run Trophy: Stage 2",
    "Rd01 Lls Raids Rewards Enc03 Trophies": "Speed Run Trophy: Stage 3",
    "Rd01 Lls Raids Rewards Enc04 Trophies": "Speed Run Trophy: Stage 4",
    "Rd01 Lls Raids Rewards Enc05 Trophies": "Speed Run Trophy: Stage 5",
    "Xpd Ac LL Mission01 Reward Mission": "Mission Rewards (All Optionals)",
    "Xpd Pitt LL Mission Reward Mission": "Mission Rewards (All Optionals)",
    "Xpd LL Mission Reward Repeatable": "Repeatable Mission Rewards",
}

_HEX8 = re.compile(r'^[0-9A-Fa-f]{8}$')


# ---------------------------------------------------------------------------
# LOADERS
# ---------------------------------------------------------------------------

def load_events_index(events_rewards_path: Path) -> Dict[str, Any]:
    """Load events_rewards.json and index each event by questFormID and name."""
    events_rewards_path = Path(events_rewards_path)
    if not events_rewards_path.exists():
        raise FileNotFoundError(f"{events_rewards_path} not found")

    with open(events_rewards_path) as f:
        data = json.load(f)

    index: Dict[str, Any] = {}
    for event in data.get("events", []):
        index[event["questFormID"]] = event
        index[event["name"]] = event
    return index


def load_drop_rate_pools(drop_rates_path: Optional[Path]) -> Dict[str, Any]:
    """
    Optional: load the 'pools' map from drop_rates.json for KEYM name-resolution
    fallback. None/missing -> empty dict. The event-based pages (Daily Ops,
    Expos, Raids) currently need no fallback, so this is normally skipped.
    """
    if not drop_rates_path:
        return {}
    drop_rates_path = Path(drop_rates_path)
    if not drop_rates_path.exists():
        return {}
    with open(drop_rates_path) as f:
        return json.load(f).get("pools", {})


# ---------------------------------------------------------------------------
# HELPERS (ported from build_reho_json.py)
# ---------------------------------------------------------------------------

def generate_pool_id(title: str) -> str:
    pool_id = re.sub(r'[^\w\s-]', '', title.lower())
    pool_id = re.sub(r'[\s-]+', '-', pool_id)
    return pool_id.strip('-')


def format_seconds(seconds: int) -> str:
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}:{secs:02d}"


def _get_tier_seconds(config: Dict[str, Any], tier: str) -> int:
    if "timerGlobs" in config and tier in config["timerGlobs"]:
        return config["timerGlobs"][tier][1]
    return 0


def summarize_conditions(conditions: List[str], config: Dict[str, Any]) -> str:
    if not conditions or conditions == ["GetItemCount"]:
        return ""

    summaries = []
    for condition in conditions:
        if not condition or condition == "GetItemCount":
            continue
        if "GetRemainingQuestTimeSeconds" in condition:
            # tier extraction returns None in the original (kept for parity)
            pass
        elif "IsActivePlayer" in condition and "1.000000" in condition:
            summaries.append("Must be an active participant")
        elif "HasLearnedRecipe" in condition and "0.000000" in condition:
            summaries.append("Must not already know the recipe")
        elif "GetExpeditionsInstanceNumOptbjectivesCompleted" in condition:
            summaries.append("Complete all optional objectives")
        elif "GetRandomPercent" in condition:
            summaries.append("Chance-based drop")

    return " | ".join(summaries) if summaries else ""


def resolve_item_name(formid: str, edid: str, drop_rate_pools: Dict[str, Any]) -> str:
    """Resolve a FormID to a display name using drop_rates.json pools."""
    for pool in drop_rate_pools.values():
        for item in pool.get("items", []):
            if item.get("formid") == formid:
                name = item.get("name", "")
                if name and name != formid:
                    return name
    if edid:
        return edid
    return formid


def process_pool(pool: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single reward pool into the page-ready shape."""
    title = pool.get("title", "")
    clean_title = POOL_TITLE_REPLACEMENTS.get(title, title)

    tier = ""
    if "Elder" in clean_title:
        tier = "elder"
    elif "Paladin" in clean_title:
        tier = "paladin"
    elif "Knight" in clean_title:
        tier = "knight"

    return {
        "title": clean_title,
        "poolId": generate_pool_id(clean_title),
        "lvliFormID": pool.get("lvliFormID", ""),
        "lvliEdid": pool.get("lvliEdid", ""),
        "tier": tier,
        "poolChance": pool.get("poolChance", 100.0),
        "conditions": pool.get("conditions", []),
        "conditionSummary": summarize_conditions(pool.get("conditions", []), config),
        "items": pool.get("items", []),
        "itemCount": len(pool.get("items", [])),
    }


def merge_duplicate_pools(pools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge pools with the same lvliFormID (mutually exclusive conditions)."""
    seen: Dict[str, int] = {}
    merged: List[Dict[str, Any]] = []

    for pool in pools:
        lvli_fid = pool.get("lvliFormID", "")
        if not lvli_fid or lvli_fid not in seen:
            seen[lvli_fid] = len(merged)
            merged.append(pool)
        else:
            existing = merged[seen[lvli_fid]]
            existing_conds = existing.get("conditions", [])
            new_conds = pool.get("conditions", [])
            if new_conds and new_conds != existing_conds:
                existing["conditions"] = existing_conds + new_conds
                existing["conditionSummary"] = "Chance-based drop (toggle)"

    return merged


# ---------------------------------------------------------------------------
# PAGE BUILDER
# ---------------------------------------------------------------------------

def build_page_from_events(
    events_index: Dict[str, Any],
    config: Dict[str, Any],
    drop_rate_pools: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build one page's data from events_rewards.json (indexed by
    load_events_index). Returns {} when the configured quest is absent.

    config keys used:
      name, pageType, questFormID   (all)
      timerGlobs                    (dailyops — drives timer-tier meta)
      speedrunSeconds               (raid — drives speedrun meta)
    """
    drop_rate_pools = drop_rate_pools or {}
    quest_id = config["questFormID"]

    if quest_id not in events_index:
        print(f"  Warning: Quest {quest_id} not found in events_rewards.json")
        return {}

    event = events_index[quest_id]

    page_data = {
        "name": config["name"],
        "pageType": config["pageType"],
        "questFormID": quest_id,
        "gameName": event.get("gameName", ""),
        "description": event.get("description", ""),
        "pools": [],
        "baseRewards": {"xp": "varies", "caps": "varies"},
    }

    for pool in event.get("pools", []):
        pool_data = process_pool(pool, config)
        if pool_data:
            page_data["pools"].append(pool_data)

    page_data["pools"] = merge_duplicate_pools(page_data["pools"])

    # Resolve any hex/formid-looking item names (no-op for event pages today).
    for pool in page_data["pools"]:
        for item in pool.get("items", []):
            fid = item.get("formid", "")
            name = item.get("name", "")
            if name == fid or (len(name) == 8 and _HEX8.match(name)):
                resolved = resolve_item_name(fid, item.get("edid", ""), drop_rate_pools)
                if resolved != fid:
                    item["name"] = resolved

    # Daily Ops timer-tier meta.
    if config["pageType"] == "dailyops":
        page_data["meta"] = {
            "timerTiers": {
                "elder": {"seconds": 480, "label": "Elder (≤ 8:00)"},
                "paladin": {"seconds": 720, "label": "Paladin (≤ 12:00)"},
                "knight": {"seconds": 960, "label": "Knight (≤ 16:00)"},
            }
        }

    # Raid speedrun meta.
    if config["pageType"] == "raid" and "speedrunSeconds" in config:
        page_data.setdefault("meta", {})
        page_data["meta"]["speedrunSeconds"] = config["speedrunSeconds"]
        page_data["meta"]["speedrunLabel"] = format_seconds(config["speedrunSeconds"])

    return page_data
