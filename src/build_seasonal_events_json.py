#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_seasonal_events_json.py - Seasonal Events Rewards (Rewrite April 2026)

Builds:
  dist/seasonal_events/seasonal_events_rewards_by_page.json

Uses the shared rng76 engine (Rng76Data.from_tsv_root) for LVLI resolution.
Each event produces a flat combined reward list with per-item drop rates
across all tiers/containers.

Output schema per event page:
  {
    "name":           "Halloween Scorched",
    "description":    "Take down Spooky Scorched to earn...",
    "slug":           "halloween-scorched-all-rewards",
    "eventSlug":      "halloween-scorched",
    "isContainerLoot": true,
    "baseRewards":    { "food": [...], "goodies": [...], "other": [...] },
    "rewards": [
      {
        "name":         "Plan: Alien Jack O'Lantern",
        "formId":       "00620123",
        "edid":         "Plan_Halloween_JackOLantern_Alien",
        "imageUrl":     "https://.../{slug}/{item-slug}.avif",
        "releaseYear":  "2022",
        "tradeable":    true,
        "isTrackable":  true,
        "howToObtain":  "<strong>Source:</strong> Spooky Treat Bag",
        "dropRates":    [ {"tier": "Spooky Treat Bag", "rate": "5.26%"} ],
        "group":        null
      }
    ],
    "groups":  null | [ {"key": "cook", "label": "Meat-Cook Rewards"}, ... ],
    "gallery": []
  }

Excludes:
  - Drifter Activation Card (P62_LLS_Rewards_TheDrifter_ActivationKeyCard)
  - Any EDID starting with zzz_, CUT_, POST_, DEL_, P62_

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
# Exclusions
# ---------------------------------------------------------------------------
_EXCLUDE_RE = re.compile(r"^(zzz_|CUT_|POST_|DEL_|P62_)", re.IGNORECASE)
_DRIFTER_EDID = "P62_LLS_Rewards_TheDrifter_ActivationKeyCard"

IMAGE_BASE = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/seasonal-events/"

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
# Helpers
# ---------------------------------------------------------------------------

def is_excluded(edid):
    """Check if an EDID should be excluded from output."""
    if not edid:
        return False
    if _EXCLUDE_RE.match(edid):
        return True
    if _DRIFTER_EDID.lower() in edid.lower():
        return True
    return False


def is_trackable(name):
    """Only plan: and recipe: items get checkboxes."""
    lower = (name or "").lower()
    return lower.startswith("plan:") or lower.startswith("recipe:")


def slugify_item(name):
    """Convert item name to URL-safe slug for image path."""
    s = (name or "").lower()
    s = re.sub(r"^(plan|recipe):\s*", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_image_url(event_slug, item_name):
    """Build the expected image URL for an item."""
    return IMAGE_BASE + event_slug + "/" + slugify_item(item_name) + ".avif"


# ---------------------------------------------------------------------------
# GMRW -> LVLI mapping
# ---------------------------------------------------------------------------

def get_gmrw_lvlis_for_quest(quest_formid, gmrw_rows):
    """
    Find all GMRW rows linked to a quest and return their reward LVLI FormIDs.
    The GMRW TSV has one row per rewarded item. We look for rows where:
      - ParentQuestLink starts with the quest FormID
      - RewardedItem ends with :LVLI (is a leveled item list, not ALCH/CNCY/etc)
      - EDID is not excluded

    Returns list of {"title", "lvliFormID", "tier"} - deduplicated by LVLI FormID.
    """
    seen_lvlis = set()
    results = []

    for row in gmrw_rows:
        parent_raw = pick(row, "ParentQuestLink", "Parent Quest", default="")
        parent_fid = parent_raw.split(":")[0].strip() if parent_raw else ""
        if parent_fid != quest_formid:
            continue

        edid = pick(row, "EDID", "GMRW_EDID", default="")
        if is_excluded(edid):
            continue

        # RewardedItem format: "FormID:EDID:SIG" - we want only LVLI items
        rewarded = pick(row, "RewardedItem", default="")
        if not rewarded:
            continue
        parts = rewarded.split(":")
        if len(parts) < 3 or parts[-1].strip().upper() != "LVLI":
            continue

        lvli_fid = parts[0].strip()
        lvli_edid = parts[1].strip() if len(parts) > 1 else ""

        if not lvli_fid or lvli_fid in seen_lvlis:
            continue
        if is_excluded(lvli_edid):
            continue

        seen_lvlis.add(lvli_fid)

        tier = pick(row, "TierLabel", "Tier", "TierName", default="")
        title = humanize_edid(lvli_edid) if lvli_edid else humanize_edid(edid)

        results.append({
            "title": title,
            "lvliFormID": lvli_fid,
            "tier": tier,
        })

    return results


# ---------------------------------------------------------------------------
# Main Build
# ---------------------------------------------------------------------------

def main():
    print("[build_seasonal_events] Loading rng76 engine...")
    data = Rng76Data.from_tsv_root(TSV_ROOT)
    resolver = data.resolver

    # Load GMRW separately for quest->reward mapping
    gmrw_path = newest(str(_REPO_ROOT / "tsv" / "GMRW_Export_*.tsv"))
    gmrw_rows = read_tsv(gmrw_path)
    print("[build_seasonal_events] Loaded %d GMRW rows" % len(gmrw_rows))

    # Output structure
    output = {"byPage": {}}

    for slug, event_def in EVENTS.items():
        ev_name = event_def["name"]
        ev_slug = event_def["eventSlug"]
        print("\n[build_seasonal_events] Processing: %s (%s)" % (ev_name, slug))

        page_data = {
            "name": ev_name,
            "description": event_def["description"],
            "slug": slug,
            "eventSlug": ev_slug,
            "isContainerLoot": event_def["isContainerLoot"],
            "baseRewards": None,
            "rewards": [],
            "groups": event_def.get("groups"),
            "gallery": [],
        }

        # -- Resolve rewards --
        reward_sources = []  # list of (title, lvli_formid, group_key)

        if event_def["isContainerLoot"]:
            # Container events: use the defined container LVLIs
            for container in event_def.get("containers", []):
                reward_sources.append((
                    container["title"],
                    container["lvliFormID"],
                    None,
                ))
        else:
            # Quest events: find GMRW -> LVLI chains
            groups = event_def.get("groups")
            for quest_fid in event_def.get("questFormIDs", []):
                gmrw_lvlis = get_gmrw_lvlis_for_quest(quest_fid, gmrw_rows)
                if not gmrw_lvlis:
                    print("  [WARN] No GMRW LVLIs found for quest %s" % quest_fid)
                    continue

                # Determine group key if applicable
                group_key = None
                if groups:
                    for g in groups:
                        if g["questFormID"] == quest_fid:
                            group_key = g["key"]
                            break

                for entry in gmrw_lvlis:
                    reward_sources.append((
                        entry["title"],
                        entry["lvliFormID"],
                        group_key,
                    ))

        if not reward_sources:
            print("  [WARN] No reward sources found, skipping")
            output["byPage"][slug] = page_data
            continue

        # -- Resolve all LVLIs and merge into combined reward list --
        # Two-pass: first accumulate raw rates, then format and filter.
        # Structure: merged_rewards[formid] = {..., "_rates": {source_title: float}}
        merged_rewards = {}  # formid -> {...}

        for source_title, lvli_fid, group_key in reward_sources:
            print("  Resolving LVLI %s (%s)..." % (lvli_fid, source_title))
            try:
                items = resolver.resolve_deep(lvli_fid)
            except Exception as e:
                print("    [ERROR] resolve_deep failed: %s" % str(e))
                continue

            print("    -> %d leaf items" % len(items))

            for item in items:
                fid = item.get("formid", "")
                edid = item.get("edid", "")
                name = item.get("name", "")
                rate = item.get("dropRate", 0.0)

                if is_excluded(edid):
                    continue
                if not name or not fid:
                    continue

                if fid not in merged_rewards:
                    merged_rewards[fid] = {
                        "name": name,
                        "formId": fid,
                        "edid": edid,
                        "imageUrl": build_image_url(ev_slug, name),
                        "releaseYear": None,
                        "tradeable": None,
                        "isTrackable": is_trackable(name),
                        "howToObtain": "<strong>Source:</strong> " + source_title,
                        "_rates": {},  # source_title -> accumulated float
                        "group": group_key,
                    }

                # Accumulate rate per source (same item can appear in
                # multiple branches of the same LVLI tree)
                # resolve_deep returns decimal fractions (0.07 = 7%), accumulate as-is
                rates = merged_rewards[fid]["_rates"]
                rates[source_title] = rates.get(source_title, 0.0) + rate

                # If item appears in multiple groups, note first group
                if group_key and not merged_rewards[fid]["group"]:
                    merged_rewards[fid]["group"] = group_key

        # -- Format rates and filter out items with all-zero rates --
        # resolve_deep returns decimals (0.07 = 7%). Convert to % for fmt_pct.
        MIN_RATE = 0.0001  # Filter items below 0.01% (as decimal: 0.0001 = 0.01%)
        for fid in list(merged_rewards.keys()):
            entry = merged_rewards[fid]
            raw_rates = entry.pop("_rates")
            # Check if any rate exceeds threshold
            if not any(v >= MIN_RATE for v in raw_rates.values()):
                del merged_rewards[fid]
                continue
            # Build formatted dropRates list — multiply by 100 for fmt_pct
            entry["dropRates"] = [
                {"tier": tier, "rate": fmt_pct(val * 100)}
                for tier, val in raw_rates.items()
                if val >= MIN_RATE
            ]

        # -- Convert to sorted list --
        rewards_list = sorted(merged_rewards.values(), key=lambda r: r["name"].lower())
        page_data["rewards"] = rewards_list
        trackable_count = sum(1 for r in rewards_list if r["isTrackable"])
        print("  Total unique rewards: %d" % len(rewards_list))
        print("  Trackable (plans/recipes): %d" % trackable_count)

        # -- Store with multiple key variants for URL matching --
        output["byPage"][slug] = page_data
        url_path = "/df/seasonal-events/" + ev_slug + "/" + slug + "/"
        output["byPage"][url_path] = page_data
        output["byPage"][url_path.rstrip("/")] = page_data

    # -- Write output --
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DIST_DIR / "seasonal_events_rewards_by_page.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print("[build_seasonal_events] Written: %s" % out_path)
    print("[build_seasonal_events] File size: %d bytes" % out_path.stat().st_size)


if __name__ == "__main__":
    main()
