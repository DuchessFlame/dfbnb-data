#!/usr/bin/env python3
"""
Build REHO (Raid Expo Hunts Ops) reward checklist JSON.

Generates dist/reho/reho_rewards_by_page.json with reward data for 10 specific
Fallout 76 reward checklist pages, reading from:
- dist/events/events_rewards.json (for Daily Ops, Expos, Raids)
- dist/drop_rates.json (for Bounty Hunts — pre-computed by build_drop_rates.py)

Usage:
  python3 build_reho_json.py

Requires: build_drop_rates.py and build_events_rewards_json.py to run first.
"""

import json
import csv
import os
import re
import glob
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple


class REHOBuilder:
    """Build REHO reward checklist JSON."""

    # Page mappings: slug -> (quest_name, quest_form_id, page_type, path)
    PAGE_MAPPINGS = {
        "daily-ops-reward-checklist": {
            "name": "Daily Ops",
            "questName": "Daily Ops",
            "questFormID": "005A77D4",
            "pageType": "dailyops",
            "path": "/df/daily-ops/daily-ops-reward-checklist",
            "timerGlobs": {
                "elder": ("005CB976", 480),
                "paladin": ("005CB977", 720),
                "knight": ("005CB978", 960),
            },
        },
        "atlantic-city-expos-reward-checklist": {
            "name": "Atlantic City Expos",
            "questName": "Atlantic City Expos",
            "questFormID": "006BAA3D",
            "pageType": "expedition",
            "path": "/df/expos/atlantic-city/atlantic-city-expos-reward-checklist",
        },
        "pitt-expos-reward-checklist": {
            "name": "The Pitt Expos",
            "questName": "The Pitt Expos",
            "questFormID": "006274EC",
            "pageType": "expedition",
            "path": "/df/expos/the-pitt/pitt-expos-reward-checklist",
        },
        "gleaming-depths-stage-1-reward-checklist": {
            "name": "Gleaming Depths Stage 1",
            "questName": "Gleaming Depths Stage 1",
            "questFormID": "00772A47",
            "pageType": "raid",
            "path": "/df/raids/gleaming-depths/gleaming-depths-stage-1-reward-checklist",
            "stage": 1,
            "speedrunSeconds": 148,
        },
        "gleaming-depths-stage-2-reward-checklist": {
            "name": "Gleaming Depths Stage 2",
            "questName": "Gleaming Depths Stage 2",
            "questFormID": "0078F7A1",
            "pageType": "raid",
            "path": "/df/raids/gleaming-depths/gleaming-depths-stage-2-reward-checklist",
            "stage": 2,
            "speedrunSeconds": 220,
        },
        "gleaming-depths-stage-3-reward-checklist": {
            "name": "Gleaming Depths Stage 3",
            "questName": "Gleaming Depths Stage 3",
            "questFormID": "0078B59E",
            "pageType": "raid",
            "path": "/df/raids/gleaming-depths/gleaming-depths-stage-3-reward-checklist",
            "stage": 3,
            "speedrunSeconds": 139,
        },
        "gleaming-depths-stage-4-reward-checklist": {
            "name": "Gleaming Depths Stage 4",
            "questName": "Gleaming Depths Stage 4",
            "questFormID": "00788127",
            "pageType": "raid",
            "path": "/df/raids/gleaming-depths/gleaming-depths-stage-4-reward-checklist",
            "stage": 4,
            "speedrunSeconds": 254,
        },
        "gleaming-depths-stage-5-reward-checklist": {
            "name": "Gleaming Depths Stage 5",
            "questName": "Gleaming Depths Stage 5",
            "questFormID": "00786D41",
            "pageType": "raid",
            "path": "/df/raids/gleaming-depths/gleaming-depths-stage-5-reward-checklist",
            "stage": 5,
            "speedrunSeconds": 134,
        },
        "grunt-hunt-rewards": {
            "name": "Bounty Hunting: Grunt Hunt",
            "questName": "Burn_BountyHunt_GruntHunt",
            "questFormID": "007D6A80",
            "pageType": "bountyhunt",
            "path": "/df/bounty-hunting/grunt-hunts/grunt-hunt-rewards",
            "gmrwFormID": "007D6A81",
            "lvliFormID": "007D6A6D",
            "xpGlobID": "007D6A68",
            "capsGlobID": "007D6A67",
        },
        "head-hunt-rewards": {
            "name": "Bounty Hunting: Head Hunt",
            "questName": "Burn_BountyHunt_Headhunt",
            "questFormID": "007EBDF4",
            "pageType": "bountyhunt",
            "path": "/df/bounty-hunting/head-hunts/head-hunt-rewards",
            "gmrwFormID": "007EBDEE",
            "lvliFormID": "007EBDF3",
            "xpGlobID": "007EBDED",
            "capsGlobID": "007D6A67",
        },
    }

    # Pool title replacements for human-readable names
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

    def __init__(self):
        """Initialize the REHO builder."""
        self.base_path = Path.cwd()
        self.tsv_path = self.base_path / "tsv"
        self.dist_path = self.base_path / "dist"
        self.events_rewards_path = self.dist_path / "events" / "events_rewards.json"
        self.drop_rates_path = self.dist_path / "drop_rates.json"
        self.events_data = {}
        self.tiers_map = {}
        self.quest_data = {}
        self.gmrw_data = {}

        # Pre-computed drop rates from build_drop_rates.py
        self.drop_rates = {}
        self.globs = {}

    def run(self):
        """Build the REHO JSON file."""
        print("Building REHO reward checklist JSON...")

        # Load shared drop rates JSON (replaces all LVLI/GLOB/item-name loading)
        self._load_drop_rates()

        # Load events rewards data
        self._load_events_rewards()

        # Load minimal QUEST/GMRW data for bounty hunt metadata
        self._load_bounty_hunt_metadata()

        # Build output structure
        output = {"byPage": {}}

        for slug, config in self.PAGE_MAPPINGS.items():
            print(f"Processing {slug}...")

            if config["pageType"] in ("dailyops", "expedition", "raid"):
                page_data = self._build_from_events(slug, config)
            else:  # bountyhunt
                page_data = self._build_bountyhunt_page(slug, config)

            if page_data:
                output["byPage"][slug] = page_data
                output["byPage"][config["path"]] = page_data

        # Write output
        output_dir = self.dist_path / "reho"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "reho_rewards_by_page.json"

        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)

        print(f"✓ Wrote {output_file}")
        print(f"✓ Total pages: {len(output['byPage']) // 2}")

    # ------------------------------------------------------------------
    # DATA LOADING (slimmed — uses shared drop_rates.json)
    # ------------------------------------------------------------------

    def _load_drop_rates(self):
        """Load the shared drop_rates.json produced by build_drop_rates.py."""
        if not self.drop_rates_path.exists():
            print(f"  Warning: {self.drop_rates_path} not found — bounty hunts will have no items")
            return

        with open(self.drop_rates_path) as f:
            data = json.load(f)

        self.drop_rates = data.get("pools", {})
        self.globs = data.get("globs", {})
        print(f"  Loaded drop_rates.json: {len(self.drop_rates)} pools, {len(self.globs)} globs")

    def _load_events_rewards(self):
        """Load the events_rewards.json file."""
        if not self.events_rewards_path.exists():
            raise FileNotFoundError(f"{self.events_rewards_path} not found")

        with open(self.events_rewards_path) as f:
            data = json.load(f)

        # Index events by name and questFormID
        for event in data.get("events", []):
            self.events_data[event["questFormID"]] = event
            self.events_data[event["name"]] = event

    def _load_bounty_hunt_metadata(self):
        """Load minimal QUEST/GMRW data for bounty hunt page metadata."""
        def read_tsv(path):
            try:
                with open(path, encoding="utf-8-sig", newline="") as f:
                    return list(csv.DictReader(f, delimiter="\t"))
            except UnicodeDecodeError:
                with open(path, encoding="cp1252", errors="replace", newline="") as f:
                    return list(csv.DictReader(f, delimiter="\t"))

        # Load QUEST data (only bounty hunt quests)
        quest_files = glob.glob(str(self.tsv_path / "QUEST_Export_*.tsv"))
        if quest_files:
            quest_files.sort(key=lambda x: os.path.getmtime(x))
            for row in read_tsv(quest_files[-1]):
                formid = (row.get("FormID") or "").strip()
                if formid in ("007D6A80", "007EBDF4"):
                    self.quest_data[formid] = row

        # Load GMRW data (only bounty hunt GMRW)
        gmrw_files = glob.glob(str(self.tsv_path / "GMRW_Export_*.tsv"))
        if gmrw_files:
            gmrw_files.sort(key=lambda x: os.path.getmtime(x))
            for row in read_tsv(gmrw_files[-1]):
                formid = (row.get("FormID") or "").strip()
                if formid in ("007D6A81", "007EBDEE"):
                    self.gmrw_data[formid] = row

    # ------------------------------------------------------------------
    # ITEM RESOLUTION (from shared JSON instead of raw TSV)
    # ------------------------------------------------------------------

    def _resolve_lvli_items_from_json(self, lvli_formid: str) -> List[Dict[str, Any]]:
        """
        Look up pre-resolved LVLI items from drop_rates.json.

        Replaces the old _resolve_lvli_items() which duplicated the full
        rng-76 LVLI tree walker. Items come back with 0-1 drop rates.
        """
        pool = self.drop_rates.get(lvli_formid)
        if not pool:
            return []

        return [
            {
                "formid":   item["formid"],
                "name":     item["name"],
                "qty":      item.get("qty", 1),
                "dropRate": item["dropRate"],
                "edid":     item.get("edid", ""),
                "sig":      item.get("sig", ""),
            }
            for item in pool.get("items", [])
        ]

    def _resolve_item_name(self, formid: str, edid: str, sig: str) -> str:
        """Resolve a FormID to a display name using drop_rates.json pools."""
        # Check all pools for this formid in their items
        for pool in self.drop_rates.values():
            for item in pool.get("items", []):
                if item.get("formid") == formid:
                    name = item.get("name", "")
                    if name and name != formid:
                        return name
        if edid:
            return edid
        return formid

    # ------------------------------------------------------------------
    # EVENT-BASED PAGES (unchanged — reads from events_rewards.json)
    # ------------------------------------------------------------------

    def _build_from_events(self, slug: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build page data from existing events_rewards.json."""
        quest_id = config["questFormID"]

        if quest_id not in self.events_data:
            print(f"  Warning: Quest {quest_id} not found in events_rewards.json")
            return {}

        event = self.events_data[quest_id]

        page_data = {
            "name": config["name"],
            "pageType": config["pageType"],
            "questFormID": quest_id,
            "gameName": event.get("gameName", ""),
            "description": event.get("description", ""),
            "pools": [],
            "baseRewards": {
                "xp": "varies",
                "caps": "varies",
            },
        }

        # Process pools
        for pool in event.get("pools", []):
            pool_data = self._process_pool(pool, config)
            if pool_data:
                page_data["pools"].append(pool_data)

        # Merge duplicate pools (same lvliFormID, mutually exclusive conditions)
        page_data["pools"] = self._merge_duplicate_pools(page_data["pools"])

        # Resolve KEYM item names in existing pools (e.g. Activation Keycard)
        for pool in page_data["pools"]:
            for item in pool.get("items", []):
                fid = item.get("formid", "")
                name = item.get("name", "")
                if name == fid or (len(name) == 8 and all(c in '0123456789ABCDEFabcdef' for c in name)):
                    resolved = self._resolve_item_name(fid, item.get("edid", ""), "")
                    if resolved != fid:
                        item["name"] = resolved

        # Add metadata for Daily Ops with timer tiers
        if config["pageType"] == "dailyops":
            page_data["meta"] = {
                "timerTiers": {
                    "elder": {"seconds": 480, "label": "Elder (≤ 8:00)"},
                    "paladin": {"seconds": 720, "label": "Paladin (≤ 12:00)"},
                    "knight": {"seconds": 960, "label": "Knight (≤ 16:00)"},
                }
            }

        # Add speedrun metadata for raids
        if config["pageType"] == "raid" and "speedrunSeconds" in config:
            if "meta" not in page_data:
                page_data["meta"] = {}
            page_data["meta"]["speedrunSeconds"] = config["speedrunSeconds"]
            page_data["meta"]["speedrunLabel"] = self._format_seconds(config["speedrunSeconds"])

        return page_data

    def _process_pool(self, pool: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single reward pool."""
        title = pool.get("title", "")
        clean_title = self.POOL_TITLE_REPLACEMENTS.get(title, title)

        tier = ""
        if "Elder" in clean_title:
            tier = "elder"
        elif "Paladin" in clean_title:
            tier = "paladin"
        elif "Knight" in clean_title:
            tier = "knight"

        pool_data = {
            "title": clean_title,
            "poolId": self._generate_pool_id(clean_title),
            "lvliFormID": pool.get("lvliFormID", ""),
            "lvliEdid": pool.get("lvliEdid", ""),
            "tier": tier,
            "poolChance": pool.get("poolChance", 100.0),
            "conditions": pool.get("conditions", []),
            "conditionSummary": self._summarize_conditions(pool.get("conditions", []), config),
            "items": pool.get("items", []),
            "itemCount": len(pool.get("items", [])),
        }

        return pool_data

    def _generate_pool_id(self, title: str) -> str:
        pool_id = re.sub(r'[^\w\s-]', '', title.lower())
        pool_id = re.sub(r'[\s-]+', '-', pool_id)
        return pool_id.strip('-')

    def _summarize_conditions(self, conditions: List[str], config: Dict[str, Any]) -> str:
        if not conditions or conditions == ["GetItemCount"]:
            return ""

        summaries = []
        for condition in conditions:
            if not condition or condition == "GetItemCount":
                continue
            if "GetRemainingQuestTimeSeconds" in condition:
                tier = self._extract_tier_from_title(config)
                if tier:
                    summaries.append(f"Complete in under {self._format_seconds(self._get_tier_seconds(config, tier))}")
            elif "IsActivePlayer" in condition and "1.000000" in condition:
                summaries.append("Must be an active participant")
            elif "HasLearnedRecipe" in condition and "0.000000" in condition:
                summaries.append("Must not already know the recipe")
            elif "GetExpeditionsInstanceNumOptbjectivesCompleted" in condition:
                summaries.append("Complete all optional objectives")
            elif "GetRandomPercent" in condition:
                summaries.append("Chance-based drop")

        return " | ".join(summaries) if summaries else ""

    def _extract_tier_from_title(self, config: Dict[str, Any]) -> Optional[str]:
        if "timerGlobs" in config:
            return None
        return None

    def _get_tier_seconds(self, config: Dict[str, Any], tier: str) -> int:
        if "timerGlobs" in config and tier in config["timerGlobs"]:
            return config["timerGlobs"][tier][1]
        return 0

    def _format_seconds(self, seconds: int) -> str:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}:{secs:02d}"

    def _merge_duplicate_pools(self, pools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge pools with the same lvliFormID (mutually exclusive conditions)."""
        seen = {}
        merged = []

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

    # ------------------------------------------------------------------
    # BOUNTY HUNT PAGES (slimmed — uses drop_rates.json)
    # ------------------------------------------------------------------

    def _build_bountyhunt_page(self, slug: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build a bounty hunt page using pre-computed drop rates."""
        quest_id = config["questFormID"]
        gmrw_id = config["gmrwFormID"]
        root_lvli_id = config["lvliFormID"]

        quest_info = self.quest_data.get(quest_id, {})
        quest_name = quest_info.get("FULL - Name", config["name"])
        quest_desc = quest_info.get("DESC - Description", "")

        gmrw_info = self.gmrw_data.get(gmrw_id, {})

        # XP and Caps from shared globs
        xp_value = "varies"
        caps_value = "5000"

        xp_glob_id = config.get("xpGlobID")
        if xp_glob_id and xp_glob_id in self.globs:
            xp_value = str(int(self.globs[xp_glob_id].get("value", 0))) or "varies"

        caps_glob_id = config.get("capsGlobID")
        if caps_glob_id and caps_glob_id in self.globs:
            caps_value = str(int(self.globs[caps_glob_id].get("value", 5000)))

        # Get items from shared drop_rates.json (replaces _resolve_lvli_items)
        lvli_items = self._resolve_lvli_items_from_json(root_lvli_id)

        items_list = []
        for item in lvli_items:
            dr = item["dropRate"]
            dr_pct = round(dr * 100, 2) if dr <= 1.0 else round(dr, 2)
            items_list.append({
                "formid": item["formid"],
                "name": item["name"],
                "qty": item["qty"],
                "dropRate": dr_pct,
                "dropRatePercent": f"{dr_pct}%",
                "edid": item["edid"],
                "sig": item["sig"],
            })

        page_data = {
            "name": config["name"],
            "pageType": config["pageType"],
            "questFormID": quest_id,
            "gameName": quest_name,
            "description": quest_desc,
            "pools": [
                {
                    "title": config["name"] + " Rewards",
                    "poolId": self._generate_pool_id(config["name"] + " Rewards"),
                    "lvliFormID": root_lvli_id,
                    "lvliEdid": gmrw_info.get("LVLI_List_EDID", ""),
                    "tier": "",
                    "poolChance": 100.0,
                    "conditions": [],
                    "conditionSummary": "Varies by star level",
                    "items": items_list,
                    "itemCount": len(items_list),
                }
            ],
            "baseRewards": {
                "xp": xp_value,
                "caps": caps_value,
            },
            "meta": {
                "gmrwFormID": gmrw_id,
                "xpGlobID": config.get("xpGlobID", ""),
                "capsGlobID": config.get("capsGlobID", ""),
            },
        }

        return page_data


def main():
    """Main entry point."""
    builder = REHOBuilder()
    builder.run()


if __name__ == "__main__":
    main()
