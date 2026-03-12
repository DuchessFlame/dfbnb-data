#!/usr/bin/env python3
"""
Build REHO (Raid Expo Hunts Ops) reward checklist JSON.

Generates dist/reho/reho_rewards_by_page.json with reward data for 10 specific
Fallout 76 reward checklist pages, reading from:
- dist/events/events_rewards.json (for Daily Ops, Expos, Raids)
- TSV files in tsv/ symlink (for Bounty Hunts)

Usage:
  python3 build_reho_json.py

Run from /sessions/intelligent-great-thompson/mnt/tsv/
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
        self.events_data = {}
        self.tiers_map = {}
        self.quest_data = {}
        self.gmrw_data = {}
        self.glob_data = {}

        # LVLI resolution data for bounty hunts
        self.lvli_list_data = {}
        self.lvli_entries_data = defaultdict(list)
        self.lvli_math_data = {}
        self.book_names = {}
        self.misc_names = {}
        self.keym_names = {}
        self.lvli_cache = {}

    def run(self):
        """Build the REHO JSON file."""
        print("Building REHO reward checklist JSON...")

        # Load events rewards data
        self._load_events_rewards()

        # Load TSV data for bounty hunts
        self._load_bounty_hunt_data()

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

    def _load_bounty_hunt_data(self):
        """Load TSV data needed for bounty hunts."""
        # Load QUEST data
        quest_file = self.tsv_path / "QUEST_Export_March_2026.tsv"
        if quest_file.exists():
            with open(quest_file, encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    formid = row.get("FormID", "").strip()
                    if formid in ("007D6A80", "007EBDF4"):
                        self.quest_data[formid] = row

        # Load GMRW data
        gmrw_file = self.tsv_path / "GMRW_Export_March_2026.tsv"
        if gmrw_file.exists():
            with open(gmrw_file, encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    formid = row.get("FormID", "").strip()
                    if formid in ("007D6A81", "007EBDEE"):
                        self.gmrw_data[formid] = row

        # Load GLOB data for XP and Caps
        glob_file = self.tsv_path / "GLOB_Export_March_2026.tsv"
        if glob_file.exists():
            with open(glob_file, encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    formid = row.get("FormID", "").strip()
                    if formid in ("007D6A68", "007EBDED", "007D6A67"):
                        self.glob_data[formid] = row

        # Load LVLI data for bounty hunt item resolution
        self._load_lvli_data()
        # Load item name data (BOOK, MISC, KEYM)
        self._load_item_names()

    def _load_lvli_data(self):
        """Load LVLI TSV data for bounty hunt resolution."""
        def read_tsv(path):
            try:
                with open(path, encoding="utf-8-sig", newline="") as f:
                    return list(csv.DictReader(f, delimiter="\t"))
            except UnicodeDecodeError:
                with open(path, encoding="cp1252", errors="replace", newline="") as f:
                    return list(csv.DictReader(f, delimiter="\t"))

        # Load LVLI List data
        lvli_list_files = glob.glob(str(self.tsv_path / "LVLI_Export_*_LVLI_List.tsv"))
        if lvli_list_files:
            lvli_list_file = sorted(lvli_list_files)[-1]
            for row in read_tsv(lvli_list_file):
                formid = row.get("FormID", "").strip()
                if formid:
                    self.lvli_list_data[formid] = row

        # Load LVLI Entries data
        lvli_entries_files = glob.glob(str(self.tsv_path / "LVLI_Export_*_LVLI_Entries.tsv"))
        if lvli_entries_files:
            lvli_entries_file = sorted(lvli_entries_files)[-1]
            for row in read_tsv(lvli_entries_file):
                formid = row.get("LVLI_FormID", "").strip()
                if formid:
                    self.lvli_entries_data[formid].append(row)

        # Load LVLI Math data
        lvli_math_files = glob.glob(str(self.tsv_path / "LVLI_Export_*_LVLI_Math.tsv"))
        if lvli_math_files:
            lvli_math_file = sorted(lvli_math_files)[-1]
            for row in read_tsv(lvli_math_file):
                lvli_fid = row.get("LVLI_FormID", "").strip()
                entry_idx = row.get("EntryIndex", "").strip()
                if lvli_fid and entry_idx:
                    key = (lvli_fid, entry_idx)
                    self.lvli_math_data[key] = row

    def _load_item_names(self):
        """Load item name data from BOOK, MISC, and KEYM exports."""
        def read_tsv(path):
            try:
                with open(path, encoding="utf-8-sig", newline="") as f:
                    return list(csv.DictReader(f, delimiter="\t"))
            except UnicodeDecodeError:
                with open(path, encoding="cp1252", errors="replace", newline="") as f:
                    return list(csv.DictReader(f, delimiter="\t"))

        # Load BOOK names
        book_files = glob.glob(str(self.tsv_path / "BOOK_Export_*.tsv"))
        if book_files:
            book_file = sorted(book_files)[-1]
            for row in read_tsv(book_file):
                formid = row.get("FormID", "").strip()
                full = row.get("FULL - Name", row.get("FULL", "")).strip()
                if formid and full:
                    self.book_names[formid] = full

        # Load MISC names
        misc_files = glob.glob(str(self.tsv_path / "MISC_Export_*.tsv"))
        if misc_files:
            misc_file = sorted(misc_files)[-1]
            for row in read_tsv(misc_file):
                formid = row.get("FormID", "").strip()
                full = row.get("FULL - Name", row.get("FULL", "")).strip()
                if formid and full:
                    self.misc_names[formid] = full

        # Load KEYM names (keys like Activation Keycard)
        all_keym = glob.glob(str(self.tsv_path / "KEYM_Export_*.tsv"))
        keym_files = [f for f in all_keym
                      if not any(s in os.path.basename(f) for s in ("_Locations", "_Refs", "_KYWD"))]
        if keym_files:
            keym_file = sorted(keym_files)[-1]
            for row in read_tsv(keym_file):
                formid = row.get("FormID", "").strip()
                full = row.get("FULL - Name", row.get("FULL", "")).strip()
                if formid and full:
                    self.keym_names[formid] = full

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

    def _resolve_lvli_items(self, lvli_formid: str, depth: int = 0, seen: Optional[set] = None) -> List[Dict[str, Any]]:
        """Resolve LVLI FormID to a list of items with drop rates and names."""
        if seen is None:
            seen = set()
        if lvli_formid in seen or depth > 50:
            return []
        seen = seen | {lvli_formid}

        if lvli_formid in self.lvli_cache:
            return self.lvli_cache[lvli_formid]

        items = []
        entries = self.lvli_entries_data.get(lvli_formid, [])

        for entry in entries:
            idx = entry.get("EntryIndex", "").strip()
            if not idx:
                continue

            math = self.lvli_math_data.get((lvli_formid, idx))
            if not math:
                continue

            ref = entry.get("LVLO_Reference", "").strip()
            if not ref:
                continue

            list_none = float(math.get("ListChanceNoneResolved", 0) or 0)
            entry_pres = float(math.get("EntryPresenceChance", 1) or 1)
            entry_none = float(math.get("EntryChanceNoneResolved", 0) or 0)

            cond_rand = 1.0
            cond_str = entry.get("Cond1", "").strip() if entry else ""
            if "GetRandomPercent" in cond_str:
                match = re.search(r"GetRandomPercent.*?(\d+(?:\.\d+)?)", cond_str)
                if match:
                    try:
                        pct = float(match.group(1))
                        cond_rand = max(0, min(100, pct)) / 100.0
                    except (ValueError, AttributeError):
                        cond_rand = 1.0

            apriori = float(math.get("EntryAprioriChance_NoSublist", 1) or 1)

            list_none = max(0, min(1, list_none))
            entry_none = max(0, min(1, entry_none))
            drop_rate = (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori
            drop_rate = max(0, drop_rate)

            qty_raw = entry.get("LVIV_Quantity", entry.get("LVLO_Count", entry.get("Count", "1"))).strip()
            try:
                qty = int(float(qty_raw))
            except (ValueError, TypeError):
                qty = 1

            ref_parts = ref.split(":")
            if len(ref_parts) < 1:
                continue

            fid = ref_parts[0].strip()
            edid = ref_parts[1].strip() if len(ref_parts) > 1 else ""
            sig = ref_parts[2].upper().strip() if len(ref_parts) > 2 else ""

            sub_lvli_fid = math.get("SubLVLI_FormID", "").strip()
            if sub_lvli_fid:
                sub_items = self._resolve_lvli_items(sub_lvli_fid, depth + 1, seen)
                for sub_item in sub_items:
                    items.append({
                        "formid": sub_item["formid"],
                        "name": sub_item["name"],
                        "qty": sub_item["qty"],
                        "dropRate": sub_item["dropRate"] * drop_rate,
                        "edid": sub_item["edid"],
                        "sig": sub_item["sig"],
                    })
            else:
                name = self._resolve_item_name(fid, edid, sig)
                items.append({
                    "formid": fid,
                    "name": name,
                    "qty": qty,
                    "dropRate": drop_rate,
                    "edid": edid,
                    "sig": sig,
                })

        total_rate = sum(item["dropRate"] for item in items) if items else 0
        if total_rate > 1.001 and items:
            for item in items:
                item["dropRate"] = item["dropRate"] / total_rate if total_rate > 0 else 0

        self.lvli_cache[lvli_formid] = items
        return items

    def _resolve_item_name(self, formid: str, edid: str, sig: str) -> str:
        """Resolve a FormID to a display name."""
        if formid in self.misc_names:
            return self.misc_names[formid]
        if formid in self.book_names:
            return self.book_names[formid]
        if formid in self.keym_names:
            return self.keym_names[formid]
        if edid:
            return edid
        return formid

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

    def _build_bountyhunt_page(self, slug: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build a bounty hunt page from TSV data."""
        quest_id = config["questFormID"]
        gmrw_id = config["gmrwFormID"]
        root_lvli_id = config["lvliFormID"]

        quest_info = self.quest_data.get(quest_id, {})
        quest_name = quest_info.get("FULL - Name", config["name"])
        quest_desc = quest_info.get("DESC - Description", "")

        gmrw_info = self.gmrw_data.get(gmrw_id, {})

        xp_value = "varies"
        caps_value = "5000"

        xp_glob_id = config.get("xpGlobID")
        if xp_glob_id and xp_glob_id in self.glob_data:
            xp_info = self.glob_data[xp_glob_id]
            xp_value = xp_info.get("Value", "varies")

        caps_glob_id = config.get("capsGlobID")
        if caps_glob_id and caps_glob_id in self.glob_data:
            caps_info = self.glob_data[caps_glob_id]
            caps_value = caps_info.get("Value", "5000")

        lvli_items = self._resolve_lvli_items(root_lvli_id)

        items_list = []
        for item in lvli_items:
            items_list.append({
                "formid": item["formid"],
                "name": item["name"],
                "qty": item["qty"],
                "dropRate": round(item["dropRate"] * 100, 2),
                "dropRatePercent": f"{round(item['dropRate'] * 100, 2)}%",
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
