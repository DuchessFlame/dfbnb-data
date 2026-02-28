#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build deterministic Events Rewards JSON for DF/BNB.

Inputs (newest matching file for each prefix):
- guide_index.tsv
- QUEST_Export_*.tsv
- GMRW_Export_*.tsv
- GLOB_Export_*.tsv
- LVLI_Export_*_LVLI_List.tsv
- LVLI_Export_*_LVLI_Entries.tsv
- LVLI_Export_*_LVLI_Refs.tsv
- LVLI_Export_*_LVLI_Math.tsv
- BOOK_Export_*.tsv
- ARMO_Export_*.tsv

Outputs:
- dist/events/events_rewards.json
- dist/events/events_rewards_by_page.json
- dist/patchlogs/patchlog_latest_df_events.json
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable


# ----------------------------
# Config
# ----------------------------

IGNORE_PREFIXES = ("DEL", "CUT", "POST", "ZZZ", "zzz", "TheDrifter")
DEFAULT_ENCODING = "utf-8-sig"  # handles BOM cleanly

DIST_EVENTS_DIR = Path("dist/events")
DIST_PATCHLOG_DIR = Path("dist/patchlogs")

OUT_EVENTS_ALL = DIST_EVENTS_DIR / "events_rewards.json"
OUT_EVENTS_BY_PAGE = DIST_EVENTS_DIR / "events_rewards_by_page.json"
OUT_PATCHLOG_LATEST = DIST_PATCHLOG_DIR / "patchlog_latest_df_events.json"


# ----------------------------
# Helpers
# ----------------------------

def stable_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_tsv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding=DEFAULT_ENCODING, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [dict(row) for row in reader]

def newest_file(pattern: str) -> Optional[str]:
    matches = glob.glob(pattern)
    if not matches:
        return None
    matches.sort(key=lambda p: os.path.getmtime(p))
    return matches[-1]

def pick_required(pattern: str, label: str) -> str:
    p = newest_file(pattern)
    if not p:
        raise FileNotFoundError(f"Missing required TSV for {label}: pattern={pattern}")
    return p

def norm(s: Any) -> str:
    return str(s).strip()

def is_ignored_ref(edid_or_full: str) -> bool:
    s = norm(edid_or_full)
    return any(s.startswith(pref) for pref in IGNORE_PREFIXES)

def safe_float(s: Any, default: float = 0.0) -> float:
    try:
        return float(str(s).strip())
    except Exception:
        return default

def safe_int(s: Any, default: int = 0) -> int:
    try:
        return int(float(str(s).strip()))
    except Exception:
        return default

def sort_key_alpha(s: str) -> Tuple[str, str]:
    # case-insensitive then original
    return (s.lower(), s)

def json_dump_deterministic(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)

def pct(x: float) -> float:
    # clamp tiny floating noise, keep 6 dp max
    x = max(0.0, min(100.0, x))
    return float(f"{x:.6f}".rstrip("0").rstrip(".")) if x != int(x) else float(int(x))


# ----------------------------
# Data models
# ----------------------------

@dataclass
class LvliEntry:
    entry_id: str
    parent_formid: str
    ref_formid: str
    ref_edid: str
    count: int
    level: int
    chance: float  # per-entry chance if available, else 100
    # math / conditions raw
    math_raw: str = ""
    # parsed conditions
    glob_toggle_formid: Optional[str] = None
    glob_toggle_op: Optional[str] = None  # "==" or "!="
    glob_toggle_value: Optional[str] = None  # usually "1"
    rand_percent: Optional[float] = None  # if GetRandomPercent < X pattern found

@dataclass
class LvliList:
    formid: str
    edid: str
    chance_none: int
    flags: str = ""
    # entries
    entries: List[LvliEntry] = field(default_factory=list)

@dataclass
class ResolvedItem:
    formid: str
    edid: str
    full: str
    kind: str  # "ARMO" / "BOOK" / "MISC" etc
    tradeable: Optional[bool] = None
    release_date: Optional[str] = None


# ----------------------------
# TSV loaders
# ----------------------------

def index_by(rows: List[Dict[str, str]], key_field: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        k = norm(r.get(key_field, ""))
        if k:
            out[k] = r
    return out

def load_guide_index(path: str) -> Dict[str, Dict[str, str]]:
    rows = read_tsv(path)
    # guide_index usually has slug/path columns. Keep whole row keyed by page_id or slug if present.
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        slug = norm(r.get("slug") or r.get("Slug") or r.get("page_slug") or r.get("PageSlug") or "")
        url = norm(r.get("url") or r.get("URL") or r.get("path") or r.get("Path") or "")
        page_id = norm(r.get("page_id") or r.get("PageID") or r.get("id") or r.get("ID") or "")
        key = page_id or slug or url
        if key:
            out[key] = r
    return out

def load_glob(path: str) -> Dict[str, float]:
    rows = read_tsv(path)
    # Expect: GLOB_FormID / FLTV_Value etc
    out: Dict[str, float] = {}
    for r in rows:
        fid = norm(r.get("GLOB_FormID") or r.get("FormID") or r.get("formid") or "")
        if not fid:
            continue
        val = safe_float(r.get("FLTV_Value") or r.get("Value") or r.get("value") or 0.0, 0.0)
        out[fid] = val
    return out

def load_items_book_armo(book_path: str, armo_path: str) -> Dict[str, ResolvedItem]:
    items: Dict[str, ResolvedItem] = {}
    # BOOK
    for r in read_tsv(book_path):
        fid = norm(r.get("BOOK_FormID") or r.get("FormID") or "")
        edid = norm(r.get("BOOK_EDID") or r.get("EDID") or "")
        full = norm(r.get("BOOK_FULL") or r.get("FULL") or "")
        if fid:
            items[fid] = ResolvedItem(formid=fid, edid=edid, full=full or edid or fid, kind="BOOK")
    # ARMO
    for r in read_tsv(armo_path):
        fid = norm(r.get("ARMO_FormID") or r.get("FormID") or "")
        edid = norm(r.get("ARMO_EDID") or r.get("EDID") or "")
        full = norm(r.get("ARMO_FULL") or r.get("FULL") or "")
        if fid:
            items[fid] = ResolvedItem(formid=fid, edid=edid, full=full or edid or fid, kind="ARMO")
    return items

def load_lvli(
    list_path: str,
    entries_path: str,
    refs_path: str,
    math_path: str
) -> Dict[str, LvliList]:
    list_rows = read_tsv(list_path)
    entry_rows = read_tsv(entries_path)
    ref_rows = read_tsv(refs_path)
    math_rows = read_tsv(math_path)

    # Build base LVLI lists
    lvli: Dict[str, LvliList] = {}
    for r in list_rows:
        fid = norm(r.get("LVLI_FormID") or r.get("FormID") or "")
        edid = norm(r.get("LVLI_EDID") or r.get("EDID") or "")
        chance_none = safe_int(r.get("LVLI_ChanceNone") or r.get("ChanceNone") or r.get("chance_none") or 0, 0)
        flags = norm(r.get("LVLI_Flags") or r.get("Flags") or "")
        if fid:
            lvli[fid] = LvliList(formid=fid, edid=edid, chance_none=chance_none, flags=flags)

    # Index refs by (parent lvli, entry id) if available, else best-effort by RowKey fields
    refs_index: Dict[Tuple[str, str], Dict[str, str]] = {}
    for r in ref_rows:
        parent = norm(r.get("LVLI_FormID") or r.get("Parent_FormID") or r.get("Parent") or "")
        entry_id = norm(r.get("Entry_ID") or r.get("LVLI_EntryID") or r.get("EntryId") or r.get("entry_id") or "")
        if parent and entry_id:
            refs_index[(parent, entry_id)] = r

    # Index math similarly
    math_index: Dict[Tuple[str, str], Dict[str, str]] = {}
    for r in math_rows:
        parent = norm(r.get("LVLI_FormID") or r.get("Parent_FormID") or r.get("Parent") or "")
        entry_id = norm(r.get("Entry_ID") or r.get("LVLI_EntryID") or r.get("EntryId") or r.get("entry_id") or "")
        if parent and entry_id:
            math_index[(parent, entry_id)] = r

    # Attach entries
    for r in entry_rows:
        parent = norm(r.get("LVLI_FormID") or r.get("Parent_FormID") or r.get("Parent") or "")
        entry_id = norm(r.get("Entry_ID") or r.get("LVLI_EntryID") or r.get("EntryId") or r.get("entry_id") or "")
        if not parent or parent not in lvli:
            continue

        # ref from entries row or refs table
        ref_formid = norm(r.get("Ref_FormID") or r.get("RefFormID") or r.get("REFR_FormID") or "")
        ref_edid = norm(r.get("Ref_EDID") or r.get("RefEDID") or "")
        if not ref_formid or not ref_edid:
            rr = refs_index.get((parent, entry_id), {})
            ref_formid = ref_formid or norm(rr.get("Ref_FormID") or rr.get("RefFormID") or "")
            ref_edid = ref_edid or norm(rr.get("Ref_EDID") or rr.get("RefEDID") or "")

        count = safe_int(r.get("Count") or r.get("LVLI_Count") or 1, 1)
        level = safe_int(r.get("Level") or r.get("LVLI_Level") or 1, 1)
        chance = safe_float(r.get("Chance") or r.get("LVLI_Chance") or 100.0, 100.0)

        mr = math_index.get((parent, entry_id), {})
        math_raw = norm(mr.get("Math") or mr.get("Math_Raw") or mr.get("Conditions") or "")

        entry = LvliEntry(
            entry_id=entry_id or f"row{len(lvli[parent].entries)+1}",
            parent_formid=parent,
            ref_formid=ref_formid,
            ref_edid=ref_edid,
            count=count,
            level=level,
            chance=chance if chance > 0 else 100.0,
            math_raw=math_raw
        )

        parse_conditions_into_entry(entry)
        lvli[parent].entries.append(entry)

    # Deterministic ordering of entries inside each list
    for l in lvli.values():
        l.entries.sort(key=lambda e: (e.level, sort_key_alpha(e.ref_edid), sort_key_alpha(e.ref_formid)))

    return lvli


# ----------------------------
# Condition parsing
# ----------------------------

_RE_GLOB_TOGGLE = re.compile(r"GetGlobalValue\((?P<glob>[0-9A-Fa-f]{8})\)\s*(?P<op>==|!=|<>|=)\s*(?P<val>-?\d+)", re.IGNORECASE)
_RE_RANDP_LT = re.compile(r"GetRandomPercent\(\)\s*<\s*(?P<pct>\d+(\.\d+)?)", re.IGNORECASE)
_RE_RANDP_LE = re.compile(r"GetRandomPercent\(\)\s*<=\s*(?P<pct>\d+(\.\d+)?)", re.IGNORECASE)

def parse_conditions_into_entry(e: LvliEntry) -> None:
    s = e.math_raw
    if not s:
        return

    # Toggle detection (GetGlobalValue(GLOB) == 1 or != 1 or <> 1)
    m = _RE_GLOB_TOGGLE.search(s)
    if m:
        op = m.group("op")
        if op == "=":
            op = "=="
        if op == "<>":
            op = "!="
        e.glob_toggle_formid = m.group("glob").upper()
        e.glob_toggle_op = op
        e.glob_toggle_value = m.group("val")

    # Percent gating via GetRandomPercent
    m2 = _RE_RANDP_LT.search(s) or _RE_RANDP_LE.search(s)
    if m2:
        e.rand_percent = safe_float(m2.group("pct"), None)  # meaning "X% chance gate"


# ----------------------------
# Probability engine
# ----------------------------

@dataclass
class ProbScenario:
    name: str  # "Toggle Enabled" / "Toggle Disabled" / "Default"
    # map item_formid -> probability (0..1)
    item_probs: Dict[str, float] = field(default_factory=dict)
    # pool probability (0..1) that the LVLI yields something (after chance none + gates)
    pool_prob: float = 1.0

def lvli_yield_prob(chance_none: int) -> float:
    # Rule: if Chance None is 0, treat as 100% drop
    if chance_none == 0:
        return 1.0
    # otherwise chance_none is the percent chance of NONE
    return max(0.0, min(1.0, 1.0 - (chance_none / 100.0)))

def combine_probs_additive(dest: Dict[str, float], src: Dict[str, float], scale: float) -> None:
    for k, v in src.items():
        dest[k] = dest.get(k, 0.0) + v * scale

def compute_lvli_scenarios(
    lvli_map: Dict[str, LvliList],
    root_formid: str,
    max_depth: int = 25
) -> List[ProbScenario]:
    """
    Returns one or more scenarios for the LVLI tree.
    If toggle patterns are detected among siblings, returns Toggle Enabled/Disabled scenarios.
    """
    visited_stack: List[str] = []

    def walk(list_formid: str, depth: int) -> List[ProbScenario]:
        if depth > max_depth:
            return [ProbScenario(name="Default", item_probs={}, pool_prob=1.0)]
        if list_formid in visited_stack:
            # cycle guard
            return [ProbScenario(name="Default", item_probs={}, pool_prob=1.0)]

        l = lvli_map.get(list_formid)
        if not l:
            return [ProbScenario(name="Default", item_probs={}, pool_prob=1.0)]

        visited_stack.append(list_formid)

        base_pool_prob = lvli_yield_prob(l.chance_none)

        # Detect toggles: group entries by same ref, different glob toggle ops
        toggle_glob = None
        has_eq = False
        has_neq = False
        for e in l.entries:
            if e.glob_toggle_formid and e.glob_toggle_value == "1":
                toggle_glob = toggle_glob or e.glob_toggle_formid
                if e.glob_toggle_op == "==":
                    has_eq = True
                if e.glob_toggle_op == "!=":
                    has_neq = True

        scenarios: List[ProbScenario]
        if toggle_glob and has_eq and has_neq:
            scenarios = [ProbScenario(name="Toggle Enabled"), ProbScenario(name="Toggle Disabled")]
        else:
            scenarios = [ProbScenario(name="Default")]

        # For each scenario, accumulate entries
        for sc in scenarios:
            sc.pool_prob = base_pool_prob
            sc.item_probs = {}

            # Determine eligible entries in this scenario
            eligible: List[LvliEntry] = []
            for e in l.entries:
                if is_ignored_ref(e.ref_edid):
                    continue
                if toggle_glob and has_eq and has_neq and e.glob_toggle_formid == toggle_glob and e.glob_toggle_value == "1":
                    # Keep only matching op for scenario
                    if sc.name == "Toggle Enabled" and e.glob_toggle_op != "==":
                        continue
                    if sc.name == "Toggle Disabled" and e.glob_toggle_op != "!=":
                        continue
                eligible.append(e)

            # If there are per-entry chance values, treat as weights.
            # We do not invent engine-accurate "LVLF Calculate for each item in count" behavior without your TSV truth,
            # but we deterministically model:
            # - choose one entry uniformly if no chance weights
            # - else normalize by chance
            total_weight = 0.0
            weights: List[float] = []
            for e in eligible:
                w = e.chance if e.chance > 0 else 100.0
                weights.append(w)
                total_weight += w

            if not eligible or total_weight <= 0:
                visited_stack.pop()
                continue

            # For each eligible entry, compute its contribution
            for e, w in zip(eligible, weights):
                pick_prob = (w / total_weight)

                # Apply GetRandomPercent gate if present
                gate = 1.0
                if e.rand_percent is not None:
                    gate = max(0.0, min(1.0, e.rand_percent / 100.0))

                branch_prob = base_pool_prob * pick_prob * gate

                # If entry references another LVLI, recurse. Otherwise it is an item.
                if e.ref_formid in lvli_map:
                    sub_scenarios = walk(e.ref_formid, depth + 1)
                    # If sub returns multiple scenarios, merge by matching name where possible, else fold into this scenario.
                    # Here we fold all sub scenario probs into current scenario, preserving totals.
                    for sub in sub_scenarios:
                        # scale by branch_prob and by sub.pool_prob already baked into sub.item_probs
                        combine_probs_additive(sc.item_probs, sub.item_probs, branch_prob)
                else:
                    # treat as terminal item
                    sc.item_probs[e.ref_formid] = sc.item_probs.get(e.ref_formid, 0.0) + branch_prob

        visited_stack.pop()
        return scenarios

    return walk(root_formid, 0)


# ----------------------------
# Event builder logic
# ----------------------------

def resolve_item_name(items: Dict[str, ResolvedItem], formid: str, fallback_edid: str = "") -> str:
    it = items.get(formid)
    if it and it.full:
        return it.full
    return fallback_edid or formid

def build_warning_block(msg: str) -> Dict[str, Any]:
    return {
        "type": "warning",
        "title": "Rewards data missing",
        "message": msg
    }

def build_party_crasher_banner(quest_row: Dict[str, str], glob_vals: Dict[str, float]) -> Optional[Dict[str, Any]]:
    # Expected columns (per your description):
    # PartyCrasherCount, PartyCrasher_NPC_0, PartyCrasher_GLOB_0, PartyCrasher_MESG
    count = safe_int(quest_row.get("PartyCrasherCount") or 0, 0)
    if count <= 0:
        return None

    banners: List[str] = []
    for i in range(count):
        npc = norm(quest_row.get(f"PartyCrasher_NPC_{i}") or "")
        glob = norm(quest_row.get(f"PartyCrasher_GLOB_{i}") or "")
        if not npc or not glob:
            continue
        fltv = glob_vals.get(glob.upper())
        if fltv is None:
            continue
        chance_percent = pct(fltv * 100.0 if fltv <= 1.0 else fltv)  # supports both 0.33 and 33
        banners.append(f"{chance_percent:g}% chance for {npc} to spawn at the end of the event.")

    if not banners:
        return None

    return {
        "type": "notice",
        "style": "party-crasher",
        "lines": banners
    }

def detect_mutated(quest_row: Dict[str, str]) -> bool:
    # Placeholder: harden once we see your actual QUEST columns.
    # Common patterns: keywords, flags, or text fields containing "Mutated"
    hay = " ".join([
        norm(quest_row.get("Keywords_Flat") or ""),
        norm(quest_row.get("QUEST_EDID") or ""),
        norm(quest_row.get("QUEST_FULL") or ""),
        norm(quest_row.get("Notes") or "")
    ])
    return "mutated" in hay.lower()

def build_event_payload(
    quest_row: Dict[str, str],
    gmrw_rows_by_quest: Dict[str, List[Dict[str, str]]],
    lvli_map: Dict[str, LvliList],
    items: Dict[str, ResolvedItem],
    glob_vals: Dict[str, float]
) -> Dict[str, Any]:
    quest_formid = norm(quest_row.get("QUEST_FormID") or quest_row.get("FormID") or "")
    quest_full = norm(quest_row.get("QUEST_FULL") or quest_row.get("FULL") or "")
    quest_edid = norm(quest_row.get("QUEST_EDID") or quest_row.get("EDID") or "")

    payload: Dict[str, Any] = {
        "questFormID": quest_formid,
        "name": quest_full or quest_edid or quest_formid,
        "baseRewards": [],
        "rewards": {
            "default": [],
            "headwear": {
                "common": [],
                "uncommon": [],
                "rare": []
            },
            "plans": {
                "count": 0,
                "poolChance": 100.0,
                "perItemChance": None,
                "items": []
            }
        },
        "scenarios": [],  # toggle scenarios, if any
        "banners": [],
        "warnings": []
    }

    # Mutated banner
    if detect_mutated(quest_row):
        payload["banners"].append({
            "type": "notice",
            "style": "mutated",
            "lines": ["This event can be a Mutated Public Event."]
        })

    # Party Crasher banner(s)
    pcb = build_party_crasher_banner(quest_row, glob_vals)
    if pcb:
        payload["banners"].append(pcb)

    # Base rewards from GMRW
    base_blocks = gmrw_rows_by_quest.get(quest_formid, [])
    if base_blocks:
        payload["baseRewards"] = build_base_rewards_from_gmrw(base_blocks, lvli_map, items)
    else:
        payload["warnings"].append(build_warning_block("No GMRW reward blocks found for this quest."))

    # Event rewards from LVLI: locate a root list reference in QUEST row if available,
    # else infer by naming convention. Hardening needs your actual columns.
    root_lvli_formid = norm(quest_row.get("QuestRewards_LVLI") or quest_row.get("_LL_Quest_Rewards") or "")
    if not root_lvli_formid:
        # fallback: try columns commonly exported
        for k in quest_row.keys():
            if "Quest_Rewards" in k and "LVLI" in k:
                root_lvli_formid = norm(quest_row.get(k))
                if root_lvli_formid:
                    break

    if root_lvli_formid and root_lvli_formid in lvli_map:
        scenarios = compute_lvli_scenarios(lvli_map, root_lvli_formid)
        payload["scenarios"] = [serialize_scenario(sc, items, lvli_map) for sc in scenarios]

        # Also populate "rewards" using the Default scenario for standard UI blocks
        default_sc = next((s for s in scenarios if s.name == "Default"), scenarios[0] if scenarios else None)
        if default_sc:
            hydrate_reward_groups_from_probs(payload, default_sc, items)
        else:
            payload["warnings"].append(build_warning_block("Could not compute LVLI reward probabilities."))
    else:
        payload["warnings"].append(build_warning_block("Quest reward LVLI root not found or not exported."))

    # Always ensure non-empty render surface
    if not payload["baseRewards"] and not payload["scenarios"] and not payload["banners"]:
        payload["warnings"].append(build_warning_block("No rewards data available. This page will show warnings instead of blank content."))

    return payload

def build_base_rewards_from_gmrw(
    gmrw_rows: List[Dict[str, str]],
    lvli_map: Dict[str, LvliList],
    items: Dict[str, ResolvedItem]
) -> List[Dict[str, Any]]:
    """
    This is intentionally schema-agnostic.
    It looks for common keys like XP, Caps, Bullion, Mystery Pick,
    plus QRLI/QRLR and Legendary Modules in _Quest_Rewards-style LVLI.
    """
    out: List[Dict[str, Any]] = []

    # Best-effort extraction by known column names.
    # You will likely have consistent fields in your export, we can tighten once you paste the TSV header row.
    joined = " ".join([" ".join([f"{k}={norm(v)}" for k, v in r.items() if norm(v)]) for r in gmrw_rows])

    def add_line(label: str, value: Any) -> None:
        if value is None:
            return
        out.append({"label": label, "value": value})

    # Common keys
    for r in gmrw_rows:
        xp = r.get("XP") or r.get("Reward_XP") or r.get("GMRW_XP")
        caps = r.get("Caps") or r.get("Reward_Caps") or r.get("GMRW_Caps")
        gb = r.get("GoldBullion") or r.get("Bullion") or r.get("GMRW_Bullion")
        if xp:
            add_line("XP", safe_int(xp, 0))
        if caps:
            add_line("Caps", safe_int(caps, 0))
        if gb:
            add_line("Gold Bullion", safe_int(gb, 0))

    # Murmrgh’s Mystery Pick (3* legendary item)
    if "Mystery" in joined or "Murmrgh" in joined or "MURMRGH" in joined:
        out.append({"label": "Murmrgh’s Mystery Pick", "value": "3★ Legendary Item"})

    # Legendary rank (QRLR) and reward list (QRLI)
    # We keep raw refs if present
    qrlr = None
    qrli = None
    for r in gmrw_rows:
        qrlr = qrlr or norm(r.get("QRLR") or r.get("LegendaryRank") or "")
        qrli = qrli or norm(r.get("QRLI") or r.get("LegendaryList") or "")
    if qrlr:
        out.append({"label": "Legendary Rank", "value": qrlr})
    if qrli:
        out.append({"label": "Legendary Reward List", "value": qrli})

    # Bubble Legendary Modules if referenced anywhere in _Quest_Rewards lists
    # This is a conservative heuristic: look for "Legendary Module" in resolved FULL names in any referenced LVLI.
    modules_found = set()
    for r in gmrw_rows:
        maybe_lvli = norm(r.get("LVLI_FormID") or r.get("RewardList") or "")
        if maybe_lvli and maybe_lvli in lvli_map:
            scs = compute_lvli_scenarios(lvli_map, maybe_lvli)
            for sc in scs:
                for fid in sc.item_probs.keys():
                    nm = resolve_item_name(items, fid, fid)
                    if "Legendary Module" in nm:
                        modules_found.add(nm)
    if modules_found:
        out.append({"label": "Legendary Modules", "value": sorted(modules_found, key=sort_key_alpha)})

    # Deterministic ordering of base rewards
    label_order = {"XP": 1, "Caps": 2, "Gold Bullion": 3, "Murmrgh’s Mystery Pick": 4, "Legendary Reward List": 5, "Legendary Rank": 6, "Legendary Modules": 7}
    out.sort(key=lambda x: (label_order.get(x["label"], 99), sort_key_alpha(str(x["label"]))))
    return out

def serialize_scenario(sc: ProbScenario, items: Dict[str, ResolvedItem], lvli_map: Dict[str, LvliList]) -> Dict[str, Any]:
    # Convert probs to user-friendly percents while keeping exact computed values
    rows = []
    for fid, p in sc.item_probs.items():
        nm = resolve_item_name(items, fid, fid)
        rows.append({
            "formid": fid,
            "name": nm,
            "chance": pct(p * 100.0)
        })
    rows.sort(key=lambda r: sort_key_alpha(r["name"]))
    return {
        "name": sc.name,
        "poolChance": pct(sc.pool_prob * 100.0),
        "items": rows
    }

def hydrate_reward_groups_from_probs(payload: Dict[str, Any], sc: ProbScenario, items: Dict[str, ResolvedItem]) -> None:
    """
    Populate Default / Headwear / Plans from Default scenario.
    Classification rules should be tightened once we see your LVLI structure names and/or item keywords.
    For now:
    - Plans: item FULL starting with "Plan:" or "Recipe:"
    - Headwear: ARMO items whose FULL contains keywords like "Hat", "Mask", "Helmet", "Headwear"
    - Everything else: Default
    """
    plans = []
    headwear = []
    default = []

    for fid, prob in sc.item_probs.items():
        nm = resolve_item_name(items, fid, fid)
        if is_ignored_ref(nm):
            continue

        row = {
            "formid": fid,
            "name": nm,
            "dropRate": pct(prob * 100.0),
            "releaseDate": None,
            "tradeable": None
        }

        if nm.startswith("Plan:") or nm.startswith("Recipe:"):
            plans.append(row)
        elif any(k in nm for k in ("Mask", "Hat", "Helmet", "Headwear", "Beret", "Cap")):
            headwear.append(row)
        else:
            default.append(row)

    # Plans
    plans.sort(key=lambda r: sort_key_alpha(r["name"]))
    plan_count = len(plans)
    payload["rewards"]["plans"]["count"] = plan_count
    payload["rewards"]["plans"]["poolChance"] = 100.0  # per your rule for plan rewards pool display
    payload["rewards"]["plans"]["items"] = plans
    if plan_count > 0:
        payload["rewards"]["plans"]["perItemChance"] = pct(100.0 / plan_count)

    # Headwear sections (Common / Uncommon / Rare)
    # Without your rarity source-of-truth columns, we do NOT invent rarity.
    # We place everything into Common by default and leave a warning if we cannot classify.
    headwear.sort(key=lambda r: sort_key_alpha(r["name"]))
    payload["rewards"]["headwear"]["common"] = headwear
    if headwear:
        payload["warnings"].append(build_warning_block("Headwear rarity (Common/Uncommon/Rare) not classified because no non-guess source was provided in TSV. Paste your ARMO/LVLI rarity columns and we will wire it deterministically."))

    # Default rewards
    default.sort(key=lambda r: sort_key_alpha(r["name"]))
    payload["rewards"]["default"] = default


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    # Locate TSVs (newest)
    guide_index_path = pick_required("tsv/guide_index.tsv", "guide_index.tsv")
    quest_path = pick_required("tsv/QUEST_Export_*.tsv", "QUEST")
    gmrw_path = pick_required("tsv/GMRW_Export_*.tsv", "GMRW")
    glob_path = pick_required("tsv/GLOB_Export_*.tsv", "GLOB")
    lvli_list_path = pick_required("tsv/LVLI_Export_*_LVLI_List.tsv", "LVLI_List")
    lvli_entries_path = pick_required("tsv/LVLI_Export_*_LVLI_Entries.tsv", "LVLI_Entries")
    lvli_refs_path = pick_required("tsv/LVLI_Export_*_LVLI_Refs.tsv", "LVLI_Refs")
    lvli_math_path = pick_required("tsv/LVLI_Export_*_LVLI_Math.tsv", "LVLI_Math")
    book_path = pick_required("tsv/BOOK_Export_*.tsv", "BOOK")
    armo_path = pick_required("tsv/ARMO_Export_*.tsv", "ARMO")

    guide_index = load_guide_index(guide_index_path)
    quest_rows = read_tsv(quest_path)
    gmrw_rows = read_tsv(gmrw_path)
    glob_vals = load_glob(glob_path)
    items = load_items_book_armo(book_path, armo_path)
    lvli_map = load_lvli(lvli_list_path, lvli_entries_path, lvli_refs_path, lvli_math_path)

    # Group GMRW by quest formid if possible
    gmrw_by_quest: Dict[str, List[Dict[str, str]]] = {}
    for r in gmrw_rows:
        qid = norm(r.get("QUEST_FormID") or r.get("Quest_FormID") or r.get("QuestFormID") or "")
        if not qid:
            continue
        gmrw_by_quest.setdefault(qid, []).append(r)

    # Build all events
    events_all: List[Dict[str, Any]] = []
    events_by_page: Dict[str, Dict[str, Any]] = {}

    for qr in quest_rows:
        q_formid = norm(qr.get("QUEST_FormID") or qr.get("FormID") or "")
        if not q_formid:
            continue

        # Determine page slug
        # Hardening: use your known column name once provided.
        page_slug = norm(qr.get("PageSlug") or qr.get("page_slug") or qr.get("Slug") or "")
        if not page_slug:
            # try to infer from guide_index by quest id match if present
            page_slug = norm(qr.get("GuideSlug") or "")

        payload = build_event_payload(qr, gmrw_by_quest, lvli_map, items, glob_vals)
        events_all.append(payload)

        if page_slug:
            events_by_page[page_slug] = payload

    # Guarantee: Never blank pages for guide-indexed event pages
    for _, row in guide_index.items():
        ptype = norm(row.get("type") or row.get("page_type") or "")
        slug = norm(row.get("slug") or row.get("page_slug") or "")
        if not slug:
            continue
        # Only enforce for event rewards pages (best-effort detection)
        if "event" in (ptype.lower() if ptype else "") or "/events/" in norm(row.get("url") or row.get("path") or "").lower():
            if slug not in events_by_page:
                events_by_page[slug] = {
                    "name": norm(row.get("title") or row.get("Title") or slug),
                    "questFormID": None,
                    "baseRewards": [],
                    "rewards": {"default": [], "headwear": {"common": [], "uncommon": [], "rare": []}, "plans": {"count": 0, "poolChance": 0.0, "perItemChance": None, "items": []}},
                    "scenarios": [],
                    "banners": [],
                    "warnings": [build_warning_block("No event data matched this page slug. JSON emitted to prevent blank page.")]
                }

    # Deterministic sort of events_all by name
    events_all.sort(key=lambda e: sort_key_alpha(norm(e.get("name") or "")))

    # Patchlog latest (minimal stub, your patchlog system can overwrite this)
    patchlog = {
        "builtAt": stable_now_iso(),
        "system": "events-rewards",
        "note": "Latest Events Rewards build.",
        "source": {
            "quest": os.path.basename(quest_path),
            "gmrw": os.path.basename(gmrw_path),
            "glob": os.path.basename(glob_path),
            "lvli_list": os.path.basename(lvli_list_path),
            "lvli_entries": os.path.basename(lvli_entries_path),
            "lvli_refs": os.path.basename(lvli_refs_path),
            "lvli_math": os.path.basename(lvli_math_path),
            "book": os.path.basename(book_path),
            "armo": os.path.basename(armo_path)
        }
    }

    ensure_dir(DIST_EVENTS_DIR)
    ensure_dir(DIST_PATCHLOG_DIR)

    OUT_EVENTS_ALL.write_text(json_dump_deterministic({"builtAt": stable_now_iso(), "events": events_all}), encoding="utf-8")
    OUT_EVENTS_BY_PAGE.write_text(json_dump_deterministic({"builtAt": stable_now_iso(), "byPage": events_by_page}), encoding="utf-8")
    OUT_PATCHLOG_LATEST.write_text(json_dump_deterministic(patchlog), encoding="utf-8")

    print(f"Wrote: {OUT_EVENTS_ALL}")
    print(f"Wrote: {OUT_EVENTS_BY_PAGE}")
    print(f"Wrote: {OUT_PATCHLOG_LATEST}")

if __name__ == "__main__":
    main()