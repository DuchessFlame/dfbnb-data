#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
treasure_map_sources.py — the Treasure Maps root expand for every
"{Item} Spawn Locations" / farming guide page.

Spawn-guide skill §9l. Answers ONE question per row:

    "If I dig up {this map}, what is the chance {this item} is in the haul?"

Three map families, all resolved from game data (rng76) — nothing typed:

  1. Treasure Maps        — the 35 region maps (Forest #01 … Savage Divide #10).
                            A dig rolls LL_TreasureMap_Reward [001A7220]: three
                            all-region pools (recipes / weapon-mod plans /
                            armour-mod plans) PLUS one region pool picked by a
                            GetIsInRegion condition on the dig site. A Forest map
                            only ever digs in the Forest, so its region branch is
                            fixed — which is why the per-region pool is selected
                            here instead of letting rng76 walk all five branches.
  2. Lucky Strike         — the U Mine It maps (Miner / Prospector / Excavator).
                            A completion rolls the tier's own mining LVLI PLUS the
                            shared quest-reward pools (Aid, Waste Acid) that fire
                            on every Lucky Strike hand-in.
  3. Pint-Sized Phantoms  — the "Secrets to the Grave" map. A grave dig rolls the
                            repeatable-rewards list, plus the quest-related list
                            while the quest is active.

MAP NAMES AND IDs ARE NEVER HARDCODED — they are joined from dist/treasure_maps.json
(built by build_treasure_maps_json.py), the same way the Collectron / Resource
Generator cards join from dist/collectrons.json. A new map added to the game shows
up on every item page it can pay out with no edit here.

The ONE structural constant is LL_TreasureMap_Reward [001A7220] — the root a
treasure-map dig rolls. Its own entries are read from the LVLI tables to work out
which pools are all-region and which one is the region branch, so a Bethesda
change to that list is picked up automatically.

RATES follow the drop-rate-engine skill. Each contributing pool's chance is
rng76.appearance_prob(pool, item) — the fully resolved waterfall / pick-one /
ChanceNone chance that the item shows when that pool is rolled once. The pools of
one dig fire independently (001A7220 is UseAll with max_count 30, i.e. the
"independent, each entry at its own rate" case — drop-rate-engine §3a/3c), so the
map's headline number is the standard at-least-once combination:

    rate = 1 - PRODUCT(1 - pool_rate)

Never a bare ChanceNone, never hand-rolled, and a pool that resolves to 0 simply
contributes nothing. A map with no contributing pool is DROPPED from the list (the
closure over-approximates; rng76 gives the true chance) — the same rule the Events
& Activities expand uses.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Sequence

# Root LVLI a treasure-map dig rolls: LL_TreasureMap_Reward. Its four entries are
# read from the LVLI tables (see _split_root_pools), so the all-region vs region
# split is data-driven rather than a second hardcoded list.
TREASURE_MAP_REWARD_ROOT = "001A7220"

# LL_TreasureMap_TeammateReward — the SECOND pool the 35 region-map mound
# activators roll, paying a teammate standing at the dig. It is one shared list
# with no GetIsInRegion branch, so every region map pays it at the same chance.
# It used to surface in Events & Activities as "Treasure Map Teammate Reward",
# which told the reader nothing about which map to dig; it belongs here.
TREASURE_MAP_TEAMMATE_ROOT = "001A721E"

# Group headings, their order in the expand, and the page each links to.
GROUP_TREASURE = "Treasure Maps"
GROUP_TEAMMATE = "Teammate Reward"
GROUP_LUCKY = "Lucky Strike"
GROUP_PHANTOM = "Pint-Sized Phantoms"

GROUP_ORDER = [GROUP_TREASURE, GROUP_TEAMMATE, GROUP_LUCKY, GROUP_PHANTOM]

GROUP_LINKS = {
    GROUP_TREASURE: {
        "href": "https://www.buffsnbrew.com/df/treasure-maps/rewards/",
        "label": "Treasure map rewards",
    },
    GROUP_TEAMMATE: {
        "href": "https://www.buffsnbrew.com/df/treasure-maps/rewards/",
        "label": "Treasure map rewards",
    },
    GROUP_LUCKY: {
        "href": "https://www.buffsnbrew.com/df/treasure-maps/lucky-strike/rewards/",
        "label": "Lucky Strike rewards",
    },
    GROUP_PHANTOM: {
        "href": "https://www.buffsnbrew.com/df/treasure-maps/pint-sized-phantoms/rewards/",
        "label": "Pint-Sized Phantoms rewards",
    },
}


# ── dist/treasure_maps.json loader ───────────────────────────────────────────

_TM_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}


def load_treasure_maps(dist_dir: str) -> Optional[Dict[str, Any]]:
    """dist/treasure_maps.json, or None when it hasn't been built yet.

    Cached per dist_dir so a --all run reads it once. A missing file is not an
    error: the expand falls back to its empty state, exactly like a missing
    collectrons.json drops the producer cards.
    """
    if dist_dir in _TM_CACHE:
        return _TM_CACHE[dist_dir]
    path = os.path.join(dist_dir, "treasure_maps.json")
    doc: Optional[Dict[str, Any]] = None
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        print("  [warn] treasure_maps.json not built — Treasure Maps expand skipped")
    _TM_CACHE[dist_dir] = doc
    return doc


def _fid(value: Any) -> str:
    """Normalise a FormID cell ('0050CC2E', '0050CC2E:EDID:LVLI') to bare hex."""
    s = str(value or "").strip()
    if not s:
        return ""
    return s.split(":")[0].strip().upper()


def _child_ids(lvli: Any, list_id: str) -> List[str]:
    """FormIDs of every entry in an LVLI (sub-lists and leaf items alike)."""
    out: List[str] = []
    for entry in (lvli.entries_by_list.get(_fid(list_id), []) if lvli else []):
        child = _fid(entry.get("LVLO_Reference"))
        if child:
            out.append(child)
    return out


# ── Which pools a region map's dig rolls ─────────────────────────────────────

def _split_root_pools(lvli: Any, region_pool_ids: Sequence[str]) -> Dict[str, Any]:
    """Read LL_TreasureMap_Reward's own entries and split them into:

        all_region: pools every treasure map rolls regardless of where it digs
                    (recipes, weapon-mod plans, armour-mod plans)
        base:       the region-routing list (LLS_TreasureMap_Reward_Base) whose
                    entries are the per-region pools, chosen by a GetIsInRegion
                    condition on the dig site

    Identified structurally — the base is the entry that actually contains the
    region pools — so a Bethesda reshuffle of that root is picked up for free.
    """
    wanted = {_fid(x) for x in region_pool_ids if x}
    all_region: List[str] = []
    base: List[str] = []
    for child in _child_ids(lvli, TREASURE_MAP_REWARD_ROOT):
        if set(_child_ids(lvli, child)) & wanted:
            base.append(child)
        else:
            all_region.append(child)
    return {"all_region": all_region, "base": base}


# ── Rate maths (drop-rate-engine §3a/3c: independently-firing pools) ─────────

def combine(probs: Sequence[float]) -> float:
    """At-least-once chance across pools that fire independently in one dig.

    LL_TreasureMap_Reward is UseAll with max_count 30, so every entry is rolled
    on its own — the "independent, each entry at its own rate" case. The chance
    the item shows at least once is therefore 1 - PRODUCT(1 - each pool's chance),
    the empty-chance product from drop-rate-engine §3c read the other way round.
    The same shape applies to a Lucky Strike hand-in (tier list + shared quest
    pools) and a Phantom grave dig (repeatable list + quest-related list).
    """
    fail = 1.0
    for p in probs:
        p = 0.0 if not p else float(p)
        if p <= 0:
            continue
        if p >= 1.0:
            return 1.0
        fail *= (1.0 - p)
    return 1.0 - fail


def _pool_rates(appearance, pool_ids: Sequence[str], targets: set,
                lvli: Any = None) -> List[Dict[str, Any]]:
    """[{form_id, edid, rate}] for every pool that can actually pay the item out.

    `appearance(list_id, targets) -> float` is the caller's rng76 appearance
    probability (VendorRates.appearance in the farming build). Pools resolving to
    0 are dropped — they are structurally reachable but cannot pay out.

    The EDID is carried so a row's number can be traced back to the exact pool
    without re-deriving it (the renderer shows the headline % only).
    """
    rows: List[Dict[str, Any]] = []
    seen = set()
    for pid in pool_ids:
        pid = _fid(pid)
        if not pid or pid in seen:
            continue
        seen.add(pid)
        try:
            p = float(appearance(pid, targets) or 0.0)
        except Exception:
            p = 0.0
        if p <= 0:
            continue
        edid = ""
        if lvli is not None:
            try:
                edid = lvli.edid_for(pid) or ""
            except Exception:
                edid = ""
        rows.append({
            "form_id": pid,
            "edid": edid,
            "rate": round(p, 6),
        })
    rows.sort(key=lambda r: -r["rate"])
    return rows


# ── The map list ─────────────────────────────────────────────────────────────

def build_maps(dist_dir: str, targets: set, appearance, lvli: Any,
               fmt_rate) -> List[Dict[str, Any]]:
    """Every treasure map that can pay the item out, one row per MAP.

    Args:
        dist_dir:   repo dist/ (or dist/pts) — where treasure_maps.json lives.
        targets:    the item's ALCH/MISC FormIDs (upper hex).
        appearance: rng76 appearance probability, `f(list_id, targets) -> float`.
        lvli:       the loaded LvliIndex (for reading the reward root's entries).
        fmt_rate:   the build's percentage formatter, `f(0.3333) -> '33.33%'`.

    Returns rows shaped for the renderer:
        {group, name, region, form_id, edid, rate, rate_display, sources[]}
    sorted by group order, then rate desc, then name.
    """
    doc = load_treasure_maps(dist_dir)
    if not (doc and targets and appearance):
        return []

    rows: List[Dict[str, Any]] = []

    # 1. Region treasure maps ────────────────────────────────────────────────
    # Each region's maps all roll the same pools, so the pool set is resolved
    # once per region and reused for every map name in it.
    regions = doc.get("regions") or {}
    region_pool_ids: Dict[str, str] = {}
    for rkey in regions:
        pool = _region_pool_id(rkey)
        if pool:
            region_pool_ids[rkey] = pool
    split = _split_root_pools(lvli, list(region_pool_ids.values()))
    shared_ids = split["all_region"]

    for rkey, region in regions.items():
        maps = region.get("maps") or []
        if not maps:
            continue
        pool_ids = list(shared_ids)
        if region_pool_ids.get(rkey):
            pool_ids.append(region_pool_ids[rkey])
        sources = _pool_rates(appearance, pool_ids, targets, lvli)
        if not sources:
            continue
        rate = combine([s["rate"] for s in sources])
        if rate <= 0:
            continue
        for m in maps:
            rows.append(_row(GROUP_TREASURE, m.get("name") or "", region.get("name") or "",
                             m.get("form_id") or "", m.get("edid") or "",
                             rate, sources, fmt_rate))

    # 1b. Teammate reward ────────────────────────────────────────────────────
    # A separate roll made for a teammate at the dig, from ONE shared pool with
    # no region branch — so it resolves once and every region map carries the
    # same chance. Mirrors the map table above (one row per MAP, with its region)
    # so the reader looks their own map up the same way; kept as its own group
    # rather than folded into "Chance per dig", which would overstate the number
    # for anyone digging solo.
    teammate = _pool_rates(appearance, [TREASURE_MAP_TEAMMATE_ROOT], targets, lvli)
    teammate_rate = combine([s["rate"] for s in teammate])
    if teammate and teammate_rate > 0:
        for rkey, region in regions.items():
            for m in (region.get("maps") or []):
                rows.append(_row(GROUP_TEAMMATE,
                                 (m.get("name") or "") + " Teammate Reward",
                                 region.get("name") or "",
                                 m.get("form_id") or "", m.get("edid") or "",
                                 teammate_rate, teammate, fmt_rate))

    # 2. Lucky Strike (U Mine It) ────────────────────────────────────────────
    # The tier's own mining list plus the shared quest-reward pools that fire on
    # every hand-in, so an item that only lives in the shared Aid pool correctly
    # shows on all three maps.
    umine = doc.get("u_mine_it") or {}
    shared_umine = [_fid(p.get("form_id")) for p in (umine.get("shared_pools") or [])]
    shared_umine += _umine_extra_pool_ids()
    for tier in (umine.get("tiers") or {}).values():
        tier_id = _fid(tier.get("form_id"))
        if not tier_id:
            continue
        sources = _pool_rates(appearance, [tier_id] + shared_umine, targets, lvli)
        if not sources:
            continue
        rate = combine([s["rate"] for s in sources])
        if rate <= 0:
            continue
        # No region column: a Lucky Strike map's dig sites aren't grouped by
        # Appalachian region in the export, so nothing honest to put there.
        rows.append(_row(GROUP_LUCKY, tier.get("name") or "", "",
                         tier_id, tier.get("edid") or "", rate, sources, fmt_rate))

    # 3. Pint-Sized Phantoms ─────────────────────────────────────────────────
    # Inert on the live channel until the Slasher content ships — the block is
    # simply absent from treasure_maps.json there, so nothing renders.
    phantom = doc.get("pint_sized_phantoms") or {}
    if phantom:
        pmap = phantom.get("map") or {}
        pool_ids = [
            _fid((phantom.get("repeatable_rewards") or {}).get("list_formid")),
            _fid((phantom.get("quest_related") or {}).get("list_formid")),
        ]
        sources = _pool_rates(appearance, pool_ids, targets, lvli)
        rate = combine([s["rate"] for s in sources])
        if sources and rate > 0:
            rows.append(_row(GROUP_PHANTOM, pmap.get("name") or "Pint-Sized Phantoms' Map",
                             "", _fid(pmap.get("form_id")), pmap.get("edid") or "",
                             rate, sources, fmt_rate))

    order = {g: i for i, g in enumerate(GROUP_ORDER)}
    rows.sort(key=lambda r: (order.get(r["group"], 99), -r["rate"], r["name"].lower()))
    return rows


def _row(group: str, name: str, region: str, form_id: str, edid: str,
         rate: float, sources: List[Dict[str, Any]], fmt_rate) -> Dict[str, Any]:
    return {
        "group": group,
        "name": name,
        "region": region,
        "form_id": (form_id or "").upper(),
        "edid": edid,
        "rate": round(rate, 6),
        "rate_display": fmt_rate(rate),
        "sources": [dict(s, rate_display=fmt_rate(s["rate"])) for s in sources],
    }


# ── The two lookups that still need build_treasure_maps_json ─────────────────
# Kept as lazy imports so this module stays importable (and the expand degrades to
# its empty state) even if that build script is missing or fails to load. Importing
# it costs nothing at module level — it reads no TSVs until main() runs.

def _region_pool_id(region_key: str) -> str:
    """The per-region treasure-map reward pool for a region key ('forest')."""
    try:
        import build_treasure_maps_json as TM
        return _fid((TM.REWARD_REGION_FORMIDS or {}).get(region_key, ""))
    except Exception:
        return ""


def _umine_extra_pool_ids() -> List[str]:
    """Lucky Strike pools that fire on every hand-in but aren't listed under
    u_mine_it.shared_pools in the export (the Waste Acid scrap pool, which the
    treasure-maps build folds into each tier's Junk & Scrap sub-expand)."""
    try:
        import build_treasure_maps_json as TM
        return [_fid(TM.UMINE_ACID_FORMID)]
    except Exception:
        return []
