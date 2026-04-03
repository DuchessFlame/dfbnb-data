#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_drop_rates.py — Shared Drop Rate JSON Builder

Imports the rng76.py engine, loads all TSVs once, resolves every LVLI
pool referenced by events / bounty hunts / container loot / titles,
and writes dist/drop_rates.json so each page-specific build script can
consume pre-computed rates instead of re-implementing the engine.

Run from the repo root (where tsv/ and dist/ live):
    python3 src/build_drop_rates.py

Output: dist/drop_rates.json

Output schema:
{
  "meta": { "built": "<ISO timestamp>", "tsvRoot": "tsv" },

  "pools": {
    "<LVLI FormID>": {
      "edid":   "LL_QuestReward_...",
      "flags":  { "use_all": false, "for_each": false, ... },
      "simple": { "<leaf FormID>": <chance 0-1>, ... },
      "items":  [
        {
          "formid":     "00ABCDEF",
          "name":       "Plan: Something Cool",
          "qty":        1,
          "dropRate":   0.05,        // 0-1 probability
          "dropRatePct": 5.0,        // 0-100 percent
          "edid":       "Recipe_SomethingCool",
          "sig":        "BOOK",
          "conditions": []
        }
      ]
    }
  },

  "regionPools": {
    "<LVLI FormID>": [
      { "formid": "...", "chance": 0.05, "region": "Forest" }
    ]
  },

  "globs": {
    "<GLOB FormID>": {
      "value":    90.0,
      "dropRate": "10%",
      "edid":     "SomeGlobalChanceNone"
    }
  },

  "titleRates": {
    "<PLYT/CMPT EDID lower>": {
      "dropRate":    "Tier 1 - 5%\\nTier 2 - 10%\\n...",
      "howToObtain": "Open Crafted Holiday Gifts ..."   // only if overridden
    }
  }
}

Consumers:
  - build_events_rewards_json.py  → pools[fid].items / pools[fid].simple / regionPools[fid]
  - build_reho_json.py            → pools[fid].items (bounty hunts)
  - build_titles_json.py          → titleRates[edid] / globs[fid]
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Import the shared engine (rng76.py must be on sys.path or in the same dir)
# ---------------------------------------------------------------------------
# Try importing from same directory, then from src/
_this_dir = Path(__file__).resolve().parent
for p in [_this_dir, _this_dir / "src", _this_dir.parent / "src"]:
    if p.exists() and str(p) not in sys.path:
        sys.path.insert(0, str(p))

from rng76 import (
    Rng76Data,
    Rng76Resolver,
    LvliIndex,
    GlobIndex,
    CurvIndex,
    ItemNameIndex,
    REGION_BY_SUBLVLI_EDID,
    compute_chancenone_rate,
    glob_formid_from_lvli_field,
    tier_info_from_edid,
    fmt_pct,
    pct,
    read_tsv,
    newest,
    pick,
    safe_float,
    humanize_edid,
)


# ============================================================
# CONFIG
# ============================================================

# Resolve paths relative to the repo root (one level up from src/) so the
# script produces correct output regardless of which directory it's run from.
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_ROOT = str(_REPO_ROOT / "tsv")
DIST_DIR = _REPO_ROOT / "dist"
OUTPUT   = DIST_DIR / "drop_rates.json"

# Bounty hunt root LVLIs (from REHO page mappings)
BOUNTY_HUNT_LVLIS = {
    "007D6A6D",   # Grunt Hunt rewards
    "007EBDF3",   # Head Hunt rewards
}

# Container-loot seasonal event LVLIs (from CONTAINER_LOOT_EVENTS)
CONTAINER_LOOT_LVLIS = {
    # Treasure Hunter pails
    "005D805A", "005D8054", "005D8056", "005D8053", "005D8059", "005D8055",
    # Holiday Scorched gifts
    "005DCA88", "005DCA8A", "005DCA89", "005DCA85", "005DCA87", "005DCA86",
    # Spooky Scorched treat bags
    "0062038D",
}

# Title manual overrides (from build_titles_json.py)
HOW_TO_OBTAIN_OVERRIDES: Dict[str, str] = {
    "playertitles_suffix_zookeeper":
        "Complete the Event: Project Paradise\nCondition: Keep all 3 animals alive",
    "sfs09_playertitles_suffix_tamer":
        "Complete the Event: Project Paradise\nCondition: Keep all 3 animals alive",
    "playertitles_suffix_researcher":
        "Complete the Event: Project Paradise\nCondition: Shut down ARIC-4 (any number of animals alive)",
    "sfs09_playertitles_suffix_manager":
        "Complete the Event: Project Paradise\nCondition: Shut down ARIC-4 (any number of animals alive)",
    "playertitles_prefix_festive":
        "Open Crafted Holiday Gifts during the Holiday Scorched seasonal event",
    "playertitles_suffix_surveyor":
        "Open Crafted Mole Miner Pails during the Treasure Hunter seasonal event",
    "playertitles_prefix_spooky":
        "Open Crafted Spooky Treat Bags during the Spooky Scorched seasonal event",
    "camptitles_suffix_array":
        "Complete the Activity: Always Vigilant",
    "camptitles_suffix_battery":
        "Complete the Activity: Distant Thunder",
    "camptitles_suffix_crashsite":
        "Complete the Activity: Fly Swatter",
    "camptitles_suffix_bulwark":
        "Complete the Activity: AWOL Armaments",
    "camptitles_suffix_archive":
        "Complete the Activity: Census Violence",
    "camptitles_suffix_apiary":
        "Complete the Activity: Irrational Fear",
    "camptitles_prefix_fugitives":
        "Complete the Activity: Manhunt",
    "camptitles_prefix_leaders":
        "Complete the Activity: Leader of the Pack",
    "camptitles_lifetime_prefix_homestead":
        "Complete the Activity: Project Beanstalk",
    "camptitles_lifetime_both_precinct":
        "Complete the Activity: Back on the Beat",
    "camptitles_lifetime_both_forge":
        "Complete the Activity: Breach and Clear",
    "camptitles_lifetime_suffix_laboratory":
        "Complete the Activity: Fertile Soil",
    "camptitles_lifetime_prefix_electric":
        "Complete the Activity: Powering Up",
    "camptitles_lifetime_prefix_excavator":
        "Complete the Activity: Lucky Strike",
    "camptitles_lifetime_both_chapel":
        "Complete the Event: The Mothman Equinox",
    "camptitles_lifetime_both_junkyard":
        "Complete the Event: Neurological Warfare",
    "camptitles_lifetime_suffix_hideout":
        "Complete the Event: Bounty Hunting: Head Hunt",
    "camptitles_lifetime_both_sinkhole":
        "Complete the Event: Sinkhole Solutions",
    "camptitles_lifetime_suffix_cove":
        "Complete the Challenge: Catch Any Local Legend",
    "camptitles_lifetime_suffix_cryptidhunter":
        "Complete the Challenge: Kill Different Kinds of Cryptids",
    "camptitles_lifetime_both_workshop":
        "Complete the Challenge: Claim a Workshop",
    "camptitles_lifetime_suffix_settlement":
        "Complete the Challenge: Deploy C.A.M.P.s to settle Appalachia",
}

DROP_RATE_OVERRIDES: Dict[str, str] = {
    "playertitles_prefix_festive":
        "Loot Tier 1 - 0%\nLoot Tier 2 - 0%\nLoot Tier 3 - 0%\n"
        "Crafted Tier 1 - 5%\nCrafted Tier 2 - 10%\nCrafted Tier 3 - 25%",
    "playertitles_suffix_surveyor":
        "Loot Tier 1 - 0%\nLoot Tier 2 - 0%\nLoot Tier 3 - 0%\n"
        "Crafted Tier 1 - 5%\nCrafted Tier 2 - 10%\nCrafted Tier 3 - 25%",
    "playertitles_prefix_spooky":
        "Loot Tier 1 - 0%\nLoot Tier 2 - 0%\nLoot Tier 3 - 0%\n"
        "Crafted Tier 1 - 5%\nCrafted Tier 2 - 10%\nCrafted Tier 3 - 25%",
    "playertitles_suffix_rumbler": "10%",
    "e05_playertitles_suffix_scavenger": "10%",
}


# ============================================================
# HELPERS
# ============================================================

def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def find_gmrw_lvli_formids(tsv_root: str) -> Set[str]:
    """
    Scan GMRW TSV for RewardedItem entries that reference LVLIs.
    Returns the set of LVLI FormIDs found.
    """
    fids: Set[str] = set()
    try:
        gmrw_rows = read_tsv(newest(os.path.join(tsv_root, "GMRW_Export_*.tsv")))
    except FileNotFoundError:
        return fids

    for r in gmrw_rows:
        ref = (r.get("RewardedItem") or "").strip()
        if not ref:
            continue
        parts = ref.split(":")
        if len(parts) >= 3 and parts[-1].strip().upper() == "LVLI":
            fid = parts[0].strip()
            if re.fullmatch(r"[0-9A-Fa-f]{8}", fid):
                fids.add(fid.upper())
        elif len(parts) >= 2 and parts[-1].strip().upper() == "LVLI":
            fid = parts[0].strip()
            if re.fullmatch(r"[0-9A-Fa-f]{8}", fid):
                fids.add(fid.upper())
    return fids


def find_title_cobj_lvli_chains(
    tsv_root: str,
    lvli_index: LvliIndex,
    glob_index: GlobIndex,
    curv_index: CurvIndex = None,
) -> Dict[str, Dict[str, str]]:
    """
    Walk COBJ → BOOK → LVLI chains for player/camp titles and compute
    ChanceNone-based drop rates.

    Returns: { plyt_edid_lower: { "dropRate": "...", "howToObtain": "..." } }
    """
    results: Dict[str, Dict[str, str]] = {}

    # Load COBJ
    try:
        cobj_rows = read_tsv(newest(os.path.join(tsv_root, "COBJ_Export_*.tsv")))
    except FileNotFoundError:
        return results

    # Load PLYT + CMPT for EDID→title mapping
    plyt_by_edid: Dict[str, Dict[str, Any]] = {}
    try:
        plyt_rows = read_tsv(newest(os.path.join(tsv_root, "PLYT_Export_*.tsv")))
        for r in plyt_rows:
            edid = pick(r, "EDID - Editor ID", "EDID")
            if edid:
                plyt_by_edid[edid] = r
    except FileNotFoundError:
        pass

    cmpt_by_edid: Dict[str, Dict[str, Any]] = {}
    try:
        cmpt_rows = read_tsv(newest(os.path.join(tsv_root, "CMPT_Export_*.tsv")))
        for r in cmpt_rows:
            edid = pick(r, "EDID")
            if edid:
                cmpt_by_edid[edid] = r
    except FileNotFoundError:
        pass

    # Load BOOK for COBJ GNAM → BOOK EDID mapping
    try:
        book_files = [f for f in __import__("glob").glob(os.path.join(tsv_root, "BOOK_Export_*.tsv"))
                      if "_Locations" not in f]
        if not book_files:
            return results
        book_files.sort(key=lambda x: os.path.getmtime(x))
        book_rows = read_tsv(book_files[-1])
    except FileNotFoundError:
        return results

    book_by_formid: Dict[str, Dict[str, str]] = {}
    for r in book_rows:
        fid = pick(r, "BOOK_FormID", "FormID")
        if fid:
            book_by_formid[fid] = r

    # For each COBJ that creates a title BOOK, find the LVLI entry referencing
    # that BOOK and compute the ChanceNone-based rate.
    for cobj in cobj_rows:
        gnam = (cobj.get("GNAM_FormID") or "").strip().upper()
        if not gnam or not re.fullmatch(r"[0-9A-F]{8}", gnam):
            continue

        book_row = book_by_formid.get(gnam)
        if not book_row:
            continue

        book_edid = pick(book_row, "BOOK_EDID", "EDID")
        if not book_edid:
            continue

        # Match to PLYT or CMPT
        base = book_edid.replace("_Recipe_", "_")
        plyt_key = base.replace("Title_", "Titles_")
        cmpt_key = re.sub(r"(?i)CampTitle_", "CAMPTitles_", base)
        cmpt_key2 = base.replace("Title_", "Titles_")
        cmpt_key2 = re.sub(r"(?i)^camptitles_", "CAMPTitles_", cmpt_key2)

        title_edid = None
        if plyt_key in plyt_by_edid:
            title_edid = plyt_key
        elif cmpt_key in cmpt_by_edid:
            title_edid = cmpt_key
        elif cmpt_key2 in cmpt_by_edid:
            title_edid = cmpt_key2
        else:
            continue

        title_edid_lower = title_edid.lower()

        # Check manual override first
        if title_edid_lower in DROP_RATE_OVERRIDES:
            entry: Dict[str, str] = {"dropRate": DROP_RATE_OVERRIDES[title_edid_lower]}
            if title_edid_lower in HOW_TO_OBTAIN_OVERRIDES:
                entry["howToObtain"] = HOW_TO_OBTAIN_OVERRIDES[title_edid_lower]
            results[title_edid_lower] = entry
            continue

        # Find LVLI entry rows referencing this BOOK
        matches = [
            r for r in lvli_index.entry_rows
            if gnam in (r.get("LVLO_Reference") or "").upper()
        ]
        if not matches:
            continue

        # Group by LVLI FormID
        by_lvli: Dict[str, List[Dict[str, str]]] = {}
        for r in matches:
            fid = (r.get("LVLI_FormID") or r.get("FormID") or "").strip().upper()
            if fid and re.fullmatch(r"[0-9A-F]{8}", fid):
                by_lvli.setdefault(fid, []).append(r)

        # Check for tier families (resolve through parent map if needed)
        resolved_by_lvli: Dict[str, List[Dict[str, str]]] = dict(by_lvli)
        for direct_fid, entry_rows in list(by_lvli.items()):
            ed = lvli_index.edid_for(direct_fid)
            if tier_info_from_edid(ed) is not None:
                continue
            for parent_fid in lvli_index.parent_map.get(direct_fid, set()):
                parent_ed = lvli_index.edid_for(parent_fid)
                if tier_info_from_edid(parent_ed) is None:
                    continue
                if parent_fid not in resolved_by_lvli:
                    resolved_by_lvli[parent_fid] = entry_rows

        tier_hits: List[Tuple[str, str, int, str]] = []
        for fid in resolved_by_lvli:
            ed = lvli_index.edid_for(fid)
            info = tier_info_from_edid(ed)
            if info:
                fam, lab, order = info
                tier_hits.append((fam, lab, order, fid))

        rate_str: Optional[str] = None

        if tier_hits:
            # Multi-tier rate computation
            fam_counts: Dict[str, int] = {}
            for fam, _l, _o, _f in tier_hits:
                fam_counts[fam] = fam_counts.get(fam, 0) + 1
            best_family = sorted(fam_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

            # Discover all tiers in this family
            family_tiers: Dict[int, Tuple[str, str]] = {}
            family_named: Dict[str, str] = {}
            for lr in lvli_index.list_rows:
                fid = (lr.get("LVLI_FormID") or lr.get("FormID") or "").strip().upper()
                ed = (lr.get("LVLI_EDID") or lr.get("EDID") or "").strip()
                info = tier_info_from_edid(ed)
                if not info:
                    continue
                fam, lab, order = info
                if fam != best_family:
                    continue
                if lab.lower().startswith("tier ") or lab.lower().startswith("mutated tier "):
                    family_tiers[order] = (lab, fid)
                else:
                    family_named[lab] = fid

            parts: List[str] = []

            # Bad/Good/Best
            if family_named:
                for lab in ("Bad", "Good", "Best"):
                    fid = family_named.get(lab)
                    if not fid:
                        continue
                    if fid not in resolved_by_lvli:
                        parts.append(f"{lab} - 0%")
                        continue
                    entry_row = resolved_by_lvli[fid][0]
                    dr = compute_chancenone_rate(entry_row, fid, lvli_index, glob_index, curv_index)
                    parts.append(f"{lab} - {dr or 'N/A'}")

            # Numeric tiers
            if not parts and family_tiers:
                for order in sorted(family_tiers.keys()):
                    lab, fid = family_tiers[order]
                    if fid not in resolved_by_lvli:
                        parts.append(f"{lab} - 0%")
                        continue
                    entry_row = resolved_by_lvli[fid][0]
                    dr = compute_chancenone_rate(entry_row, fid, lvli_index, glob_index, curv_index)
                    parts.append(f"{lab} - {dr or 'N/A'}")

            if parts:
                rate_str = "\n".join(parts)

        if rate_str is None:
            # Single-rate: pick the best match
            def _rank(row):
                eg = (row.get("LVOG_ChanceNoneGlobal") or "").strip()
                return 0 if eg else 1
            matches.sort(key=_rank)
            best = matches[0]
            lvli_fid = (best.get("LVLI_FormID") or best.get("FormID") or "").strip().upper()
            rate_str = compute_chancenone_rate(best, lvli_fid, lvli_index, glob_index, curv_index)

        if rate_str:
            entry = {"dropRate": rate_str}
            if title_edid_lower in HOW_TO_OBTAIN_OVERRIDES:
                entry["howToObtain"] = HOW_TO_OBTAIN_OVERRIDES[title_edid_lower]
            results[title_edid_lower] = entry

    # Also add any manual overrides that weren't covered by COBJ chains
    for edid_lower, override_rate in DROP_RATE_OVERRIDES.items():
        if edid_lower not in results:
            entry = {"dropRate": override_rate}
            if edid_lower in HOW_TO_OBTAIN_OVERRIDES:
                entry["howToObtain"] = HOW_TO_OBTAIN_OVERRIDES[edid_lower]
            results[edid_lower] = entry

    for edid_lower, override_how in HOW_TO_OBTAIN_OVERRIDES.items():
        if edid_lower not in results:
            results[edid_lower] = {"howToObtain": override_how}
        elif "howToObtain" not in results[edid_lower]:
            results[edid_lower]["howToObtain"] = override_how

    return results


def collect_sub_lvlis(lvli_index: LvliIndex, root_fids: Set[str], max_depth: int = 8) -> Set[str]:
    """
    Given a set of root LVLI FormIDs, collect all sub-LVLIs referenced
    by their entries (recursively) so we can pre-resolve the full tree.
    """
    all_fids: Set[str] = set(root_fids)
    frontier = set(root_fids)
    for _ in range(max_depth):
        next_frontier: Set[str] = set()
        for fid in frontier:
            for entry in lvli_index.entries_by_list.get(fid, []):
                idx = entry.get("EntryIndex")
                if idx is None:
                    continue
                math = lvli_index.math_by_entry.get((fid, idx))
                if not math:
                    continue
                sub = (math.get("SubLVLI_FormID") or "").strip()
                if sub and sub not in all_fids:
                    next_frontier.add(sub)
                    all_fids.add(sub)
        if not next_frontier:
            break
        frontier = next_frontier
    return all_fids


# ============================================================
# MAIN
# ============================================================

def main():
    print("build_drop_rates.py — loading TSVs...")

    # 1) Load everything via the shared engine
    data = Rng76Data.from_tsv_root(TSV_ROOT)
    resolver = data.resolver
    lvli = data.lvli
    globs = data.globs
    names = data.names

    print(f"  LVLI lists: {len(lvli.list_by_formid)}")
    print(f"  LVLI entries: {len(lvli.entry_rows)}")
    print(f"  GLOB values: {len(globs.vals)}")

    # 2) Identify all "root" LVLIs that need deep resolution
    gmrw_lvlis = find_gmrw_lvli_formids(TSV_ROOT)
    print(f"  GMRW-referenced LVLIs: {len(gmrw_lvlis)}")

    root_lvlis = gmrw_lvlis | BOUNTY_HUNT_LVLIS | CONTAINER_LOOT_LVLIS
    # Also add any LVLI referenced by the entries of root LVLIs (for sub-pools)
    all_lvlis = collect_sub_lvlis(lvli, root_lvlis)
    print(f"  Total LVLIs to resolve (incl. sub-lists): {len(all_lvlis)}")

    # 3) Resolve each LVLI
    pools_out: Dict[str, Any] = {}
    region_pools_out: Dict[str, Any] = {}

    for fid in sorted(all_lvlis):
        edid = lvli.edid_for(fid)
        flags = lvli.flags_for(fid)

        # Simple resolve (cached probability map)
        simple = resolver.resolve_simple(fid)

        # Deep resolve (full items with names)
        items_raw = resolver.resolve_deep(fid)

        items_json = []
        for it in items_raw:
            dr = it["dropRate"]
            items_json.append({
                "formid":      it["formid"],
                "name":        it["name"],
                "qty":         it["qty"],
                "dropRate":    round(dr, 8),
                "dropRatePct": round(pct(dr) if dr <= 1.0 else dr * 100, 6),
                "edid":        it.get("edid", ""),
                "sig":         it.get("sig", ""),
                "conditions":  it.get("conditions", []),
            })

        # Simplify the simple dict: round values
        simple_clean = {k: round(v, 8) for k, v in simple.items() if v > 0}

        pools_out[fid] = {
            "edid":   edid,
            "flags":  flags,
            "simple": simple_clean,
            "items":  items_json,
        }

        # Region-aware resolve for regional schematics
        edid_lower = edid.lower()
        is_regional = (
            "regionalschematics" in edid_lower
            or "regional_schematics" in edid_lower
            or "allregions" in edid_lower
        )
        if is_regional and fid in root_lvlis:
            region_items = resolver.resolve_with_region(fid, REGION_BY_SUBLVLI_EDID)
            region_pools_out[fid] = [
                {
                    "formid": ri["formid"],
                    "chance": round(ri["chance"], 8),
                    "region": ri.get("region") or "",
                }
                for ri in region_items
            ]

    print(f"  Pools resolved: {len(pools_out)}")
    print(f"  Region pools: {len(region_pools_out)}")

    # 4) Build GLOB export
    globs_out: Dict[str, Any] = {}
    for fid, val in globs.vals.items():
        edid = globs.edids.get(fid, "")
        dr_pct = globs.drop_rate_pct(fid)
        dr_str = globs.drop_rate_str(fid)
        globs_out[fid] = {
            "value":    val,
            "dropRate": dr_str,
            "edid":     edid,
        }

    print(f"  GLOB entries: {len(globs_out)}")

    # 5) Build title rate export
    print("  Computing title drop rates...")
    title_rates = find_title_cobj_lvli_chains(TSV_ROOT, lvli, globs, data.curvs)
    print(f"  Title rates computed: {len(title_rates)}")

    # 6) Assemble output
    output = {
        "meta": {
            "built":   now_iso(),
            "tsvRoot": TSV_ROOT,
            "stats": {
                "pools":       len(pools_out),
                "regionPools": len(region_pools_out),
                "globs":       len(globs_out),
                "titleRates":  len(title_rates),
            },
        },
        "pools":       pools_out,
        "regionPools": region_pools_out,
        "globs":       globs_out,
        "titleRates":  title_rates,
    }

    # 7) Write output
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Wrote {OUTPUT}")
    print(f"  File size: {OUTPUT.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
