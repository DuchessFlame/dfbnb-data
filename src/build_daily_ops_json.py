#!/usr/bin/env python3
"""
build_daily_ops_json.py — Daily Ops All Rewards

Builds the Daily Ops page directly from dist/events/events_rewards.json (via the
shared events_page_pools helper) and enriches it with tradeable status from
BOOK + ARMO TSVs. Previously derived its pools from the retired dist/reho tree.

Two modes (mirrors build_hto_rewards_json.py):
  (default / live)  reads dist/events/... + tsv/     -> dist/daily_ops/daily_ops_rewards.json
  --pts             reads dist/pts/events/... + tsv/pts/ (falling back to the live
                    twins when a PTS twin is absent)  -> dist/pts/daily_ops/daily_ops_rewards.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the Daily Ops rewards page AND the Daily Ops guide load the right twin
automatically. In the dedicated PTS workflow (dfbnb-pts-build.yml) the whole
dist/ tree is relocated to dist/pts/ after a normal (non --pts) run, so this
flag is only needed for the live patch build's inline PTS twin step.

Tradeable logic (same pattern as build_titles_json.py):
  - BOOK TSV:  Plans/recipes. If row contains NonPlayerTradeable / NonPlayerTradable
               / UnsellableObject → non-tradeable, else tradeable.
  - ARMO TSV:  Apparel/outfits. Same keyword check.
  - Items not found in either TSV get no tradeable field.

Usage:
  python build_daily_ops_json.py            # live
  python build_daily_ops_json.py --pts      # PTS twin

Requires: build_events_rewards_json.py to have run first (needs
dist/events/events_rewards.json, or dist/pts/events/events_rewards.json for --pts).
"""

import json
import csv
import os
import sys
import glob
from pathlib import Path
from typing import Dict, List

from events_page_pools import load_events_index, build_page_from_events
from patchlog_utils import write_patchlog_feed
import tsv_source          # one resolver for every export selection

PTS = "--pts" in sys.argv


# Daily Ops page config (was PAGE_MAPPINGS["daily-ops-all-rewards"] in build_reho_json.py).
# timerGlobs are retained for parity; the timer-tier meta is emitted by the helper.
DAILY_OPS_CONFIG = {
    "name": "Daily Ops",
    "questFormID": "005A77D4",
    "pageType": "dailyops",
    "path": "/df/daily-ops/daily-ops-all-rewards",
    "timerGlobs": {
        "elder": ("005CB976", 480),
        "paladin": ("005CB977", 720),
        "knight": ("005CB978", 960),
    },
}


# ---------------------------------------------------------------------------
# TSV HELPERS
# ---------------------------------------------------------------------------

def read_tsv(path: str) -> List[Dict[str, str]]:
    """Read a TSV file, trying UTF-8-sig first then cp1252."""
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))


def is_non_tradeable(row: Dict[str, str]) -> bool:
    """
    Check whether a TSV row contains keywords indicating non-tradeable.

    Matches the logic in build_titles_json.py book_tradeable_map():
    - NonPlayerTradeable (common Bethesda naming)
    - NonPlayerTradable  (variant spelling)
    - UnsellableObject   (also treated as non-tradeable)
    """
    blob = " ".join(str(v) for v in row.values() if v).lower()
    return (
        "nonplayertradeable" in blob
        or "nonplayertradable" in blob
        or "unsellableobject" in blob
    )


def latest_tsv(tsv_dir: Path, pattern: str, exclude_keywords=None) -> str | None:
    """Return the newest TSV matching a glob pattern, optionally excluding filenames."""
    exclude_keywords = [kw.lower() for kw in (exclude_keywords or [])]
    candidates = sorted(
        glob.glob(str(tsv_dir / pattern)),
        key=tsv_source.export_key,
    )
    if exclude_keywords:
        candidates = [
            p for p in candidates
            if not any(kw in os.path.basename(p).lower() for kw in exclude_keywords)
        ]
    return candidates[-1] if candidates else None


# ---------------------------------------------------------------------------
# TRADEABLE MAP BUILDER
# ---------------------------------------------------------------------------

def build_tradeable_map(tsv_dir: Path) -> Dict[str, bool]:
    """
    Build a FormID → bool tradeable map from the latest BOOK and ARMO TSVs.

    BOOK rows cover plans/recipes (isPlan=true items).
    ARMO rows cover apparel/outfit drops (isPlan=false items).
    """
    tradeable: Dict[str, bool] = {}

    # --- BOOK TSV (plans/recipes) ---
    book_path = latest_tsv(tsv_dir, "BOOK_Export_*.tsv", exclude_keywords=["location"])
    if book_path:
        count = 0
        for row in read_tsv(book_path):
            fid = (row.get("FormID") or "").strip().upper()
            if not fid:
                continue
            tradeable[fid] = not is_non_tradeable(row)
            count += 1
        print(f"  BOOK tradeable: {count} entries from {os.path.basename(book_path)}")
    else:
        print("  Warning: No BOOK TSV found")

    # --- ARMO TSV (apparel/outfits) ---
    armo_path = latest_tsv(
        tsv_dir, "ARMO_Export_*.tsv",
        exclude_keywords=["objecttemplate", "slot"],
    )
    if armo_path:
        count = 0
        for row in read_tsv(armo_path):
            # Handle both column naming conventions:
            #   Dec 2025 format: "FormID"
            #   March 2026 format: "ARMO_FormID"
            fid = (
                row.get("FormID")
                or row.get("ARMO_FormID")
                or ""
            ).strip().upper()
            if not fid:
                continue
            # BOOK takes priority — only add if not already set
            if fid not in tradeable:
                tradeable[fid] = not is_non_tradeable(row)
                count += 1
        print(f"  ARMO tradeable: {count} new entries from {os.path.basename(armo_path)}")
    else:
        print("  Warning: No ARMO TSV found")

    print(f"  Total tradeable map: {len(tradeable)} entries")
    return tradeable


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    base = Path(os.path.dirname(os.path.dirname(__file__)))

    # --- PTS-aware path resolution (mirrors build_hto_rewards_json.py) ---
    # TSV: prefer tsv/pts when running --pts and it exists, else tsv/.
    tsv_dir = base / "tsv" / "pts"
    if not PTS or not tsv_dir.exists():
        tsv_dir = base / "tsv"

    # Events input: prefer the PTS twin when running --pts and it exists, else
    # fall back to the live twin (harmless during the live patch build, where
    # dist/pts/events is not produced — the twin simply mirrors live until the
    # dedicated PTS build regenerates it from tsv/pts).
    events_path = base / "dist" / "pts" / "events" / "events_rewards.json"
    if not PTS or not events_path.exists():
        events_path = base / "dist" / "events" / "events_rewards.json"

    out_dir = (base / "dist" / "pts" / "daily_ops") if PTS else (base / "dist" / "daily_ops")
    out_file = out_dir / "daily_ops_rewards.json"

    print(f"Building Daily Ops rewards JSON... (mode={'PTS' if PTS else 'LIVE'})")
    print(f"  TSV dir   : {tsv_dir}")
    print(f"  Events in : {events_path}")
    print(f"  Out dir   : {out_dir}")

    # 1. Build the Daily Ops page directly from events_rewards.json
    if not events_path.exists():
        print(f"  Error: {events_path} not found — run build_events_rewards_json.py first")
        return 1

    slug = "daily-ops-all-rewards"
    events_index = load_events_index(events_path)
    page_data = build_page_from_events(events_index, DAILY_OPS_CONFIG)

    if not page_data:
        print(f"  Error: No data for '{slug}' built from events_rewards.json")
        return 1

    print(f"  Built Daily Ops data: {len(page_data.get('pools', []))} pools")

    # 2. Build tradeable map from BOOK + ARMO TSVs
    tradeable_map = build_tradeable_map(tsv_dir)

    # 3. Enrich items with tradeable field
    enriched = 0
    for pool in page_data.get("pools", []):
        for item in pool.get("items", []):
            fid = (item.get("formid") or item.get("formId") or "").strip().upper()
            if fid in tradeable_map:
                item["tradeable"] = tradeable_map[fid]
                enriched += 1

    print(f"  Enriched {enriched} items with tradeable status")

    # 4. Write output
    out_dir.mkdir(parents=True, exist_ok=True)

    output = {
        "slug": slug,
        "pageType": page_data.get("pageType", "dailyops"),
        "name": page_data.get("name", "Daily Ops"),
        "meta": page_data.get("meta", {}),
        "pools": page_data.get("pools", []),
        "baseRewards": page_data.get("baseRewards", {}),
    }
    if PTS:
        output["isPts"] = True

    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Wrote {out_file}")

    # Generate patchlog feed
    def extract_items_from_output(data):
        """Flatten all items from pools."""
        items = []
        for pool in data.get("pools", []):
            items.extend(pool.get("items", []))
        return items

    write_patchlog_feed(
        dist_dir=str(out_dir),
        feed_name="patchlog_latest_df_daily_ops_rewards.json",
        current_items=extract_items_from_output(output),
        key_field="formId",
        name_field="name",
        compare_fields=["name", "category", "rarity"],
        prev_json_path=str(out_file),
        items_extractor=extract_items_from_output,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
