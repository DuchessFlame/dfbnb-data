#!/usr/bin/env python3
"""
build_expos_json.py — Expeditions (Expos) All Rewards

Builds dist/expos/expos_rewards.json from dist/events/events_rewards.json,
using the shared events_page_pools helper. Covers the Atlantic City and
The Pitt expedition "all rewards" pages. Replaces the old wrapper that
delegated to the retired build_reho_json.py.

Output shape (unchanged from the renderer's point of view):
    { "byPage": { "<slug>": {...page...}, "<path>": {...page...}, ... } }
keyed by both slug and path so df-bnb-expos.js's byPage[slug] lookup and its
path fallback both resolve.

Two modes (mirrors build_daily_ops_json.py):
  (default / live)  reads dist/events/...          -> dist/expos/expos_rewards.json
  --pts             reads dist/pts/events/... (falling back to the live twin
                    when a PTS twin is absent)      -> dist/pts/expos/expos_rewards.json

Usage:
  python build_expos_json.py            # live
  python build_expos_json.py --pts      # PTS twin

Requires: build_events_rewards_json.py to have run first.
"""

import json
import os
import sys
from pathlib import Path

from events_page_pools import load_events_index, build_page_from_events
from patchlog_utils import write_empty_patchlog_feed

PTS = "--pts" in sys.argv


PAGE_MAPPINGS = {
    "atlantic-city-expos-all-rewards": {
        "name": "Atlantic City Expos",
        "questFormID": "006BAA3D",
        "pageType": "expedition",
        "path": "/df/expos/atlantic-city/atlantic-city-expos-all-rewards",
    },
    "pitt-expos-all-rewards": {
        "name": "The Pitt Expos",
        "questFormID": "006274EC",
        "pageType": "expedition",
        "path": "/df/expos/the-pitt/pitt-expos-all-rewards",
    },
}


def main() -> int:
    base = Path(os.path.dirname(os.path.dirname(__file__)))

    events_path = base / "dist" / "pts" / "events" / "events_rewards.json"
    if not PTS or not events_path.exists():
        events_path = base / "dist" / "events" / "events_rewards.json"

    out_dir = (base / "dist" / "pts" / "expos") if PTS else (base / "dist" / "expos")
    out_file = out_dir / "expos_rewards.json"

    print(f"Building Expos rewards JSON... (mode={'PTS' if PTS else 'LIVE'})")
    print(f"  Events in : {events_path}")
    print(f"  Out       : {out_file}")

    if not events_path.exists():
        print(f"  Error: {events_path} not found — run build_events_rewards_json.py first")
        return 1

    events_index = load_events_index(events_path)

    output = {"byPage": {}}
    total_items = 0
    for slug, config in PAGE_MAPPINGS.items():
        print(f"Processing {slug}...")
        page_data = build_page_from_events(events_index, config)
        if page_data:
            output["byPage"][slug] = page_data
            output["byPage"][config["path"]] = page_data
            total_items += sum(len(p.get("items", [])) for p in page_data.get("pools", []))
    if PTS:
        output["isPts"] = True

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Wrote {out_file}")
    print(f"✓ Pages: {len(PAGE_MAPPINGS)}  Items: {total_items}")

    # Preserve the wrapper's two feed names (empty feeds), written into the
    # page's own dist dir to match build_daily_ops_json.py.
    write_empty_patchlog_feed(str(out_dir), "patchlog_latest_df_expos_atlantic_city_rewards.json", total_items)
    write_empty_patchlog_feed(str(out_dir), "patchlog_latest_df_expos_pitt_rewards.json", total_items)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
