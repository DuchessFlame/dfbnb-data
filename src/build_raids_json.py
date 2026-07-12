#!/usr/bin/env python3
"""
build_raids_json.py — Raids (Gleaming Depths) All Rewards

Builds dist/raids/raids_rewards.json from dist/events/events_rewards.json,
using the shared events_page_pools helper. Covers the five Gleaming Depths
stage "all rewards" pages. Replaces the old wrapper that delegated to the
retired build_reho_json.py.

Output shape (unchanged from the renderer's point of view):
    { "byPage": { "<slug>": {...page...}, "<path>": {...page...}, ... } }
keyed by both slug and path so df-bnb-raids.js's byPage[slug] lookup and its
path fallback both resolve.

Two modes (mirrors build_daily_ops_json.py):
  (default / live)  reads dist/events/...          -> dist/raids/raids_rewards.json
  --pts             reads dist/pts/events/... (falling back to the live twin
                    when a PTS twin is absent)      -> dist/pts/raids/raids_rewards.json

The global PTS toggle (df-bnb-pts.js) rewrites dist/ -> dist/pts/ at fetch time.
In the dedicated PTS workflow the whole dist/ tree is relocated to dist/pts/
after a normal (non --pts) run, so this flag is only needed for the live patch
build's inline PTS twin step.

Usage:
  python build_raids_json.py            # live
  python build_raids_json.py --pts      # PTS twin

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
    "gleaming-depths-stage-1-all-rewards": {
        "name": "Gleaming Depths Stage 1",
        "questFormID": "00772A47",
        "pageType": "raid",
        "path": "/df/raids/gleaming-depths/gleaming-depths-stage-1-all-rewards",
        "stage": 1,
        "speedrunSeconds": 148,
    },
    "gleaming-depths-stage-2-all-rewards": {
        "name": "Gleaming Depths Stage 2",
        "questFormID": "0078F7A1",
        "pageType": "raid",
        "path": "/df/raids/gleaming-depths/gleaming-depths-stage-2-all-rewards",
        "stage": 2,
        "speedrunSeconds": 220,
    },
    "gleaming-depths-stage-3-all-rewards": {
        "name": "Gleaming Depths Stage 3",
        "questFormID": "0078B59E",
        "pageType": "raid",
        "path": "/df/raids/gleaming-depths/gleaming-depths-stage-3-all-rewards",
        "stage": 3,
        "speedrunSeconds": 139,
    },
    "gleaming-depths-stage-4-all-rewards": {
        "name": "Gleaming Depths Stage 4",
        "questFormID": "00788127",
        "pageType": "raid",
        "path": "/df/raids/gleaming-depths/gleaming-depths-stage-4-all-rewards",
        "stage": 4,
        "speedrunSeconds": 254,
    },
    "gleaming-depths-stage-5-all-rewards": {
        "name": "Gleaming Depths Stage 5",
        "questFormID": "00786D41",
        "pageType": "raid",
        "path": "/df/raids/gleaming-depths/gleaming-depths-stage-5-all-rewards",
        "stage": 5,
        "speedrunSeconds": 134,
    },
}


def main() -> int:
    base = Path(os.path.dirname(os.path.dirname(__file__)))

    events_path = base / "dist" / "pts" / "events" / "events_rewards.json"
    if not PTS or not events_path.exists():
        events_path = base / "dist" / "events" / "events_rewards.json"

    out_dir = (base / "dist" / "pts" / "raids") if PTS else (base / "dist" / "raids")
    out_file = out_dir / "raids_rewards.json"

    print(f"Building Raids rewards JSON... (mode={'PTS' if PTS else 'LIVE'})")
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

    # Preserve the wrapper's feed name (empty feed carrying the current count),
    # written into the page's own dist dir to match build_daily_ops_json.py.
    write_empty_patchlog_feed(str(out_dir), "patchlog_latest_df_raids.json", total_items)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
