#!/usr/bin/env python3
"""
build_seasonal_events_json.py — Seasonal Events
STATUS: WRAPPER — the real build logic is in build_events_rewards_json.py.

The parent script (build_events_rewards_json.py) handles Seasonal Events, Public Events, Activities.
This wrapper exists so each category has its own entry point.
When the parent script is split in future, this file will hold
the Seasonal Events-specific logic.

Usage: python build_seasonal_events_json.py
"""
import subprocess, sys, os
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_events_rewards_json.py")
    print(f"[build_seasonal_events_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))

    # Write empty patchlog feed after subprocess completes
    if result.returncode == 0:
        dist_dir = Path(__file__).parent.parent / "dist"
        write_empty_patchlog_feed(str(dist_dir), "patchlog_latest_df_seasonal_events.json")

    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
