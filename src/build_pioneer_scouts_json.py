#!/usr/bin/env python3
"""
build_pioneer_scouts_json.py — Pioneer Scouts
STATUS: WRAPPER — the real build logic is in build_challenges_json_v3.py.

The parent script (build_challenges_json_v3.py) handles Score Challenges, Lifetime Challenges,
Mini Seasons, Quests, Random Encounters, Pioneer Scouts.
This wrapper exists so each category has its own entry point.
When the parent script is split in future, this file will hold
the Pioneer Scouts-specific logic.

Usage: python build_pioneer_scouts_json.py
"""
import subprocess, sys, os
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_challenges_json_v3.py")
    print(f"[build_pioneer_scouts_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))

    # Write empty patchlog feed after subprocess completes
    if result.returncode == 0:
        dist_dir = Path(__file__).parent.parent / "dist"
        write_empty_patchlog_feed(str(dist_dir), "patchlog_latest_df_scouts.json")

    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
