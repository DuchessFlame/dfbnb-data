#!/usr/bin/env python3
"""
build_expos_json.py — Expos
STATUS: WRAPPER — the real build logic is in build_reho_json.py.

The parent script (build_reho_json.py) handles Daily Ops, Expos, Raids, Bounty Hunting.
This wrapper exists so each category has its own entry point.
When the parent script is split in future, this file will hold
the Expos-specific logic.

Usage: python build_expos_json.py
"""
import subprocess, sys, os
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_reho_json.py")
    print(f"[build_expos_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))

    # Write empty patchlog feeds after subprocess completes (Expos has two feeds)
    if result.returncode == 0:
        dist_dir = Path(__file__).parent.parent / "dist"
        write_empty_patchlog_feed(str(dist_dir), "patchlog_latest_df_expos_atlantic_city_rewards.json")
        write_empty_patchlog_feed(str(dist_dir), "patchlog_latest_df_expos_pitt_rewards.json")

    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
