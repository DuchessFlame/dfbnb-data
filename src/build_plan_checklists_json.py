#!/usr/bin/env python3
"""
build_plan_checklists_json.py — Plan Checklists
STATUS: WRAPPER — the real build logic is in tools/build-plan-master.mjs.

The parent script (tools/build-plan-master.mjs) handles Plan Checklists.
This wrapper exists so each category has its own entry point.
When the parent script is refactored in future, this file will hold
the Plan Checklists-specific logic.

Note: The parent is a Node.js .mjs file, not Python.

Usage: python build_plan_checklists_json.py
"""
import subprocess, sys, os
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

def main():
    parent_mjs = os.path.join(os.path.dirname(__file__), "..", "tools", "build-plan-master.mjs")
    print(f"[build_plan_checklists_json.py] Delegating to {parent_mjs}")
    result = subprocess.run(["node", parent_mjs], cwd=os.path.dirname(os.path.dirname(__file__)))

    # Write empty patchlog feeds after subprocess completes (two feeds for df and bnb)
    if result.returncode == 0:
        dist_dir = Path(__file__).parent.parent / "dist"
        write_empty_patchlog_feed(str(dist_dir), "patchlog_latest_df_plan_checklists.json")
        write_empty_patchlog_feed(str(dist_dir), "patchlog_latest_bnb_plan_checklists.json")

    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
