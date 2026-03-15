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

def main():
    parent_mjs = os.path.join(os.path.dirname(__file__), "..", "tools", "build-plan-master.mjs")
    print(f"[build_plan_checklists_json.py] Delegating to {parent_mjs}")
    result = subprocess.run(["node", parent_mjs], cwd=os.path.dirname(os.path.dirname(__file__)))
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
