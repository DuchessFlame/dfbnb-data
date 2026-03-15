#!/usr/bin/env python3
"""
build_lifetime_challenges_json.py — Lifetime Challenges
STATUS: WRAPPER — the real build logic is in build_challenges_json_v3.py.

The parent script (build_challenges_json_v3.py) handles Score Challenges, Lifetime Challenges,
Mini Seasons, Quests, Random Encounters, Pioneer Scouts.
This wrapper exists so each category has its own entry point.
When the parent script is split in future, this file will hold
the Lifetime Challenges-specific logic.

Usage: python build_lifetime_challenges_json.py
"""
import subprocess, sys, os

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_challenges_json_v3.py")
    print(f"[build_lifetime_challenges_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
