#!/usr/bin/env python3
"""
build_public_events_json.py — Public Events
STATUS: WRAPPER — the real build logic is in build_events_rewards_json.py.

The parent script (build_events_rewards_json.py) handles Seasonal Events, Public Events, Activities.
This wrapper exists so each category has its own entry point.
When the parent script is split in future, this file will hold
the Public Events-specific logic.

Usage: python build_public_events_json.py
"""
import subprocess, sys, os

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_events_rewards_json.py")
    print(f"[build_public_events_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
