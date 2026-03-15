#!/usr/bin/env python3
"""
build_raids_json.py — Raids
STATUS: WRAPPER — the real build logic is in build_reho_json.py.

The parent script (build_reho_json.py) handles Daily Ops, Expos, Raids, Bounty Hunting.
This wrapper exists so each category has its own entry point.
When the parent script is split in future, this file will hold
the Raids-specific logic.

Usage: python build_raids_json.py
"""
import subprocess, sys, os

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_reho_json.py")
    print(f"[build_raids_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
