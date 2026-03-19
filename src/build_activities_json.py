#!/usr/bin/env python3
"""
build_activities_json.py — Activities
Delegates to build_activities_rewards_json.py which handles the full build pipeline.

Usage: python build_activities_json.py
"""
import subprocess, sys, os

def main():
    parent = os.path.join(os.path.dirname(__file__), "build_activities_rewards_json.py")
    print(f"[build_activities_json.py] Delegating to {parent}")
    result = subprocess.run([sys.executable, parent], cwd=os.path.dirname(os.path.dirname(__file__)))
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
