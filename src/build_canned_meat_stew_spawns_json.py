#!/usr/bin/env python3
"""Thin wrapper — delegates to the generic build_farming_spawns_json.py."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_farming_spawns_json import main
main(["--item", "canned-meat-stew"] + (["--pts"] if "--pts" in sys.argv else []))
