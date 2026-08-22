#!/usr/bin/env python3
"""Honeycomb spawn page build.

Runs the generic non-perishable farming build, then folds in the Honey Beast
creature (spawns, drops, challenges, the Big Bloom event, random encounters) via
honeycomb_honey_beast.inject — Honey Beast lives on this page, not a cryptid page.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_farming_spawns_json import main as farming_main
import honeycomb_honey_beast

_pts = ["--pts"] if "--pts" in sys.argv else []
farming_main(["--item", "honeycomb"] + _pts)
# Note: in CI the Honey Beast fold-in also runs from build_farming_used_for.py
# (the honeycomb-aware post-step); this call keeps the standalone build complete.
honeycomb_honey_beast.main(_pts)
