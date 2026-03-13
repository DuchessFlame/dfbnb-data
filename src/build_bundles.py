#!/usr/bin/env python3
"""
src/build_bundles.py
Validates src/bundles.json and writes a clean copy to dist/bundles.json.
No external dependencies — runs on stdlib only.
"""

import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC  = os.path.join(SCRIPT_DIR, "bundles.json")
DIST = os.path.join(SCRIPT_DIR, "..", "dist", "bundles.json")

# ---- Load ----
try:
    with open(SRC, "r", encoding="utf-8") as f:
        raw = f.read()
except FileNotFoundError:
    print(f"[bundles] Cannot read {SRC}", file=sys.stderr)
    sys.exit(1)

try:
    data = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"[bundles] JSON parse error in {SRC}: {e}", file=sys.stderr)
    sys.exit(1)

# ---- Validate ----
items = data.get("items", [])
if not isinstance(items, list) or not items:
    print("[bundles] No items found in src/bundles.json", file=sys.stderr)
    sys.exit(1)

errors = 0
for i, item in enumerate(items):
    if not item.get("name"):
        print(f"[bundles] Item {i} missing name", file=sys.stderr)
        errors += 1
    if item.get("isBundle") and not item.get("bundleItems"):
        print(f'[bundles] Bundle "{item.get("name", "?")}" has no bundleItems', file=sys.stderr)

if errors:
    print(f"[bundles] {errors} validation error(s). Aborting.", file=sys.stderr)
    sys.exit(1)

# ---- Write dist ----
os.makedirs(os.path.dirname(DIST), exist_ok=True)
with open(DIST, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
    f.write("\n")

print(f"[bundles] OK — {len(items)} items written to dist/bundles.json")
