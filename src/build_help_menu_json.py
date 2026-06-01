#!/usr/bin/env python3
"""
build_help_menu_json.py
========================
Generates dist/help_menu.json from the xEdit MESG TSV export.

Input file (in tsv/ folder or pass via --data-dir):
  MESG_Help_Export*.tsv

Output:
  dist/help_menu.json — alphabetised help menu entries for buffsnbrew.com

The script is fully generative — any new Help MESG records added to the
TSV are automatically picked up without code changes.

Usage:
  python build_help_menu_json.py
  python build_help_menu_json.py --data-dir tsv --out dist/help_menu.json
"""

import csv
import json
import os
import re
import sys
import argparse
from collections import defaultdict

from patchlog_utils import diff_item_lists, _write_json, _git_show_json


# ─────────────────────────────────────────────────────────────────────────────
#  PLATFORM HANDLING
# ─────────────────────────────────────────────────────────────────────────────
#
# Some Help entries have _Console and _PC variants with the same FULL name.
# We merge these into a single entry and tag the platform.

def _detect_platform(edid):
    """Return ('Console', edid) or ('PC', edid) or (None, edid)."""
    if edid.endswith("_Console") or edid.endswith("tConsole"):
        return "Console", edid
    if edid.endswith("_PC") or edid.endswith("tPC"):
        return "PC", edid
    return None, edid


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN BUILD
# ─────────────────────────────────────────────────────────────────────────────

def build_help_menu(tsv_path):
    """Read the MESG Help TSV and return the structured JSON data."""

    items = []
    raw_count = 0

    with open(tsv_path, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            form_id = row.get("MESG_FormID", "").strip().strip('"')
            edid    = row.get("MESG_EDID", "").strip().strip('"')
            name    = row.get("FULL_Name", "").strip().strip('"')
            desc    = row.get("DESC_Description", "").strip().strip('"')

            if not form_id or not edid:
                continue

            # Only include Help entries
            if not edid.startswith("Help"):
                continue

            raw_count += 1

            # Normalise empties to None
            if not name:
                name = None
            if not desc:
                desc = None

            # Skip entries with no name and no description
            if not name and not desc:
                continue

            platform, _ = _detect_platform(edid)

            item = {
                "formId": form_id,
                "edid":   edid,
                "name":   name or _name_from_edid(edid),
                "desc":   desc,
            }
            if platform:
                item["platform"] = platform

            items.append(item)

    # Sort alphabetically by name, then platform (Console before PC before universal)
    def sort_key(x):
        plat = x.get("platform", "")
        plat_order = {"Console": 0, "PC": 1}.get(plat, 2)
        return ((x["name"] or "").lower(), plat_order)

    items.sort(key=sort_key)

    return {
        "generated":  __import__("datetime").date.today().isoformat(),
        "totalCount": len(items),
        "rawCount":   raw_count,
        "items":      items,
    }


def _name_from_edid(edid):
    """
    Fallback: derive a display name from the EDID.
    Strips 'Help' prefix and CamelCase-splits.
    """
    e = edid
    if e.startswith("Help"):
        e = e[4:]
    # CamelCase split
    e = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', e)
    e = e.replace("_", " ")
    e = re.sub(r'\s+', ' ', e).strip()
    return e


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate help_menu.json from MESG Help TSV export"
    )
    parser.add_argument(
        "--data-dir", default="tsv",
        help="Folder containing TSV files (default: tsv)"
    )
    parser.add_argument(
        "--out", default="dist/help_menu.json",
        help="Output JSON path (default: dist/help_menu.json)"
    )
    parser.add_argument(
        "--tsv", default=None,
        help="Override MESG TSV path directly"
    )
    args = parser.parse_args()

    # Resolve TSV path
    def find_tsv(directory, keyword):
        for fname in sorted(os.listdir(directory), reverse=True):
            if keyword.lower() in fname.lower() and fname.endswith(".tsv"):
                return os.path.join(directory, fname)
        raise FileNotFoundError(
            f'No TSV matching "{keyword}" in {directory}'
        )

    tsv_path = args.tsv or find_tsv(args.data_dir, "MESG_Help")
    print(f"MESG Help TSV: {tsv_path}")

    print("Building help menu data…")
    data = build_help_menu(tsv_path)
    print(f"  {data['totalCount']} merged entries from {data['rawCount']} raw records")

    # Write output
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)

    size_kb = os.path.getsize(args.out) / 1024
    print(f"\nSaved → {args.out}  ({size_kb:.1f} KB)")

    # ── Patchlog feed ──────────────────────────────────────────────────────
    dist_base = os.path.dirname(args.out) or "dist"
    prev_json = _git_show_json("HEAD^", args.out)

    def extract_all_items(d):
        if not d:
            return []
        return d.get("items", [])

    entry = diff_item_lists(
        prev_items=extract_all_items(prev_json),
        curr_items=extract_all_items(data),
        key_field="formId",
        name_field="name",
        compare_fields=["name", "desc"],
    )
    feed = {"entries": [entry]}
    feed_path = os.path.join(dist_base, "patchlog_latest_help_menu.json")
    _write_json(feed_path, feed)
    a, r, c = len(entry["added"]), len(entry["removed"]), len(entry["changed"])
    print(
        f"[patchlog] patchlog_latest_help_menu.json: "
        f"current={entry['current']}  added={a}  removed={r}  changed={c}"
    )


if __name__ == "__main__":
    main()
