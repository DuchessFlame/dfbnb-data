#!/usr/bin/env python3
"""Check every Unique Weapons & Armour source line against the game files.

Added 12 Oct 2026 after Meadow Breeze Sprayer and Prototype ABX03 shipped as
"Quest: Shadow over Appalachia", a line copied from their BASE weapon in the
old hand-built list. Same test the build uses (uwa_source_links.py): does the
named quest's reward tree reach the unique's OWN leveled list?

    python3 src/check_uwa_sources.py            # live
    python3 src/check_uwa_sources.py --channel pts

Run it after a build. Exit code 1 when a published line is contradicted by
the files (the build should already have dropped those — if this fires, a
source was set after the check, by a pass that needs the same test).
Read-only.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_unique_weapons_armour_json as B   # noqa: E402
import uwa_source_links                        # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", default="live", choices=["live", "pts"])
    ch = ap.parse_args().channel
    path = B.REPO_ROOT / "dist" / ("pts" if ch == "pts" else "") / \
        "unique_weapons_armour" / "unique_weapons_armour.json"
    items = json.load(open(path, encoding="utf-8"))["items"]
    links = uwa_source_links.SourceLinks(lambda p: B.pick(p, ch), B.read_tsv,
                                         B.strip_q, B.norm_formid)
    own_lists, _find = B._event_lists(ch)
    linked, bad, unknown, no_list = 0, [], [], []
    for it in items:
        key = B.norm_name(it.get("name"))
        own = own_lists.get(key) or B.OWN_LIST_ALIAS.get(key)
        how = str(it.get("howToObtain") or "")
        res = links.check(how, own)
        linked += len(res["linked"])
        bad += [(it["name"], q, own) for q in res["contradicted"]]
        if own:
            unknown += [(it["name"], q) for q in res["unknown"]]
        else:
            no_list += [(it["name"], q) for q in res["unknown"]]
    print(f"Linked in the game files: {linked}")
    print(f"CONTRADICTED — the quest exists but never hands out the item's own list: {len(bad)}")
    for n, q, own in bad:
        print(f"  {n}: '{q}' never reaches {own}")
    print(f"Not a quest name in the files (event page / hand line — check by eye): {len(unknown)}")
    for n, q in unknown:
        print(f"  {n}: '{q}'")
    print(f"No own leveled list to test against: {len(no_list)}")
    for n, q in no_list:
        print(f"  {n}: '{q}'")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
