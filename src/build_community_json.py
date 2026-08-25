#!/usr/bin/env python3
"""
build_community_json.py
=======================
Builds dist/community.json for the /community/ pages on buffsnbrew.com
and theduchessflame.com (rendered by df-bnb-community.js in the
dfbnb-child theme).

This file carries PAGE COPY only — titles and blurbs.

The community group list itself does NOT live here. It lives in the
theme, in:

    dfbnb-child/assets/df-bnb-community-groups.js

as `window.__DFBNB_GROUPS.list`. That is deliberate: each group card
carries an inline SVG platform icon, which does not round-trip through
JSON cleanly, and the same list is shared with /about/follow/. Keeping
one copy in one file is what stops the two pages drifting apart.

So: to add or edit a GROUP, edit df-bnb-community-groups.js.
    To change the page TITLE or BLURB, edit this file.

Output:
  dist/community.json — keyed by page slug (hyphens, matching the URL):
    {
      "community-groups":   { "title": ..., "subtitle": ... },
      "safe-trading-guide": { "title": ..., "subtitle": ... }
    }

Usage:
  python src/build_community_json.py
  python src/build_community_json.py --outdir dist
"""

import argparse
import json
import sys
from pathlib import Path


PLACEHOLDER_SUBTITLE = "Coming soon."

COMMUNITY_GROUPS = {
    "title": "Community Groups",
    "subtitle": (
        "Depending on your platform, where you live and how you like to "
        "play, there's a community here to suit you. These are the groups "
        "and servers I run or recommend — sorted by the platform they're "
        "for, so start with your own and work down."
    ),
}


def build() -> dict:
    community = {}

    community["community-groups"] = COMMUNITY_GROUPS

    # Pages without real content yet — keyed by URL slug; the JS
    # dispatches by slug. Keep this loop style so a missing page never
    # crashes the build.
    for slug, title in [
        ("safe-trading-guide", "Safe Trading Guide"),
    ]:
        community[slug] = {
            "title": title,
            "subtitle": PLACEHOLDER_SUBTITLE,
        }

    return community


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="dist", help="Output directory")
    args = ap.parse_args()

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "community.json"

    data = build()

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {out_path}")
    for k in sorted(data):
        print(f"  {k:<20} title={data[k]['title']!r}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
