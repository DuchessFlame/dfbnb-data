#!/usr/bin/env python3
"""Every internal link a builder emits must be a real page in guide_index.tsv.

WHY THIS EXISTS
---------------
The URL of a page is decided in four independent places:

  1. guide_index.tsv          - the source of truth; the WP importer in the child
                                theme creates and MOVES pages from it, matching on
                                the row id (_dfbnb_tsv_id post meta).
  2. nav.json                 - the menu, which must agree with (1).
  3. a builder config         - URL_BASE / region_index_base, hardcoded per family.
  4. the renderer's routing   - hardcoded path regexes, per family.

Nothing ever checked that the four agree. They drifted, silently:

  - The magazines guide linked its ten region pages at
    /df/farming/consumables/magazines/location-guide/<region>/ while the guide
    index had them at /magazines/<region>-magazines/. Every one of those links
    was dead, and no build ever said so.

A dead link is invisible in a diff of our own output - the JSON is well-formed
and the page renders; only the link is wrong. So it has to be checked against
the index, which is what this does.

WHAT IT CHECKS
--------------
One direction only: every /df/... or /bnb/... URL appearing anywhere in a live
dist JSON must exist as a row url in guide_index.tsv. The reverse is not an
error - plenty of pages are legitimately not linked from datamined output.

BASELINE
--------
There is pre-existing debt (61 links at the time of writing - stale meat guide
paths, a /bnb/farming/non-perishable/ prefix that should be /bnb/farming/non-perishable/,
and some "perishable-" slug typos). Failing on those immediately would just mean
the check gets skipped, so they live in check_guide_links_baseline.txt and only
NEW breakage fails the build. Shrink the baseline; never grow it.

    python src/check_guide_links.py              # fail on new breakage
    python src/check_guide_links.py --list       # show everything, baseline included
    python src/check_guide_links.py --baseline   # re-freeze (only when fixing debt)
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

GUIDE_INDEX = os.path.join(REPO, "tsv", "guide_index.tsv")
DIST = os.path.join(REPO, "dist")
BASELINE = os.path.join(HERE, "check_guide_links_baseline.txt")

URL_COL = 13
# A page path, not an asset or an anchor. Deliberately strict: anything with a
# file extension or a non-path character is not a page link.
PAGE_URL = re.compile(r"^/(?:df|bnb)/[A-Za-z0-9\-/_]*/?$")


def known_urls(path=GUIDE_INDEX):
    """Every internal page URL the guide index declares, with and without a
    trailing slash so a link is not reported dead over punctuation."""
    out = set()
    with open(path, encoding="utf-8") as f:
        rows = [l for l in f.read().split("\n") if l.strip()]
    for line in rows[1:]:
        cells = line.split("\t")
        if len(cells) <= URL_COL:
            continue
        u = cells[URL_COL].strip()
        if u.startswith("/"):
            out.add(u)
            out.add(u.rstrip("/") + "/")
            out.add(u.rstrip("/"))
    return out


def scan_dist(dist=DIST):
    """{url: {files that link to it}} for every page-looking URL in live dist.

    dist/pts/ is skipped: it is a preview mirror built from PTS exports and is
    expected to reference pages that do not exist on the live site yet.
    """
    found = collections.defaultdict(set)

    def walk(node, rel):
        if isinstance(node, dict):
            for v in node.values():
                walk(v, rel)
        elif isinstance(node, list):
            for v in node:
                walk(v, rel)
        elif isinstance(node, str):
            s = node.split("#")[0].split("?")[0]
            if PAGE_URL.match(s):
                found[s].add(rel)

    for dirpath, dirnames, filenames in os.walk(dist):
        dirnames[:] = [d for d in dirnames if d != "pts"]
        for fn in filenames:
            if not fn.endswith(".json"):
                continue
            full = os.path.join(dirpath, fn)
            try:
                with open(full, encoding="utf-8") as f:
                    doc = json.load(f)
            except (OSError, ValueError):
                continue
            walk(doc, os.path.relpath(full, REPO).replace("\\", "/"))
    return found


def load_baseline():
    if not os.path.exists(BASELINE):
        return set()
    with open(BASELINE, encoding="utf-8") as f:
        return {l.strip() for l in f
                if l.strip() and not l.lstrip().startswith("#")}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", action="store_true",
                    help="re-freeze the accepted-debt list from what is broken now")
    ap.add_argument("--list", action="store_true",
                    help="report every dead link, baseline included")
    args = ap.parse_args()

    known = known_urls()
    found = scan_dist()

    dead = {u: fs for u, fs in found.items()
            if u not in known and u.rstrip("/") + "/" not in known}

    if args.baseline:
        with open(BASELINE, "w", encoding="utf-8", newline="\n") as f:
            f.write("# Internal links in dist/ that are not pages in guide_index.tsv.\n"
                    "# Pre-existing debt, accepted so that NEW breakage is what fails\n"
                    "# the build. Shrink this list; never grow it.\n"
                    "# Regenerate: python src/check_guide_links.py --baseline\n")
            for u in sorted(dead):
                f.write(u + "\n")
        print(f"baseline written: {len(dead)} accepted dead link(s) -> "
              f"{os.path.relpath(BASELINE, REPO)}")
        return 0

    baseline = load_baseline()
    new = {u: fs for u, fs in dead.items() if u not in baseline}
    fixed = sorted(baseline - set(dead))

    print(f"guide-link check: {len(found)} distinct internal link(s) across dist/, "
          f"{len(known) // 3} pages in guide_index.tsv")

    if args.list and dead:
        print(f"\nAll {len(dead)} dead link(s) (including {len(baseline)} baselined):")
        for u in sorted(dead):
            tag = "" if u in baseline else "   <-- NEW"
            print(f"  {u}{tag}")
            for src in sorted(dead[u])[:3]:
                print(f"      from {src}")

    if fixed:
        print(f"\n{len(fixed)} baselined link(s) now resolve — "
              f"drop them with --baseline:")
        for u in fixed[:20]:
            print(f"  {u}")

    if not new:
        print(f"\nOK — no new dead links "
              f"({len(baseline)} pre-existing, tracked in "
              f"{os.path.basename(BASELINE)}).")
        return 0

    print(f"\nFAILED — {len(new)} link(s) point at pages that do not exist in "
          f"guide_index.tsv:\n")
    for u in sorted(new):
        print(f"  {u}")
        for src in sorted(new[u])[:4]:
            print(f"      linked from {src}")
    print("\nEither add the page to guide_index.tsv (and nav.json), or fix the "
          "builder that emits the link.\nA page's URL lives in guide_index.tsv — "
          "builders must agree with it, not invent their own.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
