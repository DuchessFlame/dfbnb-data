#!/usr/bin/env python3
"""
season_guide_titles.py
----------------------
Puts an announced season's NAME into the Scoreboards rows of tsv/guide_index.tsv,
so the Guide dropdown, the Scoreboards filter and every page title pick it up
without anyone hand-editing four rows per season.

    fallout76_seasons.tsv   SCORE_S27  27  You're S.P.E.C.I.A.L.
    guide_index.tsv  before "Season 27"              / "Season 27 - Scoreboard"
                     after  "Season 27: You're S.P.E.C.I.A.L." / "... - Scoreboard"

Rows touched: df-sc-scoreboards-season-{N} (the season hub) and every
df-scoreboard-season-{N}-* page under it. Columns touched: subCategory and title
only. menuTitle, slug, url, blurb and everything else are left as they are.

ONLY a bare placeholder is filled in - a title that is still just "Season N" or
"Legacy Season N". A season that already carries a name is never rewritten,
because several of those were worded by hand ("Season 24: Cryptids From Beyond
the Cosmos" is not the SeasonName in fallout76_seasons.tsv) and a build must not
undo that. A season with no name yet stays "Season N" until one is added.

The file is rewritten line by line: only the changed cells move, every other
byte (other sessions' uncommitted rows included) is kept exactly.

Called by build_season_rewards.py and build_pts_season_scoreboard.py, so adding
the name to fallout76_seasons.tsv and running either build is all it takes.

USAGE:  python src/season_guide_titles.py            # apply
        python src/season_guide_titles.py --dry-run  # report only
"""

import argparse
import csv
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GUIDE_INDEX = REPO_ROOT / "tsv" / "guide_index.tsv"
SEASONS_TSV = REPO_ROOT / "tsv" / "fallout76_seasons.tsv"
TAG = "[season_guide_titles]"

HUB_RE = re.compile(r"^df-sc-scoreboards-season-(\d+)$")
PAGE_RE = re.compile(r"^df-scoreboard-season-(\d+)-")
BARE_RE = re.compile(r"^(Legacy Season|Season) (\d+)$")


def season_names(path=SEASONS_TSV):
    out = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            try:
                n = int((row.get("SeasonNumber") or "").strip())
            except ValueError:
                continue
            name = (row.get("SeasonName") or "").strip()
            if name:
                out[n] = name
    return out


def sync(dry_run=False, guide_index=GUIDE_INDEX, seasons_tsv=SEASONS_TSV):
    """Fill bare season titles. Returns a list of (row id, old title, new title)."""
    names = season_names(seasons_tsv)
    raw = guide_index.read_bytes().decode("utf-8")
    lines = raw.split("\n")
    header = lines[0].rstrip("\r").split("\t")
    try:
        i_id, i_sub, i_title, i_menu = (header.index(c) for c in
                                        ("id", "subCategory", "title", "menuTitle"))
    except ValueError:
        print(TAG + " [WARN] guide_index.tsv header not recognised - nothing done.")
        return []

    changes = []
    for li in range(1, len(lines)):
        line = lines[li]
        cr = line.endswith("\r")
        cells = (line[:-1] if cr else line).split("\t")
        if len(cells) <= max(i_id, i_sub, i_title, i_menu):
            continue
        rid = cells[i_id]
        m = HUB_RE.match(rid) or PAGE_RE.match(rid)
        if not m:
            continue
        n = int(m.group(1))
        name = names.get(n)
        bare = BARE_RE.match(cells[i_sub].strip())
        if not name or not bare or int(bare.group(2)) != n:
            continue          # no name yet, or already named (hand-worded) - leave it
        full = bare.group(1) + " " + str(n) + ": " + name
        is_hub = bool(HUB_RE.match(rid))
        new_title = full if is_hub else full + " - " + cells[i_menu].strip()
        changes.append((rid, cells[i_title], new_title))
        cells[i_sub] = full
        cells[i_title] = new_title
        lines[li] = "\t".join(cells) + ("\r" if cr else "")

    if changes and not dry_run:
        guide_index.write_bytes("\n".join(lines).encode("utf-8"))
    for rid, old, new in changes:
        print(TAG + (" would rename " if dry_run else " renamed ") + rid + ": "
              + repr(old) + " -> " + repr(new))
    if not changes:
        print(TAG + " all named seasons already titled - nothing to do.")
    return changes


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dry-run", action="store_true")
    sync(dry_run=ap.parse_args().dry_run)
