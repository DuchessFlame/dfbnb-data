#!/usr/bin/env python3
r"""
build_scoreboard_art_json.py — /df/plan-checklists/scoreboard-art/

WHY ITS OWN DATASET
-------------------
This is the one page in the category with NOTHING behind it in plan_master —
zero rows, so it rendered an empty header card. That is not a bug in the
routing: scoreboard art is not learned from a plan at all. It is handed out as
a season reward entitlement, so there is no BOOK record for plan_master to
carry and no amount of re-classifying would have found one. The art has to come
from the season data instead.

ONE ROOT PER SEASON, RERUNS INSIDE IT
-------------------------------------
Duchess's call, verbatim:

    "season scoreboard art plans but do the roots per season because they are
     releasing some of the old seasons with new art so sub expand them so it
     catches under one season expand"

So: a root expand per season, the season's original art loose inside it, and
any art a LATER RERUN of that season added in a sub-expand beneath it. Season 4
was rerun in Sep 2026 and gained a piece of art it did not ship with; without
the sub-expand that either hides the new piece or splits Season 4 into two root
expands, and both are worse than saying plainly which run a piece came from.

`addedInRerun` in season_rewards.tsv is the field that records this, and it is
already curated, so the page reads it rather than guessing from dates.

WHAT COUNTS AS ART
------------------
The reward's `storefrontEntitlement`, which is the record the scoreboard hands
over. CAMP wall decor, posters, paintings, murals and banners are art you hang
up. Deliberately NOT included:

  * Photomode frames and poses. They carry art-ish words and are the obvious
    near-miss, but a photo frame is a camera overlay, not something you build —
    they have a `photomode` folder of their own for when that page exists.
  * Statues, plushies and floor decor. They are CAMP items and are on the CAMP
    plan checklist, where a player looking for furniture will go.

A reward whose entitlement is missing or zzz-prefixed is dev leftover and is
skipped with a count printed, never silently.

    python3 src/build_scoreboard_art_json.py
    python3 src/build_scoreboard_art_json.py --pts     -> dist/pts/
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

SCHEMA = 1
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

PTS = "--pts" in sys.argv
TSV_DIR = os.path.join(REPO, "tsv", "pts") if PTS else os.path.join(REPO, "tsv")
DIST_DIR = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
OUT = os.path.join(DIST_DIR, "scoreboard_art.json")

# season_rewards.tsv is a CURATED live file. The PTS tsv root does not carry it,
# so fall back to the live one rather than shipping an empty PTS page — the same
# fallback plan_images.py makes for the same file and the same reason.
SEASONS_TSV = os.path.join(TSV_DIR, "season_rewards.tsv")
if not os.path.exists(SEASONS_TSV):
    SEASONS_TSV = os.path.join(REPO, "tsv", "season_rewards.tsv")

sys.path.insert(0, HERE)
try:
    import asset_paths
except ImportError:                                  # pragma: no cover
    asset_paths = None

# The entitlement words that mean "art you hang on a wall".
ART = re.compile(r"(walldeco(?:r)?|_poster|painting|mural|_art$|art_|tapestry|banner)", re.I)
# The near-misses, named out so the rule is a decision rather than an accident.
NOT_ART = re.compile(r"photomode|playericon|playertitle|camptitle", re.I)
DEV = re.compile(r"^(zzz|CUT_|DEL_|DEBUG)", re.I)

IMG_BASE = "/wp-content/uploads/guide-images/plan-checklist/scoreboard-art/"


def route(url):
    """Season art through the site's own routing rule; anything else as-is.

    NEVER use the TSV value raw: it holds the flat authoring form
    (/season_images/score_s3_*.webp) while the file is served from
    /season_images/season-3/score_s3_*.avif. asset_paths.asset_url is the one
    routing rule the scoreboard pages use, so a row here lands on exactly the
    URL the scoreboard row does.
    """
    if not url or asset_paths is None:
        return url
    try:
        return asset_paths.asset_url(url)
    except Exception:                                # noqa: BLE001 - never fatal
        return url


def read_rows():
    with open(SEASONS_TSV, encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def is_art(row):
    ent = (row.get("storefrontEntitlement") or "").strip()
    if not ent or DEV.search(ent) or NOT_ART.search(ent):
        return False
    return bool(ART.search(ent))


def make_row(r):
    season = (r.get("seasonNumber") or "").strip()
    rank = (r.get("rank") or "").strip()
    cost = (r.get("cost") or "").strip()
    name = (r.get("name") or "").strip()

    where = f"Season {season} Scoreboard"
    line = f"Claimed from the {where}" + (f", rank {rank}." if rank else ".")
    if cost and cost not in ("0", ""):
        line += f" Costs {cost} to claim."
    if (r.get("addedInRerun") or "").strip():
        line += (f" Added when Season {season} was rerun in "
                 f"{r['addedInRerun'].strip()} — it was not on the original board.")

    img = route((r.get("imageUrl") or "").strip())
    return {
        "kind": "plan", "brand": "df", "type": "scoreboard-art",
        "id": f"SCOREART_{(r.get('id') or name).strip()}",
        "name": name,
        "display_name": name,
        "has_image_box": True,
        "image_dir": "scoreboard-art",
        "obtain": "Not learned from a plan — it unlocks on your account when you "
                  "claim the scoreboard rank.",
        "category_label": "Scoreboard Art",
        "obtain_routes": [],
        "obtain_unlocks": [line],
        "obtain_ledger": [{"label": "Scoreboard", "unlocks": [0], "drop": "N/A"}],
        "plan_item": None,
        "cobj": None,
        "cnam": None,
        "tradeable": False,
        "stops_dropping": None,
        "effects": None,
        "cut": False,
        "cut_reason": None,
        "images": [img] if img else [],
        "image_source": "season" if img else "",
        "description": (r.get("description") or "").strip(),
        "season": int(season) if season.isdigit() else 0,
        "rank": int(rank) if rank.isdigit() else 0,
        "added_in_rerun": (r.get("addedInRerun") or "").strip(),
        "entitlement": (r.get("storefrontEntitlement") or "").strip(),
    }


def build():
    rows = read_rows()
    art = [make_row(r) for r in rows if is_art(r)]
    skipped_dev = sum(1 for r in rows
                      if DEV.search(r.get("storefrontEntitlement") or "")
                      and ART.search(r.get("storefrontEntitlement") or ""))

    by_season = {}
    for it in art:
        by_season.setdefault(it["season"], []).append(it)

    groups = []
    for season in sorted(by_season):
        mine = by_season[season]
        original = [i for i in mine if not i["added_in_rerun"]]
        reruns = {}
        for i in mine:
            if i["added_in_rerun"]:
                reruns.setdefault(i["added_in_rerun"], []).append(i)
        for lst in [original] + list(reruns.values()):
            lst.sort(key=lambda x: (x["rank"], x["name"]))
        groups.append({
            "key": f"season-{season}",
            "label": f"Season {season}",
            "blurb": "",
            "count": len(mine),
            "items": original,
            # A rerun of a season is still that season, so its new art sits
            # INSIDE the season expand rather than making a second one.
            "groups": [
                {"key": f"season-{season}-rerun-{re.sub(r'[^a-z0-9]+', '-', label.lower()).strip('-')}",
                 "label": f"Added in the {label} rerun",
                 "blurb": "Art this season did not ship with. It was added when the "
                          "season was rerun, so a player who finished the original "
                          "board will not have it.",
                 "items": items}
                for label, items in sorted(reruns.items())
            ],
        })

    doc = {
        "schemaVersion": SCHEMA,
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": PTS,
        "title": "Scoreboard Art",
        "note": ["Not plans.",
                 "Scoreboard art is claimed from a season's board rather than learned "
                 "from a plan you find, so none of it appears in the plan data. This "
                 "page is built from the season reward list instead."],
        "source_files": [os.path.basename(SEASONS_TSV)],
        "groups": groups,
    }
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

    total = sum(g["count"] for g in groups)
    rerun = sum(len(sg["items"]) for g in groups for sg in g["groups"])
    print(f"[scoreboard-art] wrote {OUT} — {total} pieces across {len(groups)} seasons "
          f"({rerun} added by a rerun)")
    print(f"  source: {os.path.basename(SEASONS_TSV)}")
    if skipped_dev:
        print(f"  skipped {skipped_dev} dev leftovers (zzz/CUT prefixed entitlements)")
    with_art = sum(1 for g in groups for i in g["items"] if i["images"])
    print(f"  with a picture: {with_art} of {total}")


if __name__ == "__main__":
    build()
