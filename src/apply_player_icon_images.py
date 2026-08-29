#!/usr/bin/env python3
r"""
apply_player_icon_images.py
---------------------------
Points every `kind=playerIcon` row in tsv/season_rewards.tsv at the canonical
player-icon image, and back-fills a missing storefrontEntitlement while it is
there.

WHY THIS EXISTS
===============
Scoreboard player-icon rewards had drifted across two image folders:

  163 rows  /wp-content/uploads/storefront/player-icons/*.avif
   48 rows  /wp-content/uploads/season_images/*.webp     <- season art, not the icon

The second group was a fallback: when nobody had extracted the icon texture,
the row borrowed whatever season image existed. Player icons now live in ONE
folder addressed by the icon's own DDS filename — the same rule the Player
Icons page uses — so the fallback is no longer needed and the two folders can
collapse into one.

Run this AFTER build_player_icons_json.py. It reads dist/player_icons.json,
which already carries the resolved image URL per entitlement.

MATCHING
========
1. storefrontEntitlement -> ENTM EDID. The reliable path; covers ~199 rows.
2. Reward name -> icon name, for rows whose entitlement column is blank
   (mostly Season 25, curated before the ENTM export landed). The
   entitlement is written back so the row matches by ID next time.
3. A curated row whose entitlement points at a cut (zzz*) ENTM record still
   names a real reward on a real board, so it is resolved straight from the
   ENTM export rather than being skipped.

Anything still unmatched is reported and LEFT ALONE — this script never
invents an image.

USAGE
=====
    python src/apply_player_icon_images.py            # rewrite in place
    python src/apply_player_icon_images.py --dry-run  # report only
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import sys
from datetime import datetime

import tsv_source

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.join(SCRIPT_DIR, "..")
REWARDS_TSV = os.path.join(REPO, "tsv", "season_rewards.tsv")
ICONS_JSON = os.path.join(REPO, "dist", "player_icons.json")

SITE_ROOT = "https://www.buffsnbrew.com"
TAG = "[player-icon-images]"
DRY = "--dry-run" in sys.argv


def site_relative(url: str) -> str:
    """season_rewards.tsv stores site-relative URLs; player_icons.json absolute."""
    return url[len(SITE_ROOT):] if url.startswith(SITE_ROOT) else url


def load_cut_icon_images() -> dict:
    """EDID -> image filename for the zzz/dev ENTM records the builder drops.

    The Player Icons page has no business listing these, but a curated
    scoreboard row may legitimately point at one, so resolve them here.
    """
    import build_player_icons_json as B

    out = {}
    for r in B.read_tsv(tsv_source.newest("ENTM_Export_*.tsv"), repair=True):
        edid = (r.get("EDID") or "").strip()
        if not edid or "playericon" not in edid.lower():
            continue
        fn = B.image_filename(r.get("ETDI") or "")
        if fn:
            out[edid] = fn
    return out


def main() -> None:
    if not os.path.exists(ICONS_JSON):
        raise SystemExit(f"{TAG} [ERROR] missing {ICONS_JSON} — run "
                         "build_player_icons_json.py first")

    data = json.load(open(ICONS_JSON, encoding="utf-8"))
    image_base = site_relative(data["imageBase"])
    by_edid = {i["edid"]: i for i in data["icons"]}
    by_name = {}
    for i in data["icons"]:
        by_name.setdefault(i["name"].strip().lower(), i)

    all_entm = load_cut_icon_images()

    with open(REWARDS_TSV, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    header = rows[0]
    KIND = header.index("kind")
    ENT = header.index("storefrontEntitlement")
    IMG = header.index("imageUrl")
    NAME = header.index("name")
    SEASON = header.index("seasonNumber")

    changed = 0
    filled_ent = 0
    unmatched = []

    for r in rows[1:]:
        if len(r) <= max(KIND, ENT, IMG, NAME) or r[KIND] != "playerIcon":
            continue

        ent = r[ENT].strip()
        icon = by_edid.get(ent)
        filename = icon["imageFilename"] if icon else None

        # Curated row pointing at a cut ENTM record — still a real reward.
        if filename is None and ent and ent in all_entm:
            filename = all_entm[ent]

        # No entitlement recorded: match on the reward name. "Player Icon: X"
        # is the curated naming convention, so strip the prefix first.
        if filename is None and not ent:
            key = r[NAME].strip()
            if key.lower().startswith("player icon:"):
                key = key.split(":", 1)[1].strip()
            match = by_name.get(key.lower())
            if match:
                filename = match["imageFilename"]
                r[ENT] = match["edid"]
                filled_ent += 1

        if filename is None:
            unmatched.append((r[SEASON], r[NAME], ent))
            continue

        new_url = image_base + filename
        if r[IMG] != new_url:
            r[IMG] = new_url
            changed += 1

    print(f"{TAG} playerIcon rows repointed: {changed}")
    print(f"{TAG} blank storefrontEntitlement back-filled: {filled_ent}")
    print(f"{TAG} unmatched (left untouched): {len(unmatched)}")
    for s, n, e in unmatched:
        print(f"{TAG}   S{s:<3} {n:<44} ent={e or '(blank)'}")

    if DRY:
        print(f"{TAG} --dry-run: no file written")
        return
    if not changed and not filled_ent:
        print(f"{TAG} nothing to write")
        return

    # Local runs get a .bak next to the file; CI does not — git already has
    # the previous version, and a stray .bak in the workspace is just noise.
    note = ""
    if not os.environ.get("GITHUB_ACTIONS"):
        backup = REWARDS_TSV + ".bak-" + datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copyfile(REWARDS_TSV, backup)
        note = f" (backup: {os.path.basename(backup)})"
    with open(REWARDS_TSV, "w", encoding="utf-8", newline="") as f:
        csv.writer(f, delimiter="\t", lineterminator="\n").writerows(rows)
    print(f"{TAG} wrote {os.path.relpath(REWARDS_TSV, REPO)}{note}")


if __name__ == "__main__":
    main()
