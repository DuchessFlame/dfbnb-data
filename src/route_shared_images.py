#!/usr/bin/env python3
"""
route_shared_images.py
----------------------
Points season rewards whose art is SHARED across seasons at the shared upload
folders, instead of the per-season /season_images/season-{N}/ path.

Replaces route_title_images.py, which handled titles only.

WHY: some reward art is not per-season at all.

  Titles   Camp/player titles have always been served from
           /wp-content/uploads/storefront/titles-player/ and titles-camp/ by the
           /df/titles/ pages, and those folders already hold the season titles.

  Icons    Player icons repeat across seasons (and across bundles, the Atom Shop and
           request items), so per-season copies would duplicate the same file. They
           get one shared folder, /wp-content/uploads/storefront/player-icons/, which
           the planned player-icons page can also read.

The back-fill derived every imageUrl from storefrontEntitlement, which sent all of
these to /season_images/season-{N}/ - a path their textures never live under.
df-bnb-seasons.js resolveImageUrl() leaves non-/season_images/ URLs untouched, so a
storefront URL passes straight through to the page.

FILE NAMING is always the SOURCE TEXTURE stem with any _l variant suffix stripped,
lowercased. That is what the titles folders already use
(score_s19_playertitles_suffix_delver.avif) and what the handful of existing icons use
(ATX_PlayerIcon_SCORE_33.avif -> atx_playericon_score_33). Icon textures are opaquely
named (atx_playericon_score_01) and cannot be derived from the entitlement, so the
texture path is read from dist/season_images/season_{N}_images.json.

PRESENCE CHECKING differs per category:
  - Titles are already uploaded, so a row is only rewritten when the file is really
    there; a miss means the name is wrong and is reported rather than guessed.
  - The player-icons folder does not exist yet, so icons are routed regardless. That
    is safe: imgSlot() in df-bnb-seasons.js swaps a failed image for a "No image"
    placeholder, and these rewards already point at an equally absent season_images
    path today. Routing now means the art works the moment it is uploaded.

STATUS: active
INPUT:  tsv/season_rewards.tsv, dist/season_images/*.json, the storefront upload mirror
OUTPUT: tsv/season_rewards.tsv (rewritten in place, .bak kept)
USAGE:  python src/route_shared_images.py --storefront "C:\\...\\uploads\\fo76\\storefront" --dry-run
        python src/route_shared_images.py --storefront "C:\\...\\uploads\\fo76\\storefront"
"""

import argparse
import csv
import glob
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REWARDS_TSV = REPO_ROOT / "tsv" / "season_rewards.tsv"
MANIFEST_DIR = REPO_ROOT / "dist" / "season_images"

TAG = "[route_shared_images]"

REMOTE_ROOT = "/wp-content/uploads/storefront/"

# Remote subfolder per category. Change ICON_DIR here if the folder should be called
# something else on the server - it is referenced nowhere else.
TITLES_PLAYER_DIR = "titles-player"
TITLES_CAMP_DIR = "titles-camp"
ICON_DIR = "player-icons"

CAMP_KINDS = {"campTitlePrefix", "campTitleSuffix"}
PLAYER_KINDS = {"playerTitlePrefix", "playerTitleSuffix", "playerTitlePrefixSuffix"}

# Rows predating the kind column carry no `kind`, so fall back to the entitlement.
CAMP_ENT_RE = re.compile(r"camptitles_", re.IGNORECASE)
PLAYER_ENT_RE = re.compile(r"playertitles_", re.IGNORECASE)
ICON_ENT_RE = re.compile(r"playericon", re.IGNORECASE)

VARIANT_RE = re.compile(r"_(l|c\d+|d|n|r)$", re.IGNORECASE)


def classify(row: dict) -> str | None:
    """Return 'camp', 'player', 'icon' or None."""
    kind = (row.get("kind") or "").strip()
    ent = row.get("storefrontEntitlement") or ""
    name = row.get("name") or ""
    if kind == "playerIcon" or ICON_ENT_RE.search(ent) or name.startswith("Player Icon"):
        return "icon"
    if kind in CAMP_KINDS:
        return "camp"
    if kind in PLAYER_KINDS:
        return "player"
    if CAMP_ENT_RE.search(ent):
        return "camp"
    if PLAYER_ENT_RE.search(ent):
        return "player"
    return None


def texture_stems() -> dict[str, str]:
    """entitlement (lower) -> source texture stem, from the season image manifests."""
    out: dict[str, str] = {}
    for p in glob.glob(str(MANIFEST_DIR / "season_*_images.json")):
        for i in json.loads(Path(p).read_text(encoding="utf-8"))["images"]:
            stem = os.path.basename(i["ddsPath"])
            stem = os.path.splitext(stem)[0]
            out[i["entitlement"].lower()] = VARIANT_RE.sub("", stem).lower()
    return out


def stem_of(url: str) -> str:
    m = re.search(r"/([^/]+)\.\w+$", url or "")
    return m.group(1).lower() if m else ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--storefront", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    sf = Path(args.storefront)
    pools: dict[str, dict] = {}
    for key, sub in (("player", TITLES_PLAYER_DIR), ("camp", TITLES_CAMP_DIR)):
        d = sf / sub
        if not d.is_dir():
            sys.exit(f"{TAG} [ERROR] missing {d}")
        pools[key] = {p.stem.lower(): (sub, p.name) for p in d.glob("*.avif")}
        print(f"{TAG} {sub}: {len(pools[key])} files")

    icon_dir = sf / ICON_DIR
    icon_pool = ({p.stem.lower(): p.name for p in icon_dir.glob("*.avif")}
                 if icon_dir.is_dir() else {})
    print(f"{TAG} {ICON_DIR}: {len(icon_pool)} files"
          + ("" if icon_dir.is_dir() else "  (folder does not exist yet)"))

    tex = texture_stems()
    print(f"{TAG} texture stems known for {len(tex)} entitlements")

    with REWARDS_TSV.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        cols = reader.fieldnames
        rows = list(reader)

    routed = {"player": 0, "camp": 0, "icon": 0}
    icon_present = 0
    misses: list[tuple] = []

    for r in rows:
        which = classify(r)
        if which is None:
            continue
        url = r.get("imageUrl") or ""
        if "/season_images/" not in url:
            continue                       # already routed or hand-set - leave alone

        if which == "icon":
            # Icon textures are opaquely named, so the entitlement cannot produce the
            # filename - it has to come from the manifest's recorded texture path.
            stem = tex.get((r.get("storefrontEntitlement") or "").lower())
            if not stem:
                misses.append((r["seasonNumber"], r["name"], "no texture path in ENTM"))
                continue
            r["imageUrl"] = f"{REMOTE_ROOT}{ICON_DIR}/{stem}.avif"
            routed["icon"] += 1
            if stem in icon_pool:
                icon_present += 1
            continue

        # Titles: trust the art filename over the reward's `kind` about which folder
        # holds it. S24's "Player Title Prefix/Suffix: Alien" is a player title whose
        # artwork is CAMPTitles_Prefix_Alien.
        stem = stem_of(url)
        candidates = [stem, re.sub(r"^score_s\d+_", "atx_", stem)]
        if CAMP_ENT_RE.search(stem):
            order = ["camp", "player"]
        elif PLAYER_ENT_RE.search(stem):
            order = ["player", "camp"]
        else:
            order = [which]

        found = next((pools[p][c] for p in order for c in candidates if c in pools[p]),
                     None)
        if not found:
            misses.append((r["seasonNumber"], r["name"], f"no art in titles-* ({stem})"))
            continue
        sub, fname = found
        r["imageUrl"] = f"{REMOTE_ROOT}{sub}/{fname}"
        routed[which] += 1

    total = sum(routed.values())
    print(f"{TAG} routed {total}: "
          f"{routed['player']} player titles, {routed['camp']} camp titles, "
          f"{routed['icon']} player icons")
    print(f"{TAG}   of those icons, {icon_present} already have art uploaded, "
          f"{routed['icon'] - icon_present} await upload to {ICON_DIR}/")
    print(f"{TAG} left on season_images: {len(misses)}")
    for s, n, why in misses:
        print(f"{TAG}    S{s} {n}  ({why})")

    if args.dry_run:
        print(f"{TAG} dry run - nothing written.")
        return
    if not total:
        print(f"{TAG} nothing to write.")
        return

    bak = REWARDS_TSV.with_suffix(
        REWARDS_TSV.suffix + ".bak-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
    shutil.copy2(REWARDS_TSV, bak)
    print(f"{TAG} backup written: {bak.name}")

    with REWARDS_TSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"{TAG} written: {REWARDS_TSV.name}")
    print(f"{TAG} Done. Now run: python src/build_season_rewards.py")


if __name__ == "__main__":
    main()
