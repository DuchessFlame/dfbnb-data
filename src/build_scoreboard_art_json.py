#!/usr/bin/env python3
r"""
build_scoreboard_art_json.py — /df/plan-checklists/scoreboard-art/

WHAT "SCOREBOARD ART" IS
------------------------
Every season's Scoreboard ends with one framed piece of the board's own
artwork: the "Framed … Gameboard" (Seasons 1-15) and the "Framed … Wall Art"
(Season 16 on, EDID ``…_EndOfSeasonArt``). This page is a checklist of those —
one root expand per piece, the usual Item Image / How to Obtain / Technical.

It is NOT every wall decoration a season gave out (Hanging Raider Cage, the
M.I.N.D. posters …). The first version of this page matched any wall decor and
read as a random pile; the board art is the collectable set players track.

WHERE THE DATA COMES FROM (generative)
--------------------------------------
* ENTM export — membership. Any entitlement whose EDID is a season board-art
  record is on the page, so a new season's art appears on the next TSV drop
  with no edit here (Season 25 was missing from season_rewards.tsv and only
  showed up once this read ENTM).
* tsv/season_rewards.tsv — rank, cost and ``addedInRerun`` where curated.

LEGACY vs RE-RUN
----------------
Bethesda re-runs old seasons (Legacy scoreboards) and a re-run can ship NEW
board art — Season 4 got "Framed Cold Steel Wall Art" in Sep 2026 on top of the
original "Framed Cold Steel Gameboard". Duchess's call: keep both in season
order and tag them so it is obvious why a season appears twice:

    Framed Cold Steel Gameboard (Legacy)
    Framed Cold Steel Wall Art (Re-Run)

The re-run piece is the one season_rewards marks ``addedInRerun``; failing that,
the later FormID (a re-run record is always added after the original).

IMAGES — season-first
---------------------
The tile is always the one the site already serves under
/season_images/season-N/ (routed through asset_paths, the scoreboard pages' own
rule), never a second copy in guide-images.

    python3 src/build_scoreboard_art_json.py          -> dist/scoreboard_art.json
    python3 src/build_scoreboard_art_json.py --pts    -> dist/pts/scoreboard_art.json
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

SCHEMA = 2
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import tsv_source                                   # one export resolver
try:
    import asset_paths
except ImportError:                                  # pragma: no cover
    asset_paths = None

PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
DIST_DIR = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
OUT = os.path.join(DIST_DIR, "scoreboard_art.json")

# season_rewards.tsv is curated on the live side only; PTS reads the live copy.
SEASONS_TSV = os.path.join(REPO, "tsv", "pts", "season_rewards.tsv") if PTS else ""
if not SEASONS_TSV or not os.path.exists(SEASONS_TSV):
    SEASONS_TSV = os.path.join(REPO, "tsv", "season_rewards.tsv")

csv.field_size_limit(10 ** 9)

# SCORE_S4_ENTM_CAMP_WallDeco_ColdSteel_Gameboard
# SCORE_S11_ENTM_CAMP_WallDecor_S11Board_NukaWorld / S13BoardHollywood
# SCORE_S16_ENTM_CAMP_WallDecor_EndofSeasonArt / S25 …_UnderSiege_EndOfSeasonArt
BOARD_ART = re.compile(
    r"^SCORE_S(\d+)_ENTM_CAMP_WallDeco(?:r)?_(?:.*_Gameboard|S\d+Board.*|(?:.*_)?EndOfSeasonArt)$",
    re.I)
DEV = re.compile(r"^(zzz|CUT_|DEL_|DEBUG)", re.I)


def route(url):
    if not url or asset_paths is None:
        return url
    try:
        return asset_paths.asset_url(url)
    except Exception:                                # noqa: BLE001
        return url


def season_image(season, etdi):
    """/season_images/season-N/<etdi stem>.avif — the scoreboard pages' name."""
    stem = os.path.splitext(os.path.basename((etdi or "").replace("\\", "/")))[0].lower()
    if not stem:
        return ""
    stem = re.sub(r"_l$", "", stem)
    return route(f"/wp-content/uploads/season_images/season-{season}/{stem}.avif")


def load_entm():
    path = tsv_source.newest("ENTM_Export_*.tsv", channel=CHANNEL)
    out = []
    with open(path, encoding="latin1", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            if DEV.search(edid):
                continue
            m = BOARD_ART.match(edid)
            if m:
                out.append((int(m.group(1)), r))
    return path, out


def load_rewards():
    by_ent = {}
    with open(SEASONS_TSV, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            ent = (r.get("storefrontEntitlement") or "").strip().lower()
            if ent and ent not in by_ent:
                by_ent[ent] = r
    return by_ent


def load_season_names():
    names = {}
    p = os.path.join(REPO, "tsv", "fallout76_seasons.tsv")
    if not os.path.exists(p):
        return names
    with open(p, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            keys = {k.lower(): v for k, v in r.items() if k}
            num = next((keys[k] for k in ("season", "seasonnumber", "number", "season_number")
                        if keys.get(k)), "")
            name = next((keys[k] for k in ("name", "theme", "seasonname", "title")
                         if keys.get(k)), "")
            if str(num).strip().isdigit() and name:
                names[int(num)] = name.strip()
    return names


def clean_desc(s):
    s = re.sub(r"\s*-\s*C\.A\.M\.P\. ITEMS APPEAR.*$", "", s or "", flags=re.I)
    return s.strip()


def build():
    entm_path, entm = load_entm()
    rewards = load_rewards()
    names = load_season_names()

    by_season = {}
    for season, r in entm:
        by_season.setdefault(season, []).append(r)

    items = []
    for season in sorted(by_season):
        rows = by_season[season]
        # Which piece is the re-run art: curated flag first, else newest FormID.
        rr = {id(r): (rewards.get(r["EDID"].strip().lower()) or {}) for r in rows}
        flagged = [r for r in rows if (rr[id(r)].get("addedInRerun") or "").strip()]
        if len(rows) > 1 and not flagged:
            flagged = [max(rows, key=lambda r: int(r.get("FormID") or "0", 16))]
        rows.sort(key=lambda r: (r in flagged, int(r.get("FormID") or "0", 16)))

        theme = names.get(season, "")
        for r in rows:
            rw = rr[id(r)]
            is_rerun = r in flagged
            tag = ""
            if len(rows) > 1:
                tag = " (Re-Run)" if is_rerun else " (Legacy)"
            name = (r.get("FULL") or rw.get("name") or r["EDID"]).strip()
            rank = (rw.get("rank") or "").strip()
            rerun_when = (rw.get("addedInRerun") or "").strip()

            board = f"Season {season}" + (f" — {theme}" if theme else "")
            # S16+ and any re-run board (re-runs use the ticket system) are
            # bought with tickets; the original S1-S15 boards were claimed.
            if season <= 15 and not is_rerun:
                line = f"Claim from the Season {season} Scoreboard"
            else:
                line = f"Purchase with tickets from the Season {season} Scoreboard"
            if theme:
                line = line.replace(f"Season {season} Scoreboard",
                                    f"{re.sub(r'^the ', '', theme, flags=re.I)} Scoreboard (Season {season}"
                                    + (" re-run)" if is_rerun else ")"))
            line += (f", rank {rank}." if rank else ".")
            if is_rerun:
                line += (" Added when the season was re-run"
                         + (f" in {rerun_when}" if rerun_when else "")
                         + " — it was not on the original board.")
            elif tag:
                line += " The original board's art — the re-run added a second piece."

            img = route((rw.get("imageUrl") or "").strip()) or season_image(season, r.get("ETDI"))
            items.append({
                "kind": "plan", "brand": "df", "type": "scoreboard-art",
                "id": f"SCOREART_{r['EDID'].strip()}",
                "name": name + tag,
                "display_name": f"Season {season}: {name}{tag}",
                "has_image_box": True,
                "image_dir": "scoreboard-art",
                "obtain": line,
                "category_label": "Scoreboard Art",
                "obtain_routes": [],
                "obtain_unlocks": [line],
                "obtain_ledger": [{"label": "Scoreboard", "unlocks": [0], "drop": "N/A"}],
                "plan_item": None, "cobj": None, "cnam": None,
                "tradeable": False,
                "stops_dropping": None, "effects": None,
                "cut": False, "cut_reason": None,
                "images": [img] if img else [],
                "image_source": "season" if img else "",
                "description": clean_desc(r.get("DESC") or rw.get("description") or ""),
                "season": season,
                "season_name": theme,
                "board": board,
                "rank": int(rank) if rank.isdigit() else 0,
                "run": "rerun" if is_rerun else ("legacy" if tag else ""),
                "added_in_rerun": rerun_when,
                "entitlement": r["EDID"].strip(),
                "formid": (r.get("FormID") or "").strip(),
                "texture": (r.get("ETDI") or "").strip(),
            })

    doc = {
        "schemaVersion": SCHEMA,
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": PTS,
        "title": "Scoreboard Art",
        "sub": "Track which season Scoreboard art you have claimed.",
        "noun": {"one": "piece of scoreboard art", "many": "pieces of scoreboard art"},
        "keep_order": True,   # season order, not A-Z — see df-bnb-plan-checklists.js
        "note": ["Not plans.",
                 "Each season's board art is claimed from that season's Scoreboard. "
                 "Where a Legacy season was re-run with new art, both pieces are listed "
                 "and tagged (Legacy) and (Re-Run)."],
        "source_files": [os.path.basename(entm_path), os.path.basename(SEASONS_TSV)],
        "groups": [{"key": "all", "label": "", "items": items}],
    }
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

    reruns = sum(1 for i in items if i["run"] == "rerun")
    print(f"[scoreboard-art] wrote {OUT} — {len(items)} pieces across "
          f"{len(by_season)} seasons ({reruns} re-run piece(s))")
    print(f"  sources: {os.path.basename(entm_path)}, {os.path.basename(SEASONS_TSV)}")
    missing = [i["name"] for i in items if not i["images"]]
    if missing:
        print(f"  no image: {', '.join(missing)}")


if __name__ == "__main__":
    build()
