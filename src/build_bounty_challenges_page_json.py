#!/usr/bin/env python3
"""
build_bounty_challenges_page_json.py - Grunt Hunt and Head Hunt Challenges page

Page:   /df/bounty-hunting/grunt-and-head-hunt-challenges/
Reads:  dist/challenges/challenges.json   (built by build_challenges_json_v3.py,
        so every row already carries leaf-expanded conditions_display, rewards,
        META -> SUB children, is_new and is_cut)
Writes: dist/challenges/bounty_challenges_page.json

Nothing is hand-listed. Every live (non-cut) challenge whose name or EDID is
about Grunt Hunts, Head Hunts or bounties is picked up and sorted into:

  root  (one per hunt)            panels (fixed order inside each root)
  ----------------------------    ---------------------------------------
  Grunt Hunts                     Daily -> Weekly -> Event -> Lifetime -> World Pets
  Burning Springs Head Hunts      (empty panels are left out)
  Skyline Valley Head Hunts
  Head Hunts (all regions)        <- only if a Head Hunt challenge names no region

How a challenge is placed:
  hunt   - "Grunt Hunt" / "Head Hunt" in the in-game name first, then the
           condition lists it counts ("Completed Grunt Hunts" ...), then EDID.
           Region-wide challenges with no hunt word (the location METAs) go in
           their region's Head Hunt root.
  region - EDID token (Burn / RTSV / Skyline), else the FLST names in its own
           or its sub-challenges' conditions.
  panel  - CHAL scope Daily / Weekly / Event; WorldPets_ EDIDs -> World Pets;
           everything else Lifetime.

Daily and Weekly challenges carry "trackable": false - they reset, so the page
shows them greyed out and leaves them out of the progress bar and Tick All.

Usage:
  python src/build_bounty_challenges_page_json.py          # live: dist/challenges/
  python src/build_bounty_challenges_page_json.py --pts    # local PTS run: dist/pts/challenges/
In the PTS workflow run the plain form: tsv/ and dist/ are already the PTS
channel there and the relocate step moves dist/challenges -> dist/pts/.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PTS = "--pts" in sys.argv[1:]
DIST = ROOT / "dist" / ("pts" if PTS else "") / "challenges"
SRC = DIST / "challenges.json"
OUT = DIST / "bounty_challenges_page.json"

HUNTS = {
    "grunt": {"label": "Grunt Hunts",
              "guides": [{"title": "Burning Springs Grunt Hunt Guide",
                          "url": "/df/bounty-hunting/grunt-hunts/grunt-hunts-guide/"}]},
    "bs":    {"label": "Burning Springs Head Hunts",
              "guides": [{"title": "Burning Springs Head Hunt Guide",
                          "url": "/df/bounty-hunting/head-hunts/head-hunts-guide/"}]},
    "sv":    {"label": "Skyline Valley Head Hunts",
              "guides": [{"title": "Skyline Valley Head Hunt Guide",
                          "url": "/df/bounty-hunting/head-hunts/skyline-valley-head-hunts-guide/"}]},
    "head":  {"label": "Head Hunts (all regions)", "guides": []},
}
HUNT_ORDER = ["grunt", "bs", "sv", "head"]
PANEL_ORDER = ["Daily", "Weekly", "Event", "Lifetime", "World Pets"]
UNTRACKED = {"Daily", "Weekly"}

TOPIC_RX = re.compile(r"grunt hunt|head hunt|bounty", re.I)
EDID_RX = re.compile(r"GruntHunt|HeadHunt|Bounty", re.I)
SKIP_EDID_RX = re.compile(r"^(ATX_|zzz|CUT_|DEL_|POST_)|Seasonal|SDOW", re.I)
SV_RX = re.compile(r"RTSV|Skyline", re.I)
BS_RX = re.compile(r"Burn", re.I)


def conds(it):
    return [str(c) for c in (it.get("conditions_display") or []) if c]


def all_conds(it):
    out = conds(it)
    for ch in it.get("children") or []:
        out += all_conds(ch)
    return out


def hunt_of(it):
    full = str(it.get("full") or "").lower()
    edid = str(it.get("edid") or "")
    if "grunt hunt" in full:
        return "grunt"
    if "head hunt" in full:
        return "head"
    joined = " ".join(conds(it)).lower()
    if "grunt hunts" in joined and "head hunts" not in joined:
        return "grunt"
    if re.search(r"Grunt", edid, re.I):
        return "grunt"
    return "head"


def region_of(it):
    edid = str(it.get("edid") or "")
    if SV_RX.search(edid):
        return "sv"
    if BS_RX.search(edid):
        return "bs"
    joined = " ".join(all_conds(it))
    sv, bs = bool(SV_RX.search(joined)), bool(BS_RX.search(joined))
    if sv and not bs:
        return "sv"
    if bs and not sv:
        return "bs"
    return "head"


def panel_of(it):
    edid = str(it.get("edid") or "")
    scope = str(it.get("scope") or "").strip().title()
    if scope in ("Daily", "Weekly", "Event"):
        return scope
    if edid.startswith("WorldPets_"):
        return "World Pets"
    return "Lifetime"


def stamp(it, guides, trackable):
    it = dict(it)
    if not it.get("guides"):
        it["guides"] = guides
    it["trackable"] = trackable
    it["children"] = [stamp(ch, guides, trackable) for ch in it.get("children") or []]
    return it


def main():
    if not SRC.exists():
        sys.exit(f"[bounty-challenges] ERROR: {SRC} not found - run build_challenges_json_v3.py first")
    data = json.loads(SRC.read_text(encoding="utf-8"))
    pages = data.get("pages") or {}

    # Every row that sits under another row travels with its parent.
    child_ids = set()
    def walk(it):
        for ch in it.get("children") or []:
            child_ids.add(ch.get("form_id"))
            walk(ch)
    for pg in pages.values():
        for it in pg.get("items") or []:
            walk(it)

    picked, seen = [], set()
    for key, pg in pages.items():
        for it in pg.get("items") or []:
            fid, edid = it.get("form_id"), str(it.get("edid") or "")
            if not fid or fid in seen or fid in child_ids:
                continue
            if it.get("is_cut") or SKIP_EDID_RX.search(edid):
                continue
            text = f"{it.get('full', '')} " + " ".join(str(ch.get("full", "")) for ch in it.get("children") or [])
            if not (TOPIC_RX.search(text) or EDID_RX.search(edid)):
                continue
            seen.add(fid)
            picked.append(it)

    buckets = {h: {p: [] for p in PANEL_ORDER} for h in HUNT_ORDER}
    for it in picked:
        hunt = hunt_of(it)
        root = "grunt" if hunt == "grunt" else region_of(it)
        panel = panel_of(it)
        buckets[root][panel].append(stamp(it, HUNTS[root]["guides"], panel not in UNTRACKED))

    hunts, totals = [], {"rows": 0, "trackable": 0, "untracked": 0}
    for h in HUNT_ORDER:
        groups = []
        for p in PANEL_ORDER:
            items = buckets[h][p]
            if not items:
                continue
            groups.append({"label": p, "trackable": p not in UNTRACKED, "items": items})
            n = sum(1 + len(i.get("children") or []) for i in items)
            totals["rows"] += n
            totals["trackable" if p not in UNTRACKED else "untracked"] += n
        if groups:
            hunts.append({"id": h, "label": HUNTS[h]["label"], "guides": HUNTS[h]["guides"], "groups": groups})

    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "channel": "pts" if PTS else "live",
        "source": {"file": "dist/challenges/challenges.json", "generated": data.get("generated")},
        "page": "/df/bounty-hunting/grunt-and-head-hunt-challenges/",
        "totals": totals,
        "hunts": hunts,
    }
    DIST.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"[bounty-challenges] {len(picked)} top-level challenges, {totals['rows']} rows "
          f"({totals['trackable']} tracked, {totals['untracked']} daily/weekly) -> {OUT.relative_to(ROOT)}")
    for hnt in hunts:
        print("   ", hnt["label"], "|", ", ".join(f"{g['label']} {len(g['items'])}" for g in hnt["groups"]))


if __name__ == "__main__":
    main()
