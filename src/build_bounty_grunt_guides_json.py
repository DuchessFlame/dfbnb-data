#!/usr/bin/env python3
"""
build_bounty_grunt_guides_json.py - data for the Skyline Valley bounty guide pages (Nov 2026).

Pages:  /df/bounty-hunting/grunt-hunts/skyline-valley-grunt-hunts-guide/
        /df/bounty-hunting/head-hunts/skyline-valley-head-hunts-guide/   (byPage key
        "skyline-valley-head-hunts-guide", see build_head_guide() at the end)
Output: dist/bounty-hunting/grunt_hunt_guides.json   (PTS twin: dist/pts/...)
        (file name kept from the first version - it holds both pages)

The guide's words live in df-bnb-bounty-hunting.js; every fact the words lean
on comes from here, read from the xEdit TSVs - nothing typed in:

  locations     FLST BountyHunt_GruntHuntLocations_<Region> (the list the
                master quest's GruntHuntRegionLists property points at), names
                from the FLST / LCTN exports
  support wave  each location's BountyHunt_Location<Faction> keyword (LCTN
                export KW_ columns), matched to the region's support-wave
                keywords (FLST BountyHunt_GruntHuntSupportWaves_<Region>_DEBUG,
                whose entries carry the in-game wave names)
  outlaws       FLST BountyHunt_GruntHuntTargets_DEBUG (in-game names)
  star tiers    BountyHunt_Master's Challenge_2StarUnlock / _3StarUnlock
                properties -> CHAL export (TNAM count, name); the challenges
                count Grunt Hunts from BOTH regions (FLST
                Challenge_AllBounty_CompletedGruntHunt)
  timers etc.   GLOB BountyHunt_DailyFailTimer, the Wanted Poster range GLOBs,
                the quest's MinNumOfEnemies and Recent* properties
  start point   the region's bounty hub LCTN name

One thing is not in any export: which support wave a location WITHOUT a
faction keyword falls back to. It is a struct property on the quest script
(SupportWaves[].IsDefaultSupportWave, read from SeventySix.esm with
tools/esm_reader.py on 15 Nov 2026) - kept in DEFAULT_SUPPORT below.

A region whose location list is not in this channel's exports is skipped, so
live (no Skyline Valley yet) writes no Skyline Valley page.

Usage:
    python src/build_bounty_grunt_guides_json.py            # live: tsv/ -> dist/
    python src/build_bounty_grunt_guides_json.py --pts      # local PTS: tsv/pts -> dist/pts
    (CI PTS: tsv/ is already PTS and the relocate step moves dist/ -> dist/pts/)
"""
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tsv_source  # noqa: E402

REPO = Path(__file__).resolve().parent.parent

REGIONS = {
    "skyline-valley-grunt-hunts-guide": {
        "region": "skyline-valley",
        "regionName": "Skyline Valley",
        "quest": "008A8998",            # BountyHunt_SkylineValley_GruntHunt
        "locations": "008A8985",        # BountyHunt_GruntHuntLocations_SkylineValley
        "supportWaves": "008B2123",     # BountyHunt_GruntHuntSupportWaves_SkylineValley_DEBUG
        "hub": "006B9689",              # LocStormTradingPostLocation "The Trading Post"
        "rewardsUrl": "/df/bounty-hunting/grunt-hunts/skyline-valley-grunt-hunt-rewards/",
    },
}
TARGETS_FLST = "0083C250"     # BountyHunt_GruntHuntTargets_DEBUG
MASTER_QUEST = "007F1E8F"     # BountyHunt_Master
DAILY_FAIL_TIMER = "007F74B7" # BountyHunt_DailyFailTimer (seconds)
POSTER_MIN, POSTER_MAX = "00848D6A", "00848D6B"
POSTER_LT_MIN, POSTER_LT_MAX, POSTER_LT_TOGGLE = "00848D69", "00848D6C", "00848D68"

# Struct property, not exported (see docstring). Keyed by support wave keyword.
DEFAULT_SUPPORT = {
    "skyline-valley": "008AC528",   # BountyHunt_Skyline_GruntSupport_BloodEagles_WaveKeyword
    "burning-springs": "0083B063",  # BountyHunt_Burn_GruntSupport_Ferals_WaveKeyword
}


def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, encoding=enc, newline="") as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except UnicodeDecodeError:
            continue
    return []


class Exports:
    def __init__(self, tsv_dir):
        self.tsv = Path(tsv_dir)
        self.used = {}

    def rows(self, pattern):
        p = tsv_source.newest(str(self.tsv / pattern), required=False)
        self.used[pattern] = os.path.basename(p) if p else None
        return read_tsv(p)


def _fid(s):
    s = (s or "").strip().upper()
    return s.split(":")[0][-8:] if s else ""


def _token(edid, prefix_re):
    m = re.search(prefix_re, edid or "")
    t = (m.group(1) if m else "").lower()
    return re.sub(r"s$", "", t)          # RustRaiders ~ RustRaider, Ferals ~ Feral


def build(tsv_dir):
    ex = Exports(tsv_dir)
    flst = {}
    for r in ex.rows("FLST_Export_*_Entries.tsv"):
        flst.setdefault(_fid(r.get("FLST_FormID")), []).append(r)
    lctn = {_fid(r.get("LCTN_FormID")): r for r in ex.rows("LCTN_Export_*_LCTN.tsv")}
    globs = {}
    for r in ex.rows("GLOB_Export_*.tsv"):
        try:
            globs[_fid(r.get("FormID"))] = float(r.get("FLTV") or 0)
        except ValueError:
            pass
    props = {}
    for r in ex.rows("QUST2_Export_*_ScriptProps.tsv"):
        props.setdefault(_fid(r.get("QuestFormID")), []).append(r)
    chal = {_fid(r.get("FormID")): r for r in ex.rows("CHAL_Export_*.tsv")}
    misc = {_fid(r.get("FormID")): r for r in ex.rows("MISC_Export_*.tsv")}

    def prop(quest, name, all_values=False):
        vals = [r.get("Value") for r in props.get(quest, []) if r.get("Property") == name]
        return vals if all_values else (vals[0] if vals else None)

    outlaws = sorted({(r.get("Entry_FULL") or "").strip() for r in flst.get(TARGETS_FLST, [])} - {""})

    stars = []
    for n, pname in ((2, "Challenge_2StarUnlock"), (3, "Challenge_3StarUnlock")):
        cfid = _fid(prop(MASTER_QUEST, pname))
        c = chal.get(cfid) or {}
        try:
            count = int(float(c.get("TNAM") or 0))
        except ValueError:
            count = 0
        if count:
            stars.append({"star": n, "count": count, "challenge": (c.get("FULL") or "").strip(),
                          "challengeLabel": (c.get("MNAM") or "").strip(), "formid": cfid})

    pages = {}
    for slug, cfg in REGIONS.items():
        locs = flst.get(cfg["locations"])
        if not locs:
            print(f"[grunt guides] {slug}: location list {cfg['locations']} not in this channel - skipped")
            continue
        waves = []
        for r in flst.get(cfg["supportWaves"], []):
            ed = r.get("Entry_EDID") or ""
            waves.append({"formid": _fid(r.get("Entry_FormID")), "name": (r.get("Entry_FULL") or "").strip(),
                          "token": _token(ed, r"GruntSupport_([A-Za-z]+)_WaveKeyword")})
        by_token = {w["token"]: w for w in waves}
        default = next((w for w in waves if w["formid"] == DEFAULT_SUPPORT.get(cfg["region"])), None)
        locations = []
        for r in locs:
            lf = _fid(r.get("Entry_FormID"))
            row = lctn.get(lf, {})
            name = (r.get("Entry_FULL") or row.get("LCTN_FULL") or "").strip()
            wave, explicit = None, False
            for i in range(1, 40):
                kw = row.get(f"KW_{i}") or ""
                m = re.search(r":BountyHunt_Location([A-Za-z]+):", kw)
                if m and _token("x" + m.group(1), r"x([A-Za-z]+)") in by_token:
                    wave, explicit = by_token[_token("x" + m.group(1), r"x([A-Za-z]+)")], True
                    break
            if wave is None:
                wave = default
            locations.append({"formid": lf, "name": name,
                              "support": wave["name"] if wave else None,
                              "supportIsDefault": not explicit})
        locations.sort(key=lambda x: x["name"].lower())

        q = cfg["quest"]
        poster_fid = _fid(prop(q, "HeadhuntWantedPoster"))
        recent = len([p for p in props.get(q, []) if re.match(r"RecentGruntEnemy_\d+$", p.get("Property") or "")])
        try:
            min_enemies = int(float(prop(q, "MinNumOfEnemies") or 0))
        except ValueError:
            min_enemies = 0
        hub = lctn.get(cfg["hub"], {})
        pages[slug] = {
            "region": cfg["region"],
            "regionName": cfg["regionName"],
            "questFormID": q,
            "questName": next((r.get("QuestName") for r in props.get(q, []) if r.get("QuestName")), ""),
            "hub": (hub.get("LCTN_FULL") or "").strip(),
            "hubFormID": cfg["hub"],
            "timeLimitSeconds": int(globs.get(DAILY_FAIL_TIMER, 0)),
            "minEnemies": min_enemies,
            "recentMemory": recent,
            "outlaws": outlaws,
            "stars": stars,
            "wantedPoster": {
                "formid": poster_fid,
                "name": ((misc.get(poster_fid) or {}).get("FULL") or "").strip(),
                "everyMin": int(globs.get(POSTER_MIN, 0)), "everyMax": int(globs.get(POSTER_MAX, 0)),
                "limitedTimeOn": globs.get(POSTER_LT_TOGGLE, 0) >= 1,
                "limitedMin": int(globs.get(POSTER_LT_MIN, 0)), "limitedMax": int(globs.get(POSTER_LT_MAX, 0)),
            },
            "supportWaves": [{"name": w["name"], "formid": w["formid"],
                              "isDefault": bool(default and w["formid"] == default["formid"])} for w in waves],
            "locations": locations,
            "rewardsUrl": cfg["rewardsUrl"],
        }
        print(f"[grunt guides] {slug}: {len(locations)} locations, {len(waves)} support waves, "
              f"{len(outlaws)} outlaws, stars {[s['count'] for s in stars]}")
    return {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sources": ex.used, "byPage": pages}


# ── Skyline Valley Head Hunt guide ──────────────────────────────────────────
HEAD_REGIONS = {
    "skyline-valley-head-hunts-guide": {
        "region": "skyline-valley",
        "regionName": "Skyline Valley",
        "quest": "008A8999",            # BountyHunt_SkylineValley_Headhunt
        "locations": "008A8981",        # BountyHunt_HeadHuntLocations_SkylineValley
        "hub": "006B9689",              # The Trading Post
        "giverNpc": "008ABBD0",         # RTSV_MarshalCanton (sells the poster - the game's
                                        # own Bounty Hunting note says so)
        "bossStats": "boss_stats/head_hunts_skyline.json",
        "rewardsUrl": "/df/bounty-hunting/head-hunts/skyline-valley-head-hunt-rewards/",
        "bossStatsUrl": "/df/bounty-hunting/head-hunts/skyline-valley-head-hunt-boss-stats/",
    },
}
HEAD_TIMERS = {   # GLOBs, seconds
    "beaconWait": "007EBDEC",   # BountyHunt_PublicBeaconWaitTimer
    "launchFail": "00856077",   # BountyHunt_PublicLaunchFailTimer
    "findFail":   "00856076",   # BountyHunt_PublicFindFailTimer
    "killFail":   "007F74B8",   # BountyHunt_PublicKillFailTimer
}
POSTER_PRICE = "00829BA0"       # BountyHunty_CapsPriceHeadHunt
XP_SPELL = "00848D81"           # Burn_Bounty_FortifyXP_SP "On the Hunt"


def build_head(tsv_dir, dist_dir, ex=None):
    ex = ex or Exports(tsv_dir)
    flst = {}
    for r in ex.rows("FLST_Export_*_Entries.tsv"):
        flst.setdefault(_fid(r.get("FLST_FormID")), []).append(r)
    lctn = {_fid(r.get("LCTN_FormID")): r for r in ex.rows("LCTN_Export_*_LCTN.tsv")}
    npc = {_fid(r.get("FormID")): r for r in ex.rows("NPC_Export_*.tsv")}
    misc = {_fid(r.get("FormID")): r for r in ex.rows("MISC_Export_*.tsv")}
    globs, recent_locs = {}, 0
    for r in ex.rows("GLOB_Export_*.tsv"):
        if re.match(r"^BountyHunt_Skyline_RecentHeadhuntLoc_\d+$", r.get("EDID") or ""):
            recent_locs += 1                 # one GLOB per remembered location
        try:
            globs[_fid(r.get("FormID"))] = float(r.get("FLTV") or 0)
        except ValueError:
            pass
    spel = [r for r in ex.rows("SPEL_Export_*_EFFECTS.tsv") if _fid(r.get("SPEL_FormID")) == XP_SPELL]
    props = {}
    for r in ex.rows("QUST2_Export_*_ScriptProps.tsv"):
        props.setdefault(_fid(r.get("QuestFormID")), []).append(r)

    pages = {}
    for slug, cfg in HEAD_REGIONS.items():
        locs = flst.get(cfg["locations"])
        if not locs:
            print(f"[bounty guides] {slug}: location list {cfg['locations']} not in this channel - skipped")
            continue
        poster_fid = ""
        for qf, rows in props.items():                 # the region's Grunt Hunt names the poster
            for r in rows:
                if r.get("Property") == "HeadhuntWantedPoster" and r.get("QuestName", "").startswith(cfg["regionName"]):
                    poster_fid = _fid(r.get("Value"))
        # XP bonus: magnitude / duration and which quest(s) its condition checks
        xp = None
        if spel:
            r0 = spel[0]
            conds = " ".join((r.get("Conditions") or "") for r in spel)
            quests = re.findall(r'PlayerHasQuest\((\w+) "([^"]+)"', conds)
            xp = {"name": (r0.get("SPEL_FULL") or "").strip(),
                  "magnitude": float(r0.get("EFIT_Magnitude") or 0),
                  "seconds": int(globs.get(_fid(r0.get("DURG_Global")), 0) or float(r0.get("EFIT_Duration") or 0)),
                  "conditionQuests": [q[1] for q in quests],
                  "appliesHere": any(cfg["regionName"] in q[1] for q in quests) or not quests}
        # boss groups + side-kicks + support waves from the Boss Stats JSON
        groups, support = [], []
        bs = None
        try:
            with open(Path(dist_dir) / cfg["bossStats"], encoding="utf-8") as f:
                bs = json.load(f)
        except (OSError, ValueError):
            print(f"[bounty guides] {slug}: {cfg['bossStats']} not found - groups left out")
        if bs:
            kicks = {}
            for n in (bs.get("npcs") or {}).get("rows") or []:
                if n.get("boss") and n.get("name") and n["name"] not in kicks.setdefault(n["boss"], []):
                    kicks[n["boss"]].append(n["name"])
            for g in bs.get("groups") or []:
                if g.get("seasonal"):
                    continue
                groups.append({"group": g.get("groupNumber"),
                               "bosses": [{"name": b.get("name"), "sidekicks": kicks.get(b.get("name"), [])}
                                          for b in sorted(g.get("bosses") or [], key=lambda b: b.get("name") or "")]})
            seen = []
            for r in (bs.get("support") or {}).get("rows") or []:
                if r.get("faction") and r["faction"] not in seen:
                    seen.append(r["faction"])
            support = seen
        giver = (npc.get(cfg["giverNpc"]) or {}).get("FULL", "").strip()
        pages[slug] = {
            "region": cfg["region"], "regionName": cfg["regionName"],
            "questFormID": cfg["quest"],
            "hub": (lctn.get(cfg["hub"], {}).get("LCTN_FULL") or "").strip(),
            "giver": giver,
            "posterName": ((misc.get(poster_fid) or {}).get("FULL") or "").strip(),
            "posterPrice": int(globs.get(POSTER_PRICE, 0)),
            "timers": {k: int(globs.get(v, 0)) for k, v in HEAD_TIMERS.items()},
            "xpBonus": xp,
            "locations": sorted({(r.get("Entry_FULL") or "").strip() for r in locs} - {""}, key=str.lower),
            "groups": groups,
            "supportFactions": support,
            "recentMemory": recent_locs or 3,
            "rewardsUrl": cfg["rewardsUrl"],
            "bossStatsUrl": cfg["bossStatsUrl"],
        }
        print(f"[bounty guides] {slug}: {len(pages[slug]['locations'])} locations, {len(groups)} groups, "
              f"giver {giver!r}, poster {pages[slug]['posterName']!r}")
    return pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--tsv-dir", default=None)
    ap.add_argument("--dist", default=None)
    a = ap.parse_args()
    tsv = Path(a.tsv_dir) if a.tsv_dir else REPO / "tsv" / ("pts" if a.pts else "")
    dist = Path(a.dist) if a.dist else REPO / "dist" / ("pts" if a.pts else "")
    out = dist / "bounty-hunting" / "grunt_hunt_guides.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    data = build(tsv)
    data["byPage"].update(build_head(tsv, dist))
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print(f"[grunt guides] wrote {out} ({len(data['byPage'])} pages)")


if __name__ == "__main__":
    main()
