#!/usr/bin/env python3
r"""
slasher_guide.py — data for /df/seasonal-events/the-slasher/the-slasher-guide/

Called by build_seasonal_events_json.py; the page reads
byPage["the-slasher-guide"] from seasonal_events_rewards_by_page.json.

WHAT COMES FROM THE GAME FILES (rebuilt every patch, nothing typed by hand)
  phases          the objectives of the tracker quest "(Seasonal) The Slasher"
                  ("Phase 1: ...", "Finale: ..."; a line with no "Phase"/
                  "Finale" prefix continues the one before it)
  story quests    name, steps, the quest chain that starts it, availability —
                  plan_quest_guides.Guides over QUST2 _RewardQuests
  Disturbed Grave the repeatable quest's steps (same reader)
  grave count     dist/collectable_spawns_pint-sized-phantom-graves.json "total"
  challenges      CHAL export: every challenge whose EditorID or conditions
                  name a Slasher (SDOW_) record; type (Daily / Weekly /
                  Lifetime), required count, reward (lifetime rewards)
  PA paints       the finale quest's reward list + the Reborn Slasher's death
                  list (NPC from the "kill" challenge's GetIsID condition ->
                  NPC INAM), with rates/tradeable from dist/plan_master.json

WHAT IS CONFIG (structure only — no game text)
  QUEST_FOR_PHASE   which story quest belongs to which phase number. The files
                    only tie them together through script toggles.
  CHALLENGE_PHASE   which phase a challenge belongs to, by a word in its
                    EditorID. Anything unmatched goes to "whole event".
The page's own prose (plain-words lines, descriptions, links, pictures) lives
in df-bnb-seasonal-events.js, keyed by the phase "key" written here.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import tsv_source              # noqa: E402
import plan_quest_guides       # noqa: E402

csv.field_size_limit(1 << 30)

SLUG = "the-slasher-guide"
URL = "/df/seasonal-events/the-slasher/the-slasher-guide/"
TRACKER_PREFIX = "(Seasonal) The Slasher"
EDID_PREFIX = "SDOW_"
GRAVES_JSON = "collectable_spawns_pint-sized-phantom-graves.json"

# phase key -> story quest EditorID (structure only; names/steps come from files)
QUEST_FOR_PHASE = {
    "phase-1": "SDOW_MQ01_Bodies",
    "phase-2": "SDOW_MQ02_Graves",
    "phase-3": "SDOW_MQ04_Infestations",
    "finale": "SDOW_MQ05_Headhunt",
}
# the repeatable side quest shown under a phase
EXTRA_QUESTS = {"phase-2": ["SDOW_SQ01_Graves_Repeatable"]}

# first matching word in a challenge's EditorID -> phase key
CHALLENGE_PHASE = [
    ("SlasherClue", "phase-1"),
    ("DailyOps", "phase-2"),
    ("SDOW_SQ01", "phase-2"),
    ("HostileTakeover", "phase-3"),
    ("PartyCrasher_MN2", "phase-4"),
    ("PartyCrasher_HeadHunt", "finale"),
]
TYPE_ORDER = ["Lifetime", "Daily", "Weekly"]


def _rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)


def _newest(root, pat):
    return tsv_source.newest(os.path.join(root, pat), required=False)


def _fid(cell):
    return (cell or "").strip().split(":")[0].upper()


def _rate4(r):
    return "{:.4f}%".format((r or 0) * 100)


def _phase_key(label):
    m = re.match(r"phase\s*(\d+)", label, re.I)
    return "phase-{}".format(m.group(1)) if m else re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def _tracker(guides):
    """(quest record, [phase dicts]) from the '(Seasonal) The Slasher' quest."""
    for fid, q in guides.quests.items():
        if not q["name"].startswith(TRACKER_PREFIX):
            continue
        phases = []
        for _, raw in sorted(q["obj"], key=lambda x: x[0]):
            t = plan_quest_guides.clean_step(raw)
            m = re.match(r"^((?:Phase\s*\d+)|Finale)\s*:\s*(.+)$", t, re.I)
            if m:
                label = m.group(1).strip()
                phases.append({"key": _phase_key(label), "label": label,
                               "gameText": m.group(2).strip()})
            elif phases and t:
                phases[-1]["gameText"] += " " + t
        log = next((l for _, l in sorted(q["logs"]) if l), "")
        intro = re.sub(r"\s*\([^()]*\)\s*$", "", log).strip()
        return q, phases, intro
    return None, [], ""


def _quest_block(guides, edid):
    fid = guides.edid_to_fid.get(edid)
    if not fid:
        return None
    g = guides.guide(fid)
    if not g:
        return None
    out = {"formId": fid, "edid": edid, "name": g["name"], "steps": g["steps"]}
    for k in ("requires", "availability", "more_steps"):
        if g.get(k):
            out[k] = g[k]
    q = guides.quests[fid]
    out["rewardGmrw"] = sorted(q["rewards"])
    return out


def _challenges(tsv_root):
    p = _newest(tsv_root, "CHAL_Export_*.tsv")
    out = []
    if not p:
        return out
    for r in _rows(p):
        edid = (r.get("EDID") or "").strip()
        if not edid or edid.lower().startswith(("zzz", "cut_", "del_")):
            continue
        conds = " ".join(v for k, v in r.items() if k and k.startswith("Cond") and v)
        if EDID_PREFIX not in edid and EDID_PREFIX not in conds:
            continue
        name = (r.get("FULL") or "").strip()
        if not name:
            continue
        ctype = (r.get("CNAM") or "").strip().title()
        phase = next((p_ for w, p_ in CHALLENGE_PHASE if w.lower() in edid.lower()), "event")
        try:
            req = int(float(r.get("TNAM") or 0))
        except ValueError:
            req = None
        c = {"name": name, "type": ctype, "required": req, "phase": phase, "edid": edid}
        rew = (r.get("MNAM") or "").strip()
        if rew and ctype == "Lifetime":
            c["reward"] = rew
        m = re.search(r"GetIsID\|[^|]*\|[^|]*\|[^\"\[]*\"([^\"]+)\" \[NPC_:([0-9A-F]{8})\]", conds)
        if m:
            c["target"] = {"name": m.group(1), "formId": m.group(2)}
        out.append(c)
    order = {t: i for i, t in enumerate(TYPE_ORDER)}
    out.sort(key=lambda c: (order.get(c["type"], 9), c["required"] or 0, c["name"]))
    return out


def _death_lists(tsv_root, npc_fids):
    p = _newest(tsv_root, "NPC_Export_*.tsv", )
    if not p or not npc_fids:
        return {}
    out = {}
    for r in _rows(p):
        fid = (r.get("FormID") or "").strip().upper()
        if fid in npc_fids and r.get("INAM_FormID"):
            out[fid] = (r.get("INAM_FormID") or "").strip().upper()
    return out


def _paints(dist_root, finale, challenges, tsv_root):
    """Plans the finale pays out (quest reward) or its boss drops (death list)."""
    pm = Path(dist_root) / "plan_master.json"
    if not pm.exists():
        return []
    items = json.load(open(pm, encoding="utf-8")).get("items") or []
    boss = {c["target"]["formId"]: c["target"]["name"] for c in challenges
            if c.get("phase") == "finale" and c.get("target")}
    deaths = _death_lists(tsv_root, set(boss))
    death_lists = {v: boss[k] for k, v in deaths.items()}
    # the finale quest's own reward lists (its GMRW -> RewardedItem)
    reward_lists = set()
    gp = _newest(tsv_root, "GMRW_Export_*.tsv")
    if gp and finale:
        for r in _rows(gp):
            if (r.get("FormID") or "").strip().upper() in finale.get("rewardGmrw", []):
                reward_lists.add(_fid(r.get("RewardedItem")))
    out = []
    for it in items:
        if it.get("cut"):
            continue
        rows = []
        for r in it.get("obtain_routes") or []:
            lv = {str(x).upper() for x in r.get("lvli") or []}
            # Only the finale's own guaranteed reward - not the shared quest
            # grab-bag every story quest also rolls (hundreds of plans at <1%).
            if finale and r.get("rate") == 1.0 and (
                    r.get("quest_guide") == finale.get("formId")
                    or (r.get("kind") == "quest" and lv & reward_lists)):
                rows.append({"how": "quest", "quest": finale["name"],
                             "rate": r.get("rate"), "rateDisplay": _rate4(r.get("rate"))})
            elif lv & set(death_lists):
                who = death_lists[next(iter(lv & set(death_lists)))]
                rows.append({"how": "drop", "from": who,
                             "rate": r.get("rate"), "rateDisplay": _rate4(r.get("rate"))})
        if rows:
            out.append({"name": it.get("name"), "tradeable": it.get("tradeable"),
                        "image": (it.get("images") or [None])[0], "sources": rows})
    out.sort(key=lambda x: (x["sources"][0]["how"] != "quest", x["name"] or ""))
    return out


def build(tsv_root, dist_root=None):
    tsv_root = str(tsv_root)
    if dist_root is None:
        repo = HERE.parent
        dist_root = repo / "dist" / ("pts" if os.path.basename(tsv_root.rstrip("/\\")) == "pts" else "")
    guides = plan_quest_guides.Guides(tsv_root)
    if not guides.ok:
        print("  [WARN] {}: no QUST2 _RewardQuests export - page has no data".format(SLUG))
        return {"slug": SLUG, "ok": False}
    tq, phases, intro = _tracker(guides)
    if not tq:
        print("  [WARN] {}: no '{}' tracker quest in the export".format(SLUG, TRACKER_PREFIX))
    challenges = _challenges(tsv_root)
    for ph in phases:
        e = QUEST_FOR_PHASE.get(ph["key"])
        if e:
            ph["quest"] = _quest_block(guides, e)
        extra = [b for b in (_quest_block(guides, x) for x in EXTRA_QUESTS.get(ph["key"], [])) if b]
        if extra:
            ph["sideQuests"] = extra
        ph["challenges"] = [c for c in challenges if c["phase"] == ph["key"]]
    finale = next((p.get("quest") for p in phases if p["key"] == "finale"), None)
    graves = None
    gp = Path(dist_root) / GRAVES_JSON
    if gp.exists():
        try:
            graves = json.load(open(gp, encoding="utf-8")).get("total")
        except Exception:                              # noqa: BLE001
            graves = None
    data = {
        "slug": SLUG, "ok": True,
        "tracker": {"name": tq["name"] if tq else TRACKER_PREFIX, "intro": intro},
        "phases": phases,
        "storyQuests": [p["quest"]["name"] for p in phases if p.get("quest")],
        "eventChallenges": [c for c in challenges if c["phase"] == "event"],
        "graveCount": graves,
        "finaleRewards": _paints(dist_root, finale, challenges, tsv_root),
    }
    print("  {}: {} phases, {} story quests, {} challenges, {} finale rewards, graves={}".format(
        SLUG, len(phases), len(data["storyQuests"]), len(challenges),
        len(data["finaleRewards"]), graves))
    return data


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent / "tsv")
    print(json.dumps(build(root), indent=2, ensure_ascii=False)[:6000])
