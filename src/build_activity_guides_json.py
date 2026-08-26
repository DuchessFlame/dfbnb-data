#!/usr/bin/env python3
r"""
build_activity_guides_json.py — Activity Guide pages (quest STAGE walkthroughs)

Generates one JSON per "Activity:" quest for the DF Activity *guide* pages
(/df/activities/<slug>/<slug>-guide/). Each page renders every quest STAGE as a
collapsible expand (stage order) carrying that stage's verbatim journal/log text
plus an empty per-stage image slot the editor fills later.

DATA SOURCE — the QUST2 export set (NOT the plain QUEST export, which has no
stage data):
    QUST2_Export_*_Quests.tsv   -> quest FULL name / EDID / DESC
    QUST2_Export_*_Stages.tsv   -> StageIndex / StageFlags / LogText (the stages)

Stage text is pulled VERBATIM. A stage with no log text is emitted honestly with
log_text == "" (an internal script trigger, not a player journal step); the
renderer decides how to show it.

Channel-explicit via tsv_source: live reads tsv/, pts reads tsv/pts/.

Usage:
    python src/build_activity_guides_json.py                 # live  -> dist/activity_guides/
    python src/build_activity_guides_json.py --pts           # pts   -> dist/pts/activity_guides/
    python src/build_activity_guides_json.py --out <dir>     # override output dir (checkpoint/testing)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import tsv_source  # noqa: E402

CATEGORY = "Activities"

# ── Seed: quest FormID -> existing guide slug (guide_index df-events-activities-<slug>) ──
# Authoritative mapping. Quest FULL names don't always slugify to the live slug
# ("Battle Bots" -> battle-bot, "Powering Up <Alias=...>" -> powering-up-power-station),
# and "Monster Mash" has two quests — we take the one that carries journal text.
# This is config, not data — no rates or reward FormIDs are hardcoded here.
QUEST_SLUGS = {
    "00560B13": "riding-shotgun",
    "0052BDF7": "awol-armaments",
    "0052808B": "census-violence",
    "0002443A": "monster-mash",            # Watoga High Monster Mash (has journal text)
    "003E4E89": "powering-up-power-station",
    "0034608A": "surface-to-air",
    "00331AB2": "battle-bot",
    "0031307F": "distant-thunder",
    "00304A39": "its-a-trap",
    "002D64EE": "collision-course",
    "00275BC5": "irrational-fear",
    "0025C0F4": "grafton-day",
    "0015D682": "back-on-the-beat",
    "0010E200": "manhunt",
    "000A73DC": "always-vigilant",
    "00095360": "leader-of-the-pack",
    "0006A378": "breach-and-clear",
    "0005A243": "patrol-duty",
    "0004695C": "project-beanstalk",
    "00036191": "death-blossoms",
    "00029183": "fly-swatter",
    "0000FFED": "fertile-soil",
    "0000418B": "protest-march",
    # "0000123F": Activity: <Alias.Race=Alpha> Horde  -> no guide page exists yet
    # "0051AA0B": second "Monster Mash" quest (2 stages, no journal text) -> folded out
}


def clean_name(full: str) -> str:
    """Strip the 'Activity:' prefix and any <Alias...> placeholders, tidy spacing."""
    s = re.sub(r"^\s*Activity:\s*", "", full or "")
    s = re.sub(r"<[^>]*>", "", s)          # drop alias tokens
    s = re.sub(r"\s{2,}", " ", s)
    s = s.strip(" :–—-")
    return s.strip()


def read_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


def build(channel: str, out_dir: str) -> dict:
    quests_path = tsv_source.newest("QUST2_Export_*_Quests.tsv", channel=channel)
    stages_path = tsv_source.newest("QUST2_Export_*_Stages.tsv", channel=channel)

    quests = {}   # formid -> row
    for r in read_rows(quests_path):
        quests[(r.get("FormID") or "").strip().upper()] = r

    # stages grouped: formid -> {stage_index: {flags, logs:[...]}}
    stages_by_quest = {}
    stage_order = {}
    for r in read_rows(stages_path):
        fid = (r.get("QuestFormID") or "").strip().upper()
        if fid not in QUEST_SLUGS:
            continue
        try:
            idx = int((r.get("StageIndex") or "0").strip())
        except ValueError:
            continue
        d = stages_by_quest.setdefault(fid, {})
        s = d.setdefault(idx, {"index": idx, "flags": "", "logs": []})
        flags = (r.get("StageFlags") or "").strip()
        if flags and not s["flags"]:
            s["flags"] = flags
        log = (r.get("LogText") or "").strip()
        if log:
            s["logs"].append(log)
        stage_order.setdefault(fid, [])

    os.makedirs(out_dir, exist_ok=True)
    manifest = {"channel": channel, "pages": [], "source": {
        "quests": os.path.basename(quests_path),
        "stages": os.path.basename(stages_path),
    }}

    for fid, slug in sorted(QUEST_SLUGS.items(), key=lambda kv: kv[1]):
        q = quests.get(fid)
        if not q:
            print(f"[warn] quest {fid} ({slug}) not found in {os.path.basename(quests_path)} — skipped")
            continue
        name = clean_name(q.get("FULL", ""))
        raw_stages = stages_by_quest.get(fid, {})
        total_stages = len(raw_stages)          # every stage incl. script triggers
        # LIST model: only stages that carry player-facing journal/task text get a
        # row. Empty script-trigger stages have no task to list, so they're skipped
        # here — the honest total is still reported via stage_count.
        stages = []
        step = 0
        for idx in sorted(raw_stages):
            s = raw_stages[idx]
            log = "\n\n".join(s["logs"]).strip()
            if not log:
                continue
            step += 1
            stages.append({
                "step": step,                   # 1..N running order of listed tasks
                "index": idx,                   # true StageIndex from the export
                "flags": s["flags"],
                "log_text": log,                # verbatim task text
                "image": "",                    # per-stage slot (editor fills later)
            })
        doc = {
            "slug": slug,
            "quest_form_id": fid,
            "quest_edid": (q.get("EDID") or "").strip(),
            "category": CATEGORY,
            "name": name,
            "page_title": f"{CATEGORY} - {name} Guide",
            "blurb": (q.get("DESC") or "").strip(),
            "quest_type": (q.get("QuestType") or "").strip(),
            "stage_count": total_stages,        # honest total (all stages)
            "stages_with_log": len(stages),     # how many have task text (listed)
            "stages": stages,
            "_source": manifest["source"],
        }
        out_path = os.path.join(out_dir, f"{slug}.json")
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
        manifest["pages"].append({"slug": slug, "name": name, "form_id": fid,
                                  "stage_count": total_stages, "stages_with_log": len(stages)})
        print(f"[ok] {slug:28s} listed={len(stages):3d} / total={total_stages:3d}  ({name})")

    with open(os.path.join(out_dir, "_index.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    return manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true", help="read tsv/pts/ -> dist/pts/activity_guides/")
    ap.add_argument("--out", default=None, help="override output directory")
    args = ap.parse_args()
    channel = "pts" if args.pts else "live"
    out_dir = args.out or os.path.join(
        ROOT, "dist", "pts" if args.pts else "", "activity_guides")
    out_dir = os.path.normpath(out_dir)
    m = build(channel, out_dir)
    print(f"\n[done] {len(m['pages'])} activity guide pages -> {out_dir}")
    print(f"       source: {m['source']['quests']} / {m['source']['stages']}")


if __name__ == "__main__":
    main()
