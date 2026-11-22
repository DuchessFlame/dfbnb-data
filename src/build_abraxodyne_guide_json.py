#!/usr/bin/env python3
r"""
build_abraxodyne_guide_json.py - /df/collectables/abraxodyne-intel-briefcases/guide/

Prose:  data/abraxodyne_intel_briefcases/guide.json (hand-written; edit that file).
Output: dist/abraxodyne_intel_briefcase_guide.json, mounted by df-bnb-collectable-guide.js.
Shared template + JSON shape: src/collectable_guide.py (Duchess, 22 Nov 2026).

Everything countable comes from the game files:
  {quest}        QUST2 _ScriptProps QuestName of Burn_SQ04_DirtyLaundry
  {batch_count}  hand-ins (CollectablesNeededPerBatch rows, in order)
  {quest_total}  briefcases for every hand-in
  {batch_table}  Hand-in | Briefcases | Running total + Receive: Abraxodyne Tape NN
                 (NOTE Burn_SQ04_DirtyLaundry_HolotapeN, tape N = hand-in N)
  {caps} {xp} {plan}  GMRW Burn_SQ04_QuestReward_Final (caps/XP globals, the BOOK it gives)
  {item_name}    MISC Burn_SQ04_AbraxoIntelDispenser (what a briefcase becomes in the Pip-Boy)
  {total}        total_real in dist/abraxodyne_intel_briefcases.json
If the quest export is missing, the dist is left as it is.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import collectable_guide as cg  # noqa: E402

QUEST = "Burn_SQ04_DirtyLaundry"
SRC = ROOT / "data" / "abraxodyne_intel_briefcases" / "guide.json"
OUT = ROOT / "dist" / "abraxodyne_intel_briefcase_guide.json"
IMG = "/wp-content/uploads/guide-images/collectables/Abraxodyne-Intel-Briefcase-Locations/guide-images/"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(ROOT / "tsv"))
    tsv = ap.parse_args().data_dir
    quest, batches = cg.quest_batches(tsv, QUEST)
    if not batches:
        print(f"build_abraxodyne_guide_json: no CollectablesNeededPerBatch for {QUEST}; dist left as is")
        return 0
    tapes = cg.numbered_names(tsv, "NOTE_Export_*.tsv", "NOTE_EDID", "Burn_SQ04_DirtyLaundry_Holotape")
    rw = cg.gmrw_reward(tsv, "Burn_SQ04_QuestReward_Final")
    plan = next((cg.record_name(tsv, e) for e in rw["items"] if cg.record_name(tsv, e)), "")
    item = cg.record_name(tsv, "Burn_SQ04_AbraxoIntelDispenser", ("MISC_Export_*.tsv",)) or "Abraxodyne Intel"
    try:
        total = json.load(open(ROOT / "dist" / "abraxodyne_intel_briefcases.json", encoding="utf-8")).get("total_real") or ""
    except OSError:
        total = ""
    table = cg.batch_table(batches, tapes, "Hand-in", "Briefcases")
    num = lambda v: f"{v:,}" if isinstance(v, int) else "some"
    fill = lambda t: (t.replace("{quest}", quest or "Dirty Laundry").replace("{batch_count}", str(len(batches)))
                       .replace("{quest_total}", str(sum(batches))).replace("{batch_table}", table)
                       .replace("{caps}", num(rw["caps"])).replace("{xp}", num(rw["xp"]))
                       .replace("{plan}", plan or "a plan").replace("{item_name}", item)
                       .replace("{total}", str(total)))
    doc = cg.build(SRC, OUT, fill, IMG)
    print(f"{OUT.name}: {quest}, {len(batches)} hand-ins, {sum(batches)} briefcases, "
          f"{len(tapes)} tapes, {len(doc['missing_images'])} pictures not on the server yet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
