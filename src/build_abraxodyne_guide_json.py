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
  {pickup_xp}    MISCSP Burn_SQ04_AbraxoIntelDispenser AmountOfXPToGive (XP per briefcase)
  {cache_xp}     ACTI Burn_SQ04_AbraxoCache_Activator AmountOfXPToGive
  {rewards_table} Hand-in | Caps | XP from RewardsList[i] (QUST2 _ScriptProps struct rows)
  {challenge_steps} CHAL Burn_Challenge_Lifetime_Collect_AbraxodyneChemicalBriefcase_* TNAM
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
        locs = json.load(open(ROOT / "dist" / "abraxodyne_intel_briefcases.json", encoding="utf-8"))
    except OSError:
        locs = {}
    total = locs.get("total_real") or ""
    # Every briefcase is in Burning Springs (the location data has no region column).
    finding = cg.finding_sentence(total, {"Burning Springs": total}, "Abraxodyne Intel Briefcases",
                                  "Briefcase Locations", len(locs.get("pages") or [])) if total else ""
    # Which hand-ins unlock an Abraxodyne Cache: the script's
    # CacheStageAndBatchNumber struct (7, 11, 14 on live, Nov 2026).
    caches = cg.struct_batches(tsv, QUEST, "CacheStageAndBatchNumber")
    cache_rows = {b: [("Cache", "Keycode and coordinates for an Abraxodyne Cache (optional)")] for b in caches}
    table = cg.batch_table(batches, tapes, "Hand-in", "Briefcases", cache_rows)
    # Quest Rewards (Duchess, 28 Nov 2026): pick-up XP from the briefcase's own
    # script (MISCSP), the cache's from its activator, caps + XP per hand-in from
    # the quest script's RewardsList, challenge tiers from CHAL.
    pickup_xp = cg.misc_script_value(tsv, "Burn_SQ04_AbraxoIntelDispenser", "AmountOfXPToGive")
    cache_xp = cg.acti_script_value(tsv, "Burn_SQ04_AbraxoCache_Activator", "AmountOfXPToGive")
    rewards = cg.rewards_table(cg.hand_in_rewards(tsv, QUEST))
    steps = cg.challenge_steps(tsv, "Burn_Challenge_Lifetime_Collect_AbraxodyneChemicalBriefcase_")
    num = lambda v: f"{v:,}" if isinstance(v, int) else "some"
    fill = lambda t: (t.replace("{quest}", quest or "Dirty Laundry").replace("{batch_count}", str(len(batches)))
                       .replace("{quest_total}", str(sum(batches))).replace("{batch_table}", table)
                       .replace("{caps}", num(rw["caps"])).replace("{xp}", num(rw["xp"]))
                       .replace("{plan}", plan or "a plan").replace("{item_name}", item)
                       .replace("{total}", str(total)).replace("{finding_sentence}", finding)
                       .replace("{cache_count}", str(len(caches)))
                       .replace("{cache_batches}", cg.and_list(caches) or "some")
                       .replace("{pickup_xp}", num(pickup_xp)).replace("{cache_xp}", num(cache_xp))
                       .replace("{rewards_table}", rewards)
                       .replace("{challenge_steps}", cg.and_list(steps) or "several"))
    doc = cg.build(SRC, OUT, fill, IMG, quest=cg.quest_info(tsv, QUEST),
                   faq_nums={"total": total, "quest_total": sum(batches),
                             "challenge_max": steps[-1] if steps else None})
    print(f"{OUT.name}: {quest}, {len(batches)} hand-ins, {sum(batches)} briefcases, "
          f"{len(tapes)} tapes, caches after hand-ins {caches}, "
          f"{len(doc['missing_images'])} pictures not on the server yet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
