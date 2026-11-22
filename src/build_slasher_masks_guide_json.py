#!/usr/bin/env python3
r"""
build_slasher_masks_guide_json.py - /df/collectables/pint-sized-slasher-masks/guide/

Prose:  data/pint_sized_slasher_masks/guide.json (hand-written; edit that file).
Output: dist/pint_sized_slasher_masks_guide.json, mounted by df-bnb-collectable-guide.js.
Shared template + JSON shape: src/collectable_guide.py (Duchess, 22 Nov 2026).

Everything countable comes from the game files:
  {quest}          QUST2 _ScriptProps QuestName of SDOW_MQ01_Bodies
  {quest_masks}    masks the quest needs (sum of CollectablesNeededPerBatch)
  {batch_table}    Batch | Masks | Running total + Receive: the note each batch gives
                   (BOOK SDOW_MQ01_Bodies_Note0N, note N = batch N)
  {caps} {plan}    GMRW SDOW_MQ01_QuestRewards (caps global; the plan in its reward list)
  {pickup_xp}      GLOB SDOW_SlasherClue_XP_Reward
  {item_name}      MISC SDOW_SlasherClue
  {total} {challenge_steps} {challenge_max}  dist/collectable_spawns_pint-sized-slasher-masks.json
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

QUEST = "SDOW_MQ01_Bodies"
SRC = ROOT / "data" / "pint_sized_slasher_masks" / "guide.json"
OUT = ROOT / "dist" / "pint_sized_slasher_masks_guide.json"
IMG = "/wp-content/uploads/guide-images/collectables/Slasher-Mask-Locations/mask-guide-images/"


def ordinal(n):
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def plan_in_list(tsv, lvli):
    """The plan (BOOK) a reward list gives: any LVLI export row of that list
    with a ...:BOOK cell."""
    for pat in ("LVLI_Export_*_LVLI_Entries.tsv", "LVLI_Export_*_LVLI_Math.tsv"):
        for r in cg.rows(cg.newest(tsv, pat)):
            v = list(r.values())
            if len(v) > 1 and v[1] == lvli:
                for cell in v:
                    bits = (cell or "").split(":")
                    if len(bits) >= 3 and bits[-1] == "BOOK":
                        name = cg.record_name(tsv, bits[1], ("BOOK_Export_*.tsv",))
                        if name:
                            return name
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(ROOT / "tsv"))
    tsv = ap.parse_args().data_dir
    quest, batches = cg.quest_batches(tsv, QUEST)
    if not batches:
        print(f"build_slasher_masks_guide_json: no CollectablesNeededPerBatch for {QUEST}; dist left as is")
        return 0
    notes = cg.numbered_names(tsv, "BOOK_Export_*.tsv", "EDID", "SDOW_MQ01_Bodies_Note0", skip=("_Locations.tsv",))
    rw = cg.gmrw_reward(tsv, "SDOW_MQ01_QuestRewards")
    plan = next((p for p in (plan_in_list(tsv, l) for l in rw["lists"]) if p), "")
    xp = cg.glob_value(tsv, "SDOW_SlasherClue_XP_Reward")
    item = cg.record_name(tsv, "SDOW_SlasherClue", ("MISC_Export_*.tsv",)) or "Pint-Sized Slasher Mask"
    try:
        spawns = json.load(open(ROOT / "dist" / "collectable_spawns_pint-sized-slasher-masks.json", encoding="utf-8"))
    except OSError:
        spawns = {}
    steps = sorted({c.get("required") for c in (spawns.get("challenges") or {}).get("items", []) if c.get("required")})
    steps_txt = (", ".join(str(s) for s in steps[:-1]) + f" and {steps[-1]}") if len(steps) > 1 else (str(steps[0]) if steps else "")
    need = sum(batches)
    table = cg.batch_table(batches, notes, "Batch", "Masks")
    num = lambda v: f"{v:,}" if isinstance(v, int) else "some"
    fill = lambda t: (t.replace("{quest}", quest or "The Slasher: Masked Truth")
                       .replace("{quest_masks_th}", ordinal(need)).replace("{quest_masks}", str(need))
                       .replace("{batch_table}", table).replace("{caps}", num(rw["caps"]))
                       .replace("{plan}", plan or "a plan").replace("{pickup_xp}", num(xp))
                       .replace("{item_name}", item).replace("{total}", str(spawns.get("total") or ""))
                       .replace("{challenge_steps}", steps_txt).replace("{challenge_max}", str(steps[-1] if steps else "")))
    doc = cg.build(SRC, OUT, fill, IMG)
    print(f"{OUT.name}: {quest}, {need} masks in {len(batches)} batches, {len(notes)} notes, "
          f"{len(doc['missing_images'])} pictures not on the server yet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
