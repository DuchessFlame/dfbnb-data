#!/usr/bin/env python3
r"""
build_slasher_masks_rewards_json.py - /df/collectables/pint-sized-slasher-masks/checklist/

The Pint-Sized Slasher Masks reward checklist (Duchess, 28 Nov 2026): the
lifetime challenge rewards for collecting masks + the quest reward for
The Slasher: Masked Truth. Same JSON shape as the Brain in a Jar rewards
(dist/brain_in_a_jar.json -> "rewards"), rendered by df-bnb-brain-in-a-jar.js.

Everything comes from the game files - nothing typed by hand:
  milestones   CHAL SDOW_Challenge_Lifetime_Collect_SlasherClue_* (FormID, EDID,
               FULL, TNAM = masks needed, MNAM = reward text)
  reward item  matched to the Slasher uniques list (build_slasher_uniques.py ->
               dist/seasonal_events/seasonal_events_rewards_by_page.json) by the
               challenge source line, for the real item name, kind, IDs and picture.
               Falls back to the CHAL reward text when no unique matches.
  quest        QUST2 _ScriptProps QuestName of SDOW_MQ01_Bodies
  hand-in      GMRW SDOW_MQ01_QuestRewards (caps global, XP curve, the plan in its list)

Output: dist/slasher_masks_rewards.json
Run after build_slasher_uniques.py (it reads that output for pictures).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import collectable_guide as cg  # noqa: E402
from build_slasher_masks_guide_json import plan_in_list  # noqa: E402

QUEST = "SDOW_MQ01_Bodies"
GMRW = "SDOW_MQ01_QuestRewards"
CHAL_PREFIX = "SDOW_Challenge_Lifetime_Collect_SlasherClue_"
UNIQUES = Path("seasonal_events") / "seasonal_events_rewards_by_page.json"
UNIQUES_KEY = "the-slasher-all-rewards"
LORE_URL = "/df/collectables/slasher-lore/checklist/"
SPAWNS_URL = "/df/collectables/pint-sized-slasher-masks/spawn-locations/"

KIND_TO_TYPE = {"Chem": "Aid", "Player Title": "Title", "C.A.M.P. Title": "Title"}


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def clean_challenge(full):
    return re.sub(r"^\(Seasonal\)\s*", "", full or "").strip()


def load_uniques(dist):
    try:
        d = json.load(open(Path(dist) / UNIQUES, encoding="utf-8"))
        return (d.get("byPage") or {}).get(UNIQUES_KEY, {}).get("uniques") or []
    except (OSError, ValueError):
        return []


def unique_for_challenge(uniques, challenge_full):
    want = clean_challenge(challenge_full).lower()
    for u in uniques:
        for s in u.get("sources") or []:
            if s.get("route") == "Challenges" and want in (s.get("name") or "").lower():
                return u
    return None


def score_utility_pic(name):
    """Fallback picture: the S.C.O.R.E. utility art on the server
    (season_images/utility/score_game_<name>.avif, e.g. Ghost Boy ->
    score_game_ghostboy), only when data/server_listing.tsv lists it."""
    key = re.sub(r"[^a-z0-9]", "", (name or "").lower())
    rel = f"season_images/utility/score_game_{key}.avif"
    try:
        with open(ROOT / "data" / "server_listing.tsv", encoding="utf-8") as fh:
            if any(line.split("\t", 1)[0] == rel for line in fh):
                return "/wp-content/uploads/" + rel
    except OSError:
        pass
    return ""


def unique_by_name(uniques, name):
    for u in uniques:
        if (u.get("name") or "") == name:
            return u
    return None


def milestones(tsv, uniques):
    out = []
    for r in cg.rows(cg.newest(tsv, "CHAL_Export_*.tsv")):
        edid = (r.get("EDID") or "").strip()
        if not edid.startswith(CHAL_PREFIX):
            continue
        try:
            need = int(float(r.get("TNAM") or 0))
        except ValueError:
            continue
        reward_text = (r.get("MNAM") or "").strip()
        u = unique_for_challenge(uniques, r.get("FULL"))
        name = (u or {}).get("name") or reward_text
        kind = (u or {}).get("kind") or ""
        pics = (u or {}).get("pics") or []
        m = {
            "id": "rw-" + slug(name),
            "name": name,
            "milestone": need,
            "type": KIND_TO_TYPE.get(kind, kind or "Reward"),
            "challenge": clean_challenge(r.get("FULL")),
            "form_id": (r.get("FormID") or "").strip(),
            "edid": edid,
            "image": pics[0] if pics else score_utility_pic(name),
            "note": "",
        }
        if reward_text and reward_text != name:
            m["reward_text"] = reward_text          # e.g. "Ghost Boy (1)" - the amount
        if u and u.get("formid"):
            m["item"] = {"form_id": u["formid"], "edid": u.get("edid") or "", "sig": u.get("sig") or ""}
        out.append(m)
    out.sort(key=lambda m: m["milestone"])
    return out


def hand_in(tsv, uniques):
    rw = cg.gmrw_reward(tsv, GMRW)
    plan = next((p for p in (plan_in_list(tsv, l) for l in rw["lists"]) if p), "")
    base_xp = cg.curve_base_xp(tsv, rw["xp_curve"]) if rw.get("xp_curve") else None
    u = unique_by_name(uniques, plan) if plan else None
    unlocks = []
    if plan:
        unlocks.append({
            "name": plan, "kind": "Plan",
            "item": (u or {}).get("formid", ""), "item_edid": (u or {}).get("edid", ""),
            "image": ((u or {}).get("pics") or [""])[0],
        })
    return {"edid": GMRW, "caps": rw.get("caps"), "base_xp": base_xp,
            "xp_curve": [rw["xp_curve"]] if rw.get("xp_curve") else [],
            "loot_list": ", ".join(l for l in rw["lists"]), "unlocks": unlocks}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(ROOT / "tsv"))
    ap.add_argument("--dist", default=str(ROOT / "dist"))
    a = ap.parse_args()
    uniques = load_uniques(a.dist)
    quest, _ = cg.quest_batches(a.data_dir, QUEST)
    miles = milestones(a.data_dir, uniques)
    if not miles:
        print("build_slasher_masks_rewards_json: no SlasherClue lifetime challenges found; dist left as is")
        return 0
    doc = {
        "_meta": {"built_by": "src/build_slasher_masks_rewards_json.py",
                  "sources": ["CHAL", "GMRW", "QUST2 _ScriptProps", "BOOK", "LVLI", str(UNIQUES)]},
        "set": "pint-sized-slasher-masks",
        "name": "Pint-Sized Slasher Masks",
        "quest": quest or "The Slasher: Masked Truth",
        "quest_edid": QUEST,
        "lore_url": LORE_URL,
        "spawns_url": SPAWNS_URL,
        "rewards": {"milestones": miles, "hand_in": hand_in(a.data_dir, uniques), "quest_weapon": None},
    }
    out = Path(a.dist) / "slasher_masks_rewards.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
        f.write("\n")
    pics = sum(1 for m in miles if m["image"])
    print(f"{out.name}: {doc['quest']}, {len(miles)} lifetime rewards ({pics} with pictures), "
          f"quest plan: {(doc['rewards']['hand_in']['unlocks'] or [{}])[0].get('name', '-')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
