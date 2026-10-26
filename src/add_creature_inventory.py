#!/usr/bin/env python3
r"""
add_creature_inventory.py — post-step that adds the Inventory and Creatures
expands to the creature pages that do NOT go through build_meat_guide_docs.py:

  dist/insects/<slug>.json    Farming - Insects
  dist/critters/<slug>.json   Farming - Critters (when built)

The insect/critter dist/pts/ copies are left alone — they come from the PTS exports.

With --meat it ALSO patches the two keys into the meat guide docs
(dist/farming_spawns/meat-<slug>_spawns.json and their dist/pts/ copy, which
build_meat_guide_docs.py writes identically), reading the creature from
dist/meat/<slug>.json. That refreshes Inventory + Creatures without a full meat
rebuild, which would drop what later post-steps added to those docs.

Writes two top-level keys (the meat guide docs carry the same data, built inline
by build_meat_guide_docs.py):

  inventory        creature_inventory.build_inventory — what the creature drops
  container_loot   creature_inventory.container_loot — what comes out of the
                   creature's own containers (nests, corpses, guano piles)
  creatures_rows   creature_inventory.build_creatures — who drops this page's
                   own meat and bits, other species included

Run AFTER the insect (and critter) builds, since it rewrites their docs.
Idempotent.

  python src/add_creature_inventory.py              # every insect/critter page
  python src/add_creature_inventory.py bloodbug     # named pages only
  python src/add_creature_inventory.py --meat       # the meat guide docs instead
"""
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import creature_inventory as CI
import rng76

DIST = os.path.join(REPO, "dist")
TSV = os.path.join(REPO, "tsv")
FAMILIES = ["insects", "critters"]


def patch_meat_guides(ictx, want):
    sys.path.insert(0, HERE)
    import build_meat_guide_docs as M
    quests = M._load_quest_names()
    for mp in sorted(glob.glob(os.path.join(DIST, "meat", "*.json"))):
        slug = os.path.basename(mp)[:-5]
        if want and slug not in want:
            continue
        out = os.path.join(DIST, "farming_spawns", f"meat-{slug}_spawns.json")
        if not os.path.exists(out):
            continue
        md = json.load(open(mp, encoding="utf-8"))
        creature = md.get("name") or slug
        lists = [{"form_id": l.get("form_id"), "edid": l.get("edid")}
                 for l in (md.get("drops") or {}).get("lists", []) if l.get("form_id")]
        for path in (out, os.path.join(DIST, "pts", "farming_spawns", os.path.basename(out))):
            if not os.path.exists(path):
                continue
            doc = json.load(open(path, encoding="utf-8"))
            doc["inventory"] = CI.build_inventory(
                ictx, lists, base_name=creature,
                event_note=lambda e: ("only during " + M.event_of(e, quests))
                if M.event_of(e, quests) else "")
            loot = CI.container_loot(ictx, creature)
            cont = doc.setdefault("drop_rates", {}).get("containers")
            if loot:
                if not isinstance(cont, dict):
                    cont = doc["drop_rates"]["containers"] = {"types": []}
                cont["loot"] = loot
            elif isinstance(cont, dict):
                cont.pop("loot", None)
            cr = (doc.setdefault("drop_rates", {}).get("creatures") or {})
            cr["rows"] = CI.build_creatures(ictx, CI.own_items(ictx, slug, md.get("meat_items")))
            doc["drop_rates"]["creatures"] = cr
            open(path, "w", encoding="utf-8").write(json.dumps(doc, ensure_ascii=False, indent=1))
        inv = doc["inventory"]
        print(f"  meat-{slug:<16} columns:{inv['columns']}  "
              + "  ".join(f"{g['name']}:{len(g['rows'])}" for g in inv["groups"])
              + f"  creatures:{len(cr['rows'])}")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    want = {a.lower() for a in argv if not a.startswith("-")}
    if "--meat" in argv:
        data = rng76.Rng76Data.from_tsv_root(TSV)
        patch_meat_guides(CI.Ctx(data), want)
        return
    paths = []
    for fam in FAMILIES:
        paths += sorted(glob.glob(os.path.join(DIST, fam, "*.json")))
    if want:
        paths = [p for p in paths if os.path.basename(p)[:-5] in want]
    if not paths:
        print("[creature-inventory] no pages")
        return
    data = rng76.Rng76Data.from_tsv_root(TSV)
    ictx = CI.Ctx(data)
    print(f"[creature-inventory] {len(paths)} page(s)")
    for p in paths:
        doc = json.load(open(p, encoding="utf-8"))
        slug = doc.get("slug") or os.path.basename(p)[:-5]
        lists = [{"form_id": l.get("form_id"), "edid": l.get("edid")}
                 for l in (doc.get("drops") or {}).get("lists", []) if l.get("form_id")]
        doc["inventory"] = CI.build_inventory(ictx, lists, base_name=doc.get("name") or "")
        loot = CI.container_loot(ictx, doc.get("name") or slug)
        if loot:
            doc["container_loot"] = loot
        else:
            doc.pop("container_loot", None)
        doc["creatures_rows"] = CI.build_creatures(
            ictx, CI.own_items(ictx, slug, doc.get("meat_items")))
        txt = json.dumps(doc, ensure_ascii=False, indent=1)
        open(p, "w", encoding="utf-8").write(txt)
        inv = doc["inventory"]
        print(f"  {os.path.basename(os.path.dirname(p))}/{slug:<16} columns:{len(inv['columns'])}"
              f"  " + "  ".join(f"{g['name']}:{len(g['rows'])}" for g in inv["groups"])
              + f"  creatures:{len(doc['creatures_rows'])}")


if __name__ == "__main__":
    main()
