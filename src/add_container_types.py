#!/usr/bin/env python3
r"""
add_container_types.py — attach the Containers type+rate list to the meat and
drinks (Nuka-Cola) spawn docs.

The farming family (chems / non-perishable / eggs) gets its Containers type list
inside build_farming_used_for._patch_containers. Meat and drinks run through
their own build pipelines, so this post-step applies the SAME shared model to
their docs: for each doc it seeds the item's LVLI up-closure, finds the lootable
container TYPES that reference it (classify == "container" + a CONT_Export display
name), and computes the item's rng76 appearance rate per container list. Distinct
rates under one name are separate rows; identical dedupe; 0% dropped; sorted desc.
The result is written to doc['drop_rates']['containers'] = {'types': [...]}, the
same shape the farming renderer + the meat/drinks renderers read.

Reuses build_farming_used_for.container_types (one implementation, rng76 rates,
no hardcoded FormIDs). Idempotent. Run after the meat / nuka-cola builds.

  python src/add_container_types.py            # dist  (live)
  python src/add_container_types.py dist/pts tsv/pts
"""
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_farming_used_for as B
import farming_spawns_sources as sources
import rng76
from spawns_configs.nuka_cola import drink_alch


_DROP = {"container", "loot-list"}


def _strip_container_placements(doc):
    from collections import Counter
    for holder in (doc, doc.get("fixed_spawns") if isinstance(doc.get("fixed_spawns"), dict) else None):
        if not isinstance(holder, dict):
            continue
        regs = holder.get("regions")
        if not isinstance(regs, list):
            continue
        for region in regs:
            kept_locs = []
            for loc in region.get("locations", []):
                spawns = loc.get("spawns") or []
                kept = [s for s in spawns if s.get("source_type") not in _DROP]
                if not spawns:
                    kept_locs.append(loc)
                    continue
                if not kept:
                    continue
                loc["spawns"] = kept
                loc["count"] = len(kept)
                loc["sources"] = dict(Counter(s.get("source_type") for s in kept))
                kept_locs.append(loc)
            region["locations"] = kept_locs


def _meat_formids(doc):
    return [i.get("form_id") or i.get("formid") for i in (doc.get("meat_items") or [])
            if (i.get("form_id") or i.get("formid"))]


def _drink_formids_for(path):
    # dist/nuka_cola_spawns_<slug>.json -> the variant's ALCH FormIDs.
    # Goes through nuka_cola.drink_alch because the file slug drops the
    # "nuka-cola-" prefix that DRINK_ALCH's keys carry (see its comment).
    base = os.path.basename(path)
    slug = base[len("nuka_cola_spawns_"):-len(".json")]
    return [f"{int(f):08X}" if str(f).isdigit() else str(f) for f in drink_alch(slug)]


def run(dist_dir="dist", data_dir="tsv"):
    tables = sources.load_tables(data_dir)
    VR = B.VendorRates(rng76.Rng76Data.from_tsv_root(data_dir))
    cont = B._load_cont_names(data_dir)
    lvli_refs, parent_edid = tables["lvli_refs"], tables["parent_edid"]

    def do(path, formids):
        if not formids:
            return False
        doc = json.load(open(path, encoding="utf-8"))
        src = sources.get_sources([{"formid": f, "sig": "ALCH"} for f in formids], tables)
        targets = {f.upper() for f in formids}
        types = B.container_types(src["lvli_closure"], targets,
                                  lambda L, t: VR.appearance([L], t),
                                  cont, lvli_refs, parent_edid)
        dr = doc.get("drop_rates")
        if not isinstance(dr, dict):
            dr = {}
        dr["containers"] = {"types": types}
        doc["drop_rates"] = dr
        # Strip now-redundant container/loot-list placements from the region data
        # (represented by the type list above) so no page carries them. Handles both
        # doc['regions'] (drinks) and doc['fixed_spawns']['regions'] (meat).
        _strip_container_placements(doc)
        json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        return True

    meat_glob = os.path.join(dist_dir, "meat", "*.json")
    n_meat = 0
    for p in sorted(glob.glob(meat_glob)):
        if os.path.basename(p) in ("meat.json",):
            continue
        if do(p, _meat_formids(json.load(open(p, encoding="utf-8")))):
            n_meat += 1

    n_drink = 0
    for p in sorted(glob.glob(os.path.join(dist_dir, "nuka_cola_spawns_*.json"))):
        if p.endswith("manifest.json"):
            continue
        if do(p, _drink_formids_for(p)):
            n_drink += 1

    print(f"[add_container_types] meat docs: {n_meat}  drink docs: {n_drink}  (dist={dist_dir})")


if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else "dist"
    t = sys.argv[2] if len(sys.argv) > 2 else "tsv"
    run(d, t)
