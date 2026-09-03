#!/usr/bin/env python3
r"""
add_treasure_maps.py — attach the Treasure Maps expand to the drinks (Nuka-Cola),
meat and plants spawn docs.

The farming family (chems / non-perishable / eggs) gets its Treasure Maps list
inside build_farming_used_for._patch_treasure_maps. Drinks, meat and plants run
through their own build pipelines, so this post-step applies the SAME shared model
to their docs — exactly the split that made add_container_types.py necessary for
the Containers expand.

For each doc it seeds the item's FormIDs, asks treasure_map_sources.build_maps for
every treasure map whose dig can pay the item out, and writes

    doc['treasure_maps'] = {'maps': [ ... ]}

the same shape the farming renderer reads. Rates are rng76-resolved (spawn-guide
§9l, drop-rate-engine §3a/3c) — nothing typed, no hardcoded FormIDs. A map that
resolves to 0 is dropped, so an item with no map source gets an empty list and the
renderer shows its honest empty state. Idempotent — safe to re-run.

SEEDS (per family — the only per-family knowledge in this file):
  drinks  dist/nuka_cola_spawns_<slug>.json -> spawns_configs.nuka_cola.drink_alch(slug)
  meat    doc['meat_items'][].form_id
  plants  doc['flora'][].form_id are FLOR *plant* records, not the item you pick
          up, so they are resolved through the FLOR produce column
          (build_farming_used_for.HarvestProduce) to the harvested ALCH FormIDs.
          Treasure-map pools hold items, never flora, so seeding the FLOR ids
          directly would silently match nothing.

RUN ORDER: after build_treasure_maps_json.py (this reads dist/treasure_maps.json)
and after the meat / nuka-cola / plants builds (it rewrites their docs).

  python src/add_treasure_maps.py              # dist  (live)
  python src/add_treasure_maps.py dist/pts tsv/pts
"""
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_farming_used_for as B
import farming_spawns_sources as sources  # noqa: F401  (kept for parity / future seeds)
import rng76
import treasure_map_sources as TMS
from spawns_configs.nuka_cola import drink_alch


def _hex(f):
    """DRINK_ALCH stores decimals; every other seed is already hex."""
    s = str(f).strip()
    return f"{int(s):08X}" if s.isdigit() else s.upper()


def _meat_formids(doc):
    return [_hex(i.get("form_id") or i.get("formid"))
            for i in (doc.get("meat_items") or [])
            if (i.get("form_id") or i.get("formid"))]


def _drink_formids_for(path):
    # dist/nuka_cola_spawns_<slug>.json -> the variant's ALCH FormIDs. Goes through
    # nuka_cola.drink_alch because the file slug drops the "nuka-cola-" prefix that
    # DRINK_ALCH's keys carry (see its comment) — indexing the dict directly is what
    # left 8 flavour pages with no item FormID before Sept 2026.
    base = os.path.basename(path)
    slug = base[len("nuka_cola_spawns_"):-len(".json")]
    return [_hex(f) for f in drink_alch(slug)]


def _plant_formids(doc, harvest):
    """FLOR plant record -> the ALCH item it produces on harvest."""
    out = set()
    for fl in (doc.get("flora") or []):
        fid = (fl.get("form_id") or fl.get("formid") or "").upper()
        if fid:
            out |= harvest.flor.get(fid, set())
            out |= harvest.acti.get(fid, set())
    return sorted(out)


def run(dist_dir="dist", data_dir="tsv"):
    data = rng76.Rng76Data.from_tsv_root(data_dir)
    VR = B.VendorRates(data)
    harvest = B.HarvestProduce(data_dir, lvli=data.lvli)

    def do(path, formids):
        """Always writes the key — an item with no map source needs the empty
        list so the renderer shows its honest empty state rather than nothing.
        Returns True only when the item actually has map rows."""
        doc = json.load(open(path, encoding="utf-8"))
        maps = []
        if formids:
            targets = {f.upper() for f in formids}
            maps = TMS.build_maps(dist_dir, targets,
                                  lambda L, t: VR.appearance([L], t),
                                  data.lvli, B._fmt_rate)
        doc["treasure_maps"] = {"maps": maps}
        json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        return bool(maps)

    counts = {"meat": [0, 0], "drink": [0, 0], "plant": [0, 0]}

    for p in sorted(glob.glob(os.path.join(dist_dir, "meat", "*.json"))):
        if os.path.basename(p) == "meat.json":       # the hub, not an item doc
            continue
        counts["meat"][1] += 1
        if do(p, _meat_formids(json.load(open(p, encoding="utf-8")))):
            counts["meat"][0] += 1

    for p in sorted(glob.glob(os.path.join(dist_dir, "nuka_cola_spawns_*.json"))):
        if p.endswith("manifest.json"):
            continue
        counts["drink"][1] += 1
        if do(p, _drink_formids_for(p)):
            counts["drink"][0] += 1

    for p in sorted(glob.glob(os.path.join(dist_dir, "plants", "*.json"))):
        if os.path.basename(p) in ("plants.json",):  # the hub, not an item doc
            continue
        counts["plant"][1] += 1
        if do(p, _plant_formids(json.load(open(p, encoding="utf-8")), harvest)):
            counts["plant"][0] += 1

    print("[add_treasure_maps] "
          + "  ".join(f"{k}: {v[0]}/{v[1]} with maps" for k, v in counts.items())
          + f"  (dist={dist_dir})")


if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else "dist"
    t = sys.argv[2] if len(sys.argv) > 2 else "tsv"
    run(d, t)
