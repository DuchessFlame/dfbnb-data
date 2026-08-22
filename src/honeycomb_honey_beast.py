#!/usr/bin/env python3
r"""
honeycomb_honey_beast.py — fold the Honey Beast CREATURE onto the Honeycomb
non-perishable farming spawn page.

Honey Beast is not its own cryptid page; its home is the Honeycomb item page
(build_honeycomb_spawns_json.py). This post-processor runs AFTER the farming build
rewrites dist/farming_spawns/honeycomb_spawns.json and attaches a `honey_beast`
object that the renderer surfaces as:
  * a second "Used For - Honey Beasts" sub-expand,
  * a "Honey Beasts" middle expand (creature intro + Drops + Events + REs),
  * Honey Beast creature spawns folded into Fixed Spawn Locations (labelled).

All Honey Beast content is derived by spawns_configs.cryptids.compute_bundle
(RACE->NPC->death lists + Mappalachia placements + rng76 rates), using the SAME
tested logic the cryptid pages use — no parallel pipeline. Fixed-spawn placements
resolve from a dedicated committed geo cache so CI (no DB) reproduces them.

The Honey Beast drops Honeycomb at 35%, and during the Big Bloom public event it
spawns as Beezlebaby / Beezlebub (SSE_LLD_Creature_HoneyBeast) — so the two
subjects genuinely belong on the same page.

Usage: run automatically by build_honeycomb_spawns_json.py, or:
    python src/honeycomb_honey_beast.py [--pts]
"""

import os, sys, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import cryptids as C

# Honey Beast page config (creature seed). event_tokens catch the Big Bloom event
# quest ("Event: The Big Bloom" / Beezlebaby / Beezlebub) which the cryptid name
# alone doesn't match.
HONEY_BEAST = {
    "slug": "honey-beast",
    "name": "Honey Beast",
    "races": ["HoneyBeastRace"],
    "tokens": ["honeybeast"],
    "event_tokens": ["beezle", "big bloom", "bigbloom"],
    "mappalachia": ["Honey Beast"],
    "used_for_url": "",
}

GEO_CACHE = os.path.join(REPO, "data", "honeycomb_honey_beast", "geo_cache.json")

INTRO = ("Honey Beasts are giant mutated bees found across Appalachia. They drop "
         "Honeycomb (35% chance), which is why they share this page. During the "
         "Big Bloom public event they appear as Beezlebaby and Beezlebub.")


def _doc_path(dist_dir):
    return os.path.join(dist_dir, "farming_spawns", "honeycomb_spawns.json")


def _keep_from_doc(path):
    """Preserve hand-authored Honey Beast photography across rebuilds — read the
    existing doc's honey_beast.regions into the {(region,marker):{...}} keep shape
    (same structure ebuild.load_existing produces, but from the nested block)."""
    keep = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    hb = (old.get("honey_beast") or {}).get("fixed_spawns") or {}
    for reg in hb.get("regions", []):
        for loc in reg.get("locations", []):
            spawns = {}
            for sp in loc.get("spawns") or []:
                ref = sp.get("ref") or ""
                saved = {k: sp.get(k, "") for k in ("image_top", "directions", "image_bottom")
                         if sp.get(k)}
                if ref and saved:
                    spawns[ref] = saved
            keep[(reg.get("region", ""), loc.get("marker", ""))] = {
                "image_top": loc.get("image_top", ""),
                "directions": loc.get("directions", ""),
                "image_bottom": loc.get("image_bottom", ""),
                "spawns": spawns,
            }
    return keep


def inject(dist_dir=None):
    """Attach the Honey Beast bundle to honeycomb_spawns.json under dist_dir
    (default: repo dist/). Idempotent — re-run any time after the farming build."""
    dist_dir = dist_dir or os.path.join(REPO, "dist")
    doc_path = _doc_path(dist_dir)
    if not os.path.exists(doc_path):
        print(f"[honeycomb+honeybeast] {doc_path} not found — run the farming build first.")
        return
    keep = _keep_from_doc(doc_path)
    bundle = C.compute_bundle(HONEY_BEAST, GEO_CACHE, keep=keep)
    bundle["intro"] = INTRO

    doc = json.load(open(doc_path, encoding="utf-8"))
    doc["honey_beast"] = bundle
    json.dump(doc, open(doc_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    fs = bundle["fixed_spawns"]
    print(f"[honeycomb+honeybeast] injected into {os.path.relpath(doc_path, REPO)} — "
          f"used_for:{len(bundle['used_for'])} events:{len(bundle['events_activities'])} "
          f"REs:{len(bundle['random_encounters'])} "
          f"drop-lists:{len(bundle['drops']['lists'])} "
          f"fixed:{fs['total_placements']}/{fs['total_markers']}m")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    inject(os.path.join(REPO, "dist"))
    if "--pts" in argv:
        inject(os.path.join(REPO, "dist", "pts"))


if __name__ == "__main__":
    main()
