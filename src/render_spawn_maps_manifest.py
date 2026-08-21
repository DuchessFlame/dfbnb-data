#!/usr/bin/env python3
r"""
render_spawn_maps_manifest.py - build the job manifest for render_spawn_maps.py and run it.

Maps every spawn set to its category folder under "Guides and Stuff":
  .Farming - Eggs\<Item>\
  .Farming - Non Perishable\<Item>\
  .Farming - Non Perishable\Nuka Cola\<Variant>\

Existing folder names are reused where they already exist (e.g. "Deathclaw Eggs"),
otherwise the item's display name from the spawns JSON is used.

Usage:
  python render_spawn_maps_manifest.py --root "C:\...\Guides and Stuff" [--only eggs|np|nuka] [--dry-run]
"""

import argparse, json, os, sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

EGGS = ["deathclaw-egg", "frog-egg", "mirelurk-egg", "mothman-egg",
        "radscorpion-egg", "radtoad-egg"]

NON_PERISHABLE = ["blood-sac", "canned-meat-stew", "cream", "glowing-blood", "honey",
                  "honeycomb", "hotdog", "perfect-bubblegum", "purified-water",
                  "royal-jelly", "salt-pepper-spices-sugar", "sugar-bombs", "tick-blood"]

NUKA = ["nuka-cola", "nuka-cola-cherry", "nuka-cola-cranberry", "nuka-cola-dark",
        "nuka-cola-grape", "nuka-cola-orange", "nuka-cola-quantum", "nuka-cola-twist",
        "nuka-cola-vaccinated", "nuka-cola-wild", "nukashine", "sunset-sarsaparilla"]

EGGS_DIR = ".Farming - Eggs"
NP_DIR = ".Farming - Non Perishable"
NUKA_DIR = os.path.join(NP_DIR, "Nuka Cola")


def display_name(slug, source):
    p = (os.path.join(REPO, "dist", "farming_spawns", f"{slug}_spawns.json")
         if source == "farming" else
         os.path.join(REPO, "dist", f"nuka_cola_spawns_{slug}.json"))
    try:
        return json.load(open(p, encoding="utf-8")).get("name") or slug
    except Exception:
        return slug


def pick_folder(parent, name):
    """Reuse an existing folder that matches loosely (e.g. 'Deathclaw Eggs' vs 'Deathclaw Egg')."""
    if not os.path.isdir(parent):
        return os.path.join(parent, name)
    low = {d.lower(): d for d in os.listdir(parent) if os.path.isdir(os.path.join(parent, d))}
    for cand in (name, name + "s", name.rstrip("s")):
        if cand.lower() in low:
            return os.path.join(parent, low[cand.lower()])
    return os.path.join(parent, name)


def build(root, only=None):
    jobs = []
    groups = [("eggs", EGGS, "farming", os.path.join(root, EGGS_DIR)),
              ("np", NON_PERISHABLE, "farming", os.path.join(root, NP_DIR)),
              ("nuka", NUKA, "nuka", os.path.join(root, NUKA_DIR))]
    for key, slugs, source, parent in groups:
        if only and key != only:
            continue
        for slug in slugs:
            name = display_name(slug, source)
            jobs.append({"slug": slug, "source": source,
                         "out": pick_folder(parent, name), "name": name})
    return jobs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--only", choices=["eggs", "np", "nuka"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--manifest-out")
    ap.add_argument("--skip-existing", action="store_true",
                    help="skip sets that already have a .rendered stamp")
    ap.add_argument("--budget-seconds", type=float, default=0,
                    help="stop cleanly once this much wall time has passed (0 = no limit)")
    args = ap.parse_args()

    jobs = build(args.root, args.only)
    if args.manifest_out:
        json.dump(jobs, open(args.manifest_out, "w", encoding="utf-8"), indent=1)
    if args.dry_run:
        for j in jobs:
            print(f"{j['slug']:28} -> {j['out']}")
        return

    import time
    import render_spawn_maps as R
    conn = R._db()
    spaces = R.load_spaces(conn)
    boxes = R.region_bboxes(conn)
    t0 = time.time()

    for j in jobs:
        stamp = os.path.join(j["out"], ".rendered")
        if args.skip_existing and os.path.exists(stamp):
            continue
        if args.budget_seconds and (time.time() - t0) > args.budget_seconds:
            print("BUDGET REACHED - more sets remaining")
            return
        try:
            res = R.render_set(j["slug"], j["source"], j["out"], conn, spaces, boxes)
            with open(stamp, "w", encoding="utf-8") as fh:
                json.dump(res, fh)
        except Exception as e:
            print(f"  !! {j['slug']}: {e}", file=sys.stderr)
    print("ALL DONE")


if __name__ == "__main__":
    main()
