#!/usr/bin/env python3
r"""
render_all_maps.py — ONE command that renders every farming spawn map.

    python src/render_all_maps.py

That is the whole thing. It stamps `map_base` onto every spawn doc, then renders the
full map, the numbered map, the region tiles and the interior maps for every item
across the seven farming families, straight into that item's folder under
"Guides and Stuff".

Per item you get the layout the Cream and Deathclaw Egg folders already use:

    .Farming - <Category>/<Item>/
        01 Full Maps (4096)/<slug>.jpg
        02 Numbered Maps (4096)/<slug>_numbered.jpg
        03 Region Tiles/<Region>_<slug>.jpg          <- archive copy
                        <region-slug>-spawn-map.jpg  <- UPLOAD THIS ONE
                        <Region>_coords.csv
        04 Interior Maps/<cell>_<slug>.jpg + interior_cells.csv
        05 Chance Maps/...                           <- only where the doc has
                                                        chance refs (farming + nuka)
        <slug>_exterior_coords.csv

`<region-slug>-spawn-map.jpg` is the file the site's new "View {Region} spawn map →"
link points at. Convert it to .avif and drop it in
    /wp-content/uploads/guide-images/<category>/<item>/
— the same folder the item's spawn photos already live in.

Needs the Mappalachia DB and images, so it only runs on your machine, not in CI.
Set MAPPALACHIA_DIR if Mappalachia is not at D:\Mappalachia, and GUIDES_ROOT if
"Guides and Stuff" is not at its usual OneDrive path.

Handy flags:
    --family meat            just one family (repeatable)
    --slug angler            just one item (repeatable)
    --skip-existing          don't redo an item that already has 03 Region Tiles
    --skip-map-base          don't re-stamp map_base first
    --list                   print what WOULD be rendered, render nothing
"""

import argparse, json, os, re, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import render_spawn_maps as R   # noqa: E402
except ModuleNotFoundError as e:
    # Pillow does the drawing and is the one dependency this script has that the
    # rest of the repo does not. Say so plainly instead of dumping a traceback.
    if e.name in ("PIL", "Pillow"):
        sys.exit("Pillow is not installed — that is what draws the maps.\n"
                 "Install it and run this again:\n\n"
                 "    pip install pillow\n")
    raise

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GUIDES_ROOT = os.environ.get(
    "GUIDES_ROOT", r"C:\Users\Duche\OneDrive\Guides and Stuff")

# The DF collectable guides ride in dist/farming_spawns but are not farming pages and
# have their own folders elsewhere, so they are not part of "all the farming maps".
SKIP_SLUGS = {"manifest", "bobbleheads", "magazines", "scouts-banner", "lunchbox"}

EGG_SLUGS = {"deathclaw-egg", "frog-egg", "mirelurk-egg", "mothman-egg",
             "radscorpion-egg", "radtoad-egg"}


def category_folder(source, slug):
    """The .Farming - X folder this item's maps belong in."""
    if source == "meat":
        return ".Farming - Meat"
    if source == "plants":
        return ".Farming - Plants"
    if source == "insects":
        return ".Farming - Insects"
    if source == "nuka":
        return ".Farming - Nuka Cola"
    if slug.startswith("chems-"):
        return ".Farming - Chems"
    if slug in EGG_SLUGS:
        return ".Farming - Eggs"
    return ".Farming - Non Perishable"


def norm(s):
    """Comparison key for folder matching — letters and digits only."""
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


_BAD_FS = re.compile(r'[\\/:*?"<>|]')


def safe_folder(name):
    return _BAD_FS.sub("", str(name or "")).strip().rstrip(".") or "Unnamed"


def item_folder(cat_dir, name, slug):
    """Reuse the item's EXISTING folder when there is one, else make it from the
    display name.

    Matching on a normalised key is what stops a second "Deathclaw Eggs" appearing
    next to the "Deathclaw Egg" folder that already holds the photography, and what
    lets the nuka slug `cherry` find "Nuka Cola Cherry"."""
    want = {norm(name), norm(slug)}
    want.discard("")
    if os.path.isdir(cat_dir):
        for entry in sorted(os.listdir(cat_dir)):
            if os.path.isdir(os.path.join(cat_dir, entry)) and norm(entry) in want:
                return os.path.join(cat_dir, entry)
    return os.path.join(cat_dir, safe_folder(name or slug))


def discover():
    """-> [{slug, source, name, placements}] for every renderable farming item."""
    dist = os.path.join(REPO, "dist")
    jobs = []

    def add(source, path, slug):
        if slug in SKIP_SLUGS:
            return
        try:
            doc = json.load(open(path, encoding="utf-8"))
        except Exception as e:
            print(f"  !! {path}: {e}", file=sys.stderr)
            return
        regions = R.doc_regions(doc)
        placements = sum(len(l.get("spawns") or []) or l.get("count", 0)
                         for r in regions for l in (r.get("locations") or []))
        jobs.append({"slug": slug, "source": source,
                     "name": doc.get("name") or slug, "placements": placements})

    d = os.path.join(dist, "farming_spawns")
    for fn in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        if fn.endswith("_spawns.json"):
            add("farming", os.path.join(d, fn), fn[:-len("_spawns.json")])

    for source, sub in (("meat", "meat"), ("plants", "plants"), ("insects", "insects")):
        d = os.path.join(dist, sub)
        for fn in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            if fn.endswith(".json"):
                add(source, os.path.join(d, fn), fn[:-len(".json")])

    for fn in sorted(os.listdir(dist)):
        if fn.startswith("nuka_cola_spawns_") and fn.endswith(".json"):
            add("nuka", os.path.join(dist, fn),
                fn[len("nuka_cola_spawns_"):-len(".json")])

    return jobs


def stamp_map_base():
    script = os.path.join(REPO, "src", "add_spawn_map_base.py")
    if not os.path.exists(script):
        print("  (no add_spawn_map_base.py — skipping the map_base stamp)")
        return
    subprocess.run([sys.executable, script], cwd=REPO, check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=GUIDES_ROOT,
                    help="the 'Guides and Stuff' folder maps are written into")
    ap.add_argument("--family", action="append", default=[],
                    choices=["farming", "meat", "plants", "insects", "nuka"])
    ap.add_argument("--slug", action="append", default=[])
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--skip-map-base", action="store_true")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    jobs = discover()
    if args.family:
        jobs = [j for j in jobs if j["source"] in args.family]
    if args.slug:
        want = set(args.slug)
        jobs = [j for j in jobs if j["slug"] in want]

    empty = [j for j in jobs if not j["placements"]]
    jobs = [j for j in jobs if j["placements"]]

    for j in jobs:
        cat = category_folder(j["source"], j["slug"])
        j["out"] = item_folder(os.path.join(args.root, cat), j["name"], j["slug"])

    if args.skip_existing:
        before = len(jobs)
        jobs = [j for j in jobs
                if not os.path.isdir(os.path.join(j["out"], "03 Region Tiles"))]
        print(f"skip-existing: {before - len(jobs)} already done")

    if args.list:
        for j in jobs:
            print(f"  {j['source']:8} {j['slug']:34} {j['placements']:6}  -> {j['out']}")
        print(f"\n{len(jobs)} to render, {len(empty)} skipped (no mappable spawns)")
        return 0

    if not args.skip_map_base:
        print("stamping map_base on every spawn doc ...")
        stamp_map_base()

    if not os.path.exists(R.MAPPALACHIA_DB):
        print(f"\nNo Mappalachia DB at {R.MAPPALACHIA_DB}.\n"
              f"Set MAPPALACHIA_DIR to wherever Mappalachia lives and run again.",
              file=sys.stderr)
        return 2

    print(f"\nrendering {len(jobs)} items into {args.root}")
    print(f"({len(empty)} skipped — no mappable spawns)\n")

    conn = R._db()
    spaces = R.load_spaces(conn)
    boxes = R.region_bboxes(conn)

    t0 = time.time()
    done = failed = 0
    for i, j in enumerate(jobs, 1):
        print(f"[{i}/{len(jobs)}] {j['source']}/{j['slug']}")
        try:
            os.makedirs(j["out"], exist_ok=True)
            R.render_set(j["slug"], j["source"], j["out"], conn, spaces, boxes)
            done += 1
        except Exception as e:
            failed += 1
            print(f"  !! {j['slug']}: {e}", file=sys.stderr)

    mins = (time.time() - t0) / 60
    print(f"\ndone: {done} rendered, {failed} failed, {len(empty)} skipped "
          f"— {mins:.1f} min")
    if empty:
        print("no mappable spawns: " + ", ".join(sorted(j["slug"] for j in empty)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
