#!/usr/bin/env python3
r"""
render_pre_war_food_maps.py — maps for the Pre-War Food pages (one page per food).

Reads dist/farming_spawns/pre_war_food_pages.json and each page's
<slug>_spawns.json (src/build_pre_war_food_guide.py) and the shared geo cache,
and draws with the shared render_spawn_maps.py code, so the house style is the
same as every farming map: illustrated Appalachia background, amber dots,
numbered map, watermark on every file, JPEG.

Output, under  <Guides and Stuff>/.Farming - Non Perishable/ :

  <Food>/01 Full Maps (4096)/<food-slug>.jpg               one folder per page
  <Food>/02 Numbered Maps (4096)/<food-slug>_numbered.jpg  (same layout as every
  <Food>/03 Region Tiles/<region-slug>-spawn-map.jpg       farming item: 01-04
  <Food>/04 Interior Maps/<cell>_<food-slug>.jpg           maps, 05 Gallery,
  <Food>/05 Gallery, 06 Spawn Photos                       06 Spawn Photos)
  <Food>/<food-slug>_exterior_coords.csv
  <Food>/<Food> (no rads)/01-04 + coords csv               the rad-free copy,
                                                           same page
  Pre-War Food/05 Chance Maps/pre-war-food_chance.jpg      the MIXED pre-war
  Pre-War Food/05 Chance Maps/<region-slug>-chance-map.jpg food list — ONE set,
                                                           shared by every page

Site folders (upload the .jpg as is — maps stay JPEG):
  <Food> maps           -> /wp-content/uploads/guide-images/farming-non-perishable/<food-slug>/
  <Food> (no rads) maps -> /wp-content/uploads/guide-images/farming-non-perishable/<food-slug>/no-rads/
  chance maps           -> /wp-content/uploads/guide-images/farming-non-perishable/pre-war-food/
The page only shows a map link once that file is on the server.

  set MAPPALACHIA_DIR=D:\Mappalachia
  python src/render_pre_war_food_maps.py --out "<Guides and Stuff>\.Farming - Non Perishable"
  python src/render_pre_war_food_maps.py --out "..." --skip-existing --budget-seconds 140
  python src/render_pre_war_food_maps.py --out "..." --folders-only   # just make folders
"""
import argparse, csv, json, os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

REPO = os.path.dirname(HERE)
SLUG = "pre-war-food"
DIST = os.path.join(REPO, "dist", "farming_spawns")
MANIFEST = os.path.join(DIST, "pre_war_food_pages.json")
GEO = os.path.join(REPO, "data", "farming_spawns", "geo_cache_pre_war_food.json")
MAP_FOLDERS = ["01 Full Maps (4096)", "02 Numbered Maps (4096)", "03 Region Tiles",
               "04 Interior Maps"]
PAGE_FOLDERS = MAP_FOLDERS + ["05 Gallery", "06 Spawn Photos"]
CHANCE_FOLDER = os.path.join("Pre-War Food", "05 Chance Maps")
NO_RADS = " (no rads)"


def safe_folder(name):
    return "".join("-" if c in '<>:"/\\|?*' else c for c in name).strip()


def food_jobs(out_root):
    """-> [(page name, food name, food key, folder, regions)] for every food."""
    pages = json.load(open(MANIFEST, encoding="utf-8"))["pages"]
    jobs = []
    for pg in pages:
        doc = json.load(open(os.path.join(DIST, f"{pg['slug']}_spawns.json"), encoding="utf-8"))
        page_dir = os.path.join(out_root, safe_folder(pg["name"]))
        regions_by_key = {}
        if doc.get("fixed_items") is not None:
            for fi in doc["fixed_items"]:
                regions_by_key[fi["key"]] = fi.get("regions") or []
        else:
            regions_by_key[doc.get("food_key")] = doc.get("regions") or []
        for f in pg["foods"]:
            folder = page_dir
            if len(pg["foods"]) > 1 and f["name"].endswith(NO_RADS):
                folder = os.path.join(page_dir, safe_folder(f["name"]))
            jobs.append((pg["name"], f["name"], f["key"], folder,
                         regions_by_key.get(f["key"], []), page_dir))
    return jobs


def food_points(regions, geo, R):
    pts = []
    for reg in regions:
        for loc in reg.get("locations") or []:
            for sp in loc.get("spawns") or []:
                ref = sp.get("ref") or ""
                g = geo.get(str(int(ref, 16))) if ref else None
                if g:
                    space, x, y = int(g["space"]), float(g["x"]), float(g["y"])
                else:
                    c = sp.get("coords") or loc.get("coords")
                    if not c:
                        continue
                    space, x, y = R.APPALACHIA_SPACE, float(c[0]), float(c[1])
                pts.append({"ref": ref, "space": space, "x": x, "y": y,
                            "region": reg.get("region", ""), "marker": loc.get("marker", ""),
                            "label": sp.get("label", ""),
                            # one legend row per food, labelled "Fixed spawn"
                            "source_type": "static"})
    return pts


def render_food(name, key, root, regions, geo, spaces, boxes, R):
    pts = food_points(regions, geo, R)
    ext = [p for p in pts if p["space"] == R.APPALACHIA_SPACE]
    to_px = R.projector(spaces[R.APPALACHIA_SPACE])
    rows = R.cluster_by_marker(ext, to_px)
    bg = os.path.join(R.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    tiles = []
    if rows:
        _plain, numbered = R.render_exterior(
            bg, rows, name,
            os.path.join(root, "01 Full Maps (4096)", f"{key}.jpg"),
            os.path.join(root, "02 Numbered Maps (4096)", f"{key}_numbered.jpg"))
        tiles = R.render_region_tiles(numbered, rows, boxes, to_px,
                                      os.path.join(root, "03 Region Tiles"), key,
                                      name_fn=lambda r: f"{R._region_slug(r)}-spawn-map.jpg")
    ints = R.render_interiors(pts, spaces, os.path.join(root, "04 Interior Maps"), key, name)
    with open(os.path.join(root, f"{key}_exterior_coords.csv"), "w",
              newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["n", "region", "marker", "source_type", "count", "game_x", "game_y", "refs"])
        for r in rows:
            w.writerow([r["n"], r["region"], r["marker"], r.get("source_type", ""),
                        r["count"], round(r["x"], 1), round(r["y"], 1), " ".join(r["refs"])])
    print(f"  {name:<36} ext={len(ext):4} markers={len(rows):3} tiles={len(tiles):2} "
          f"interiors={len(ints):3}")


def render_shared_chance(out_dir, geo, spaces, boxes, R):
    """The mixed-list chance maps, once. Every page's chance_spawns.regions are the
    same spots (one mixed list as of Oct 2026); take the page with the most."""
    pages = json.load(open(MANIFEST, encoding="utf-8"))["pages"]
    best = None
    for pg in pages:
        cs = json.load(open(os.path.join(DIST, f"{pg['slug']}_spawns.json"),
                            encoding="utf-8")).get("chance_spawns") or {}
        if not best or (cs.get("total") or 0) > (best.get("total") or 0):
            best = cs
    pts = []
    for reg in (best or {}).get("regions", []):
        for m in reg.get("markers", []):
            for ref in (m.get("refs") or []) if isinstance(m, dict) else []:
                g = geo.get(str(int(ref, 16)))
                if g:
                    pts.append({"ref": ref, "space": int(g["space"]), "x": float(g["x"]),
                                "y": float(g["y"]), "region": reg.get("region", ""),
                                "marker": m.get("name", ""), "label": "",
                                "source_type": "chance"})
    ext = [p for p in pts if p["space"] == R.APPALACHIA_SPACE]
    if not ext:
        return []
    os.makedirs(out_dir, exist_ok=True)
    to_px = R.projector(spaces[R.APPALACHIA_SPACE])
    rows = R.cluster_by_marker(ext, to_px)
    bg = os.path.join(R.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    _plain, numbered = R.render_exterior(
        bg, rows, "Pre-War Food — chance spawns",
        os.path.join(out_dir, f"{SLUG}_chance.jpg"),
        os.path.join(out_dir, f"{SLUG}_chance_numbered.jpg"))
    return R.render_region_tiles(numbered, rows, boxes, to_px, out_dir, SLUG,
                                 name_fn=lambda r: f"{R._region_slug(r)}-chance-map.jpg")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help='the ".Farming - Non Perishable" folder')
    ap.add_argument("--skip-existing", action="store_true",
                    help="skip a food that is already drawn")
    ap.add_argument("--budget-seconds", type=float, default=0,
                    help="stop (exit 3) after this long; run again to carry on")
    ap.add_argument("--only", help="one food key, e.g. cram")
    ap.add_argument("--folders-only", action="store_true",
                    help="make every page's folders, draw nothing")
    args = ap.parse_args()
    t0 = time.time()

    jobs = food_jobs(args.out)
    for _pn, _fn, _k, folder, _r, page_dir in jobs:
        for f in PAGE_FOLDERS:
            os.makedirs(os.path.join(page_dir, f), exist_ok=True)
        for f in MAP_FOLDERS:
            os.makedirs(os.path.join(folder, f), exist_ok=True)
    if args.folders_only:
        print(f"folders ready for {len(jobs)} foods")
        return

    import render_spawn_maps as R
    geo = R.load_geo(GEO)
    conn = R._db()
    spaces = R.load_spaces(conn)
    boxes = R.region_bboxes(conn)

    for _pn, name, key, folder, regions, _pd in jobs:
        if args.only and key != args.only:
            continue
        if not regions or not any(l.get("spawns") for r in regions for l in r.get("locations") or []):
            continue                      # no fixed spawns — nothing to draw
        # the coords CSV is written last for every food, even one whose spawns are
        # all inside interiors (so it has no full map) — use it as the "done" mark
        done = os.path.join(folder, f"{key}_exterior_coords.csv")
        if args.skip_existing and os.path.exists(done):
            continue
        if args.budget_seconds and time.time() - t0 > args.budget_seconds:
            print("time budget reached — run again with --skip-existing to carry on")
            sys.exit(3)
        render_food(name, key, folder, regions, geo, spaces, boxes, R)

    if not args.only:
        out_dir = os.path.join(args.out, CHANCE_FOLDER)
        chance = os.path.join(out_dir, f"{SLUG}_chance.jpg")
        if not (args.skip_existing and os.path.exists(chance)):
            if args.budget_seconds and time.time() - t0 > args.budget_seconds:
                print("time budget reached — run again with --skip-existing to carry on")
                sys.exit(3)
            tiles = render_shared_chance(out_dir, geo, spaces, boxes, R)
            print(f"  shared pre-war food chance maps: {len(tiles)} region tiles")
    print("done")


if __name__ == "__main__":
    main()
