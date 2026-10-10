#!/usr/bin/env python3
r"""
render_pre_war_food_maps.py — maps for the Pre-War Food Location Guide.

Reads dist/farming_spawns/pre-war-food_spawns.json (src/build_pre_war_food_guide.py)
and its geo cache, and draws with the shared render_spawn_maps.py code, so the house
style is the same as every farming map: illustrated Appalachia background, amber
dots, numbered map, watermark on every file, JPEG.

Output, under  <Guides and Stuff>/.Farming - Non Perishable/Pre-War Food/ :

  <Food>/01 Full Maps (4096)/<food-slug>.jpg              one set per food with
  <Food>/02 Numbered Maps (4096)/<food-slug>_numbered.jpg fixed spawns (foods with
  <Food>/03 Region Tiles/<region-slug>-spawn-map.jpg      their own guide are
  <Food>/04 Interior Maps/<cell>_<food-slug>.jpg          skipped — their page has
  <Food>/05 Gallery, 06 Spawn Photos                      its own maps)
  <Food>/<food-slug>_exterior_coords.csv
  05 Chance Maps/pre-war-food_chance.jpg (+ _numbered)    the MIXED pre-war food
  05 Chance Maps/<region-slug>-chance-map.jpg             list (Chance to Spawn)
  06 Gallery, 07 Spawn Photos                             page-level

Site folders (upload the .jpg as is — maps stay JPEG):
  food maps   -> /wp-content/uploads/guide-images/farming-non-perishable/pre-war-food/<food-slug>/
  chance maps -> /wp-content/uploads/guide-images/farming-non-perishable/pre-war-food/
The page only shows a map link once that file is on the server.

  set MAPPALACHIA_DIR=D:\Mappalachia
  python src/render_pre_war_food_maps.py --out "<Guides and Stuff>\.Farming - Non Perishable\Pre-War Food"
  python src/render_pre_war_food_maps.py --out "..." --skip-existing --budget-seconds 140
"""
import argparse, csv, json, os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import render_spawn_maps as R

REPO = os.path.dirname(HERE)
SLUG = "pre-war-food"
DOC = os.path.join(REPO, "dist", "farming_spawns", f"{SLUG}_spawns.json")
GEO = os.path.join(REPO, "data", "farming_spawns", "geo_cache_pre_war_food.json")
FOOD_FOLDERS = ["01 Full Maps (4096)", "02 Numbered Maps (4096)", "03 Region Tiles",
                "04 Interior Maps", "05 Gallery", "06 Spawn Photos"]
PAGE_FOLDERS = ["05 Chance Maps", "06 Gallery", "07 Spawn Photos"]


def safe_folder(name):
    return "".join("-" if c in '<>:"/\\|?*' else c for c in name).strip()


def food_points(fi, geo):
    pts = []
    for reg in fi.get("regions") or []:
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


def render_food(fi, out_root, geo, spaces, boxes):
    key, name = fi["key"], fi["name"]
    root = os.path.join(out_root, safe_folder(name))
    for f in FOOD_FOLDERS:
        os.makedirs(os.path.join(root, f), exist_ok=True)
    pts = food_points(fi, geo)
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="the Pre-War Food item folder")
    ap.add_argument("--skip-existing", action="store_true",
                    help="skip a food that is already drawn")
    ap.add_argument("--budget-seconds", type=float, default=0,
                    help="stop (exit 3) after this long; run again to carry on")
    ap.add_argument("--only", help="one food key, e.g. cram")
    args = ap.parse_args()
    t0 = time.time()

    doc = json.load(open(DOC, encoding="utf-8"))
    geo = R.load_geo(GEO)
    conn = R._db()
    spaces = R.load_spaces(conn)
    boxes = R.region_bboxes(conn)
    for f in PAGE_FOLDERS:
        os.makedirs(os.path.join(args.out, f), exist_ok=True)

    for fi in doc.get("fixed_items") or []:
        if fi.get("page_url"):
            continue                      # has its own guide, and its own maps
        if args.only and fi["key"] != args.only:
            continue
        # the coords CSV is written last for every food, even one whose spawns are
        # all inside interiors (so it has no full map) — use it as the "done" mark
        done = os.path.join(args.out, safe_folder(fi["name"]), f"{fi['key']}_exterior_coords.csv")
        if args.skip_existing and os.path.exists(done):
            continue
        if args.budget_seconds and time.time() - t0 > args.budget_seconds:
            print("time budget reached — run again with --skip-existing to carry on")
            sys.exit(3)
        render_food(fi, args.out, geo, spaces, boxes)

    if not args.only:
        chance = os.path.join(args.out, "05 Chance Maps", f"{SLUG}_chance.jpg")
        if not (args.skip_existing and os.path.exists(chance)):
            if args.budget_seconds and time.time() - t0 > args.budget_seconds:
                print("time budget reached — run again with --skip-existing to carry on")
                sys.exit(3)
            tiles = R.render_chance(SLUG, "farming", args.out, spaces, boxes)
            print(f"  mixed pre-war food chance maps: {len(tiles)} region tiles")
    print("done")


if __name__ == "__main__":
    main()
