#!/usr/bin/env python3
r"""
finish_terminal_maps.py - the step after render_spawn_maps.py for the four
"Level N Terminal Locations" pages (src/build_terminal_locations.py).

Reads what was rendered under
  <GUIDES_ROOT>\.Score Challenges\Terminal Spawn Locations\Level N Terminal Locations\
and stamps the page doc (dist/farming_spawns + the dist/pts twin) with what is there,
so the Maps expand only offers those files. The files are uploaded AS RENDERED
(no copies, no renaming, maps stay JPEG) from the four folders into
  /wp-content/uploads/guide-images/score-challenges/terminal-spawn-locations/Level-N/
        01 Full Maps      level-N-terminals.jpg            -> full_map
        02 Numbered Maps  level-N-terminals_numbered.jpg   (the page derives it from full_map)
        03 Region Tiles   <region-slug>-spawn-map.jpg      -> map_regions
        04 Interior Maps  <region>-<location>-cell-map.jpg -> interior_maps
(The page HEAD-checks each one, so nothing shows until it is uploaded.)

  python src/finish_terminal_maps.py [0 1 2 3]
"""
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_terminal_locations as T   # noqa: E402

GUIDES_ROOT = os.environ.get("GUIDES_ROOT", r"C:\Users\Duche\OneDrive\Guides and Stuff")
ROOT = os.environ.get("TERMINAL_MAPS_ROOT",
                      os.path.join(GUIDES_ROOT, ".Score Challenges", "Terminal Spawn Locations"))


def slugify(s):
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-", str(s or "").lower()).lstrip("-"))


def folder_for(n):
    return os.path.join(ROOT, f"Level {n} Terminal Locations")


def finish(n):
    key = T.page_slug(n)
    folder = folder_for(n)
    docs = [os.path.join(T.OUT_DIR, f"{key}_spawns.json"),
            os.path.join(T.PTS_OUT_DIR, f"{key}_spawns.json")]
    doc = json.load(open(docs[0], encoding="utf-8"))
    base = doc.get("map_base") or ""

    full = os.path.join(folder, "01 Full Maps (4096)", f"{key}.jpg")
    fresh = os.path.getmtime(full) - 120 if os.path.exists(full) else 0
    by_slug = {slugify(r): r for r in T.ALL_REGIONS}

    tiles = []
    d = os.path.join(folder, "03 Region Tiles")
    for fn in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        if fn.endswith("-spawn-map.jpg") and os.path.getmtime(os.path.join(d, fn)) >= fresh:
            tiles.append(fn[:-len("-spawn-map.jpg")])
    doc["map_regions"] = [by_slug[s] for s in tiles if s in by_slug]
    doc["full_map"] = base + f"{key}.jpg" if (doc["map_regions"] and os.path.exists(full)) else ""

    ints = []
    csv_p = os.path.join(folder, "04 Interior Maps", "interior_cells.csv")
    if os.path.exists(csv_p):
        for row in csv.DictReader(open(csv_p, encoding="utf-8")):
            f = row.get("file") or ""
            if f and os.path.exists(os.path.join(folder, "04 Interior Maps", f)):
                ints.append({"name": row["display_name"], "desc": row["region"], "url": base + f})
    ints.sort(key=lambda m: m["name"].lower().removeprefix("the "))
    doc["interior_maps"] = ints

    txt = json.dumps(doc, ensure_ascii=False, indent=1)
    for p in docs:
        open(p, "w", encoding="utf-8").write(txt)
    print(f"  level {n}: region-tiles:{len(doc['map_regions']):>2} interiors:{len(ints):>2} "
          f"full-map:{'yes' if doc['full_map'] else 'no'}  -> {base}")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    for n in [int(a) for a in argv if a.isdigit()] or T.LEVELS:
        finish(n)


if __name__ == "__main__":
    main()
