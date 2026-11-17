#!/usr/bin/env python3
r"""
finish_terminal_maps.py - the step after render_spawn_maps.py for the four
"Level N Terminal Locations" pages (src/build_terminal_locations.py).

For each level it reads what was rendered under
  <GUIDES_ROOT>\.Score Challenges\Level N Terminal Locations\
and:
  1. copies the upload set into  ...\For Upload\  named exactly as the page expects
     under map_base (/wp-content/uploads/guide-images/score-challenges/level-N-terminals/).
     Maps stay JPEG on the site (guide-maps-expand), so nothing is converted:
        level-N-terminals-full-map.jpg      01 Full Maps      (Maps card + 4K download)
        level-N-terminals_numbered.jpg      02 Numbered Maps
        <region-slug>-spawn-map.jpg         03 Region Tiles
        <region>-<location>-cell-map.jpg    04 Interior Maps
  2. stamps the doc (dist/farming_spawns + the dist/pts twin) with full_map,
     map_regions and interior_maps, so the Maps expand only offers those files.
     (The page still HEAD-checks each one, so nothing shows until it is uploaded.)

  python src/finish_terminal_maps.py [0 1 2 3]
"""
import csv
import json
import os
import re
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_terminal_locations as T   # noqa: E402

GUIDES_ROOT = os.environ.get("GUIDES_ROOT", r"C:\Users\Duche\OneDrive\Guides and Stuff")
ROOT = os.environ.get("SCORE_ROOT", os.path.join(GUIDES_ROOT, ".Score Challenges"))


def slugify(s):
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-", str(s or "").lower()).lstrip("-"))


def copy(src, dst):
    if os.path.exists(dst) and os.path.getmtime(dst) >= os.path.getmtime(src):
        return 0
    shutil.copyfile(src, dst)
    return 1


def finish(n):
    key = T.page_slug(n)
    folder = os.path.join(ROOT, f"Level {n} Terminal Locations")
    up = os.path.join(folder, "For Upload")
    os.makedirs(up, exist_ok=True)
    docs = [os.path.join(T.OUT_DIR, f"{key}_spawns.json"),
            os.path.join(T.PTS_OUT_DIR, f"{key}_spawns.json")]
    doc = json.load(open(docs[0], encoding="utf-8"))
    base = doc.get("map_base") or ""
    made = 0

    full = os.path.join(folder, "01 Full Maps (4096)", f"{key}.jpg")
    num = os.path.join(folder, "02 Numbered Maps (4096)", f"{key}_numbered.jpg")
    fresh = os.path.getmtime(full) - 120 if os.path.exists(full) else 0
    by_slug = {slugify(r): r for r in T.ALL_REGIONS}

    tiles = []
    d = os.path.join(folder, "03 Region Tiles")
    for fn in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        p = os.path.join(d, fn)
        if fn.endswith("-spawn-map.jpg") and os.path.getmtime(p) >= fresh:
            made += copy(p, os.path.join(up, fn))
            tiles.append(fn[:-len("-spawn-map.jpg")])
    doc["map_regions"] = [by_slug[s] for s in tiles if s in by_slug]

    if doc["map_regions"] and os.path.exists(full):
        made += copy(full, os.path.join(up, f"{key}-full-map.jpg"))
        if os.path.exists(num):
            made += copy(num, os.path.join(up, f"{key}_numbered.jpg"))
        doc["full_map"] = base + f"{key}-full-map.jpg"
    else:
        doc["full_map"] = ""

    ints = []
    csv_p = os.path.join(folder, "04 Interior Maps", "interior_cells.csv")
    if os.path.exists(csv_p):
        for row in csv.DictReader(open(csv_p, encoding="utf-8")):
            src = os.path.join(folder, "04 Interior Maps", row.get("file") or "")
            if not row.get("file") or not os.path.exists(src):
                continue
            made += copy(src, os.path.join(up, row["file"]))
            ints.append({"name": row["display_name"], "desc": row["region"],
                         "url": base + row["file"]})
    ints.sort(key=lambda m: m["name"].lower().removeprefix("the "))
    doc["interior_maps"] = ints

    txt = json.dumps(doc, ensure_ascii=False, indent=1)
    for p in docs:
        open(p, "w", encoding="utf-8").write(txt)
    print(f"  level {n}: region-tiles:{len(doc['map_regions']):>2} interiors:{len(ints):>2} "
          f"full-map:{'yes' if doc['full_map'] else 'no'} new-files:{made}")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    for n in [int(a) for a in argv if a.isdigit()] or T.LEVELS:
        finish(n)


if __name__ == "__main__":
    main()
