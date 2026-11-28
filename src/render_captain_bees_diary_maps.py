#!/usr/bin/env python3
r"""
render_captain_bees_diary_maps.py - maps for the Captain Bee's Diary page
(/df/quests/secrets-and-easter-eggs/captain-bees-diary/). 28 Nov 2026.

Duchess (28 Nov 2026): people prefer a map with the places marked over written
directions, so the page offers both. House style is the brain-in-a-jar /
render_spawn_maps.py one (imported, not copied): amber dots, numbers clear of
the dots, the game's map-marker icons, watermark.

Points come straight from the newest BOOK2 export (where each diary page sits):
  1-6  Storm_SteelJackNote_01..06 (Bee's Diary pages, inside their containers)
  7    Storm_Bees_finalNote       (Bee's Final Note, on the ground by the skeleton)

Writes into the page's PC photo folder (mirrors the server folder):
  captain-bees-diary-map.jpg            full 4096 map, dots only
  captain-bees-diary-map_numbered.jpg   full 4096 map, numbered 1-7
  captain-bees-diary-skyline-valley-map.jpg  Skyline Valley crop, numbered

Usage: python src/render_captain_bees_diary_maps.py [--out DIR]
Env:   MAPPALACHIA_DIR (default D:\Mappalachia)
"""
import argparse, csv, glob, os, re, sys
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import render_spawn_maps as rsm
import render_brain_in_a_jar_maps as brain   # draw_crop / bbox / _MARKS (same look)
import render_event_swap_maps as esw

DEFAULT_OUT = os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff",
                           ".Quests", "Captain Bee's Diary", "guide-images")
STEM = "captain-bees-diary"
POINTS = {**{f"Storm_SteelJackNote_0{i}": i for i in range(1, 7)}, "Storm_Bees_finalNote": 7}


def newest_book2():
    def key(p):
        m = re.search(r"BOOK2_Export_([A-Za-z]+)_(\d{4})", p)
        months = "jan feb mar apr may jun jul aug sep oct nov dec".split()
        return (int(m.group(2)), months.index(m.group(1)[:3].lower())) if m else (0, 0)
    files = glob.glob(os.path.join(REPO, "tsv", "BOOK2_Export_*_*.tsv"))
    return max(files, key=key)


def load_points():
    pts = {}
    with open(newest_book2(), encoding="utf-8") as fh:
        for row in csv.reader(fh, delimiter="\t"):
            # notes  FormID  EDID  FULL  BOOK  WRLD  Cell  X  Y  Z  ContainerRef  ContainerEDID ...
            if len(row) > 9 and row[2] in POINTS and row[5] == "APPALACHIA":
                pts[POINTS[row[2]]] = {"n": POINTS[row[2]], "name": row[3], "ref": row[10] or row[1],
                                       "x": float(row[7]), "y": float(row[8])}
    missing = sorted(set(POINTS.values()) - set(pts))
    if missing:
        raise SystemExit(f"Not in {newest_book2()}: points {missing}")
    return [pts[n] for n in sorted(pts)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    pts = load_points()
    conn = rsm._db()
    to_px = rsm.projector(rsm.load_spaces(conn)[rsm.APPALACHIA_SPACE])
    for p in pts:
        p["px"], p["py"] = to_px(p["x"], p["y"])

    plain_bg = os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    for mx, my, lbl, im in esw.channel_marker_icons("live", size=brain.TILE_ICON_PX * 2):
        px, py = to_px(mx, my)
        brain._MARKS.append((px, py, lbl, im))
    bg_path = esw._icon_background(plain_bg, to_px, marks=esw.channel_marker_icons("live"))
    bg = Image.open(plain_bg).convert("RGB")
    if bg.size != (rsm.S, rsm.S):
        bg = bg.resize((rsm.S, rsm.S), Image.LANCZOS)

    # Legend wording on the 4096 maps (default would read "Spawn").
    rsm._PAGE["slug"] = STEM
    rsm.PAGE_TYPE_LABELS[STEM] = {"": "Diary pages + final note"}
    rows = [dict(p, count=1, refs=[p["ref"]], source_type="") for p in pts]
    rsm.render_exterior(bg_path, rows, "Captain Bee's Diary",
                        os.path.join(a.out, f"{STEM}-map.jpg"),
                        os.path.join(a.out, f"{STEM}-map_numbered.jpg"))

    img = brain.draw_crop(bg, pts, brain.bbox(pts, pad=320), title="Captain Bee's Diary - Skyline Valley",
                          legend="Diary pages + final note")
    img.save(os.path.join(a.out, f"{STEM}-skyline-valley-map.jpg"), "JPEG", quality=88, optimize=True)
    for p in pts:
        print(p["n"], p["name"], round(p["x"]), round(p["y"]))
    print("maps ->", a.out)


if __name__ == "__main__":
    main()
