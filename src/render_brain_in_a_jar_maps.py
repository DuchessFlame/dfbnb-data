#!/usr/bin/env python3
r"""
render_brain_in_a_jar_maps.py - maps for the Brain in a Jar collectable (A Matter of Matter).

House style is render_spawn_maps.py's (it is imported, not copied): amber dots at
DOT_D, numbers placed clear of every dot and number, watermark on every file.

Reads:   data/brain_in_a_jar/placements_<channel>.tsv  (esm_scan_placements.py)
         data/brain_in_a_jar/numbering.tsv              (fixed numbers, route order)
Writes (under --out, default Guides and Stuff\.Collectables\Brain in a Jar Locations):
  01 Full Maps (4096)/brain_in_a_jar.jpg
  02 Numbered Maps (4096)/brain_in_a_jar_numbered.jpg
  03 Region Tiles/<region-slug>-spawn-map.jpg  + <Region>_coords.csv
  04 Page Maps/brain-in-a-jar-locations-<a>-<b>-map.jpg   one per location page:
        that page's brains numbered, every other brain a small grey dot for context
  <a>-<b>/                                                 empty photo folders, one per page
  brain_in_a_jar_coords.csv

Page and region maps are drawn natively at tile size (the background is cropped and
scaled first, then the dots are drawn), so the numbers stay sharp - a crop of the
4096 map scaled up would blur them.

Usage: python src/render_brain_in_a_jar_maps.py [--channel pts|live] [--out DIR]
Env:   MAPPALACHIA_DIR
"""
import argparse, csv, os, sys
from PIL import Image, ImageDraw

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import render_spawn_maps as rsm
import map_watermark

DATA = os.path.join(REPO, "data", "brain_in_a_jar")
PAGE_SIZE = 30
DEFAULT_OUT = os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff",
                           ".Collectables", "Brain in a Jar Locations")
CONTEXT_FILL = (150, 150, 150)
CONTEXT_D = 18


def load_points(channel):
    num = {}
    with open(os.path.join(DATA, "numbering.tsv"), encoding="utf-8") as fh:
        rd = csv.DictReader((ln for ln in fh if not ln.startswith("#")), delimiter="\t")
        for r in rd:
            num[r["RefFormID"]] = int(r["n"])
    pts = []
    with open(os.path.join(DATA, f"placements_{channel}.tsv"), encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["WorldspaceFormID"] != "0025DA15" or r["RefFormID"] not in num:
                continue
            pts.append({"n": num[r["RefFormID"]], "ref": r["RefFormID"],
                        "x": float(r["X"]), "y": float(r["Y"]),
                        "region": r["region"], "marker": r["marker"]})
    return sorted(pts, key=lambda p: p["n"])


def pages_for(total):
    return [(a, min(a + PAGE_SIZE - 1, total)) for a in range(1, total + 1, PAGE_SIZE)]


def draw_crop(bg, rows, box, context=(), title=None, legend="Brain in a Jar", label=lambda r: str(r["n"])):
    """Crop bg to box (4096 px space), scale to TILE_WIDTH, draw context dots grey,
    then rows numbered. rows carry px/py in 4096 space."""
    x0, y0, x1, y1 = box
    crop = bg.crop((x0, y0, x1, y1))
    k = rsm.TILE_WIDTH / (x1 - x0)
    crop = crop.resize((rsm.TILE_WIDTH, max(1, int((y1 - y0) * k))), Image.LANCZOS)
    d = ImageDraw.Draw(crop)
    for c in context:
        cx, cy = (c["px"] - x0) * k, (c["py"] - y0) * k
        r = CONTEXT_D / 2
        d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=CONTEXT_FILL, outline=rsm.DOT_OUTLINE, width=2)
    loc = [dict(r, px=(r["px"] - x0) * k, py=(r["py"] - y0) * k) for r in rows]
    rsm.spread_dots(loc)
    for r in loc:
        rsm.draw_dot(d, r["px"], r["py"])
    f = rsm._font(48)
    for r in loc:                      # place_labels measures str(r["n"])
        r["_n"], r["n"] = r["n"], label(r)
    rsm.place_labels(d, loc, f, gap=rsm.DOT_D * 0.6 + 6)
    for r in loc:
        r["n"] = r["_n"]
    for r in loc:
        rsm.draw_outlined_text(d, r["label_xy"], label(r), f, w=3)
    if title:
        rsm.draw_legend(crop, title, [(legend, len(rows))])
    return map_watermark.apply(crop, corner="bottom-right")


def bbox(rows, pad=260, min_side=900):
    xs = [r["px"] for r in rows]; ys = [r["py"] for r in rows]
    x0, x1, y0, y1 = min(xs) - pad, max(xs) + pad, min(ys) - pad, max(ys) + pad
    if x1 - x0 < min_side:
        c = (x0 + x1) / 2; x0, x1 = c - min_side / 2, c + min_side / 2
    if y1 - y0 < min_side * 0.6:
        c = (y0 + y1) / 2; y0, y1 = c - min_side * 0.3, c + min_side * 0.3
    return (max(0, int(x0)), max(0, int(y0)), min(rsm.S, int(x1)), min(rsm.S, int(y1)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", default="pts", choices=["pts", "live"])
    ap.add_argument("--out", default=DEFAULT_OUT)
    a = ap.parse_args()
    out = a.out
    pts = load_points(a.channel)
    conn = rsm._db()
    spaces = rsm.load_spaces(conn)
    to_px = rsm.projector(spaces[rsm.APPALACHIA_SPACE])
    boxes = rsm.region_bboxes(conn)
    for p in pts:
        p["px"], p["py"] = to_px(p["x"], p["y"])

    bg_path = os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    bg = Image.open(bg_path).convert("RGB")
    if bg.size != (rsm.S, rsm.S):
        bg = bg.resize((rsm.S, rsm.S), Image.LANCZOS)

    # 01 / 02 — full maps, one dot per brain (rows keep their own n, no clustering)
    rows = [dict(p) for p in pts]
    for r in rows:
        r["count"] = 1; r["refs"] = [r["ref"]]; r["source_type"] = ""
    rsm.render_exterior(bg_path, rows, "Brain in a Jar",
                        os.path.join(out, "01 Full Maps (4096)", "brain_in_a_jar.jpg"),
                        os.path.join(out, "02 Numbered Maps (4096)", "brain_in_a_jar_numbered.jpg"))

    # 03 — region tiles, drawn natively
    tdir = os.path.join(out, "03 Region Tiles"); os.makedirs(tdir, exist_ok=True)
    by_reg = {}
    for p in pts:
        by_reg.setdefault(p["region"], []).append(p)
    for reg, rr in sorted(by_reg.items()):
        img = draw_crop(bg, rr, bbox(rr), title=f"Brain in a Jar - {reg}")
        img.save(os.path.join(tdir, f"{rsm._region_slug(reg)}-spawn-map.jpg"), "JPEG", quality=88, optimize=True)
        with open(os.path.join(tdir, f"{reg.replace(' ', '')}_coords.csv"), "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh); w.writerow(["n", "region", "marker", "game_x", "game_y", "ref"])
            for p in rr:
                w.writerow([p["n"], p["region"], p["marker"], round(p["x"], 1), round(p["y"], 1), p["ref"]])

    # 04 — one map per location page + its photo folder
    pdir = os.path.join(out, "04 Page Maps"); os.makedirs(pdir, exist_ok=True)
    for lo, hi in pages_for(len(pts)):
        rr = [p for p in pts if lo <= p["n"] <= hi]
        ctx = [p for p in pts if not lo <= p["n"] <= hi]
        img = draw_crop(bg, rr, bbox(rr), context=ctx, title=f"Brains in Jars {lo} - {hi}")
        img.save(os.path.join(pdir, f"brain-in-a-jar-locations-{lo}-{hi}-map.jpg"), "JPEG", quality=88, optimize=True)
        os.makedirs(os.path.join(out, f"{lo}-{hi}"), exist_ok=True)

    with open(os.path.join(out, "brain_in_a_jar_coords.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(["n", "region", "marker", "game_x", "game_y", "ref"])
        for p in pts:
            w.writerow([p["n"], p["region"], p["marker"], round(p["x"], 1), round(p["y"], 1), p["ref"]])
    print(f"{len(pts)} brains -> {out}")


if __name__ == "__main__":
    main()
