#!/usr/bin/env python3
r"""
render_pet_taming_maps.py - the pet-taming route maps (Find and Tame a Pet page).

Kat Pender's route (full 4096 map) and Garust's Cranberry Bog route (a zoomed crop),
drawn on the clean Mappalachia base with the PTS map-marker icons and the live Scene
encounter spots. Kat, 19 Nov 2026: no "White line = the route..." note box - the
legend holds the title and the Scene encounter count only.

WHERE THINGS COME FROM
  base      Mappalachia img/wrld/Appalachia_menu.jpg (4096), same projection as every
            map in this repo: centre (-500, 135), range 582550.
  icons     dist/pts/locations/all_locations.json (the channel's own markers, cut ones
            left out), icon art = Mappalachia img/mapmarker/<icon>.svg.
  spots     the live RETriggerScene refs (trigger has a script) in Appalachia - the 74
            in scene-encounter-spots.json (re-checked 19 Nov against the 13 Nov PTS ESM
            and the live ESM: same 74).
  routes    data/pet_taming_routes/<name>.png - the white route line only (RGBA, 4096
            for Kat, the crop size for Garust), lifted from the 17 Nov maps. Draw order:
            base > route > icons > dots, like the originals.

Usage (Sally device shell):
  PYTHONPATH=/tmp/pylib python3 src/render_pet_taming_maps.py [--out DIR]
"""
import argparse
import io
import json
import os
import sys

from PIL import Image, ImageDraw

Image.MAX_IMAGE_PIXELS = None
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
os.environ.setdefault("MAPPALACHIA_DIR", os.path.expanduser("~/mnt/Mappalachia"))
import map_watermark                     # standing rule: every map is watermarked
import render_slasher_maps as rsm        # fonts, legend look

S = 4096
CX, CY, RANGE = -500, 135, 582550
GUIDES = os.environ.get("GUIDES_ROOT", os.path.expanduser("~/mnt/Guides and Stuff"))
PET_DIR = os.path.join(GUIDES, ".Pets", "Tamed Pets", "Find and Tame a Pet")
SPOTS = os.path.join(PET_DIR, "02 Scene Maps (4096)", "scene-encounter-spots.json")
ROUTES = os.path.join(REPO, "data", "pet_taming_routes")
SCENE_FILL = (240, 228, 66)

# name -> title, crop box in 4096 px (None = whole map), output width, icon px, dot px
MAPS = {
    "kat-pender-pet-hunting-route": ("Kat Pender's Pet Hunting Route", None, 4096, 46, 38),
    "garust-cranberry-bog-pet-hunting-route": ("Garust's Cranberry Bog Pet Route",
                                               (2563.0, 1981.9, 2563.0 + 2600 / 2.2103,
                                                1981.9 + 2703 / 2.2103), 2600, 70, 48),
}


def to_px(x, y):
    return ((x - CX) / RANGE * S + S / 2, (CY - y) / RANGE * S + S / 2)


_icons = {}
def icon(name, size):
    import cairosvg
    k = (name, size)
    if k not in _icons:
        svg = os.path.join(rsm.MAPPALACHIA, "img", "mapmarker", name + ".svg")
        if not os.path.exists(svg):
            _icons[k] = None
        else:
            im = Image.open(io.BytesIO(cairosvg.svg2png(url=svg, output_height=size * 4))).convert("RGBA")
            im.thumbnail((size, size), Image.LANCZOS)
            _icons[k] = im
    return _icons[k]


def markers():
    d = json.load(open(os.path.join(REPO, "dist", "pts", "locations", "all_locations.json"), encoding="utf-8"))
    return [(to_px(l["x"], l["y"]), l["icon"]) for r in d["regions"] for l in r["locations"]
            if l.get("x") is not None and l.get("icon") and not l.get("cut")]


def spots():
    return [to_px(s["x"], s["y"]) for s in json.load(open(SPOTS, encoding="utf-8"))]


def plain(box, out_w, isz, dot, route=None):
    """base (+ route) + icons + dots, no legend, no watermark."""
    bg = Image.open(os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")).convert("RGB")
    if bg.size != (S, S):
        bg = bg.resize((S, S), Image.LANCZOS)
    if box is None:
        box = (0, 0, S, S)
    k = out_w / (box[2] - box[0])
    out_h = round((box[3] - box[1]) * k)
    im = bg.resize((out_w, out_h), Image.LANCZOS, box=box).convert("RGBA")
    if route is not None:
        im.alpha_composite(route)
    f = lambda p: ((p[0] - box[0]) * k, (p[1] - box[1]) * k)
    for p, ic in markers():
        ox, oy = f(p)
        i = icon(ic, isz)
        if i and -isz < ox < out_w + isz and -isz < oy < out_h + isz:
            im.alpha_composite(i, (int(ox - i.width / 2), int(oy - i.height / 2)))
    d = ImageDraw.Draw(im)
    r = dot / 2
    n = 0
    for p in spots():
        ox, oy = f(p)
        if -r < ox < out_w + r and -r < oy < out_h + r:
            d.ellipse([ox - r, oy - r, ox + r, oy + r], fill=SCENE_FILL,
                      outline=rsm.DOT_OUTLINE, width=max(4, round(dot / 10)))
            if 0 <= ox < out_w and 0 <= oy < out_h:
                n += 1
    return im.convert("RGB"), n


def render(name, out_dir):
    title, box, out_w, isz, dot = MAPS[name]
    rp = os.path.join(ROUTES, name + ".png")
    route = Image.open(rp).convert("RGBA") if os.path.exists(rp) else None
    im, n = plain(box, out_w, isz, dot, route)
    rsm.draw_legend(im, title, [("Scene encounter", n, SCENE_FILL, "circle")])
    out = os.path.join(out_dir, name + ".jpg")
    map_watermark.apply(im, corner="bottom-left").convert("RGB").save(out, "JPEG", quality=90, optimize=True)
    print(f"{name}: {im.size[0]}x{im.size[1]}, {n} Scene spots -> {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(PET_DIR, "01 Route Maps (4096)"))
    ap.add_argument("maps", nargs="*", default=list(MAPS))
    a = ap.parse_args()
    for m in a.maps:
        render(m, a.out)
