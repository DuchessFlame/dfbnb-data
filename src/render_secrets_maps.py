#!/usr/bin/env python3
r"""
render_secrets_maps.py - maps for the Quests > Secrets & Easter Eggs pages (28 Nov 2026).

Duchess: people prefer a map with the places marked over written directions, so
every Secrets page with places offers both. House style is render_spawn_maps.py's
(imported): watermark, the game's map-marker icons, numbers clear of the dots.

Reads  data/secrets/<slug>/map_points.tsv   n, type, label, x, y, source
         type: note (amber circle), place (white circle), key (cyan diamond),
               prize (yellow star) - shape as well as colour, for colour-blind readers.
Writes into the page's PC photo folder (mirrors the server folder):
  <slug>-map.jpg            full 4096 map, marks only
  <slug>-map_numbered.jpg   full 4096 map, labelled
  <slug>-<region>-map.jpg   close-up of the spots, labelled

Usage: python src/render_secrets_maps.py <slug> [<slug> ...]
Env:   MAPPALACHIA_DIR (default D:\Mappalachia)
"""
import argparse, csv, os, sys
from PIL import Image, ImageDraw

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import render_spawn_maps as rsm
import render_brain_in_a_jar_maps as brain   # bbox + map-marker icon cache
import render_event_swap_maps as esw
import map_watermark

QUESTS = os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff", ".Quests")
PAGES = {   # slug -> (PC photo folder, region for the close-up, map title)
    "pioneer-treasure-hunt": ("Pioneer Treasure Hunt", "Skyline Valley", "Pioneer Treasure Hunt"),
    "dustys-treasure-hunt": ("Dustys Treasure Hunt", "Burning Springs", "Dusty's Treasure Hunt"),
    "odd-looking-rock": ("Odd Looking Rock", "Burning Springs", "Odd Looking Rock"),
    "tnt-dome-2-key": ("TNT Dome Keys", "Black Mountain Ordnance Works", "TNT Dome 2 Key"),
    "tnt-dome-3-key": ("TNT Dome Keys", "Black Mountain Ordnance Works", "TNT Dome 3 Key"),
    "tnt-dome-7-key": ("TNT Dome Keys", "Black Mountain Ordnance Works", "TNT Dome 7 Key"),
    "tnt-dome-8-key": ("TNT Dome Keys", "Black Mountain Ordnance Works", "TNT Dome 8 Key"),
}
# Close-up only shows these types (the domes' chains cross the whole map; the close-up is the dome).
CLOSEUP_ONLY = {s: {"prize"} for s in ("tnt-dome-2-key", "tnt-dome-3-key", "tnt-dome-7-key", "tnt-dome-8-key")}
TYPES = {   # type -> (legend label, colour, shape)
    "note": ("Note", (255, 193, 7), "circle"),
    "place": ("Riddle answer", (255, 255, 255), "circle"),
    "key": ("Key", (0, 209, 255), "diamond"),
    "prize": ("Prize", (255, 235, 59), "star"),
}
LEGEND = {  # per-page legend wording
    "dustys-treasure-hunt": {"note": "Note (one of these)", "key": "Dusty's Keys", "prize": "Storage room doors"},
    "pioneer-treasure-hunt": {"note": "Start note", "place": "Riddle answers", "prize": "Prize stump"},
    "odd-looking-rock": {"prize": "Odd Looking Rock"},
    **{s: {"key": "Key and code steps", "place": "Places to go", "prize": "The dome"}
       for s in ("tnt-dome-2-key", "tnt-dome-3-key", "tnt-dome-7-key", "tnt-dome-8-key")},
}


def load(slug):
    with open(os.path.join(REPO, "data", "secrets", slug, "map_points.tsv"), encoding="utf-8") as fh:
        rd = csv.DictReader((ln for ln in fh if not ln.startswith("#")), delimiter="\t")
        return [{"n": r["n"], "type": r["type"], "label": r["label"],
                 "x": float(r["x"]), "y": float(r["y"])} for r in rd]


def setup_types(slug):
    words = LEGEND.get(slug, {})
    rsm._PAGE["slug"] = slug
    rsm.PAGE_TYPE_LABELS[slug] = {}
    for t, (lab, col, shape) in TYPES.items():
        key = "sx-" + t
        rsm.TYPE_COLOURS[key] = col
        rsm.TYPE_SHAPES[key] = shape
        rsm.PAGE_TYPE_LABELS[slug][key] = words.get(t, lab)


CROP = {"dustys-treasure-hunt": dict(pad=110, min_side=440),
        **{s: dict(pad=260, min_side=620) for s in ("tnt-dome-2-key", "tnt-dome-3-key", "tnt-dome-7-key", "tnt-dome-8-key")}}   # all spots inside one POI: zoom in


def close_up(bg, rows, title, slug=""):
    x0, y0, x1, y1 = brain.bbox(rows, **CROP.get(slug, dict(pad=320)))
    crop = bg.crop((x0, y0, x1, y1))
    k = rsm.TILE_WIDTH / (x1 - x0)
    crop = crop.resize((rsm.TILE_WIDTH, max(1, int((y1 - y0) * k))), Image.LANCZOS)
    for mx, my, _lbl, im in brain._MARKS:
        cx, cy = (mx - x0) * k, (my - y0) * k
        w = brain.TILE_ICON_PX
        if -w < cx < crop.width + w and -w < cy < crop.height + w:
            ic = im.resize((w, max(1, int(im.height * w / im.width))), Image.LANCZOS)
            crop.paste(ic, (int(cx - ic.width / 2), int(cy - ic.height / 2)), ic)
    d = ImageDraw.Draw(crop)
    loc = [dict(r, px=(r["px"] - x0) * k, py=(r["py"] - y0) * k) for r in rows]
    rsm.spread_dots(loc)
    for r in loc:
        rsm.draw_dot(d, r["px"], r["py"], fill=rsm.type_colour(r["source_type"]), shape=rsm.type_shape(r["source_type"]))
    f = rsm._font(48)
    rsm.place_labels(d, loc, f, gap=rsm.DOT_D * 0.6 + 6)
    for r in loc:
        rsm.draw_outlined_text(d, r["label_xy"], str(r["n"]), f, w=3)
    rsm.draw_legend(crop, title, rsm.legend_rows(loc))
    return map_watermark.apply(crop, corner="bottom-left")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slugs", nargs="+", choices=sorted(PAGES))
    a = ap.parse_args()
    conn = rsm._db()
    to_px = rsm.projector(rsm.load_spaces(conn)[rsm.APPALACHIA_SPACE])
    plain_bg = os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    if not brain._MARKS:
        for mx, my, lbl, im in esw.channel_marker_icons("live", size=brain.TILE_ICON_PX * 2):
            px, py = to_px(mx, my)
            brain._MARKS.append((px, py, lbl, im))
    bg_path = esw._icon_background(plain_bg, to_px, marks=esw.channel_marker_icons("live"))
    bg = Image.open(plain_bg).convert("RGB")
    if bg.size != (rsm.S, rsm.S):
        bg = bg.resize((rsm.S, rsm.S), Image.LANCZOS)
    for slug in a.slugs:
        folder, region, title = PAGES[slug]
        out = os.environ.get("SECRETS_OUT") or os.path.join(QUESTS, folder, "guide-images")
        out = out.replace("{folder}", folder)
        os.makedirs(out, exist_ok=True)
        setup_types(slug)
        rows = load(slug)
        for r in rows:
            r["px"], r["py"] = to_px(r["x"], r["y"])
            r.update(count=1, refs=[], source_type="sx-" + r["type"])
        rsm.render_exterior(bg_path, [dict(r) for r in rows], title,
                            os.path.join(out, f"{slug}-map.jpg"), os.path.join(out, f"{slug}-map_numbered.jpg"))
        reg = region.lower().replace(" ", "-")
        only = CLOSEUP_ONLY.get(slug)
        close_up(bg, [r for r in rows if not only or r["type"] in only], f"{title} - {region}", slug).save(os.path.join(out, f"{slug}-{reg}-map.jpg"), "JPEG", quality=88, optimize=True)
        print(slug, len(rows), "spots ->", out)


if __name__ == "__main__":
    main()
