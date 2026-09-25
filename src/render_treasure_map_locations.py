#!/usr/bin/env python3
r"""
render_treasure_map_locations.py - the overview maps for the three Treasure Maps
dig-site pages that render through df-bnb-collectables-spawns.js:

  /df/treasure-maps/pint-sized-phantoms/grave-sites/   set pint-sized-phantom-graves
  /df/treasure-maps/locations/                         set treasure-maps-locations
  /df/treasure-maps/lucky-strike/dig-locations/        set u-mine-it

Each set gets the same four outputs the Slasher mask page has, written into its own
folder under "Guides and Stuff\.Treasure Maps":

  01 Full Maps (4096)/<file>.jpg               plain 4K map, legend, watermark
  02 Numbered Maps (4096)/<file>_numbered.jpg  same map, every dig site numbered
  03 Region Tiles/<RegionSlug>_<file>.jpg      numbered crop per region (archive)
  04 Upload to Site/<file>-<YYYY-MM>.jpg       dated copies for WordPress (cache-busting)
  <file>_coords.csv                            n, region, marker, ref, x, y

and publishes its numbering to tsv/collectable_map_numbers.tsv, so the printable
tick-off checklist on the page reads the same numbers as the numbered map.

Numbering (the number a reader already sees in game or on the page):
  graves          the grave's site number   (SDOW_GraveNN / site_number column)
  treasure maps   the mound's map number    (TreasureMapMoundActivatorNN)
  u-mine-it       1..N in PAGE order         (region A-Z, marker order, TSV order)

Coordinates come straight from the committed dig-site TSVs (the same rows the page is
built from), so the map and the page can never disagree about how many sites exist.
The Mappalachia DB supplies only the projection and the region extents.

Usage:
  python render_treasure_map_locations.py                     # all three
  python render_treasure_map_locations.py --sets treasure-maps
  python render_treasure_map_locations.py --stamp 2026-09 --out "D:\...\.Treasure Maps"

Env: MAPPALACHIA_DIR / MAPPALACHIA_DB / TREASURE_MAPS_OUT
"""

import argparse, csv, datetime, os, re, shutil, sys
from collections import defaultdict

from PIL import Image, ImageDraw

Image.MAX_IMAGE_PIXELS = None

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import map_watermark                                   # every saved map is watermarked
import render_slasher_maps as rsm                      # projection, markers, legend, font
from render_spawn_maps import region_bboxes            # the fixed <Family>SubRegion## lookup

try:                                                   # page marker order (Rollins before NWOT)
    from build_collectable_spawns_json import marker_sort_key
except Exception:                                      # pragma: no cover
    def marker_sort_key(region, marker):
        return (marker or "").lower()

S = rsm.S
MAPPALACHIA = rsm.MAPPALACHIA

DEFAULT_OUT = os.environ.get(
    "TREASURE_MAPS_OUT",
    os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff", ".Treasure Maps"))

MAP_NUMBERS_TSV = os.path.join(REPO, "tsv", "collectable_map_numbers.tsv")
TILE_WIDTH = 2600

SETS = {
    "graves": {
        "slug": "pint-sized-phantom-graves",
        "tsv": "phantom_grave_sites.tsv",
        "folder": "Slasher Grave Locations",
        "file": "slasher_graves",
        "title": "Pint-Sized Phantoms' Grave Sites",
        "label": "Disturbed Graves",
        "fill": rsm.GRAVE_FILL,           # magenta diamond, as on the Slasher maps
        "shape": "diamond",
        "number": "site",
    },
    "treasure-maps": {
        "slug": "treasure-maps-locations",
        "tsv": "treasure_map_dig_sites.tsv",
        "folder": "Treasure Map Locations",
        "file": "treasure_map_locations",
        "title": "Treasure Map Dig Sites",
        "label": "Treasure Map Mounds",
        "fill": (0, 200, 220),            # cyan
        "shape": "circle",
        "number": "edid",
    },
    "u-mine-it": {
        "slug": "u-mine-it",
        "tsv": "u_mine_it_dig_sites.tsv",
        "folder": "U Mine It Locations",
        "file": "u_mine_it_locations",
        "title": "U Mine It (Lucky Strike) Dig Sites",
        "label": "Dig Sites",
        "fill": (255, 112, 40),           # orange, like the Mappalachia export
        "shape": "circle",
        "number": "sequence",
    },
}


# ---------------------------------------------------------------- data

def _tsv_path(name):
    p = os.path.join(REPO, "tsv", name)
    if not os.path.exists(p):
        raise SystemExit(f"missing {p}")
    return p


def _coord_backfill(name):
    """{ref: (x, y)} from the PTS copy of a dig-site TSV. Graves 5/7/9 were added to the
    live TSV by hand from xEdit (no coordinates); the PTS placement export has them."""
    out = {}
    p = os.path.join(REPO, "tsv", "pts", name)
    if os.path.exists(p):
        with open(p, encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                try:
                    out[(r.get("ref_formid") or "").strip().upper()] = (float(r["x"]), float(r["y"]))
                except (TypeError, ValueError, KeyError):
                    pass
    return out


def _grave_overrides():
    """{ref: (region, marker)} as the PAGE shows them (phantom_grave_notes.tsv overrides
    applied by the page builder), so the coords CSV and checklist agree with the page."""
    try:
        import build_collectable_spawns_json as bcs
        return {(r.get("ref_formid") or "").upper(): (r.get("region"), r.get("marker"))
                for r in bcs._read_grave_rows()}
    except Exception as e:                             # pragma: no cover
        print(f"  (grave overrides unavailable: {e})", file=sys.stderr)
        return {}


def load_points(meta):
    """Rows of the set's dig-site TSV, numbered the way the page reader will know them."""
    rows = []
    backfill = _coord_backfill(meta["tsv"])
    overrides = _grave_overrides() if meta["number"] == "site" else {}
    with open(_tsv_path(meta["tsv"]), encoding="utf-8", newline="") as fh:
        for i, r in enumerate(csv.DictReader(fh, delimiter="\t")):
            ref = (r.get("ref_formid") or "").strip().upper()
            try:
                x, y = float(r.get("x") or ""), float(r.get("y") or "")
            except ValueError:
                if ref not in backfill:
                    print(f"  WARNING {meta['slug']}: {ref or r.get('ref_edid')} has no "
                          "coordinates — not on the map", file=sys.stderr)
                    continue
                x, y = backfill[ref]
            reg, mk = overrides.get(ref, (None, None))
            if reg:
                r["region"] = reg
            if mk:
                r["closest_fast_travel"] = mk
            rows.append({
                "i": i,
                "ref": ref,
                "edid": (r.get("ref_edid") or "").strip(),
                "site": (r.get("site_number") or "").strip(),
                "region": (r.get("region") or "").strip() or "Unknown",
                "marker": (r.get("closest_fast_travel") or "").strip() or "(unknown location)",
                "x": x, "y": y,
            })

    # page order: region A-Z, marker (with the builder's overrides), then TSV order
    rows.sort(key=lambda r: (r["region"], marker_sort_key(r["region"], r["marker"]), r["i"]))

    for seq, r in enumerate(rows, 1):
        if meta["number"] == "site":
            r["n"] = r["site"] or "?"
        elif meta["number"] == "edid":
            m = re.search(r"(\d+)\s*$", r["edid"])
            r["n"] = str(int(m.group(1))) if m else "?"
        else:
            r["n"] = str(seq)
    return rows


# ---------------------------------------------------------------- label placement

def _overlap(a, b):
    w = min(a[2], b[2]) - max(a[0], b[0])
    h = min(a[3], b[3]) - max(a[1], b[1])
    return w * h if w > 0 and h > 0 else 0


def place_labels(draw, pts, font, size, r):
    """Put each number beside its marker without covering another marker or number.

    pts = [(x, y, text)] in canvas pixels; r = marker radius; size = (w, h) canvas.
    Tries eight positions around the dot, then the same eight pushed further out with a
    thin leader line back to the dot, and keeps the first clear spot (else the least
    covered) — so a tight cluster like The Burning Mine's five sites stays readable."""
    W, H = size
    gap = 6
    taken = [(x - r - 2, y - r - 2, x + r + 2, y + r + 2) for x, y, _ in pts]
    # most-crowded markers first, so they get the free space
    def crowd(p):
        return -sum(1 for q in pts if abs(q[0] - p[0]) < 6 * r and abs(q[1] - p[1]) < 6 * r)
    for cx, cy, text in sorted(pts, key=lambda p: (crowd(p), p[1], p[0])):
        x0, y0, x1, y1 = draw.textbbox((0, 0), text, font=font)
        w, h = x1 - x0, y1 - y0
        best, best_cost = None, None
        for ring in (0, 1.6 * r + h * 0.6, 3.2 * r + h * 1.1):
            rr = r + gap + ring
            cands = [
                (cx + rr, cy - h / 2),               # right
                (cx - rr - w, cy - h / 2),           # left
                (cx - w / 2, cy - rr - h),           # above
                (cx - w / 2, cy + rr),               # below
                (cx + rr * 0.7, cy - rr * 0.7 - h),  # above-right
                (cx - rr * 0.7 - w, cy - rr * 0.7 - h),
                (cx + rr * 0.7, cy + rr * 0.7),      # below-right
                (cx - rr * 0.7 - w, cy + rr * 0.7),
            ]
            for tx, ty in cands:
                box = (tx - 4, ty - 4, tx + w + 4, ty + h + 4)
                if box[0] < 0 or box[1] < 0 or box[2] > W or box[3] > H:
                    continue
                cost = sum(_overlap(box, t) for t in taken) + ring * 0.01
                if best is None or cost < best_cost:
                    best, best_cost = (tx, ty, box, ring), cost
            if best is not None and best_cost < 1:
                break
        tx, ty, box, ring = best
        taken.append(box)
        if ring:
            # leader from the dot's edge to the nearest point of the label box
            lx = min(max(cx, box[0]), box[2]); ly = min(max(cy, box[1]), box[3])
            draw.line([(cx, cy), (lx, ly)], fill=(0, 0, 0), width=6)
            draw.line([(cx, cy), (lx, ly)], fill=(255, 255, 255), width=2)
        rsm.draw_outlined_text(draw, (tx - x0, ty - y0), text, font, w=3)


def redraw_dots(draw, pts, fill, shape, scale=1.0):
    """Re-stamp markers over any leader lines so the dots stay on top."""
    for x, y, _ in pts:
        if scale == 1.0:
            rsm.draw_marker(draw, x, y, fill, shape)
        else:
            r = rsm.DOT_D * scale / 2.0
            box = [x - r, y - r, x + r, y + r]
            if shape == "diamond":
                draw.polygon([(x, y - r), (x + r, y), (x, y + r), (x - r, y)], fill=fill,
                             outline=rsm.DOT_OUTLINE, width=rsm.DOT_OUTLINE_W)
            else:
                draw.ellipse(box, fill=fill, outline=rsm.DOT_OUTLINE, width=rsm.DOT_OUTLINE_W)


# ---------------------------------------------------------------- render

def render(key, out_root, stamp, to_px, boxes):
    meta = SETS[key]
    rows = load_points(meta)
    if not rows:
        print(f"  {key}: no rows — skipped")
        return

    set_dir = os.path.join(out_root, meta["folder"])
    d_full = os.path.join(set_dir, "01 Full Maps (4096)")
    d_num = os.path.join(set_dir, "02 Numbered Maps (4096)")
    d_tile = os.path.join(set_dir, "03 Region Tiles")
    d_up = os.path.join(set_dir, "04 Upload to Site")
    for d in (d_full, d_num, d_tile, d_up):
        os.makedirs(d, exist_ok=True)

    bg = os.path.join(MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    base = Image.open(bg).convert("RGB")
    if base.size != (S, S):
        base = base.resize((S, S), Image.LANCZOS)

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    for p in rows:
        p["px"], p["py"] = to_px(p["x"], p["y"])
        rsm.draw_marker(d, p["px"], p["py"], meta["fill"], meta["shape"])
    rsm.draw_legend(plain, meta["title"], [(meta["label"], len(rows), meta["fill"], meta["shape"])])

    numbered = plain.copy()
    dn = ImageDraw.Draw(numbered)
    full_pts = [(p["px"], p["py"], str(p["n"])) for p in rows]
    place_labels(dn, full_pts, rsm._font(56), (S, S), rsm.DOT_D / 2.0)
    redraw_dots(dn, full_pts, meta["fill"], meta["shape"])

    f_plain = f"{meta['file']}.jpg"
    f_num = f"{meta['file']}_numbered.jpg"
    map_watermark.apply(plain).save(os.path.join(d_full, f_plain), "JPEG", quality=88, optimize=True)
    map_watermark.apply(numbered).save(os.path.join(d_num, f_num), "JPEG", quality=88, optimize=True)

    # dated copies for the site (a same-name replacement sits behind CDN caches for days)
    shutil.copyfile(os.path.join(d_full, f_plain), os.path.join(d_up, f"{meta['file']}-{stamp}.jpg"))
    shutil.copyfile(os.path.join(d_num, f_num), os.path.join(d_up, f"{meta['file']}_numbered-{stamp}.jpg"))

    # region tiles — numbered crops at the region's real extent
    by_region = defaultdict(list)
    for p in rows:
        by_region[p["region"]].append(p)
    for region, rr in sorted(by_region.items()):
        box = boxes.get(region)
        if box:
            x0, y0 = to_px(box[0], box[3])
            x1, y1 = to_px(box[2], box[1])
        else:
            xs = [p["px"] for p in rr]; ys = [p["py"] for p in rr]
            x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
        pad = 120
        x0 = max(0, int(x0 - pad)); y0 = max(0, int(y0 - pad))
        x1 = min(S, int(x1 + pad)); y1 = min(S, int(y1 + pad))
        if x1 - x0 < 800 or y1 - y0 < 800:
            cxm, cym = (x0 + x1) // 2, (y0 + y1) // 2
            x0, x1 = max(0, cxm - 400), min(S, cxm + 400)
            y0, y1 = max(0, cym - 400), min(S, cym + 400)
        # Crop the UNNUMBERED map and number it again at tile scale: zooming in gives a
        # tight cluster room its full-map labels never had.
        base_crop = base.crop((x0, y0, x1, y1))
        w, h = base_crop.size
        k = TILE_WIDTH / w
        crop = base_crop.resize((TILE_WIDTH, max(1, int(h * k))), Image.LANCZOS)
        dt = ImageDraw.Draw(crop)
        tpts = [((p["px"] - x0) * k, (p["py"] - y0) * k, str(p["n"])) for p in rows
                if x0 <= p["px"] <= x1 and y0 <= p["py"] <= y1]
        dscale = min(k, 1.6)
        redraw_dots(dt, tpts, meta["fill"], meta["shape"], scale=dscale)
        place_labels(dt, tpts, rsm._font(64), crop.size, rsm.DOT_D * dscale / 2.0)
        redraw_dots(dt, tpts, meta["fill"], meta["shape"], scale=dscale)
        crop = map_watermark.apply(crop, corner="bottom-right")
        crop.save(os.path.join(d_tile, f"{region.replace(' ', '')}_{meta['file']}.jpg"),
                  "JPEG", quality=88, optimize=True)

    with open(os.path.join(set_dir, f"{meta['file']}_coords.csv"), "w", newline="",
              encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["n", "region", "marker", "ref_formid", "ref_edid", "game_x", "game_y"])
        for p in rows:
            w_.writerow([p["n"], p["region"], p["marker"], p["ref"], p["edid"],
                         round(p["x"], 1), round(p["y"], 1)])

    publish_numbers(meta["slug"], rows)
    print(f"  {key:14} points={len(rows):3}  regions={len(by_region)}  -> {set_dir}")


def publish_numbers(slug, rows):
    """Rewrite this set's rows in tsv/collectable_map_numbers.tsv; keep every other set."""
    header = "set\tref_formid\tmap_number\tmap_region\tmap_marker"
    keep = []
    if os.path.exists(MAP_NUMBERS_TSV):
        with open(MAP_NUMBERS_TSV, encoding="utf-8") as fh:
            fh.readline()
            keep = [ln.rstrip("\n") for ln in fh
                    if ln.strip() and ln.split("\t", 1)[0] != slug]
    mine = ["\t".join([slug, p["ref"], str(p["n"]), p["region"], p["marker"]])
            for p in rows if p["ref"] and str(p["n"]).isdigit()]
    with open(MAP_NUMBERS_TSV, "w", newline="", encoding="utf-8") as fh:
        fh.write(header + "\n")
        for ln in sorted(keep) + mine:
            fh.write(ln + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sets", nargs="+", default=list(SETS), choices=list(SETS))
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--stamp", default=datetime.date.today().strftime("%Y-%m"),
                    help="date suffix for the 04 Upload to Site copies (default YYYY-MM)")
    args = ap.parse_args()

    conn = rsm._db()
    to_px = rsm.projector(rsm.load_space(conn))
    boxes = region_bboxes(conn)
    for k in args.sets:
        render(k, args.out, args.stamp, to_px, boxes)


if __name__ == "__main__":
    main()
