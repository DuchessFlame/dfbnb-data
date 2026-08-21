#!/usr/bin/env python3
r"""
render_spawn_maps.py - render Mappalachia-style spawn location maps for DF/BNB farming pages.

Replaces the lost, ad-hoc render_cream_maps.py. Unlike that script this one takes the
coordinate transform straight from the Mappalachia DB `Space` table instead of estimated
constants, so dots land where they should (the old constants were ~175px out at 4096).

Backgrounds (never the Satellite map):
  exterior : <MAPPALACHIA>/img/wrld/Appalachia_menu.jpg   (illustrated "Tricentennial")
  interior : <MAPPALACHIA>/img/cell/<spaceEditorID>.jpg

Transform (identical for worldspace and interior cells, S = 4096):
  px = (x - centerX) / maxRange * S + S/2
  py = S/2 - (y - centerY) / maxRange * S
northAngle is deliberately NOT applied - it only orients the compass rose.

Outputs, per item, mirroring the existing Cream layout:
  01 Full Maps (4096)/<slug>.jpg
  02 Numbered Maps (4096)/<slug>_numbered.jpg
  03 Region Tiles/<RegionSlug>_<slug>.jpg  +  <RegionSlug>_coords.csv
  04 Interior Maps/<spaceEditorID>_<slug>.jpg  +  interior_cells.csv
  <slug>_exterior_coords.csv

Usage:
  python render_spawn_maps.py --set deathclaw-egg --out "<...>/.Farming - Eggs/Deathclaw Eggs"
  python render_spawn_maps.py --all-farming --root "<...>/Guides and Stuff"
"""

import argparse, csv, json, os, re, sqlite3, sys
from collections import defaultdict, OrderedDict

from PIL import Image, ImageDraw, ImageFont

Image.MAX_IMAGE_PIXELS = None

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPALACHIA = os.environ.get("MAPPALACHIA_DIR", r"D:\Mappalachia")
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", os.path.join(MAPPALACHIA, "data", "mappalachia.db"))

S = 4096                       # native canvas size, matches Mappalachia's own exports
APPALACHIA_SPACE = 2480661

DOT_D = 20                     # dot diameter in px at S=4096
DOT_FILL = (255, 193, 7)       # #FFC107 amber, as used on the slasher masks map
DOT_OUTLINE = (18, 18, 18)
DOT_OUTLINE_W = 3

LEGEND_PAD = 40
LEGEND_BG = (24, 24, 24)
LEGEND_BORDER = (255, 255, 255)
LEGEND_BORDER_W = 3

TILE_WIDTH = 2600              # region tiles are normalised to this width

# Interior cell maps are saved down from the 4096 working canvas. There are ~1,600 of
# them across all sets; at full 4096 that is ~5 GB into OneDrive, at 2048 it is ~1 GB
# and a single room still reads fine. Raise INTERIOR_SIZE if you want them full-fat.
INTERIOR_SIZE = 2048
INTERIOR_QUALITY = 84

FONT_PATH = os.path.join(MAPPALACHIA, "font", "futura_condensed_bold.otf")
FONT_FALLBACK = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


# ---------------------------------------------------------------- fonts

def _font(size):
    for p in (FONT_PATH, FONT_FALLBACK):
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


# ---------------------------------------------------------------- db

def _db():
    return sqlite3.connect(f"file:{MAPPALACHIA_DB}?mode=ro&immutable=1", uri=True)


def load_spaces(conn):
    """spaceFormID -> dict(edid, name, centerX, centerY, maxRange, isWorldspace)"""
    out = {}
    for fid, edid, name, isw, cx, cy, rng in conn.execute(
        "SELECT spaceFormID, spaceEditorID, spaceDisplayName, isWorldspace, "
        "centerX, centerY, maxRange FROM Space"
    ):
        out[int(fid)] = {
            "edid": edid, "name": name or edid, "is_world": bool(isw),
            "cx": cx, "cy": cy, "range": rng,
        }
    return out


REGION_FAMILIES = {
    "Forest": "Forest", "Mountains": "Savage Divide", "Swamp": "The Mire",
    "Cranberry": "Cranberry Bog", "Toxic": "Toxic Valley", "Ash": "Ash Heap",
}
# Regions with no LocRegion* tiling in the DB (newer content, or instanced spaces).
# These fall back to a padded bounding box around their own spawn points.
NO_POLYGON_REGIONS = {"Skyline Valley", "Burning Springs", "Atlantic City", "The Pitt"}


def region_bboxes(conn):
    """Region display name -> (minx, miny, maxx, maxy) from the region tilings."""
    fam_ids = defaultdict(list)
    for fid, eid in conn.execute("SELECT regionFormID, regionEditorID FROM Region"):
        m = re.match(r"LocRegion([A-Za-z]+)", eid or "")
        if m and m.group(1) in REGION_FAMILIES:
            fam_ids[REGION_FAMILIES[m.group(1)]].append(int(fid))
    boxes = {}
    for name, ids in fam_ids.items():
        q = ("SELECT MIN(x), MIN(y), MAX(x), MAX(y) FROM RegionPoints WHERE regionFormID IN (%s)"
             % ",".join("?" * len(ids)))
        row = conn.execute(q, ids).fetchone()
        if row and row[0] is not None:
            boxes[name] = tuple(row)
    return boxes


# ---------------------------------------------------------------- geometry

def projector(space):
    cx, cy, rng = space["cx"], space["cy"], space["range"]
    def to_px(x, y):
        return ((x - cx) / rng * S + S / 2.0,
                S / 2.0 - (y - cy) / rng * S)
    return to_px


# ---------------------------------------------------------------- drawing

def draw_dot(d, px, py, fill=DOT_FILL):
    r = DOT_D / 2.0
    d.ellipse([px - r, py - r, px + r, py + r],
              fill=fill, outline=DOT_OUTLINE, width=DOT_OUTLINE_W)


def draw_outlined_text(d, xy, text, font, fill=(255, 255, 255), outline=(0, 0, 0), w=3):
    x, y = xy
    for dx in range(-w, w + 1):
        for dy in range(-w, w + 1):
            if dx or dy:
                d.text((x + dx, y + dy), text, font=font, fill=outline)
    d.text((x, y), text, font=font, fill=fill)


def draw_legend(img, title, rows):
    """rows = [(label, count)]. Black box, 3px white border, swatch + text per row."""
    d = ImageDraw.Draw(img)
    f_title = _font(96)
    f_row = _font(72)

    tw = d.textlength(title, font=f_title)
    rw = max([d.textlength(f"{lbl}  ({cnt})", font=f_row) for lbl, cnt in rows] or [0])
    box_w = int(max(tw + 64, rw + 152) + 64)
    box_h = int(96 + 26 + len(rows) * 84 + 24)

    x0, y0 = LEGEND_PAD, LEGEND_PAD
    x1, y1 = x0 + box_w, y0 + box_h
    d.rectangle([x0, y0, x1, y1], fill=LEGEND_BG,
                outline=LEGEND_BORDER, width=LEGEND_BORDER_W)
    d.text((x0 + 32, y0 + 6), title, font=f_title, fill=(255, 255, 255))

    y = y0 + 96 + 26
    for lbl, cnt in rows:
        draw_dot(d, x0 + 48, y + 30)
        d.text((x0 + 88, y), f"{lbl}  ({cnt})", font=f_row, fill=(230, 230, 230))
        y += 84
    return img


# ---------------------------------------------------------------- data loading

def _slug_us(slug):
    return slug.replace("-", "_")


def load_points(slug, source):
    """
    -> (name, page_title, points)
    points = [ {ref, space, x, y, region, marker} ]  one row per spawn instance
    """
    if source == "farming":
        dist = os.path.join(REPO, "dist", "farming_spawns", f"{slug}_spawns.json")
        geo_p = os.path.join(REPO, "data", "farming_spawns", f"geo_cache_{_slug_us(slug)}.json")
    elif source == "nuka":
        dist = os.path.join(REPO, "dist", f"nuka_cola_spawns_{slug}.json")
        geo_p = os.path.join(REPO, "data", "nuka_cola_spawns", "geo_cache.json")
    else:
        raise SystemExit(f"unknown source {source}")

    data = json.load(open(dist, encoding="utf-8"))
    geo = json.load(open(geo_p, encoding="utf-8")) if os.path.exists(geo_p) else {}

    pts = []
    for reg in data.get("regions", []):
        for loc in reg.get("locations", []):
            for sp in loc.get("spawns", []):
                ref = sp.get("ref") or ""
                g = geo.get(str(int(ref, 16))) if ref else None
                if g:
                    space, x, y = int(g["space"]), float(g["x"]), float(g["y"])
                else:
                    c = sp.get("coords") or loc.get("coords")
                    if not c:
                        continue
                    space, x, y = APPALACHIA_SPACE, float(c[0]), float(c[1])
                pts.append({
                    "ref": ref, "space": space, "x": x, "y": y,
                    "region": reg.get("region", ""),
                    "marker": loc.get("marker", ""),
                    "label": sp.get("label", ""),
                })
    return data.get("name", slug), data.get("page_title", slug), pts


# Optional per-set base-form filter, slug -> {base formid ints}.
#
# NOTE: Sugar Bombs needs NO entry here. farming_spawns_config.SUGAR_BOMBS already
# targets 000330F2 / SugarBombs only, so Clean Sugar Bombs are never in the data.
# The 17,950 figure is the LVLI closure fanning one item out across containers and
# NPCs, not a second item leaking in. Do not add a base filter for it - most of those
# spawns are container-sourced, so geo_cache `base` is the CONTAINER's form, not the
# item's, and filtering on it would silently delete ~two thirds of the real spawns.
BASE_FILTERS = {}


def apply_base_filter(slug, source, pts):
    keep = BASE_FILTERS.get(slug)
    if not keep:
        return pts, 0
    geo_p = (os.path.join(REPO, "data", "farming_spawns", f"geo_cache_{_slug_us(slug)}.json")
             if source == "farming" else
             os.path.join(REPO, "data", "nuka_cola_spawns", "geo_cache.json"))
    geo = json.load(open(geo_p, encoding="utf-8")) if os.path.exists(geo_p) else {}
    out, dropped = [], 0
    for p in pts:
        g = geo.get(str(int(p["ref"], 16))) if p["ref"] else None
        base = int(g["base"]) if g and "base" in g else None
        if base is not None and base not in keep:
            dropped += 1
            continue
        out.append(p)
    return out, dropped


# ---------------------------------------------------------------- renderers

def cluster_by_marker(pts, to_px):
    """Group exterior spawns by map marker so a dot = a location, not 3,000 overlaps."""
    groups = OrderedDict()
    for p in pts:
        key = (p["region"], p["marker"] or "Unmarked")
        g = groups.setdefault(key, {"pts": [], "refs": []})
        g["pts"].append((p["x"], p["y"]))
        g["refs"].append(p["ref"])
    rows = []
    for (region, marker), g in groups.items():
        xs = [a for a, _ in g["pts"]]
        ys = [b for _, b in g["pts"]]
        cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
        px, py = to_px(cx, cy)
        rows.append({"region": region, "marker": marker, "count": len(g["pts"]),
                     "x": cx, "y": cy, "px": px, "py": py, "refs": g["refs"]})
    rows.sort(key=lambda r: (r["region"], r["marker"]))
    for i, r in enumerate(rows, 1):
        r["n"] = i
    return rows


def render_exterior(bg_path, rows, title, out_plain, out_numbered):
    base = Image.open(bg_path).convert("RGB")
    if base.size != (S, S):
        base = base.resize((S, S), Image.LANCZOS)

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    for r in rows:
        draw_dot(d, r["px"], r["py"])
    total = sum(r["count"] for r in rows)
    draw_legend(plain, title, [(title, total)])
    os.makedirs(os.path.dirname(out_plain), exist_ok=True)
    plain.save(out_plain, "JPEG", quality=88, optimize=True)

    numbered = plain.copy()
    dn = ImageDraw.Draw(numbered)
    f = _font(56)
    for r in rows:
        draw_outlined_text(dn, (r["px"] + DOT_D, r["py"] - 34), str(r["n"]), f, w=3)
    os.makedirs(os.path.dirname(out_numbered), exist_ok=True)
    numbered.save(out_numbered, "JPEG", quality=88, optimize=True)
    return plain, numbered


def render_region_tiles(numbered_img, rows, boxes, to_px, out_dir, slug):
    os.makedirs(out_dir, exist_ok=True)
    made = []
    by_region = defaultdict(list)
    for r in rows:
        by_region[r["region"]].append(r)

    for region, rrows in sorted(by_region.items()):
        box = boxes.get(region)
        if box:
            x0, y0 = to_px(box[0], box[3])
            x1, y1 = to_px(box[2], box[1])
        else:
            xs = [r["px"] for r in rrows]; ys = [r["py"] for r in rrows]
            x0, x1 = min(xs), max(xs); y0, y1 = min(ys), max(ys)
        pad = 120
        x0 = max(0, int(x0 - pad)); y0 = max(0, int(y0 - pad))
        x1 = min(S, int(x1 + pad)); y1 = min(S, int(y1 + pad))
        if x1 - x0 < 200 or y1 - y0 < 200:
            cxm = (x0 + x1) // 2; cym = (y0 + y1) // 2
            x0, x1 = max(0, cxm - 400), min(S, cxm + 400)
            y0, y1 = max(0, cym - 400), min(S, cym + 400)

        crop = numbered_img.crop((x0, y0, x1, y1))
        w, h = crop.size
        crop = crop.resize((TILE_WIDTH, max(1, int(h * TILE_WIDTH / w))), Image.LANCZOS)
        rslug = region.replace(" ", "")
        path = os.path.join(out_dir, f"{rslug}_{slug}.jpg")
        crop.save(path, "JPEG", quality=88, optimize=True)

        with open(os.path.join(out_dir, f"{rslug}_coords.csv"), "w",
                  newline="", encoding="utf-8") as fh:
            w_ = csv.writer(fh)
            w_.writerow(["n", "region", "marker", "count", "game_x", "game_y", "refs"])
            for r in rrows:
                w_.writerow([r["n"], r["region"], r["marker"], r["count"],
                             round(r["x"], 1), round(r["y"], 1), " ".join(r["refs"])])
        made.append((region, path))
    return made


def render_interiors(pts, spaces, out_dir, slug, item_name):
    interiors = defaultdict(list)
    for p in pts:
        if p["space"] != APPALACHIA_SPACE:
            interiors[p["space"]].append(p)
    if not interiors:
        return []

    os.makedirs(out_dir, exist_ok=True)
    made, csv_rows = [], []
    for space_id, ipts in sorted(interiors.items(), key=lambda kv: -len(kv[1])):
        sp = spaces.get(space_id)
        if not sp:
            csv_rows.append([space_id, "(unknown space)", "", len(ipts), 0, "no Space row"])
            continue
        bg = os.path.join(MAPPALACHIA, "img", "cell", f"{sp['edid']}.jpg")
        if not os.path.exists(bg):
            csv_rows.append([sp["edid"], sp["name"], ipts[0]["region"], len(ipts), 0,
                             "no cell image in Mappalachia"])
            continue

        img = Image.open(bg).convert("RGB")
        if img.size != (S, S):
            img = img.resize((S, S), Image.LANCZOS)
        to_px = projector(sp)
        d = ImageDraw.Draw(img)

        drawn = 0
        for p in ipts:
            px, py = to_px(p["x"], p["y"])
            if -DOT_D <= px <= S + DOT_D and -DOT_D <= py <= S + DOT_D:
                draw_dot(d, px, py)
                drawn += 1
        draw_legend(img, f"{sp['name']}", [(item_name, len(ipts))])

        path = os.path.join(out_dir, f"{sp['edid']}_{slug}.jpg")
        if INTERIOR_SIZE != S:
            img = img.resize((INTERIOR_SIZE, INTERIOR_SIZE), Image.LANCZOS)
        img.save(path, "JPEG", quality=INTERIOR_QUALITY, optimize=True)
        made.append((sp["edid"], path))
        csv_rows.append([sp["edid"], sp["name"], ipts[0]["region"], len(ipts), drawn,
                         "" if drawn == len(ipts) else "some coords fell outside the cell image"])

    with open(os.path.join(out_dir, "interior_cells.csv"), "w",
              newline="", encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["cell_edid", "display_name", "region", "total_spawns", "markers_drawn", "note"])
        w_.writerows(csv_rows)
    return made


# ---------------------------------------------------------------- driver

def render_set(slug, source, out_root, conn, spaces, boxes, verbose=True):
    name, page_title, pts = load_points(slug, source)
    pts, dropped = apply_base_filter(slug, source, pts)

    ext = [p for p in pts if p["space"] == APPALACHIA_SPACE]
    to_px = projector(spaces[APPALACHIA_SPACE])
    rows = cluster_by_marker(ext, to_px)

    os.makedirs(out_root, exist_ok=True)
    bg = os.path.join(MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")

    p_plain = os.path.join(out_root, "01 Full Maps (4096)", f"{slug}.jpg")
    p_num = os.path.join(out_root, "02 Numbered Maps (4096)", f"{slug}_numbered.jpg")
    plain, numbered = render_exterior(bg, rows, name, p_plain, p_num)

    tiles = render_region_tiles(numbered, rows, boxes, to_px,
                                os.path.join(out_root, "03 Region Tiles"), slug)
    ints = render_interiors(pts, spaces, os.path.join(out_root, "04 Interior Maps"), slug, name)

    with open(os.path.join(out_root, f"{slug}_exterior_coords.csv"), "w",
              newline="", encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["n", "region", "marker", "count", "game_x", "game_y", "refs"])
        for r in rows:
            w_.writerow([r["n"], r["region"], r["marker"], r["count"],
                         round(r["x"], 1), round(r["y"], 1), " ".join(r["refs"])])

    if verbose:
        print(f"  {slug:28} ext={len(ext):5} markers={len(rows):4} "
              f"tiles={len(tiles):2} interiors={len(ints):3}"
              + (f" dropped={dropped}" if dropped else ""))
    return {"slug": slug, "markers": len(rows), "ext": len(ext),
            "tiles": len(tiles), "interiors": len(ints), "dropped": dropped}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set")
    ap.add_argument("--source", default="farming", choices=["farming", "nuka"])
    ap.add_argument("--out")
    ap.add_argument("--manifest", help="JSON list of {slug, source, out}")
    args = ap.parse_args()

    conn = _db()
    spaces = load_spaces(conn)
    boxes = region_bboxes(conn)

    jobs = []
    if args.manifest:
        jobs = json.load(open(args.manifest, encoding="utf-8"))
    elif args.set and args.out:
        jobs = [{"slug": args.set, "source": args.source, "out": args.out}]
    else:
        ap.error("give --set/--out or --manifest")

    for j in jobs:
        try:
            render_set(j["slug"], j.get("source", "farming"), j["out"], conn, spaces, boxes)
        except Exception as e:
            print(f"  !! {j['slug']}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
