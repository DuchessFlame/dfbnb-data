#!/usr/bin/env python3
r"""
render_slasher_maps.py - re-render the Slasher Update overview maps.

Replaces the lost ad-hoc script that produced the July 2026 set. Uses the SAME
coordinate transform as render_spawn_maps.py (straight from the Mappalachia DB
`Space` table), the same illustrated Appalachia background, and reproduces the
original legend/marker styling exactly:

  Pint-Sized Slasher Masks   amber circle   #FFC107   from the SlasherClue REFRs
  Disturbed Graves           magenta diamond #E040E0  from tsv/phantom_grave_sites.tsv

Outputs (under --out, default the ".Slasher Update" folder):
  01 Full Maps (4096)/slasher_graves.jpg   _masks.jpg   _combined.jpg
  02 Numbered Maps (4096)/<same>_numbered.jpg
  03 Region Tiles/<RegionSlug>_<set>.jpg  +  <RegionSlug>_<set>_coords.csv
  <set>_coords.csv

Usage:
  python render_slasher_maps.py --sets graves
  python render_slasher_maps.py --sets graves masks combined --out "D:\...\.Slasher Update"

Env:
  MAPPALACHIA_DIR / MAPPALACHIA_DB
"""

import argparse, csv, glob, os, re, sqlite3, sys
from collections import defaultdict, OrderedDict

from PIL import Image, ImageDraw, ImageFont

Image.MAX_IMAGE_PIXELS = None

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

MAPPALACHIA = os.environ.get("MAPPALACHIA_DIR", r"D:\Mappalachia")
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB",
                                os.path.join(MAPPALACHIA, "data", "mappalachia.db"))

DEFAULT_OUT = os.environ.get(
    "SLASHER_OUT",
    os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff", ".Slasher Update"))

S = 4096
APPALACHIA_SPACE = 2480661

MASK_BASE_FORMID = "008E069E"      # SDOW_SlasherClue
GRAVE_TSV = os.path.join(REPO, "tsv", "phantom_grave_sites.tsv")

DOT_D = 36                          # marker size in px at S=4096 (matches the July set)
DOT_OUTLINE = (18, 18, 18)
DOT_OUTLINE_W = 3

MASK_FILL = (255, 193, 7)           # amber
GRAVE_FILL = (224, 64, 224)         # magenta

LEGEND_PAD = 40
LEGEND_BG = (24, 24, 24)
LEGEND_BORDER = (255, 255, 255)
LEGEND_BORDER_W = 3

TILE_WIDTH = 2600

# Region tile filenames from the original July 2026 set (kept so a re-render
# overwrites the existing tiles instead of leaving both spellings behind).
TILE_SLUGS = {"Forest": "TheForest"}

FONT_PATH = os.path.join(MAPPALACHIA, "font", "futura_condensed_bold.otf")
FONT_FALLBACK = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

SETS = {
    "graves": {
        "title": "Disturbed Graves",
        "layers": ["graves"],
    },
    "masks": {
        "title": "Pint-Sized Slasher Masks",
        "layers": ["masks"],
    },
    "combined": {
        "title": "Slasher Event \u2014 Masks & Graves",
        "layers": ["masks", "graves"],
    },
}

LAYERS = {
    "masks": {"label": "Pint-Sized Slasher Masks", "fill": MASK_FILL, "shape": "circle"},
    "graves": {"label": "Disturbed Graves", "fill": GRAVE_FILL, "shape": "diamond"},
}


# ---------------------------------------------------------------- fonts

def _font(size):
    for p in (FONT_PATH, FONT_FALLBACK):
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


# ---------------------------------------------------------------- db / geometry

def _db():
    return sqlite3.connect(f"file:{MAPPALACHIA_DB}?mode=ro&immutable=1", uri=True)


def load_space(conn, space_id=APPALACHIA_SPACE):
    cx, cy, rng = conn.execute(
        "SELECT centerX, centerY, maxRange FROM Space WHERE spaceFormID=?", (space_id,)
    ).fetchone()
    return {"cx": cx, "cy": cy, "range": rng}


def projector(space):
    cx, cy, rng = space["cx"], space["cy"], space["range"]
    def to_px(x, y):
        return ((x - cx) / rng * S + S / 2.0,
                S / 2.0 - (y - cy) / rng * S)
    return to_px


REGION_FAMILIES = {
    "Forest": "Forest", "Mountains": "Savage Divide", "Swamp": "The Mire",
    "Cranberry": "Cranberry Bog", "Toxic": "Toxic Valley", "Ash": "Ash Heap",
}


def region_bboxes(conn):
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


# ---------------------------------------------------------------- drawing

def draw_marker(d, px, py, fill, shape="circle"):
    r = DOT_D / 2.0
    if shape == "diamond":
        d.polygon([(px, py - r), (px + r, py), (px, py + r), (px - r, py)],
                  fill=fill, outline=DOT_OUTLINE, width=DOT_OUTLINE_W)
    else:
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
    """rows = [(label, count, fill, shape)]"""
    d = ImageDraw.Draw(img)
    f_title = _font(96)
    f_row = _font(72)

    tw = d.textlength(title, font=f_title)
    rw = max([d.textlength(f"{lbl}  ({cnt})", font=f_row) for lbl, cnt, _, _ in rows] or [0])
    box_w = int(max(tw + 64, rw + 152) + 64)
    box_h = int(96 + 26 + len(rows) * 84 + 24)

    x0, y0 = LEGEND_PAD, LEGEND_PAD
    d.rectangle([x0, y0, x0 + box_w, y0 + box_h], fill=LEGEND_BG,
                outline=LEGEND_BORDER, width=LEGEND_BORDER_W)
    d.text((x0 + 32, y0 + 6), title, font=f_title, fill=(255, 255, 255))

    y = y0 + 96 + 26
    for lbl, cnt, fill, shape in rows:
        draw_marker(d, x0 + 48, y + 30, fill, shape)
        d.text((x0 + 88, y), f"{lbl}  ({cnt})", font=f_row, fill=(230, 230, 230))
        y += 84
    return img


# ---------------------------------------------------------------- data loading

def load_graves():
    """[{ref, region, marker, x, y}] straight from the rebuilt grave TSV."""
    if not os.path.exists(GRAVE_TSV):
        raise SystemExit(f"missing {GRAVE_TSV} - run build_phantom_grave_sites_tsv.py first")
    pts = []
    with open(GRAVE_TSV, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if not r.get("x") or not r.get("y"):
                continue
            pts.append({
                "ref": (r.get("ref_formid") or "").strip(),
                "region": (r.get("region") or "").strip() or "Unknown",
                "marker": (r.get("closest_fast_travel") or "").strip() or "Unmarked",
                "x": float(r["x"]), "y": float(r["y"]),
                "label": (r.get("ref_edid") or "").strip(),
            })
    return pts


def _newest_refr_placements(base_formid):
    paths = sorted(glob.glob(os.path.join(REPO, "tsv", "REFR_Placements_*.tsv")) +
                   glob.glob(os.path.join(REPO, "tsv", "pts", "REFR_Placements_*.tsv")))
    best = None
    for p in reversed(paths):
        with open(p, encoding="utf-8", errors="replace") as f:
            if "BaseFormID" not in f.readline():
                continue
            n = sum(1 for ln in f if base_formid.upper() in ln.upper().split("\t", 1)[0])
        if n and (best is None or n > best[1]):
            best = (p, n)
    return best[0] if best else None


def load_masks(rings=None, markers=None):
    """[{ref, region, marker, x, y}] for every placed SlasherClue."""
    path = _newest_refr_placements(MASK_BASE_FORMID)
    if not path:
        raise SystemExit("no REFR_Placements export containing base 008E069E was found")
    pts = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        for r in rd:
            if (r.get("BaseFormID") or "").upper() != MASK_BASE_FORMID:
                continue
            try:
                x, y = float(r.get("X") or 0), float(r.get("Y") or 0)
            except ValueError:
                continue
            if (r.get("WorldspaceEDID") or "").strip() not in ("", "Appalachia"):
                continue
            region = ""
            marker = ""
            if rings is not None:
                try:
                    import crossref_mappalachia_markers as xref
                    region = xref.region_for_xy(rings, x, y, nearest=True) or ""
                    if markers:
                        m = xref.nearest_marker(markers, x, y)
                        marker = (m[0] if isinstance(m, (list, tuple)) else m) or ""
                except Exception:
                    pass
            pts.append({"ref": (r.get("RefFormID") or "").strip(),
                        "region": region or "Unknown", "marker": marker or "Unmarked",
                        "x": x, "y": y, "label": (r.get("RefEDID") or "").strip()})
    return pts


# ---------------------------------------------------------------- render

def render_set(key, layer_pts, out_root, to_px, boxes):
    meta = SETS[key]
    bg = os.path.join(MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    base = Image.open(bg).convert("RGB")
    if base.size != (S, S):
        base = base.resize((S, S), Image.LANCZOS)

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    numbered_rows = []
    legend_rows = []
    for lname in meta["layers"]:
        L = LAYERS[lname]
        pts = layer_pts[lname]
        for p in pts:
            px, py = to_px(p["x"], p["y"])
            p["px"], p["py"] = px, py
            draw_marker(d, px, py, L["fill"], L["shape"])
        legend_rows.append((L["label"], len(pts), L["fill"], L["shape"]))
        numbered_rows.extend(sorted(pts, key=lambda r: (r["region"], r["marker"].lower())))

    draw_legend(plain, meta["title"], legend_rows)

    p_plain = os.path.join(out_root, "01 Full Maps (4096)", f"slasher_{key}.jpg")
    os.makedirs(os.path.dirname(p_plain), exist_ok=True)
    plain.save(p_plain, "JPEG", quality=88, optimize=True)

    numbered = plain.copy()
    dn = ImageDraw.Draw(numbered)
    f = _font(56)
    for i, r in enumerate(numbered_rows, 1):
        r["n"] = i
        draw_outlined_text(dn, (r["px"] + DOT_D, r["py"] - 34), str(i), f, w=3)
    p_num = os.path.join(out_root, "02 Numbered Maps (4096)", f"slasher_{key}_numbered.jpg")
    os.makedirs(os.path.dirname(p_num), exist_ok=True)
    numbered.save(p_num, "JPEG", quality=88, optimize=True)

    # region tiles
    tiles_dir = os.path.join(out_root, "03 Region Tiles")
    os.makedirs(tiles_dir, exist_ok=True)
    by_region = defaultdict(list)
    for r in numbered_rows:
        by_region[r["region"]].append(r)
    tiles = []
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
            cxm, cym = (x0 + x1) // 2, (y0 + y1) // 2
            x0, x1 = max(0, cxm - 400), min(S, cxm + 400)
            y0, y1 = max(0, cym - 400), min(S, cym + 400)
        crop = numbered.crop((x0, y0, x1, y1))
        w, h = crop.size
        crop = crop.resize((TILE_WIDTH, max(1, int(h * TILE_WIDTH / w))), Image.LANCZOS)
        rslug = TILE_SLUGS.get(region, region.replace(" ", ""))
        crop.save(os.path.join(tiles_dir, f"{rslug}_{key}.jpg"), "JPEG",
                  quality=88, optimize=True)
        tiles.append(region)

    # coords csv
    with open(os.path.join(out_root, f"slasher_{key}_coords.csv"), "w",
              newline="", encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["n", "region", "marker", "ref_formid", "ref_edid", "game_x", "game_y"])
        for r in numbered_rows:
            w_.writerow([r["n"], r["region"], r["marker"], r["ref"], r.get("label", ""),
                         round(r["x"], 1), round(r["y"], 1)])

    print(f"  slasher_{key:9} points={len(numbered_rows):4} tiles={len(tiles)}")
    return numbered_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sets", nargs="+", default=["graves", "masks", "combined"],
                    choices=list(SETS))
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    conn = _db()
    space = load_space(conn)
    to_px = projector(space)
    boxes = region_bboxes(conn)

    need = set()
    for k in args.sets:
        need.update(SETS[k]["layers"])

    rings = markers = None
    if "masks" in need:
        try:
            import crossref_mappalachia_markers as xref
            rings, markers = xref.load_mappalachia()[:2]
        except Exception as e:
            print(f"  (region cross-ref unavailable: {e})", file=sys.stderr)

    layer_pts = {}
    if "graves" in need:
        layer_pts["graves"] = load_graves()
    if "masks" in need:
        layer_pts["masks"] = load_masks(rings, markers)

    os.makedirs(args.out, exist_ok=True)
    for k in args.sets:
        render_set(k, {n: list(layer_pts[n]) for n in SETS[k]["layers"]},
                   args.out, to_px, boxes)


if __name__ == "__main__":
    main()
