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
  05 Chance Maps/<slug>_chance.jpg, <slug>_chance_numbered.jpg,
                 <region-slug>-chance-map.jpg  +  <RegionSlug>_coords.csv
  <slug>_exterior_coords.csv

01-04 are the GUARANTEED fixed spawns. 05 is the Chance to Spawn Locations expand's
shared-loot-pool points (spawn-guide 9k) - the place is real but the item is a roll,
so they get their own maps and never contaminate the fixed-spawn tiles. Those tiles
are named exactly as the site expects, so uploading is a straight drag into
/wp-content/uploads/guide-images/<category>/<item>/ (converted to .avif).

The 03 Region Tiles folder holds each tile TWICE: `<Region>_<slug>.jpg` for the
archive, and `<region-slug>-spawn-map.jpg` under the name the website expects, so the
"View {Region} spawn map →" link in Fixed Spawn Locations resolves once the file is
converted to .avif and dropped into guide-images/<category>/<item>/.

Sources (--source): the family whose doc + geo cache to read.
  farming   dist/farming_spawns/<slug>_spawns.json    regions[]
  nuka      dist/nuka_cola_spawns_<slug>.json         regions[]
  chainsaw  dist/chainsaws/<slug>.json                regions[]
  meat      dist/meat/<slug>.json                     fixed_spawns.regions[]
  plants    dist/plants/<slug>.json                   fixed_spawns.regions[]
  insects   dist/insects/<slug>.json                  fixed_spawns.regions[]
`doc_regions()` reads both shapes, so everything downstream is shape-agnostic.

Usage:
  python render_spawn_maps.py --set deathclaw-egg --out "<...>/.Farming - Eggs/Deathclaw Eggs"
  python render_spawn_maps.py --set angler --source meat --out "<...>/.Farming - Meat/Angler"
  python render_spawn_maps.py --manifest jobs.json

To render EVERY farming page in one go, use the driver instead:
  python src/render_all_maps.py
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

# ── what actually goes on a spawn map ───────────────────────────────────────
# A map answers "where do I walk to get this?", so it plots only the source types
# you can physically go and take. Vendor stock, chance container loot and quest
# hand-ins are real sources but they are NOT places to farm, and putting them on
# the map inflates the dot count and misleads the reader — exactly the failure
# that had the Mirelurk Egg map showing unharvestable clutches. They stay in the
# JSON (Vendors / Containers / Events & Activities expands) and off the picture.
EXCLUDE_TYPES = {"vendor", "container", "loot-list", "quest-reward",
                 "collectron", "resource-generator", "dispenser"}

# Friendly legend labels + a distinct dot colour per source type, so a Deathclaw
# nest never reads as a loose egg. Anything unlisted falls back to amber.
TYPE_LABELS = {
    "direct": "Loose spawn",
    "static": "Fixed spawn",
    "static-raw": "Raw egg (fixed)",
    "static-cracked": "Cracked egg (fixed)",
    "nest": "Nest",
    "harvestable": "Harvestable",
    "flora": "Flora",
    "enlightened-flora": "Enlightened flora",
    "npc": "Creature",
    # Weapon pages (spawns_engine.classify.weapon_classify): a wall rack / gun rack
    # is a fixed world point that hands you the weapon, so it IS a place to walk to
    # and belongs on the map — unlike a vendor or a chance container.
    "machine": "Weapon rack",
    # Shared-loot-pool points (spawn-guide 9k). The place is real, the item is a roll,
    # so they get their OWN maps — never mixed into the fixed-spawn tiles.
    "chance": "Chance spawn",
    # Creature pages (meat / insects / cryptids). Without these the legend fell back
    # to a capitalised source_type and read "Placement (53)" on the Radstag map.
    "placement": "Fixed spawn",
    "spawn": "Spawn point",
    "ambush": "Ambush",
}
TYPE_COLOURS = {
    "direct": (255, 193, 7),            # amber
    "static": (255, 193, 7),
    "static-raw": (255, 193, 7),
    "static-cracked": (255, 138, 30),   # orange
    "nest": (139, 92, 246),             # violet
    "harvestable": (255, 193, 7),
    "flora": (76, 217, 100),            # green
    "enlightened-flora": (0, 209, 255), # cyan
    "npc": (244, 67, 54),               # red
    "machine": (0, 209, 255),           # cyan
    "chance": (120, 160, 255),          # pale blue — reads as "maybe", not "go here"
    "placement": (244, 67, 54),         # red, same as npc — it IS a creature
    "spawn": (255, 138, 30),            # orange — a spawn point, not the creature itself
    "ambush": (139, 92, 246),           # violet
}


def type_label(stype):
    return TYPE_LABELS.get(stype, (stype or "Spawn").replace("-", " ").capitalize())


def type_colour(stype):
    return TYPE_COLOURS.get(stype, DOT_FILL)

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


# Region editor-ID family -> the display name the spawn docs use.
#
# TWO naming schemes exist and BOTH are matched, because which one a Mappalachia DB
# carries depends on the build: the older `LocRegion<Family>` tilings, and the
# current `<Family>SubRegion##` ones. Matching only LocRegion* silently produced an
# EMPTY box table, and every region tile then fell back to a padded box around the
# item's own dots — a 3-dot region cropped to a few hundred px of dirt instead of the
# region. If a whole run's tiles look zoomed in, check this table first.
#
# Ash Heap is `MountainRemoval` (the strip mines) and Skyline Valley is `Storm`;
# neither is guessable from the display name, so never "simplify" this map.
REGION_FAMILIES = {
    # LocRegion<Family>
    "Forest": "Forest", "Mountains": "Savage Divide", "Swamp": "The Mire",
    "Cranberry": "Cranberry Bog", "Toxic": "Toxic Valley", "Ash": "Ash Heap",
    # <Family>SubRegion##
    "Mountain": "Savage Divide", "MountainRemoval": "Ash Heap",
    "ToxicValley": "Toxic Valley", "BurningSprings": "Burning Springs",
    "Storm": "Skyline Valley",
}
# Regions with no tiling in the DB at all (instanced worldspaces). These fall back to
# a padded bounding box around their own spawn points, which is the right answer for
# a space that has no Appalachian extent.
NO_POLYGON_REGIONS = {"Atlantic City", "The Pitt"}

_REGION_EDID_RE = re.compile(r"^(?:LocRegion([A-Za-z]+)|([A-Za-z]+)SubRegion\d)")


def region_bboxes(conn):
    """Region display name -> (minx, miny, maxx, maxy) from the region tilings."""
    fam_ids = defaultdict(list)
    for fid, eid in conn.execute("SELECT regionFormID, regionEditorID FROM Region"):
        m = _REGION_EDID_RE.match(eid or "")
        if not m:
            continue
        fam = m.group(1) or m.group(2)
        if fam in REGION_FAMILIES:
            fam_ids[REGION_FAMILIES[fam]].append(int(fid))
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
    """rows = [(label, count)] or [(label, count, colour)]. Black box, 3px white
    border, swatch + text per row. The swatch uses the row's own colour so the
    legend and the dots on the map always agree."""
    d = ImageDraw.Draw(img)
    f_title = _font(96)
    f_row = _font(72)

    rows = [(r[0], r[1], r[2] if len(r) > 2 else DOT_FILL) for r in rows]
    tw = d.textlength(title, font=f_title)
    rw = max([d.textlength(f"{lbl}  ({cnt})", font=f_row) for lbl, cnt, _ in rows] or [0])
    box_w = int(max(tw + 64, rw + 152) + 64)
    box_h = int(96 + 26 + len(rows) * 84 + 24)

    x0, y0 = LEGEND_PAD, LEGEND_PAD
    x1, y1 = x0 + box_w, y0 + box_h
    d.rectangle([x0, y0, x1, y1], fill=LEGEND_BG,
                outline=LEGEND_BORDER, width=LEGEND_BORDER_W)
    d.text((x0 + 32, y0 + 6), title, font=f_title, fill=(255, 255, 255))

    y = y0 + 96 + 26
    for lbl, cnt, colour in rows:
        draw_dot(d, x0 + 48, y + 30, fill=colour)
        d.text((x0 + 88, y), f"{lbl}  ({cnt})", font=f_row, fill=(230, 230, 230))
        y += 84
    return img


# ---------------------------------------------------------------- data loading

def _slug_us(slug):
    return slug.replace("-", "_")


# slug -> (doc path, geo cache path), per family.
#
# Two doc shapes exist and `doc_regions()` reads both:
#   A. regions[] at the top level            — farming, nuka, chainsaw
#   B. fixed_spawns.regions[]                — meat, plants, insects
# Under either, a region is {region, locations[]} and a location is
# {marker, coords, spawns[]}, so everything below this point is shape-agnostic.
SOURCES = {
    "farming":  lambda slug: (
        os.path.join(REPO, "dist", "farming_spawns", f"{slug}_spawns.json"),
        os.path.join(REPO, "data", "farming_spawns", f"geo_cache_{_slug_us(slug)}.json")),
    "nuka":     lambda slug: (
        os.path.join(REPO, "dist", f"nuka_cola_spawns_{slug}.json"),
        os.path.join(REPO, "data", "nuka_cola_spawns", "geo_cache.json")),
    "chainsaw": lambda slug: (
        os.path.join(REPO, "dist", "chainsaws", f"{slug}.json"),
        os.path.join(REPO, "data", "chainsaw_spawns", "geo_cache.json")),
    "meat":     lambda slug: (
        os.path.join(REPO, "dist", "meat", f"{slug}.json"),
        os.path.join(REPO, "data", "meat_spawns", f"{slug}.json")),
    "plants":   lambda slug: (
        os.path.join(REPO, "dist", "plants", f"{slug}.json"),
        os.path.join(REPO, "data", "plant_spawns", f"{slug}.json")),
    "insects":  lambda slug: (
        os.path.join(REPO, "dist", "insects", f"{slug}.json"),
        os.path.join(REPO, "data", "insect_spawns", f"{slug}.json")),
}


def source_paths(slug, source):
    fn = SOURCES.get(source)
    if not fn:
        raise SystemExit(f"unknown source {source!r}; expected one of {sorted(SOURCES)}")
    return fn(slug)


def doc_regions(data):
    """The region list, whichever shape the family writes it in (see SOURCES)."""
    if isinstance(data.get("regions"), list):
        return data["regions"]
    fs = data.get("fixed_spawns")
    if isinstance(fs, dict) and isinstance(fs.get("regions"), list):
        return fs["regions"]
    return []


def load_geo(geo_p):
    """Geo cache as {decimal-ref-string: row}.

    The farming / nuka caches are keyed by the bare decimal ref. The meat, plant and
    insect caches are keyed `<page>:<decimal ref>` because one file holds one page,
    so the suffix is indexed as well and both shapes look the same from here."""
    if not os.path.exists(geo_p):
        return {}
    raw = json.load(open(geo_p, encoding="utf-8"))
    out = {}
    for k, v in raw.items():
        out[str(k)] = v
        if ":" in str(k):
            out[str(k).rsplit(":", 1)[1]] = v
    return out


def _marker_type(loc):
    """The marker's dominant source type — used for a ref recovered from the
    marker-level list, which carries no type of its own. Keeps the dot colour and
    legend honest on dense pages."""
    srcs = loc.get("sources") or {}
    return max(srcs, key=srcs.get) if srcs else ""


def load_points(slug, source):
    """
    -> (name, page_title, points)
    points = [ {ref, space, x, y, region, marker} ]  one row per spawn instance
    """
    dist, geo_p = source_paths(slug, source)
    if not os.path.exists(dist):
        raise SystemExit(f"no doc at {dist} — build the page first.")

    data = json.load(open(dist, encoding="utf-8"))
    geo = load_geo(geo_p)

    pts = []
    for reg in doc_regions(data):
        for loc in reg.get("locations", []):
            # A DENSE page keeps only the spawn entries that carry authored photos —
            # the blank placeholders are stripped at build time because the renderer
            # collapses to one marker-level slot anyway (spawns_engine.build
            # .compact_spawns). The marker-level `refs` are always complete, so fall
            # back to those and every point still lands on the map. Without this the
            # 13,342-point Blackberries map would draw nothing.
            entries = loc.get("spawns") or []
            if len(entries) < len(loc.get("refs") or []):
                have = {e.get("ref") for e in entries}
                entries = list(entries) + [{"ref": r} for r in loc["refs"]
                                           if r not in have]
            for sp in entries:
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
                    "source_type": sp.get("source_type") or _marker_type(loc),
                })
    return data.get("name", slug), data.get("page_title", slug), pts


def filter_mappable(pts):
    """Drop the source types that are sources but not PLACES (see EXCLUDE_TYPES).
    Returns (kept, {dropped_type: n})."""
    kept, dropped = [], defaultdict(int)
    for p in pts:
        if p.get("source_type") in EXCLUDE_TYPES:
            dropped[p["source_type"]] += 1
            continue
        kept.append(p)
    return kept, dict(dropped)


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
    _, geo_p = source_paths(slug, source)
    geo = load_geo(geo_p)
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
    """Group exterior spawns by map marker AND source type, so a dot = one kind of
    thing at one location. Splitting on source type is what lets a Deathclaw nest
    carry its own colour and its own count instead of being averaged into the loose
    eggs at the same POI."""
    groups = OrderedDict()
    for p in pts:
        key = (p["region"], p["marker"] or "Unmarked", p.get("source_type", ""))
        g = groups.setdefault(key, {"pts": [], "refs": []})
        g["pts"].append((p["x"], p["y"]))
        g["refs"].append(p["ref"])
    rows = []
    for (region, marker, stype), g in groups.items():
        xs = [a for a, _ in g["pts"]]
        ys = [b for _, b in g["pts"]]
        cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
        px, py = to_px(cx, cy)
        rows.append({"region": region, "marker": marker, "source_type": stype,
                     "count": len(g["pts"]), "x": cx, "y": cy, "px": px, "py": py,
                     "refs": g["refs"]})
    rows.sort(key=lambda r: (r["region"], r["marker"], r["source_type"]))
    for i, r in enumerate(rows, 1):
        r["n"] = i
    return rows


def legend_rows(rows):
    """Per-source-type totals for the legend, biggest bucket first."""
    tally = defaultdict(int)
    for r in rows:
        tally[r.get("source_type", "")] += r["count"]
    return [(type_label(t), n, type_colour(t))
            for t, n in sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))]


def render_exterior(bg_path, rows, title, out_plain, out_numbered):
    base = Image.open(bg_path).convert("RGB")
    if base.size != (S, S):
        base = base.resize((S, S), Image.LANCZOS)

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    for r in rows:
        draw_dot(d, r["px"], r["py"], fill=type_colour(r.get("source_type", "")))
    draw_legend(plain, title, legend_rows(rows))
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


def render_region_tiles(numbered_img, rows, boxes, to_px, out_dir, slug,
                        name_fn=None, also_name_fn=None):
    """Crop one tile per region out of the numbered map.

    `name_fn` names the tile; `also_name_fn` saves a SECOND copy under the name the
    website expects, so uploading is a straight drag into the item's guide-images
    folder. The fixed-spawn tiles keep their working `<Region>_<slug>.jpg` name for
    the archive AND get `<region-slug>-spawn-map.jpg` for the site — the sibling of
    the chance tiles' `<region-slug>-chance-map.jpg`."""
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
        path = os.path.join(out_dir, name_fn(region) if name_fn else f"{rslug}_{slug}.jpg")
        crop.save(path, "JPEG", quality=88, optimize=True)
        if also_name_fn:
            crop.save(os.path.join(out_dir, also_name_fn(region)),
                      "JPEG", quality=88, optimize=True)

        with open(os.path.join(out_dir, f"{rslug}_coords.csv"), "w",
                  newline="", encoding="utf-8") as fh:
            w_ = csv.writer(fh)
            w_.writerow(["n", "region", "marker", "source_type", "count",
                         "game_x", "game_y", "refs"])
            for r in rrows:
                w_.writerow([r["n"], r["region"], r["marker"],
                             r.get("source_type", ""), r["count"],
                             round(r["x"], 1), round(r["y"], 1), " ".join(r["refs"])])
        made.append((region, path))
    return made


def _region_slug(name):
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-", str(name or "").lower()).lstrip("-"))


def load_chance_points(slug, source):
    """Points for the Chance to Spawn maps, from the doc's chance_spawns refs.

    These are the shared-loot-pool placements (spawn-guide 9k) — the place is real
    but the item is a roll, so they are NEVER drawn on the fixed-spawn tiles. The
    page lists them by name only and links out to these maps, one per ITEM per
    region: a shared blank region tile would have to carry every farming page's
    points at once, which is useless."""
    dist, geo_p = source_paths(slug, source)
    if not os.path.exists(dist):
        return "", []
    data = json.load(open(dist, encoding="utf-8"))
    geo = load_geo(geo_p)
    pts = []
    # Only the families whose chance_spawns carry `regions[].markers[].refs` can be
    # drawn. Meat / plants / insects list chance points as a flat `locations[]` with
    # no refs, so there is nothing to resolve through the geo cache and they get no
    # chance maps — that is a data gap, not a render failure.
    for reg in (data.get("chance_spawns") or {}).get("regions", []):
        for m in reg.get("markers", []):
            name = m.get("name", "") if isinstance(m, dict) else str(m)
            for ref in (m.get("refs") or []) if isinstance(m, dict) else []:
                g = geo.get(str(int(ref, 16)))
                if not g:
                    continue
                pts.append({"ref": ref, "space": int(g["space"]),
                            "x": float(g["x"]), "y": float(g["y"]),
                            "region": reg.get("region", ""), "marker": name,
                            "label": "", "source_type": "chance"})
    return data.get("name", slug), pts


def render_chance(slug, source, out_root, spaces, boxes):
    """Per-item chance maps: one full map + one tile per region.

    Tiles are named EXACTLY as the site expects them, so uploading is a straight
    drag into the item's guide-images folder:
        <region-slug>-chance-map.jpg   ->   .../guide-images/<cat>/<item>/<same>.avif
    """
    name, pts = load_chance_points(slug, source)
    ext = [p for p in pts if p["space"] == APPALACHIA_SPACE]
    if not ext:
        return []

    out_dir = os.path.join(out_root, "05 Chance Maps")
    os.makedirs(out_dir, exist_ok=True)
    to_px = projector(spaces[APPALACHIA_SPACE])
    rows = cluster_by_marker(ext, to_px)
    bg = os.path.join(MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")

    _plain, numbered = render_exterior(
        bg, rows, f"{name} — chance spawns",
        os.path.join(out_dir, f"{slug}_chance.jpg"),
        os.path.join(out_dir, f"{slug}_chance_numbered.jpg"))

    tiles = render_region_tiles(numbered, rows, boxes, to_px, out_dir, slug,
                                name_fn=lambda r: f"{_region_slug(r)}-chance-map.jpg")
    return tiles


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
                draw_dot(d, px, py, fill=type_colour(p.get("source_type", "")))
                drawn += 1
        tally = defaultdict(int)
        for p in ipts:
            tally[p.get("source_type", "")] += 1
        draw_legend(img, f"{sp['name']}",
                    [(f"{item_name} — {type_label(t)}", n, type_colour(t))
                     for t, n in sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))])

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
    pts, off_map = filter_mappable(pts)

    ext = [p for p in pts if p["space"] == APPALACHIA_SPACE]
    to_px = projector(spaces[APPALACHIA_SPACE])
    rows = cluster_by_marker(ext, to_px)

    os.makedirs(out_root, exist_ok=True)
    bg = os.path.join(MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")

    p_plain = os.path.join(out_root, "01 Full Maps (4096)", f"{slug}.jpg")
    p_num = os.path.join(out_root, "02 Numbered Maps (4096)", f"{slug}_numbered.jpg")
    plain, numbered = render_exterior(bg, rows, name, p_plain, p_num)

    tiles = render_region_tiles(numbered, rows, boxes, to_px,
                                os.path.join(out_root, "03 Region Tiles"), slug,
                                also_name_fn=lambda r: f"{_region_slug(r)}-spawn-map.jpg")
    ints = render_interiors(pts, spaces, os.path.join(out_root, "04 Interior Maps"), slug, name)
    # Chance to Spawn maps — their own files, never mixed into the fixed-spawn tiles.
    chance = render_chance(slug, source, out_root, spaces, boxes)

    with open(os.path.join(out_root, f"{slug}_exterior_coords.csv"), "w",
              newline="", encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["n", "region", "marker", "source_type", "count",
                     "game_x", "game_y", "refs"])
        for r in rows:
            w_.writerow([r["n"], r["region"], r["marker"], r.get("source_type", ""),
                         r["count"], round(r["x"], 1), round(r["y"], 1),
                         " ".join(r["refs"])])

    if verbose:
        off = ("  off-map=" + ",".join(f"{k}:{v}" for k, v in sorted(off_map.items()))
               if off_map else "")
        print(f"  {slug:28} ext={len(ext):5} markers={len(rows):4} "
              f"tiles={len(tiles):2} chance-tiles={len(chance):2} interiors={len(ints):3}"
              + (f" dropped={dropped}" if dropped else "") + off)
    return {"slug": slug, "markers": len(rows), "ext": len(ext),
            "tiles": len(tiles), "chance_tiles": len(chance),
            "interiors": len(ints), "dropped": dropped,
            "off_map": off_map}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set")
    ap.add_argument("--source", default="farming", choices=sorted(SOURCES))
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
