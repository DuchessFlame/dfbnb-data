"""All-locations page maps (Duchess, 6 Nov 2026).
01 Location Maps/<id>.jpg   one per map marker: 1600x900, fixed zoom, lime ring round the
                            location, names on the locations inside an oval round it.
02 Region Maps/<region-slug>-map[-N].jpg   same box size, one fixed (wider) zoom for every
                            region; big regions split into sections; that region's
                            locations named. Every map watermarked (badge bottom-left).

ONE folder for live and PTS (Duchess, 9 Nov 2026). PTS (LOCATIONS_JSON=
dist/pts/locations/all_locations.json) draws NO duplicates of the live maps: only places
new on the PTS ("01 Location Maps/<id>.avif" — already waiting there when the update goes
live) and the regions whose markers changed ("02 Region Maps/<slug>-map[-N]-pts.jpg" —
"-pts" in the name, so they can never overwrite the live ones; each tile carries a "sig"
of its region's markers so the live build can use it once live matches). Each run writes
its tile list: data/locations/region_maps.json (live) or region_maps_pts.json (PTS).
Location maps are saved as AVIF (one-off for this page; region maps stay JPEG).
Cut places (taken out of the game) are not drawn on any map and get no new map."""
import os, sys, json, sqlite3, io, math, re
os.environ.setdefault("MAPPALACHIA_DIR", os.path.expanduser("~/mnt/Mappalachia"))
os.environ.setdefault("GUIDES_ROOT", os.path.expanduser("~/mnt/Guides and Stuff"))
REPO = os.path.expanduser("~/mnt/GitHub/dfbnb-data")
sys.path.insert(0, os.path.join(REPO, "src"))
import render_spawn_maps as rsm, map_watermark, cairosvg
from PIL import Image, ImageDraw

OUT = os.path.expanduser("~/mnt/Guides and Stuff/.Data Mining/All Locations")
OUT_W, OUT_H = 1600, 900
LOC_SRC_W, LOC_ICON = 900, 54          # location maps (the approved sample)
REG_SRC_W, REG_ICON = 2050, 34         # region maps: Savage Divide fits in 3 sections
LABEL_RX, LABEL_RY = 460, 240
CREAM, DARK, EDGE, LIME = (240, 232, 210), (20, 18, 14), (90, 84, 70), (166, 226, 46)

con = sqlite3.connect("file:%s?mode=ro" % rsm.MAPPALACHIA_DB, uri=True)
to_px = rsm.projector(rsm.load_spaces(con)[rsm.APPALACHIA_SPACE])
marks = con.execute("SELECT x, y, label, icon FROM MapMarker WHERE spaceFormID=?", [rsm.APPALACHIA_SPACE]).fetchall()
bg = Image.open(os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")).convert("RGB")
if bg.size != (rsm.S, rsm.S): bg = bg.resize((rsm.S, rsm.S), Image.LANCZOS)
_icons = {}
def icon(name, size):
    k = (name, size)
    if k not in _icons:
        svg = os.path.join(rsm.MAPPALACHIA, "img", "mapmarker", name + ".svg")
        if not os.path.exists(svg): _icons[k] = None
        else:
            im = Image.open(io.BytesIO(cairosvg.svg2png(url=svg, output_height=size * 4))).convert("RGBA")
            im.thumbnail((size, size), Image.LANCZOS); _icons[k] = im
    return _icons[k]
MARKS = [(to_px(x, y), lab, ic) for x, y, lab, ic in marks]

def base(cx, cy, src_w, isz):
    k = OUT_W / src_w; src_h = OUT_H / k
    x0 = int(min(max(cx - src_w / 2, 0), rsm.S - src_w)); y0 = int(min(max(cy - src_h / 2, 0), rsm.S - src_h))
    im = bg.crop((x0, y0, int(x0 + src_w), int(y0 + src_h))).resize((OUT_W, OUT_H), Image.LANCZOS)
    on = []
    for (px, py), lab, ic in MARKS:
        ox, oy = (px - x0) * k, (py - y0) * k
        i = icon(ic, isz)
        if i and -isz < ox < OUT_W + isz and -isz < oy < OUT_H + isz:
            im.paste(i, (int(ox - i.width / 2), int(oy - i.height / 2)), i)
            on.append((ox, oy, lab))
    return im, on, (lambda px, py: ((px - x0) * k, (py - y0) * k))

def labels(im, items, isz, font, fh, focus=None):
    d = ImageDraw.Draw(im); taken = [(ox - isz/2, oy - isz/2, ox + isz/2, oy + isz/2) for ox, oy, _ in items]
    def clash(b): return any(not (b[2] < t[0] or b[0] > t[2] or b[3] < t[1] or b[1] > t[3]) for t in taken)
    if focus: items = sorted(items, key=lambda n: (n[0]-focus[0])**2 + (n[1]-focus[1])**2)
    for ox, oy, lab in items:
        me = focus and abs(ox - focus[0]) < 2 and abs(oy - focus[1]) < 2
        g = (isz if me else isz / 2)
        w = d.textlength(lab, font=font); pad = max(5, fh // 4)
        opts = [(ox - w/2, oy + g + 6), (ox - w/2, oy - g - fh - 10), (ox + isz/2 + 8, oy - fh/2), (ox - isz/2 - 8 - w, oy - fh/2)]
        b = None
        for lx, ly in opts:
            bb = (lx - pad, ly - 4, lx + w + pad, ly + fh + 2)
            if bb[0] > 4 and bb[2] < im.width - 4 and bb[1] > 4 and bb[3] < im.height - 4 and not clash(bb):
                b = bb; break
        if b is None: continue          # no clear spot: leave this one unnamed rather than overlap
        taken.append(b)
        d.rounded_rectangle(b, radius=8, fill=DARK, outline=EDGE, width=1)
        d.text((b[0] + pad, b[1] + 4), lab, font=font, fill=CREAM)

def save(im, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    im = map_watermark.apply(im, corner="bottom-left").convert("RGB")
    if path.endswith(".avif"): im.save(path, "AVIF", quality=70)
    else: im.save(path, "JPEG", quality=90)

def slug(s): return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

data = json.load(open(os.path.join(REPO, os.environ.get("LOCATIONS_JSON", "dist/locations/all_locations.json"))))
# The icons and names drawn are the channel's own map markers, from the locations data
# (built from the game-file export), not Mappalachia's list: Mappalachia lags the game,
# so it still had Milepost Zero (taken out) and lacked the new PTS markers (Duchess,
# 7 Nov 2026). Mappalachia still supplies the background, projection and icon art.
MARKS = [(to_px(_l["x"], _l["y"]), _l["name"], _l["icon"])
         for _r in data["regions"] for _l in _r["locations"]
         if _l.get("x") is not None and _l.get("icon") and not _l.get("cut")]
# One folder for live and PTS (Duchess, 9 Nov 2026); PTS region tiles end "-pts".
IS_PTS = (data.get("channel") or "").lower() == "pts"
LOC_DIR, REG_DIR = "01 Location Maps", "02 Region Maps"
import hashlib
def region_sig(locs):   # same maths as build_locations_json.region_sig
    pts = sorted((l["name"], l["x"], l["y"]) for l in locs if l.get("x") is not None and not l.get("cut"))
    return hashlib.sha1(json.dumps(pts).encode("utf-8")).hexdigest()[:16]
SIGS = {_r["name"]: region_sig(_r["locations"]) for _r in data["regions"]}
def _marks_by_region(d):
    return {r["name"]: sorted((l["name"], l["x"], l["y"]) for l in r["locations"]
                              if l.get("x") is not None and not l.get("cut")) for r in d["regions"]}
if IS_PTS:
    live = json.load(open(os.path.join(REPO, "dist/locations/all_locations.json")))
    _lm, _pm = _marks_by_region(live), _marks_by_region(data)
    CHANGED = {r for r in _pm if _pm[r] != _lm.get(r)}
    LIVE_IDS = {l["id"] for r in live["regions"] for l in r["locations"]}
    print("PTS regions changed:", ", ".join(sorted(CHANGED)) or "none", flush=True)
tiles = []
only = set(sys.argv[1:])
LOCS_ALL = "locations" in only
only.discard("locations"); only.discard("--skip-existing")
F_LOC, F_REG = rsm._font(24), rsm._font(20)
done = 0
for r in data["regions"]:
    # Pitt / AC: hand-made maps. Cut places: not on the map any more. Places new this
    # season are named first, so they always get their label when space is tight.
    r = dict(r, locations=sorted((l for l in r["locations"] if l.get("x") is not None and not l.get("cut")),
                                 key=lambda l: not l.get("added")))
    if not r["locations"]:
        continue
    if ((not only and not LOCS_ALL) or "regions" in only) and (not IS_PTS or r["name"] in CHANGED):
        pts = [to_px(l["x"], l["y"]) for l in r["locations"]]
        pad = 90
        X0, X1 = min(p[0] for p in pts) - pad, max(p[0] for p in pts) + pad
        Y0, Y1 = min(p[1] for p in pts) - pad, max(p[1] for p in pts) + pad
        sw = REG_SRC_W; sh = sw * OUT_H / OUT_W
        cols, rows = max(1, math.ceil((X1 - X0) / sw)), max(1, math.ceil((Y1 - Y0) / sh))
        n = 0
        for ry in range(rows):
            for cx_ in range(cols):
                cx = X0 + (X1 - X0) * (cx_ + .5) / cols; cy = Y0 + (Y1 - Y0) * (ry + .5) / rows
                im, on, tr = base(cx, cy, sw, REG_ICON)
                mine = [tr(*p) + (l["name"],) for p, l in zip(pts, r["locations"])]
                mine = [m for m in mine if 0 <= m[0] <= OUT_W and 0 <= m[1] <= OUT_H]
                labels(im, mine, REG_ICON, F_REG, 22)
                n += 1
                name = slug(r["name"]) + "-map" + ("-%d" % n if cols * rows > 1 else "") + ("-pts" if IS_PTS else "") + ".jpg"
                save(im, os.path.join(OUT, REG_DIR, name))
                tiles.append(dict({"region": r["name"], "file": name, "part": n, "parts": cols * rows},
                                  **({"sig": SIGS[r["name"]]} if IS_PTS else {})))
        print(r["name"], "region maps:", cols * rows, flush=True)
    if only == {"regions"}: continue
    for l in r["locations"]:
        if only and not LOCS_ALL and l["id"] not in only: continue
        if l.get("cut") or (IS_PTS and l["id"] in LIVE_IDS): continue   # cut / live map already covers it
        dest = os.path.join(OUT, LOC_DIR, l["id"] + ".avif")
        if "--skip-existing" in sys.argv and os.path.exists(dest) and os.path.getsize(dest) > 1000: continue
        p = to_px(l["x"], l["y"])
        im, on, tr = base(p[0], p[1], LOC_SRC_W, LOC_ICON)
        tx, ty = tr(*p)
        near = [o for o in on if o[2] and ((o[0]-tx)/LABEL_RX)**2 + ((o[1]-ty)/LABEL_RY)**2 <= 1]
        d = ImageDraw.Draw(im); R = LOC_ICON * .95
        d.ellipse([tx-R-3, ty-R-3, tx+R+3, ty+R+3], outline=(15, 15, 15), width=13)
        d.ellipse([tx-R, ty-R, tx+R, ty+R], outline=LIME, width=7)
        labels(im, near, LOC_ICON, F_LOC, 26, focus=(tx, ty))
        save(im, dest)
        done += 1
        if done % 25 == 0: print("locations", done, flush=True)
print("DONE locations", done)

# Tile list for the page (build_locations_json.py reads it). Only rewritten when the
# region maps were drawn this run, and only for the regions drawn.
if tiles:
    lst = os.path.join(REPO, "data", "locations", "region_maps_pts.json" if IS_PTS else "region_maps.json")
    try: old = json.load(open(lst)).get("maps", [])
    except (OSError, ValueError): old = []
    drawn = {t["region"] for t in tiles}
    keep = [] if IS_PTS else [m for m in old if m["region"] not in drawn]
    json.dump({"_note": "Region map tiles drawn by src/render_all_locations_maps.py (1600x900, one zoom "
                        "for every region). Read by build_locations_json.py." +
                        (" PTS: only regions whose markers differ from live." if IS_PTS else ""),
               "maps": keep + tiles}, open(lst, "w"), indent=1)
    print("tile list ->", os.path.relpath(lst, REPO), len(keep + tiles), flush=True)
