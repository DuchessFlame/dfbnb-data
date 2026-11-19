#!/usr/bin/env python3
r"""
render_hideout_cache_maps.py - numbered Hideout Cache maps, one per floor, drawn
straight from the game files (19 Nov 2026).

Mappalachia has no cell images for the Hideout interiors (PTS-only), so the floor
plan is drawn from SeventySix.esm itself: every placed STAT / SCOL / MSTT / FURN /
CONT / DOOR / ACTI / TACT in the Hideout's interior cell, as its object bounds
(OBND x ref scale), rotated by the ref's Z rotation and seen from above.
  * flat pieces (under 80 units tall)  -> floor fill
  * thin pieces (under 180 units wide) -> wall fill
  * everything else                    -> outline only, so room shells and
                                          static collections don't hide the walls
  * effects with broken / huge bounds (any side over 6000 units, MSTT over 2000)
    are skipped
A floor is a slice of the cell around the cache height (objects whose height
range overlaps cache z - 60 .. cache z + 170). Caches within 200 units of height
share a floor. Caches are numbered 1-4 per Hideout, lowest floor first, then
west to east.

Cache spots = every placed ref of HIDE_BonusContainer_Easy (the Challenging /
Lethal / Marked copies sit on the same spots), found with esm_refs. Hideout cells
come from HIDEHideoutDataFormList (each data list's LCTN -> its interior cell via
the cache refs themselves). Nothing is typed per Hideout.

House style: render_spawn_maps dots / labels / legend + map_watermark.

Usage (on the PC, from dfbnb-data):
  python src\render_hideout_cache_maps.py            (PTS install, default)
  python src\render_hideout_cache_maps.py --live
  --data <Data folder>   --out <folder>
Default --out: Guides and Stuff\.Hideouts\Hideout Cache Maps
Upload to /wp-content/uploads/guide-images/hideouts/hideouts-guide/
"""
import argparse, math, os, struct, sys
HERE = os.path.dirname(os.path.abspath(__file__)); REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(REPO, "tools"))
from PIL import Image, ImageDraw
import esm_reader as R
import esm_refs
import render_spawn_maps as rsm
import map_watermark

DATA = {"pts": r"D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data",
        "live": r"D:\steamlibrary\steamapps\common\Fallout76\Data"}
DEFAULT_OUT = os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff", ".Hideouts", "Hideout Cache Maps")
CACHE_EDID = "HIDE_BonusContainer_Easy"
BASE_SIGS = ["STAT", "SCOL", "MSTT", "FURN", "CONT", "DOOR", "ACTI", "TACT"]
DOT_D = 60
LEGEND_BAND = 230      # px kept clear at the top for the title box


def slug(t):
    import re
    return re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--data", default=None)
    ap.add_argument("--out", default=DEFAULT_OUT)
    a = ap.parse_args()
    esm = R.Esm(a.data or DATA["live" if a.live else "pts"], BASE_SIGS)
    cache_base = next((f for f in esm.index if esm.edid_of(f) == CACHE_EDID), None)
    if cache_base is None:
        print(f"[cache-maps] no {CACHE_EDID} in this ESM - nothing to draw."); return 0
    refs, _ = esm_refs.referenced_by(esm, {cache_base}, referrers={"REFR"})
    spots = {}
    esm.prefetch_world(refs.get(cache_base, []))
    for f in refs.get(cache_base, []):
        sig, rec, cell, _w = esm.world_record(f)
        if cell is None:
            continue
        x, y, z = struct.unpack_from("<3f", rec.first("DATA"), 0)
        spots.setdefault(cell, []).append((x, y, z))
    # Hideout name = the cell's FULL. The quest's pack-in storage cell and test
    # cells (all-caps placeholder names) are skipped.
    cells = {}
    for c, pts in spots.items():
        crec = esm.world_record(c)[1]
        name = esm.lstring(crec.first("FULL")) if crec.first("FULL") else ""
        if not name or name.isupper() or len(pts) < 2:
            continue
        cells[c] = (name, pts)
    want = [f for f, (s, o, cell, w) in esm._world.items() if cell in cells and s == "REFR"]
    esm.prefetch_world(want)
    bounds, geo = {}, {c: [] for c in cells}
    for f in want:
        r = esm._world_cache[f]; nm, d = r.first("NAME"), r.first("DATA")
        if not nm or not d:
            continue
        base = struct.unpack_from("<I", nm, 0)[0]
        if base not in bounds:
            br = esm.get(base); ob = br.first("OBND") if br else None
            bounds[base] = (struct.unpack_from("<6h", ob, 0) if ob else None, esm.sig_of(base) or "")
        b, sig = bounds[base]
        if not b or sig not in BASE_SIGS:
            continue
        x, y, z, _rx, _ry, rz = struct.unpack_from("<6f", d, 0)
        sc = r.first("XSCL"); s = struct.unpack_from("<f", sc, 0)[0] if sc else 1.0
        geo[esm._world[f][2]].append((x, y, z, rz, s, b, sig))

    os.makedirs(a.out, exist_ok=True)
    rsm.DOT_D = DOT_D
    for c, (name, pts) in sorted(cells.items(), key=lambda kv: kv[1][0]):
        cs = sorted(pts, key=lambda p: (p[2], p[0]))
        floors = []
        for i, p in enumerate(cs, 1):
            for fl in floors:
                if abs(fl["z"] - p[2]) < 200:
                    fl["c"].append((i, p)); break
            else:
                floors.append({"z": p[2], "c": [(i, p)]})
        for fi, fl in enumerate(floors, 1):
            draw_floor(name, geo[c], fl, fi, len(floors), a.out)
    return 0


def draw_floor(name, objs, fl, fi, nfl, out):
    cz = min(p[2] for _, p in fl["c"])
    polys = []
    for x, y, z, rz, s, (x1, y1, z1, x2, y2, z2), sig in objs:
        w, h, t = (x2 - x1) * s, (y2 - y1) * s, (z2 - z1) * s
        if max(w, h, t) > 6000 or (sig == "MSTT" and max(w, h) > 2000) or (w < 24 and h < 24):
            continue
        if z + z2 * s < cz - 60 or z + z1 * s > cz + 170:
            continue
        co, si = math.cos(rz), math.sin(rz)
        pts = [(x + s * (px * co + py * si), y + s * (-px * si + py * co)) for px, py in ((x1, y1), (x2, y1), (x2, y2), (x1, y2))]
        polys.append(("floor" if t < 80 else ("wall" if min(w, h) < 180 else "bulk"), pts))
    xs = sorted(p[0] for _, pp in polys for p in pp); ys = sorted(p[1] for _, pp in polys for p in pp)
    q = lambda arr, f: arr[int(f * (len(arr) - 1))]
    X0, X1, Y0, Y1 = q(xs, .02) - 300, q(xs, .98) + 300, q(ys, .02) - 300, q(ys, .98) + 300
    for _, p in fl["c"]:
        X0, X1, Y0, Y1 = min(X0, p[0] - 700), max(X1, p[0] + 700), min(Y0, p[1] - 700), max(Y1, p[1] + 700)
    W = rsm.TILE_WIDTH; k = W / (X1 - X0); H = int((Y1 - Y0) * k)
    if H > W * 1.4:
        k = W * 1.4 / (Y1 - Y0); H = int(W * 1.4); W = int((X1 - X0) * k)
    H += LEGEND_BAND
    img = Image.new("RGB", (W, H), (22, 24, 27))
    lay = Image.new("RGBA", (W, H), (0, 0, 0, 0)); d = ImageDraw.Draw(lay)
    tp = lambda X, Y: ((X - X0) * k, (Y1 - Y) * k + LEGEND_BAND)
    for kind, pts in sorted(polys, key=lambda p: {"floor": 0, "bulk": 1, "wall": 2}[p[0]]):
        P = [tp(*p) for p in pts]
        if kind == "floor":
            d.polygon(P, fill=(92, 98, 106, 255))
        elif kind == "bulk":
            d.polygon(P, outline=(150, 156, 164, 200), width=2)
        else:
            d.polygon(P, fill=(214, 218, 224, 255))
    img.paste(lay, (0, 0), lay)
    dd = ImageDraw.Draw(img)
    rows = [{"n": i, "px": tp(p[0], p[1])[0], "py": tp(p[0], p[1])[1]} for i, p in fl["c"]]
    for r in rows:
        rsm.draw_dot(dd, r["px"], r["py"])
    f = rsm._font(72)
    rsm.place_labels(dd, rows, f, gap=rsm.DOT_D * 0.6 + 8)
    for r in rows:
        rsm.draw_outlined_text(dd, r["label_xy"], str(r["n"]), f, w=4)
    lvl = "" if nfl == 1 else f" - Level {fi} of {nfl}"
    rsm.draw_legend(img, f"{name}{lvl}", [("Hideout Cache", len(rows))])
    img = map_watermark.apply(img, corner="bottom-left")
    fn = f"{slug(name)}-caches" + ("" if nfl == 1 else f"-level-{fi}") + ".jpg"
    img.convert("RGB").save(os.path.join(out, fn), "JPEG", quality=88, optimize=True)
    print(f"[cache-maps] {fn}  {img.size}  caches {[i for i, _ in fl['c']]}")


if __name__ == "__main__":
    sys.exit(main())
