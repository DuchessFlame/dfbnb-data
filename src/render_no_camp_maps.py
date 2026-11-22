#!/usr/bin/env python3
r"""
render_no_camp_maps.py - the No CAMP Build Location Map (DF > CAMP), live and PTS.

WHAT BLOCKS A CAMP (read straight from SeventySix.esm, Nov 2026)
  * ACTI 00437F9D NoCampAllowedTrigger       - invisible box / sphere volumes
  * ACTI 0062039A NoCampAllowedWorldsTrigger - 17 big boxes (Whitespring, Charleston ...)
  * KYWD 003EA4E6 NoCampAllowedLocationKeyword - only on Vault76LocationExterior;
    every exterior cell of that location is blocked whole.
  The base records only carry MustPersist, so the rule lives in the exe keyed on
  these bases - nothing else in the ESM marks a no-CAMP area.

GEOMETRY
  XPRM bounds are HALF extents (abutting boxes prove it); sphere = bounds X radius.
  Rotation is radians, clockwise-positive seen from above (Bethesda), X then Y then Z.
  Each zone is rasterised at 4x, then cut into boxes of one map pixel (~142 units);
  a box is coloured when at least half of it is inside a zone.

WATER
  Mappalachia's own img/wrld/APPALACHIA_waterMask.png (real water, not plane
  footprints - water planes overhang the banks).

USAGE
  python src/render_no_camp_maps.py --out "<Guides and Stuff>\.CAMP\No CAMP Build Location Map"
  env: FO76_LIVE_DATA, FO76_PTS_DATA (game Data folders), MAPPALACHIA_DIR, GUIDES_ROOT
"""
import argparse, math, mmap, os, struct, zlib
import numpy as np
from PIL import Image, ImageDraw

import map_watermark
from render_spawn_maps import MAPPALACHIA, S, LEGEND_PAD, LEGEND_BG, LEGEND_BORDER, \
    LEGEND_BORDER_W, _font, draw_dot

Image.MAX_IMAGE_PIXELS = None
LIVE = os.environ.get("FO76_LIVE_DATA", r"D:\steamlibrary\steamapps\common\Fallout76\Data")
PTS = os.environ.get("FO76_PTS_DATA", r"D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data")
SLUG = "no-camp-build-location-map"

WRLD_APPALACHIA = 0x25DA15
NOCAMP_BASES = (0x437F9D, 0x62039A)
NOCAMP_LOC_KW = 0x3EA4E6
CX, CY, RNG = -500.0, 135.0, 582550.0          # Mappalachia Space row for APPALACHIA

ZONE = (230, 159, 0)        # Okabe-Ito orange - reads for colour-blind readers, not red/purple
WATER = (0, 114, 178)       # Okabe-Ito blue


# ------------------------------------------------------------ ESM reading
def _subrecs(data):
    i, big = 0, None
    while i < len(data):
        t = data[i:i + 4]; sz = struct.unpack_from('<H', data, i + 4)[0]; i += 6
        if t == b'XXXX':
            big = struct.unpack_from('<I', data, i)[0]; i += sz; continue
        if big is not None:
            sz, big = big, None
        yield t, data[i:i + sz]; i += sz


def _rec(mm, off):
    t, sz, fl, fid = struct.unpack_from('<4sIII', mm, off)
    d = mm[off + 24:off + 24 + sz]
    if fl & 0x40000:
        d = zlib.decompress(d[4:])
    return t, fid, d


def _walk(mm, s, e):
    off = s
    while off < e:
        t, sz = struct.unpack_from('<4sI', mm, off)
        if t == b'GRUP':
            lab, gt = struct.unpack_from('<II', mm, off + 8)
            yield 'G', off, sz, lab, gt; off += sz
        else:
            yield 'R', off, t, sz; off += 24 + sz


def read_zones(esm):
    """[(kind, data)] in world units: ('box', [(x,y)...]) / ('circle', (x, y, r))."""
    f = open(esm, 'rb'); mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    off = struct.unpack_from('<I', mm, 4)[0] + 24
    groups = {}
    while off < len(mm):
        _, sz, lab = struct.unpack_from('<4sI4s', mm, off)
        groups[lab] = (off, sz); off += sz
    locs = set()
    o, sz = groups[b'LCTN']
    for k in _walk(mm, o + 24, o + sz):
        if k[0] != 'R':
            continue
        _, fid, d = _rec(mm, k[1])
        for st, sd in _subrecs(d):
            if st == b'KWDA' and NOCAMP_LOC_KW in struct.unpack('<%dI' % (len(sd) // 4), sd):
                locs.add(fid)
    zones, cells = [], {}
    state = {'cell': None}

    def desc(s, e):
        for k in _walk(mm, s, e):
            if k[0] == 'G':
                _, go, gsz, glab, gt = k
                if gt == 1 and glab != WRLD_APPALACHIA:
                    continue
                desc(go + 24, go + gsz); continue
            _, ro, t, _ = k
            if t == b'CELL':
                _, fid, d = _rec(mm, ro); c = {}
                for st, sd in _subrecs(d):
                    if st == b'XCLC': c['xy'] = struct.unpack_from('<ii', sd)
                    elif st == b'XLCN': c['loc'] = struct.unpack('<I', sd)[0]
                cells[fid] = c
            elif t == b'REFR':
                _, fid, d = _rec(mm, ro); sub = dict(_subrecs(d))
                if 'NAME'.encode() not in sub:
                    continue
                if struct.unpack('<I', sub[b'NAME'])[0] not in NOCAMP_BASES:
                    continue
                pos = struct.unpack_from('<3f', sub[b'DATA']); rot = struct.unpack_from('<3f', sub[b'DATA'], 12)
                pr = sub[b'XPRM']; bx = struct.unpack_from('<3f', pr); ptype = struct.unpack_from('<I', pr, len(pr) - 4)[0]
                sc = struct.unpack('<f', sub[b'XSCL'])[0] if b'XSCL' in sub else 1.0
                zones.append(_shape(pos, rot, [v * sc for v in bx], ptype))
    o, sz = groups[b'WRLD']
    desc(o + 24, o + sz)
    for c in cells.values():
        if c.get('loc') in locs and 'xy' in c:
            X, Y = c['xy'][0] * 4096, c['xy'][1] * 4096
            zones.append(('box', [(X, Y), (X + 4096, Y), (X + 4096, Y + 4096), (X, Y + 4096)]))
    return zones


def _shape(pos, rot, half, ptype):
    x0, y0, _ = pos
    if ptype == 2:
        return ('circle', (x0, y0, half[0]))
    rx, ry, rz = rot

    def r(p):
        x, y, z = p
        c, s = math.cos(rx), math.sin(rx); y, z = y * c + z * s, -y * s + z * c
        c, s = math.cos(ry), math.sin(ry); x, z = x * c - z * s, x * s + z * c
        c, s = math.cos(rz), math.sin(rz); x, y = x * c + y * s, -x * s + y * c
        return round(x, 3), round(y, 3)
    pts = [r((a * half[0], b * half[1], c * half[2])) for a in (-1, 1) for b in (-1, 1) for c in (-1, 1)]
    return ('box', [(x0 + a, y0 + b) for a, b in _hull(pts)])


def _hull(pts):
    pts = sorted(set(pts))
    cr = lambda o, a, b: (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
    lo, up = [], []
    for p in pts:
        while len(lo) >= 2 and cr(lo[-2], lo[-1], p) <= 0: lo.pop()
        lo.append(p)
    for p in reversed(pts):
        while len(up) >= 2 and cr(up[-2], up[-1], p) <= 0: up.pop()
        up.append(p)
    return lo[:-1] + up[:-1]


# ------------------------------------------------------------ raster
def box_grid(zones, ss=4):
    """S x S bool grid, one box per map pixel, True when >= half covered."""
    big = S * ss; G = np.zeros((S, S), bool); T = S          # do it in 4x4 tiles of S px
    for ty in range(ss):
        for tx in range(ss):
            im = Image.new('L', (T, T), 0); d = ImageDraw.Draw(im)
            ox, oy = tx * T, ty * T
            to = lambda x, y: ((x - CX) / RNG * big + big / 2 - ox, big / 2 - (y - CY) / RNG * big - oy)
            for kind, z in zones:
                if kind == 'circle':
                    px, py = to(z[0], z[1]); rr = z[2] / RNG * big
                    if -rr < px < T + rr and -rr < py < T + rr:
                        d.ellipse([px - rr, py - rr, px + rr, py + rr], fill=255)
                else:
                    P = [to(x, y) for x, y in z]
                    xs = [p[0] for p in P]; ys = [p[1] for p in P]
                    if max(xs) < 0 or max(ys) < 0 or min(xs) > T or min(ys) > T:
                        continue
                    d.polygon(P, fill=255)
            m = (np.asarray(im) > 127).reshape(T // ss, ss, T // ss, ss).mean(axis=(1, 3)) >= 0.5
            G[ty * T // ss:(ty + 1) * T // ss, tx * T // ss:(tx + 1) * T // ss] = m
    return G


def tint(img, mask, colour, alpha):
    a = np.asarray(img).astype(np.float32)
    a[mask] = a[mask] * (1 - alpha) + np.array(colour, np.float32) * alpha
    return Image.fromarray(a.clip(0, 255).astype(np.uint8))


def legend(img, title, rows):
    """Same box as render_spawn_maps.draw_legend (black, 3px white border, futura
    96/72), but a square swatch and an optional count - the zones are areas."""
    d = ImageDraw.Draw(img)
    f_title, f_row = _font(96), _font(72)
    labels = [f"{lbl}  ({cnt:,})" if cnt is not None else lbl for lbl, cnt, _ in rows]
    box_w = int(max(d.textlength(title, font=f_title) + 64,
                    max(d.textlength(t, font=f_row) for t in labels) + 152) + 64)
    box_h = int(96 + 26 + len(rows) * 84 + 24)
    x0, y0 = LEGEND_PAD, LEGEND_PAD
    d.rectangle([x0, y0, x0 + box_w, y0 + box_h], fill=LEGEND_BG,
                outline=LEGEND_BORDER, width=LEGEND_BORDER_W)
    d.text((x0 + 32, y0 + 6), title, font=f_title, fill=(255, 255, 255))
    y = y0 + 96 + 26
    for (lbl, cnt, colour), text in zip(rows, labels):
        draw_dot(d, x0 + 48, y + 30, fill=colour, shape="square")
        d.text((x0 + 88, y), text, font=f_row, fill=(230, 230, 230))
        y += 84


def render(channel, esm, out):
    zones = read_zones(esm)
    G = box_grid(zones)
    bg = Image.open(os.path.join(MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")).convert("RGB")
    water = np.array(Image.open(os.path.join(MAPPALACHIA, "img", "wrld", "APPALACHIA_waterMask.png")))[..., 3] > 20
    img = tint(bg, water, WATER, 0.6)
    img = tint(img, G, ZONE, 0.7)
    legend(img, f"No CAMP Zones ({'Live' if channel == 'live' else 'PTS'})",
           [("No CAMP allowed", len(zones), ZONE), ("Water", None, WATER)])
    img = map_watermark.apply(img)
    path = os.path.join(out, f"{SLUG}-{channel}.jpg")
    img.save(path, "JPEG", quality=92, subsampling=0)
    print(channel, len(zones), "zones ->", path)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("channels", nargs="*", default=["live", "pts"])
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    for ch in a.channels:
        render(ch, os.path.join(LIVE if ch == "live" else PTS, "SeventySix.esm"), a.out)
