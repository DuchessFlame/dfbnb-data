"""Render a Fallout 76 interface .swf (vector Flash) to a PNG.

Reads the SWF straight out of the game's BA2 archives (via render_game_model.BA2Index)
or from a loose file, draws frame N of the main timeline with pycairo.

  python src/render_swf_icon.py --data "<Fallout76>/Data" \
      --swf "interface/components/quest vault boys/miscellaneous quests/hide_hideoutsquest.swf" \
      --out icon.png --size 512 [--frame 1] [--list] [--symbol HideoutsIcon]
"""
import argparse, math, os, struct, sys, zlib
import cairo

class Bits:
    def __init__(s, b, p=0): s.b, s.p, s.bit = b, p, 0
    def align(s):
        if s.bit: s.p += 1; s.bit = 0
    def u(s, n):
        v = 0
        for _ in range(n):
            v = (v << 1) | ((s.b[s.p] >> (7 - s.bit)) & 1)
            s.bit += 1
            if s.bit == 8: s.bit = 0; s.p += 1
        return v
    def s_(s, n):
        if n == 0: return 0
        v = s.u(n)
        return v - (1 << n) if v & (1 << (n - 1)) else v
    def fb(s, n): return s.s_(n) / 65536.0
    def u8(s): s.align(); v = s.b[s.p]; s.p += 1; return v
    def u16(s): s.align(); v = struct.unpack_from("<H", s.b, s.p)[0]; s.p += 2; return v
    def u32(s): s.align(); v = struct.unpack_from("<I", s.b, s.p)[0]; s.p += 4; return v
    def rgb(s): return (s.u8(), s.u8(), s.u8(), 255)
    def rgba(s): return (s.u8(), s.u8(), s.u8(), s.u8())
    def string(s):
        s.align(); e = s.b.index(0, s.p); v = s.b[s.p:e]; s.p = e + 1; return v.decode("utf8", "replace")
    def rect(s):
        s.align(); n = s.u(5); r = [s.s_(n) for _ in range(4)]; s.align(); return r
    def matrix(s):
        s.align(); a = d = 1.0; b = c = 0.0
        if s.u(1): n = s.u(5); a = s.fb(n); d = s.fb(n)
        if s.u(1): n = s.u(5); b = s.fb(n); c = s.fb(n)
        n = s.u(5); tx = s.s_(n); ty = s.s_(n); s.align()
        # x' = a*x + c*y + tx ; y' = b*x + d*y + ty
        return cairo.Matrix(a, b, c, d, tx, ty)
    def cxform(s, alpha):
        s.align(); ha = s.u(1); hm = s.u(1); n = s.u(4); k = 4 if alpha else 3
        m = [256] * 4; a = [0] * 4
        if hm: m[:k] = [s.s_(n) for _ in range(k)]
        if ha: a[:k] = [s.s_(n) for _ in range(k)]
        s.align(); return (m, a)

def tags(b, p, end):
    while p + 2 <= end:
        h = struct.unpack_from("<H", b, p)[0]; p += 2; code, ln = h >> 6, h & 0x3F
        if ln == 0x3F: ln = struct.unpack_from("<I", b, p)[0]; p += 4
        yield code, p, ln
        p += ln
        if code == 0: return

def fillstyle(r, ver):
    t = r.u8()
    if t == 0: return ("solid", r.rgba() if ver >= 3 else r.rgb())
    if t in (0x10, 0x12, 0x13):
        m = r.matrix(); r.align(); spread = r.u(2); interp = r.u(2); n = r.u(4)
        stops = [(r.u8() / 255.0, r.rgba() if ver >= 3 else r.rgb()) for _ in range(n)]
        if t == 0x13: r.u16()
        return ("lin" if t == 0x10 else "rad", m, stops, spread)
    if 0x40 <= t <= 0x43: r.u16(); r.matrix(); return ("solid", (128, 128, 128, 0))
    raise ValueError("fill type %x" % t)

def stylearrays(r, ver):
    n = r.u8()
    if n == 0xFF and ver >= 2: n = r.u16()
    fills = [fillstyle(r, ver) for _ in range(n)]
    n = r.u8()
    if n == 0xFF and ver >= 2: n = r.u16()
    lines = []
    for _ in range(n):
        w = r.u16()
        if ver == 4:
            r.u(2); join = r.u(2); hasfill = r.u(1); r.u(5); r.u(1); r.u(2)  # caps/flags
            r.align()
            if join == 2: r.u16()
            col = fillstyle(r, ver) if hasfill else ("solid", r.rgba())
            if col[0] != "solid": col = ("solid", col[2][0][1])
            lines.append((w, col[1]))
        else:
            lines.append((w, r.rgba() if ver >= 3 else r.rgb()))
    r.align(); nf = r.u(4); nl = r.u(4)
    return fills, lines, nf, nl

def parse_shape(b, p, ver):
    r = Bits(b, p); sid = r.u16(); r.rect()
    if ver == 4: r.rect(); r.u8()
    fills, lines, nf, nl = stylearrays(r, ver)
    groups = []                      # [(fills, lines, fillEdges{idx:[edges]}, lineEdges{idx:[edges]})]
    cur = (fills, lines, {}, {}); groups.append(cur)
    x = y = 0; f0 = f1 = ln = 0
    while True:
        if r.u(1) == 0:
            fl = r.u(5)
            if fl == 0: break
            if fl & 1: n = r.u(5); x = r.s_(n); y = r.s_(n)
            if fl & 2: f0 = r.u(nf)
            if fl & 4: f1 = r.u(nf)
            if fl & 8: ln = r.u(nl)
            if fl & 16:
                fills, lines, nf, nl = stylearrays(r, ver)
                cur = (fills, lines, {}, {}); groups.append(cur); f0 = f1 = ln = 0
        else:
            if r.u(1):
                n = r.u(4) + 2
                if r.u(1): dx = r.s_(n); dy = r.s_(n)
                elif r.u(1): dx = 0; dy = r.s_(n)
                else: dx = r.s_(n); dy = 0
                e = ((x, y), None, (x + dx, y + dy))
            else:
                n = r.u(4) + 2
                cx = x + r.s_(n); cy = y + r.s_(n); ax = cx + r.s_(n); ay = cy + r.s_(n)
                e = ((x, y), (cx, cy), (ax, ay))
            x, y = e[2]
            if f1: cur[2].setdefault(f1, []).append(e)
            if f0: cur[2].setdefault(f0, []).append((e[2], e[1], e[0]))
            if ln: cur[3].setdefault(ln, []).append(e)
    return sid, groups

def chain(edges):
    """Join edges into closed loops (SWF fills come as loose edges)."""
    by = {}
    for e in edges: by.setdefault(e[0], []).append(e)
    used = set(); loops = []
    for e in edges:
        if id(e) in used: continue
        loop = [e]; used.add(id(e)); end = e[2]
        while end != loop[0][0]:
            nxt = next((c for c in by.get(end, []) if id(c) not in used), None)
            if nxt is None: break
            loop.append(nxt); used.add(id(nxt)); end = nxt[2]
        loops.append(loop)
    return loops

def path(cr, loops):
    for loop in loops:
        cr.move_to(*loop[0][0])
        for a, c, b in loop:
            if c is None: cr.line_to(*b)
            else:
                (x0, y0) = cr.get_current_point()
                cr.curve_to(x0 + 2/3*(c[0]-x0), y0 + 2/3*(c[1]-y0), b[0] + 2/3*(c[0]-b[0]), b[1] + 2/3*(c[1]-b[1]), *b)
        cr.close_path()

def xc(col, cx):
    out = list(col)
    for m, a in cx:
        out = [max(0, min(255, out[i] * m[i] / 256 + a[i])) for i in range(4)]
    return tuple(v / 255.0 for v in out)

def source(cr, f, cx):
    if f[0] == "solid":
        cr.set_source_rgba(*xc(f[1], cx)); return
    _, m, stops, spread = f
    g = cairo.LinearGradient(-16384, 0, 16384, 0) if f[0] == "lin" else cairo.RadialGradient(0, 0, 0, 0, 0, 16384)
    for t, c in stops: g.add_color_stop_rgba(t, *xc(c, cx))
    g.set_extend({0: cairo.EXTEND_PAD, 1: cairo.EXTEND_REFLECT, 2: cairo.EXTEND_REPEAT}.get(spread, cairo.EXTEND_PAD))
    im = cairo.Matrix(*m); im.invert(); g.set_matrix(im); cr.set_source(g)

class SWF:
    def __init__(s, data):
        sig = data[:3]
        if sig == b"CWS": body = zlib.decompress(data[8:])
        elif sig == b"FWS": body = data[8:]
        else: raise ValueError("unsupported swf %r" % sig)
        s.b = body; r = Bits(body); s.frame = r.rect(); r.u16(); r.u16(); r.align(); s.start = r.p
        s.shapes, s.sprites, s.names = {}, {}, {}
        s.main = s._timeline(s.start, len(body), top=True)
    def _timeline(s, p, end, top=False):
        frames, cur = [], []
        for code, tp, ln in tags(s.b, p, end):
            if code in (2, 22, 32, 83):
                ver = {2: 1, 22: 2, 32: 3, 83: 4}[code]
                try: sid, g = parse_shape(s.b, tp, ver); s.shapes[sid] = g
                except Exception as ex: print("shape parse failed:", ex, file=sys.stderr)
            elif code == 39:
                sid = struct.unpack_from("<H", s.b, tp)[0]
                s.sprites[sid] = s._timeline(tp + 4, tp + ln)
            elif code in (26, 70, 4, 28, 5):
                cur.append((code, tp, ln))
            elif code == 76:                      # SymbolClass: id -> exported class name
                r = Bits(s.b, tp)
                for _ in range(r.u16()):
                    cid = r.u16(); s.names[r.string()] = cid
            elif code == 1:
                frames.append(cur); cur = []
        if cur: frames.append(cur)
        return frames
    def display(s, frames, n):
        dl = {}
        for fr in frames[:max(1, n)]:
            for code, tp, ln in fr:
                if code == "shape": dl[1] = dict(cid=tp, m=cairo.Matrix(), cx=None, clip=0); continue
                if code is None: continue
                r = Bits(s.b, tp)
                if code == 28: dl.pop(r.u16(), None); continue
                if code == 5: r.u16(); dl.pop(r.u16(), None); continue
                if code == 4:
                    cid = r.u16(); d = r.u16(); dl[d] = dict(cid=cid, m=r.matrix(), cx=None, clip=0); continue
                f = r.u8(); f2 = r.u8() if code == 70 else 0
                d = r.u16()
                if code == 70 and (f2 & 8 or (f2 & 16 and f & 2)): r.string()
                it = dict(dl.get(d, {})) if f & 1 else {}
                if f & 2: it["cid"] = r.u16()
                if f & 4: it["m"] = r.matrix()
                if f & 8: it["cx"] = r.cxform(True)
                if f & 16: r.u16()
                if f & 32: it["name"] = r.string()
                it["clip"] = r.u16() if f & 64 else it.get("clip", 0)
                it.setdefault("m", cairo.Matrix()); it.setdefault("cx", None)
                if "cid" in it: dl[d] = it
        return dl
    def draw(s, cr, frames, n, cx):
        dl = s.display(frames, n); depths = sorted(dl); i = 0
        while i < len(depths):
            it = dl[depths[i]]
            if it["clip"]:
                inner = [d for d in depths[i+1:] if d <= it["clip"]]
                cr.push_group()
                for d in inner: s._item(cr, dl[d], n, cx)
                content = cr.pop_group()
                cr.push_group(); s._item(cr, it, n, cx, mask=True); m = cr.pop_group()
                cr.save(); cr.set_source(content); cr.mask(m); cr.restore()
                i += 1 + len(inner); continue
            s._item(cr, it, n, cx); i += 1
    def _item(s, cr, it, n, cx, mask=False):
        cr.save(); cr.transform(it["m"])
        cx2 = ([it["cx"]] if it.get("cx") else []) + cx
        cid = it["cid"]
        if cid in s.shapes: s._shape(cr, s.shapes[cid], cx2, mask)
        elif cid in s.sprites:
            fr = s.sprites[cid]
            s.draw(cr, fr, ((n - 1) % max(1, len(fr))) + 1 if fr else 1, cx2)
        cr.restore()
    def _shape(s, cr, groups, cx, mask):
        for fills, lines, fe, le in groups:
            for idx in sorted(fe):
                if idx > len(fills): continue
                cr.new_path(); path(cr, chain(fe[idx])); cr.set_fill_rule(cairo.FILL_RULE_WINDING)
                if mask: cr.set_source_rgba(0, 0, 0, 1)
                else: source(cr, fills[idx - 1], cx)
                cr.fill()
            for idx in sorted(le):
                if idx > len(lines) or mask: continue
                w, col = lines[idx - 1]
                cr.new_path()
                for a, c, b in le[idx]:
                    cr.move_to(*a)
                    if c is None: cr.line_to(*b)
                    else:
                        cr.curve_to(a[0]+2/3*(c[0]-a[0]), a[1]+2/3*(c[1]-a[1]), b[0]+2/3*(c[0]-b[0]), b[1]+2/3*(c[1]-b[1]), *b)
                cr.set_line_width(max(w, 20)); cr.set_line_cap(cairo.LINE_CAP_ROUND); cr.set_line_join(cairo.LINE_JOIN_ROUND)
                cr.set_source_rgba(*xc(col, cx)); cr.stroke()

def bounds(swf, n):
    surf = cairo.RecordingSurface(cairo.CONTENT_COLOR_ALPHA, None); cr = cairo.Context(surf)
    swf.draw(cr, swf.main, n, []); return surf.ink_extents()

def render(data, out, size=512, frame=1, pad=0.04, tint=None, symbol=None):
    swf = SWF(data)
    if symbol:                       # draw one exported symbol instead of the main timeline
        cid = swf.names.get(symbol)
        if cid is None:
            raise SystemExit("no symbol %r (have: %s)" % (symbol, ", ".join(sorted(swf.names))[:2000]))
        swf.main = swf.sprites.get(cid) or [[(None, None, None)]]
        if cid in swf.shapes: swf.main = [[("shape", cid, 0)]]
    x, y, w, h = bounds(swf, frame)
    if w <= 0 or h <= 0: raise SystemExit("frame %d is empty" % frame)
    sc = size * (1 - 2 * pad) / max(w, h)
    surf = cairo.ImageSurface(cairo.FORMAT_ARGB32, size, size); cr = cairo.Context(surf)
    cr.translate(size / 2, size / 2); cr.scale(sc, sc); cr.translate(-(x + w / 2), -(y + h / 2))
    cr.set_antialias(cairo.ANTIALIAS_BEST)
    swf.draw(cr, swf.main, frame, [])
    if tint:
        cr.identity_matrix(); cr.set_operator(cairo.OPERATOR_IN); cr.set_source_rgb(*tint); cr.paint()
    surf.write_to_png(out)
    return len(swf.main)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data"); ap.add_argument("--swf", required=True); ap.add_argument("--out")
    ap.add_argument("--size", type=int, default=512); ap.add_argument("--frame", type=int, default=1)
    ap.add_argument("--list", action="store_true"); ap.add_argument("--symbol")
    a = ap.parse_args()
    if os.path.exists(a.swf): data = open(a.swf, "rb").read()
    else:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from render_game_model import BA2Index
        data = BA2Index(a.data).read(a.swf)
    if a.list:
        s = SWF(data); print("frames", len(s.main), "shapes", len(s.shapes), "sprites", len(s.sprites))
        for k in sorted(s.names): print("  symbol", k)
        return
    n = render(data, a.out, a.size, a.frame, symbol=a.symbol); print("wrote", a.out, "(frames in movie: %d)" % n)

if __name__ == "__main__": main()
