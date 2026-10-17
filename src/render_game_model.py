#!/usr/bin/env python3
r"""
render_game_model.py - build an item picture straight from the Fallout 76 game files,
the way NifSkope would show it, without NifSkope. (Oct 2026)

  record (ESM, by FormID or EDID) -> MODL model path
  -> .nif pulled out of the "SeventySix - *.ba2" mesh archives
  -> every mesh part, placed with its node transforms (the "assembly")
  -> each part's material (.bgsm / .bgem in Materials.ba2) -> its textures
  -> textures pulled out of the DX10 texture archives (DDS rebuilt, mip 0)
  -> software 3D render (numba): textured, lit, transparent background, PNG/AVIF

Read-only: it never writes to the game folder. Animation, particles and hidden
nodes are skipped; glass / glow / liquid (.bgem effect materials) are drawn as
see-through layers over the solid parts.

Usage:
  python src/render_game_model.py --data "D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data" \
      --form 008ABB18 --out brain_in_a_jar.png
  python src/render_game_model.py --data ... --nif meshes\setdressing\...\thing.nif --out thing.png
Options: --size 1024  --yaw 30  --pitch 18  --no-effects  --avif
Needs: numpy, pillow, numba (pip install numba).

Not handled yet: weapon mods (OMOD attach points), armour on a body, ENTM storefront
art (those are ready-made images in the Interface archives - pull them instead).
"""
import argparse, glob, json, math, os, re, struct, sys, zlib
import numpy as np
from PIL import Image

try:
    from numba import njit
except ImportError:            # still works, just slowly
    def njit(*a, **k):
        return (lambda f: f) if not (a and callable(a[0])) else a[0]

# ───────────────────────────── BA2 archives ─────────────────────────────

class BA2Index:
    """Name -> (archive, record) for every GNRL (meshes/materials) and DX10 (texture)
    archive in Data. Built once per Data folder and cached next to the user profile."""

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.files = {}          # lower path -> (ba2 path, kind, record tuple)
        cache = os.path.join(os.path.expanduser("~"), ".dfbnb_ba2_index_" +
                             re.sub(r"[^A-Za-z0-9]+", "_", os.path.abspath(data_dir))[-60:] + ".json")
        sig = {os.path.basename(p): os.path.getsize(p) for p in glob.glob(os.path.join(data_dir, "*.ba2"))}
        if os.path.exists(cache):
            try:
                c = json.load(open(cache))
                if c.get("sig") == sig:
                    self.files = {k: tuple(v) for k, v in c["files"].items()}
                    return
            except Exception:
                pass
        for ba in sorted(glob.glob(os.path.join(data_dir, "*.ba2"))):
            self._index(ba)
        try:
            json.dump({"sig": sig, "files": self.files}, open(cache, "w"))
        except OSError:
            pass

    def _index(self, ba):
        with open(ba, "rb") as f:
            magic, ver, typ, cnt, nto = struct.unpack("<4sI4sIQ", f.read(24))
            if ver in (2, 3):
                f.read(8 if ver == 2 else 12)
            recs = []
            if typ == b"GNRL":
                for _ in range(cnt):
                    recs.append(("G",) + struct.unpack("<IIIIQIII", f.read(36)))
            elif typ == b"DX10":
                for _ in range(cnt):
                    h = struct.unpack("<I4sIBBHHHBBBB", f.read(24))
                    nchunks = h[4]
                    chunks = [struct.unpack("<QIIHHI", f.read(24)) for _ in range(nchunks)]
                    recs.append(("T", h[6], h[7], h[8], h[9], h[10], [c[:3] for c in chunks]))
            else:
                return
            f.seek(nto)
            for r in recs:
                n = struct.unpack("<H", f.read(2))[0]
                name = f.read(n).decode("latin1").replace("\\", "/").lower()
                # later archives (updates) override earlier ones - same as the game
                self.files[name] = (ba,) + tuple(r)

    def find(self, path):
        p = path.replace("\\", "/").lower().lstrip("/")
        for cand in (p, "meshes/" + p, "materials/" + p, "textures/" + p):
            if cand in self.files:
                return cand
        return None

    def read(self, path):
        key = self.find(path)
        if not key:
            return None
        e = self.files[key]
        ba, kind = e[0], e[1]
        with open(ba, "rb") as f:
            if kind == "G":
                off, packed, unpacked = e[6], e[7], e[8]
                f.seek(off)
                d = f.read(packed or unpacked)
                return zlib.decompress(d) if packed else d
            # DX10 texture: rebuild a DDS holding mip 0 only
            height, width, nmips, fmt, cube, chunks = e[2], e[3], e[4], e[5], e[6], e[7]
            off, packed, unpacked = chunks[0]
            f.seek(off)
            d = f.read(packed or unpacked)
            d = zlib.decompress(d) if packed else d
            return _dds(width, height, fmt, d)


_BLOCK8 = {70, 71, 72, 79, 80, 81}          # BC1, BC4 = 8 bytes per 4x4 block


# sRGB variants hold the same bytes as their UNORM twins; Pillow only knows the UNORM
# codes, so relabel (colour is gamma-encoded either way, which is what we want for a PNG).
_SRGB_TO_UNORM = {72: 71, 75: 74, 78: 77, 99: 98, 29: 28, 91: 87, 93: 92}


def _dds(w, h, fmt, data):
    fmt = _SRGB_TO_UNORM.get(fmt, fmt)
    if fmt in (28, 29, 87, 91):              # RGBA8 / BGRA8
        size = w * h * 4
    else:
        size = max(1, (w + 3) // 4) * max(1, (h + 3) // 4) * (8 if fmt in _BLOCK8 else 16)
    data = data[:size]
    hdr = struct.pack("<4sIIIIIII44x", b"DDS ", 124, 0x1 | 0x2 | 0x4 | 0x1000 | 0x80000, h, w, size, 0, 1)
    pf = struct.pack("<II4s20x", 32, 0x4, b"DX10")
    caps = struct.pack("<IIII4x", 0x1000, 0, 0, 0)
    dx10 = struct.pack("<IIIII", fmt, 3, 0, 1, 0)
    return hdr + pf + caps + dx10 + data


def load_texture(ba2, path):
    if not path:
        return None
    d = ba2.read(path)
    if not d:
        return None
    try:
        import io
        im = Image.open(io.BytesIO(d))
        im.load()
        return np.asarray(im.convert("RGBA"), dtype=np.float32) / 255.0
    except Exception as ex:
        print(f"  [texture] could not decode {path}: {ex}")
        return None

# ───────────────────────────── ESM record -> model path ─────────────────────────────

def _subs(data):
    p, big = 0, None
    while p + 6 <= len(data):
        t, sz = struct.unpack_from("<4sH", data, p); p += 6
        if t == b"XXXX":
            big = struct.unpack_from("<I", data, p)[0]; p += sz; continue
        if big is not None:
            sz, big = big, None
        yield t, data[p:p + sz]; p += sz


def model_for(esm, form=None, edid=None):
    buf = open(esm, "rb").read()
    want = int(form, 16) if form else None
    o, L = 24 + struct.unpack_from("<I", buf, 4)[0], len(buf)
    while o < L:
        t = buf[o:o + 4]; sz = struct.unpack_from("<I", buf, o + 4)[0]
        if t == b"GRUP":
            o += 24; continue
        fid = struct.unpack_from("<I", buf, o + 12)[0]
        if (want is not None and fid == want) or edid:
            d = buf[o + 24:o + 24 + sz]
            if struct.unpack_from("<I", buf, o + 8)[0] & 0x40000:
                d = zlib.decompress(d[4:])
            ed = mod = None
            for st, v in _subs(d):
                if st == b"EDID": ed = v.rstrip(b"\0").decode("latin1")
                elif st == b"MODL" and mod is None: mod = v.rstrip(b"\0").decode("latin1")
            if (want is not None and fid == want) or (edid and ed == edid):
                return t.decode(), f"{fid:08X}", ed, mod
        o += 24 + sz
    return None

# ───────────────────────────── NIF (Fallout 76, bsver 155) ─────────────────────────────

SHAPES = {"BSTriShape", "BSSubIndexTriShape", "BSMeshLODTriShape", "BSDynamicTriShape"}


class Nif:
    def __init__(self, b):
        self.b = b
        p = b.index(b"\n") + 1
        self.ver, _e, self.user, n = struct.unpack_from("<IBII", b, p); p += 13
        self.bsver = struct.unpack_from("<I", b, p)[0]; p += 4

        def es():
            nonlocal p
            k = b[p]; s = b[p + 1:p + 1 + k]; p += 1 + k; return s
        es()
        if self.bsver > 130: p += 4
        if self.bsver < 131: es()
        es()
        if self.bsver >= 103: es()
        nt = struct.unpack_from("<H", b, p)[0]; p += 2
        types = []
        for _ in range(nt):
            k = struct.unpack_from("<I", b, p)[0]; types.append(b[p + 4:p + 4 + k].decode()); p += 4 + k
        idx = struct.unpack_from(f"<{n}H", b, p); p += 2 * n
        sizes = struct.unpack_from(f"<{n}I", b, p); p += 4 * n
        ns, _ml = struct.unpack_from("<II", b, p); p += 8
        self.strings = []
        for _ in range(ns):
            k = struct.unpack_from("<I", b, p)[0]; self.strings.append(b[p + 4:p + 4 + k].decode("latin1")); p += 4 + k
        ng = struct.unpack_from("<I", b, p)[0]; p += 4 + 4 * ng
        self.blocks = []
        for i in range(n):
            self.blocks.append((types[idx[i] & 0x7FFF], p, sizes[i])); p += sizes[i]

    def s(self, i):
        return self.strings[i] if 0 <= i < len(self.strings) else ""

    def av(self, i):
        """NiObjectNET + NiAVObject header -> (name, flags, T, R, scale, offset after)."""
        _t, p, _sz = self.blocks[i]
        b = self.b
        name = struct.unpack_from("<i", b, p)[0]; p += 4
        nex = struct.unpack_from("<I", b, p)[0]; p += 4 + 4 * nex
        p += 4                                       # controller
        flags = struct.unpack_from("<I", b, p)[0]; p += 4
        T = np.array(struct.unpack_from("<3f", b, p)); p += 12
        R = np.array(struct.unpack_from("<9f", b, p)).reshape(3, 3); p += 36
        sc = struct.unpack_from("<f", b, p)[0]; p += 4
        p += 4                                       # collision
        return self.s(name), flags, T, R, sc, p

    def children(self, i):
        *_, p = self.av(i)
        n = struct.unpack_from("<I", self.b, p)[0]
        return [c for c in struct.unpack_from(f"<{n}i", self.b, p + 4) if 0 <= c < len(self.blocks)]

    def shape(self, i):
        name, flags, T, R, sc, p = self.av(i)
        b = self.b
        p += 16                                      # bounding sphere
        if self.bsver == 155:
            p += 24                                  # Fallout 76 bound min/max
        skin, shader, alpha = struct.unpack_from("<3i", b, p); p += 12
        desc = struct.unpack_from("<Q", b, p)[0]; p += 8
        ntri = struct.unpack_from("<I" if self.bsver >= 130 else "<H", b, p)[0]; p += 4 if self.bsver >= 130 else 2
        nv = struct.unpack_from("<H", b, p)[0]; p += 2
        dsize = struct.unpack_from("<I", b, p)[0]; p += 4
        if not dsize or not nv:
            return None
        stride = (desc & 0xF) * 4
        attrs = desc >> 44
        full = bool(attrs & 0x400)
        raw = np.frombuffer(b, dtype=np.uint8, count=stride * nv, offset=p).reshape(nv, stride)
        p += stride * nv
        tris = np.frombuffer(b, dtype=np.uint16, count=3 * ntri, offset=p).reshape(ntri, 3).astype(np.int32)
        if full:
            pos = raw[:, 0:12].copy().view(np.float32).reshape(nv, 3)
        else:
            pos = raw[:, 0:6].copy().view(np.float16).reshape(nv, 3).astype(np.float32)
        uv = np.zeros((nv, 2), np.float32)
        if attrs & 0x2:
            o = ((desc >> 8) & 0xF) * 4
            uv = raw[:, o:o + 4].copy().view(np.float16).reshape(nv, 2).astype(np.float32)
        nrm = np.tile(np.array([0, 0, 1], np.float32), (nv, 1))
        if attrs & 0x8:
            o = ((desc >> 16) & 0xF) * 4
            nrm = raw[:, o:o + 3].astype(np.float32) / 127.5 - 1.0
        col = None
        if attrs & 0x20:
            o = ((desc >> 24) & 0xF) * 4
            col = raw[:, o:o + 4].astype(np.float32) / 255.0
        return {"name": name, "flags": flags, "T": T, "R": R, "scale": sc, "pos": pos, "uv": uv,
                "nrm": nrm, "col": col, "tris": tris, "shader": shader, "alpha": alpha, "skinned": skin >= 0}

    def material(self, shader_idx):
        """(material path, kind, texture-set first texture). Name field of the shader
        property holds the .bgsm/.bgem path on Fallout 4/76."""
        if not (0 <= shader_idx < len(self.blocks)):
            return None, None, None
        t, p, sz = self.blocks[shader_idx]
        kind = "effect" if "Effect" in t else "lit"
        mat = None
        for off in range(p, p + min(sz, 64) - 3, 4):
            k = struct.unpack_from("<i", self.b, off)[0]
            s = self.s(k)
            if s.lower().endswith((".bgsm", ".bgem")):
                mat = s; break
        tex = None
        for off in range(p, p + sz - 3):
            k = struct.unpack_from("<i", self.b, off)[0]
            if 0 <= k < len(self.blocks) and self.blocks[k][0] == "BSShaderTextureSet" and k != shader_idx:
                _tt, tp, _ts = self.blocks[k]
                if struct.unpack_from("<I", self.b, tp)[0] in range(1, 20):
                    sl = struct.unpack_from("<I", self.b, tp + 4)[0]
                    tex = self.b[tp + 8:tp + 8 + sl].decode("latin1") or None
                    break
        return mat, kind, tex


def collect(nif):
    """Walk the scene graph from the root, composing node transforms, and return every
    visible mesh part in model space."""
    out = []

    def walk(i, M, depth=0):
        t = nif.blocks[i][0]
        if depth > 64:
            return
        if t in SHAPES:
            sh = nif.shape(i)
            if not sh or (sh["flags"] & 1):
                return
            L = np.eye(4); L[:3, :3] = sh["R"].T * sh["scale"]; L[:3, 3] = sh["T"]
            W = M @ L
            p = sh["pos"] @ W[:3, :3].T + W[:3, 3]
            n = sh["nrm"] @ W[:3, :3].T
            n /= np.maximum(np.linalg.norm(n, axis=1, keepdims=True), 1e-6)
            sh["wpos"], sh["wnrm"] = p, n
            out.append(sh)
        elif t.endswith("Node") or t in ("NiNode",):
            name, flags, T, R, sc, _p = nif.av(i)
            if flags & 1:
                return
            L = np.eye(4); L[:3, :3] = R.T * sc; L[:3, 3] = T
            for c in nif.children(i):
                walk(c, M @ L, depth + 1)

    walk(0, np.eye(4))
    return out


def material_textures(ba2, mat):
    """Diffuse / base texture from a .bgsm or .bgem (length-prefixed .dds strings;
    the first one is the diffuse on .bgsm and the base map on .bgem)."""
    d = ba2.read(mat) if mat else None
    if not d:
        return None, {}
    found = []
    for m in re.finditer(rb"([\x01-\xff])\x00\x00\x00([ -~]{4,255}?\.dds)\x00", d, re.I):
        found.append(m.group(2).decode("latin1"))
    info = {"magic": d[:4].decode("latin1", "replace")}
    diffuse = next((f for f in found if re.search(r"_d\.dds$", f, re.I)), found[0] if found else None)
    return diffuse, info

# ───────────────────────────── software renderer ─────────────────────────────

@njit(cache=True)
def _raster(sx, sy, sz, uv, shade, tris, tex, zbuf, color, mode, opacity, tint):
    """mode 0 = solid (alpha-tested, writes depth); 1 = see-through effect layer
    (depth-tested against the solids, blended, no depth write)."""
    H, W = zbuf.shape
    th, tw = tex.shape[0], tex.shape[1]
    for t in range(tris.shape[0]):
        a, b, c = tris[t, 0], tris[t, 1], tris[t, 2]
        x0, y0, x1, y1, x2, y2 = sx[a], sy[a], sx[b], sy[b], sx[c], sy[c]
        area = (x1 - x0) * (y2 - y0) - (x2 - x0) * (y1 - y0)
        if abs(area) < 1e-9:
            continue
        minx = max(int(min(x0, x1, x2)), 0); maxx = min(int(max(x0, x1, x2)) + 1, W - 1)
        miny = max(int(min(y0, y1, y2)), 0); maxy = min(int(max(y0, y1, y2)) + 1, H - 1)
        for py in range(miny, maxy + 1):
            for px in range(minx, maxx + 1):
                fx = px + 0.5; fy = py + 0.5
                w0 = ((x1 - fx) * (y2 - fy) - (x2 - fx) * (y1 - fy)) / area
                w1 = ((x2 - fx) * (y0 - fy) - (x0 - fx) * (y2 - fy)) / area
                w2 = 1.0 - w0 - w1
                if w0 < 0 or w1 < 0 or w2 < 0:
                    continue
                z = w0 * sz[a] + w1 * sz[b] + w2 * sz[c]
                if z >= zbuf[py, px]:
                    continue
                u = w0 * uv[a, 0] + w1 * uv[b, 0] + w2 * uv[c, 0]
                v = w0 * uv[a, 1] + w1 * uv[b, 1] + w2 * uv[c, 1]
                tx = int((u - math.floor(u)) * tw) % tw
                ty = int((v - math.floor(v)) * th) % th
                s = w0 * shade[a] + w1 * shade[b] + w2 * shade[c]
                r = tex[ty, tx, 0] * tint[0]; g = tex[ty, tx, 1] * tint[1]; bl = tex[ty, tx, 2] * tint[2]
                al = tex[ty, tx, 3]
                if mode == 0:
                    if al < 0.5:
                        continue
                    zbuf[py, px] = z
                    color[py, px, 0] = r * s; color[py, px, 1] = g * s; color[py, px, 2] = bl * s
                    color[py, px, 3] = 1.0
                else:
                    # effect textures are mostly greyscale masks: brightness x alpha drives coverage
                    k = al * opacity * (0.3 + 0.7 * (tex[ty, tx, 0] + tex[ty, tx, 1] + tex[ty, tx, 2]) / 3.0)
                    color[py, px, 0] = color[py, px, 0] * (1 - k) + r * k
                    color[py, px, 1] = color[py, px, 1] * (1 - k) + g * k
                    color[py, px, 2] = color[py, px, 2] * (1 - k) + bl * k
                    color[py, px, 3] = min(1.0, color[py, px, 3] + k)


def render(parts, textures, size=1024, yaw=30.0, pitch=18.0, effects=True, ss=2):
    S = size * ss
    cy, sy_ = math.cos(math.radians(yaw)), math.sin(math.radians(yaw))
    cp, sp = math.cos(math.radians(pitch)), math.sin(math.radians(pitch))
    # Fallout is Z-up and models face -Y; look from the front, turned by yaw, tilted down.
    Rz = np.array([[cy, -sy_, 0], [sy_, cy, 0], [0, 0, 1]])
    Rx = np.array([[1, 0, 0], [0, cp, -sp], [0, sp, cp]])
    V = Rx @ Rz
    solid = [q for q in parts if q["kind"] != "effect"] or parts
    allp = np.vstack([q["wpos"] for q in solid]) @ V.T
    lo, hi = allp.min(0), allp.max(0)
    span = max(hi[0] - lo[0], hi[2] - lo[2]) or 1.0
    k = S * 0.86 / span
    cx, cz = (lo[0] + hi[0]) / 2, (lo[2] + hi[2]) / 2
    light = np.array([-0.45, -0.6, 0.65]); light /= np.linalg.norm(light)
    zbuf = np.full((S, S), np.inf, np.float32)
    color = np.zeros((S, S, 4), np.float32)
    white = np.ones((4, 4, 4), np.float32)
    order = [q for q in parts if q["kind"] != "effect"] + ([q for q in parts if q["kind"] == "effect"] if effects else [])
    for q in order:
        p = q["wpos"] @ V.T
        sx = ((p[:, 0] - cx) * k + S / 2).astype(np.float32)
        sy = (S / 2 - (p[:, 2] - cz) * k).astype(np.float32)
        sz = (p[:, 1]).astype(np.float32)               # +Y (after view) = further away
        n = q["wnrm"] @ V.T
        shade = (0.42 + 0.58 * np.clip(np.abs(n @ (V @ light)), 0, 1)).astype(np.float32)
        tex = textures.get(q.get("texture")) if q.get("texture") else None
        if tex is None:
            tex = white * np.array([0.7, 0.7, 0.7, 1], np.float32)
        tint = np.array([1, 1, 1], np.float32)
        mode = 1 if q["kind"] == "effect" else 0
        m = (q.get("material") or "").lower()
        # glass barely tints; liquids/glows a little more (in-game shaders add refraction we skip)
        op = 0.10 if "glass" in m else (0.18 if ("liquid" in m or "water" in m or "mixture" in m) else 0.30)
        _raster(sx, sy, sz, q["uv"].astype(np.float32), shade, q["tris"], tex.astype(np.float32),
                zbuf, color, mode, np.float32(op), tint)
    img = Image.fromarray((np.clip(color, 0, 1) * 255).astype(np.uint8), "RGBA")
    return img.resize((size, size), Image.LANCZOS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Fallout 76 (or Playtest) Data folder")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--form"); g.add_argument("--edid"); g.add_argument("--nif")
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", type=int, default=1024)
    ap.add_argument("--yaw", type=float, default=30.0)
    ap.add_argument("--pitch", type=float, default=18.0)
    ap.add_argument("--no-effects", action="store_true")
    a = ap.parse_args()
    ba2 = BA2Index(a.data)
    nif_path = a.nif
    if not nif_path:
        rec = model_for(os.path.join(a.data, "SeventySix.esm"), a.form, a.edid)
        if not rec or not rec[3]:
            sys.exit(f"No model found for {a.form or a.edid}")
        print(f"{rec[0]} {rec[1]} {rec[2]} -> {rec[3]}")
        nif_path = rec[3]
    raw = ba2.read(nif_path)
    if not raw:
        sys.exit(f"{nif_path} is not in any archive in {a.data}")
    nif = Nif(raw)
    parts = collect(nif)
    textures = {}
    for q in parts:
        mat, kind, texset = nif.material(q["shader"])
        q["kind"], q["material"] = kind or "lit", mat
        diffuse, _info = material_textures(ba2, mat)
        q["texture"] = diffuse or texset
        if q["texture"] and q["texture"] not in textures:
            textures[q["texture"]] = load_texture(ba2, q["texture"])
        print(f"  part {q['name'] or '?':40s} {len(q['tris']):6d} tris  {q['kind']:6s}  {mat or '-'}  -> {q['texture'] or 'no texture'}")
    img = render(parts, textures, a.size, a.yaw, a.pitch, not a.no_effects)
    img = img.crop(img.getbbox() or (0, 0, a.size, a.size))
    os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
    img.save(a.out)
    print(f"saved {a.out} ({img.size[0]}x{img.size[1]})")


if __name__ == "__main__":
    main()
