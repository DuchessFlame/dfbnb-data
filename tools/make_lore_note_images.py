#!/usr/bin/env python3
r"""
make_lore_note_images.py — a picture of each lore note on its in-game paper.

The game has no picture per note: inspecting a note shows a plain paper model
(BOOK INAM -> STAT) and the Pip-Boy lays the note's text over it. This tool
does the same: the paper's own texture from the game files + the note's text
in the game's layout (line breaks, alignment, typewriter / handwriting,
colours), and saves <slug>-note.avif for the page.

Reads   dist/collectables_slasher_lore.json   (items with "note" + "image")
        the paper textures from the game's Textures ba2s (--data), or from a
        folder of already-pulled .dds / .png files (--paper-dir)
Writes  <out>/<file name from the item's "image">   e.g.
        a-page-torn-from-a-children-s-book-note.avif
Upload those to wp-content/uploads/guide-images/collectables/Slasher-Lore/.

    python tools\make_lore_note_images.py --data "C:\...\Fallout76\Data"
    python tools\make_lore_note_images.py --paper-dir notes_img --out out_imgs

Paper STAT -> texture is the one table here (PAPER_TEXTURE); a paper not in it
uses the plain paper and is listed at the end so it can be added.
Fonts (tools/fonts, Apache / OFL licensed Google fonts) stand in for the
game's: Special Elite = $Typewriter_Font, Caveat = handwriting,
Homemade Apple = script, VT323 = terminal.
"""
from __future__ import annotations

import argparse
import glob
import io
import json
import os
import re
import struct
import sys
import zlib
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
FONTS = HERE / "fonts"
LIVE_DATA = r"C:\Program Files (x86)\Steam\steamapps\common\Fallout76\Data"

# paper STAT EditorID -> its _d texture (from the STAT's model -> BGSM)
PAPER_TEXTURE = {
    "HighPoly_Note": "Interface/Note/Note_d.dds",
    "HighPoly_Note_Plain02": "Interface/Note/Note_Plain02HighPoly_d.dds",
    "HighPoly_Note_Plain03": "Interface/Note/Note_Plain03HighPoly_d.dds",
    "HighPoly_Note_Postcard03_NoText": "Interface/Note/Note_Postcard03HighPoly_NoText_d.dds",
    "HighPoly_Note_Classified": "Interface/Note/Note_ClassifiedHighPoly_d.dds",
    "HighPoly_NoteRipped": "Interface/Note/NoteRipped_d.dds",
    "HighPoly_SlasherMap": "Interface/Note/Note_LinedPaper_SlasherMap_d.dds",
}
DEFAULT_PAPER = "Interface/Note/Note_Plain03HighPoly_d.dds"

FONT_FILE = {
    "typewriter": "SpecialElite-Regular.ttf",
    "handwritten": "Caveat-Regular.ttf",
    "handwritten-neat": "Caveat-Regular.ttf",
    "script": "HomemadeApple-Regular.ttf",
    "terminal": "VT323-Regular.ttf",
    "main": "SpecialElite-Regular.ttf",
}
# handwriting fonts run small next to the typewriter one at the same size
FONT_SCALE = {"handwritten": 1.35, "handwritten-neat": 1.35, "script": 0.9, "terminal": 1.2}
INK = (38, 32, 28, 255)
OUT_W = 800                 # longest side of the saved picture
BASE_SIZE = 20              # the game's default note size


# ------------------------------------------------------------------ textures
def _dds_dx10(w, h, mips, fmt, data):
    hdr = struct.pack("<4sIIIIIII44s", b"DDS ", 124, 0x1 | 0x2 | 0x4 | 0x1000 | 0x20000, h, w, 0, 0, mips, b"\0" * 44)
    pf = struct.pack("<II4sIIIII", 32, 0x4, b"DX10", 0, 0, 0, 0, 0)
    caps = struct.pack("<IIIII", 0x1000 | 0x400000 | 0x8, 0, 0, 0, 0)
    return hdr + pf + caps + struct.pack("<IIIII", fmt, 3, 0, 1, 0) + data


def pull_textures(data_dir, wanted):
    """{lower path: dds bytes} from the Textures ba2s (DX10 archives)."""
    want = {w.lower().replace("\\", "/") for w in wanted}
    got = {}
    paths = sorted(glob.glob(os.path.join(data_dir, "SeventySix - *Textures*.ba2")))
    for p in paths:
        with open(p, "rb") as f:
            magic, ver, typ, nf, nto = struct.unpack("<4sI4sIQ", f.read(24))
            if typ != b"DX10":
                continue
            if ver in (2, 3):
                f.read(8)
            files = []
            for _ in range(nf):
                _nh, _ext, _dh, _u8, nch, _chl, hgt, wid, nm, fmt, _cube, _tile = struct.unpack("<I4sIBBHHHBBBB", f.read(24))
                ch = [struct.unpack("<QIIHHI", f.read(24)) for _ in range(nch)]
                files.append((hgt, wid, nm, fmt, ch))
            f.seek(nto)
            for meta in files:
                n = struct.unpack("<H", f.read(2))[0]
                name = f.read(n).decode("latin1").replace("\\", "/").lower()
                if name.startswith("textures/"):
                    name = name[9:]
                if name in want and name not in got:
                    h, w, nm, fmt, ch = meta
                    pos = f.tell()
                    blob = b""
                    for off, ps, us, _sm, _em, _al in ch:
                        f.seek(off)
                        b = f.read(ps or us)
                        blob += zlib.decompress(b) if ps else b
                    f.seek(pos)
                    got[name] = _dds_dx10(w, h, nm, fmt, blob)
    return got


def open_dds(raw):
    b = bytearray(raw)
    if b[84:88] == b"DX10":
        fmt = struct.unpack_from("<I", b, 128)[0]
        # Pillow reads the UNORM twin of each sRGB block format (same blocks)
        srgb = {72: 71, 75: 74, 78: 77, 99: 98, 29: 28}
        if fmt in srgb:
            struct.pack_into("<I", b, 128, srgb[fmt])
    im = Image.open(io.BytesIO(bytes(b)))
    im.load()
    return im.convert("RGBA")


def paper_image(tex_path, cache):
    key = tex_path.lower()
    if key in cache:
        return cache[key].copy()
    return None


# ------------------------------------------------------------------ layout
_font_cache = {}


def font(fam, px):
    fn = FONT_FILE.get(fam, FONT_FILE["main"])
    k = (fn, px)
    if k not in _font_cache:
        _font_cache[k] = ImageFont.truetype(str(FONTS / fn), px)
    return _font_cache[k]


def _tokens(block, scale):
    """Words and line breaks of a block, each with its style."""
    out = []
    for r in block["runs"]:
        fam = r.get("font", "main")
        px = max(8, int(round((r.get("size") or BASE_SIZE) * scale * FONT_SCALE.get(fam, 1.0))))
        col = r.get("color")
        col = tuple(int(col[i:i + 2], 16) for i in (1, 3, 5)) + (255,) if col else INK
        for part in re.split(r"(\n)", r["text"]):
            if part == "\n":
                out.append(("\n", None))
                continue
            for w in re.findall(r"\S+|\s+", part):
                out.append((w, {"font": font(fam, px), "fill": col, "u": r.get("u"), "px": px}))
    return out


def layout(blocks, width, scale):
    """[(align, [(word, style, x)], line_height)] and total height."""
    lines = []
    for b in blocks:
        cur, x, lh = [], 0, 0

        def flush(force_h=None):
            nonlocal cur, x, lh
            # drop trailing spaces
            while cur and not cur[-1][0].strip():
                x -= cur[-1][3]
                cur.pop()
            h = lh or force_h or int(BASE_SIZE * scale * 1.3)
            lines.append((b["align"], cur, h, x))
            cur, x, lh = [], 0, 0

        for w, st in _tokens(b, scale):
            if w == "\n":
                flush(int(BASE_SIZE * scale * 1.3))
                continue
            wd = st["font"].getlength(w)
            if not w.strip() and not cur:
                continue
            if x + wd > width and cur and w.strip():
                flush()
            if not w.strip() and not cur:
                continue
            cur.append((w, st, x, wd))
            x += wd
            lh = max(lh, int(st["px"] * 1.3))
        flush()
        lines.append((None, [], int(BASE_SIZE * scale * 0.7), 0))      # gap between blocks
    if lines:
        lines.pop()
    return lines, sum(h for _, _, h, _ in lines)


def text_box(paper):
    """Where text goes: the paper's own (opaque) area, less a margin."""
    a = paper.getchannel("A").point(lambda v: 255 if v > 40 else 0)
    bb = a.getbbox() or (0, 0, paper.width, paper.height)
    w, h = bb[2] - bb[0], bb[3] - bb[1]
    mx, my = int(w * 0.11), int(h * 0.09)
    return bb, (bb[0] + mx, bb[1] + my, bb[2] - mx, bb[3] - my)


def render(blocks, paper):
    bb, (x0, y0, x1, y1) = text_box(paper)
    W, H = x1 - x0, y1 - y0
    lo, hi, best = 0.4, 1.7, None
    for _ in range(18):                          # biggest text that still fits
        mid = (lo + hi) / 2
        lines, th = layout(blocks, W, mid)
        widest = max([sum(t[3] for t in ln[1]) for ln in lines] or [0])
        if th <= H and widest <= W + 1:
            best, lo = (mid, lines, th), mid
        else:
            hi = mid
    if best is None:
        best = (lo, *layout(blocks, W, lo))
    _scale, lines, th = best
    d = ImageDraw.Draw(paper)
    y = y0 + max(0, (H - th) // 6)               # sit a little below the top
    for align, words, h, lw in lines:
        if align == "center":
            off = x0 + (W - lw) / 2
        elif align == "right":
            off = x1 - lw
        else:
            off = x0
        for w, st, x, wd in words:
            base_y = y + h - int(st["px"] * 1.3)
            d.text((off + x, base_y), w, font=st["font"], fill=st["fill"])
            if st.get("u") and w.strip():
                uy = base_y + int(st["px"] * 1.12)
                d.line((off + x, uy, off + x + wd, uy), fill=st["fill"], width=max(1, st["px"] // 14))
        y += h
    return paper.crop(bb)


def save(img, path):
    w, h = img.size
    s = OUT_W / max(w, h)
    if s < 1:
        img = img.resize((int(w * s), int(h * s)), Image.LANCZOS)
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, "AVIF", quality=62)


# ------------------------------------------------------------------ main
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lore", default=str(REPO / "dist" / "collectables_slasher_lore.json"))
    ap.add_argument("--data", help="game Data folder (paper textures are read from its ba2s)")
    ap.add_argument("--paper-dir", help="folder of already-pulled paper textures (.dds or .png, by file name)")
    ap.add_argument("--out", default=str(REPO / "out" / "lore-note-images"))
    ap.add_argument("--only", nargs="*", help="EditorIDs to make (default: every note)")
    a = ap.parse_args(argv)

    lore = json.load(open(a.lore, encoding="utf-8"))
    notes = [i for g in lore["groups"] for i in g["items"] if i.get("note") and i.get("image")]
    if a.only:
        notes = [n for n in notes if n["edid"] in a.only]
    unknown = sorted({n["note"].get("paper") or "?" for n in notes} - set(PAPER_TEXTURE))
    tex_for = {n["edid"]: PAPER_TEXTURE.get(n["note"].get("paper") or "", DEFAULT_PAPER) for n in notes}

    cache = {}
    if a.paper_dir:
        for p in glob.glob(os.path.join(a.paper_dir, "*")):
            base = os.path.basename(p).lower()
            for t in set(tex_for.values()):
                tb = os.path.basename(t).lower()
                if base == tb:
                    cache[t.lower()] = open_dds(open(p, "rb").read())
                elif base == tb[:-4] + ".png" and t.lower() not in cache:
                    cache[t.lower()] = Image.open(p).convert("RGBA")
    missing = [t for t in set(tex_for.values()) if t.lower() not in cache]
    if missing:
        data = a.data or LIVE_DATA
        if not os.path.isdir(data):
            sys.exit("paper textures not found - use --data (game Data folder) or --paper-dir")
        for k, raw in pull_textures(data, missing).items():
            cache[k] = open_dds(raw)
    out = Path(a.out)
    made = 0
    for n in notes:
        paper = paper_image(tex_for[n["edid"]], cache)
        if paper is None:
            print("  ! no paper texture for", n["edid"], tex_for[n["edid"]])
            continue
        img = render(n["note"]["blocks"], paper)
        save(img, out / n["image"].rsplit("/", 1)[-1])
        made += 1
    print("[lore notes] made {} pictures in {}".format(made, out))
    if unknown:
        print("  papers not in PAPER_TEXTURE (plain paper used):", ", ".join(unknown))


if __name__ == "__main__":
    main()
