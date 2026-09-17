#!/usr/bin/env python3
r"""
map_watermark.py — Duchess Flame's watermark, applied to EVERY map this repo renders.

WHY THIS EXISTS
===============
The July 2026 Slasher maps shipped with no mark on them and were reposted without
credit. Standing rule from here on: a map that leaves this repo carries the
watermark. Full maps, numbered maps, region tiles, spawn maps — all of them.

WHAT IT DRAWS
=============
Two layers, because one alone is not enough:

  1. A corner BADGE — the logo plus theduchessflame.com, readable, bottom-left by
     default. This is the credit.
  2. A repeating DIAGONAL TILE of the same wordmark at low opacity across the whole
     image. This is the part that survives a crop: someone lifting a section of the
     map still lifts the mark with it.

Both are drawn onto a copy, so a caller can keep its clean render if it wants one.

USAGE
-----
    import map_watermark
    img = map_watermark.apply(img)                  # defaults, 4096px full map
    img = map_watermark.apply(img, badge=False)     # tile only (region tiles)

The logo is looked up under the branding folder and is OPTIONAL: if it can't be
read (OneDrive keeps some of those files cloud-only), the badge falls back to
text alone rather than failing the render. A map still gets marked.
"""

from __future__ import annotations

import os

from PIL import Image, ImageDraw, ImageFont

SITE = "theduchessflame.com"
CREDIT = "Map by Duchess Flame"

GUIDES_ROOT = os.environ.get(
    "GUIDES_ROOT",
    os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff"))

# First readable one wins. Add to the front to change the mark.
LOGO_CANDIDATES = [
    os.path.join(GUIDES_ROOT, "!!!Duchess Flame Branding", "Logos and banners",
                 "Duchess Flame spider no background logo (2).png"),
    os.path.join(GUIDES_ROOT, "!!!Duchess Flame Branding", "Logos and banners",
                 "Duchessflame spider.png"),
]

FONT_CANDIDATES = [
    r"C:\Windows\Fonts\segoeuib.ttf",
    r"C:\Windows\Fonts\arialbd.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
]


def _font(size):
    for p in FONT_CANDIDATES:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _key_out_white(img, thresh=238):
    """Make a flat white background transparent.

    Some of the logo files are saved WITH a white backing rather than alpha (the
    "no background" one is usually cloud-only, so it can't be relied on). Dropped
    onto a dark plate, that backing shows as an ugly white card, so near-white
    pixels connected to the border are cleared. Only runs when the image has no
    real alpha of its own.
    """
    if img.getextrema()[3][0] < 250:
        return img                       # already has transparency — leave it alone
    px = img.load()
    W, H = img.size
    seen = [[False] * W for _ in range(H)]
    stack = [(x, y) for x in range(W) for y in (0, H - 1)]
    stack += [(x, y) for y in range(H) for x in (0, W - 1)]
    while stack:
        x, y = stack.pop()
        if x < 0 or y < 0 or x >= W or y >= H or seen[y][x]:
            continue
        r, g, b, a = px[x, y]
        if r < thresh or g < thresh or b < thresh:
            continue
        seen[y][x] = True
        px[x, y] = (r, g, b, 0)
        stack += [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]
    return img


def _logo():
    """The logo as RGBA, or None. A cloud-only OneDrive file raises on read — that
    is not a reason to publish an unmarked map, so the caller falls back to text."""
    for p in LOGO_CANDIDATES:
        try:
            im = Image.open(p).convert("RGBA")
        except Exception:
            continue
        im.thumbnail((512, 512), Image.LANCZOS)   # flood fill is O(pixels)
        return _key_out_white(im)
    return None


def _tile_layer(size, opacity, angle=30, step_scale=1.0):
    """A full-size transparent layer of repeated, rotated wordmarks."""
    W, H = size
    f = _font(max(22, int(min(W, H) * 0.018)))
    stamp_txt = f"  {SITE}  "
    tmp = Image.new("RGBA", (10, 10))
    box = ImageDraw.Draw(tmp).textbbox((0, 0), stamp_txt, font=f)
    tw, th = box[2] - box[0], box[3] - box[1]

    stamp = Image.new("RGBA", (tw + 20, th + 20), (0, 0, 0, 0))
    ImageDraw.Draw(stamp).text((10, 4), stamp_txt, font=f,
                               fill=(255, 255, 255, int(255 * opacity)))
    stamp = stamp.rotate(angle, expand=True, resample=Image.BICUBIC)

    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sx = int(stamp.width * 1.9 * step_scale)
    sy = int(stamp.height * 3.4 * step_scale)
    row = 0
    y = -stamp.height
    while y < H + stamp.height:
        x = -stamp.width + (sx // 2 if row % 2 else 0)
        while x < W + stamp.width:
            layer.alpha_composite(stamp, (x, y))
            x += sx
        y += sy
        row += 1
    return layer


def _badge(img, corner="bottom-left"):
    """Logo + credit line in one corner, on a dark rounded plate so it stays legible
    over both the pale map paper and the dark nuke zones."""
    W, H = img.size
    scale = min(W, H) / 4096.0
    pad = int(40 * scale)
    logo_px = int(260 * scale)

    f_site = _font(max(14, int(58 * scale)))
    f_credit = _font(max(11, int(34 * scale)))

    logo = _logo()
    if logo is not None:
        logo = logo.copy()
        logo.thumbnail((logo_px, logo_px), Image.LANCZOS)

    tmp = ImageDraw.Draw(Image.new("RGBA", (10, 10)))
    tw = max(tmp.textbbox((0, 0), SITE, font=f_site)[2],
             tmp.textbbox((0, 0), CREDIT, font=f_credit)[2])

    inner = int(22 * scale)
    lw = logo.width + inner if logo is not None else 0
    bw = lw + tw + inner * 2
    bh = max(logo.height if logo is not None else 0, int(110 * scale)) + inner * 2

    plate = Image.new("RGBA", (bw, bh), (18, 18, 18, 205))
    d = ImageDraw.Draw(plate)
    d.rectangle([0, 0, bw - 1, bh - 1], outline=(255, 255, 255, 190),
                width=max(1, int(3 * scale)))
    x = inner
    if logo is not None:
        plate.alpha_composite(logo, (x, (bh - logo.height) // 2))
        x += logo.width + inner
    d.text((x, inner + int(6 * scale)), SITE, font=f_site, fill=(255, 255, 255, 240))
    d.text((x, inner + int(6 * scale) + int(64 * scale)), CREDIT, font=f_credit,
           fill=(235, 200, 90, 235))

    pos = {
        "bottom-left":  (pad, H - bh - pad),
        "bottom-right": (W - bw - pad, H - bh - pad),
        "top-left":     (pad, pad),
        "top-right":    (W - bw - pad, pad),
    }[corner]
    img.alpha_composite(plate, pos)
    return img


def apply(img, badge=True, corner="bottom-left", tile=True, tile_opacity=0.13):
    """Return a watermarked COPY of img (RGB in, RGB out).

    badge — the readable credit plate. Off for a thumbnail too small to carry it.
    tile  — the repeated diagonal mark. Leave it on: it is what survives a crop.
    """
    base = img.convert("RGBA")
    if tile:
        base = Image.alpha_composite(base, _tile_layer(base.size, tile_opacity))
    if badge:
        base = _badge(base, corner)
    return base.convert("RGB")


if __name__ == "__main__":
    import sys
    src, dst = sys.argv[1], sys.argv[2]
    Image.MAX_IMAGE_PIXELS = None
    apply(Image.open(src)).save(dst, "JPEG", quality=92, subsampling=0)
    print("watermarked ->", dst)
