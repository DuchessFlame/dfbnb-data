#!/usr/bin/env python3
"""
convert_season_images_to_avif.py
--------------------------------
Repeatable DDS -> AVIF converter for the DF/BNB season reward images.

Given a season image source folder (the raw .dds export dropped in
"...\\.Season Images\\Season {N}\\"), this converts the icon/product .dds files
to alpha-preserving AVIF and stages them into destination subfolders that map
1:1 to the FileZilla upload targets on the WP Engine server.

SOURCE VARIANT RULES (Season 26 convention):
  *_l.dds     -> the transparent "large" inventory icon. Primary reward image.
                 Routed by name:
                   *camptitles*   -> avif/titles-camp/    (storefront/titles-camp/)
                   *playertitles* -> avif/titles-player/  (storefront/titles-player/)
                   atx_s{N}_*     -> avif/_unmatched_hold/ (set-dressing; DO NOT upload)
                   everything else-> avif/season-{N}/      (season_images/season-{N}/)
  *_c1/_c2.dds-> storefront product/carousel shots (usually opaque). Only
                 converted for stems named in --carousel-stems, routed to
                 avif/camp-utility/ (storefront/camp-utility/). Used by the
                 weather-station / camp-utility pages whose JSON imageUrl +
                 imageCarousel already reference *_c1.avif / *_c2.avif.
  no-suffix    -> e.g. score_s{N}_playericon_*.dds. Convert ONLY with
                 --player-icons (routed to avif/season-{N}/). Off by default
                 because player icons have no _l variant and are opt-in.

Output name = source filename with the variant suffix stripped (for _l),
lowercased, .avif. This matches how df-bnb-upcoming-rewards.js derives the URL
from ddsHandle and how the titles/weather build scripts write imageUrl.

USAGE:
  python tools/convert_season_images_to_avif.py --src "C:\\...\\Season 26" --season 26
  python tools/convert_season_images_to_avif.py --src "...\\Season 26" --season 26 \
      --carousel-stems score_s26_camp_utility_weatherstation_bloodmoon
  python tools/convert_season_images_to_avif.py --src "...\\Season 26" --season 26 --player-icons

Requires: pip install pillow pillow-avif-plugin  (Pillow reads DXT5/BC3 DDS).
"""

import argparse
import glob
import os
import re
import sys

try:
    import pillow_avif  # noqa: F401  (registers the AVIF plugin)
    from PIL import Image
except ImportError:
    sys.exit("Missing deps. Run: pip install pillow pillow-avif-plugin")

QUALITY = 72


def out_name_l(fname):
    """_l.dds source -> lowercased basename with trailing _l stripped + .avif"""
    return re.sub(r"_l\.dds$", "", fname, flags=re.I).lower() + ".avif"


def route_l(name, season):
    if "camptitles" in name:
        return "titles-camp"
    if "playertitles" in name:
        return "titles-player"
    if name.startswith("atx_s%d_" % season) or name.startswith("atx_s"):
        return "_unmatched_hold"
    return "season-%d" % season


def convert(src_path, dst_path):
    im = Image.open(src_path).convert("RGBA")
    alpha = im.getchannel("A").getextrema()
    im.save(dst_path, format="AVIF", quality=QUALITY)
    # verify round-trip alpha survives
    chk = Image.open(dst_path).convert("RGBA").getchannel("A").getextrema()
    ok = (alpha == chk)
    return im.size, alpha, ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="Season image source folder (.dds live here)")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--carousel-stems", nargs="*", default=[],
                    help="Stems (without _c1/_c2) to convert as camp-utility carousel shots.")
    ap.add_argument("--player-icons", action="store_true",
                    help="Also convert no-suffix score_s{N}_playericon_*.dds into season-{N}/.")
    args = ap.parse_args()

    src = args.src
    n = args.season
    avif_root = os.path.join(src, "avif")
    for d in ("season-%d" % n, "titles-camp", "titles-player", "camp-utility", "_unmatched_hold"):
        os.makedirs(os.path.join(avif_root, d), exist_ok=True)

    total = fails = 0

    # --- _l.dds icons ---
    for path in sorted(glob.glob(os.path.join(src, "*_l.dds"))):
        fname = os.path.basename(path)
        name = out_name_l(fname)
        sub = route_l(name, n)
        dst = os.path.join(avif_root, sub, name)
        size, alpha, ok = convert(path, dst)
        total += 1
        if not ok:
            fails += 1
        print("[_l] %-55s -> %s/%s  %s alpha=%s%s" % (
            fname, sub, name, size, alpha, "" if ok else "  !!ALPHA MISMATCH"))

    # --- _c1/_c2 carousel shots for named stems ---
    for stem in args.carousel_stems:
        for suf in ("_c1", "_c2", "_c3", "_c4"):
            path = os.path.join(src, stem + suf + ".dds")
            if not os.path.exists(path):
                continue
            name = (stem + suf + ".avif").lower()
            dst = os.path.join(avif_root, "camp-utility", name)
            size, alpha, ok = convert(path, dst)
            total += 1
            print("[car] %-55s -> camp-utility/%s  %s alpha=%s" % (
                os.path.basename(path), name, size, alpha))

    # --- opt-in no-suffix player icons ---
    if args.player_icons:
        for path in sorted(glob.glob(os.path.join(src, "score_s%d_playericon_*.dds" % n))):
            fname = os.path.basename(path)
            if fname.endswith("_l.dds"):
                continue
            name = fname.replace(".dds", ".avif").lower()
            dst = os.path.join(avif_root, "season-%d" % n, name)
            size, alpha, ok = convert(path, dst)
            total += 1
            print("[icon] %-53s -> season-%d/%s  %s alpha=%s" % (fname, n, name, size, alpha))

    print("\nDone: %d converted, %d alpha mismatches. Staged under: %s" % (total, fails, avif_root))
    print("Upload map:")
    print("  avif/season-%d/     -> /wp-content/uploads/season_images/season-%d/" % (n, n))
    print("  avif/titles-camp/   -> /wp-content/uploads/storefront/titles-camp/")
    print("  avif/titles-player/ -> /wp-content/uploads/storefront/titles-player/")
    print("  avif/camp-utility/  -> /wp-content/uploads/storefront/camp-utility/")
    print("  avif/_unmatched_hold/  (DO NOT UPLOAD - set-dressing / unmatched)")


if __name__ == "__main__":
    main()
