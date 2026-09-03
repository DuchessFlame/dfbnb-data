#!/usr/bin/env python3
"""
build_season_gallery_avif.py
----------------------------
Builds the scoreboard-page GALLERY images for a season: the community calendar
plus every board / "ticket price" page, converted to AVIF.

Source  : ".Season Images/Season {N} - *"
            Community Calender S{N}.*          -> s{N}_calendar.avif
            Season {N} - Ticket Prices/*        -> s{N}_page_{key}.avif
          (board-game seasons S1-S8 have a single scoreboard image instead of
           numbered pages -> s{N}_board.avif)

Output  : <season folder>/AVIF/          (working copy, lives with the source)
          and, with --stage, a mirror into the WP upload staging folder:
          1 site-data/json/uploads/fo76/season_images/season-{N}/AVIF/

Page keys are taken from the ORDER of the source files, with any file whose
name contains "bonus" treated as B1, B2, ... matching the page keys used in
season_tickets_s{N}.json.

A gallery.json manifest is written alongside the images. df-bnb-seasons.js
reads THAT rather than guessing filenames, so a season whose reward JSON is
still an uncurated PTS stub still gets its full board strip. No manifest on the
server simply means no gallery on that page.

USAGE
  python3 build_season_gallery_avif.py --season 26
  python3 build_season_gallery_avif.py --season 26 --stage
  python3 build_season_gallery_avif.py --season 26 --max-width 1600 --quality 62
"""

import argparse
import json
import os
import re
import sys
import glob
import shutil

try:
    import pillow_avif  # noqa: F401  (registers the AVIF plugin)
    from PIL import Image
except ImportError:
    sys.exit("Missing deps. Run: pip install pillow pillow-avif-plugin")

def _mount_root():
    """The sandbox session id changes every session, so the mount prefix cannot
    be hardcoded - it was, and every later session broke. Find the live one."""
    env = os.environ.get("SEASON_IMAGES_ROOT")
    if env and os.path.isdir(env):
        return os.path.dirname(env.rstrip("/"))
    for base in sorted(glob.glob("/sessions/*/mnt")):
        if os.path.isdir(os.path.join(base, ".Season Images")):
            return base
    sys.exit("Could not find a mounted '.Season Images' folder under /sessions/*/mnt")


_MNT = _mount_root()
SEASON_IMAGES_ROOT = os.path.join(_MNT, ".Season Images")
STAGE_ROOT = os.path.join(_MNT, "1 site-data", "json", "uploads",
                          "fo76", "season_images")

IMG_EXT = (".png", ".jpg", ".jpeg", ".webp", ".jfif", ".avif")


def season_folder(n):
    hits = [d for d in glob.glob(os.path.join(SEASON_IMAGES_ROOT, "Season %d - *" % n))
            if os.path.isdir(d)]
    if not hits:
        sys.exit("No folder found for Season %d under %s" % (n, SEASON_IMAGES_ROOT))
    return hits[0]


def natural_key(path):
    """Sort page_2 before page_10, and push bonus pages to the end."""
    name = os.path.basename(path).lower()
    bonus = 1 if "bonus" in name else 0
    nums = [int(x) for x in re.findall(r"\d+", re.sub(r"s\d+|20\d\d", "", name))]
    return (bonus, nums or [0], name)


def page_keys(files):
    """Assign the season_tickets page keys: 1..N then B1..Bn."""
    keys, n, b = [], 0, 0
    for f in files:
        if "bonus" in os.path.basename(f).lower():
            b += 1
            keys.append("b%d" % b)
        else:
            n += 1
            keys.append(str(n))
    return keys


def caption_for(name, season):
    """s26_page_b1.avif -> 'Bonus Page 1'. Matches the labels groupByPage()
    puts on the page-group headers so the gallery and the list agree."""
    stem = re.sub(r"^s%d_" % season, "", os.path.splitext(name)[0])
    if stem == "board":
        return "Scoreboard"
    if stem.startswith("calendar"):
        tail = stem[len("calendar"):].lstrip("_")
        if not tail:
            return "Community Calendar"
        return "Community Calendar (%s)" % (tail.upper() if re.fullmatch(r"q\d", tail) else tail)
    m = re.fullmatch(r"page_b(\d+)", stem)
    if m:
        return "Bonus Page %s" % m.group(1)
    m = re.fullmatch(r"page_(\d+)", stem)
    if m:
        return "Page %s" % m.group(1)
    return stem.replace("_", " ").title()


def convert(src, dst, max_width, quality):
    im = Image.open(src)
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGBA" if "A" in im.getbands() else "RGB")
    if max_width and im.width > max_width:
        h = round(im.height * max_width / im.width)
        im = im.resize((max_width, h), Image.LANCZOS)
    im.save(dst, format="AVIF", quality=quality)
    return im.size, os.path.getsize(src), os.path.getsize(dst)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--max-width", type=int, default=1920)
    ap.add_argument("--quality", type=int, default=68)
    ap.add_argument("--stage", action="store_true",
                    help="also copy into the WP upload staging folder")
    ap.add_argument("--require-complete", action="store_true",
                    help="build ONLY if every source file is readable; a season "
                         "with any cloud-only placeholder is left untouched "
                         "rather than written as a partial manifest")
    a = ap.parse_args()

    n = a.season
    folder = season_folder(n)
    out = os.path.join(folder, "AVIF")
    os.makedirs(out, exist_ok=True)
    print("Season %d: %s" % (n, os.path.basename(folder)))

    jobs = []

    # 1. community calendar (there may be more than one, e.g. (Q1)/(Q2))
    cals = sorted(g for g in glob.glob(os.path.join(folder, "Community Calender*"))
                  if g.lower().endswith(IMG_EXT))
    for i, c in enumerate(cals):
        m = re.search(r"\((Q\d|revised)\)", os.path.basename(c), re.I)
        suffix = "_" + m.group(1).lower() if m else ("" if i == 0 else "_%d" % (i + 1))
        jobs.append((c, "s%d_calendar%s.avif" % (n, suffix)))

    # 2. board / ticket-price pages
    tp = os.path.join(folder, "Season %d - Ticket Prices" % n)
    pages = sorted((p for p in glob.glob(os.path.join(tp, "*"))
                    if p.lower().endswith(IMG_EXT)), key=natural_key)
    if len(pages) == 1:
        jobs.append((pages[0], "s%d_board.avif" % n))
    else:
        for src, key in zip(pages, page_keys(pages)):
            jobs.append((src, "s%d_page_%s.avif" % (n, key)))

    if not jobs:
        sys.exit("Nothing to convert for Season %d" % n)

    # A manifest listing 6 of 22 images is worse than no manifest at all: the
    # page renders a gallery that silently claims to be complete. So under
    # --require-complete a season is all-or-nothing.
    if a.require_complete:
        unreadable = []
        for src, _name in jobs:
            try:
                with open(src, "rb") as fh:
                    fh.read(64)
            except OSError as e:
                unreadable.append((os.path.basename(src), e.__class__.__name__))
        if unreadable:
            print("  INCOMPLETE - %d of %d source files still unreadable "
                  "(cloud-only). Leaving Season %d untouched."
                  % (len(unreadable), len(jobs), n))
            for base, cls in unreadable[:5]:
                print("    %s (%s)" % (base, cls))
            if len(unreadable) > 5:
                print("    ... and %d more" % (len(unreadable) - 5))
            return 1

    # Clean rebuild: an earlier partial run can leave AVIFs that are no longer
    # in the manifest. Clear them so output always equals the manifest exactly.
    for stale in glob.glob(os.path.join(out, "*.avif")):
        os.remove(stale)
    stale_manifest = os.path.join(out, "gallery.json")
    if os.path.exists(stale_manifest):
        os.remove(stale_manifest)

    tot_in = tot_out = 0
    manifest = []
    skipped = []
    for src, name in jobs:
        dst = os.path.join(out, name)
        try:
            size, bi, bo = convert(src, dst, a.max_width, a.quality)
        except OSError as e:
            # OneDrive Files On-Demand: a cloud-only placeholder cannot be read
            # until Windows has hydrated it. Skip rather than abort so the rest
            # of the season still builds, and report at the end.
            skipped.append((os.path.basename(src), e))
            if os.path.exists(dst):
                os.remove(dst)
            continue
        tot_in += bi
        tot_out += bo
        manifest.append({"file": name, "caption": caption_for(name, n),
                         "w": size[0], "h": size[1]})
        print("  %-22s %5dx%-5d %7.2f MB -> %6.2f MB   (%s)"
              % (name, size[0], size[1], bi / 1e6, bo / 1e6, os.path.basename(src)))

    if skipped:
        print("  SKIPPED %d unreadable source file(s):" % len(skipped))
        for base, e in skipped:
            print("    %s  (%s)" % (base, e.__class__.__name__))
        print("    If these are OneDrive cloud-only files, right-click the")
        print("    .Season Images folder -> 'Always keep on this device', wait")
        print("    for the sync to finish, then re-run this command.")

    if not manifest:
        sys.exit("  Nothing converted for Season %d - manifest not written." % n)

    # The renderer reads this instead of guessing filenames, so the gallery does
    # not depend on season_tickets_s{N}.json being curated yet - a season whose
    # reward list is still a PTS stub still gets its full board strip.
    with open(os.path.join(out, "gallery.json"), "w", encoding="utf-8") as fh:
        json.dump({"season": n, "images": manifest}, fh, indent=1)
    print("  gallery.json    %d entries" % len(manifest))

    print("  %d files  %.1f MB -> %.1f MB  (%.0f%% smaller)"
          % (len(jobs), tot_in / 1e6, tot_out / 1e6, 100 * (1 - tot_out / tot_in)))

    if a.stage:
        stage = os.path.join(STAGE_ROOT, "season-%d" % n, "AVIF")
        os.makedirs(stage, exist_ok=True)
        for stale in (glob.glob(os.path.join(stage, "*.avif"))
                      + glob.glob(os.path.join(stage, "gallery.json"))):
            os.remove(stale)
        staged = 0
        for f in glob.glob(os.path.join(out, "*.avif")) + [os.path.join(out, "gallery.json")]:
            shutil.copy2(f, stage)
            staged += 1
        print("  staged %d files -> %s" % (staged, stage))


if __name__ == "__main__":
    sys.exit(main() or 0)
