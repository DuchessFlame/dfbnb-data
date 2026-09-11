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

Page keys are taken from the page NUMBER in each file name (see page_info), with
"Bonus"/"Page_B1" files keyed b1, b2, ... matching season_tickets_s{N}.json.
Ticket-cost charts and ticket checklists become their own entries; duplicate
shots of one page and unnamed portrait images are skipped with a warning.

A gallery.json manifest is written alongside the images. df-bnb-seasons.js
reads THAT rather than guessing filenames, so a season whose reward JSON is
still an uncurated PTS stub still gets its full board strip. No manifest on the
server simply means no gallery on that page.

USAGE
  python3 build_season_gallery_avif.py --season 26
  python3 build_season_gallery_avif.py --season 26 --stage
  python3 build_season_gallery_avif.py --season 26 --stage --require-complete
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
    from PIL import Image, features
except ImportError:
    sys.exit("Missing deps. Run: pip install pillow pillow-avif-plugin")
try:
    import pillow_avif  # noqa: F401  (registers the AVIF plugin on Pillow < 11.3)
except ImportError:
    if not features.check("avif"):
        sys.exit("No AVIF encoder. Run: pip install pillow-avif-plugin (or Pillow >= 11.3)")


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


def page_info(path):
    """(is_bonus, page_number) for a board-page file.

    Only the number that belongs to the page counts. Matching any digit let the
    "2" in "BP2" and the "21" in "Season_21_Page_3" decide the order, which
    slotted the "Tickets to reach BP2" charts in as pages 3-6 (S20, S21) and
    shifted every real page after them.

    Handles: "Page 3", "S22_Page_3", "Page_Bonus_1", "S24_Page_B1", "Bonus Page 2",
    "pg 3", and names with no "page" at all ("VB_S26_BloodMoon_3_ria85z").
    """
    name = os.path.splitext(os.path.basename(path))[0].lower()
    bonus = "bonus" in name
    m = re.search(r"(?:page|pg)[\s_-]*(?:bonus[\s_-]*)?(b)?[\s_-]*(\d+)", name)
    if m:
        return (bonus or bool(m.group(1)), int(m.group(2)))
    rest = re.sub(r"season[\s_-]*\d+|s\d+|20\d\d", " ", name)
    m = re.search(r"\d+", rest)
    return (bonus, int(m.group(0)) if m else 0)


def natural_key(path):
    bonus, num = page_info(path)
    return (1 if bonus else 0, num, len(os.path.basename(path)), os.path.basename(path).lower())


# Ticket-cost charts and ticket checklists are not board pages. They get their
# own gallery entries (colour versions only - black-and-white ones are print copies):
#   "Tickets to reach BP2 - FOF.png", "Season 21 Minimum Ticket Cost - BP2 NON FOF.jpg"
#   "Season 24 Ticket Checklist - FOF.jpg"
TICKET_CHART_RE = re.compile(r"ticket\s*cost|tickets?\s*to\s*reach|\bbp\s*2\b|ticket\s*checklist", re.I)


def ticket_chart_name(path, season):
    name = os.path.basename(path).lower()
    if re.search(r"\b(bnw|black\s*n?\s*white|b\s*&\s*w)\b", name):
        return None
    kind = "nonfof" if re.search(r"non[\s_-]*fof|non[\s_-]*fallout", name) else "fof"
    what = "ticket_checklist" if re.search(r"ticket\s*checklist", name) else "bp2_tickets"
    return "s%d_%s_%s.avif" % (season, what, kind)


def is_portrait(path):
    """Board pages are landscape screenshots. A portrait image with no chart
    keyword in its name is something else (a checklist saved from Facebook, a
    poster) and must not be numbered as a page. Unreadable (cloud-only) files
    are assumed to be pages so --require-complete can report them."""
    try:
        with Image.open(path) as im:
            return im.height > im.width
    except OSError:
        return False


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
    m = re.fullmatch(r"ticket_checklist_(fof|nonfof)", stem)
    if m:
        return "Season Ticket Checklist (%sFallout 1st)" % ("Non " if m.group(1) == "nonfof" else "")
    m = re.fullmatch(r"bp2_tickets_(fof|nonfof)", stem)
    if m:
        return "Tickets to Reach Bonus Page 2 (%sFallout 1st)" % ("Non " if m.group(1) == "nonfof" else "")
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
    all_imgs = [p for p in glob.glob(os.path.join(tp, "*")) if p.lower().endswith(IMG_EXT)]
    charts = sorted(p for p in all_imgs if TICKET_CHART_RE.search(os.path.basename(p)))
    others = [p for p in all_imgs if p not in charts]
    odd = [p for p in others if is_portrait(p)]
    for p in odd:
        print("  SKIPPED %s - portrait image, not a board page. If it is a ticket chart,"
              " rename it to include 'Ticket Cost' or 'Ticket Checklist' plus 'FOF' or"
              " 'Non FOF'." % os.path.basename(p))
    pages, claimed = [], {}
    for p in sorted((p for p in others if p not in odd), key=natural_key):
        key = page_info(p)
        if key in claimed:
            # Two files for one page (an alternate shot like "Page_7_DoctorsAidBox",
            # or a social graphic like "graphic rewards pg 1"). natural_key puts the
            # shortest name first, so the plain page file wins.
            print("  SKIPPED %s - duplicate of %s"
                  % (os.path.basename(p), os.path.basename(claimed[key])))
            continue
        claimed[key] = p
        pages.append(p)
    if len(pages) == 1:
        jobs.append((pages[0], "s%d_board.avif" % n))
    else:
        for src in pages:
            bonus, num = page_info(src)
            jobs.append((src, "s%d_page_%s%d.avif" % (n, "b" if bonus else "", num)))
    seen = set()
    for src in charts:
        name = ticket_chart_name(src, n)
        if name and name not in seen:
            seen.add(name)
            jobs.append((src, name))

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
