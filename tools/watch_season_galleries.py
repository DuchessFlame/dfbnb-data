#!/usr/bin/env python3
"""
watch_season_galleries.py
-------------------------
Hourly watcher for the season gallery backlog.

The source screenshots live in OneDrive with Files On-Demand, so most of them
are cloud-only placeholders that cannot be read until Windows hydrates them.
They arrive gradually. This script is meant to be run on a schedule: each run
it looks at every outstanding season, and rebuilds the ones whose source files
have ALL become readable since last time.

Deliberately all-or-nothing per season. build_season_gallery_avif.py on its own
will skip an unreadable file and still write a manifest from whatever converted
- that is how the half-finished galleries were created. A manifest listing 6 of
22 images is worse than no manifest, because the page then renders a gallery
that looks complete. So a season is either fully converted or left alone.

Seasons 1-4 are calendar-only (their Ticket Prices folders are empty) and are
already correct. Season 26 is finished. Neither is touched.

USAGE
  python3 watch_season_galleries.py              # convert whatever is ready
  python3 watch_season_galleries.py --dry-run    # just report readiness
"""

import argparse
import glob
import json
import os
import subprocess
import sys

TOOLS = os.path.dirname(os.path.abspath(__file__))
BUILDER = os.path.join(TOOLS, "build_season_gallery_avif.py")

SEASONS = range(5, 26)          # 1-4 calendar-only, 26 done
IMG_EXT = (".png", ".jpg", ".jpeg", ".webp", ".jfif", ".avif")


def mount_root():
    env = os.environ.get("SEASON_IMAGES_ROOT")
    if env and os.path.isdir(env):
        return os.path.dirname(env.rstrip("/"))
    for base in sorted(glob.glob("/sessions/*/mnt")):
        if os.path.isdir(os.path.join(base, ".Season Images")):
            return base
    sys.exit("Could not find a mounted '.Season Images' folder under /sessions/*/mnt")


MNT = mount_root()
SRC_ROOT = os.path.join(MNT, ".Season Images")
STAGE_ROOT = os.path.join(MNT, "1 site-data", "json", "uploads",
                          "fo76", "season_images")


def season_folder(n):
    hits = [d for d in glob.glob(os.path.join(SRC_ROOT, "Season %d - *" % n))
            if os.path.isdir(d)]
    return hits[0] if hits else None


def sources(n, folder):
    """Every file the gallery for this season is built from."""
    out = [g for g in glob.glob(os.path.join(folder, "Community Calender*"))
           if g.lower().endswith(IMG_EXT)]
    tp = os.path.join(folder, "Season %d - Ticket Prices" % n)
    out += [p for p in glob.glob(os.path.join(tp, "*"))
            if p.lower().endswith(IMG_EXT)]
    return sorted(out)


def readable(path):
    try:
        with open(path, "rb") as fh:
            fh.read(64)
        return True
    except OSError:
        return False


def already_done(n, folder, n_src):
    """Complete means: AVIF count == manifest entries == source files, in both
    the working folder and the upload staging folder."""
    out = os.path.join(folder, "AVIF")
    stage = os.path.join(STAGE_ROOT, "season-%d" % n, "AVIF")
    counts = []
    for d in (out, stage):
        avif = len(glob.glob(os.path.join(d, "*.avif")))
        mp = os.path.join(d, "gallery.json")
        if not os.path.exists(mp):
            return False
        try:
            man = len(json.load(open(mp, encoding="utf-8")).get("images", []))
        except Exception:
            return False
        counts += [avif, man]
    return all(c == n_src for c in counts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    done, built, waiting, failed = [], [], [], []

    for n in SEASONS:
        folder = season_folder(n)
        if not folder:
            failed.append((n, "no source folder"))
            continue
        srcs = sources(n, folder)
        if not srcs:
            failed.append((n, "no source images"))
            continue

        if already_done(n, folder, len(srcs)):
            done.append(n)
            continue

        ready = sum(1 for s in srcs if readable(s))
        if ready < len(srcs):
            waiting.append((n, ready, len(srcs)))
            continue

        if a.dry_run:
            built.append((n, len(srcs)))
            continue

        r = subprocess.run(
            [sys.executable, BUILDER, "--season", str(n),
             "--stage", "--require-complete"],
            capture_output=True, text=True)
        if r.returncode == 0 and already_done(n, folder, len(srcs)):
            built.append((n, len(srcs)))
        else:
            failed.append((n, (r.stderr or r.stdout or "").strip()[-200:]))

    verb = "would build" if a.dry_run else "BUILT"
    print("Season gallery watcher")
    print("  already complete : %s" % (", ".join("S%d" % n for n in done) or "-"))
    print("  %-17s: %s" % (verb, ", ".join("S%d (%d imgs)" % (n, c)
                                           for n, c in built) or "-"))
    print("  still hydrating  : %s" % (", ".join("S%d %d/%d" % w
                                                 for w in waiting) or "-"))
    if failed:
        print("  FAILED:")
        for n, msg in failed:
            print("    S%d  %s" % (n, msg))

    remaining = len(waiting) + len(failed)
    print()
    print("  %d of %d outstanding seasons complete."
          % (len(done) + len(built), len(list(SEASONS))))
    if remaining == 0:
        print("  ALL DONE - nothing left waiting on OneDrive.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
