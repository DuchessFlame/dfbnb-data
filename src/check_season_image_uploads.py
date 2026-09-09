#!/usr/bin/env python3
"""Find the season manifest rows the site does not actually serve.

A per-season manifest (``dist/season_images/season_{N}_images.json``) is a
TO-DO list: its own note says "extract each ddsPath ... then upload with
sync_season_images_to_site.ps1". A row appears the moment a reward is curated,
whether or not the tile was ever extracted, converted or uploaded.

``src/reusable_images.py`` reads those manifests to answer "is this art already
hosted?", so every unuploaded row is a booby trap: a CAMP page asks for a tile,
the index hands back a season URL that 404s, and the page never falls through to
the ETDI-named copy sitting in its own camp-items folder. That is exactly how
the Silver Collectron (S8) and the Zetan Matrix Collectron (S7) ended up broken
on both the collectrons page and their scoreboards.

Nothing in the repo records what was actually uploaded, and the two things that
look like they might are unreliable in both directions:

  * the manifest over-claims (rows never uploaded), and
  * ``missing_images.tsv`` goes stale the moment an upload run happens — at the
    last check 40 of its rows were serving fine.

So this asks the only authority there is: the server. It HEAD-checks every
manifest URL and writes the verified-missing set to
``dist/season_images/unpublished_images.json``.

Player icons, player titles and CAMP titles are reported separately and left out
of the exclusion file. They are shared-folder art — ``dfbnbAssetUrl()`` in the
front end reroutes them by filename to /guide-images/atom-shop/player-icons/ and
/guide-images/titles/ — so their season-folder URL is expected to 404 and is
never requested.

Run it after an upload run, and after adding a season. It is network-bound and
read-only against the site.

USAGE:
  python src/check_season_image_uploads.py
  python src/check_season_image_uploads.py --dist dist --site https://www.buffsnbrew.com
  python src/check_season_image_uploads.py --dry-run          # report, write nothing
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import glob
import json
import os
import re
import urllib.error
import urllib.request

TAG = "[check_season_image_uploads]"

# Art that lives in a shared folder and is rerouted by filename in the front end,
# so its season-folder URL is expected to 404 and must not be excluded (or
# "fixed") on the strength of that.
SHARED_ART = re.compile(r"playericon|playertitles|camptitles", re.IGNORECASE)

DEFAULT_SITE = "https://www.buffsnbrew.com"


def log(msg):
    print("{} {}".format(TAG, msg))


def manifest_rows(dist_dir):
    """Every (season, url, outAvif, name, entitlement) the manifests claim."""
    rows = {}
    for path in sorted(glob.glob(os.path.join(dist_dir, "season_images", "season_*_images.json"))):
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            log("skipping {}: {}".format(os.path.basename(path), exc))
            continue
        base = str(data.get("uploadTo") or "").rstrip("/")
        for img in data.get("images") or []:
            out = str(img.get("outAvif") or "").strip()
            folder = str(img.get("uploadTo") or base or "").rstrip("/")
            if not out or not folder:
                continue
            rows.setdefault(out.lower(), {
                "outAvif": out,
                "season": data.get("seasonNumber"),
                "name": img.get("name") or "",
                "entitlement": img.get("entitlement") or "",
                "path": "{}/{}".format(folder, out),
            })
    return list(rows.values())


def head(url, timeout=20):
    """True if the URL serves 200. Anything else — including a transport error —
    is reported as its status so a network wobble is not silently filed as a
    missing image."""
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception:
        return "ERR"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", default="dist")
    ap.add_argument("--site", default=DEFAULT_SITE, help="site root, no trailing slash")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    args = ap.parse_args()

    site = args.site.rstrip("/")
    rows = manifest_rows(args.dist)
    if not rows:
        log("no manifest rows found under {} — nothing to check".format(args.dist))
        return 1
    log("checking {} manifest image(s) against {}".format(len(rows), site))

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        statuses = list(pool.map(lambda r: head(site + r["path"]), rows))

    errors = [r for r, s in zip(rows, statuses) if s == "ERR"]
    if errors:
        log("{} request(s) failed outright — NOT writing, rerun when the network "
            "is steady (first: {})".format(len(errors), errors[0]["path"]))
        return 2

    missing = [r for r, s in zip(rows, statuses) if s != 200]
    shared = [r for r in missing if SHARED_ART.search(r["outAvif"])]
    real = [r for r in missing if not SHARED_ART.search(r["outAvif"])]
    real.sort(key=lambda r: (r["season"] or 0, r["outAvif"]))

    log("{} serving, {} missing ({} genuinely unpublished, {} shared-folder art "
        "that is rerouted by filename)".format(
            len(rows) - len(missing), len(missing), len(real), len(shared)))

    by_season = {}
    for r in real:
        by_season[r["season"]] = by_season.get(r["season"], 0) + 1
    if by_season:
        log("unpublished by season: " + ", ".join(
            "S{}={}".format(k, v) for k, v in sorted(by_season.items(), key=lambda kv: kv[0] or 0)))

    if args.dry_run:
        for r in real:
            print("  S{:<3} {:<58} {}".format(r["season"], r["outAvif"], r["name"]))
        return 0

    out_path = os.path.join(args.dist, "season_images", "unpublished_images.json")
    doc = {
        "_generated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_note": (
            "Manifest rows whose image is NOT on the server. A season manifest is a TO-DO "
            "list ('extract each ddsPath ... then upload with sync_season_images_to_site.ps1'), "
            "not a record of what was uploaded, so reusable_images.py must subtract this file "
            "before treating a manifest row as hosted art. Regenerate with "
            "src/check_season_image_uploads.py. Do not hand-edit: an entry that is wrong in "
            "either direction sends a CAMP page to a 404 or re-uploads a tile that exists."),
        "_method": "HTTP HEAD against {} for every outAvif in dist/season_images/season_*_images.json".format(site),
        "_checked": len(rows),
        "_excludes": (
            "Player icons, player titles and CAMP titles are left out: they are shared-folder "
            "art that df-bnb-*.js reroutes by filename, so their season-folder URL is expected "
            "to 404 and is never used."),
        "count": len(real),
        "images": [{k: r[k] for k in ("outAvif", "season", "name", "entitlement")} for r in real],
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    log("wrote {} ({} entries)".format(out_path, len(real)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
