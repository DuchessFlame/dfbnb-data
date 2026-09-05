#!/usr/bin/env python3
"""One lookup for art the site already hosts.

A tile uploaded twice is a tile that drifts, and storage costs money, so before
a builder invents a fresh image path it asks here whether the site is already
serving that art somewhere else. Everything is read out of ``dist/`` itself —
each already-published JSON is a record of what has been uploaded — so there is
no separate manifest to keep in step with reality.

Sources, in the order a hit wins:

  1. ``dist/season_images/season_*_images.json`` — the per-season upload
     manifests. 1,700-odd entries, each carrying the entitlement EDID, the
     source ``.dds`` path and the ``.avif`` name it was published under. This is
     the set that matters most: anything that came off a Scoreboard already has
     its tile here, whatever page it later shows up on.
  2. ``dist/atom_shop.json`` and ``dist/bundles.json`` — the Atom Shop and
     bundle sets, which carry main tiles for a scattering of CAMP items.

Two keys are indexed for every image:

  * **EDID** — exact, used when the record needing art carries one.
  * **texture stem** — the ETDI/ECIL filename with its extension and any
    ``_l`` / ``_c1`` / ``_c2`` / ``_c3`` suffix stripped. This is what actually
    names the file on disk, so a tile uploaded for a season page is still found
    by the CAMP page that wants the same item.

The stem match is deliberately suffix-insensitive: a season manifest may hold
``foo.avif`` while a CAMP builder asks for ``foo_l``, and they are the same
picture. Where several images share a stem the largest-tile candidate wins
(``_l`` over ``_c2``), because the main image on an item expand must be the
single-item transparent tile — see the camp-item-expands skill.
"""

from __future__ import annotations

import glob
import json
import os
import re

_IMG_EXT = (".avif", ".webp", ".png", ".jpg", ".jpeg")
_SUFFIX = re.compile(r"_(?:l|c\d)$", re.IGNORECASE)


def texture_stem(value):
    """Reduce any texture path, filename or URL to its comparable stem.

    ``Textures/ATX/Storefront/Camp/ATX_CAMP_Collector_CookieJar_L.dds`` and
    ``/wp-content/uploads/season_images/season-4/atx_camp_collector_cookiejar.avif``
    both reduce to ``atx_camp_collector_cookiejar``.
    """
    if not value:
        return ""
    name = os.path.splitext(os.path.basename(str(value).strip()))[0].lower()
    name = re.sub(r"__+", "_", name)
    return _SUFFIX.sub("", name)


def _rank(url):
    """Prefer the main single-item tile when a stem has several images."""
    stem = os.path.splitext(os.path.basename(url))[0].lower()
    if stem.endswith("_l"):
        return 0
    if re.search(r"_c\d$", stem):
        return 2
    return 1


class Index:
    """EDID and texture-stem lookup over the art the site already serves."""

    def __init__(self):
        self.by_edid = {}
        self.by_stem = {}
        self.sources = []

    def _add(self, edid, stem, url):
        if not url or not url.lower().endswith(_IMG_EXT):
            return
        if edid:
            self.by_edid.setdefault(edid.strip().upper(), url)
        if stem:
            cur = self.by_stem.get(stem)
            if cur is None or _rank(url) < _rank(cur):
                self.by_stem[stem] = url

    def find(self, edid="", stem="", texture=""):
        """Return a hosted URL for this item, or "" if the site has no art.

        ``texture`` is a convenience: pass the raw ETDI/ECIL value and it is
        reduced to a stem for you.
        """
        if edid:
            hit = self.by_edid.get(str(edid).strip().upper())
            if hit:
                return hit
        key = stem or texture_stem(texture)
        if key:
            hit = self.by_stem.get(_SUFFIX.sub("", str(key).strip().lower()))
            if hit:
                return hit
        return ""

    def __len__(self):
        return len(self.by_stem) + len(self.by_edid)

    def summary(self):
        return "{} image(s) already hosted ({} by EDID, {} by texture stem) from {}".format(
            len(self), len(self.by_edid), len(self.by_stem), ", ".join(self.sources) or "nothing")


def build_index(dist_dir):
    """Scan dist/ for art the site already serves. Never raises."""
    idx = Index()
    if not dist_dir or not os.path.isdir(dist_dir):
        return idx

    # --- 1. season upload manifests -----------------------------------------
    season_files = sorted(glob.glob(os.path.join(dist_dir, "season_images", "season_*_images.json")))
    n = 0
    for path in season_files:
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        base = str(data.get("uploadTo") or "").rstrip("/")
        for img in data.get("images") or []:
            out = str(img.get("outAvif") or "").strip()
            if not out:
                continue
            folder = str(img.get("uploadTo") or base or "").rstrip("/")
            if not folder:
                continue
            url = "{}/{}".format(folder, out)
            idx._add(img.get("entitlement") or "", texture_stem(img.get("ddsPath") or out), url)
            n += 1
    if n:
        idx.sources.append("{} season manifest(s)".format(len(season_files)))

    # --- 2. atom shop + bundles ---------------------------------------------
    for fname in ("atom_shop.json", "bundles.json"):
        path = os.path.join(dist_dir, fname)
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        got = 0
        for it in data.get("items") or []:
            url = str(it.get("imageUrl") or "").strip()
            if not url:
                continue
            idx._add(it.get("edid") or "", texture_stem(it.get("imageDds") or url), url)
            got += 1
        if got:
            idx.sources.append(fname)

    return idx


if __name__ == "__main__":
    import sys
    d = sys.argv[1] if len(sys.argv) > 1 else "dist"
    ix = build_index(d)
    print(ix.summary())
    for probe in ("ATX_CAMP_Collector_CookieJar.dds", "score_s24_camp_utility_phoropter",
                  "SCORE_S24_ENTM_CAMP_Utility_WeatherStation_Invasion"):
        print("  {:<58} -> {}".format(probe, ix.find(edid=probe, texture=probe) or "(no hosted art)"))
