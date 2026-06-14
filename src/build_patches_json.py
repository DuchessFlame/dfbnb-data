#!/usr/bin/env python3
"""
build_patches_json.py
=====================
Builds dist/patch-release-dates.json for the
/df/data-mining/guide-patch-release-dates/ page on
buffsnbrew.com / theduchessflame.com (rendered by
df-bnb-patch-release-dates.js in the dfbnb-child theme).

Source of truth
---------------
tsv/fallout76_patches.tsv  (hand-maintained, newest-first)
    patch_number  version  name  type  release_date  notes_url

Season context is joined in from dist/seasons.json (built by
build_seasons_json.py) by matching each patch's release_date into the
season date ranges. Patches released before Season 1 (30 Jun 2020) get
no season.

Output
------
dist/patch-release-dates.json
    {
      "title": "Patch Release Dates",
      "subtitle": "...",
      "_generated": "YYYY-MM-DDTHH:MM:SSZ",
      "current_year": 2026,
      "source": { "tsv": "...", "count": N },
      "years": [
        { "year": 2026, "entries": [
            { "patch_number","version","name","type",
              "release_date","notes_url",
              "season_num","season_name","season_update",
              "season_start","season_end" }, ... ] },
        ...
      ]
    }

Years are sorted newest-first; within a year, entries newest-first. The
renderer opens the most recent year and shows the oldest patch at the
very bottom of the page.

Usage
-----
  python src/build_patches_json.py
  python src/build_patches_json.py --outdir dist
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
TSV_PATH   = SCRIPT_DIR / ".." / "tsv" / "fallout76_patches.tsv"
SEASONS    = SCRIPT_DIR / ".." / "dist" / "seasons.json"

PAGE_TITLE    = "Patch Release Dates"
PAGE_SUBTITLE = (
    "Every Fallout 76 update, patch and hotfix since launch — build "
    "version, patch number, release date and the season it landed in. "
    "Newest at the top, the November 2018 launch at the very bottom."
)


def parse_season_date(s: str):
    """seasons.json dates look like '30/06/2020' (D/M/YYYY). Return a
    datetime.date or None."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        d, m, y = s.split("/")
        return datetime(int(y), int(m), int(d)).date()
    except Exception:
        return None


def parse_iso(s: str):
    try:
        return datetime.strptime((s or "").strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def load_seasons():
    """Return a list of (start_date, end_date, season_dict) sorted by start."""
    try:
        data = json.loads(SEASONS.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"  WARN: {SEASONS} not found — patches will have no season",
              file=sys.stderr)
        return []
    out = []
    for key, s in (data.get("seasons") or {}).items():
        start = parse_season_date(s.get("startDate"))
        end   = parse_season_date(s.get("endDate"))
        if not start:
            continue
        out.append((start, end, s))
    out.sort(key=lambda t: t[0])
    return out


def season_for(date, seasons):
    """Find the season whose [start, end) contains `date`. Falls back to the
    last started season if date is after every end (covers the live season
    whose endDate may be in the future or just past)."""
    if not date or not seasons:
        return None
    chosen = None
    for start, end, s in seasons:
        if date >= start:
            chosen = s
        else:
            break
    # `chosen` is the most recent season that had started by `date`. Good
    # enough: each season runs until the next one starts.
    return chosen


def read_patches():
    with TSV_PATH.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = [r for r in reader]
    return rows


def build():
    seasons = load_seasons()
    rows = read_patches()

    entries = []
    for r in rows:
        date_str = (r.get("release_date") or "").strip()
        d = parse_iso(date_str)
        s = season_for(d, seasons)
        e = {
            "patch_number": (r.get("patch_number") or "").strip(),
            "version":      (r.get("version") or "").strip(),
            "name":         (r.get("name") or "").strip(),
            "type":         (r.get("type") or "").strip(),
            "release_date": date_str,
            "notes_url":    (r.get("notes_url") or "").strip(),
            "season_num":    s.get("num")    if s else None,
            "season_name":   s.get("name")   if s else None,
            "season_update": s.get("update") if s else None,
            "season_start":  s.get("startDate") if s else None,
            "season_end":    s.get("endDate")   if s else None,
        }
        entries.append(e)

    # Group by year of release_date, newest-first at every level.
    by_year = {}
    for e in entries:
        d = parse_iso(e["release_date"])
        y = d.year if d else 0
        by_year.setdefault(y, []).append(e)
    for y in by_year:
        by_year[y].sort(key=lambda e: e["release_date"], reverse=True)
    years_sorted = sorted(by_year.keys(), reverse=True)
    current_year = years_sorted[0] if years_sorted else 0

    return {
        "title":        PAGE_TITLE,
        "subtitle":     PAGE_SUBTITLE,
        "_generated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "current_year": current_year,
        "source": {
            "tsv":   "tsv/fallout76_patches.tsv",
            "count": len(entries),
        },
        "years": [
            {"year": y, "entries": by_year[y]}
            for y in years_sorted
        ],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="dist")
    args = ap.parse_args()

    data = build()

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "patch-release-dates.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {out_path}")
    print(f"  total entries: {data['source']['count']}")
    print(f"  years: {[y['year'] for y in data['years']]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
