#!/usr/bin/env python3
"""
src/build_seasons_json.py
==========================
Reads tsv/fallout76_seasons.tsv (hand-maintained) and writes
dist/seasons.json — the canonical season-number -> metadata map
the website consumes when rendering "Season N" labels (e.g. in the
tents page "How to Obtain" line, the emotes page season grouping,
and anywhere else that used to hardcode SEASON_NAMES in JS).

Source TSV columns:
    SeasonKey, SeasonNumber, SeasonName, UpdateAlongside,
    StartDate, EndDate, Days, Availability,
    UnlockRequiredCount, UnlockRankRequired, UnlockLineText

Output shape:
    {
      "_generated":         "YYYY-MM-DDTHH:MM:SSZ",
      "_source":            "fallout76_seasons.tsv",
      "_count":             N,
      "_gone_fission_num":  21,   // first ticket-based season
      "seasons": {
        "1": {
          "key":          "SCORE_S01",
          "num":          1,
          "name":         "The Legendary Run",
          "update":       "The Legendary Run",
          "startDate":    "30/06/2020",
          "endDate":      "8/09/2020",
          "days":         70,
          "availability": "June 30, 2020 - September 8, 2020"
        },
        ...
      }
    }

Usage:
    python build_seasons_json.py
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_PATH   = os.path.join(SCRIPT_DIR, "..", "tsv", "fallout76_seasons.tsv")
DIST_PATH  = os.path.join(SCRIPT_DIR, "..", "dist", "seasons.json")

# First season that switched to ticket-based gameboard rewards. Kept here
# so the website doesn't have to hardcode it — consumers can read it from
# the JSON and tweak phrasing ("Reward from..." vs "Purchase with tickets
# from...") without code changes.
GONE_FISSION_NUM = 21


def _int_or_none(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _clean(s: str) -> str:
    return (s or "").strip().strip('"')


def main() -> None:
    if not os.path.isfile(TSV_PATH):
        print(f"[seasons] Missing TSV: {TSV_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"[seasons] Reading {os.path.basename(TSV_PATH)}")

    seasons: dict[str, dict] = {}
    with open(TSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            num = _int_or_none(row.get("SeasonNumber"))
            name = _clean(row.get("SeasonName"))
            if num is None or not name:
                continue
            seasons[str(num)] = {
                "key":          _clean(row.get("SeasonKey")),
                "num":          num,
                "name":         name,
                "update":       _clean(row.get("UpdateAlongside")),
                "startDate":    _clean(row.get("StartDate")),
                "endDate":      _clean(row.get("EndDate")),
                "days":         _int_or_none(row.get("Days")),
                "availability": _clean(row.get("Availability")),
            }

    # Sort numerically so the on-disk JSON reads naturally 1..N
    seasons_sorted = dict(sorted(seasons.items(), key=lambda kv: int(kv[0])))

    out = {
        "_generated":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source":           os.path.basename(TSV_PATH),
        "_count":            len(seasons_sorted),
        "_gone_fission_num": GONE_FISSION_NUM,
        "seasons":           seasons_sorted,
    }

    os.makedirs(os.path.dirname(DIST_PATH), exist_ok=True)
    with open(DIST_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"[seasons] OK — {len(seasons_sorted)} seasons -> "
          f"{os.path.relpath(DIST_PATH, start=os.path.join(SCRIPT_DIR, '..'))}")


if __name__ == "__main__":
    main()
