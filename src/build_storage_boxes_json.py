#!/usr/bin/env python3
"""
build_storage_boxes_json.py
===========================
Builds the CAMP storage-box guides for /df/camp/storage/{aid,ammo,scrap}-box/.

The three limited-type CAMP containers each accept a fixed, game-defined set of
items. The gate is two layers deep in the data:

    CONT  F1_ScrapBox_Standard
      └─ KYWD  LimitedTypeStorageKeyword_Scrap        (marks the container)
           ↕
    FLST  LimitedTypeStorageFilterList_Scrap          (what it will accept)
           └─ either the items themselves (Scrap → 42 CMPO records)
              or a single item KYWD that the items carry
              (Ammo → LimitedTypeAmmo, Aid → LimitedTypeChems_Aidbox)

So this script resolves the FLST for each box, then expands every entry: an
item record is taken as-is, a KYWD entry is expanded through the keyword's own
reference list. Bethesda can add items at either layer — a new ammo type
carrying LimitedTypeAmmo, or a new component added straight to the scrap list —
and the next build picks it up with no code change. A whole new filter list
(e.g. LimitedTypeStorageFilterList_AidBox_Food, if the Aid Box ever takes food)
is picked up too, because each box matches its lists by EDID pattern rather
than by a hardcoded FormID.

Reads from:
  - FLST_Export_*_Entries.tsv   (the filter lists, one row per entry)
  - KYWD_Export_*_Refs.tsv      (keyword → every record carrying it)

Outputs:
  - dist/storage_boxes.json          (live)
  - dist/pts/storage_boxes.json      (--pts)

The PTS file is what the site's PTS toggle reads: df-bnb-pts.js rewrites every
dist/ fetch to dist/pts/, so building both channels is all the toggle needs.

Output shape:
  {
    "boxes": [
      { "slug": "scrap-box", "title": "Scrap Box",
        "containerKeyword": "LimitedTypeStorageKeyword_Scrap",
        "filterLists": ["LimitedTypeStorageFilterList_Scrap"],
        "variants": 19, "count": 42,
        "items": [ { "name": "Acid", "edid": "c_Acid",
                     "formId": "0001FA8C", "sig": "CMPO" }, ... ] },
      ...
    ],
    "_meta": { "generated": "...", "observed": "...", "channel": "live",
               "sources": [...] }
  }

Items are sorted A–Z by display name. No external dependencies — stdlib only.

Usage:  python src/build_storage_boxes_json.py
PTS:    python src/build_storage_boxes_json.py --pts
"""

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import tsv_source  # noqa: E402

ROOT = os.path.dirname(HERE)
CHANNEL = tsv_source.channel_of()
OUT_DIR = os.path.join(ROOT, "dist", "pts") if CHANNEL == "pts" else os.path.join(ROOT, "dist")

# Record signatures that are actual storable items. Anything else on a keyword's
# reference list is plumbing — the filter list pointing back at the keyword, the
# default-object (DFOB) wiring — and must not reach the page.
ITEM_SIGS = {"ALCH", "AMMO", "CMPO", "MISC", "ARMO", "WEAP", "BOOK", "KEYM", "NOTE"}

# One entry per box page. `lists` matches FLST EDIDs, so a new filter list added
# under the same box prefix is picked up automatically.
BOXES = [
    {
        "slug": "aid-box",
        "title": "Aid Box",
        "container": "LimitedTypeStorageKeyword_AidBox",
        "lists": re.compile(r"^LimitedTypeStorageFilterList_AidBox(_|$)", re.I),
    },
    {
        "slug": "ammo-box",
        "title": "Ammo Box",
        "container": "LimitedTypeStorageKeyword_Ammo",
        "lists": re.compile(r"^LimitedTypeStorageFilterList_Ammo(_|$)", re.I),
    },
    {
        "slug": "scrap-box",
        "title": "Scrap Box",
        "container": "LimitedTypeStorageKeyword_Scrap",
        "lists": re.compile(r"^LimitedTypeStorageFilterList_Scrap(_|$)", re.I),
    },
]


def open_tsv(path):
    """xEdit exports are usually UTF-8 but some land as Windows-1252.

    A hard decode error on one stray byte would otherwise take the whole build
    down, so the encoding is sniffed once on a sample and the file re-opened.
    """
    with open(path, "rb") as f:
        sample = f.read(1 << 20)
    try:
        sample.decode("utf-8-sig")
        enc = "utf-8-sig"
    except UnicodeDecodeError:
        enc = "cp1252"
    return open(path, "r", encoding=enc, errors="replace", newline="")


def read_tsv(path):
    with open_tsv(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            yield row


def load_filter_lists(flst_path):
    """{FLST_EDID: [entry rows]} for the limited-type storage filter lists only."""
    out = {}
    for row in read_tsv(flst_path):
        edid = (row.get("FLST_EDID") or "").strip()
        if not edid.lower().startswith("limitedtypestoragefilterlist"):
            continue
        out.setdefault(edid, []).append({
            "sig": (row.get("Entry_Sig") or "").strip(),
            "formId": (row.get("Entry_FormID") or "").strip().upper(),
            "edid": (row.get("Entry_EDID") or "").strip(),
            "name": (row.get("Entry_FULL") or "").strip(),
        })
    return out


def load_keyword_refs(kywd_path, wanted):
    """{KeywordEDID: [ref rows]} for the keywords we actually need.

    The refs export is ~30 MB, so it is streamed once and filtered down rather
    than held in memory whole.
    """
    out = {k: [] for k in wanted}
    for row in read_tsv(kywd_path):
        edid = (row.get("KeywordEDID") or "").strip()
        if edid not in out:
            continue
        out[edid].append({
            "sig": (row.get("RefSignature") or "").strip(),
            "formId": (row.get("RefFormID") or "").strip().upper(),
            "edid": (row.get("RefEDID") or "").strip(),
            "name": (row.get("RefName") or "").strip(),
        })
    return out


def expand(entries, kw_refs, warn_for):
    """Filter-list entries → the item records a player can actually store.

    An item entry is taken as-is. A KYWD entry is the indirection the Ammo and
    Aid boxes use, and expands to every record carrying that keyword.
    """
    items = []
    for e in entries:
        if e["sig"] == "KYWD":
            refs = kw_refs.get(e["edid"])
            if not refs:
                print(f"  [WARN] {warn_for}: keyword {e['edid']} has no references",
                      file=sys.stderr)
                continue
            items.extend(r for r in refs if r["sig"] in ITEM_SIGS)
        elif e["sig"] in ITEM_SIGS:
            items.append(e)
    return items


def dedupe_and_sort(items):
    """One row per display name, A–Z, case-insensitive.

    Items with no display name are dropped — an unnamed record is not something
    a player can recognise in their inventory, and shipping a blank row on the
    page is worse than leaving it out.
    """
    seen = {}
    for it in items:
        name = it["name"].strip()
        if not name:
            continue
        seen.setdefault(name, it)
    return [
        {"name": n, "edid": seen[n]["edid"], "formId": seen[n]["formId"], "sig": seen[n]["sig"]}
        for n in sorted(seen, key=lambda s: (s.lower(), s))
    ]


def main():
    print("=" * 60)
    print("  Building CAMP storage-box JSON")
    print(f"  Channel: {CHANNEL.upper()}")
    print("=" * 60)

    flst_path = tsv_source.newest("FLST_Export_*_Entries.tsv", channel=CHANNEL)
    kywd_path = tsv_source.newest("KYWD_Export_*_Refs.tsv", channel=CHANNEL)
    print(f"  Filter lists : {os.path.basename(flst_path)}")
    print(f"  Keyword refs : {os.path.basename(kywd_path)}")

    lists = load_filter_lists(flst_path)
    if not lists:
        print("  [ERROR] No LimitedTypeStorageFilterList_* records found — "
              "did the FLST export change shape?", file=sys.stderr)
        return 1

    # Every keyword we need to expand: the item keywords sitting inside the
    # filter lists, plus each box's container keyword (for the variant count).
    wanted = {b["container"] for b in BOXES}
    for entries in lists.values():
        wanted.update(e["edid"] for e in entries if e["sig"] == "KYWD" and e["edid"])
    kw_refs = load_keyword_refs(kywd_path, wanted)

    boxes = []
    for box in BOXES:
        matched = sorted(k for k in lists if box["lists"].match(k))
        if not matched:
            print(f"  [ERROR] {box['slug']}: no filter list matched — "
                  f"the box may have been renamed in the game data", file=sys.stderr)
            return 1

        raw = []
        for edid in matched:
            raw.extend(expand(lists[edid], kw_refs, box["slug"]))
        items = dedupe_and_sort(raw)
        if not items:
            print(f"  [ERROR] {box['slug']}: resolved to zero items", file=sys.stderr)
            return 1

        # How many CAMP containers (skins, plans, tent and shelter versions)
        # behave as this box — every CONT record carrying the container keyword.
        variants = len({r["formId"] for r in kw_refs.get(box["container"], [])
                        if r["sig"] == "CONT"})

        boxes.append({
            "slug": box["slug"],
            "title": box["title"],
            "containerKeyword": box["container"],
            "filterLists": matched,
            "variants": variants,
            "count": len(items),
            "items": items,
        })
        print(f"    {box['title']:<10} {len(items):>4} items · {variants:>3} container variants")

    output = {
        "boxes": boxes,
        "_meta": {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "observed": tsv_source.observed(),
            "channel": CHANNEL,
            "sources": sorted(os.path.basename(p) for p in tsv_source.resolved()),
        },
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "storage_boxes.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n  Written to: {out_path}")
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
