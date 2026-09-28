#!/usr/bin/env python3
"""
build_apparel_formids_json.py — which ARMO records are Apparel, not Armour.

Every wearable in Fallout 76 is an ARMO record, so the reward pages used to
file every ARMO under "Armour Rewards". Outfits, hats and masks are Apparel.
The game says which is which through keywords, so we read those instead of
guessing from names.

RULE
    Apparel = an ARMO carrying ObjectTypeClothing or any ClothingType* keyword,
              and NONE of ObjectTypeArmor / ArmorTypePower / ObjectTypeUnderarmor.

    Underarmour stays Armour (Duchess, 2026-09-27) even though most underarmour
    records also carry a ClothingType* keyword.

INPUT   tsv/KYWD_Export_*_Refs.tsv   (newest, via tsv_source)
OUTPUT  dist/apparel_formids.json    { "_meta": {...}, "formids": ["005D2A0F", ...] }

The renderers (activities, public events, seasonal events, daily ops rewards,
treasure maps) load this list and split ARMO into "Armour Rewards" and
"Apparel Rewards". PTS: the PTS workflow runs this against the normalised PTS
TSVs and the output lands in dist/pts/ like every other builder.
"""
from __future__ import annotations

import csv
import io
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tsv_source  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "dist", "apparel_formids.json")

APPAREL_KW = {"ObjectTypeClothing"}
APPAREL_PREFIX = "ClothingType"
ARMOUR_KW = {"ObjectTypeArmor", "ArmorTypePower", "ObjectTypeUnderarmor"}

csv.field_size_limit(10**9)


def main() -> int:
    channel = tsv_source.channel_of()
    refs = tsv_source.newest("KYWD_Export_*_Refs.tsv", channel=channel)

    kws: dict[str, set[str]] = {}
    names: dict[str, str] = {}
    # xEdit writes these exports in Windows-1252 (e.g. "\xe9" for e-acute in
    # item names), and the PTS pull is not always re-encoded to UTF-8. Try
    # UTF-8 first, fall back to cp1252. Only names are affected; the keyword
    # and FormID columns are plain ASCII either way.
    raw = open(refs, "rb").read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
    for row in csv.DictReader(io.StringIO(text, newline=""), delimiter="\t"):
        if (row.get("RefSignature") or "").strip() != "ARMO":
            continue
        fid = (row.get("RefFormID") or "").strip().upper()
        kw = (row.get("KeywordEDID") or "").strip()
        if not fid or not kw:
            continue
        kws.setdefault(fid, set()).add(kw)
        names.setdefault(fid, (row.get("RefName") or "").strip())

    apparel = sorted(
        fid for fid, k in kws.items()
        if (k & APPAREL_KW or any(x.startswith(APPAREL_PREFIX) for x in k))
        and not (k & ARMOUR_KW)
    )
    if len(apparel) < 100:
        # A healthy export has well over a thousand. Refuse to publish a
        # near-empty list over a good one.
        print(f"ERROR: only {len(apparel)} apparel records found in {refs}", file=sys.stderr)
        return 1

    out = {
        "_meta": {
            "source": os.path.basename(refs),
            "observed": tsv_source.observed(),
            "generated": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rule": "ARMO with ObjectTypeClothing or ClothingType*, and none of "
                    + ", ".join(sorted(ARMOUR_KW)),
            "count": len(apparel),
        },
        "formids": apparel,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(out, fh, indent=1)
        fh.write("\n")
    print(f"apparel_formids.json: {len(apparel)} apparel of {len(kws)} ARMO ({os.path.basename(refs)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
