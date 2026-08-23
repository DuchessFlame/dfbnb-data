#!/usr/bin/env python3
r"""
build_u_mine_it_dig_sites_tsv.py — author-maintained dig-site TSV for the U-Mine-It /
Lucky Strike dig-locations page.

U-Mine-It (a.k.a. Lucky Strike, the MTRZ05 mining event) sends the player to dig at a
fixed set of placed "Dig Site" objects:

    BaseFormID 000355A1 · EDID MTRZ05MiningSite · "Dig Site"

Its placed references are the dig sites. Unlike the treasure-map mounds, MTRZ05MiningSite
is NOT in the ACTI2 collectables export, so this script resolves its placements DIRECTLY
from the Mappalachia DB (Position.referenceFormID = the base object's FormID) to a region
+ nearest map marker via spawns_engine.geo.Geo. Region + closest_fast_travel are
PRECOMPUTED here (needs the DB) so the collectables-spawns CI build works without it.

Author-supplied directions / photo_approach / photo_spawn are MERGE-PRESERVED across
regenerations (keyed by ref FormID) — a rebuild never wipes editorial content.

Columns (match build_collectable_spawns_json.build_dig_set / the treasure-map dig TSV):
  region  site_number  ref_edid  ref_formid  closest_fast_travel  directions
  photo_approach  photo_spawn  x  y  z

Run locally with the Mappalachia DB present, then commit tsv/u_mine_it_dig_sites.tsv.
    MAPPALACHIA_DB=D:\Mappalachia\data\mappalachia.db python src/build_u_mine_it_dig_sites_tsv.py
"""

import os, csv, sqlite3, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import crossref_mappalachia_markers as xref
import tsv_source

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")


def _newest_placements(channel):
    """Newest REFR export FOR THIS CHANNEL that actually contains the dig-site base.

    Never falls back across channels — a live page built from PTS placements is the
    original bug this whole pass exists to fix.
    """
    for p in reversed(tsv_source.all_matching("REFR_Placements_*.tsv", channel=channel)):
        with open(p, encoding="utf-8", errors="replace") as f:
            if "BaseFormID" not in f.readline():
                continue
            if any(f"{BASE_FORMID:08X}" in ln.upper().split("\t", 1)[0] for ln in f):
                return p
    return None

# Channel-scoped — see build_treasure_map_dig_sites_tsv.py for why.
OUT_TSV = os.environ.get("UMINE_DIG_SITES_TSV",
                         tsv_source.derived("u_mine_it_dig_sites.tsv",
                                            tsv_source.channel_of()))

BASE_FORMID = 0x000355A1          # MTRZ05MiningSite ("Dig Site") — routing seed, not hardcoded output
BASE_EDID = "MTRZ05MiningSite"
FIELDS = ["region", "site_number", "ref_edid", "ref_formid", "closest_fast_travel",
          "directions", "photo_approach", "photo_spawn", "x", "y", "z"]
PRESERVE = ["directions", "photo_approach", "photo_spawn"]


def load_existing_preserved(path):
    keep = {}
    if not os.path.exists(path):
        return keep
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            ref = (row.get("ref_formid") or "").strip().upper()
            saved = {k: (row.get(k) or "").strip() for k in PRESERVE if (row.get(k) or "").strip()}
            region = (row.get("region") or "").strip()
            if region:
                saved["region"] = region
            if ref and saved:
                keep[ref] = saved
    return keep


def main():
    # Placements come from the REFR export, not the Mappalachia DB.
    #
    # This used to read Mappalachia's Position table, which made the DB the DATA
    # SOURCE rather than just the geography — so no committed snapshot could stand
    # in for it and the whole generator was local-only. It now reads the same
    # REFR_Placements export the grave generator does, resolving region + nearest
    # marker from X/Y exactly the way that one does. Two consequences: it runs in
    # CI, and the dig sites refresh from the game rather than from whenever someone
    # last updated their copy of Mappalachia.
    channel = tsv_source.channel_of()
    path = _newest_placements(channel)
    if not path:
        chan = "PTS (tsv/pts/)" if channel == "pts" else "LIVE (tsv/)"
        print(f"[umine_dig_sites] no REFR_Placements export containing base "
              f"{BASE_FORMID:08X} for the {chan} channel — nothing written, existing "
              f"TSV left intact. Export REFR for this channel first.")
        return

    rings, markers = xref.load_mappalachia()
    placements = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if (r.get("BaseFormID") or "").strip().upper() != f"{BASE_FORMID:08X}":
                continue
            try:
                px, py, pz = float(r["X"]), float(r["Y"]), float(r["Z"])
            except (KeyError, ValueError):
                continue
            placements.append(((r.get("RefFormID") or "").strip(), px, py, pz))

    preserved = load_existing_preserved(OUT_TSV)
    out_rows, unplaced = [], []
    for ref_raw, x, y, z in placements:
        # Keep the 6-hex form the committed TSV already uses, so hand-authored
        # directions and photos keyed on ref_formid still join after the switch.
        ref = f"{int(ref_raw, 16):06X}"
        region = xref.region_for_xy(rings, x, y, nearest=True)
        marker, _, _ = xref.nearest_marker(markers, x, y)
        keep = preserved.get(ref, {})
        region = region or keep.get("region", "")
        out_rows.append({
            "region": region, "site_number": "",
            "ref_edid": BASE_EDID, "ref_formid": ref,
            "closest_fast_travel": marker or "",
            "directions": keep.get("directions", ""),
            "photo_approach": keep.get("photo_approach", ""),
            "photo_spawn": keep.get("photo_spawn", ""),
            "x": round(x, 6), "y": round(y, 6), "z": round(z, 6),
        })
        if not region:
            unplaced.append(marker or ref)

    out_rows.sort(key=lambda d: (d["region"] == "", d["region"].lower(),
                                 (d["closest_fast_travel"] or "").lower(), d["ref_formid"]))

    os.makedirs(os.path.dirname(OUT_TSV), exist_ok=True)
    with open(OUT_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t")
        w.writeheader(); w.writerows(out_rows)

    kept = sum(1 for r in out_rows if any(r[k] for k in PRESERVE))
    print(f"[umine_dig_sites] wrote {len(out_rows)} dig sites -> {os.path.basename(OUT_TSV)} "
          f"({kept} with author directions/photos preserved)")
    if unplaced:
        print(f"[umine_dig_sites] {len(unplaced)} site(s) with no region (polygon gap) — set "
              f"'region' by hand: {unplaced}")


if __name__ == "__main__":
    main()
