#!/usr/bin/env python3
r"""
build_treasure_map_dig_sites_tsv.py — author-maintained dig-site TSV for the Treasure Maps page.

Treasure maps are their OWN category (not a collectable). Their dig-site locations page
(/df/treasure-maps/locations/, rendered by df-bnb-treasure-maps.js) reads
treasure_maps.json -> treasure_map_locations, which build_treasure_maps_json.py fills from a
committed TSV via _load_location_sites() — exactly like phantom_grave_sites.tsv.

This script generates/refreshes that TSV (tsv/treasure_map_dig_sites.tsv) from the resolved
ACTI2 mound export (TreasureMapMoundActivator1..35 -> nearest map marker + region, via
crossref_mappalachia_markers). Region + closest_fast_travel are PRECOMPUTED here (needs the
Mappalachia DB) so the CI build works without it. Author-supplied directions / photo_approach /
photo_spawn are MERGE-PRESERVED across regenerations (keyed by ref FormID).

Columns (match _load_location_sites in build_treasure_maps_json.py):
  region  site_number  ref_edid  ref_formid  closest_fast_travel  directions
  photo_approach  photo_spawn  x  y  z

Run locally with the Mappalachia DB present, then commit the TSV. Env: same as the cross-ref.
"""

import os, csv
import crossref_mappalachia_markers as xref

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
import tsv_source

# Channel-scoped: LIVE -> tsv/, PTS -> tsv/pts/. Writing a derived TSV to one shared
# path lets a PTS run overwrite live data — the same class of bug as reading across
# channels, one step further down the pipeline.
OUT_TSV = os.environ.get("TM_DIG_SITES_TSV",
                         tsv_source.derived("treasure_map_dig_sites.tsv",
                                            tsv_source.channel_of()))

SET = "treasure-maps"
FIELDS = ["region", "site_number", "ref_edid", "ref_formid", "closest_fast_travel",
          "directions", "photo_approach", "photo_spawn", "x", "y", "z"]
# author-supplied columns preserved across rebuilds
PRESERVE = ["directions", "photo_approach", "photo_spawn"]


def load_existing_preserved(path):
    """{ref_formid: {directions, photo_approach, photo_spawn, region}} from the committed TSV.
    'region' is preserved too, but only used as a fallback when a site can't be auto-placed
    (Mappalachia polygon gap) — a hand-assigned region then survives regeneration."""
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
    resolved, _cache, _db_ok = xref.resolve_dataset(verbose=True)
    rows = [r for r in resolved if (r.get("set") or "") == SET]
    if not rows:
        print(f"[tm_dig_sites] no '{SET}' rows in the resolved export — run the ACTI2 export first.")
        return

    preserved = load_existing_preserved(OUT_TSV)

    out_rows = []
    unplaced = []
    for r in rows:
        ref = (r.get("ref_formid") or "").strip().upper()
        keep = preserved.get(ref, {})
        # auto-resolved region wins; fall back to a hand-assigned one for polygon-gap sites
        region = (r.get("region") or "") or keep.get("region", "")
        out_rows.append({
            "region": region,
            "site_number": "",  # author may number sites; blank sorts stable
            "ref_edid": r.get("edid") or "",
            "ref_formid": ref,
            "closest_fast_travel": r.get("marker") or "",
            "directions": keep.get("directions", ""),
            "photo_approach": keep.get("photo_approach", ""),
            "photo_spawn": keep.get("photo_spawn", ""),
            "x": r.get("x") or "", "y": r.get("y") or "", "z": r.get("z") or "",
        })
        if not region:
            unplaced.append(r.get("marker") or ref)

    # region A-Z, then marker A-Z (matches _load_location_sites ordering intent)
    out_rows.sort(key=lambda d: (d["region"] == "", d["region"].lower(),
                                 d["closest_fast_travel"].lower()))

    os.makedirs(os.path.dirname(OUT_TSV), exist_ok=True)
    with open(OUT_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)

    kept = sum(1 for r in out_rows if any(r[k] for k in PRESERVE))
    print(f"[tm_dig_sites] wrote {len(out_rows)} dig sites -> {os.path.basename(OUT_TSV)} "
          f"({kept} with author-filled directions/photos preserved)")
    if unplaced:
        print(f"[tm_dig_sites] {len(unplaced)} site(s) have no region (polygon gap) — set the "
              f"'region' column by hand so they show on the page: {unplaced}")


if __name__ == "__main__":
    main()
