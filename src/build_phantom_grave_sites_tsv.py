#!/usr/bin/env python3
r"""
build_phantom_grave_sites_tsv.py — re-derive tsv/phantom_grave_sites.tsv from the
ACTUAL datamined Pint-Sized Phantoms grave placements (not a hand-kept list).

Authoritative source
--------------------
Every Pint-Sized Phantoms grave the player digs is a placed reference of ONE
activator base object:

    BaseFormID 008F1672  ·  EDID SDOW_MQ02_Graves_GraveActivator01  ·  "Disturbed Grave"

(SDOW = the season's internal codename; MQ02 is the "Pint-Sized Phantoms' Map" /
SlasherMap quest.) Its placed REFRs are the grave sites. We read them straight
from the newest xEdit REFR export that carries coordinates
(tsv[/pts]/REFR_Placements_*.tsv), then resolve each to a region + nearest map
marker with Mappalachia — exactly how the collectable spawn extraction works.

This makes the grave list reproducible: if Bethesda ever adds/moves a grave, a
fresh export flows straight through here. Hand-authored columns (directions,
photo_approach, photo_spawn) are MERGED by ref FormID and never clobbered.

Output columns (unchanged): region, site_number, ref_edid, ref_formid,
closest_fast_travel, directions, photo_approach, photo_spawn, x, y, z

Env:
  MAPPALACHIA_DB   Mappalachia SQLite   default D:\Mappalachia\data\mappalachia.db
"""

import os, sys, re, glob, sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import crossref_mappalachia_markers as xref

GRAVE_BASE_FORMID = "008F1672"                       # SDOW_MQ02_Graves_GraveActivator01
OUT_TSV = os.path.join(REPO, "tsv", "phantom_grave_sites.tsv")
COLUMNS = ["region", "site_number", "ref_edid", "ref_formid", "closest_fast_travel",
           "directions", "photo_approach", "photo_spawn", "x", "y", "z"]
# Hand-authored fields to carry across a rebuild (keyed by ref FormID).
HANDFILL_COLS = ("directions", "photo_approach", "photo_spawn")


def _newest_refr_placements():
    """Newest REFR_Placements_*.tsv (PTS or main) that actually contains grave refs."""
    paths = sorted(glob.glob(os.path.join(REPO, "tsv", "REFR_Placements_*.tsv")) +
                   glob.glob(os.path.join(REPO, "tsv", "pts", "REFR_Placements_*.tsv")))
    for p in reversed(paths):
        with open(p, encoding="utf-8", errors="replace") as f:
            head = f.readline()
            if "BaseFormID" not in head:
                continue
            if any(GRAVE_BASE_FORMID.upper() in ln.upper().split("\t", 1)[0] for ln in f):
                return p
    return None


def read_grave_placements():
    """Return [{ref_formid, ref_edid, x, y, z}] for every placed 'Disturbed Grave'."""
    path = _newest_refr_placements()
    if not path:
        raise SystemExit("No REFR_Placements export containing base 008F1672 was found.")
    rows = []
    with open(path, encoding="utf-8", errors="replace") as f:
        hd = f.readline().rstrip("\n").split("\t")
        idx = {h: i for i, h in enumerate(hd)}
        for ln in f:
            p = ln.rstrip("\n").split("\t")
            if len(p) <= idx["Z"]:
                continue
            if p[idx["BaseFormID"]].strip().upper() != GRAVE_BASE_FORMID.upper():
                continue
            try:
                x, y, z = float(p[idx["X"]]), float(p[idx["Y"]]), float(p[idx["Z"]])
            except ValueError:
                continue
            rows.append({"ref_formid": p[idx["RefFormID"]].strip().upper(),
                         "ref_edid": p[idx["RefEDID"]].strip(), "x": x, "y": y, "z": z})
    return rows, os.path.basename(path)


def site_number_from_edid(edid):
    """SDOW_Grave07 -> 7, SDOW_Graves12 -> 12, SDOW_Grave_06 -> 6, (none) -> ''."""
    m = re.search(r"Grave_?s?_?(\d+)", edid or "", re.I)
    return str(int(m.group(1))) if m else ""


def load_existing_handfills():
    keep = {}
    if not os.path.exists(OUT_TSV):
        return keep
    with open(OUT_TSV, encoding="utf-8") as f:
        hd = f.readline().rstrip("\n").split("\t")
        ti = {h: i for i, h in enumerate(hd)}
        for ln in f:
            p = ln.rstrip("\n").split("\t")
            fid = (p[ti["ref_formid"]].strip().upper() if ti.get("ref_formid") is not None
                   and ti["ref_formid"] < len(p) else "")
            if not fid:
                continue
            keep[fid] = {c: (p[ti[c]] if ti.get(c) is not None and ti[c] < len(p) else "")
                         for c in HANDFILL_COLS}
    return keep


def main():
    xref.MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", xref.MAPPALACHIA_DB)
    if not os.path.exists(xref.MAPPALACHIA_DB):
        raise SystemExit(f"Mappalachia DB not found at {xref.MAPPALACHIA_DB} — set MAPPALACHIA_DB.")
    rings, markers = xref.load_mappalachia()
    graves, src = read_grave_placements()
    handfills = load_existing_handfills()

    out = []
    for g in graves:
        region = xref.region_for_xy(rings, g["x"], g["y"], nearest=True)   # canonical region name
        marker, _, _ = xref.nearest_marker(markers, g["x"], g["y"])
        hf = handfills.get(g["ref_formid"], {})
        out.append({
            "region": region,
            "site_number": site_number_from_edid(g["ref_edid"]),
            "ref_edid": g["ref_edid"],
            "ref_formid": g["ref_formid"],
            "closest_fast_travel": marker,
            "directions": hf.get("directions", ""),
            "photo_approach": hf.get("photo_approach", ""),
            "photo_spawn": hf.get("photo_spawn", ""),
            "x": f"{g['x']:.1f}", "y": f"{g['y']:.1f}", "z": f"{g['z']:.1f}",
        })

    # Stable order: region (A-Z), then numbered graves by number, unnumbered last.
    def sort_key(r):
        try:
            sn = (0, int(r["site_number"]))
        except (TypeError, ValueError):
            sn = (1, 0)
        return (r["region"], sn)
    out.sort(key=sort_key)

    with open(OUT_TSV, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for r in out:
            f.write("\t".join(r[c] for c in COLUMNS) + "\n")

    kept = sum(1 for r in out if any(r[c] for c in HANDFILL_COLS))
    print(f"[phantom_grave_sites] {len(out)} graves from base {GRAVE_BASE_FORMID} "
          f"({src}); {kept} row(s) with preserved hand-fills -> {os.path.relpath(OUT_TSV, REPO)}")


if __name__ == "__main__":
    main()
