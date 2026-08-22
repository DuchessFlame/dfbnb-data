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
SlasherMap quest.) Its placed REFRs are the grave sites. We read them from the newest
REFR export **for the requested channel**, then resolve each to a region + nearest map
marker with Mappalachia — exactly how the collectable spawn extraction works.

This file writes PLACEMENTS ONLY
--------------------------------
Directions and the three photo slots are NOT written here and are not this script's
business. They live in tsv/phantom_grave_notes.tsv, hand-authored, keyed on the grave's
identity (its number, or "@<marker>" when unnumbered), and are joined in by
build_collectable_spawns_json.py.

That separation is deliberate. Previously the editorial columns lived in this generated
file and were carried across a rebuild keyed on ref_formid. When Bethesda re-placed
graves 05/07/09 under new refs (00930AA0/AA7/AAE, EDIDs SDOW__GraveNN) the key changed,
the merge missed, and the directions + photo paths were silently deleted by a routine
rebuild. A rebuild can no longer destroy editorial content because it no longer writes it.

Channel
-------
LIVE reads tsv/REFR_Placements_*.tsv. PTS reads tsv/pts/REFR_Placements_*.tsv. It never
reads both. Falling back across channels is how PTS placements ended up rendering on a
live page for five weeks — PTS content diverges from live by design, so a live page built
from a PTS export is wrong, not merely stale. If the requested channel has no export, this
exits and says so rather than quietly using the other channel's.

Regression guard
----------------
Refuses to overwrite the existing TSV with an export OLDER than the provenance already
recorded there, and never silently discards rows marked `manual:` (hand-verified in xEdit
against a build newer than any committed export). Override with --force only when you
mean it.

Output columns: region, site_number, ref_edid, ref_formid, closest_fast_travel,
source_export, x, y, z

Usage:
  python src/build_phantom_grave_sites_tsv.py            # live channel
  python src/build_phantom_grave_sites_tsv.py --pts      # PTS channel
  python src/build_phantom_grave_sites_tsv.py --force    # allow a regression

Env:
  MAPPALACHIA_DB   Mappalachia SQLite   default D:\Mappalachia\data\mappalachia.db
  DFBNB_CHANNEL    "pts" is equivalent to passing --pts
"""

import os, sys, re, glob, sqlite3, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import crossref_mappalachia_markers as xref

GRAVE_BASE_FORMID = "008F1672"                       # SDOW_MQ02_Graves_GraveActivator01
OUT_TSV = os.path.join(REPO, "tsv", "phantom_grave_sites.tsv")
NOTES_TSV = os.path.join(REPO, "tsv", "phantom_grave_notes.tsv")
COLUMNS = ["region", "site_number", "ref_edid", "ref_formid", "closest_fast_travel",
           "source_export", "x", "y", "z"]

PTS = "--pts" in sys.argv or os.environ.get("DFBNB_CHANNEL", "").strip().lower() == "pts"
FORCE = "--force" in sys.argv

MONTHS = {"jan": 1, "feb": 2, "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5,
          "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8, "august": 8,
          "sep": 9, "sept": 9, "september": 9, "oct": 10, "october": 10,
          "nov": 11, "november": 11, "dec": 12, "december": 12}


def export_date(name):
    """Sortable date for an export filename. Returns a date, or date.min if undated.

    Handles both naming schemes in the repo:
      REFR_Placements_PTS_2026-07-18_0757.tsv   -> 2026-07-18
      REFR_Placements_August_2026.tsv           -> 2026-08-01

    The second case is why filenames must never be sorted lexically: "August" sorts
    BEFORE "July", so a max()/sorted()[-1] on the filename picks the older export.
    """
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", name)
    if m:
        return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.search(r"_([A-Za-z]+)_(\d{4})", name)
    if m and m.group(1).lower() in MONTHS:
        return datetime.date(int(m.group(2)), MONTHS[m.group(1).lower()], 1)
    return datetime.date.min


def _newest_refr_placements():
    """Newest REFR_Placements export FOR THIS CHANNEL that contains grave refs.

    Newest by parsed export date, not by filename order.
    """
    root = os.path.join(REPO, "tsv", "pts") if PTS else os.path.join(REPO, "tsv")
    paths = glob.glob(os.path.join(root, "REFR_Placements_*.tsv"))
    if not PTS:
        # tsv/*.tsv would otherwise not include tsv/pts/, but be explicit about it.
        paths = [p for p in paths if os.path.basename(os.path.dirname(p)) != "pts"]
    for p in sorted(paths, key=lambda p: (export_date(os.path.basename(p)),
                                          os.path.basename(p)), reverse=True):
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
        chan = "PTS (tsv/pts/)" if PTS else "LIVE (tsv/)"
        raise SystemExit(
            f"No REFR_Placements export containing base {GRAVE_BASE_FORMID} was found "
            f"for the {chan} channel.\n"
            f"Refusing to fall back to the other channel — that is what put PTS "
            f"placements on the live grave page. Export REFR for this channel first."
        )
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
    """SDOW_Grave07 -> 7, SDOW__Grave07 -> 7, SDOW_Graves12 -> 12, (none) -> ''."""
    m = re.search(r"Grave_?s?_?(\d+)", edid or "", re.I)
    return str(int(m.group(1))) if m else ""


def existing_provenance():
    """Provenance already recorded in the output TSV: (newest_date, manual_rows)."""
    newest, manual = datetime.date.min, []
    if not os.path.exists(OUT_TSV):
        return newest, manual
    with open(OUT_TSV, encoding="utf-8") as f:
        hd = f.readline().rstrip("\n").split("\t")
        ti = {h.strip(): i for i, h in enumerate(hd)}
        si, ri = ti.get("source_export"), ti.get("ref_formid")
        if si is None:
            return newest, manual          # pre-provenance file; nothing to protect
        for ln in f:
            cols = ln.rstrip("\n").split("\t")
            src = cols[si].strip() if si < len(cols) else ""
            ref = cols[ri].strip() if ri is not None and ri < len(cols) else ""
            if not src:
                continue
            newest = max(newest, export_date(src))
            if src.lower().startswith("manual:"):
                manual.append((ref, src))
    return newest, manual


def guard(src_name):
    """Refuse to regress the file to older provenance unless --force."""
    chosen = export_date(src_name)
    newest, manual = existing_provenance()
    problems = []
    if newest > chosen:
        problems.append(f"existing rows are from {newest.isoformat()}, this export is "
                        f"{chosen.isoformat() if chosen != datetime.date.min else 'undated'}")
    stale_manual = [(r, s) for r, s in manual if export_date(s) > chosen]
    if stale_manual:
        problems.append(f"{len(stale_manual)} hand-verified row(s) would be discarded: "
                        + ", ".join(r for r, _ in stale_manual))
    if not problems:
        return
    msg = ("[phantom_grave_sites] REFUSING to write — this would replace current data "
           "with older data:\n  - " + "\n  - ".join(problems) +
           f"\n  export offered: {src_name}\n"
           "  Re-export REFR for this channel, or pass --force if you really mean it.")
    if FORCE:
        print(msg.replace("REFUSING to write", "FORCED past the guard"))
        return
    raise SystemExit(msg)


def main():
    xref.MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", xref.MAPPALACHIA_DB)
    if not os.path.exists(xref.MAPPALACHIA_DB):
        raise SystemExit(f"Mappalachia DB not found at {xref.MAPPALACHIA_DB} — set MAPPALACHIA_DB.")
    graves, src = read_grave_placements()
    guard(src)
    rings, markers = xref.load_mappalachia()

    out = []
    for g in graves:
        region = xref.region_for_xy(rings, g["x"], g["y"], nearest=True)   # canonical region name
        marker, _, _ = xref.nearest_marker(markers, g["x"], g["y"])
        out.append({
            "region": region,
            "site_number": site_number_from_edid(g["ref_edid"]),
            "ref_edid": g["ref_edid"],
            "ref_formid": g["ref_formid"],
            "closest_fast_travel": marker,
            "source_export": src,
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

    chan = "PTS" if PTS else "LIVE"
    print(f"[phantom_grave_sites] {len(out)} graves from base {GRAVE_BASE_FORMID} "
          f"[{chan}] ({src}) -> {os.path.relpath(OUT_TSV, REPO)}")

    # Editorial content is a separate file; flag any grave that has no notes row so a
    # re-placed grave shows up as "needs photos" instead of silently rendering empty.
    keys = set()
    if os.path.exists(NOTES_TSV):
        with open(NOTES_TSV, encoding="utf-8") as f:
            f.readline()
            keys = {ln.split("\t", 1)[0].strip() for ln in f if ln.strip()}
    missing = [(r["site_number"] or "@" + r["closest_fast_travel"]) for r in out
               if (r["site_number"] or "@" + r["closest_fast_travel"]) not in keys]
    if missing:
        print(f"[phantom_grave_sites] {len(missing)} grave(s) with no row in "
              f"{os.path.relpath(NOTES_TSV, REPO)}: {', '.join(missing)}")


if __name__ == "__main__":
    main()
