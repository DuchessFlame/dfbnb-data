#!/usr/bin/env python3
r"""
esm_scan_placements.py - list every placed reference of chosen base records, read
straight from SeventySix.esm, with region + nearest map marker.

Why this exists: a brand-new PTS collectable (the Brain in a Jar, Oct 2026) lands
in the game files before the xEdit MISC2 / REFR_Placements exports are re-run. This
reads the ESM itself (uncompressed GRUP walk, zlib per record) so the maps and pages
can be built the day the PTS drops. It is a READ-ONLY scan; it never touches the ESM.

Output columns (tab-separated, one row per placed REFR/ACHR):
  RefFormID BaseFormID WorldspaceFormID CellFormID X Y Z region marker

region / marker come from crossref_mappalachia_markers (needs the Mappalachia DB,
so this runs locally, never in CI). CI reads the committed TSV this writes.

Usage:
  python src/esm_scan_placements.py --esm "D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data\SeventySix.esm" \
      --bases 008ABB18 --out data/brain_in_a_jar/placements_pts.tsv
  (live: point --esm at ...\Fallout76\Data\SeventySix.esm and write placements_live.tsv)
"""
import argparse, csv, os, struct, sys, zlib

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

COMPRESSED = 0x40000


def _subs(data):
    p = 0; big = None
    while p + 6 <= len(data):
        t, sz = struct.unpack_from("<4sH", data, p); p += 6
        if t == b"XXXX":
            big = struct.unpack_from("<I", data, p)[0]; p += sz; continue
        if big is not None:
            sz = big; big = None
        yield t, data[p:p + sz]; p += sz


def scan(esm_path, bases):
    """[{ref, base, wrld, cell, x, y, z}] for every REFR/ACHR whose NAME is in bases."""
    targets = set(bases)
    tb = {struct.pack("<I", t) for t in targets}
    with open(esm_path, "rb") as fh:
        buf = fh.read()
    L = len(buf)
    o = 24 + struct.unpack_from("<I", buf, 4)[0]          # skip TES4 header
    stack, out = [], []                                     # (end, group type, label)
    while o < L:
        while stack and o >= stack[-1][0]:
            stack.pop()
        sig = buf[o:o + 4]
        size = struct.unpack_from("<I", buf, o + 4)[0]
        if sig == b"GRUP":
            label = struct.unpack_from("<I", buf, o + 8)[0]
            gtype = struct.unpack_from("<i", buf, o + 12)[0]
            stack.append((o + size, gtype, label)); o += 24; continue
        if sig in (b"REFR", b"ACHR"):
            flags = struct.unpack_from("<I", buf, o + 8)[0]
            fid = struct.unpack_from("<I", buf, o + 12)[0]
            data = buf[o + 24:o + 24 + size]
            if flags & COMPRESSED:
                try:
                    data = zlib.decompress(data[4:])
                except zlib.error:
                    data = b""
            if any(k in data for k in tb):
                base = pos = None
                for st, d in _subs(data):
                    if st == b"NAME":
                        base = struct.unpack("<I", d[:4])[0]
                    elif st == b"DATA" and len(d) >= 12:
                        pos = struct.unpack("<3f", d[:12])
                if base in targets and pos:
                    wrld = next((g[2] for g in stack if g[1] == 1), None)       # world children
                    cell = next((g[2] for g in reversed(stack) if g[1] in (6, 8, 9, 10)), None)
                    out.append({"ref": f"{fid:08X}", "base": f"{base:08X}",
                                "wrld": f"{wrld:08X}" if wrld else "",
                                "cell": f"{cell:08X}" if cell else "",
                                "x": pos[0], "y": pos[1], "z": pos[2]})
        o += 24 + size
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--esm", required=True)
    ap.add_argument("--bases", required=True, help="comma-separated base FormIDs (hex)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    bases = [int(b, 16) for b in a.bases.split(",") if b.strip()]
    rows = scan(a.esm, bases)

    import crossref_mappalachia_markers as xref
    geo = xref.load_mappalachia()
    rings, markers = geo[0], geo[1]
    for r in rows:
        if r["wrld"] == "0025DA15":                         # Appalachia
            r["region"] = xref.region_for_xy(rings, r["x"], r["y"], nearest=True) or ""
            m = xref.nearest_marker(markers, r["x"], r["y"])
            r["marker"] = (m[0] if isinstance(m, (list, tuple)) else m) or ""
        else:
            r["region"] = r["marker"] = ""

    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(["RefFormID", "BaseFormID", "WorldspaceFormID", "CellFormID",
                    "X", "Y", "Z", "region", "marker"])
        for r in sorted(rows, key=lambda r: r["ref"]):
            w.writerow([r["ref"], r["base"], r["wrld"], r["cell"],
                        f"{r['x']:.3f}", f"{r['y']:.3f}", f"{r['z']:.3f}",
                        r["region"], r["marker"]])
    print(f"{len(rows)} placements -> {a.out}")


if __name__ == "__main__":
    main()
