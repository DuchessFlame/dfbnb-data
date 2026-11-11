#!/usr/bin/env python3
r"""
export_ammo_converter_from_esm.py - Ammo Converter export, straight from SeventySix.esm.

Feeds the DF Ammo Converter Calculator (/df/calculators/ammo-converter-calculator/).
Kat 11 Nov 2026.

Reads the Ammo Converter's terminal menus (TERM SCORE_Vending_Terminal_AmmoConverter*),
the AmmoConverter_<Ammo>_Ammo_Count / _Point_SellGain / _Point_BuyCost settings
(GLOB) and the ammo item names (AMMO), and writes one row per ammo the converter takes:

    tsv/AMMOCONV_Export_<Month>_<Year>.tsv         (LIVE)
    tsv/pts/AMMOCONV_Export_PTS_<yyyy-mm-dd_hhnn>.tsv   (--pts)

Columns:
    GroupOrder  Group  Order  Key  MenuName  AMMO_FormID  AMMO_EDID  AMMO_FULL
    Count  SellGain  BuyCost  Count_GLOB  SellGain_GLOB  BuyCost_GLOB

The trade itself runs on Bethesda's servers (Fallout76.exe has nothing on it, checked
11 Nov 2026), so these settings + the terminal text are everything the files hold.
The build (src/build_ammo_converter_json.py) re-reads the GLOB export for the numbers,
so a patch that only changes rates is picked up even if this export isn't re-run.

Run on the PC with the game installed:
    python tools/export_ammo_converter_from_esm.py          (LIVE)
    python tools/export_ammo_converter_from_esm.py --pts    (PTS)
"""
from __future__ import annotations

import argparse
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from export_term_from_esm import load_records, edid_of, LIVE_DATA, PTS_DATA  # noqa: E402
from esm_reader import load_strings, out_name, write_tsv, append_ledger, clean_cell  # noqa: E402

MAIN = "SCORE_Vending_Terminal_AmmoConverter"
AMMO_PREFIX = MAIN + "_Ammo_"
# Converter key -> AMMO EDID, where it isn't simply "Ammo" + key.
AMMO_EDID_OVERRIDE = {"Arrow": "Ammo_Arrow_Broadhead", "Crossbow": "AmmoCrossbowBolt"}
HEAD = ["GroupOrder", "Group", "Order", "Key", "MenuName", "AMMO_FormID", "AMMO_EDID", "AMMO_FULL",
        "Count", "SellGain", "BuyCost", "Count_GLOB", "SellGain_GLOB", "BuyCost_GLOB"]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="Fallout76 Data folder (default: LIVE install, or Playtest with --pts)")
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--outdir")
    a = ap.parse_args(argv)
    data = a.data or (PTS_DATA if a.pts else LIVE_DATA)
    S = load_strings(os.path.join(data, "SeventySix - Localization.ba2"))
    R = load_records(os.path.join(data, "SeventySix.esm"), {b"TERM", b"GLOB", b"AMMO"})

    def lstr(b):
        if len(b) == 4:
            return S.get(struct.unpack("<I", b)[0], "")
        return b.rstrip(b"\0").decode("utf-8", "replace")

    by_edid, glob_val, ammo_by_edid = {}, {}, {}
    for fid, (sig, subs) in R.items():
        ed = edid_of(subs)
        if sig == "TERM":
            by_edid[ed] = (fid, subs)
        elif sig == "GLOB":
            for k, v in subs:
                if k == "FLTV" and len(v) >= 4:
                    glob_val[ed] = struct.unpack("<f", v[:4])[0]
        elif sig == "AMMO":
            full = ""
            for k, v in subs:
                if k == "FULL":
                    full = lstr(v)
            ammo_by_edid[ed] = (fid, full)
    fid_edid = {fid: edid_of(subs) for fid, (sig, subs) in R.items() if sig == "TERM"}

    def items(edid):
        out, cur = [], None
        for k, v in by_edid[edid][1]:
            if k == "ITXT":
                cur = {"text": lstr(v), "target": None}
                out.append(cur)
            elif k == "TNAM" and cur is not None and len(v) >= 4:
                cur["target"] = fid_edid.get(struct.unpack("<I", v[:4])[0])
        return out

    if MAIN not in by_edid:
        raise SystemExit(f"{MAIN} not found in the ESM - has the Ammo Converter been renamed?")
    lines, seen = ["\t".join(HEAD)], set()
    for gi, grp in enumerate(items(MAIN), 1):
        sub = grp["target"]
        if not sub or sub not in by_edid:
            continue
        order = 0
        for it in items(sub):
            t = it["target"] or ""
            if not t.startswith(AMMO_PREFIX):
                continue                       # "Back to Ammo Type Selection..."
            key = t[len(AMMO_PREFIX):]
            if key in seen:
                continue
            seen.add(key)
            order += 1
            g = {n: f"AmmoConverter_{key}_Ammo_{n}" for n in ("Count", "Point_SellGain", "Point_BuyCost")}
            for n, ge in g.items():
                if ge not in glob_val:
                    print(f"[AmmoConv] WARN {ge} missing")
            aed = AMMO_EDID_OVERRIDE.get(key, "Ammo" + key)
            afid, afull = ammo_by_edid.get(aed, (None, ""))
            if afid is None:
                print(f"[AmmoConv] WARN no AMMO record {aed} for {key}")
            menu = it["text"].strip()
            row = [str(gi), grp["text"].strip(), str(order), key, menu,
                   f"{afid:08X}" if afid else "", aed if afid else "", afull,
                   "%g" % glob_val.get(g["Count"], 0), "%g" % glob_val.get(g["Point_SellGain"], 0),
                   "%g" % glob_val.get(g["Point_BuyCost"], 0), g["Count"], g["Point_SellGain"], g["Point_BuyCost"]]
            lines.append("\t".join(clean_cell(c) for c in row))

    # Settings for ammo the menus don't list (new ammo added to the files but not the terminal yet)
    for ge in sorted(glob_val):
        if ge.startswith("AmmoConverter_") and ge.endswith("_Ammo_Count"):
            key = ge[len("AmmoConverter_"):-len("_Ammo_Count")]
            if key not in seen:
                print(f"[AmmoConv] WARN {key} has converter settings but isn't on the terminal menu - left out")

    channel = "PTS" if a.pts else "LIVE"
    outdir = a.outdir or (os.path.join(REPO, "tsv", "pts") if a.pts else os.path.join(REPO, "tsv"))
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, out_name("AMMOCONV_Export", channel))
    write_tsv(path, lines)
    if not a.outdir:
        append_ledger(channel, path)
    print(f"[AmmoConv] {len(lines) - 1} ammo types -> {path}")


if __name__ == "__main__":
    main()
