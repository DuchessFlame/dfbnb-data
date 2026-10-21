#!/usr/bin/env python3
r"""
export_from_esm.py - Python versions of the simple xEdit exports.

Writes the same TSVs as the matching  xedit scripts\!!!Wordpress - Export*.pas
straight from the game files, so these no longer need xEdit:

    GMST    GMST_Export_<Month>_<Year>.tsv        (ExportGMSTToTSV.pas)
    EMOT    EMOT_Export_<Month>_<Year>.tsv        (ExportEMOTToTSV.pas)
    MESG    MESG_Help_Export_<Month>_<Year>.tsv   (ExportMESGHelpToTSV.pas)
    MGEFKW  MGEFKW_Export_<Month>_<Year>.tsv      (ExportMGEFKeywordsToTSV.pas)
    FLOR    FLOR_Export_<Month>_<Year>.tsv        (ExportFLORToTSV.pas)

Run on the PC with the game installed:
    python tools\export_from_esm.py                 every export, LIVE  -> tsv\
    python tools\export_from_esm.py --pts           every export, PTS   -> tsv\pts\
    python tools\export_from_esm.py GMST EMOT       just those
Options: --data "<Data folder>"  --out "<folder>"  --no-ledger
Each file written adds a line to tsv\_exports.tsv, like the .pas scripts do.
"""
from __future__ import annotations

import argparse
import os
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import esm_reader as R  # noqa: E402

cc = R.clean_cell


def _fmt_float(v):
    s = "%.6f" % v
    return "-0.000000" if s == "-0.000000" else s


# ------------------------------------------------------------- GMST ------
def exp_gmst(esm):
    types = {"f": "float", "i": "int", "u": "uint", "b": "bool", "s": "string"}
    out = ["FormID\tEDID\tType\tValue"]
    for r in R.xedit_order(esm, "GMST"):
        ed = cc(r.edid)
        t = types.get(ed[:1].lower(), "")
        d = r.first("DATA")
        v = ""
        if d is not None:
            if t == "float":
                v = _fmt_float(struct.unpack("<f", d[:4])[0])
            elif t == "int":
                v = str(struct.unpack("<i", d[:4])[0])
            elif t == "uint":
                v = str(struct.unpack("<I", d[:4])[0])
            elif t == "bool":
                v = "True" if struct.unpack("<I", d[:4])[0] else "False"
            elif t == "string":
                v = esm.lstring(d)
        out.append("%08X\t%s\t%s\t%s" % (r.fid, ed, t, cc(v)))
    return out


# ------------------------------------------------------------- EMOT ------
def _link(esm, raw):
    if not raw or len(raw) < 4:
        return ""
    return esm.link_edit_value(struct.unpack("<I", raw[:4])[0])


def exp_emot(esm):
    out = ["FormID\tEDID\tFULL\tRENT\tSNAM\tCNAM\tDNAM"]
    for r in R.xedit_order(esm, "EMOT"):
        rent = r.first("RENT")
        rent_s = ""
        if rent:
            fid = struct.unpack("<I", rent[:4])[0]
            rent_s = cc(esm.edid_of(fid)) if esm.exists(fid) else cc(esm.link_edit_value(fid))
        snam = r.first("SNAM")
        out.append("\t".join([
            "%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.first("FULL"))), rent_s,
            cc(snam.rstrip(b"\0").decode("utf-8", "replace") if snam else ""),
            cc(_link(esm, r.first("CNAM"))), cc(_link(esm, r.first("DNAM")))]))
    return out


# ------------------------------------------------------------- MESG ------
def exp_mesg_help(esm):
    out = ["MESG_FormID\tMESG_EDID\tFULL_Name\tDESC_Description"]

    def fix(s):
        return s.replace("\t", " ").replace("\r", " ").replace("\n", " ").replace('"', '""')

    for r in R.xedit_order(esm, "MESG"):
        ed = r.edid
        if not ed.startswith("Help"):
            continue
        full = fix(esm.lstring(r.first("FULL")))
        desc = fix(esm.lstring(r.first("DESC")))
        if desc.startswith("- "):
            desc = desc[2:]
        out.append('"%08X"\t"%s"\t"%s"\t"%s"' % (r.fid, ed, full, desc))
    return out


# ----------------------------------------------------------- MGEFKW ------
def exp_mgefkw(esm):
    out = ["MGEF_FormID\tEDID\tDATA_Flags_Hex\tDetrimental\tKeywordCount\tKeywords_Flat"]
    for r in R.xedit_order(esm, "MGEF"):
        flat, cnt, detr = [], 0, "0"
        kw = r.first("KWDA")
        if kw:
            # xEdit shows KWDA sorted by FormID
            for fid in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw)):
                if not esm.exists(fid):
                    continue
                ed = esm.edid_of(fid)
                flat.append("%s[%08X]" % (cc(ed), fid))
                if ed == "EffectTypeDetrimental":
                    detr = "1"
                cnt += 1
        flags_hex = ""
        d = r.first("DATA")
        if d and len(d) >= 4:
            fl = struct.unpack("<I", d[:4])[0]
            if fl & 0x80000000:
                # xEdit quirk kept on purpose: the .pas reads Flags with GetNativeValue,
                # which fails when the top bit is set, and its "except" writes 0.
                fl = 0
            flags_hex = "%08X" % fl
            if fl & 4:
                detr = "1"
        out.append("%08X\t%s\t%s\t%s\t%d\t%s" % (r.fid, cc(r.edid), flags_hex, detr, cnt, " | ".join(flat)))
    return out


# ------------------------------------------------------------- FLOR ------
def exp_flor(esm):
    out = ["FLOR_FormID\tFLOR_EDID\tFLOR_FULL\tProduce\tProduceCount"]
    for r in R.xedit_order(esm, "FLOR"):
        produce = ""
        pf = r.first("PFIG")
        if pf and len(pf) >= 4:
            fid = struct.unpack("<I", pf[:4])[0]
            if fid and esm.exists(fid):
                produce = "%08X:%s:%s" % (fid, cc(esm.edid_of(fid)), esm.sig_of(fid))
        count = "1"
        pc = r.first("PFPC")
        if pc:
            count = str(pc[0]) or "1"
        out.append("%08X\t%s\t%s\t%s\t%s" % (r.fid, cc(r.edid), cc(esm.lstring(r.first("FULL"))), produce, count))
    return out


EXPORTS = {
    # key: (record types to load, file prefix, function)
    "GMST": (["GMST"], "GMST_Export", exp_gmst),
    "EMOT": (["EMOT"], "EMOT_Export", exp_emot),
    "MESG": (["MESG"], "MESG_Help_Export", exp_mesg_help),
    "MGEFKW": (["MGEF"], "MGEFKW_Export", exp_mgefkw),
    "FLOR": (["FLOR"], "FLOR_Export", exp_flor),
}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("which", nargs="*", help="exports to run (default: all): " + " ".join(EXPORTS))
    ap.add_argument("--pts", action="store_true", help="PTS: read the Playtest install, write tsv\\pts\\")
    ap.add_argument("--data", help="game Data folder (default: the live or PTS install)")
    ap.add_argument("--out", help="output folder (default: tsv\\ or tsv\\pts\\)")
    ap.add_argument("--no-ledger", action="store_true", help="don't add lines to tsv\\_exports.tsv")
    a = ap.parse_args(argv)

    which = [w.upper() for w in a.which] or list(EXPORTS)
    bad = [w for w in which if w not in EXPORTS]
    if bad:
        ap.error("unknown export(s): %s  (choose from %s)" % (" ".join(bad), " ".join(EXPORTS)))
    channel = "PTS" if a.pts else "LIVE"
    data = a.data or (R.PTS_DATA if a.pts else R.LIVE_DATA)
    out_dir = a.out or os.path.join(R.REPO, "tsv", "pts" if a.pts else "")
    if not os.path.isfile(os.path.join(data, "SeventySix.esm")):
        sys.exit("SeventySix.esm not found in %s - use --data" % data)

    sigs = sorted({s for w in which for s in EXPORTS[w][0]})
    t0 = time.time()
    print("[%s] reading %s ..." % (channel, data))
    esm = R.Esm(data, sigs)
    print("  loaded in %.0fs" % (time.time() - t0))
    for w in which:
        _sigs, prefix, fn = EXPORTS[w]
        lines = fn(esm)
        if len(lines) <= 1:
            print("  %-7s no records found - nothing written" % w)
            continue
        path = os.path.join(out_dir, R.out_name(prefix, channel))
        R.write_tsv(path, lines)
        if not a.no_ledger:
            R.append_ledger(channel, path)
        print("  %-7s %5d rows -> %s" % (w, len(lines) - 1, path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
