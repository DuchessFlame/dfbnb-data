#!/usr/bin/env python3
r"""
export_from_esm.py - Python versions of the simple xEdit exports.

Writes the same TSVs as the matching  xedit scripts\!!!Wordpress - Export*.pas
straight from the game files, so these no longer need xEdit:

    GMST    GMST_Export_<Month>_<Year>.tsv          (ExportGMSTToTSV.pas)
    EMOT    EMOT_Export_<Month>_<Year>.tsv          (ExportEMOTToTSV.pas)
    MESG    MESG_Help_Export_<Month>_<Year>.tsv     (ExportMESGHelpToTSV.pas)
    MGEFKW  MGEFKW_Export_<Month>_<Year>.tsv        (ExportMGEFKeywordsToTSV.pas)
    FLOR    FLOR_Export_<Month>_<Year>.tsv          (ExportFLORToTSV.pas)
    CMPT    CMPT_Export_<Month>_<Year>.tsv          (ExportCMPTToTSV.pas)
    CHAL    CHAL_Export_<Month>_<Year>.tsv          (ExportCHALToTSV.pas)
    PLYT    PLYT_Export_<Month>_<Year>.tsv          (ExportPlayerTitleToCSV.pas)
    SPEL    SPEL_Export_<Month>_<Year>_HEADER/_EFFECTS/_CONDITIONS.tsv  (ExportSPELToCSV.pas)
    GLOB    GLOB_Export_<Month>_<Year>.tsv          (ExportGLOBToCSV.pas)
    AVIF    AVIF_Export_<Month>_<Year>.tsv          (ExportAVIFToTSV.pas)

GLOB and AVIF list every record that points at each row ("Referenced By"),
worked out by esm_refs.py in one ~40 s scan (needs numpy). Known gap: xEdit
stops reading some quests part-way through their objective/alias conditions, so
its lists leave out those quests; these lists include them.

Conditions are shown exactly as xEdit shows them (esm_conditions.py, built from
xEdit's own definitions in xedit_functions.py). Links to placed references, cells
and worldspaces trigger a one-off ~20 s scan of the world data.
Checked byte-for-byte against the xEdit TSVs of the same game build.

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
import esm_conditions as C  # noqa: E402

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


# ------------------------------------------------- shared .pas helpers ------
def _cond_cols(esm, r, stop_at=("NEXT",)):
    ctx = C.Ctx(esm, r)
    return [cc(C.pas_cond(ctx, *c, clean=cc)) for c in C.conditions(r, stop_at)]


def _pad_conds(rows, base_header, prefix="Cond"):
    """Variable-width condition columns, like the .pas WriteOutput: pad to the max count."""
    mx = max((len(c) for _b, c in rows), default=0)
    out = ["\t".join(base_header + ["%s%d" % (prefix, i) for i in range(1, mx + 1)])]
    for base, conds in rows:
        out.append("\t".join(base + conds + [""] * (mx - len(conds))))
    return out


def _u32(v):
    return struct.unpack("<I", v[:4])[0] if v and len(v) >= 4 else None


def _str(v):
    return v.rstrip(b"\0").decode("utf-8", "replace") if v else ""


def _bool_enum(v):
    if not v:
        return ""
    return "True" if v[0] else "False"


# ------------------------------------------------------------- CMPT ------
def exp_cmpt(esm):
    rows = []
    for r in R.xedit_order(esm, "CMPT"):
        conds = _cond_cols(esm, r)
        rows.append(([("%08X" % r.fid), cc(r.edid), cc(esm.lstring(r.first("ANAM"))),
                      _bool_enum(r.first("PTPR")), _bool_enum(r.first("PTSU")), str(len(conds))], conds))
    return _pad_conds(rows, ["FormID", "EDID", "ANAM", "PTPR", "PTSU", "CondCount"])


# ------------------------------------------------------------- CHAL ------
CHAL_FREQ = ["Daily", "Weekly", "Lifetime", "Monthly", "Event", "Seasonal"]
CHAL_CAT = ["Character", "Survival", "Exploration", "Pets", "Combat", "Social", "World",
            "Tracker", "Sub Challenge (Unsorted)", "Fishing", "Burning Springs"]


def _enum(names, v):
    if v is None:
        return ""
    return names[v] if v < len(names) else str(v)


def exp_chal(esm):
    rows = []
    for r in R.xedit_order(esm, "CHAL"):
        conds = _cond_cols(esm, r)
        tn = _u32(r.first("TNAM"))
        hn = _u32(r.first("HNAM"))
        an = _u32(r.first("ANAM"))
        base = ["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.first("FULL"))),
                cc(_str(r.first("SNAM"))), "" if tn is None else str(tn),
                _enum(CHAL_FREQ, _u32(r.first("CNAM"))), _enum(CHAL_CAT, _u32(r.first("ENAM"))),
                cc(esm.lstring(r.first("MNAM"))), cc(_str(r.first("RNAM"))),
                "" if hn is None else cc(esm.link_edit_value(hn)),
                cc(_str(r.first("JASF"))),
                "" if an is None else cc(esm.link_edit_value(an)), str(len(conds))]
        rows.append((base, conds))
    return _pad_conds(rows, ["FormID", "EDID", "FULL", "SNAM", "TNAM", "CNAM", "ENAM", "MNAM",
                             "RNAM", "HNAM", "JASF", "ANAM", "CondCount"])


# ------------------------------------------------------------- PLYT ------
def exp_plyt(esm):
    """ExportPlayerTitleToCSV.pas - its own readable condition style:
    Top:<Run On>.<Function>(<params>) = <comparison>  (the .pas always writes '=')."""
    def esc(s):
        return s.replace("\t", " ").replace("\r", " ").replace("\n", " ")

    def junk(name, val):
        if val == "" or name in ("Unused", "None", "Run On", "Type", "Function"):
            return True
        if name.startswith("Comparison"):
            return True
        return val in ("0", "0.000000", "00 00 00 00", "-1")

    rows = []
    for r in R.xedit_order(esm, "PLYT"):
        ctx = C.Ctx(esm, r)
        conds = []
        for c in C.conditions(r):
            els, _ref = C.elements(ctx, *c)
            d = {n: v for n, v in els}
            cmp_ = d.get("Comparison Value - Float", d.get("Comparison Value - Global", ""))
            params = [v for n, v in els if not junk(n, v)]
            s = (d["Run On"] + "." if d["Run On"] else "") + d["Function"] + "(" + ", ".join(params) + ") = " + cmp_
            conds.append(esc("Top:" + esc(s)))
        rows.append((["SeventySix.esm", "%08X" % r.fid, esc(r.edid), esc(esm.lstring(r.first("ANAM"))),
                      esc(esm.lstring(r.first("BNAM"))), _bool_enum(r.first("PTPR")),
                      _bool_enum(r.first("PTSU")), str(len(conds))], conds))
    return _pad_conds(rows, ["Plugin", "FormID", "EDID - Editor ID", "ANAM - Male Title",
                             "BNAM - Female Title", "PTPR - Is Prefix", "PTSU - Is Suffix", "CondCount"])


# ------------------------------------------------------------- SPEL ------
SPEL_TYPE = ["Spell", "Disease", "Power", "Lesser Power", "Ability", "Poison", "Enchantment",
             "Potion", "Wortcraft", "Levelled", "Addiction", "Voice", "Staff Enchantment", "Scroll"]
CAST_TYPE = ["Constant Effect", "Fire and Forget", "Concentration", "Scroll"]
TARGET_TYPE = ["Self", "Contact", "Aimed", "Target Actor", "Target Location"]


def _strip(s):
    return (s or "").replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _effects(rec):
    """Split a record's subrecords into its Effects (each starts at EFID)."""
    effs, cur = [], None
    for k, v in rec.subs:
        if k == "EFID":
            cur = []
            effs.append(cur)
        elif k in ("MIID",):
            cur = None
        if cur is not None:
            cur.append((k, v))
    return effs


class _Sub:
    """A tiny Record look-alike over a slice of subrecords (one Effect)."""

    def __init__(self, rec, subs):
        self.sig, self.fid, self.subs, self.version = rec.sig, rec.fid, subs, rec.version

    def first(self, sub):
        return next((v for k, v in self.subs if k == sub), None)


def exp_spel(esm):
    header = ["SPEL_FormID\tSPEL_EDID\tSPEL_FULL\tSPEL_DESC\tSPIT_Type\tSPIT_CastType\tSPIT_Delivery\tSpellConditions"]
    effects = ["SPEL_FormID\tSPEL_EDID\tSPEL_FULL\tEffectIndex\tEFID_MGEF_FormID\tEFID_MGEF_EDID\tEFID_MGEF_FULL\t"
               "CVTO_CURV_FormID\tCVTO_CURV_EDID\tMAGA_AVIF_FormID\tMAGA_AVIF_EDID\tEFIT_Magnitude\tEFIT_Area\t"
               "EFIT_Duration\tDURG_Global\tMAGG_Global\tCODG_Global\tConditionCount\tConditions"]
    conds_out = ["SPEL_FormID\tSPEL_EDID\tEffectIndex\tCondIndex\tType_Raw\tOperator\tAndOr\tRunOn\tFunction\t"
                 "Param1\tParam2\tParam3\tCompareValue\tReference\tCondition"]

    def link_fid(fid):
        return "%08X" % fid if fid and esm.exists(fid) else ""

    def link_edid(fid):
        return _strip(esm.edid_of(fid)) if fid and esm.exists(fid) else ""

    def sig_link(sub):
        fid = _u32(sub)
        if sub is None:
            return ""
        if fid and esm.exists(fid):
            return "%08X %s" % (fid, _strip(esm.edid_of(fid)))
        return _strip(esm.link_edit_value(fid))

    def cond_rows(r, owner, eff_index):
        ctx = C.Ctx(esm, r)
        texts = []
        for ci, c in enumerate(C.conditions(owner)):
            els, ref = C.elements(ctx, *c)
            d = {n: v for n, v in els}
            typ = _strip(d["Type"])
            op = C.op_from_type(typ)
            andor = "OR" if typ[3] == "1" else "AND"
            cmp_ = _strip(d.get("Comparison Value - Float", d.get("Comparison Value - Global", "")))
            fn, run_on = _strip(d["Function"]), _strip(d["Run On"])
            refv = _strip(d.get("Reference", d.get("Unused", "")))
            refv = _strip(els[8][1])
            params = []
            for n, v in els:
                v = _strip(v)
                low = n.lower()
                if low in ("run on", "reference", "function", "type") or low.startswith("comparison") \
                        or low.startswith("unused") or v.strip() == "":
                    continue
                if "#3" in n and v.strip() == "-1":
                    continue
                params.append(v)
            p = params + ["", "", ""]
            text = (run_on + "." if run_on else "") + fn + "(" + ", ".join(params) + ") " + op + " " + cmp_ + " " + andor
            conds_out.append("\t".join(["%08X" % r.fid, _strip(r.edid), str(eff_index), str(ci), typ, op, andor,
                                        run_on, fn, p[0], p[1], p[2], cmp_, refv, text]))
            texts.append(text)
        return " | ".join(texts), len(texts)

    for r in R.xedit_order(esm, "SPEL"):
        full = _strip(esm.lstring(r.first("FULL")))
        spit = r.first("SPIT")
        sp_type = cast = deliv = ""
        if spit and len(spit) >= 20:
            t, cast_i, targ = struct.unpack_from("<I", spit, 8)[0], *struct.unpack_from("<II", spit, 16)
            sp_type, cast, deliv = _enum(SPEL_TYPE, t), _enum(CAST_TYPE, cast_i), _enum(TARGET_TYPE, targ)
        # SPEL has no record-level Conditions, so the .pas always leaves SpellConditions empty
        header.append("\t".join(["%08X" % r.fid, _strip(r.edid), full,
                                 _strip(esm.lstring(r.first("DESC"))), sp_type, cast, deliv, ""]))
        for i, eff in enumerate(_effects(r)):
            e = _Sub(r, eff)
            mg = _u32(e.first("EFID")) or 0
            cv = _u32(e.first("CVT0")) or 0
            ma = _u32(e.first("MAGA")) or 0
            efit = e.first("EFIT")
            mag = area = dur = ""
            if efit:
                o = 4 if r.version >= 166 else 0
                if len(efit) >= o + 12:
                    m, area_i, dur_i = struct.unpack_from("<fII", efit, o)
                    mag, area, dur = C.fmt_float(m), str(area_i), str(dur_i)
            text, n = cond_rows(r, e, i)
            effects.append("\t".join([
                "%08X" % r.fid, _strip(r.edid), full, str(i),
                link_fid(mg), link_edid(mg), _strip(esm.full_of(mg)) if mg else "",
                link_fid(cv), link_edid(cv), link_fid(ma), link_edid(ma),
                mag, area, dur, sig_link(e.first("DURG")), sig_link(e.first("MAGG")),
                sig_link(e.first("CODG")), str(n), text]))
    return {"_HEADER": header, "_EFFECTS": effects, "_CONDITIONS": conds_out}


# ------------------------------------------------- Referenced By exports ------
def _refcells(esm, fid):
    out = []
    for r in esm.refby.get(fid, ()):
        sig, ed = esm.refinfo[r]
        out.append(cc("%08X:%s:%s" % (r, ed, sig)))
    return out


def _pad_refs(rows, base_header, prefix="Ref"):
    return _pad_conds(rows, base_header, prefix)


def exp_glob(esm):
    rows = []
    for r in R.xedit_order(esm, "GLOB"):
        fl = r.first("FLTV")
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, cc(r.edid),
                      C.fmt_float(struct.unpack("<f", fl[:4])[0]) if fl and len(fl) >= 4 else "",
                      str(len(refs))], refs))
    return _pad_refs(rows, ["FormID", "EDID", "FLTV", "ReferencedByCount"])


def exp_avif(esm):
    rows = []
    for r in R.xedit_order(esm, "AVIF"):
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full)), cc(esm.lstring(r.first("DESC"))),
                      cc(esm.lstring(r.first("ANAM"))), str(len(refs))], refs))
    return _pad_refs(rows, ["FormID", "EDID", "FULL", "DESC", "ANAM", "ReferencedByCount"])


# ------------------------------------------------------------- MISC ------
def _kw_sorted(esm, kwda):
    """The .pas keyword list: EDIDs in a sorted, de-duplicated TStringList (case-insensitive), | joined."""
    if not kwda:
        return ""
    eds = {}
    for fid in struct.unpack_from("<%dI" % (len(kwda) // 4), kwda):
        if esm.exists(fid):
            ed = esm.edid_of(fid).strip()
            if ed:
                eds.setdefault(ed.lower(), ed)
    return "|".join(eds[k] for k in sorted(eds, key=R.ansi_sort_key))


def exp_misc(esm):
    rows = []
    for r in R.xedit_order(esm, "MISC"):
        d = r.first("DATA")
        value = weight = ""
        if d and len(d) >= 8:
            v, w = struct.unpack_from("<if", d)
            value, weight = str(v), C.fmt_float(w)
        mcqp = []
        m = r.first("MCQP")
        if m:
            for i in range(len(m) // 8):
                comp, tier = struct.unpack_from("<II", m, i * 8)
                ce = cc(esm.edid_of(comp)) if esm.exists(comp) else cc(esm.link_edit_value(comp))
                if not ce:
                    continue
                te = cc(esm.edid_of(tier)) if esm.exists(tier) else (cc(esm.link_edit_value(tier)) if tier else "")
                mcqp.append(ce + ":" + te)
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full)), _kw_sorted(esm, r.first("KWDA")),
                      value, weight, "|".join(mcqp), str(len(refs))], refs))
    return _pad_refs(rows, ["FormID", "EDID", "FULL", "Keywords", "Value", "Weight", "MCQP", "ReferencedByCount"])


# exports whose rows list "Referenced By" - one shared scan covers them all
NEEDS_REFS = {"GLOB", "AVIF", "MISC"}

EXPORTS = {
    # key: (record types to load, file prefix, function)
    "GMST": (["GMST"], "GMST_Export", exp_gmst),
    "EMOT": (["EMOT"], "EMOT_Export", exp_emot),
    "MESG": (["MESG"], "MESG_Help_Export", exp_mesg_help),
    "MGEFKW": (["MGEF"], "MGEFKW_Export", exp_mgefkw),
    "FLOR": (["FLOR"], "FLOR_Export", exp_flor),
    "CMPT": (["CMPT"], "CMPT_Export", exp_cmpt),
    "CHAL": (["CHAL"], "CHAL_Export", exp_chal),
    "PLYT": (["PLYT"], "PLYT_Export", exp_plyt),
    "SPEL": (["SPEL"], "SPEL_Export", exp_spel),
    "GLOB": (["GLOB"], "GLOB_Export", exp_glob),
    "AVIF": (["AVIF"], "AVIF_Export", exp_avif),
    "MISC": (["MISC"], "MISC_Export", exp_misc),
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
    esm.refby, esm.refinfo = {}, {}
    ref_exports = [w for w in which if w in NEEDS_REFS]
    if ref_exports:
        import esm_refs
        t1 = time.time()
        targets = {r.fid for w in ref_exports for s in EXPORTS[w][0] for r in esm.records[s]}
        esm.refby, esm.refinfo = esm_refs.referenced_by(esm, targets)
        print("  Referenced By worked out for %d records in %.0fs" % (len(targets), time.time() - t1))
    for w in which:
        _sigs, prefix, fn = EXPORTS[w]
        result = fn(esm)
        # one file, or {suffix: lines} for exports that write several files
        files = result if isinstance(result, dict) else {"": result}
        base = R.out_name(prefix, channel)[:-4]
        for suffix, lines in files.items():
            if len(lines) <= 1:
                print("  %-7s no records found - nothing written" % (w + suffix))
                continue
            path = os.path.join(out_dir, base + suffix + ".tsv")
            R.write_tsv(path, lines)
            if not a.no_ledger:
                R.append_ledger(channel, path)
            print("  %-7s %5d rows -> %s" % (w, len(lines) - 1, path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
