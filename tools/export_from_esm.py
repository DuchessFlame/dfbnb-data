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
    KYWD    KYWD_Export_<Month>_<Year>.tsv + _Refs  (ExportKYWDToCSV.pas)
    CURV    CURV_Export_<Month>_<Year>_CURV.tsv      (ExportCURVToTSV.pas; points: extract_curvetables.py + CI)
    CMPO    CMPO_Export_<Month>_<Year>.tsv          (ExportCMPOToTSV.pas)
    RESO    RESO_Export_<Month>_<Year>.tsv          (ExportRESOToTSV.pas)
    BPTD    BPTD_Export_<Month>_<Year>.tsv          (ExportBPTDToTSV.pas)
    ECAT    ECAT_Export_<Month>_<Year>.tsv          (ExportECATToTSV.pas)
    CNDF    CNDF_Export_<Month>_<Year>.tsv          (ExportCNDFToTSV.pas)
    LSCR    LSCR_Export_<Month>_<Year>.tsv          (ExportLSCRToTSV.pas)
    MISC    MISC_Export_<Month>_<Year>.tsv          (ExportMISCToCSV.pas)
    NPC     NPC_Export_<Month>_<Year>.tsv + _PRPS + _Refs  (ExportNPCToTSV.pas)
    NPC2    NPC2_Vendors_<Month>_<Year>.tsv + _Placements, _Currencies, _Factions,
            _FactionsPlacements                     (ExportNPC2VendorsToTSV.pas)
    MISC2   MISC2_Export_<Month>_<Year>.tsv         (ExportMISC2ToCSV.pas)
    ACTI    ACTI_Export_<Month>_<Year>_ACTI.tsv + _Refs  (ExportACTIToTSV.pas)
    ACTI2   ACTI2_Export_<Month>_<Year>.tsv         (ExportACTI2ToCSV.pas)
    REFRLVLI REFRLVLI_Placements_<Month>_<Year>.tsv [+ _Part2 ...]  (ExportREFRPlacementsToTSV.pas on LVLI)
    LORE    ALLYDLG_Export_<Month>_<Year>_Lore.tsv  (the _Lore file of
            ExportCampAllyDialogToTSV.pas: radio / holotape dialogue lines)
    QUST2RQ QUST2_Export_<Month>_<Year>_RewardQuests.tsv  (the _RewardQuests file of
            ExportQUST2ToTSV.pas - only that one file; the .pas stays the manual
            backup and still writes all six)
    QUSTSP  QUST2_Export_<Month>_<Year>_ScriptProps.tsv  (quest script properties:
            reward lists and chance GLOBs a quest's script rolls itself)

GLOB, AVIF, KYWD, CURV, MISC and NPC list every record that points at each row ("Referenced By"),
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
import math
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


# ------------------------------------------------------------- KYWD ------
# xEdit's names for KYWD TNAM (wbKeywordTypeEnum); anything past the list prints as its number
KYWD_TYPES = ["None", "Component Tech Level", "Attach Point", "Component Property",
              "Instantiation Filter", "Mod Association", "Sound", "Anim Archetype",
              "Function Call", "Recipe Filter", "Attraction Type", "Dialogue Subtype",
              "Quest Target", "Anim Flavor", "Anim Gender", "Anim Face", "Quest Group",
              "Anim Injured", "Dispel Effect", "Mod Attribute Name", "Mod Attribute Descriptor",
              "UI Icon Linkage Name", "Crafting Function", "Item Quality", "Component Quantity",
              "Atomic Shop Category", "Photomode Category", "Atomic Shop Filter", "Event Header",
              "Loot Bag", "Item Rarity", "Item Type", "Ghoul"]
KYWD_SKIP = 0x00195411   # MultirefLOD - 44,000+ refs of system bloat; the .pas skips it too


def _xedit_bits(v, empty_width=0):
    """xEdit's edit value of a flags field: one 0/1 per bit, lowest bit first,
    up to the highest bit that is set ('' when none - or empty_width zeros)."""
    if not v:
        return "0" * empty_width
    return "".join("1" if v >> i & 1 else "0" for i in range(v.bit_length()))


def _zstr(raw):
    """A plain (not localised) string. Most are UTF-8; a few notes were typed in
    Windows-1252 (e.g. a curly apostrophe), which xEdit reads that way too."""
    if not raw:
        return ""
    raw = raw.rstrip(b"\0")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", "replace")


def exp_kywd(esm):
    """ExportKYWDToCSV.pas - core sheet and _Refs (one row per Referenced By entry)."""
    core = ["FormID\tEDID\tFULL_Name\tNNAM_DisplayName\tTNAM_Type\tCNAM_Color\tDNAM_Notes\t"
            "FNAM_Unknown\tXALG_Flags\tFormTags\tENAM_IconName\tDATA_AttractionRule\t"
            "NTWK_NetworkMarker\tReferencedByCount"]
    refs = ["KeywordFormID\tKeywordEDID\tRefIndex\tRefFormID\tRefEDID\tRefSignature\tRefName"]
    _dial_index(esm)              # dialogue topics sit outside the shared index
    dial_full = esm._dial_full
    for r in R.xedit_order(esm, "KYWD"):
        if r.fid == KYWD_SKIP:
            continue
        fid, ed = "%08X" % r.fid, cc(r.edid)
        t = r.first("TNAM")
        tnam = ""
        if t is not None and len(t) >= 4:
            n = struct.unpack_from("<I", t)[0]
            tnam = KYWD_TYPES[n] if n < len(KYWD_TYPES) else str(n)
        f = r.first("FNAM")
        x = r.first("XALG")
        xalg = _xedit_bits(int.from_bytes(x, "little")) if x else ""
        # CNAM (a colour struct), DATA and NTWK print blank from the .pas's
        # GetElementEditValues - kept blank to match.
        rl = esm.refby.get(r.fid, ())
        core.append("\t".join([
            fid, ed, cc(esm.lstring(r.full)), cc(_zstr(r.first("NNAM"))), tnam, "",
            cc(_zstr(r.first("DNAM"))), " ".join("%02X" % b for b in f) if f else "",
            xalg, _xedit_bits(r.flags, 64), cc(_zstr(r.first("ENAM"))), "", "", str(len(rl))]))
        for i, h in enumerate(rl):
            sig, red = esm.refinfo[h]
            name = esm.lstring(dial_full.get(h)) if sig == "DIAL" else esm.full_of(h)
            refs.append("\t".join([fid, ed, str(i), "%08X" % h, cc(red), sig, cc(name)]))
    return {"": core, "_Refs": refs}


# ------------------------------------------------------------- CURV ------
def exp_curv(esm):
    """ExportCURVToTSV.pas - the records file (_CURV). The X/Y points are not in the
    record: extract_curvetables.py refreshes the curve JSONs from Startup.ba2 and
    CI's build_curv_points_tsv.py makes the _POINTS file from them."""
    rows = []
    for r in R.xedit_order(esm, "CURV"):
        path = cc(_zstr(r.first("JASF")))
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, cc(r.edid), path, path.replace("/", "\\").rsplit("\\", 1)[-1],
                      str(len(refs))], refs))
    return {"_CURV": _pad_refs(rows, ["FormID", "EDID", "JASF_Path", "JsonFileName",
                                      "ReferencedByCount"], prefix="Ref_")}


# ------------------------------------------------------------- CMPO ------
def exp_cmpo(esm):
    """ExportCMPOToTSV.pas - components: scrap item, scrap scalar, quantity tiers."""
    out = ["CMPO_FormID\tCMPO_EDID\tFULL\tScrapItem_FormID\tScrapItem_EDID\tScrapItem_FULL\t"
           "ModScrapScalar_FormID\tModScrapScalar_EDID\tCVPA\tReferencedByCount"]

    def link(raw):
        fid = struct.unpack_from("<I", raw)[0] if raw and len(raw) >= 4 else 0
        return _lk(esm, fid)

    for r in R.xedit_order(esm, "CMPO"):
        scrap, scalar = link(r.first("MNAM")), link(r.first("GNAM"))
        # CVPA: 12-byte entries (keyword, count, curve). xEdit shows the array
        # sorted on the keyword's FormID; entries whose keyword won't resolve are skipped.
        cv = r.first("CVPA") or b""
        ents = sorted((struct.unpack_from("<III", cv, i * 12) for i in range(len(cv) // 12)),
                      key=lambda t: t[0])
        tiers = []
        for kw, n, curve in ents:
            k = _lk(esm, kw)
            if k is None:
                continue
            ced = cc(esm.edid_of(curve)) if _lk(esm, curve) is not None else ""
            tiers.append("%s:%d:%s" % (cc(esm.edid_of(k)), n, ced))
        out.append("\t".join([
            "%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full)),
            _hex(scrap), esm.edid_of(scrap) if scrap else "", cc(esm.full_of(scrap)) if scrap else "",
            _hex(scalar), esm.edid_of(scalar) if scalar else "",
            "|".join(tiers), str(len(esm.refby.get(r.fid, ())))]))
    return out


# ------------------------------------------------------------- RESO ------
def exp_reso(esm):
    """ExportRESOToTSV.pas - workshop resources (actor value, produce list, interval)."""
    rows = []
    for r in R.xedit_order(esm, "RESO"):
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, cc(r.edid)] +
                     [cc(_link(esm, r.first(s))) if r.first(s) is not None else "" for s in ("NAM1", "NAM2", "NAM4")] +
                     [str(len(refs))], refs))
    return _pad_refs(rows, ["FormID", "EDID", "NAM1_ActorValue", "NAM2_Produce", "NAM4_Interval",
                            "ReferencedByCount"])


# ------------------------------------------------------------- BPTD ------
BPTD_PART_TYPES = ["Torso", "Head1", "Eye", "LookAt", "Fly Grab", "Head2", "LeftArm1", "LeftArm2",
                   "RightArm1", "RightArm2", "LeftLeg1", "LeftLeg2", "LeftLeg3", "RightLeg1",
                   "RightLeg2", "RightLeg3", "Brain", "Weapon", "Root", "COM", "Pelvis", "Camera",
                   "Offset Root", "Left Foot", "Right Foot", "Face Target Source"]
BPTD_FLAGS = ["Severable", "Hit Reaction", "Hit Reaction - Default", "Explodable",
              "Cut - Meat Cap Sever", "On Cripple", "Explodable - Absolute Chance",
              "Show Cripple Geometry"]


def _strip_tsv(s):
    """The BPTD .pas StripTSV: tabs and line breaks to spaces, no trimming."""
    return (s or "").replace("\t", " ").replace("\r", " ").replace("\n", " ")


def exp_bptd(esm):
    """ExportBPTDToTSV.pas - one row per body part, in xEdit's 'Body Parts (sorted)'
    order (VATS target, upper-case text), with the races that use the record."""
    out = ["BPTD_FormID\tBPTD_EDID\tModel\tPartIndex\tBPTN_PartName\tBPNN_PartNode\t"
           "BPNT_VATSTarget\tDamageMult\tPartType\tHealthPercent\tActorValue_FormID\t"
           "ActorValue_EDID\tActorValue_FULL\tToHitChance\tExplosionChancePct\t"
           "NonLethalDismemberChance\tFlags\tLimbReplacementScale\tRefBy_RACE_FormIDs\t"
           "RefBy_RACE_EDIDs\tRefBy_RACE_Names"]
    for r in R.xedit_order(esm, "BPTD"):
        # a part starts at its name (BPTN) - or at its node (BPNN) when the name is missing
        parts, cur = [], None
        for k, v in r.subs:
            if k == "BPTN" or (k == "BPNN" and (cur is None or "BPNN" in cur)):
                cur = {}
                parts.append(cur)
            if cur is not None:
                cur.setdefault(k, v)
        if not parts:
            # xEdit still shows one empty part (all zero) for a record with none
            parts = [{"BPND": bytes(113)}]
        parts.sort(key=lambda p: _xedit_text_key(_zstr(p.get("BPNT"))))
        races = [h for h in esm.refby.get(r.fid, ()) if esm.refinfo[h][0] == "RACE"]
        race_cols = [" | ".join("%08X" % h for h in races),
                     " | ".join(_strip_tsv(esm.refinfo[h][1]) for h in races),
                     " | ".join(_strip_tsv(esm.full_of(h)) for h in races)]
        head = ["%08X" % r.fid, _strip_tsv(r.edid), _strip_tsv(_zstr(r.first("MODL")))]
        for i, p in enumerate(parts):
            d = p.get("BPND")
            vals = [""] * 11
            if d and len(d) >= 74:
                fl, pt, hp = d[64], d[65], d[66]
                av = _lk(esm, struct.unpack_from("<I", d, 67)[0])
                vals = [C.fmt_float(struct.unpack_from("<f", d, 0)[0]),
                        BPTD_PART_TYPES[pt] if pt < len(BPTD_PART_TYPES) else "<Unknown: %d>" % pt,
                        str(hp), _hex(av), _strip_tsv(esm.edid_of(av)) if av else "",
                        _strip_tsv(esm.full_of(av)) if av else "",
                        str(d[71]), str(d[72]), str(d[73]),
                        ", ".join(BPTD_FLAGS[b] for b in range(8) if fl >> b & 1),
                        C.fmt_float(struct.unpack_from("<f", d, 60)[0])]
            out.append("\t".join(head + [str(i), _strip_tsv(esm.lstring(p.get("BPTN"))),
                                          _strip_tsv(_zstr(p.get("BPNN"))), _strip_tsv(_zstr(p.get("BPNT")))]
                                  + vals + race_cols))
    return out


# ------------------------------------------------------- ECAT / CNDF / LSCR ------
def _linkcell(esm, fid):
    return "%08X:%s:%s" % (fid, esm.edid_of(fid), esm.sig_of(fid))


def exp_ecat(esm):
    """ExportECATToTSV.pas - emote categories: keywords and Referenced By, each padded."""
    rows = []
    for r in R.xedit_order(esm, "ECAT"):
        kw = r.first("KWDA") or b""
        kws = []
        for fid in struct.unpack_from("<%dI" % (len(kw) // 4), kw):
            kws.append(cc(_linkcell(esm, fid)) if _lk(esm, fid) is not None else cc(esm.link_edit_value(fid)))
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full)), cc(_zstr(r.first("SNAM")))], kws, refs))
    mk = max((len(k) for _b, k, _r in rows), default=0)
    mr = max((len(x) for _b, _k, x in rows), default=0)
    out = ["\t".join(["FormID", "EDID", "FULL", "SNAM", "KeywordCount"] + ["KW_%d" % i for i in range(1, mk + 1)]
                     + ["ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])]
    for b, k, x in rows:
        out.append("\t".join(b + [str(len(k))] + k + [""] * (mk - len(k)) + [str(len(x))] + x + [""] * (mr - len(x))))
    return out


def exp_cndf(esm):
    """ExportCNDFToTSV.pas - condition forms: their conditions and Referenced By, each padded."""
    rows = []
    for r in R.xedit_order(esm, "CNDF"):
        conds = _cond_cols(esm, r)
        rows.append((["%08X" % r.fid, cc(r.edid)], conds, _refcells(esm, r.fid)))
    mc = max((len(c) for _b, c, _r in rows), default=0)
    mr = max((len(x) for _b, _c, x in rows), default=0)
    out = ["\t".join(["FormID", "EDID", "CondCount"] + ["Cond%d" % i for i in range(1, mc + 1)]
                     + ["ReferencedByCount"] + ["Ref%d" % i for i in range(1, mr + 1)])]
    for b, c, x in rows:
        out.append("\t".join(b + [str(len(c))] + c + [""] * (mc - len(c)) + [str(len(x))] + x + [""] * (mr - len(x))))
    return out


def exp_lscr(esm):
    """ExportLSCRToTSV.pas - loading screens: text, conditions (RunOn|Function|Param1|
    Compare|Type, ## between), artwork links, and the quests that use them."""
    st = _strip_tsv
    out = ["LSCR_FormID\tLSCR_EDID\tDESC_Description\tConditionCount\tConditions\t"
           "BNAM_BackgroundImage\tNNAM_LoadingScreenNIF\tTNAM_Transform\tONAM_Rotation\t"
           "ZNAM_Zoom\tMOD2_CameraPath\tPNAM_Unknown\tLSST_LoadingScreenText\t"
           "RefBy_QUST_FormIDs\tRefBy_QUST_EDIDs"]
    for r in R.xedit_order(esm, "LSCR"):
        ctx = C.Ctx(esm, r)
        conds = []
        for ctda, c1, c2 in C.conditions(r):
            els, _ref = C.elements(ctx, ctda, c1, c2)
            d = {}
            for name, val in els:
                d.setdefault(name.split(" - ")[0], val)
            p1 = els[5][1]
            fid = struct.unpack_from("<I", ctda, 12)[0]
            if p1.endswith("[%s:%08X]" % (esm.sig_of(fid), fid)) and esm.exists(fid):
                p1 = _linkcell(esm, fid)   # the .pas prints a linked Parameter #1 as FormID:EDID:Sig
            conds.append("|".join(st(v) for v in (d["Run On"], d["Function"], p1, d["Comparison Value"], d["Type"])))
        quests = [h for h in esm.refby.get(r.fid, ()) if esm.refinfo[h][0] == "QUST"]

        def lnk(sub):
            v = r.first(sub)
            return st(_link(esm, v)) if v is not None else ""

        def txt(sub):
            v = r.first(sub)
            return st(_zstr(v)) if v is not None else ""
        pn = r.first("PNAM")
        out.append("\t".join([
            "%08X" % r.fid, st(r.edid), st(esm.lstring(r.first("DESC"))), str(len(conds)), "##".join(conds),
            txt("BNAM"), lnk("NNAM"), lnk("TNAM"), "", "", "",
            C.hexbytes(pn) if pn is not None else "", st(esm.lstring(r.first("LSST"))),
            " | ".join("%08X" % h for h in quests), " | ".join(st(esm.refinfo[h][1]) for h in quests)]))
    return out


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


# ------------------------------------------------------------- NPC_ ------
def _lk(esm, fid):
    """xEdit LinksTo: the record, or nothing when the FormID can't be resolved."""
    return fid if fid and esm.exists(fid) else None

def _hex(fid):
    return "" if fid is None else "%08X" % fid

def _triple(esm, fid):
    if fid is None:
        return ["", "", ""]
    return [_hex(fid), cc(esm.edid_of(fid)), cc(esm.full_of(fid))]

def _pair(esm, fid):
    return [_hex(fid), cc(esm.edid_of(fid)) if fid is not None else ""]

def exp_npc(esm):
    """ExportNPCToTSV.pas - main sheet, _PRPS (object properties) and _Refs (Referenced By)."""
    main = ["\t".join(["FormID", "EDID", "FULL", "SHRT",
        "RNAM_FormID", "RNAM_EDID", "RNAM_Name", "ATKR_FormID", "ATKR_EDID", "ATKR_Name",
        "INAM_FormID", "INAM_EDID", "TPLT_FormID", "TPLT_EDID", "TPLT_Name",
        "LTPT_FormID", "LTPT_EDID", "LTPT_Name", "LTPC_FormID", "LTPC_EDID", "LTPC_Name",
        "CNAM_FormID", "CNAM_EDID", "CNAM_Name", "VTCK_FormID", "VTCK_EDID", "VTCK_Name",
        "WNAM_FormID", "WNAM_EDID", "DOFT_FormID", "DOFT_EDID",
        "ACBS_Flags", "ACBS_XPOffset", "ACBS_Level", "ACBS_CalcMinLvl", "ACBS_CalcMaxLvl",
        "ACBS_DispositionBase", "ACBS_TemplateFlags", "AJNG_EDID", "AJXG_EDID",
        "DNAM_CalcHealth", "DNAM_CalcActionPts", "DNAM_FarAwayDist", "DNAM_GearedUpWeapons",
        "ZNAM_FormID", "ZNAM_EDID", "ECOR_FormID", "ECOR_EDID", "Factions_Flat"])]
    prps_out = ["NPC_FormID\tNPC_EDID\tPropIndex\tActorValue_FormID\tActorValue_EDID\tActorValue_Name\t"
                "Value\tCurveTable_FormID\tCurveTable_EDID"]
    refs_out = ["NPC_FormID\tNPC_EDID\tRefBy_FormID\tRefBy_EDID\tRefBy_Signature"]
    for r in R.xedit_order(esm, "NPC_"):
        fid = "%08X" % r.fid
        ed = cc(r.edid)
        L = lambda sub: _lk(esm, _u32(r.first(sub)))
        # ACBS - the .pas asks for 'Flags (sorted)' / 'Template Flags (sorted)', which
        # don't exist on FO76's ACBS, so those two columns are always blank.
        xp = lvl = cmin = cmax = disp = ""
        a = r.first("ACBS")
        if a and len(a) >= 16:
            flags, xp_i, lv, cmin_i, cmax_i, disp_i = struct.unpack_from("<IhHHHh", a)
            xp, cmin, cmax, disp = str(xp_i), str(cmin_i), str(cmax_i), str(disp_i)
            # Level Mult: xEdit clamps it to 0.10-10.00 on load (wbACBSLevelMultAfterLoad)
            lvl = ("%.2f" % (min(max(lv, 100), 10000) / 1000.0)) if flags & 0x80 else str(lv)
        hp = ap = far = geared = ""
        d = r.first("DNAM")
        if d and len(d) >= 7:
            h, p, f = struct.unpack_from("<HHH", d)
            hp, ap, far, geared = str(h), str(p), str(f), str(d[6])
        # Factions: SNAM, shown sorted by faction FormID (wbRArrayS)
        facs = []
        for v in r.all("SNAM"):
            if len(v) >= 5:
                facs.append((struct.unpack_from("<I", v)[0], struct.unpack_from("<b", v, 4)[0]))
        facs.sort(key=lambda x: x[0])
        flat = " | ".join("%s[Rank:%d]" % (cc(esm.edid_of(f)) if esm.exists(f) else "", rk) for f, rk in facs)
        aj = _lk(esm, _u32(r.first("AJNG")))
        ax = _lk(esm, _u32(r.first("AJXG")))
        row = [fid, ed, cc(esm.lstring(r.full)), cc(esm.lstring(r.first("SHRT")))]
        row += _triple(esm, L("RNAM")) + _triple(esm, L("ATKR")) + _pair(esm, L("INAM"))
        row += _triple(esm, L("TPLT")) + _triple(esm, L("LTPT")) + _triple(esm, L("LTPC"))
        row += _triple(esm, L("CNAM")) + _triple(esm, L("VTCK")) + _pair(esm, L("WNAM")) + _pair(esm, L("DOFT"))
        row += ["", xp, lvl, cmin, cmax, disp, "",
                cc(esm.edid_of(aj)) if aj else "", cc(esm.edid_of(ax)) if ax else "",
                hp, ap, far, geared]
        row += _pair(esm, L("ZNAM")) + _pair(esm, L("ECOR")) + [flat]
        main.append("\t".join(row))
        # PRPS: 12-byte entries (Actor Value, Value, Curve Table), sorted by Actor Value
        pr = r.first("PRPS")
        if pr:
            ents = sorted((struct.unpack_from("<IfI", pr, i * 12) for i in range(len(pr) // 12)),
                          key=lambda x: x[0])
            for i, (av, val, cv) in enumerate(ents):
                av_l, cv_l = _lk(esm, av), _lk(esm, cv)
                prps_out.append("\t".join([fid, ed, str(i), _hex(av_l),
                    cc(esm.edid_of(av_l)) if av_l else "", cc(esm.full_of(av_l)) if av_l else "",
                    C.fmt_float(val), _hex(cv_l), cc(esm.edid_of(cv_l)) if cv_l else ""]))
        refs = list(esm.refby.get(r.fid, ()))
        if r.fid == 0x7 and 0x14 not in refs:  # xEdit's own PlayerRef [PLYR:00000014] points at Player
            refs = sorted(refs + [0x14])
        for rf in refs:
            sig, red = esm.refinfo.get(rf, ("PLYR", "PlayerRef"))
            refs_out.append("\t".join([fid, ed, "%08X" % rf, cc(red), sig]))
    return {"": main, "_PRPS": prps_out, "_Refs": refs_out}


# ------------------------------------------------------------- NPC2 ------
class _World:
    """Placed refs (REFR/ACHR) for the NPC2 vendor export: cell, worldspace,
    position, rotation and the location (LCTN) each one stands in."""

    def __init__(self, esm):
        import esm_refs
        self.esm = esm
        esm.world_record(0)  # builds the world map once
        self.w = esm._world
        # xEdit RefLocation: the first LCTN (lowest FormID) in the ref's Referenced By
        rules = esm_refs._rules()
        self.loc_of = {}
        for loc in sorted(esm.records.get("LCTN", []), key=lambda r: r.fid):
            for k, v in loc.subs:
                for o in range(0, len(v) - 3):
                    f = struct.unpack_from("<I", v, o)[0]
                    if f in self.w and f not in self.loc_of and \
                            esm_refs._is_ref(rules, "LCTN", k, o, v, "W"):
                        self.loc_of[f] = loc.fid

    def sig(self, fid):
        h = self.w.get(fid)
        return h[0] if h else ""

    def rec(self, fid):
        x = self.esm.world_record(fid)
        return x[1] if x else None

    def place(self, fid):
        """(world_edid, world_fid, cell_edid, cell_fid, x, y, z, rotz, loc_fid, loc_edid, source)"""
        esm = self.esm
        sig, _off, cell, wrld = self.w[fid]
        r = self.rec(fid)
        we = wf = ce = cf = ""
        if wrld:
            wr = self.rec(wrld)
            we, wf = cc(wr.edid if wr else ""), "%08X" % wrld
        if cell:
            cr = self.rec(cell)
            ce, cf = cc(cr.edid if cr else ""), "%08X" % cell
        x = y = z = rz = ""
        d = r.first("DATA")
        if d and len(d) >= 24:
            px, py, pz, _rx, _ry, rzr = struct.unpack_from("<6f", d)
            x, y, z = C.fmt_float(px), C.fmt_float(py), C.fmt_float(pz)
            rz = _rot_deg(rzr)
        lf = le = src = ""
        if fid in self.loc_of:
            l = self.loc_of[fid]
            lf, le, src = "%08X" % l, cc(esm.edid_of(l)), "lctn"
        elif cell:
            cr = self.rec(cell)
            xl = _u32(cr.first("XLCN")) if cr else None
            if xl and esm.exists(xl):
                lf, le, src = "%08X" % xl, cc(esm.edid_of(xl)), "cell"
        return we, wf, ce, cf, x, y, z, rz, lf, le, src


def exp_npc2(esm):
    """ExportNPC2VendorsToTSV.pas - vendor NPCs, their placements, currencies,
    vendor factions and where each vendor faction's machines / NPCs stand."""
    W = _World(esm)

    def fact(fid):
        return esm.get(fid) if fid and esm.sig_of(fid) == "FACT" else None

    def venc_ref(f):
        v = _u32(f.first("VENC"))
        return v if v and v in W.w else None   # LinksTo: only when the REFR exists

    def ref_base(rf):
        if rf is None:
            return None
        n = _u32(W.rec(rf).first("NAME"))
        return n if n and esm.exists(n) else None

    def link(f, sub):
        v = _u32(f.first(sub)) if f else None
        return v if v and esm.exists(v) else None

    def hx(v):
        return "%08X" % v if v else ""

    def ed(v):
        return cc(esm.edid_of(v)) if v else ""

    def find_merchant(npc):
        facs = sorted({struct.unpack_from("<I", v)[0] for v in npc.all("SNAM") if len(v) >= 4})
        fallback = None
        for fid in facs:
            f = fact(fid)
            if f is None:
                continue
            if ref_base(venc_ref(f)):
                return f
            # the .pas also checks for a "Vendor" flag, but reads the flags' edit
            # value (a 0/1 bit string), so that test never matches
            if fallback is None and any(f.first(s) is not None for s in ("VENC", "VEND", "VENV")):
                fallback = f
        return fallback

    main = ["\t".join(["NPC_FormID", "NPC_EDID", "NPC_FULL", "NPC_SHRT", "TPLT_FormID", "TPLT_EDID",
                       "MerchantFaction_FormID", "MerchantFaction_EDID", "VendorFlags",
                       "BuySellList_FormID", "BuySellList_EDID", "BuySellList_Sig",
                       "MerchantContainerRef_FormID", "MerchantContainerBase_FormID",
                       "MerchantContainerBase_EDID", "PlacementCount"])]
    place = ["\t".join(["NPC_FormID", "NPC_EDID", "Ref_FormID", "Ref_EDID", "Worldspace_EDID",
                        "Worldspace_FormID", "Cell_EDID", "Cell_FormID", "X", "Y", "Z", "RotZ",
                        "Location_FormID", "Location_EDID", "Location_Source"])]

    def emit(nf, ne, rf):
        if rf is None or W.sig(rf) != "REFR":
            return False
        p = W.place(rf)
        place.append("\t".join([nf, ne, "%08X" % rf, cc(W.rec(rf).edid)] + list(p)))
        return True

    for r in R.xedit_order(esm, "NPC_"):
        f = find_merchant(r)
        if f is None:
            continue
        nf, ne = "%08X" % r.fid, cc(r.edid)
        tp = link(r, "TPLT")
        bs = link(f, "VEND")   # 'Vendor Buy/Sell List'
        vr = venc_ref(f)
        vb = ref_base(vr)
        placed = 0
        for rf in esm.refby.get(r.fid, ()):
            if emit(nf, ne, rf):
                placed += 1
        if emit(nf, ne, vr):
            placed += 1
        # VendorFlags: the .pas asks for 'Vendor Values\Flags', which FO76's VENV doesn't have
        main.append("\t".join([nf, ne, cc(esm.lstring(r.full)), cc(esm.lstring(r.first("SHRT"))),
                               hx(tp), ed(tp), "%08X" % f.fid, cc(f.edid), "",
                               hx(bs), ed(bs), esm.sig_of(bs) if bs else "",
                               hx(vr), hx(vb), ed(vb), str(placed)]))

    # ---- currencies
    cur = ["CNCY_FormID\tCNCY_EDID\tFULL\tPluralName\tMaxCount\tDESC\tKeywords"]
    # These two walk the record group (GroupBySignature + ElementByIndex), which
    # runs in file order - not the newest-first order Process() sees.
    for r in esm.records["CNCY"]:
        # Keywords: the .pas looks for KWDA directly on the record, but FO76 keeps it
        # inside the 'Keywords' block, so xEdit always leaves this column blank.
        mx = _u32(r.first("MXCT"))
        du = r.first("DURL")
        cur.append("\t".join(["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full)),
                              cc(du.rstrip(b"\0").decode("utf-8", "replace") if du else ""),
                              "" if mx is None else str(mx), cc(esm.lstring(r.first("DESC"))),
                              ""]))

    # ---- vendor factions + where their machines / NPCs stand
    fac = ["\t".join(["Faction_FormID", "Faction_EDID", "Faction_FULL", "Currency_FormID", "Currency_EDID",
                      "Wallet_FormID", "Wallet_EDID", "RestockDays_FormID", "RestockDays_EDID",
                      "RestockDays_Value", "BuySell_FormID", "BuySell_EDID", "ContainerRef_FormID",
                      "ContainerBase_FormID", "ContainerBase_EDID", "Worldspace_EDID", "Cell_EDID",
                      "X", "Y", "Z", "Location_FormID", "Location_EDID"])]
    facp = ["\t".join(["Faction_FormID", "Faction_EDID", "Holder_FormID", "Holder_EDID", "Holder_FULL",
                       "Holder_Sig", "Ref_FormID", "Worldspace_EDID", "Cell_EDID", "X", "Y", "Z",
                       "Location_FormID", "Location_EDID"])]

    def place_cols(rf):
        if rf is None or W.sig(rf) not in ("REFR", "ACHR"):
            return [""] * 7
        we, _wf, ce, _cf, x, y, z, _rz, lf, le, _s = W.place(rf)
        return [we, ce, x, y, z, lf, le]

    for r in esm.records["FACT"]:
        vr = venc_ref(r)
        if vr is None:
            continue
        cu, wa, rs, bs = link(r, "VBCY"), link(r, "VENG"), link(r, "VENR"), link(r, "VEND")
        vb = ref_base(vr)
        rv = ""
        if rs:
            g = esm.get(rs)
            fl = g.first("FLTV") if g else None
            rv = C.fmt_float(struct.unpack("<f", fl[:4])[0]) if fl and len(fl) >= 4 else ""
        fac.append("\t".join(["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full)),
                              hx(cu), ed(cu), hx(wa), ed(wa), hx(rs), ed(rs), rv, hx(bs), ed(bs),
                              "%08X" % vr, hx(vb), ed(vb)] + place_cols(vr)))
        for h in esm.refby.get(r.fid, ()):
            hs = esm.refinfo[h][0]
            if hs not in ("ACTI", "FURN", "NPC_"):
                continue
            head = ["%08X" % r.fid, cc(r.edid), "%08X" % h, cc(esm.refinfo[h][1]), cc(esm.full_of(h)), hs]
            for rr in esm.refby.get(h, ()):
                if W.sig(rr) in ("REFR", "ACHR"):
                    facp.append("\t".join(head + ["%08X" % rr] + place_cols(rr)))

    return {"": main, "_Placements": place, "_Currencies": cur, "_Factions": fac,
            "_FactionsPlacements": facp}


def npc2_ref_targets(esm):
    """Referenced By is needed for the NPCs, the factions, and every ACTI / FURN
    (a vending machine points at its faction through a script property)."""
    t = {r.fid for s in ("NPC_", "FACT") for r in esm.records[s]}
    t |= {f for f, v in esm.index.items() if v[0] in ("ACTI", "FURN")}
    return t


# ------------------------------------------------------ MISC2 / ACTI2 ------
# The "second pass" placement exports: every placed copy of a target set, also one
# or two hops through a container / activator / leveled list that holds it.
# Sets: (slug, test on FormID / EDID) per record type.
PLACEMENT_SETS = {
    # ExportMISC2ToCSV.pas
    "MISC": [("pint-sized-slasher-masks",
              lambda fid, ed: fid == 0x008E069E or "sdow_slasherclue" in ed.lower())],
    # ExportACTI2ToCSV.pas: the Treasure Map mounds, not the TEMP / DEBUG ones
    "ACTI": [("treasure-maps",
              lambda fid, ed: "treasuremapmoundactivator" in ed.lower()
              and "temp" not in ed.lower() and "debug" not in ed.lower())],
}
PLACEMENT_HOLDERS = {"CONT", "ACTI", "FURN", "MSTT", "SCOL", "STAT", "NPC_", "LVLI"}


def _placement_set(sig, r):
    for slug, test in PLACEMENT_SETS[sig]:
        if test(r.fid, r.edid):
            return slug
    return ""


def _placement_targets(sig):
    return lambda esm: {r.fid for r in esm.records[sig] if _placement_set(sig, r)}


misc2_ref_targets = _placement_targets("MISC")
acti2_ref_targets = _placement_targets("ACTI")


def _set_placements(esm, sig):
    """The .pas never finds the cell or worldspace (it climbs the record's parents for
    a CELL record, which a placed reference never has), so those columns, and the
    location columns built from them, are always blank - kept blank here to match."""
    import esm_refs
    out = ["set\titem_formid\tedid\tname\tsection\tworldspace\tcell\tx\ty\tz\tref_formid\tholder\tloc_name\tloc_source"]
    esm.world_record(0)
    scanned = getattr(esm, "_placement_scanned", set())
    esm._placement_scanned = scanned

    def refs_of(fid):
        if fid not in esm.refby and fid not in scanned:
            # a holder found on the way: work out its Referenced By now (rare; one extra scan)
            more, info = esm_refs.referenced_by(esm, {fid})
            esm.refby.update(more)
            esm.refinfo.update(info)
            scanned.add(fid)
        return esm.refby.get(fid, ())

    for r in R.xedit_order(esm, sig):
        slug = _placement_set(sig, r)
        if not slug:
            continue
        fid, ed, full = "%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full))
        visited = set()

        def walk(target, holder, depth):
            if depth > 2 or target in visited:
                return
            visited.add(target)
            for rf in refs_of(target):
                rs = esm.refinfo[rf][0] if rf in esm.refinfo else ""
                if rs == "REFR":
                    w = esm.world_record(rf)
                    if w and _u32(w[1].first("NAME")) == target:
                        x = y = z = ""
                        d = w[1].first("DATA")
                        if d and len(d) >= 12:
                            x, y, z = (C.fmt_float(v) for v in struct.unpack_from("<3f", d))
                        out.append("\t".join([slug, fid, ed, full, sig, "", "", x, y, z,
                                              "%08X" % rf, holder, "", ""]))
                        continue
                if depth < 2 and rs in PLACEMENT_HOLDERS:
                    walk(rf, holder or esm.refinfo[rf][1], depth + 1)

        walk(r.fid, "", 0)
    return out


def exp_misc2(esm):
    """ExportMISC2ToCSV.pas - placements of the MISC collectable sets."""
    return _set_placements(esm, "MISC")


def exp_acti2(esm):
    """ExportACTI2ToCSV.pas - placements of the ACTI sets (Treasure Map dig mounds)."""
    return _set_placements(esm, "ACTI")


# ------------------------------------------------------------- ACTI ------
XALG_FLAGS = ["Skip HAVOK on Load", "Server Authoritative", "Disable Permanent Decals",
              "Never Visible Distant", "Item Dispenser", "Item Dispenser Pickedup",
              "Fast travel restricted", "Block Item Dispenser", "Premium", "Visible Distant",
              "Camera Weapon Detectable", "Fallout 1st", "Bullion Reward Object",
              "REFR invalidates previs", "Deleted REFR invalidates previs",
              "Container weight calculation queued", "UNUSED 17", "No refresh body 3D on load"] + \
             ["Unknown %d" % i for i in range(19, 33)]

_PAS_SEPS = ("\x1f", "\x1e", "\x1d", "\x1c", "\x1b")


def _pas_clean(s):
    """CleanCell of the ACTI .pas: also blanks its own packing separators #27-#31."""
    s = (s or "").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    for ch in _PAS_SEPS:
        s = s.replace(ch, " ")
    return s.strip(" ")


def _vmad_clean(s):
    s = _pas_clean(s)
    return s.replace("##", "  ").replace("::", ":").replace("=", " ").strip(" ")


class _VmadReader:
    """Reads a VMAD's scripts and properties with their values (xEdit wbVMAD layout)."""

    def __init__(self, d):
        self.d, self.p = d, 0

    def u8(self):
        v = self.d[self.p]; self.p += 1; return v

    def u16(self):
        v = struct.unpack_from("<H", self.d, self.p)[0]; self.p += 2; return v

    def u32(self):
        v = struct.unpack_from("<I", self.d, self.p)[0]; self.p += 4; return v

    def s(self):
        n = self.u16(); v = self.d[self.p:self.p + n]; self.p += n
        return v.decode("utf-8", "replace")

    def obj(self, fmt):
        if fmt == 2:
            self.p += 2; alias = struct.unpack_from("<h", self.d, self.p)[0]; self.p += 2
            fid = self.u32()
        else:
            fid = self.u32(); alias = struct.unpack_from("<h", self.d, self.p)[0]; self.p += 4
        return ("obj", fid, alias)

    def value(self, t, fmt):
        if t == 1:
            return self.obj(fmt)
        if t == 2:
            return ("str", self.s())
        if t == 3:
            return ("int", struct.unpack_from("<i", self.d, self.p)[0], self.__skip(4))
        if t == 4:
            v = struct.unpack_from("<f", self.d, self.p)[0]; self.p += 4; return ("float", v)
        if t == 5:
            return ("bool", self.u8())
        if t == 7:
            self.struct(fmt); return ("struct",)
        if t == 11:
            return ("arr", [self.obj(fmt) for _ in range(self.u32())])
        if t == 12:
            return ("arr", [("str", self.s()) for _ in range(self.u32())])
        if t == 13:
            n = self.u32(); v = struct.unpack_from("<%di" % n, self.d, self.p); self.p += 4 * n
            return ("arr", [("int", x) for x in v])
        if t == 14:
            n = self.u32(); v = struct.unpack_from("<%df" % n, self.d, self.p); self.p += 4 * n
            return ("arr", [("float", x) for x in v])
        if t == 15:
            n = self.u32(); v = self.d[self.p:self.p + n]; self.p += n
            return ("arr", [("bool", x) for x in v])
        if t == 16:
            self.p += 4; return ("none",)
        if t == 17:
            for _ in range(self.u32()):
                self.struct(fmt)
            return ("structarr",)
        return ("none",)

    def __skip(self, n):
        self.p += n

    def struct(self, fmt):
        for _ in range(self.u32()):
            self.s(); t = self.u8(); self.u8(); self.value(t, fmt)

    def scripts(self):
        self.u16(); fmt = struct.unpack_from("<h", self.d, 2)[0]; self.p = 4
        out = []
        for _ in range(self.u16()):
            name = self.s(); self.u8()
            props = []
            for _ in range(self.u16()):
                pn = self.s(); t = self.u8(); self.u8()
                props.append((pn, self.value(t, fmt)))
            out.append((name, props))
        return out


def _xedit_text_key(s):
    """xEdit sorts script and property names on their upper-case text."""
    return s.upper()


def _dial_index(esm):
    """{fid: edid} for dialogue topics (DIAL). They sit inside the quests' child
    groups, which the shared index skips."""
    if getattr(esm, "_dial", None) is not None:
        return esm._dial
    import zlib
    out = {}
    fulls = {}   # raw FULL of each topic (esm.lstring() it) - for the KYWD _Refs names
    with open(esm.esm_path, "rb") as f:
        size = os.path.getsize(esm.esm_path)
        f.seek(24 + struct.unpack("<I", f.read(24)[4:8])[0])

        def walk(end):
            while f.tell() < end:
                h = f.read(24)
                n = struct.unpack_from("<I", h, 4)[0]
                if h[:4] == b"GRUP":
                    walk(f.tell() + n - 24)
                    continue
                if h[:4] != b"DIAL":
                    f.seek(n, 1)
                    continue
                flags, fid = struct.unpack_from("<II", h, 8)
                data = f.read(n)
                if flags & 0x40000:
                    data = zlib.decompress(data[4:])
                ed = ""
                if data[:4] == b"EDID":
                    ln = struct.unpack_from("<H", data, 4)[0]
                    ed = data[6:6 + ln].rstrip(b"\0").decode("utf-8", "replace")
                out[fid] = ed
                full = R.main_full(R._parse_subs(data))
                if full is not None:
                    fulls[fid] = full

        while f.tell() < size:
            hdr = f.read(24)
            ds = struct.unpack_from("<I", hdr, 4)[0]
            if hdr[8:12] in (b"QUST", b"DIAL"):
                walk(f.tell() + ds - 24)
            else:
                f.seek(ds - 24, 1)
    esm._dial = out
    esm._dial_full = fulls
    return out


def exp_acti(esm):
    """ExportACTIToTSV.pas - _ACTI (flags, keywords, properties, scripts) and _Refs."""
    esm.world_record(0)
    dial = _dial_index(esm)

    def link_cell(fid):
        """FormID:EDID:Sig of a record that resolves, else None."""
        if not fid:
            return None
        if fid in esm.index:
            sig, ed = esm.index[fid][:2]
            return "%08X:%s:%s" % (fid, _pas_clean(ed), sig)
        if fid in dial:
            return "%08X:%s:DIAL" % (fid, _pas_clean(dial[fid]))
        w = esm._world.get(fid)
        if w:
            rec = esm.world_record(fid)[1]
            return "%08X:%s:%s" % (fid, _pas_clean(rec.edid), w[0])
        return None

    def vval(v):
        k = v[0]
        if k == "obj":
            lc = link_cell(v[1])
            return lc if lc is not None else ""
        if k == "str":
            return _vmad_clean(v[1])
        if k == "int":
            return str(v[1])
        if k == "float":
            return C.fmt_float(v[1])
        if k == "bool":
            return "True" if v[1] else "False"
        if k == "arr":
            parts = []
            for e in v[1]:
                if e[0] == "obj":
                    lc = link_cell(e[1])
                    if lc is not None:
                        parts.append(lc)
                    continue
                x = vval(e)
                if x != "":
                    parts.append(x)
            return "|".join(parts)
        return ""

    rows, refrows = [], []
    for r in R.xedit_order(esm, "ACTI"):
        fid = "%08X" % r.fid
        ed = _pas_clean(r.edid)
        full = _pas_clean(esm.lstring(r.full))
        xa = r.first("XALG")
        xalg = ""
        if xa:
            bits = struct.unpack("<Q", xa[:8].ljust(8, b"\0"))[0]
            xalg = "|".join(XALG_FLAGS[i] for i in range(32) if bits >> i & 1)
        ai = r.first("AIID")
        aiid = "" if ai is None else ("True" if ai[0] else "False")
        kws = []
        kw = r.first("KWDA")
        if kw:
            for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw)):
                lc = link_cell(k)
                kws.append(lc if lc is not None else "%08X" % k)
        props = []
        pr = r.first("PRPS")
        if pr:
            for av, val, cv in sorted((struct.unpack_from("<IfI", pr, i * 12) for i in range(len(pr) // 12)),
                                      key=lambda x: x[0]):
                avn = _pas_clean(esm.edid_of(av)) if esm.exists(av) else ""
                # the .pas blanks 'NULL - Null Reference' / 'NULL', but xEdit's text for an
                # empty curve is 'NULL - Null Reference [00000000]', so it stays
                cvn = _pas_clean(esm.edid_of(cv)) if cv and esm.exists(cv) else _pas_clean(esm.link_edit_value(cv))
                props.append((avn, C.fmt_float(val), cvn))
        vm = r.first("VMAD")
        vcount, entries = 0, []
        if vm:
            try:
                scripts = _VmadReader(vm).scripts()
            except Exception:
                scripts = []
            scripts.sort(key=lambda s: _xedit_text_key(s[0]))
            vcount = len(scripts)
            for sn, ps in scripts:
                sn = _vmad_clean(sn)
                if not ps:
                    entries.append(sn + "::=")
                    continue
                for pn, v in sorted(ps, key=lambda p: _xedit_text_key(p[0])):
                    entries.append(sn + "::" + _vmad_clean(pn) + "=" + vval(v))
        rows.append((fid, ed, full, xalg, aiid, kws, props, vcount, "##".join(entries)))
        refs = ["%08X:%s:%s" % (x, _pas_clean(esm.refinfo[x][1]), esm.refinfo[x][0])
                for x in esm.refby.get(r.fid, ())]
        refrows.append((fid, ed, refs))

    mk = max((len(x[5]) for x in rows), default=0)
    mp = max((len(x[6]) for x in rows), default=0)
    head = ["ACTI_FormID", "ACTI_EDID", "ACTI_FULL", "XALG_Flags", "CanBeInstanced", "KeywordCount"]
    head += ["KW_%d" % i for i in range(1, mk + 1)] + ["PropCount"]
    for i in range(1, mp + 1):
        head += ["Prop_%d_AV" % i, "Prop_%d_Val" % i, "Prop_%d_Curve" % i]
    head += ["VMAD_ScriptCount", "VMAD_Scripts"]
    out = ["\t".join(head)]
    for fid, ed, full, xalg, aiid, kws, props, vc, vm in rows:
        line = [fid, ed, full, xalg, aiid, str(len(kws))] + kws + [""] * (mk - len(kws)) + [str(len(props))]
        for p in props:
            line += list(p)
        line += ["", "", ""] * (mp - len(props))
        line += [str(vc), vm]
        out.append("\t".join(line))
    mr = max((len(x[2]) for x in refrows), default=0)
    rout = ["\t".join(["ACTI_FormID", "ACTI_EDID", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])]
    for fid, ed, refs in refrows:
        rout.append("\t".join([fid, ed, str(len(refs))] + refs + [""] * (mr - len(refs))))
    return {"_ACTI": out, "_Refs": rout}


# ---------------------------------------------------------- REFRLVLI ------
def _single_same(a, b):
    """xEdit SingleSameValue: equal within single-float resolution."""
    import numpy as np
    a, b = float(np.float32(a)), float(np.float32(b))
    return abs(a - b) <= max(min(abs(a), abs(b)) * 0.0000005, 0.0000005)


def _rot_deg(rad):
    """xEdit's display of a rotation: radians folded into 0..2pi (anything within a
    hair of 0 or 2pi shows as 0), then degrees to 4 places (wbNormalizeRadians)."""
    two_pi = 2 * math.pi
    r = rad
    if abs(r / two_pi) > 100.0:
        r -= math.copysign(1, r) * two_pi * math.trunc(abs(r / two_pi) - 100.0)
    while r < 0.0:
        r += two_pi
    while r > two_pi:
        r -= two_pi
    if _single_same(r, 0.0) or _single_same(r, two_pi):
        r = 0.0
    return "%.4f" % (r * 180.0 / math.pi)


def _ref_positions(esm, fids):
    """{ref fid: DATA bytes} for many placed refs, read in one pass over the file
    (opening the ESM once per ref is far too slow over the PC's shared folder)."""
    import zlib
    esm.world_record(0)
    offs = sorted((esm._world[f][1], f) for f in fids if f in esm._world)
    out = {}
    with open(esm.esm_path, "rb") as fh:
        for off, f in offs:
            fh.seek(off)
            hdr = fh.read(24)
            ds = struct.unpack_from("<I", hdr, 4)[0]
            data = fh.read(ds)
            if struct.unpack_from("<I", hdr, 8)[0] & 0x40000:
                data = zlib.decompress(data[4:])
            for k, v in R._parse_subs(data):
                if k == "DATA":
                    out[f] = v
                    break
    return out


REFRLVLI_PART_BYTES = 40000000   # the .pas MAX_PART_BYTES: keeps each file well under GitHub's 50 MB warning


def exp_refrlvli(esm):
    """ExportREFRPlacementsToTSV.pas run over the whole Leveled Item group: every
    placed reference (REFR) of a leveled list - the LPI_ "lying in the world" spots.
    Like the .pas, the worldspace / cell / location columns stay blank (its re-anchor
    step never finds the cell). The .pas also carries over rows for lists missing
    from the run; a full run here covers every list, so nothing is carried over."""
    import esm_refs
    targets = {r.fid for r in esm.records["LVLI"]}
    refby, refinfo = esm_refs.referenced_by(esm, targets, referrers={"REFR"})
    head = ("BaseFormID\tBaseEDID\tBaseName\tRefFormID\tRefEDID\tWorldspaceEDID\tWorldspaceFormID\t"
            "CellEDID\tCellFormID\tX\tY\tZ\tRotZ\tCellName\tLocationEDID\tLocationName")
    recs = R.xedit_order(esm, "LVLI")
    pos = _ref_positions(esm, {rf for v in refby.values() for rf in v})
    rows = []
    for r in recs:
        bf, be, bn = "%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.full))
        for rf in refby.get(r.fid, ()):
            x = y = z = rz = ""
            d = pos.get(rf)
            if d and len(d) >= 24:
                px, py, pz, _a, _b, rzr = struct.unpack_from("<6f", d)
                x, y, z = C.fmt_float(px), C.fmt_float(py), C.fmt_float(pz)
                rz = _rot_deg(rzr)
            rows.append("\t".join([bf, be, bn, "%08X" % rf, cc(refinfo[rf][1]), "", "", "", "",
                                   x, y, z, rz, "", "", ""]))
    # split into parts of at most REFRLVLI_PART_BYTES, header on each (sizes as the .pas counts them)
    parts, cur, size = [], [head], len(head) + 2
    for ln in rows:
        if len(cur) > 1 and size + len(ln) + 2 > REFRLVLI_PART_BYTES:
            parts.append(cur)
            cur, size = [head], len(head) + 2
        cur.append(ln)
        size += len(ln) + 2
    parts.append(cur)
    return {("" if i == 0 else "_Part%d" % (i + 1)): p for i, p in enumerate(parts)}


# exports whose rows list "Referenced By" - one shared scan covers them all
# ------------------------------------------------------------- QUST2 _RewardQuests --
def exp_qust2_rq(esm):
    """ExportQUST2ToTSV.pas, _RewardQuests file only: every quest with a reward
    stage (QRWD) or a "(Seasonal)" name (event trackers), one row per objective,
    log entry, stage comment and reward.
    Read by src/plan_quest_guides.py (the quest steps on the plan pages)."""
    out = ["QuestFormID\tQuestEDID\tQuestName\tRowKind\tIndex\tText\tLink"]
    for r in R.xedit_order(esm, "QUST"):
        # Reward quests, plus seasonal tracker quests ("(Seasonal) The Slasher"):
        # their objectives are the event's phases (src/slasher_guide.py).
        if not (any(k == "QRWD" for k, _ in r.subs)
                or esm.lstring(r.first("FULL")).startswith("(Seasonal)")):
            continue
        head = "%08X\t%s\t%s" % (r.fid, cc(r.edid), cc(esm.lstring(r.first("FULL"))))
        stage, in_obj, objs = None, False, []
        for k, v in r.subs:
            if k == "INDX" and not in_obj:
                stage = str(struct.unpack_from("<H", v)[0])
            elif k == "QOBJ":
                in_obj = True
                obj = str(struct.unpack_from("<H", v)[0])
            elif k == "NNAM" and in_obj:
                objs.append("%s\tObjective\t%s\t%s\t" % (head, obj, cc(esm.lstring(v))))
            elif in_obj or stage is None:
                continue
            elif k == "SCFC":
                out.append("%s\tStageNote\t%s\t%s\t" % (
                    head, stage, cc(v.rstrip(b"\0").decode("utf-8", "replace"))))
            elif k == "CNAM":
                out.append("%s\tLog\t%s\t%s\t" % (head, stage, cc(esm.lstring(v))))
            elif k == "QRWD":
                g = struct.unpack("<I", v[:4])[0]
                out.append("%s\tReward\t%s\t\t%08X:%s:%s" % (
                    head, stage, g, cc(esm.edid_of(g)), esm.sig_of(g) or "GMRW"))
        out.extend(objs)
    return {"_RewardQuests": out}


# ------------------------------------------------------------- QUST _ScriptProps --
def exp_qust_sp(esm):
    """Quest script properties (VMAD): one row per property, with the record it
    points at (FormID:EDID:SIG) or its number. A GLOB property also carries the
    GLOB's value. Read by src/uwa_obtain.py: a reward a quest's own script
    hands out (Breach and Clear's Meteorite Sword: LL_MTR05_RareWeapon, rolled
    against MTR05_Breach_RareItemPercentChance) gets its list and chance from
    here instead of "not in the game files"."""
    out = ["QuestFormID\tQuestEDID\tQuestName\tScript\tProperty\tType\tValue\tGlobValue"]
    for r in R.xedit_order(esm, "QUST"):
        vm = r.first("VMAD")
        if not vm:
            continue
        try:
            scripts = _VmadReader(vm).scripts()
        except Exception:                                 # noqa: BLE001
            continue
        head = "%08X\t%s\t%s" % (r.fid, cc(r.edid), cc(esm.lstring(r.first("FULL"))))
        for sname, props in scripts:
            for pn, v in props:
                vals = v[1] if v[0] == "arr" else [v]
                for x in vals:
                    if x[0] == "obj":
                        fid = x[1]
                        if not fid:
                            continue
                        sig = esm.sig_of(fid) or ""
                        gv = ""
                        if sig == "GLOB":
                            g = esm.get(fid)
                            f = g.first("FLTV") if g else None
                            if f and len(f) >= 4:
                                gv = "%g" % struct.unpack_from("<f", f)[0]
                        out.append("%s\t%s\t%s\tobj\t%08X:%s:%s\t%s" % (
                            head, cc(sname), cc(pn), fid, cc(esm.edid_of(fid)), sig, gv))
                    elif x[0] in ("int", "float", "bool"):
                        out.append("%s\t%s\t%s\t%s\t%g\t" % (head, cc(sname), cc(pn), x[0], x[1]))
    return {"_ScriptProps": out}


# ------------------------------------------------------------- ALLYDLG _Lore --
_LORE_QUEST_RX = None


def _lore_walk(esm):
    """Quest child groups: DIAL topics (by quest), INFOs (by topic), SCENs."""
    import re as _re
    import zlib as _zlib
    dials, infos, scens = {}, {}, []
    with open(esm.esm_path, "rb") as f:
        size = os.path.getsize(esm.esm_path)

        def walk(end, gtype, gparent):
            while f.tell() < end:
                hdr = f.read(24)
                sig = hdr[:4]
                ds = struct.unpack("<I", hdr[4:8])[0]
                if sig == b"GRUP":
                    lab = struct.unpack("<I", hdr[8:12])[0]
                    gt = struct.unpack("<i", hdr[12:16])[0]
                    walk(f.tell() + ds - 24, gt, lab)
                    continue
                fl, fid = struct.unpack("<II", hdr[8:16])
                if sig not in (b"DIAL", b"INFO", b"SCEN"):
                    f.seek(ds, 1)
                    continue
                data = f.read(ds)
                if fl & 0x40000:
                    data = _zlib.decompress(data[4:])
                subs = R._parse_subs(data)
                if sig == b"DIAL":
                    q = next((struct.unpack("<I", v[:4])[0] for k, v in subs if k == "QNAM"), 0)
                    dials[fid] = q
                elif sig == b"INFO" and gtype == 7:
                    infos.setdefault(gparent, []).append((fid, subs))
                elif sig == b"SCEN":
                    q = next((struct.unpack("<I", v[:4])[0] for k, v in subs if k == "PNAM"), 0)
                    ed = next((v.rstrip(b"\0").decode("utf-8", "replace") for k, v in subs if k == "EDID"), "")
                    scens.append((fid, q, ed, subs))

        f.seek(0)
        h = f.read(24)
        f.seek(24 + struct.unpack("<I", h[4:8])[0])
        while f.tell() < size:
            g = f.read(24)
            gs = struct.unpack("<I", g[4:8])[0]
            st = f.tell() - 24
            if g[8:12] in (b"QUST", b"DIAL", b"SCEN"):
                walk(st + gs, 0, 0)
            f.seek(st + gs)
    return dials, infos, scens


def exp_lore(esm):
    """ExportCampAllyDialogToTSV.pas, _Lore file: the spoken lines of every
    radio / holotape / broadcast quest (EditorID says so) and of every scene a
    holotape NOTE plays (NOTE SNAM), in the order the scene plays them.
    Read by src/build_slasher_lore_json.py (and any future lore page)."""
    import re as _re
    rx = _re.compile(r"radio|holotape|broadcast", _re.I)
    dials, infos, scens = _lore_walk(esm)
    info_by_fid = {fid: subs for lst in infos.values() for fid, subs in lst}
    note_for_scene = {}
    for r in esm.records.get("NOTE", []):
        sn = r.first("SNAM")
        if sn and len(sn) >= 4:
            note_for_scene.setdefault(struct.unpack("<I", sn[:4])[0], r)

    def lines_of(subs, depth=0):
        txt = [esm.lstring(v) for k, v in subs if k == "NAM1"]
        txt = [t for t in txt if t]
        if txt or depth > 3:
            return txt
        d = next((v for k, v in subs if k == "DNAM"), None)
        if d and len(d) >= 4:
            j = info_by_fid.get(struct.unpack("<I", d[:4])[0])
            if j:
                return lines_of(j, depth + 1)
        return []

    def speaker(subs):
        a = next((v for k, v in subs if k == "ANAM" and len(v) == 4), None)
        if not a:
            return "", ""
        fid = struct.unpack("<I", a)[0]
        # FormID:EDID:SIG - the .pas LinkCell format, so both write the same cell
        return "%08X:%s:%s" % (fid, esm.edid_of(fid), esm.sig_of(fid)), esm.full_of(fid)

    def actions(subs):
        acts, cur = [], None
        for k, v in subs:
            if k == "ANAM" and len(v) == 2:
                cur = {}
                acts.append(cur)
            elif cur is not None and len(v) == 4:
                if k == "INAM":
                    cur["i"] = struct.unpack("<I", v)[0]
                elif k == "SNAM":
                    cur["s"] = struct.unpack("<I", v)[0]
                elif k == "DATA":
                    cur["t"] = struct.unpack("<I", v)[0]
        acts = [a for a in acts if "t" in a]
        acts.sort(key=lambda a: (a.get("s", 0), a.get("i", 0)))
        return acts

    out = ["QuestFormID\tQuestEDID\tQuestName\tSceneOrder\tSceneFormID\tSceneEDID\t"
           "HolotapeFormID\tHolotapeName\tLineOrder\tInfoFormID\tSpeaker\tSpeakerName\tText"]
    order = {}
    for sfid, q, sed, subs in scens:
        note = note_for_scene.get(sfid)
        qed = esm.edid_of(q)
        if not (note or rx.search(qed or "")):
            continue
        order[q] = order.get(q, -1) + 1
        qname = esm.full_of(q)
        n = 0
        for a in actions(subs):
            for ifid, isubs in infos.get(a["t"], []):
                sp, spn = speaker(isubs)
                for t in lines_of(isubs):
                    out.append("\t".join([
                        "%08X" % q, cc(qed), cc(qname), str(order[q]), "%08X" % sfid, cc(sed),
                        ("%08X" % note.fid) if note else "", cc(esm.lstring(note.first("FULL"))) if note else "",
                        str(n), "%08X" % ifid, cc(sp), cc(spn), cc(t)]))
                    n += 1
    return {"_Lore": out, "_LoreNotes": _lore_notes(esm)}


def _lore_notes(esm):
    """_LoreNotes file: every readable note (BOOK with DESC text, not a recipe
    or plan) with its text kept EXACTLY as the game has it - the font / colour
    / align markup and the line breaks (written as a literal \\n). The BOOK
    export flattens the line breaks, so lore pages read this one.
    Paper = the STAT the note shows when you inspect it (BOOK INAM)."""
    out = ["FormID\tEDID\tName\tPaperFormID\tPaperEDID\tModel\tText"]
    rows = []
    for r in esm.records.get("BOOK", []):
        ed = r.edid or ""
        if not ed or "recipe" in ed.lower():
            continue
        txt = esm.lstring(r.first("DESC")) if r.first("DESC") else ""
        if not txt or not txt.strip():
            continue
        inam = r.first("INAM")
        pf = struct.unpack("<I", inam[:4])[0] if inam and len(inam) >= 4 else 0
        modl = r.first("MODL")
        modl = modl.rstrip(b"\0").decode("utf-8", "replace") if modl else ""
        txt = txt.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ").replace("\n", "\\n")
        rows.append((ed.lower(), "\t".join([
            "%08X" % r.fid, ed, cc(esm.lstring(r.first("FULL")) if r.first("FULL") else ""),
            ("%08X" % pf) if pf else "", esm.edid_of(pf) if pf else "", modl, txt])))
    rows.sort()
    return out + [x for _, x in rows]


NEEDS_REFS = {"GLOB", "AVIF", "KYWD", "CURV", "CMPO", "RESO", "BPTD", "ECAT", "CNDF", "LSCR", "MISC", "NPC", "NPC2", "MISC2", "ACTI", "ACTI2"}
# exports that need Referenced By for more than their own records
REF_TARGETS = {"NPC2": npc2_ref_targets, "MISC2": misc2_ref_targets, "ACTI2": acti2_ref_targets}

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
    "KYWD": (["KYWD"], "KYWD_Export", exp_kywd),
    "CURV": (["CURV"], "CURV_Export", exp_curv),
    "CMPO": (["CMPO"], "CMPO_Export", exp_cmpo),
    "RESO": (["RESO"], "RESO_Export", exp_reso),
    "BPTD": (["BPTD"], "BPTD_Export", exp_bptd),
    "ECAT": (["ECAT"], "ECAT_Export", exp_ecat),
    "CNDF": (["CNDF"], "CNDF_Export", exp_cndf),
    "LSCR": (["LSCR"], "LSCR_Export", exp_lscr),
    "MISC": (["MISC"], "MISC_Export", exp_misc),
    "NPC": (["NPC_"], "NPC_Export", exp_npc),
    "NPC2": (["NPC_", "FACT", "CNCY", "LCTN"], "NPC2_Vendors", exp_npc2),
    "MISC2": (["MISC"], "MISC2_Export", exp_misc2),
    "ACTI": (["ACTI"], "ACTI_Export", exp_acti),
    "ACTI2": (["ACTI"], "ACTI2_Export", exp_acti2),
    "REFRLVLI": (["LVLI"], "REFRLVLI_Placements", exp_refrlvli),
    "QUST2RQ": (["QUST"], "QUST2_Export", exp_qust2_rq),
    "QUSTSP": (["QUST", "GLOB"], "QUST2_Export", exp_qust_sp),
    "LORE": (["NOTE", "BOOK"], "ALLYDLG_Export", exp_lore),
}

# PTS file names carry the date only (no time) for these, like their .pas
DATE_ONLY_PTS = {"REFRLVLI"}


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
        targets = set()
        for w in ref_exports:
            if w in REF_TARGETS:
                targets |= REF_TARGETS[w](esm)
            else:
                targets |= {r.fid for s in EXPORTS[w][0] for r in esm.records[s]}
        esm.refby, esm.refinfo = esm_refs.referenced_by(esm, targets)
        print("  Referenced By worked out for %d records in %.0fs" % (len(targets), time.time() - t1))
    for w in which:
        _sigs, prefix, fn = EXPORTS[w]
        result = fn(esm)
        # one file, or {suffix: lines} for exports that write several files
        files = result if isinstance(result, dict) else {"": result}
        base = R.out_name(prefix, channel)[:-4]
        if channel == "PTS" and w in DATE_ONLY_PTS:
            base = base[:-5]   # drop the _HHMM
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
