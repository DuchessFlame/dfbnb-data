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
    LVLI    LVLI_Export_<Month>_<Year>_LVLI_List/_Entries/_Refs/_Math.tsv  (ExportLVLIToCSV.pas;
            _Math carries EntryChanceNoneCurveTable, the per-entry LVOT rng76 reads)
    QUST2   QUST2_Export_<Month>_<Year>_Quests/_Stages/_Objectives/_Aliases/_ItemAliases
            (the other five files of ExportQUST2ToTSV.pas)
    ALLYDLG ALLYDLG_Export_<Month>_<Year>_Quests/_Topics/_Lines/_Scenes
            (ExportCampAllyDialogToTSV.pas; _Lore is LORE above)
    ALCH    ALCH_Export_<Month>_<Year>.tsv + _Effects  (ExportALCHToTSV.pas)
    ARMO    ARMO_Export_<Month>_<Year>_SLOTS/_ARMOUR/_ObjectTemplate.tsv  (ExportARMOToCSV.pas;
            keyword columns filled since 25 Oct 2026 - every older xEdit ARMO export left them empty)
    ARMO2   ARMO2_Underarmour_<Month>_<Year>.tsv + _Slots + _Mods  (ExportARMO2UnderarmourToTSV.pas,
            underarmour only; _Mods carries each installed OMOD's includes and properties)
    AVTR    AVTR_Export_<Month>_<Year>.tsv          (ExportAVTRToTSV.pas: player icons)
    BOOK    BOOK_Export_<Month>_<Year>.tsv + _Locations  (ExportBOOKToTSV.pas)
    BOOK2   BOOK2_Export_<Month>_<Year>.tsv         (ExportBOOK2ToCSV.pas: note placements)
    COBJ    COBJ_Export_<Month>_<Year>.tsv + COBJ2_Export_<Month>_<Year>.tsv  (ExportCOBJToTSV.pas;
            COBJ2 = learn method, learn chance, conditions, first condition quest)
    CONT    CONT_Export_<Month>_<Year>.tsv          (ExportCONTToTSV.pas: containers)
    ENCH    ENCH_Export_<Month>_<Year>.tsv          (ExportENCHToTSV.pas: enchantments)
    ENTM    ENTM_Export_<Month>_<Year>.tsv          (ExportENTMToTSV.pas: entitlements)
    FISH    FISH_Export_<Month>_<Year>.tsv          (!!!Wordpress - ExportFISHToTSV.pas: fish stats, rewards, sounds)
    FLST    FLST_Export_<Month>_<Year>_List / _Entries / _Refs.tsv  (!!!Wordpress - ExportFLSTToCSV.pas: form lists)
    FURN    FURN_Export_<Month>_<Year>_FURN / _Refs.tsv  (!!!Wordpress - ExportFURNToTSV.pas: furniture)
    GMRW    GMRW_Export_<Month>_<Year>.tsv          (!!!Wordpress - ExportGMRWToTSV.pas: game rewards)
    KEYM    KEYM_Export_<Month>_<Year>.tsv / _Locations  (!!!Wordpress - ExportKEYMToTSV.pas: keys)
    LGDI    LGDI_Export_<Month>_<Year>.tsv / _Mods  (!!!Wordpress - ExportLGDIToTSV.pas: legendary item lists)
    LCTN    LCTN_Export_<Month>_<Year>_LCTN / _LCSR / _Refs.tsv  (!!!Wordpress - ExportLCTNToTSV.pas: locations)
    MGEF    MGEF_Export_<Month>_<Year>.tsv          (!!!Wordpress - ExportMGEFToTSV.pas: magic effects)
    NOTE    NOTE_Export_<Month>_<Year>.tsv          (!!!Wordpress - ExportNOTEToTSV.pas: holotapes)
    PCRD    PCRD_Export_<Month>_<Year>.tsv          (!!!Wordpress - ExportPCRDToTSV.pas: perk cards)
    TERMPL  TERM_Export_<Month>_<Year>_Placements.tsv  (ExportTERMToTSV.pas _Placements;
            the main TERM file and _Quests are tools/export_term_from_esm.py)
    REFR    REFR_Placements_<Month>_<Year>.tsv  (ExportREFRPlacementsToTSV.pas per base:
            masks, graves, dig sites, Brain in a Jar, treasure-map mounds, any base
            already in a REFR_Placements file, plus --bases)

Every xEdit .pas above stays working as the manual backup.
Coordinates are correctly rounded; xEdit's own text can differ by one in the last
decimal place (X/Y/Z) and RotZ by 0.0001 - accepted, readers place by X/Y.

GLOB, AVIF, KYWD, CURV, MISC, NPC, ARMO2, AVTR, BOOK, COBJ, CONT, ENCH, ENTM, FISH, FLST, FURN, GMRW, KEYM, LGDI, LCTN, MGEF, NOTE and PCRD list every record that points at each row ("Referenced By"),
worked out by esm_refs.py in one ~40 s scan (needs numpy). Known gap: xEdit
stops reading some quests part-way through their objective/alias conditions, so
its lists leave out those quests; these lists include them. (The CURV XCRI and
KYWD gaps were closed on 25 Oct 2026: CELL XCRI and PACK PTDA are read by layout;
also QUST QOTM, EXPL DATA placed object, PERK EPFD type 9, DMGT DSES/DSEB, LVLI LVIG,
and COBJ CNAM / FVPA of any base object.)

Conditions are shown exactly as xEdit shows them (esm_conditions.py, built from
xEdit's own definitions in xedit_functions.py). Links to placed references, cells
and worldspaces trigger a one-off ~20 s scan of the world data.
Checked byte-for-byte against the xEdit TSVs of the same game build.

Run on the PC with the game installed:
    python tools\export_from_esm.py                 every export, LIVE  -> tsv\
    python tools\export_from_esm.py --pts           every export, PTS   -> tsv\pts\
    python tools\export_from_esm.py GMST EMOT       just those
Options: --data "<Data folder>"  --out "<folder>"  --no-ledger  --bases FID,FID (REFR)
Each file written adds a line to tsv\_exports.tsv, like the .pas scripts do.
"""
from __future__ import annotations

import argparse
import math
import re
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


# ------------------------------------------------------------- ALCH ------
def _flag_bits(v):
    """xEdit's edit value of an unnamed flags field: one 0/1 per bit, lowest bit first,
    up to the highest set bit ("11011" = bits 0, 1, 3, 4). No bits set: 64 zeros
    (as xEdit writes it, like LVLI's LVLF)."""
    if not v:
        return "0" * 64
    out = ""
    while v:
        out += "1" if v & 1 else "0"
        v >>= 1
    return out


def _dtrim(s):
    """Delphi Trim: strips every character up to space (control bytes too) from both ends."""
    i, j = 0, len(s)
    while i < j and s[i] <= " ":
        i += 1
    while j > i and s[j - 1] <= " ":
        j -= 1
    return s[i:j]


def exp_alch(esm):
    """ExportALCHToTSV.pas - ingestibles (main file: keywords, ENIT, Referenced By,
    keywords and refs padded) and _Effects (one row per effect, in file order).
    ENIT (33 bytes): Value i32, Flags u32, Addiction, Addiction Chance f32, Consume
    Sound, Health curve, Spoiled item, Is Canned u8, Canned base.
    EFIT (16 bytes): Effect ID u32, Magnitude f32, Area u32, Duration u32."""
    def lk(fid):
        return fid if fid and esm.exists(fid) else None

    def trip(fid):
        f = lk(fid)
        return [_hex(f), cc(esm.edid_of(f)), cc(esm.full_of(f))] if f else ["", "", ""]

    rows, eff = [], []
    for r in R.xedit_order(esm, "ALCH"):
        fid, ed = "%08X" % r.fid, cc(r.edid)
        d = r.first("DATA")
        weight = C.fmt_float(struct.unpack_from("<f", d)[0]) if d and len(d) >= 4 else ""
        kws, flat = [], []
        kw = r.first("KWDA") or b""
        for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw)):   # xEdit sorts KWDA
            if lk(k):
                kws.append(cc(_linkcell(esm, k)))
                flat.append(cc(esm.edid_of(k)) + "[%08X]" % k)
            else:
                kws.append("")
        e = r.first("ENIT")
        en = ["", ""] + [""] * 3 + [""] + [""] * 9 + [""] + [""] * 3
        value = ""
        if e and len(e) >= 33:
            v, fl, add, ch, snd, hp, sp = struct.unpack_from("<iIIfIII", e, 0)
            canned = e[28]
            cb = struct.unpack_from("<I", e, 29)[0]
            value = str(v)
            en = ([str(v), _flag_bits(fl)] + trip(add) + [C.fmt_float(ch)] + trip(snd) + trip(hp) + trip(sp)
                  + ["True" if canned else "False"] + trip(cb))
        refs = _refcells(esm, r.fid)
        base = [fid, ed, cc(esm.lstring(r.full)), cc(esm.lstring(r.first("DESC"))), weight, value,
                _dtrim(cc(esm.lstring(r.first("DNAM")))), " | ".join(flat)]
        rows.append((base, kws, en + [" | ".join(refs)], refs))
        # effects: EFID starts each one; MAGG / DURG / EIES / CODG / CODV belong to the last EFID
        effs = []
        for k, v in r.subs:
            if k == "EFID":
                effs.append({"EFID": v})
            elif effs and k in ("EFIT", "MAGG", "DURG", "EIES", "CODG", "CODV"):
                effs[-1].setdefault(k, v)
        for i, x in enumerate(effs):
            efit = x.get("EFIT")
            ei = ["", "", "", "", ""]
            if efit and len(efit) >= 16:
                a, m, ar, du = struct.unpack_from("<IfII", efit)
                ei = [str(a), C.fmt_float(m), str(ar), str(du), ""]
            codv = x.get("CODV")
            eff.append("\t".join([fid, ed, str(len(effs)), str(i)] + trip(_u32(x["EFID"])) + ei
                                 + trip(_u32(x.get("MAGG"))) + trip(_u32(x.get("DURG")))
                                 + trip(_u32(x.get("EIES"))) + trip(_u32(x.get("CODG")))
                                 + [str(struct.unpack_from("<I", codv)[0]) if codv and len(codv) >= 4 else ""]))
    mk = max((len(k) for _b, k, _e, _r in rows), default=0)
    mr = max((len(x) for _b, _k, _e, x in rows), default=0)
    head = (["ALCH_FormID", "ALCH_EDID", "FULL", "DESC", "Weight", "Value", "DNAM_AddictionName", "Keywords_Flat"]
            + ["Keyword_%d" % i for i in range(1, mk + 1)]
            + ["ENIT_Value", "ENIT_Flags", "ENIT_Addiction_FormID", "ENIT_Addiction_EDID", "ENIT_Addiction_FULL",
               "ENIT_AddictionChance", "ENIT_ConsumeSound_FormID", "ENIT_ConsumeSound_EDID", "ENIT_ConsumeSound_FULL",
               "ENIT_HealthCurve_FormID", "ENIT_HealthCurve_EDID", "ENIT_HealthCurve_FULL",
               "ENIT_SpoiledItem_FormID", "ENIT_SpoiledItem_EDID", "ENIT_SpoiledItem_FULL", "ENIT_IsCanned",
               "ENIT_CannedBase_FormID", "ENIT_CannedBase_EDID", "ENIT_CannedBase_FULL", "Refs_Flat"]
            + ["Ref_%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for b, k, en, x in rows:
        out.append("\t".join(b + k + [""] * (mk - len(k)) + en + x + [""] * (mr - len(x))))
    eh = ("ALCH_FormID\tALCH_EDID\tEffectCount\tEffectIndex\tMGEF_FormID\tMGEF_EDID\tMGEF_FULL\t"
          "EFIT_EffectID\tEFIT_Magnitude\tEFIT_Area\tEFIT_Duration\tEFIT_Unknown\t"
          "MAGG_GLOB_FormID\tMAGG_GLOB_EDID\tMAGG_GLOB_FULL\tDURG_GLOB_FormID\tDURG_GLOB_EDID\tDURG_GLOB_FULL\t"
          "EIES_FormID\tEIES_EDID\tEIES_FULL\tCODG_GLOB_FormID\tCODG_GLOB_EDID\tCODG_GLOB_FULL\tCODV_CooldownValue")
    return {"": out, "_Effects": [eh] + eff}


# ------------------------------------------------------------- ARMO ------
# BOD2 First Person Flags: bit n is biped slot 30+n (xEdit's FO76 names)
ARMO_BIPED = ["Hair Top", "Hair Long", "FaceGen Head", "BODY", "L Hand", "R Hand",
              "[U] Torso", "[U] L Arm", "[U] R Arm", "[U] L Leg", "[U] R Leg",
              "[A] Torso", "[A] L Arm", "[A] R Arm", "[A] L Leg", "[A] R Leg",
              "Headband", "Eyes", "Beard", "Mouth", "Neck", "Ring", "Scalp", "Decapitation",
              "Backpack", "EyeOfRa", "Unnamed", "Coverall", "Unnamed", "Shield", "Pipboy", "FX"]
ARMO_DAMA = ["dtEnergy", "dtFire", "dtCryo", "dtPoison", "dtRadiationExposure", "dtPhysical"]
# Keywords (_SLOTS Keywords_EDID_Flat: sorted EDIDs, | joined; _ARMOUR KeywordCount +
# KW1..KWn: FormID:EDID:SIG in FormID order). The .pas always meant to write them,
# but until 25 Oct 2026 its keyword paths never found FO76's KWDA (nested in the
# Keywords struct), so every xEdit ARMO export up to October 2026 has an empty
# Flat column, KeywordCount 0 and no KW columns. Duchess, 25 Oct 2026: fill them;
# the .pas now finds KWDA too (ResolveKWDA, as in the BOOK .pas). False = the old output.
ARMO_KEYWORDS = True


def _ccell(s):
    """The .pas CleanCell: CR/LF/tab to spaces, then Delphi Trim."""
    return _dtrim(cc(s))


def _obt_combos(r):
    """Object Template (OBTE ... STOP): [(OBTF, FULL raw, OBTS raw)] in file order, and the OBTE count."""
    # xEdit fills a combination until a member repeats: an OBTS with no OBTF / FULL
    # before it takes the OBTF / FULL that follow it (00425D51: Mk2 lands on combination 0).
    combos, obte, inside = [], None, False
    for k, v in r.subs:
        if k == "OBTE":
            inside, obte = True, struct.unpack_from("<I", v)[0] if len(v) >= 4 else 0
        elif not inside:
            continue
        elif k == "STOP":
            inside = False
        elif k in ("OBTF", "FULL", "OBTS"):
            if not combos or k in combos[-1]:
                combos.append({})
            combos[-1][k] = v
    return obte, [(c.get("OBTF"), c.get("FULL"), c.get("OBTS")) for c in combos]


def _obts(v):
    """OBTS: Include Count u32, Property Count u32, Level Min u8, -, Level Max u8, -, ID s16,
    Default bool, Keywords (u8 count + FormIDs), Min Level For Ranks u8, Alt Levels Per Tier u8,
    Includes (Mod FormID, Attach Point Index u8, Optional bool, Don't Use All bool), Properties."""
    inc, prop = struct.unpack_from("<II", v)
    lmin, lmax, dflt, nkw = v[8], v[10], v[14], v[15]
    p = 16 + nkw * 4 + 2
    incs = []
    for i in range(inc):
        if p + 7 > len(v):
            break
        incs.append((struct.unpack_from("<I", v, p)[0], v[p + 4], v[p + 5], v[p + 6]))
        p += 7
    return inc, prop, lmin, lmax, dflt, incs


def _xbool(b):
    return "True" if b else "False"


def exp_armo(esm):
    """ExportARMOToCSV.pas - armour: _SLOTS (biped slots + sorted keyword EDIDs), _ARMOUR
    (DATA, DAMA resistances, attach slots, durability curves, ARMA models; dynamic columns
    padded to the widest record) and _ObjectTemplate (one row per OMOD include)."""
    def edid_or_text(fid):
        return esm.edid_of(fid) if _lk(esm, fid) else _ccell(esm.link_edit_value(fid))

    slots = ["ARMO_FormID\tARMO_EDID\tARMO_FULL\tBOD2_FirstPersonFlags\tBOD2_FirstPersonFlagLabels\tKeywords_EDID_Flat"]
    ot = ["\t".join(["ARMO_FormID", "ARMO_EDID", "ARMO_FULL", "OBTE_Count", "CombinationIndex", "OBTF_EditorOnly",
                     "Combination_FULL", "OBTS_IncludeCount", "OBTS_PropertyCount", "OBTS_LevelMin", "OBTS_LevelMax",
                     "OBTS_Default", "IncludeIndex", "Include_Mod", "Include_AttachPointIndex", "Include_Optional",
                     "Include_DontUseAll"])]
    rows = []
    for r in R.xedit_order(esm, "ARMO"):
        fid, ed, full = "%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full))
        # --- _SLOTS
        b = r.first("BOD2")
        bits = struct.unpack_from("<I", b)[0] if b and len(b) >= 4 else 0
        on = [i for i in range(32) if bits >> i & 1]
        kwda = r.first("KWDA") or b""
        slots.append("\t".join([fid, ed, full, "|".join(str(30 + i) for i in on),
                                "|".join(ARMO_BIPED[i] for i in on),
                                _kw_sorted(esm, kwda) if ARMO_KEYWORDS else ""]))
        # --- _ARMOUR
        kws = []
        if ARMO_KEYWORDS:
            for k in sorted(struct.unpack_from("<%dI" % (len(kwda) // 4), kwda)):   # xEdit sorts KWDA
                kws.append(_linkcell(esm, k) if _lk(esm, k) else _ccell(esm.link_edit_value(k)))
        d = r.first("DATA")
        data = ["", "", ""]
        if d and len(d) >= 12:
            v, w, h = struct.unpack_from("<ifI", d)
            data = [str(v), C.fmt_float(w), str(h)]
        dama = r.first("DAMA") or b""
        ents = [struct.unpack_from("<III", dama, i) for i in range(0, len(dama) - 11, 12)]
        res = []
        for dt in ARMO_DAMA:
            hit = next((e for e in ents if _lk(esm, e[0]) and esm.edid_of(e[0]).lower() == dt.lower()), None)
            res += [str(hit[1]), edid_or_text(hit[2])] if hit else ["", ""]
        appr = r.first("APPR") or b""
        apprs = []
        for k in sorted(struct.unpack_from("<%dI" % (len(appr) // 4), appr)):   # xEdit sorts APPR by FormID
            apprs.append(esm.edid_of(k) + ":" + _ccell(esm.full_of(k)) if _lk(esm, k)
                         else _ccell(esm.link_edit_value(k)))
        cvt = []
        for s in ("CVT1", "CVT2", "CVT3"):
            v = r.first(s)
            cvt.append(edid_or_text(struct.unpack_from("<I", v)[0]) if v and len(v) >= 4 else "")
        mods = []   # Models: INDX + MODL per entry, file order
        for k, v in r.subs:
            if k == "INDX":
                mods.append("")
            elif k == "MODL":
                m = struct.unpack_from("<I", v)[0]
                cell = "%s:%08X" % (esm.edid_of(m), m) if _lk(esm, m) else _ccell(esm.link_edit_value(m))
                if mods and mods[-1] == "":
                    mods[-1] = cell
                else:
                    mods.append(cell)
        kwn = len(kws)
        rows.append(([fid, ed, full, str(kwn)], kws, data + res + [str(len(apprs))], apprs, cvt + [str(len(mods))],
                     [x.replace("\t", " ") for x in mods]))
        # --- _ObjectTemplate
        obte, combos = _obt_combos(r)
        for ci, (obtf, cfull, obts) in enumerate(combos):
            if obts is None:
                continue
            inc, prop, lmin, lmax, dflt, incs = _obts(obts)
            for ii, (m, api, opt, dua) in enumerate(incs):
                ot.append("\t".join([fid, ed, full, str(obte), str(ci), "", esm.lstring(cfull) if cfull else "",
                                     str(inc), str(prop), str(lmin), str(lmax), _xbool(dflt), str(ii),
                                     esm.link_edit_value(m), str(api), _xbool(opt), _xbool(dua)]))
    mk = max((len(x[1]) for x in rows), default=0)
    ma = max((len(x[3]) for x in rows), default=0)
    mm = max((len(x[5]) for x in rows), default=0)
    head = (["ARMO_FormID", "ARMO_EDID", "ARMO_FULL", "KeywordCount"] + ["KW%d" % i for i in range(1, mk + 1)]
            + ["DATA_Value", "DATA_Weight", "DATA_Health"]
            + [x for n in ("Energy", "Fire", "Cryo", "Poison", "Rad", "Physical")
               for x in ("DAMA_%s_Amount" % n, "DAMA_%s_Curve" % n)]
            + ["APPR_SlotCount"] + ["APPR_%d" % i for i in range(1, ma + 1)]
            + ["CVT1_DurabilityMin", "CVT2_CondDmgScaleFactor", "CVT3_DurabilityMax", "ModelCount"]
            + ["Model%d" % i for i in range(1, mm + 1)])
    armour = ["\t".join(head)]
    for a, k, b, ap, c, m in rows:
        armour.append("\t".join(a + k + [""] * (mk - len(k)) + b + ap + [""] * (ma - len(ap)) + c + m
                                + [""] * (mm - len(m))))
    return {"_SLOTS": slots, "_ARMOUR": armour, "_ObjectTemplate": ot}


# ------------------------------------------------------------ ARMO2 ------
ARMO2_MAX_REFS, ARMO2_MAX_SLOT = 30, 12
# OMOD property names (xEdit FO76, Form Type ARMO). Only these two are in today's
# underarmour mods; any other id prints a warning so its xEdit name can be added.
OMOD_ARMO_PROPS = {3: "Keywords", 10: "Actor Values"}


def _q2(s):
    """The ARMO2 .pas Q(): quotes doubled, tab / CR / LF to spaces."""
    return (s or "").replace('"', '""').replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _armo2_kw_list(esm, raw, only_ma=False):
    """BuildKeywordList / BuildModTargetKWDs: EDID[FORMID], ' | ' joined, in xEdit's FormID order."""
    out = []
    for k in sorted(struct.unpack_from("<%dI" % (len(raw) // 4), raw)):
        if _lk(esm, k):
            if only_ma and not esm.edid_of(k).lower().startswith("ma_"):
                continue
            out.append("%s[%08X]" % (esm.edid_of(k), k))
        elif not only_ma:
            out.append(esm.link_edit_value(k))
    return " | ".join(out)


def _omod_data(v):
    """FO76 OMOD DATA: Include Count u32, Property Count u32, 2 x u8, Form Type (4 chars),
    2 x u8, Attach Point FormID, Attach Parent Slots (u32 count + FormIDs), Items (u32
    count + FormIDs), Includes (7 bytes each), Properties (24 bytes each)."""
    inc, prop = struct.unpack_from("<II", v)
    ap = struct.unpack_from("<I", v, 16)[0]
    p = 20
    for _ in range(2):
        n = struct.unpack_from("<I", v, p)[0]
        p += 4 + 4 * n
    incs = []
    for _ in range(inc):
        incs.append(struct.unpack_from("<I", v, p)[0])
        p += 7
    props = []
    for _ in range(prop):
        props.append(struct.unpack_from("<BxxxBxxxHxxIII", v, p))
        p += 24
    return ap, incs, props


_ARMO2_WARNED = set()


def _omod_prop(esm, vt, _fn, pid, v1, v2, ct):
    """_PropOneLine: Property=Value1[:Value2][@Curve Table]."""
    name = OMOD_ARMO_PROPS.get(pid)
    if name is None:
        name = "Property#%d" % pid
        if pid not in _ARMO2_WARNED:
            _ARMO2_WARNED.add(pid)
            print("  ARMO2: OMOD property id %d has no xEdit name yet - add it to OMOD_ARMO_PROPS" % pid)
    f2 = struct.unpack("<f", struct.pack("<I", v2))[0]
    if vt in (4, 6):        # FormID,Int / FormID,Float
        s1 = ("%s[%s:%08X]" % (esm.edid_of(v1), esm.sig_of(v1), v1)) if _lk(esm, v1) else esm.link_edit_value(v1)
        s2 = str(v2) if vt == 4 else C.fmt_float(f2)
    else:
        if ("vt", vt) not in _ARMO2_WARNED:
            _ARMO2_WARNED.add(("vt", vt))
            print("  ARMO2: OMOD value type %d not checked against xEdit yet" % vt)
        s1 = C.fmt_float(struct.unpack("<f", struct.pack("<I", v1))[0]) if vt == 1 else str(v1)
        s2 = ""
    out = name + "=" + s1
    if s2:
        out += ":" + s2
    return out + "@" + (esm.edid_of(ct) if _lk(esm, ct) else esm.link_edit_value(ct))


def _armo2_slots(esm, r):
    appr = r.first("APPR") or b""
    slots = []
    for k in sorted(struct.unpack_from("<%dI" % (len(appr) // 4), appr))[:ARMO2_MAX_SLOT]:
        if _lk(esm, k):
            slots.append(("%08X" % k, esm.edid_of(k), esm.full_of(k)))
        else:
            slots.append(("", esm.link_edit_value(k), ""))
    return slots


def _is_underarmour(r, slots):
    flat = " | ".join(e + ":" + n for _f, e, n in slots).lower()
    ed = r.edid.lower()
    return "underarmor_style" in flat or "underarmor" in ed or "under_armor" in ed


def armo2_ref_targets(esm):
    return {r.fid for r in esm.records["ARMO"] if _is_underarmour(r, _armo2_slots(esm, r))}


def exp_armo2(esm):
    """ExportARMO2UnderarmourToTSV.pas (underarmour only, the default answer to its
    second prompt): garments (keywords, ma_ mod-target keywords, slots, Referenced By
    capped at 30 columns), _Slots (one row per attach slot) and _Mods (one row per
    OMOD a combination installs, with that mod's includes and properties)."""
    omods = {r.fid: r for r in esm.records["OMOD"]}
    main = ["\t".join(["ARMO_FormID", "ARMO_EDID", "ARMO_FULL", "DATA_Value", "DATA_Weight", "DATA_Health",
                       "DAMA_Flat", "KWDA_Count", "KWDA_Flat", "ModTarget_KWDs", "SlotCount", "Slots_Flat",
                       "ComboCount", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, ARMO2_MAX_REFS + 1)])]
    slot_rows = ["ARMO_FormID\tARMO_EDID\tSlotIndex\tAttachPoint_FormID\tAttachPoint_EDID\tAttachPoint_Name"]
    mods = ["\t".join(["ARMO_FormID", "ARMO_EDID", "ComboIndex", "Combo_FULL", "Combo_Default", "IncludeIndex",
                       "OMOD_FormID", "OMOD_EDID", "OMOD_FULL", "OMOD_AttachPoint_EDID", "OMOD_MNAM_Flat",
                       "OMOD_IncludeCount", "OMOD_Includes_Flat", "OMOD_PropertyCount", "OMOD_Props_Flat"])]
    for r in R.xedit_order(esm, "ARMO"):
        slots = _armo2_slots(esm, r)
        if not _is_underarmour(r, slots):
            continue
        fid, ed = "%08X" % r.fid, r.edid
        for i, (sf, se, sn) in enumerate(slots):
            slot_rows.append("\t".join([fid, ed, str(i), sf, se, _q2(sn)]))
        n_mod = 0
        _obte, combos = _obt_combos(r)
        for ci, (_obtf, cfull, obts) in enumerate(combos):
            if obts is None:
                continue
            _inc, _prop, _lmin, _lmax, dflt, incs = _obts(obts)
            for ii, (m, _api, _opt, _dua) in enumerate(incs):
                om = omods.get(m)
                cells = ["", "", "", "", "", "0", "", "0", ""]
                if om is not None:
                    ap, oincs, props = _omod_data(om.first("DATA") or b"\0" * 28)
                    inc_flat = " | ".join(("%s[OMOD:%08X]" % (esm.edid_of(x), x)) if _lk(esm, x)
                                          else esm.link_edit_value(x) for x in oincs)
                    mn = om.first("MNAM")
                    cells = ["%08X" % m, om.edid, _q2(esm.lstring(om.full)), _q2(esm.link_edit_value(ap)),
                             _q2(_armo2_kw_list(esm, mn)) if mn is not None else "", str(len(oincs)),
                             _q2(inc_flat), str(len(props)),
                             _q2(" | ".join(_omod_prop(esm, *pr) for pr in props))]
                mods.append("\t".join([fid, ed, str(ci), _q2(esm.lstring(cfull) if cfull else ""),
                                       _xbool(dflt), str(ii)] + cells))
                n_mod += 1
        d = r.first("DATA")
        data = ["", "", ""]
        if d and len(d) >= 12:
            v, w, h = struct.unpack_from("<ifI", d)
            data = [str(v), C.fmt_float(w), str(h)]
        kwda = r.first("KWDA")
        refs = _refcells(esm, r.fid)
        main.append("\t".join([fid, ed, _q2(esm.lstring(r.full))] + data + [
            "",   # DAMA_Flat: no underarmour has DAMA (checked 25 Oct 2026, live + PTS)
            str(len(kwda) // 4) if kwda else "0",
            _q2(_armo2_kw_list(esm, kwda)) if kwda else "",
            _q2(_armo2_kw_list(esm, kwda, only_ma=True)) if kwda else "",
            str(len(slots)), _q2(" | ".join(e + ":" + n for _f, e, n in slots)),
            str(n_mod), str(len(refs))] + refs[:ARMO2_MAX_REFS] + [""] * (ARMO2_MAX_REFS - len(refs[:ARMO2_MAX_REFS]))))
        if r.first("DAMA"):
            print("  ARMO2: %s has DAMA - DAMA_Flat is not written yet, add it" % ed)
    return {"": main, "_Slots": slot_rows, "_Mods": mods}


# ------------------------------------------------------------- AVTR ------
AVTR_MAX_REFS = 64


def exp_avtr(esm):
    """ExportAVTRToTSV.pas - player icons: XALG flags, icon texture (SWFI), the
    entitlement that unlocks it (RENT; blank = unlocked by default) and Referenced By
    (| joined, capped at 64 with a "...+N more" tail)."""
    out = ["FormID\tEDID\tFULL\tXALG_Flags\tSWFI\tRENT_FormID\tRENT_EDID\tRENT_FULL\tReferencedBy"]
    for r in R.xedit_order(esm, "AVTR"):
        xa = r.first("XALG")
        xalg = ""
        if xa:
            bits = struct.unpack("<Q", xa[:8].ljust(8, b"\0"))[0]
            xalg = "|".join(XALG_FLAGS[i] for i in range(32) if bits >> i & 1)
        rent = ["", "", ""]
        rv = r.first("RENT")
        if rv and len(rv) >= 4:
            e = struct.unpack_from("<I", rv)[0]
            if _lk(esm, e):
                rent = ["%08X" % e, _ccell(esm.edid_of(e)), _ccell(esm.full_of(e))]
        refs = _refcells(esm, r.fid)
        ref = "|".join(refs[:AVTR_MAX_REFS])
        if len(refs) > AVTR_MAX_REFS:
            ref += "|...+%d more" % (len(refs) - AVTR_MAX_REFS)
        out.append("\t".join(["%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full)), xalg,
                              _ccell(_zstr(r.first("SWFI"))) if r.first("SWFI") is not None else ""]
                             + rent + [ref]))
    return out


# ------------------------------------------------------------- BOOK ------
def _clean_loc(s):
    """The BOOK .pas CleanLocName: CleanCell, ':' to spaces, Trim."""
    return _dtrim(_ccell(s).replace(":", " "))


def _is_test_cell(s):
    u = s.upper()
    return "TEST" in u or "DEBUG" in u or "QASMOKE" in u or u == "QA"


def _book_loc_from_name(s):
    """LocViaNameParse on a placed reference's xEdit Name() (= its link text):
    after the first ' in ', an exterior cell gives its EDID token (ExtCell), an
    interior cell the first quoted name (NameParse)."""
    p = s.find(" in ")
    if p < 0:
        return "", ""
    after = s[p + 4:]
    q, cb = after.find('"'), after.find("[CELL:")
    if cb >= 0 and (q < 0 or cb < q):
        sp = after.find(" ")
        if sp > 0:
            tok = after[:sp]
        elif cb > 0:
            tok = after[:cb - 1]
        else:
            tok = ""
        tok = _dtrim(tok)
        return (tok, "ExtCell") if tok else ("", "")
    if q >= 0:
        j = after.find('"', q + 1)
        if j >= 0:
            tok = _clean_loc(after[q + 1:j])
            if tok:
                return tok, "NameParse"
    return "", ""


def _placed_location(esm, fid):
    """The BOOK / KEYM .pas _Locations columns (LocationName, LocationSource, QuestName):
    placed copies (REFR) in Referenced By order, test cells only as a last resort, and
    the first quest that points at the record (its name, else its EDID)."""
    name = src = fb_name = fb_src = ""
    quest = None
    for h in esm.refby.get(fid, ()):
        sig = esm.refinfo[h][0]
        if sig == "QUST" and quest is None:
            qf = _dtrim(esm.full_of(h))
            quest = _clean_loc(qf) if qf else _clean_loc(esm.edid_of(h))
            if not quest:
                quest = None
        if sig != "REFR" or name:
            continue
        n, s = _book_loc_from_name(esm.link_edit_value(h))
        if not n:
            continue
        if not _is_test_cell(n):
            name, src = n, s
        elif not fb_name:
            fb_name, fb_src = n, s
    if not name:
        name, src = fb_name, fb_src
    return name, src, quest or ""


def exp_book(esm):
    """ExportBOOKToTSV.pas - books / notes / plans (main file: keywords and Referenced
    By, each padded) and _Locations (where a placed copy lies, and the first quest
    that points at it). The .pas's container walk never reaches a CELL (a REFR's
    container is a GRUP), so every location comes from the name parse, as here."""
    def ev(sub):
        v = r.first(sub)
        return _ccell(esm.link_edit_value(struct.unpack_from("<I", v)[0])) if v and len(v) >= 4 else ""

    rows = []
    locs = ["BOOK_FormID\tBOOK_EDID\tLocationName\tLocationSource\tQuestName"]
    for r in R.xedit_order(esm, "BOOK"):
        fid, ed = "%08X" % r.fid, _ccell(r.edid)
        kw = r.first("KWDA") or b""
        kws = [("%08X:%s:%s" % (k, esm.edid_of(k), esm.sig_of(k))) if _lk(esm, k) else _ccell(esm.link_edit_value(k))
               for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]   # xEdit sorts KWDA
        d = r.first("DATA")
        value = weight = ""
        if d and len(d) >= 8:
            v, w = struct.unpack_from("<if", d)
            value, weight = str(v), C.fmt_float(w)
        dn, bt = r.first("DNAM"), r.first("BTOF")
        refs = _refcells(esm, r.fid)
        rows.append(([fid, ed, _ccell(esm.lstring(r.full)), _ccell(esm.lstring(r.first("DESC"))), str(len(kws))], kws,
                     [ev("PTRN"), _ccell(_zstr(r.first("MODL"))) if r.first("MODL") is not None else "", ev("VCRY"),
                      value, weight, _flag_bits(dn[0]) if dn else "", ev("INAM"),
                      _flag_bits(struct.unpack_from("<I", bt)[0]) if bt and len(bt) >= 4 else "", ev("BVGO"),
                      str(len(refs))], refs))
        locs.append("\t".join([fid, ed] + list(_placed_location(esm, r.fid))))
    mk = max((len(x[1]) for x in rows), default=0)
    mr = max((len(x[3]) for x in rows), default=0)
    head = (["FormID", "EDID", "FULL", "DESC", "KeywordCount"] + ["KW%d" % i for i in range(1, mk + 1)]
            + ["PTRN", "Model", "VCRY", "DATA_Value", "DATA_Weight", "DNAM_Flags", "INAM", "BTOF", "BVGO",
               "ReferencedByCount"] + ["Ref%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for a, k, b, x in rows:
        out.append("\t".join(a + k + [""] * (mk - len(k)) + b + x + [""] * (mr - len(x))))
    return {"": out, "_Locations": locs}


# ------------------------------------------------------------- COBJ ------
COBJ_LEARN = ["Learned when picked up or by script", "Learned by scrapping", "Learned when ingested",
              "Known by default or when conditions are met", "Learned from plan", "Learned from challenge"]


def exp_cobj(esm):
    """ExportCOBJToTSV.pas - recipes (main file: created object, workbench keyword,
    recipe book, FNAM keywords, FVPA Component:Count:CurveTable, Referenced By flat +
    padded) and COBJ2 (learn method, learn chance, conditions, first quest a
    condition points at). COBJ2 is a sibling prefix so COBJ_Export_* globs skip it."""
    import esm_refs

    def link3(sub):
        v = r.first(sub)
        f = _u32(v) if v else None
        if not _lk(esm, f):
            return ["", "", ""]
        return ["%08X" % f, esm.edid_of(f), _ccell(esm.full_of(f))]

    rows, cond = [], ["\t".join(["COBJ_FormID", "COBJ_EDID", "CNAM_FormID", "CNAM_EDID", "LRNM_LearnMethod",
                                 "LRNC_LearnChance", "CondCount", "Conditions", "Quest_FormID", "Quest_EDID",
                                 "Quest_FULL"])]
    for r in R.xedit_order(esm, "COBJ"):
        fid, ed = "%08X" % r.fid, _ccell(r.edid)
        cn = link3("CNAM")
        fn = r.first("FNAM") or b""
        # xEdit sorts FNAM and FVPA by FormID (component FormID for FVPA)
        kws = [_ccell(esm.edid_of(k)) for k in sorted(struct.unpack_from("<%dI" % (len(fn) // 4), fn))
               if _lk(esm, k)]
        fv = r.first("FVPA") or b""
        comps = []
        for c, n, cu in sorted((struct.unpack_from("<III", fv, o) for o in range(0, len(fv) - 11, 12)),
                               key=lambda t: t[0]):
            name = _ccell(esm.edid_of(c)) if _lk(esm, c) else _ccell(esm.link_edit_value(c))
            if not name:
                continue
            comps.append("%s:%d:%s" % (name, n, _ccell(esm.edid_of(cu)) if _lk(esm, cu) else ""))
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        rows.append(([fid, ed] + cn + link3("BNAM") + link3("GNAM")
                     + ["|".join(kws), "|".join(comps), "|".join(refs), str(len(refs))], refs))
        # ---- COBJ2
        lv = r.first("LRNM")
        lrnm = (COBJ_LEARN[lv[0]] if lv[0] < len(COBJ_LEARN) else str(lv[0])) if lv else ""
        lc = r.first("LRNC")
        lrnc = ""
        if lc and len(lc) >= 4:
            g = _u32(lc)
            lrnc = _ccell(_linkcell(esm, g)) if _lk(esm, g) else _ccell(esm.link_edit_value(g))
        ctx = C.Ctx(esm, r)
        conds, quest = [], None
        for ctda, c1, c2 in C.conditions(r):
            one = _ccell(C.pas_cond(ctx, ctda, c1, c2, clean=_ccell))
            if one:
                conds.append(one)
            if quest is None:
                for o in sorted(esm_refs._ctda_offsets(ctda)):
                    q = struct.unpack_from("<I", ctda, o)[0]
                    if _lk(esm, q) and esm.sig_of(q) == "QUST":
                        quest = q
                        break
        qc = ["%08X" % quest, esm.edid_of(quest), _ccell(esm.full_of(quest))] if quest else ["", "", ""]
        cond.append("\t".join([fid, ed, cn[0], cn[1], lrnm, lrnc, str(len(conds)), " ; ".join(conds)] + qc))
    mr = max((len(x) for _b, x in rows), default=0)
    out = ["\t".join(["COBJ_FormID", "COBJ_EDID", "CNAM_FormID", "CNAM_EDID", "CNAM_FULL", "BNAM_FormID",
                      "BNAM_EDID", "BNAM_FULL", "GNAM_FormID", "GNAM_EDID", "GNAM_FULL", "FNAM_Keywords", "FVPA",
                      "ReferencedBy_Flat", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])]
    for b, x in rows:
        out.append("\t".join(b + x + [""] * (mr - len(x))))
    return {"COBJ": out, "COBJ2": cond}


# ------------------------------------------------------------- CONT ------
def exp_cont(esm):
    """ExportCONTToTSV.pas - containers: harvest global (HGLB), keywords, PRPS
    properties (AV | value | curve, three columns each) and Referenced By, padded."""
    def ed(f):
        return _ccell(esm.edid_of(f))

    def lc(f):
        return "%08X:%s:%s" % (f, ed(f), esm.sig_of(f))

    rows = []
    for r in R.xedit_order(esm, "CONT"):
        hg = r.first("HGLB")
        hglb = ""
        if hg and len(hg) >= 4:
            g = _u32(hg)
            hglb = ed(g) if _lk(esm, g) else _ccell(esm.link_edit_value(g))
        kw = r.first("KWDA") or b""
        kws = [lc(k) if _lk(esm, k) else _ccell(esm.link_edit_value(k))
               for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]   # xEdit sorts KWDA
        pr = r.first("PRPS") or b""
        props = []
        for av, val, cv in sorted((struct.unpack_from("<IfI", pr, i * 12) for i in range(len(pr) // 12)),
                                  key=lambda x: x[0]):   # PRPS sorted by Actor Value, as in NPC
            a = ed(av) if _lk(esm, av) else _ccell(esm.link_edit_value(av))
            c = ed(cv) if _lk(esm, cv) else _ccell(esm.link_edit_value(cv))
            props.append([a, C.fmt_float(val), c])
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        rows.append(([("%08X" % r.fid), _ccell(r.edid), _ccell(esm.lstring(r.full)), hglb, str(len(kws))],
                     kws, props, refs))
    mk = max((len(x[1]) for x in rows), default=0)
    mp = max((len(x[2]) for x in rows), default=0)
    mr = max((len(x[3]) for x in rows), default=0)
    head = (["FormID", "EDID", "FULL", "HGLB", "KeywordCount"] + ["KW%d" % i for i in range(1, mk + 1)]
            + ["PropertyCount"] + [c % i for i in range(1, mp + 1)
                                   for c in ("Prop_%d_AV", "Prop_%d_Val", "Prop_%d_Curve")]
            + ["ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for b, k, p, x in rows:
        out.append("\t".join(b + k + [""] * (mk - len(k)) + [str(len(p))] + [v for t in p for v in t]
                             + [""] * (3 * (mp - len(p))) + [str(len(x))] + x + [""] * (mr - len(x))))
    return out


# ------------------------------------------------------------- FISH ------
FISH_FLOATS = ("FISP", "FIDS", "FIES")
FISH_BYTES = ("FIHS", "FILS")
FISH_FLOATS2 = ("FIHD", "FILD", "FIJS", "FIHJ", "FILJ", "FILA", "FIHA")
FISH_LINKS = ("FIRI", "FIEX", "FISD", "FISU")


def exp_fish(esm):
    """!!!Wordpress - ExportFISHToTSV.pas - fishing: keywords (FormID:EDID:SIG, FormID
    order as xEdit sorts KWDA), the fish stats, the reward / splash / sound links as
    xEdit edit values (FIRI keeps 'EDID "Name" [SIG:FormID]'; the axolotl build reads
    the quoted name) and Referenced By, each padded to the widest row."""
    def num(r, s, fmt):
        d = r.first(s)
        if not d:
            return ""
        return str(d[0]) if fmt == "b" else C.fmt_float(struct.unpack_from("<f", d)[0])

    def link(r, s):
        d = r.first(s)
        return _ccell(esm.link_edit_value(_u32(d))) if d and len(d) >= 4 else ""

    rows = []
    for r in R.xedit_order(esm, "FISH"):
        kw = r.first("KWDA") or b""
        kws = [_ccell(_linkcell(esm, k)) if _lk(esm, k) else _ccell(esm.link_edit_value(k))
               for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]
        vals = ([num(r, s, "f") for s in FISH_FLOATS] + [num(r, s, "b") for s in FISH_BYTES]
                + [num(r, s, "f") for s in FISH_FLOATS2] + [link(r, s) for s in FISH_LINKS])
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        rows.append((["%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full)), str(len(kws))], kws, vals, refs))
    mk = max((len(x[1]) for x in rows), default=0)
    mr = max((len(x[3]) for x in rows), default=0)
    head = (["FormID", "EDID", "FULL", "KeywordCount"] + ["KW%d" % i for i in range(1, mk + 1)]
            + list(FISH_FLOATS + FISH_BYTES + FISH_FLOATS2 + FISH_LINKS)
            + ["ReferencedByCount"] + ["Ref%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for b, k, v, x in rows:
        out.append("\t".join(b + k + [""] * (mk - len(k)) + v + [str(len(x))] + x + [""] * (mr - len(x))))
    return out


# ------------------------------------------------------------- FLST ------
def _flst_target(esm, fid):
    """(sig, EDID, FULL) of a form-list entry as xEdit's LinksTo sees it, or None
    when the FormID resolves to nothing (xEdit then writes empty cells)."""
    if not fid:
        return None
    if esm.exists(fid):
        return esm.sig_of(fid), esm.edid_of(fid), esm.full_of(fid)
    w = esm.world_record(fid)   # placed references (REFR / ACHR ...) live in the world data
    if w is None:
        return None
    sig, rec, _cell, _wrld = w
    # a placed reference's FULL sits inside its Map Marker block, which the .pas's
    # top-level 'FULL' path never reaches: blank, as in xEdit. Cells and worldspaces
    # have a top-level FULL of their own.
    return sig, rec.edid or "", esm.lstring(rec.first("FULL")) if sig in ("CELL", "WRLD") else ""


def exp_flst(esm):
    """!!!Wordpress - ExportFLSTToCSV.pas (v4) - form lists, three files:
    _List (one row per list, Entry_1..N as FormID:EDID:SIG, file order),
    _Entries (one row per entry; a list with no entries gets one row, index 0)
    and _Refs (Referenced By of the list itself, Ref_1..N)."""
    lists, entries, refrows = [], [], []
    head_e = "\t".join(["FLST_FormID", "FLST_EDID", "FLST_FULL", "EntryIndex", "Entry_Sig",
                        "Entry_FormID", "Entry_EDID", "Entry_FULL"])
    for r in R.xedit_order(esm, "FLST"):
        fid, ed, full = "%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full))
        ln = r.first("LNAM")
        fids = [x for v in r.all("LNAM") for x in struct.unpack_from("<%dI" % (len(v) // 4), v)] if ln else []
        cells = []
        for i, f in enumerate(fids):
            t = _flst_target(esm, f)
            if t is None:
                cells.append("")
                entries.append("\t".join([fid, ed, full, str(i), "", "", "", ""]))
            else:
                sg, te, tf = t
                cells.append("%08X:%s:%s" % (f, _ccell(te), sg))
                entries.append("\t".join([fid, ed, full, str(i), sg, "%08X" % f, _ccell(te), _ccell(tf)]))
        if not fids:
            entries.append("\t".join([fid, ed, full, "0", "", "", "", ""]))
        lists.append(([fid, ed, full, str(len(fids))], cells))
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        refrows.append(([fid, ed, str(len(refs))], refs))
    me = max((len(c) for _b, c in lists), default=0)
    mr = max((len(c) for _b, c in refrows), default=0)
    out_l = ["\t".join(["FLST_FormID", "FLST_EDID", "FLST_FULL", "EntryCount"]
                       + ["Entry_%d" % i for i in range(1, me + 1)])]
    out_l += ["\t".join(b + c + [""] * (me - len(c))) for b, c in lists]
    out_r = ["\t".join(["FLST_FormID", "FLST_EDID", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])]
    out_r += ["\t".join(b + c + [""] * (mr - len(c))) for b, c in refrows]
    return {"_List": out_l, "_Entries": [head_e] + entries, "_Refs": out_r}


# ------------------------------------------------------------- GMRW ------
GMRW_MAX_ITEMS = 10
GMRW_MAX_REFS = 20
GMRW_MEMBERS = {"NAM7", "NAM9", "XPCT", "ESRE", "QRCO", "QRCX", "NAM8", "QRLI", "QRLR", "QRRI", "QSRD", "CENT",
                "CTDA", "ITME"}
GMRW_WARNED = set()


def _comma_text(items):
    """Delphi TStringList.CommaText: an item with a space, comma, quote or control
    character goes in double quotes (quotes doubled); items joined with commas."""
    out = []
    for x in items:
        if x == "" or any(c <= " " or c in ',"' for c in x):
            out.append('"' + x.replace('"', '""') + '"')
        else:
            out.append(x)
    return ",".join(out)


def exp_gmrw(esm):
    """!!!Wordpress - ExportGMRWToTSV.pas - game rewards: one row per rewarded item
    (max 10) of every reward in the Rewards List (a reward runs up to its ITME marker),
    with the reward's XP / caps globals, curve, currency, legendary list / rank, the
    first condition's tier, the conditions three ways and Referenced By (max 20)."""
    def ref_str(fid):
        ed, sg = esm.edid_of(fid), esm.sig_of(fid)
        return "%08X" % fid + (":" + _ccell(ed) if _ccell(ed) else "") + (":" + sg if sg else "")

    def link_str(d):
        if not d or len(d) < 4:
            return ""
        f = _u32(d)
        return ref_str(f) if f and esm.exists(f) else ""

    out = ["\t".join(["FormID", "EDID", "ParentQuestDisplay", "ParentQuestLink", "RewardsCount", "RewardIndex",
                       "RewardedItemIndex", "RewardedItem", "RewardedItemCount", "NAM7_XPGlobal", "NAM8_CapsGlobal",
                       "XPCT_XPCurveTable", "QRCO_CurrencyObject", "QRLI_LegendaryItemRewardList",
                       "QRLR_LegendaryItemRewardRank", "QRRI_LegendaryRankRandom", "TierLabel",
                       "TierConditionFunc", "TierConditionValue", "Conditions", "ConditionGlobs",
                       "ReferencedByCount"] + ["Ref%d" % i for i in range(1, GMRW_MAX_REFS + 1)]
                      + ["ConditionsFull"])]
    for r in R.xedit_order(esm, "GMRW"):
        ctx = C.Ctx(esm, r)
        an = r.first("ANAM")
        pq = _u32(an) if an and len(an) >= 4 else 0
        pdisp = plink = ""
        if pq and esm.exists(pq):
            pdisp = _ccell(esm.full_of(pq)) or _ccell(esm.edid_of(pq))
            plink = ref_str(pq)
        # rewards: xEdit fills one reward until a field it already has comes round
        # again (QSRD / CTDA lists only while they run on), which then starts the next.
        # So ITME doesn't close a reward: a NAM7 after it still joins that reward, and
        # every repeated CENT starts a new one (SHELS01: 8 rewards).
        rewards, cur, seen, last, started = [], None, set(), None, False
        for k, v in r.subs:
            if k == "RWDS":
                started = True
                continue
            if not started:
                continue
            key = "CTDA" if k in ("CIS1", "CIS2") else k
            if key not in GMRW_MEMBERS:
                if k not in GMRW_WARNED:
                    GMRW_WARNED.add(k)
                    print("  GMRW: %s after the rewards in %s - not a reward field yet; check with xEdit" % (k, r.edid))
                continue
            repeat = key in seen and not (key in ("QSRD", "CTDA") and last == key)
            if cur is None or repeat:
                cur, seen = [], set()
                rewards.append(cur)
            cur.append((k, v))
            seen.add(key)
            last = key
        refs = [_ccell("%08X" % x + (":" + esm.refinfo[x][1] if esm.refinfo[x][1] else "") + ":" + esm.refinfo[x][0])
                for x in esm.refby.get(r.fid, ())][:GMRW_MAX_REFS]
        base = ["%08X" % r.fid, _ccell(r.edid), pdisp, plink, str(len(rewards))]
        tail_refs = [str(len(refs))] + refs + [""] * (GMRW_MAX_REFS - len(refs))

        def emit(ri, qi, item, cnt, cols, full):
            out.append("\t".join(base + [str(ri), str(qi), item, cnt] + cols + tail_refs + [full]))

        if not rewards:
            emit(0, 0, "", "", [""] * 12, "")
            continue
        for ri, rw in enumerate(rewards):
            def one(sig):
                return next((v for k, v in rw if k == sig), None)
            qrlr = one("QRLR")
            cols = [link_str(one("NAM7")), link_str(one("NAM8")), link_str(one("XPCT")), link_str(one("QRCO")),
                    link_str(one("QRLI")), str(_u32(qrlr)) if qrlr and len(qrlr) >= 4 else "",
                    ""]   # QRRI: the .pas path 'QRRI - QRRI' never resolves in xEdit
            conds = []
            for k, v in rw:
                if k == "CTDA":
                    conds.append([v, None, None])
                elif k == "CIS1" and conds:
                    conds[-1][1] = v.rstrip(b"\0").decode("utf-8", "replace")
                elif k == "CIS2" and conds:
                    conds[-1][2] = v.rstrip(b"\0").decode("utf-8", "replace")
            simple, globs, fulls = [], [], []
            tier = ["", "", ""]
            for ci, (ctda, c1, c2) in enumerate(conds):
                els, _ref = C.elements(ctx, ctda, c1, c2)
                byname = {}
                for nm, val in els:
                    byname.setdefault(nm, val)
                fn = _ccell(byname.get("Function", ""))
                valf = _ccell(byname.get("Comparison Value - Float", ""))
                op = _ccell(byname.get("Type", ""))
                simple.append(_ccell(fn + " " + op + " " + valf))
                if ctda[0] & 4:
                    g = _u32(ctda[4:8])
                    if g and esm.exists(g):
                        globs.append(ref_str(g))
                if ci == 0:
                    label = ""
                    if "getpubliceventhasmutation" in fn.lower():
                        label = "mutated"
                    elif "getvmquestvariableunique" in fn.lower() or "getvmscriptvariable" in fn.lower():
                        try:
                            fv = float(valf)
                        except ValueError:
                            fv = 0.0
                        label = "gold" if fv >= 2.9 else "silver" if fv >= 1.9 else "bronze" if fv >= 0.9 else ""
                    tier = [label, fn, valf]
                # ConditionsFull: RunOn.Function(<every other non-empty element>) Type Comparison;
                # the 9th element (Reference / its unused slot) is left out, as the .pas does
                cmp = byname.get("Comparison Value - Float", byname.get("Comparison Value - Global", ""))
                params = []
                for i, (nm, val) in enumerate(els):
                    val = val.strip(" ")
                    if not val or i == 8 or nm in ("Type", "Function", "Run On", "Comparison Value - Float",
                                                   "Comparison Value - Global"):
                        continue
                    params.append(val)
                run_on = byname.get("Run On", "")
                fulls.append(_ccell((run_on + "." if run_on else "") + byname.get("Function", "") + "("
                                    + ", ".join(params) + ") " + byname.get("Type", "") + " " + cmp))
            cols += tier + [_comma_text(simple), _comma_text(globs)]
            full = "|".join(fulls)
            items = [v for k, v in rw if k == "QSRD"][:GMRW_MAX_ITEMS]
            if not items:
                emit(ri, 0, "", "", cols, full)
            for qi, q in enumerate(items):
                f, n = struct.unpack_from("<Ii", q)
                emit(ri, qi, ref_str(f) if f and esm.exists(f) else "", str(n), cols, full)
    return out


# ------------------------------------------------------------- KEYM ------
def exp_keym(esm):
    """!!!Wordpress - ExportKEYMToTSV.pas - keys: the BOOK columns without the four
    book-only ones (DNAM / INAM / BTOF / BVGO), and the same _Locations file."""
    def ev(r, sub):
        v = r.first(sub)
        return _ccell(esm.link_edit_value(_u32(v))) if v and len(v) >= 4 else ""

    rows = []
    locs = ["KEYM_FormID\tKEYM_EDID\tLocationName\tLocationSource\tQuestName"]
    for r in R.xedit_order(esm, "KEYM"):
        fid, ed = "%08X" % r.fid, _ccell(r.edid)
        kw = r.first("KWDA") or b""
        kws = [("%08X:%s:%s" % (k, esm.edid_of(k), esm.sig_of(k))) if _lk(esm, k) else _ccell(esm.link_edit_value(k))
               for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]
        d = r.first("DATA")
        value = weight = ""
        if d and len(d) >= 8:
            v, w = struct.unpack_from("<if", d)
            value, weight = str(v), C.fmt_float(w)
        refs = _refcells(esm, r.fid)
        rows.append(([fid, ed, _ccell(esm.lstring(r.full)), _ccell(esm.lstring(r.first("DESC"))), str(len(kws))], kws,
                     [ev(r, "PTRN"), _ccell(_zstr(r.first("MODL"))) if r.first("MODL") is not None else "",
                      ev(r, "VCRY"), value, weight, str(len(refs))], refs))
        locs.append("\t".join([fid, ed] + list(_placed_location(esm, r.fid))))
    mk = max((len(x[1]) for x in rows), default=0)
    mr = max((len(x[3]) for x in rows), default=0)
    head = (["FormID", "EDID", "FULL", "DESC", "KeywordCount"] + ["KW%d" % i for i in range(1, mk + 1)]
            + ["PTRN", "Model", "VCRY", "DATA_Value", "DATA_Weight", "ReferencedByCount"]
            + ["Ref%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for a_, k, b_, x in rows:
        out.append("\t".join(a_ + k + [""] * (mk - len(k)) + b_ + x + [""] * (mr - len(x))))
    return {"": out, "_Locations": locs}


# ------------------------------------------------------------- LGDI ------
LGDI_MAX_REFS = 30
LGDI_STAR = ["First Star Slot", "Second Star Slot", "Third Star Slot", "Fourth Star Slot", "Fifth Star Slot"]
LGDI_POOL_SIGS = {"LVLI", "OMOD", "FLST", "GLOB", "CURV", "LVLO"}


def _omod_links(esm, rec):
    """Every FormID an OMOD's elements link to, in xEdit's element order: the record
    itself first (xEdit's LinksTo of a main record is the record), then each subrecord
    in file order - DATA read field by field (attach point, attach parent slots, items,
    includes, then each property's Value 1 when it holds a form and its curve table),
    any other subrecord through the Referenced By field rules."""
    import esm_refs
    rules = esm_refs._rules()
    out = [rec.fid]
    for k, v in rec.subs:
        if k == "DATA" and len(v) >= 28:
            inc, prop = struct.unpack_from("<II", v)
            out.append(struct.unpack_from("<I", v, 16)[0])
            p = 20
            for _ in range(2):
                n = struct.unpack_from("<I", v, p)[0]
                out += list(struct.unpack_from("<%dI" % n, v, p + 4))
                p += 4 + 4 * n
            for _ in range(inc):
                out.append(struct.unpack_from("<I", v, p)[0])
                p += 7
            for _ in range(prop):
                vt, _fn, _pid, v1, _v2, ct = struct.unpack_from("<BxxxBxxxHxxIII", v, p)
                if vt in (4, 6):
                    out.append(v1)
                out.append(ct)
                p += 24
            continue
        for o in range(0, len(v) - 3):
            f = struct.unpack_from("<I", v, o)[0]
            if f and esm.exists(f) and esm_refs._is_ref(rules, "OMOD", k, o, v, esm.sig_of(f)):
                out.append(f)
    return out


def exp_lgdi(esm):
    """!!!Wordpress - ExportLGDIToTSV.pas - legendary item lists: main file (scrip cost,
    currency, applicable item list, model, preview transform, legendary template,
    include / exclude filters, star-slot mods, Referenced By: count of all, the first 30
    listed) and _Mods (one row per star-slot mod with its mod pool: every leveled list /
    mod / form list / global / curve the mod collection's elements link to, de-duplicated).
    Like the .pas, '"' is doubled in the text columns and nothing else is cleaned."""
    def q(x):
        return x.replace('"', '""')

    def full(f):
        return esm.full_of(f)

    def resolve(d):
        """ResolveRef: EDID "FULL" [SIG:FormID] of a linked record, else xEdit's edit value."""
        if not d or len(d) < 4:
            return ""
        f = _u32(d)
        if f and esm.exists(f):
            return '%s "%s" [%s:%08X]' % (esm.edid_of(f), full(f), esm.sig_of(f), f)
        return esm.link_edit_value(f)

    omods = {r.fid: r for r in esm.records.get("OMOD", [])}
    main = ["\t".join(["LGDI_FormID", "LGDI_EDID", "FULL", "ScripCost", "ValueCurrency",
                        "ApplicableItemList_FormID", "ApplicableItemList_EDID", "ApplicableItemList_FULL", "Model",
                        "PreviewTransform", "LegendaryTemplate", "IncludeFilters", "ExcludeFilters", "LegModCount",
                        "LegMods_Flat", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, LGDI_MAX_REFS + 1)])]
    mods = ["\t".join(["LGDI_FormID", "LGDI_EDID", "ModIndex", "StarSlot", "OMOD_FormID", "OMOD_EDID", "OMOD_FULL",
                        "ModPool_Refs"])]
    for r in R.xedit_order(esm, "LGDI"):
        fid, ed = "%08X" % r.fid, r.edid or ""
        dt = r.first("DATA")
        scrip = str(_u32(dt)) if dt and len(dt) >= 4 else ""
        an = r.first("ANAM")
        a_fid = a_ed = a_full = ""
        if an and len(an) >= 4:
            af = _u32(an)
            if af and esm.exists(af):
                a_fid, a_ed, a_full = "%08X" % af, esm.edid_of(af), full(af)
            else:
                a_ed = esm.link_edit_value(af)
        mo = r.first("MODL")
        model = mo.rstrip(b"\0").decode("utf-8", "replace") if mo else ""
        bn = r.first("BNAM") or b""
        flat = []
        for i in range(len(bn) // 8):
            slot, of = struct.unpack_from("<II", bn, i * 8)
            star = LGDI_STAR[slot] if slot < len(LGDI_STAR) else "<Unknown: %d>" % slot
            o_fid = o_ed = o_full = pool = ""
            if of and esm.exists(of):
                o_fid, o_ed, o_full = "%08X" % of, esm.edid_of(of), full(of)
                acc = []
                orec = omods.get(of)
                for f in (_omod_links(esm, orec) if orec else [of]):
                    if not (f and esm.exists(f)) or esm.sig_of(f) not in LGDI_POOL_SIGS:
                        continue
                    h = "%08X" % f
                    if h in " | ".join(acc):       # the .pas de-dupes with Pos(FormID, acc)
                        continue
                    acc.append("%s [%s:%s]" % (esm.edid_of(f), esm.sig_of(f), h))
                pool = " | ".join(acc)
            else:
                o_ed = esm.link_edit_value(of)
            mods.append("\t".join([fid, ed, str(i), q(star), o_fid, o_ed, q(o_full), q(pool)]))
            flat.append(star + "=" + o_ed + (" [OMOD:%s]" % o_fid if o_fid else ""))
        refs = ["%08X:%s:%s" % (x, esm.refinfo[x][1], esm.refinfo[x][0]) for x in esm.refby.get(r.fid, ())]
        shown = refs[:LGDI_MAX_REFS]
        main.append("\t".join([fid, ed, q(esm.lstring(r.full)), scrip, q(resolve(r.first("VCRY"))), a_fid, a_ed,
                                q(a_full), q(model), q(resolve(r.first("PTRN"))), q(resolve(r.first("ENAM"))),
                                # Include / Exclude Filters: blank in xEdit too. No LGDI has CNAM; the
                                # one DNAM (zzzCOMP_..._RESTRICT, 12 bytes: NULL, 0x11, NULL) isn't a
                                # form list xEdit can walk, so the .pas finds nothing to list.
                                "", "", str(len(bn) // 8),
                                q(" | ".join(flat)), str(len(refs))] + shown + [""] * (LGDI_MAX_REFS - len(shown))))
    return {"": main, "_Mods": mods}


# ------------------------------------------------------------- LCTN ------
LCTN_TYPES = {0: "None", 1: "Raid", 8: "Power Plant", 9: "Workshop"}
_LCTN_WARNED = set()


def _pas_unresolved(fid):
    """GetEditValue of a FormID xEdit can't resolve (no spaces inside the < >)."""
    return "[%08X] <Error: Could not be resolved>" % fid


def exp_lctn(esm):
    """!!!Wordpress - ExportLCTNToTSV.pas - locations, three files: _LCTN (keywords,
    PRPS, DATA, parent location FormID:EDID:FULL, radius, actor fade, colour),
    _LCSR (master special references, sorted by the placed reference's FormID as xEdit
    sorts them: location ref type, reference and its xEdit name, world / cell, grid)
    and _Refs (Referenced By)."""
    def ed(f):
        return _ccell(esm.edid_of(f))

    def ev(f):
        if not f:
            return esm.link_edit_value(0)
        if esm.exists(f) or esm.world_record(f) is not None:
            return _ccell(esm.link_edit_value(f))
        return _pas_unresolved(f)

    rows, lcsr, refrows = [], [], []
    want = set()
    for r in esm.records.get("LCTN", []):
        ls = r.first("LCSR") or b""
        for i in range(len(ls) // 16):
            want.update(struct.unpack_from("<III", ls, i * 16)[1:])
    esm.prefetch_world(want)
    lcsr.append("\t".join(["LCTN_FormID", "LCTN_EDID", "LCSR_Index", "LocRefType_FormID", "LocRefType_EDID",
                            "Ref_FormID", "Ref_Display", "WorldCell", "GridX", "GridY"]))
    for r in R.xedit_order(esm, "LCTN"):
        fid, edid = "%08X" % r.fid, _ccell(r.edid)
        kw = r.first("KWDA") or b""
        kws = [("%08X:%s:%s" % (k, esm.edid_of(k), esm.sig_of(k))) if _lk(esm, k) else _ccell(esm.link_edit_value(k))
               for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]
        pr = r.first("PRPS") or b""
        props = []
        for av, val, cv in sorted((struct.unpack_from("<IfI", pr, i * 12) for i in range(len(pr) // 12)),
                                  key=lambda x: x[0]):
            a = ed(av) if _lk(esm, av) else _ccell(esm.link_edit_value(av))
            c = ed(cv) if _lk(esm, cv) else _ccell(esm.link_edit_value(cv))
            props.append([a, C.fmt_float(val), "" if c.startswith("NULL") else c])
        d = r.first("DATA")
        data = [""] * 5
        if d and len(d) >= 8:
            ui, ub, mn, lt, mx = struct.unpack_from("<IBBBB", d)
            tn = LCTN_TYPES.get(lt)
            if tn is None:
                if lt not in _LCTN_WARNED:
                    _LCTN_WARNED.add(lt)
                    print("  LCTN: location type %d has no xEdit name yet - check against an xEdit run" % lt)
                tn = "<Unknown: %d>" % lt
            data = [str(ui), str(ub), str(mn), tn, str(mx)]
        pn = r.first("PNAM")
        pnam = ""
        if pn and len(pn) >= 4:
            pf = _u32(pn)
            pnam = ("%08X:%s:%s" % (pf, esm.edid_of(pf), _ccell(esm.full_of(pf)))) if _lk(esm, pf) else ev(pf)

        def flt(sub):
            v = r.first(sub)
            return C.fmt_float(struct.unpack_from("<f", v)[0]) if v and len(v) >= 4 else ""
        # CNAM (Color) is a struct: its edit value is empty in xEdit
        rows.append(([fid, edid, _ccell(esm.lstring(r.full)), str(len(kws))], kws, props,
                     data + [pnam, flt("RNAM"), flt("ANAM"), ""]))
        ls = r.first("LCSR") or b""
        ents = sorted((struct.unpack_from("<IIIhh", ls, i * 16) for i in range(len(ls) // 16)), key=lambda x: x[1])
        for i, (lt, ref, wc, gy, gx) in enumerate(ents):
            if _lk(esm, lt):
                lt_f, lt_e = "%08X" % lt, esm.edid_of(lt)
            else:
                lt_f, lt_e = ev(lt), ""
            if ref and esm.world_record(ref) is not None:
                rf, rd = "%08X" % ref, _ccell(esm.link_edit_value(ref))
            else:
                rf, rd = "", ev(ref)
            lcsr.append("\t".join([fid, edid, str(i), lt_f, lt_e, rf, rd, ev(wc), str(gx), str(gy)]))
        refrows.append(([fid, edid], _refcells(esm, r.fid)))
    mk = max((len(x[1]) for x in rows), default=0)
    mp = max((len(x[2]) for x in rows), default=0)
    head = (["LCTN_FormID", "LCTN_EDID", "LCTN_FULL", "KeywordCount"] + ["KW_%d" % i for i in range(1, mk + 1)]
            + ["PropCount"] + [c % i for i in range(1, mp + 1) for c in ("Prop_%d_AV", "Prop_%d_Val", "Prop_%d_Curve")]
            + ["DATA_UnknownInt", "DATA_UnknownByte", "DATA_MinLocationLevel", "DATA_LocationType",
               "DATA_MaxLocationLevel", "PNAM_ParentLocation", "RNAM_WorldLocationRadius", "ANAM_ActorFadeMult",
               "CNAM_Color"])
    out = ["\t".join(head)]
    for b, k, p_, t in rows:
        out.append("\t".join(b + k + [""] * (mk - len(k)) + [str(len(p_))] + [v for x in p_ for v in x]
                              + [""] * (3 * (mp - len(p_))) + t))
    mr = max((len(x[1]) for x in refrows), default=0)
    rout = ["\t".join(["LCTN_FormID", "LCTN_EDID", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])]
    for b, x in refrows:
        rout.append("\t".join(b + [str(len(x))] + [_ccell(y) for y in x] + [""] * (mr - len(x))))
    return {"_LCTN": out, "_LCSR": lcsr, "_Refs": rout}


# ------------------------------------------------------------- MGEF ------
# Checked against her October 2026 xEdit export (every value the game uses today).
# Indices not in use yet carry Fallout 4's name and print a warning when they turn up.
MGEF_ARCHETYPES = {0: "Value Modifier", 1: "Script", 2: "Dispel", 4: "Absorb", 5: "Dual Value Modifier", 6: "Calm",
                   7: "Demoralize", 8: "Frenzy", 9: "Disarm", 11: "Invisibility", 12: "Light", 18: "Summon Creature",
                   20: "Player Fear", 21: "Paralysis", 25: "Guide", 26: "Damage Item", 28: "Cure Addiction",
                   31: "Stimpak", 33: "Stagger", 34: "Peak Value Modifier", 35: "Cloak", 36: "Unknown 36",
                   37: "Slow Time", 40: "Spawn Hazard", 45: "Damage", 47: "Permanent Reanimate", 48: "Jetpack",
                   49: "Chameleon", 50: "Grow Flora", 51: "Corpse Highlight", 52: "Stunned"}
MGEF_ARCHETYPES_UNCHECKED = {3: "Cure Disease", 10: "Command Summoned", 13: "Darkness", 14: "Nighteye", 15: "Lock",
                             16: "Open", 17: "Bound Weapon", 19: "Detect Life", 22: "Reanimate", 23: "Soul Trap",
                             24: "Turn Undead", 27: "Cure Paralysis", 29: "Cure Poison", 30: "Concussion",
                             32: "Accumulate Magnitude", 38: "Rally", 39: "Enhance Weapon", 41: "Etherealize",
                             42: "Banish", 43: "Spawn Scripted Ref", 44: "Disguise", 46: "Immunity"}
MGEF_CASTING = ["Constant Effect", "Fire and Forget", "Concentration", "Scroll"]
MGEF_DELIVERY = ["Self", "Contact", "Aimed", "Target Actor", "Target Location"]
MGEF_SOUND = ["Loud", "Normal", "Silent", "Very Loud", "Quiet"]
MGEF_CHECKED = {"casting type": 3, "delivery": 4}   # indices below these seen in xEdit's export
_MGEF_WARNED = set()


def _mgef_enum(names, i, what):
    if isinstance(names, dict):
        v = names.get(i)
        if v is None:
            v = MGEF_ARCHETYPES_UNCHECKED.get(i, "<Unknown: %d>" % i)
            if (what, i) not in _MGEF_WARNED:
                _MGEF_WARNED.add((what, i))
                print("  MGEF: %s %d (%s) not checked against xEdit yet" % (what, i, v))
        return v
    if 0 <= i < len(names):
        if i >= MGEF_CHECKED.get(what, len(names)) and (what, i) not in _MGEF_WARNED:
            _MGEF_WARNED.add((what, i))
            print("  MGEF: %s %d (%s) not checked against xEdit yet" % (what, i, names[i]))
        return names[i]
    return "<Unknown: %d>" % i


def exp_mgef(esm):
    """!!!Wordpress - ExportMGEFToTSV.pas - magic effects: DNAM, the DATA fields (FO76's
    160-byte DATA: Fallout 4's layout with an extra u32 at 4 - Base Cost sits at 8),
    VMAD scripts and
    Referenced By. Columns the .pas asks for by a name FO76Edit doesn't use (Flags2,
    Magic Skill, Min Skill Level, Spellmaking Area / Casting Time, Casting Art, Dual
    Casting Art / Scale, Enchant Art, Hit / Enchant Visuals, Script AI Data) are blank
    in xEdit and here."""
    rows, refrows, _lc = _scriptable(esm, "MGEF")

    def ev(f):
        return _ccell(esm.link_edit_value(f))

    def edid_cell(f):
        """FieldCellValue: the linked record's EDID; NULL / unresolved -> ''."""
        if f and esm.exists(f):
            return _ccell(esm.edid_of(f))
        t = _ccell(esm.link_edit_value(f))
        return "" if "NULL - Null Reference" in t else t

    head = ["MGEF_FormID", "EDID", "FULL", "DNAM_MagicItemDescription", "DATA_Flags", "DATA_Flags2", "BaseCost",
            "Unused", "MagicSkillRaw", "ResistValue", "CounterEffectCount", "CastingLight", "TaperWeight",
            "HitShader", "EnchantShader", "MinSkillLevel", "Spellmaking_Area", "CastingTime", "TaperCurve",
            "TaperDuration", "SecondAVWeight", "Archetype", "ActorValue", "Projectile", "Explosion", "CastingType",
            "Delivery", "CastingArt", "HitEffectArt", "ImpactData", "SkillUsageMultiplier", "DualCastingArt",
            "DualCastingScale", "EnchantArt", "HitVisuals", "EnchantVisuals", "EquipAbility", "ImageSpaceModifier",
            "PerkToApply", "CastingSoundLevel", "ScriptAI_Score", "ScriptAI_DelayTime", "ScriptAI_Unknown",
            "VMAD_ScriptCount", "VMAD_Scripts", "ReferencedByCount"]
    body = []
    for (r, fid, ed, full, _xa, _kw, _pr, vc, vm), (_f, _e, refs) in zip(rows, refrows):
        d = r.first("DATA") or b""
        cols = [""] * 39
        if len(d) >= 148:
            u = lambda o: struct.unpack_from("<I", d, o)[0]
            f = lambda o: C.fmt_float(struct.unpack_from("<f", d, o)[0])
            cols = [_flag_bits(u(0)), "", f(8), C.hexbytes(d[26:28]), "", ev(u(20)),
                    str(struct.unpack_from("<H", d, 24)[0]), ev(u(28)), f(32), ev(u(36)), ev(u(40)), "", "", "",
                    f(56), f(60), f(64), _mgef_enum(MGEF_ARCHETYPES, u(68), "archetype"), edid_cell(u(72)),
                    edid_cell(u(76)), edid_cell(u(80)), _mgef_enum(MGEF_CASTING, u(84), "casting type"),
                    _mgef_enum(MGEF_DELIVERY, u(88), "delivery"), "", edid_cell(u(100)), edid_cell(u(104)),
                    f(108), "", "", "", "", "", edid_cell(u(132)), edid_cell(u(136)), edid_cell(u(140)),
                    _mgef_enum(MGEF_SOUND, u(144), "sound level"), "", "", ""]
        body.append(([fid, _ccell(ed), _ccell(full), _ccell(esm.lstring(r.first("DNAM")))] + cols
                     + [str(vc), vm, str(len(refs))], refs))
    mr = max((len(x[1]) for x in body), default=0)
    out = ["\t".join(head + ["Ref_%d" % i for i in range(1, mr + 1)])]
    for b, x in body:
        out.append("\t".join(b + x + [""] * (mr - len(x))))
    return out


# ------------------------------------------------------------- NOTE ------
NOTE_TYPES = ["Sound", "Voice", "Program", "Terminal"]


def exp_note(esm):
    """!!!Wordpress - ExportNOTEToTSV.pas - holotapes: type, model, value / weight,
    preview transform, pickup / putdown sounds, program file, keywords, Referenced By.
    VCRY / SNAM / DIQO are read in the .pas with ElementByPath(rec, 'VCRY'), which
    FO76Edit never resolves, so those nine columns are blank in xEdit and here."""
    def ev(r, sub):
        v = r.first(sub)
        return _ccell(esm.link_edit_value(_u32(v))) if v and len(v) >= 4 else ""

    rows = []
    for r in R.xedit_order(esm, "NOTE"):
        dn = r.first("DNAM")
        dtype = (NOTE_TYPES[dn[0]] if dn[0] < len(NOTE_TYPES) else "<Unknown: %d>" % dn[0]) if dn else ""
        d = r.first("DATA")
        value = weight = ""
        if d and len(d) >= 8:
            v, w = struct.unpack_from("<if", d)
            value, weight = str(v), C.fmt_float(w)
        kw = r.first("KWDA") or b""
        kws = [("%08X:%s:%s" % (k, _ccell(esm.edid_of(k)), esm.sig_of(k))) if _lk(esm, k)
               else _ccell(esm.link_edit_value(k)) for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        rows.append((["%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full)), dtype,
                      _ccell(_zstr(r.first("MODL"))) if r.first("MODL") is not None else "",
                      _ccell(_zstr(r.first("ICON"))) if r.first("ICON") is not None else "", value, weight,
                      "", "", ev(r, "PTRN"), ev(r, "YNAM"), ev(r, "ZNAM"), "", "", "", "", "", "",
                      _ccell(_zstr(r.first("PNAM"))) if r.first("PNAM") is not None else "", str(len(kws))], kws, refs))
    mk = max((len(x[1]) for x in rows), default=0)
    mr = max((len(x[2]) for x in rows), default=0)
    head = (["NOTE_FormID", "NOTE_EDID", "FULL", "DNAM_Type", "Model", "ICON", "DATA_Value", "DATA_Weight",
             "VCRY_FormID", "VCRY_EDID", "PTRN", "YNAM_PickupSound", "ZNAM_PutdownSound", "SNAM_Scene_FormID",
             "SNAM_Scene_EDID", "SNAM_Scene_FULL", "DIQO_Quest_FormID", "DIQO_Quest_EDID", "DIQO_Quest_FULL",
             "PNAM_ProgramFile", "KeywordCount"] + ["KW_%d" % i for i in range(1, mk + 1)]
            + ["ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for b, k, x in rows:
        out.append("\t".join(b + k + [""] * (mk - len(k)) + [str(len(x))] + x + [""] * (mr - len(x))))
    return out


# ------------------------------------------------------------- PCRD ------
PCRD_SPECIAL = ["Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"]
PCRD_RACE = ["None", "Human", "Ghoul"]


def exp_pcrd(esm):
    """!!!Wordpress - ExportPCRDToTSV.pas - perk cards: form version, description, male /
    female names, flags, DATA (value, min level, SPECIAL, race restriction), the cost
    global, sound, every rank (card rank, male / female perk FormID:EDID:FULL; PRKE ...
    PRKF), flat and expanded, and Referenced By."""
    def trip(f):
        return ["%08X" % f, _ccell(esm.edid_of(f)), _ccell(esm.full_of(f))] if f and esm.exists(f) else []

    def enum(names, i):
        return names[i] if i < len(names) else "<Unknown: %d>" % i

    rows = []
    for r in R.xedit_order(esm, "PCRD"):
        top, ranks, cur = {}, [], None
        for k, v in r.subs:
            if k == "PRKE":
                cur = {}
                continue
            if k == "PRKF":
                if cur is not None:
                    ranks.append(cur)
                cur = None
                continue
            tgt = cur if cur is not None else top
            tgt.setdefault(k, v)
        d = top.get("DATA") or b""
        dv = dm = ds = dr = ""
        if len(d) >= 7:
            dv = C.fmt_float(struct.unpack_from("<f", d)[0])
            dm, ds, dr = str(d[4]), enum(PCRD_SPECIAL, d[5]), enum(PCRD_RACE, d[6])
        pc = top.get("PCDV")
        pcdv = trip(_u32(pc)) if pc and len(pc) >= 4 else []
        pcdv = pcdv or ["", "", ""]
        sn = top.get("SNAM")
        snd = ""
        if sn and len(sn) >= 4 and _lk(esm, _u32(sn)):
            f = _u32(sn)
            snd = "%08X:%s:%s" % (f, _ccell(esm.edid_of(f)), esm.sig_of(f))
        an = top.get("ANAM")
        flags = _flag_bits(_u32(an)) if an and len(an) >= 4 else ""
        rk = []
        for e_ in ranks:
            cr = str(e_["DATA"][0]) if e_.get("DATA") else ""
            m = trip(_u32(e_["MNAM"])) if e_.get("MNAM") and len(e_["MNAM"]) >= 4 else []
            fm = trip(_u32(e_["FNAM"])) if e_.get("FNAM") and len(e_["FNAM"]) >= 4 else []
            rk.append((cr, m, fm))
        flat = " || ".join("%s|%s|%s" % (cr, ":".join(m), ":".join(fm)) for cr, m, fm in rk)
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        base = ["%08X" % r.fid, _ccell(r.edid), str(r.version), _ccell(esm.lstring(top.get("DESC"))),
                _ccell(_zstr(top.get("MNAM"))) if top.get("MNAM") is not None else "",
                _ccell(_zstr(top.get("FNAM"))) if top.get("FNAM") is not None else "", flags, dv, dm, ds, dr]
        rows.append((base + pcdv + [snd, str(len(rk)), flat, str(len(refs))], rk, refs))
    mk = max((len(x[1]) for x in rows), default=0)
    mr = max((len(x[2]) for x in rows), default=0)
    head = ["PCRD_FormID", "PCRD_EDID", "FormVersion", "DESC", "MNAM_MaleName", "FNAM_FemaleName",
            "ANAM_PerkCardFlags", "DATA_Value", "DATA_MinLevel", "DATA_Special", "DATA_RaceRestriction",
            "PCDV_GLOB_FormID", "PCDV_GLOB_EDID", "PCDV_GLOB_FULL", "SNAM_Sound", "RankCount", "Ranks_Flat",
            "ReferencedByCount"]
    for i in range(1, mk + 1):
        head += ["Rank_%d_CardRank" % i] + ["Rank_%d_%s_%s" % (i, g, c) for g in ("MalePerk", "FemalePerk")
                                            for c in ("FormID", "EDID", "FULL")]
    head += ["Ref_%d" % i for i in range(1, mr + 1)]
    out = ["\t".join(head)]
    for b, rk, x in rows:
        line = list(b)
        for cr, m, fm in rk:
            line += [cr] + (m or ["", "", ""]) + (fm or ["", "", ""])
        line += [""] * (7 * (mk - len(rk)))
        out.append("\t".join(line + x + [""] * (mr - len(x))))
    return out


# ------------------------------------------------------------- ENCH ------
ENCH_TYPES = {6: "Enchantment", 12: "Staff Enchantment"}


def exp_ench(esm):
    """ExportENCHToTSV.pas - enchantments: ENIT (Flags, Cast / Target / Enchant Type,
    Charge Time), per effect MGEF FormID + EDID, Magnitude, Area, Duration, curve
    (CVT0) and the actor value the curve is keyed on (MAGA), then Referenced By.
    ENIT_EnchantCost / ENIT_EnchantAmount stay blank: the .pas asks for 'Enchant Cost'
    and 'Amount', which aren't xEdit's field names ('Enchantment Cost' / 'Enchantment
    Amount'). Globals_Count is always 0 (ENCH has no PRPS)."""
    def ed(f):
        return _ccell(esm.edid_of(f))

    rows = []
    for r in R.xedit_order(esm, "ENCH"):
        e = r.first("ENIT")
        en = [""] * 7
        if e and len(e) >= 28:
            _cost, fl, ct, _amt, tt, et, ch = struct.unpack_from("<iIIiIIf", e)
            en = [_flag_bits(fl), _enum(CAST_TYPE, ct), _enum(TARGET_TYPE, tt), ENCH_TYPES.get(et, str(et)),
                  C.fmt_float(ch), "", ""]
        effs = []
        for k, v in r.subs:
            if k == "EFID":
                effs.append({"EFID": v})
            elif effs and k in ("EFIT", "CVT0", "MAGA"):
                effs[-1].setdefault(k, v)
        cells = []
        for x in effs:
            m = _u32(x["EFID"])
            fid_ed = ["%08X" % m, ed(m)] if _lk(esm, m) else ["", ""]
            mag = area = dur = ""
            efit = x.get("EFIT")
            # EFIT layout follows the record's form version: from version 170 a leading
            # u32 comes before Magnitude, Area, Duration (16 or 24 bytes); older records
            # (12 bytes, or 24 at versions 156-164) start with Magnitude.
            if efit and len(efit) >= 16 and r.version >= 170:
                _id, mg, ar, du = struct.unpack_from("<IfII", efit)
                mag, area, dur = C.fmt_float(mg), str(ar), str(du)
            elif efit and len(efit) >= 12:
                mg, ar, du = struct.unpack_from("<fII", efit)
                mag, area, dur = C.fmt_float(mg), str(ar), str(du)
            cv = _u32(x.get("CVT0"))
            ma = _u32(x.get("MAGA"))
            cells.append(fid_ed + [mag, area, dur, ed(cv) if _lk(esm, cv) else "",
                                   "%08X" % ma if ma else "", ed(ma) if _lk(esm, ma) else ""])
        refs = _refcells(esm, r.fid)
        rows.append((["%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full))] + en + [str(len(cells))],
                     cells, refs))
    me = max((len(x[1]) for x in rows), default=0)
    mr = max((len(x[2]) for x in rows), default=0)
    head = (["ENCH_FormID", "ENCH_EDID", "ENCH_FULL", "ENIT_Flags", "ENIT_CastType", "ENIT_TargetType",
             "ENIT_EnchantType", "ENIT_ChargeTime", "ENIT_EnchantCost", "ENIT_EnchantAmount", "Effects_Count"]
            + [c % i for i in range(1, me + 1)
               for c in ("Effect_%d_MGEF_FID", "Effect_%d_MGEF_EID", "Effect_%d_Magnitude", "Effect_%d_Area",
                         "Effect_%d_Duration", "Effect_%d_CURV_EID", "Effect_%d_MAGA_FID", "Effect_%d_MAGA_EID")]
            + ["Globals_Count", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])
    out = ["\t".join(head)]
    for b, c, x in rows:
        out.append("\t".join(b + [v for t in c for v in t] + [""] * (8 * (me - len(c))) + ["0", str(len(x))]
                             + x + [""] * (mr - len(x))))
    return out


# ------------------------------------------------------------- ENTM ------
ENTM_MAX_REFS = 64


def exp_entm(esm):
    """ExportENTMToTSV.pas - entitlements (Atom Shop / Fallout 1st / season items):
    names, XALG flags, keywords, storefront image path / preview / loadout, Referenced By
    (| joined, capped at 64 with a "...+N more" tail) and the confirm images (ECIL).
    ECIL quirk kept on purpose: the .pas joins the images with char(ECIL_SEP), which
    comes out empty, so every image lands in ECIL_1 run together ('A_C1.ddsA_C2.dds')
    and ECIL_2.. stay blank; build_allies_pets_weather_json.py splits them on '.dds'."""
    def txt(sub):
        v = r.first(sub)
        return _ccell(_zstr(v)) if v is not None else ""

    rows = []
    for r in R.xedit_order(esm, "ENTM"):
        xa = r.first("XALG")
        xalg = ""
        if xa:
            bits = struct.unpack("<Q", xa[:8].ljust(8, b"\0"))[0]
            xalg = "|".join(XALG_FLAGS[i] for i in range(32) if bits >> i & 1)
        kw = r.first("KWDA") or b""
        kws = [("%08X:%s:%s" % (k, _ccell(esm.edid_of(k)), esm.sig_of(k))) if _lk(esm, k)
               else _ccell(esm.link_edit_value(k)) for k in sorted(struct.unpack_from("<%dI" % (len(kw) // 4), kw))]
        lg = r.first("ETLG")
        etlg = _ccell(esm.link_edit_value(_u32(lg))) if lg and len(lg) >= 4 else ""
        refs = [_ccell(x) for x in _refcells(esm, r.fid)]
        ref = "|".join(refs[:ENTM_MAX_REFS])
        if len(refs) > ENTM_MAX_REFS:
            ref += "|...+%d more" % (len(refs) - ENTM_MAX_REFS)
        ecil = [_ccell(_zstr(v)) for v in r.all("ECIL")]
        rows.append(["%08X" % r.fid, _ccell(r.edid), _ccell(esm.lstring(r.full)), _ccell(esm.lstring(r.first("DESC"))),
                     _ccell(esm.lstring(r.first("NNAM"))), xalg, "|".join(kws), txt("SWFI"), txt("BEVA"),
                     txt("ETIP"), txt("ETDI"), etlg, ref, str(len(ecil)), "".join(ecil)])
    me = max((int(x[13]) for x in rows), default=0)
    out = ["\t".join(["FormID", "EDID", "FULL", "DESC", "NNAM", "XALG_Flags", "KEYWORDS", "SWFI", "BEVA",
                      "ETIP", "ETDI", "ETLG", "ReferencedBy", "ECIL_Count"] + ["ECIL_%d" % i for i in range(1, me + 1)])]
    for x in rows:
        cells = [x[14]] if me else []
        out.append("\t".join(x[:14] + cells + [""] * (me - len(cells))))
    return out


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
# Sets: (slug, test on FormID / EDID / record) per record type.
def _is_note_book(r):
    """ExportBOOK2ToCSV.pas IsNoteBook: the model path names a note. (Its other test,
    a MiscNote01 / MiscNote02 / MiscNote03_Torn keyword, adds no book: those names are
    preview transforms (TRNS, the BOOK PTRN), and the .pas reads a top-level KWDA that
    FO76 keeps inside the Keywords block. Checked 25 Oct 2026: 0 books on live.)"""
    m = r.first("MODL")
    return m is not None and "note" in _zstr(m).lower()


PLACEMENT_SETS = {
    # ExportMISC2ToCSV.pas
    "MISC": [("pint-sized-slasher-masks",
              lambda fid, ed, r: fid == 0x008E069E or "sdow_slasherclue" in ed.lower())],
    # ExportACTI2ToCSV.pas: the Treasure Map mounds, not the TEMP / DEBUG ones
    "ACTI": [("treasure-maps",
              lambda fid, ed, r: "treasuremapmoundactivator" in ed.lower()
              and "temp" not in ed.lower() and "debug" not in ed.lower())],
    # ExportBOOK2ToCSV.pas: every note (the Notes checklist)
    "BOOK": [("notes", lambda fid, ed, r: _is_note_book(r))],
}
PLACEMENT_HOLDERS = {"CONT", "ACTI", "FURN", "MSTT", "SCOL", "STAT", "NPC_", "LVLI"}


def _placement_set(sig, r):
    for slug, test in PLACEMENT_SETS[sig]:
        if test(r.fid, r.edid, r):
            return slug
    return ""


def _placement_targets(sig):
    return lambda esm: {r.fid for r in esm.records[sig] if _placement_set(sig, r)}


misc2_ref_targets = _placement_targets("MISC")
acti2_ref_targets = _placement_targets("ACTI")
book2_ref_targets = _placement_targets("BOOK")


def _set_placements(esm, sig):
    """worldspace / cell are EditorIDs; loc_name is the cell's name (CellFULL), else the
    worldspace's name or EditorID (WRLDName), else the cell's EditorID (CellEDID) -
    the .pas rule. Older xEdit runs left these blank (it never found the cell); the
    .pas now finds it with RefCell / CellWorld. Filled since Duchess's call, 21 Oct 2026."""
    import esm_refs
    out = ["set\titem_formid\tedid\tname\tsection\tworldspace\tcell\tx\ty\tz\tref_formid\tholder\tloc_name\tloc_source"]
    esm.world_record(0)
    scanned = getattr(esm, "_placement_scanned", set())
    esm._placement_scanned = scanned
    rows = []

    def refs_of(fid):
        if fid not in esm.refby and fid not in scanned:
            # a holder found on the way: work out its Referenced By now (rare; one extra scan)
            more, info = esm_refs.referenced_by(esm, {fid})
            esm.refby.update(more)
            esm.refinfo.update(info)
            scanned.add(fid)
        return esm.refby.get(fid, ())

    def prefetch(fids):
        # holders found on the way, a whole level at a time: one extra scan per level
        need = {f for f in fids if f not in esm.refby and f not in scanned}
        if need:
            more, info = esm_refs.referenced_by(esm, need)
            esm.refby.update(more)
            esm.refinfo.update(info)
            scanned.update(need)

    level = {r.fid for r in esm.records[sig] if _placement_set(sig, r)}
    scanned.update(level)   # the main scan covered them (a record with no referrers has no refby entry)
    for depth in (1, 2):
        level = {rf for t in level for rf in esm.refby.get(t, ())
                 if rf in esm.refinfo and esm.refinfo[rf][0] in PLACEMENT_HOLDERS}
        t0 = time.time()
        prefetch(level)
        if level:
            print("  %s: Referenced By for %d holders in %.0fs" % (sig, len(level), time.time() - t0))

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
                        rows.append((rf, [slug, fid, ed, full, sig, "", "", x, y, z,
                                          "%08X" % rf, holder, "", ""]))
                        continue
                if depth < 2 and rs in PLACEMENT_HOLDERS:
                    walk(rf, holder or esm.refinfo[rf][1], depth + 1)

        walk(r.fid, "", 0)
    pl = _Places(esm, {rf for rf, _c in rows})
    for rf, c in rows:
        we, _wf, ce, _cf, cn, _le, _ln = pl.cols(rf)
        c[5], c[6] = we, ce
        if cn:
            c[12], c[13] = cn, "CellFULL"
        elif we:
            c[12], c[13] = (pl.world_name(rf) or we), "WRLDName"
        elif ce:
            c[12], c[13] = ce, "CellEDID"
        out.append("\t".join(c))
    return out


def exp_misc2(esm):
    """ExportMISC2ToCSV.pas - placements of the MISC collectable sets."""
    return _set_placements(esm, "MISC")


def exp_acti2(esm):
    """ExportACTI2ToCSV.pas - placements of the ACTI sets (Treasure Map dig mounds)."""
    return _set_placements(esm, "ACTI")


def exp_book2(esm):
    """ExportBOOK2ToCSV.pas - placements of every note (the Notes checklist)."""
    return _set_placements(esm, "BOOK")


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


def _scriptable(esm, sig):
    """The parts the ACTI and FURN .pas share, per record in xEdit order: FormID, EDID,
    FULL, XALG flags, keywords (FormID:EDID:SIG, sorted), PRPS (AV EDID, value, curve),
    VMAD script count + packed scripts (Script::Prop=Value##...), and Referenced By.
    Returns ([(record, fid, ed, full, xalg, kws, props, vcount, vmad)], [(fid, ed, refs)],
    link_cell) - link_cell gives FormID:EDID:SIG of anything that resolves, else None."""
    esm.world_record(0)
    dial = _dial_index(esm)

    def link_cell(fid):
        """FormID:EDID:Sig of a record that resolves, else None."""
        if not fid:
            return None
        if fid in esm.index:
            sg, ed = esm.index[fid][:2]
            return "%08X:%s:%s" % (fid, _pas_clean(ed), sg)
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
    for r in R.xedit_order(esm, sig):
        fid = "%08X" % r.fid
        ed = _pas_clean(r.edid)
        full = _pas_clean(esm.lstring(r.full))
        xa = r.first("XALG")
        xalg = ""
        if xa:
            bits = struct.unpack("<Q", xa[:8].ljust(8, b"\0"))[0]
            xalg = "|".join(XALG_FLAGS[i] for i in range(32) if bits >> i & 1)
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
        rows.append((r, fid, ed, full, xalg, kws, props, vcount, "##".join(entries)))
        refs = ["%08X:%s:%s" % (x, _pas_clean(esm.refinfo[x][1]), esm.refinfo[x][0])
                for x in esm.refby.get(r.fid, ())]
        refrows.append((fid, ed, refs))
    return rows, refrows, link_cell


def _scriptable_files(rows, refrows, sig, mid_cols, mid, tail_cols, tail):
    """Write the _<SIG> and _Refs files: FormID / EDID / FULL / XALG_Flags, mid columns,
    keywords, properties, VMAD, tail columns. mid(row) / tail(row) give the values."""
    mk = max((len(x[5]) for x in rows), default=0)
    mp = max((len(x[6]) for x in rows), default=0)
    head = [sig + "_FormID", sig + "_EDID", sig + "_FULL", "XALG_Flags"] + mid_cols + ["KeywordCount"]
    head += ["KW_%d" % i for i in range(1, mk + 1)] + ["PropCount"]
    for i in range(1, mp + 1):
        head += ["Prop_%d_AV" % i, "Prop_%d_Val" % i, "Prop_%d_Curve" % i]
    head += ["VMAD_ScriptCount", "VMAD_Scripts"] + tail_cols
    out = ["\t".join(head)]
    for row in rows:
        _r, fid, ed, full, xalg, kws, props, vc, vm = row
        line = [fid, ed, full, xalg] + mid(row) + [str(len(kws))] + kws + [""] * (mk - len(kws)) + [str(len(props))]
        for p in props:
            line += list(p)
        line += ["", "", ""] * (mp - len(props))
        line += [str(vc), vm] + tail(row)
        out.append("\t".join(line))
    mr = max((len(x[2]) for x in refrows), default=0)
    rout = ["\t".join([sig + "_FormID", sig + "_EDID", "ReferencedByCount"] + ["Ref_%d" % i for i in range(1, mr + 1)])]
    for fid, ed, refs in refrows:
        rout.append("\t".join([fid, ed, str(len(refs))] + refs + [""] * (mr - len(refs))))
    return {"_" + sig: out, "_Refs": rout}


def exp_acti(esm):
    """ExportACTIToTSV.pas - _ACTI (flags, keywords, properties, scripts) and _Refs."""
    rows, refrows, _lc = _scriptable(esm, "ACTI")

    def aiid(row):
        ai = row[0].first("AIID")
        return ["" if ai is None else ("True" if ai[0] else "False")]
    return _scriptable_files(rows, refrows, "ACTI", ["CanBeInstanced"], aiid, [], lambda row: [])


# ------------------------------------------------------------- FURN ------
FURN_BENCH = {0: "None", 1: "Create Object", 2: "Weapons", 3: "Enchanting (unused)",
              4: "Enchanting Experiment (unused)", 5: "Alchemy", 6: "Alchemy Experiment (unused)",
              7: "Armor", 8: "Power Armor", 9: "Robot Mod"}
FURN_MNAM = (["Interaction Point %d" % i for i in range(22)]
             + ["Allow Awake Sound", "Enter With Weapon Drawn", "Play Anim When Full", "Disables Activation",
                "Is Perch", "Must Exit to Talk", "Use Static Avoid Node", "Unknown 29", "Has Model?",
                "Is Sleep Furniture"])
FURN_FNPR_TYPE = {0: "Unknown 0", 1: "Sit", 2: "Lay", 4: "Lean"}
FURN_ENTRY = ["Front", "Behind", "Right", "Left", "Up"]
_FURN_WARNED = set()


def _furn_warn(what):
    if what not in _FURN_WARNED:
        _FURN_WARNED.add(what)
        print("  FURN: %s has no xEdit name yet - check that row against an xEdit run" % what)


def exp_furn(esm):
    """!!!Wordpress - ExportFURNToTSV.pas (v4) - _FURN and _Refs. The ACTI columns
    (no CanBeInstanced) plus KNAM, WBDT (Bench Type / Uses Skill), MNAM flags, NAM1,
    FNPR marker parameters, Markers (ENAM / NAM0 / FNMK) and XMRK at the end, written
    the .pas way: struct fields Name=Value;..., array entries ##, set flags |."""
    rows, refrows, link_cell = _scriptable(esm, "FURN")

    def leaf_link(fid):
        if fid is None:
            return ""
        lc = link_cell(fid)
        return lc if lc is not None else _vmad_clean(esm.link_edit_value(fid))

    def tail(row):
        r = row[0]
        kn = r.first("KNAM")
        knam = leaf_link(_u32(kn)) if kn and len(kn) >= 4 else ""
        wb = r.first("WBDT")
        wbdt = ""
        if wb:
            parts = []
            if len(wb) >= 1:
                bt = FURN_BENCH.get(wb[0])
                if bt is None:
                    _furn_warn("bench type %d" % wb[0]); bt = "<Unknown: %d>" % wb[0]
                parts.append("Bench Type=" + bt)
            if len(wb) >= 2:
                parts.append("Uses Skill=" + ("True" if wb[1] else "False"))
            wbdt = ";".join(parts)
        mn = r.first("MNAM")
        mnam = ""
        if mn:
            v = _u32(mn.ljust(4, b"\0"))
            mnam = "|".join(FURN_MNAM[i] for i in range(32) if v >> i & 1) if v else _flag_bits(0)
        n1 = r.first("NAM1")
        nam1 = leaf_link(_u32(n1)) if n1 and len(n1) >= 4 else ""
        fn = []
        for d in r.all("FNPR"):
            t, ep = struct.unpack_from("<HH", d)
            tn = FURN_FNPR_TYPE.get(t)
            if tn is None:
                _furn_warn("FNPR type %d" % t); tn = "<Unknown: %d>" % t
            if ep:
                if ep >> len(FURN_ENTRY):
                    _furn_warn("FNPR entry point bits %X" % ep)
                eps = ",".join("%s:1" % FURN_ENTRY[i] for i in range(len(FURN_ENTRY)) if ep >> i & 1)
            else:
                eps = _flag_bits(0)
            fn.append("Type=%s;Entry Points=%s" % (tn, eps))
        # Markers: ENAM starts each entry; NAM0 (2 unknown bytes + Disabled Points flags), FNMK keyword
        marks, cur = [], None
        for k, d in r.subs:
            if k == "ENAM":
                cur = ["ENAM=%d" % struct.unpack_from("<i", d)[0]]
                marks.append(cur)
            elif k == "NAM0" and cur is not None and len(d) >= 4:
                dp = struct.unpack_from("<H", d, 2)[0]
                cur.append("NAM0=Unknown:%s,Disabled Points:%s" % (d[:2].hex(" ").upper(), _flag_bits(dp)))
            elif k == "FNMK" and cur is not None and len(d) >= 4:
                cur.append("FNMK=" + leaf_link(_u32(d)))
        xm = r.first("XMRK")
        xmrk = _vmad_clean(xm.rstrip(b"\0").decode("utf-8", "replace")) if xm else ""
        return [knam, wbdt, mnam, nam1, "##".join(fn), "##".join(";".join(m) for m in marks), xmrk]

    return _scriptable_files(rows, refrows, "FURN", [], lambda row: [],
                             ["KNAM_InteractionKeyword", "WBDT_BenchType", "MNAM_Flags", "NAM1_AssociatedForm",
                              "FNPR_MarkerParameters", "Markers", "XMRK_MarkerModel"], tail)


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


def _world_recs(esm, fids):
    """{fid: Record} for many CELL / WRLD / REFR records, read in one pass (sorted by
    offset) like _ref_positions - per-record opens are far too slow on the PC."""
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
            out[f] = R.Record(hdr[:4].decode(), f, R._parse_subs(data), struct.unpack_from("<I", hdr, 8)[0])
    return out


class _Places:
    """Worldspace, cell and the cell's Location for placed refs - the columns the
    REFR placements .pas means to fill (it now finds the cell with RefCell /
    CellWorld, as the NPC2 vendor .pas does). Duchess's call, 21 Oct 2026: fill them.
    cols(rf) -> (WorldspaceEDID, WorldspaceFormID, CellEDID, CellFormID,
                 CellName, LocationEDID, LocationName)"""

    def __init__(self, esm, refs):
        self.esm = esm
        esm.world_record(0)
        w = esm._world
        cells = {w[r][2] for r in refs if r in w and w[r][2]}
        wrlds = {w[r][3] for r in refs if r in w and w[r][3]}
        self.recs = _world_recs(esm, cells | wrlds)

    def cols(self, rf):
        esm = self.esm
        hit = esm._world.get(rf)
        if not hit:
            return ("",) * 7
        _sig, _off, cell, wrld = hit
        we = wf = ce = cf = cn = le = ln = ""
        if wrld:
            wr = self.recs.get(wrld)
            we, wf = cc(wr.edid if wr else ""), "%08X" % wrld
        if cell:
            cr = self.recs.get(cell)
            ce, cf = cc(cr.edid if cr else ""), "%08X" % cell
            if cr:
                cn = cc(esm.lstring(cr.first("FULL")))
                xl = _u32(cr.first("XLCN"))
                if xl and esm.exists(xl):
                    le, ln = cc(esm.edid_of(xl)), cc(esm.full_of(xl))
        return we, wf, ce, cf, cn, le, ln

    def world_name(self, rf):
        hit = self.esm._world.get(rf)
        wr = self.recs.get(hit[3]) if hit and hit[3] else None
        return cc(self.esm.lstring(wr.first("FULL"))) if wr else ""


REFRLVLI_PART_BYTES = 40000000   # the .pas MAX_PART_BYTES: keeps each file well under GitHub's 50 MB warning


def exp_refrlvli(esm):
    """ExportREFRPlacementsToTSV.pas run over the whole Leveled Item group: every
    placed reference (REFR) of a leveled list - the LPI_ "lying in the world" spots.
    Worldspace / cell / location columns are filled (_Places). The .pas also carries over rows for lists missing
    from the run; a full run here covers every list, so nothing is carried over."""
    import esm_refs
    targets = {r.fid for r in esm.records["LVLI"]}
    refby, refinfo = esm_refs.referenced_by(esm, targets, referrers={"REFR"})
    head = ("BaseFormID\tBaseEDID\tBaseName\tRefFormID\tRefEDID\tWorldspaceEDID\tWorldspaceFormID\t"
            "CellEDID\tCellFormID\tX\tY\tZ\tRotZ\tCellName\tLocationEDID\tLocationName")
    recs = R.xedit_order(esm, "LVLI")
    allrefs = {rf for v in refby.values() for rf in v}
    pos = _ref_positions(esm, allrefs)
    pl = _Places(esm, allrefs)
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
            we, wf, ce, cf, cn, le, ln = pl.cols(rf)
            rows.append("\t".join([bf, be, bn, "%08X" % rf, cc(refinfo[rf][1]), we, wf, ce, cf,
                                   x, y, z, rz, cn, le, ln]))
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


# ------------------------------------------------------- REFR_Placements --
# Base records whose placements the site reads (see the .pas header): the Pint-Sized
# Slasher masks, the Disturbed Graves, the U-Mine-It dig sites, the Brain in a Jar
# collectables (Skyline Valley) and the treasure-map mounds (every ACTI named
# TreasureMapMoundActivatorNN). Any other base already in
# a REFR_Placements file in the output folder is refreshed too, and --bases adds more.
REFR_BASES = [0x008E069E, 0x008F1672, 0x000355A1, 0x008ABB18]
REFR_BASE_EDID_RE = re.compile(r"^TreasureMapMoundActivator\d+$")


def _refr_bases(esm):
    import glob as _glob
    want = list(REFR_BASES)
    want += sorted(f for f, v in esm.index.items() if v[0] == "ACTI" and REFR_BASE_EDID_RE.match(v[1] or ""))
    for path in sorted(_glob.glob(os.path.join(getattr(esm, "out_dir", "") or ".", "REFR_Placements_*.tsv"))):
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                next(fh, None)
                for ln in fh:
                    c = ln.split("\t", 1)[0].strip()
                    if re.fullmatch(r"[0-9A-Fa-f]{8}", c):
                        want.append(int(c, 16))
        except OSError:
            pass
    want += list(getattr(esm, "extra_bases", ()) or ())
    seen, out = set(), []
    for f in want:
        if f not in seen and esm.exists(f):
            seen.add(f)
            out.append(f)
    return out


def exp_refr_placements(esm):
    """ExportREFRPlacementsToTSV.pas, per-base file REFR_Placements_<Month>_<Year>.tsv:
    one row per placed reference (REFR) of each base record in _refr_bases, with its
    worldspace, cell and the cell's Location (_Places). The .pas merges one run per
    base into the month's file; here every base is exported in one go, so the file
    is written whole. Coordinates: correctly rounded, as REFRLVLI."""
    import esm_refs
    bases = _refr_bases(esm)
    refby, refinfo = esm_refs.referenced_by(esm, set(bases), referrers={"REFR"})
    head = ("BaseFormID\tBaseEDID\tBaseName\tRefFormID\tRefEDID\tWorldspaceEDID\tWorldspaceFormID\t"
            "CellEDID\tCellFormID\tX\tY\tZ\tRotZ\tCellName\tLocationEDID\tLocationName")
    allrefs = {rf for v in refby.values() for rf in v}
    pos = _ref_positions(esm, allrefs)
    pl = _Places(esm, allrefs)
    out = [head]
    for b in bases:
        bf, be, bn = "%08X" % b, cc(esm.edid_of(b)), cc(esm.full_of(b))
        for rf in refby.get(b, ()):
            if refinfo[rf][0] != "REFR":
                continue
            x = y = z = rz = ""
            d = pos.get(rf)
            if d and len(d) >= 24:
                px, py, pz, _a, _b, rzr = struct.unpack_from("<6f", d)
                x, y, z = C.fmt_float(px), C.fmt_float(py), C.fmt_float(pz)
                rz = _rot_deg(rzr)
            we, wf, ce, cf, cn, le, ln = pl.cols(rf)
            out.append("\t".join([bf, be, bn, "%08X" % rf, cc(refinfo[rf][1]), we, wf, ce, cf,
                                   x, y, z, rz, cn, le, ln]))
    return out


# ------------------------------------------------------------- TERM _Placements --
def _any_edid(esm, fid):
    if not fid:
        return ""
    if esm.exists(fid):
        return esm.edid_of(fid) or ""
    w = esm.world_record(fid)
    return (w[1].edid or "") if w else ""


def exp_term_placements(esm):
    """ExportTERMToTSV.pas, _Placements file: where the token-exchange terminals
    stand (EDID has "Redeem", or "Exchange" and "Terminal"). One row per placed
    REFR of the terminal itself, or of a terminal whose menu opens it (Mr. Fuzzy's
    prize list is a sub-menu of the Camden Park machine). Location = the first
    LCTN that references the placement, else its cell's XLCN.
    The main TERM file and _Quests are tools/export_term_from_esm.py."""
    import esm_refs
    exch = [r for r in R.xedit_order(esm, "TERM")
            if "Redeem" in (r.edid or "") or ("Exchange" in (r.edid or "") and "Terminal" in (r.edid or ""))]
    ex_ids = {r.fid for r in exch}
    pats = {struct.pack("<I", f) for f in ex_ids}
    openers = {t.fid for t in esm.records["TERM"]
               if t.fid not in ex_ids and any(p in v for _k, v in t.subs for p in pats)}
    refby, refinfo = esm_refs.referenced_by(esm, ex_ids | openers, referrers={"REFR", "TERM"})
    rows = []          # (term, placed, ref)
    for t in exch:
        for rb in refby.get(t.fid, ()):
            sig = refinfo[rb][0]
            if sig == "REFR":
                rows.append((t.fid, t.fid, rb))
            elif sig == "TERM":
                for rr in refby.get(rb, ()):
                    if refinfo[rr][0] == "REFR":
                        rows.append((t.fid, rb, rr))
    refs = {r for _t, _p, r in rows}
    pos = _ref_positions(esm, refs)
    locby, _li = esm_refs.referenced_by(esm, refs, referrers={"LCTN"}) if refs else ({}, {})
    out = ["TERM_FormID\tTERM_EDID\tPlaced_FormID\tPlaced_EDID\tRef_FormID\tWorldspace_EDID\tCell_EDID\t"
           "X\tY\tZ\tLocation_FormID\tLocation_EDID"]
    for t, pl, rf in rows:
        hit = esm._world.get(rf) if esm._world else None
        cell, wrld = (hit[2], hit[3]) if hit else (0, 0)
        x = y = z = ""
        d = pos.get(rf)
        if d and len(d) >= 12:
            px, py, pz = struct.unpack_from("<3f", d)
            # exact value, correctly rounded - xEdit's own text can differ by
            # 0.000001 in the last place (as for NPC2 / MISC2 / ACTI2 / REFRLVLI)
            x, y, z = C.fmt_float(px), C.fmt_float(py), C.fmt_float(pz)
        loc = next(iter(locby.get(rf, ())), 0)
        if not loc and cell:
            cw = esm.world_record(cell)
            xl = cw[1].first("XLCN") if cw else None
            loc = struct.unpack_from("<I", xl, 0)[0] if xl and len(xl) >= 4 else 0
        out.append("\t".join(["%08X" % t, cc(esm.edid_of(t)), "%08X" % pl, cc(esm.edid_of(pl)), "%08X" % rf,
                              cc(_any_edid(esm, wrld)), cc(_any_edid(esm, cell)), x, y, z,
                              ("%08X" % loc) if loc else "", cc(_any_edid(esm, loc))]))
    return {"_Placements": out}


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
    here instead of "not in the game files".
    Manual backup: "!!!Wordpress - ExportQUST2ToTSV.pas" writes the same
    _ScriptProps file, same columns (CollectScriptProps, 25 Oct 2026)."""
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


# ------------------------------------------------------------- QUST2 tree --
# ExportQUST2ToTSV.pas walks xEdit's element tree. The same tree from the raw
# subrecords:  stages (INDX, then one QSDT per log entry), objectives (QOBJ,
# then a QSTA per target), aliases (ALST / ALLS ... ALED; a bare ALCS + ALMI
# is its own element - the collection marker for the alias after it).
_QLOG_SUBS = {"QSDT", "CTDA", "CIS1", "CIS2", "NAM2", "SCFC", "CNAM", "NAM0", "QRWD", "QUIM",
              "QRWT", "QQSK", "QQSV"}
_QOBJ_SUBS = {"FNAM", "QOTM", "SNAM", "NAM1", "SCCM", "PBLT", "PBRT", "NNAM"}


def _qust_tree(r):
    head, stages, objs, aliases = [], [], [], []
    st = log = ob = tg = al = None
    mode = "head"
    for k, v in r.subs:
        if k == "INDX":
            mode, st, log = "stage", {"INDX": v, "logs": []}, None
            stages.append(st)
            continue
        if k == "QOBJ":
            mode, ob, tg = "obj", {"QOBJ": v, "subs": [], "targets": []}, None
            objs.append(ob)
            continue
        if k in ("ALST", "ALLS"):
            mode, al = "alias", {"kind": k, "id": v, "subs": [], "conds": [], "marker": False}
            aliases.append(al)
            continue
        if k == "ALCS":
            mode = "aliases"
            aliases.append({"kind": k, "id": v, "subs": [], "conds": [], "marker": True})
            continue
        if k == "ANAM" and mode != "alias":
            mode = "aliases"
            head.append((k, v))
            continue
        if mode == "stage" and k in _QLOG_SUBS:
            if k == "QSDT" or log is None:
                log = {"subs": [], "conds": []}
                st["logs"].append(log)
            if k == "CTDA":
                log["conds"].append([v, None, None])
            elif k in ("CIS1", "CIS2") and log["conds"]:
                log["conds"][-1][1 if k == "CIS1" else 2] = v.rstrip(b"\0").decode("utf-8", "replace")
            else:
                log["subs"].append((k, v))
            continue
        if mode == "obj":
            if k == "QOST":
                # new objective field (PTS build 211, Oct 2026: one after every FNAM).
                # Not exported; skipped so the targets after it are still read.
                continue
            if k == "QSTA":
                tg = {"QSTA": v, "conds": [], "subs": []}
                ob["targets"].append(tg)
                continue
            if tg is not None and k in ("CTDA", "CIS1", "CIS2"):
                if k == "CTDA":
                    tg["conds"].append([v, None, None])
                elif tg["conds"]:
                    tg["conds"][-1][1 if k == "CIS1" else 2] = v.rstrip(b"\0").decode("utf-8", "replace")
                continue
            if k in _QOBJ_SUBS and tg is None:
                ob["subs"].append((k, v))
                continue
            if tg is not None and k in _QOBJ_SUBS:
                tg["subs"].append((k, v))
                continue
            mode = "tail"
        if mode == "alias":
            if k == "ALED":
                mode = "aliases"
                continue
            if k == "CTDA":
                al["conds"].append([v, None, None])
            elif k in ("CIS1", "CIS2") and al["conds"]:
                al["conds"][-1][1 if k == "CIS1" else 2] = v.rstrip(b"\0").decode("utf-8", "replace")
            else:
                al["subs"].append((k, v))
            continue
        if mode == "aliases" and k == "ALMI" and aliases and aliases[-1]["marker"]:
            aliases[-1]["subs"].append((k, v))
            continue
        head.append((k, v))
        if mode not in ("head",):
            mode = "tail"
    return {"head": head, "stages": stages, "objs": objs, "aliases": aliases}


# ------------------------------------------------------------- QUST2 -------
# The other five ExportQUST2ToTSV.pas files: _Quests, _Stages, _Objectives,
# _Aliases (quests whose name starts with an event prefix) and _ItemAliases
# (every alias that creates an item, every quest).
# Flag names and link labels were learned from the October 2026 xEdit files
# (tools learned them the way xedit_ref_fields.json was learned) and checked
# against xEdit's wbDefinitionsFO76 for the bits no exported quest uses.
_Q2_PREFIXES = [("Enclave Activity:", "Enclave Activity"), ("Activity:", "Activity"),
                ("Event:", "Event"), ("Daily Op:", "Daily Op"), ("Uplink:", "Daily Op"),
                ("Decryption:", "Daily Op"), ("Daily:", "Daily"), ("Expedition:", "Expedition"),
                ("Atlantic City:", "Expedition"), ("The Pitt:", "Expedition")]
_Q2_QUEST_FLAGS = {0: "Start Game Enabled", 1: "Completed", 2: "Add Idle Topic To Hello",
                   3: "Allow Repeat Stages", 4: "Starts Enabled", 5: "Displayed In HUD", 6: "Failed",
                   7: "Stage Wait", 8: "Run Once", 9: "Exclude from dialogue export",
                   10: "Warn on Alias Fill Failed", 11: "Tracked By UI", 12: "Repeats Conditions",
                   13: "Keep Instance Data", 14: "Want Dormant", 15: "Has Dialogue Data",
                   16: "Unique Instance", 17: "Sharable", 18: "Always Tracked", 19: "Holotape Only",
                   20: "Saves Completion",  # 21 'Unused 21': xEdit hides it
                   22: "Unique Per Player", 23: "Can Be Rejected When Active",
                   24: "Uses Custom Expire Time", 25: "Not Autotracked", 26: "Disable Event Announcement",
                   27: "Kick All Players From Instance on Shutdown", 28: "Disable Greeting Precheck",
                   29: "Incrementing Timer", 30: "Is Random Encounter",
                   31: "Not Tracked In UI After Completed"}
_Q2_STAGE_FLAGS = {0: "Done", 1: "Run On Start", 2: "Run On Stop", 3: "Keep Instance Data From Here On",
                   4: "Timer End", 5: "Start Timer", 6: "Checkpoint"}
_Q2_LOG_FLAGS = {0: "Complete Quest", 1: "Fail Quest"}
_Q2_OBJ_FLAGS = {0: "ORed With Previous", 1: "No Stats Tracking", 2: "Unknown 3", 3: "Uses Timer"}
_Q2_ALIAS_FLAGS = dict(enumerate(
    ["Reserves Location/Reference", "Optional", "Quest Object", "Allow Reuse in Quest", "Allow Dead",
     "Matching Ref - In Loaded Area", "Essential", "Allow Disabled", "Stores Text", "Allow Reserved",
     "Protected", "Forced by Aliases", "Allow Destroyed", "Matching Ref - Closest", "Uses Stored Text",
     "Initially Disabled", "Allow Cleared", "Clear Names When Removed", "Matching Ref - Actors Only",
     "Create Ref - Temp", "External Alias - Linked", "No Pickpocket",
     "Can Apply Data To Non-Aliased Refs", "Is Companion", "Optional All Scenes"]))
_Q2_ALIAS_FLAGS.update({b: "Unknown %d" % b for b in range(25, 42)})
_Q2_QUEST_TYPES = ["None", "Primary", "Secondary", "Side Quest", "Server", "Daily", "Public Event",
                   "Miscellaneous", "Event", "Daily Ops", "Expedition", "Module", "Caravan", "Raid"]
# fields whose FormID xEdit's CollectLinks writes as TAG=link (learned)
_Q2_LOG_LINKS = ("NAM0", "QRWD", "QUIM")
_Q2_ALIAS_LINKS = {"ALFR", "ALUA", "ALRT", "ALFL", "ALFC", "ALPC", "ALCO", "ALSP", "ALFF", "ALFV",
                   "ALDN", "VTCK", "ECOR", "ALEQ", "ALDI", "NAM7"}
_Q2_MAX_LINKS, _Q2_MAX_CONDS, _Q2_MAX_GMRW = 16, 16, 8


def _q2_flags(v, names):
    """FlagsNamed: the names of the set bits xEdit defines, ' ; '-joined; with
    none of them set but some bit set, the raw bit string (objective bit 4)."""
    if not v:
        return ""
    out = [names[b] for b in range(64) if v >> b & 1 and b in names]
    return " ; ".join(out) if out else _xedit_bits(v)


def _q2_cond(ctx, ctda, cis1, cis2):
    """QUST2 .pas BuildCondFromCTDA: Type|Comp|Function|<other non-empty elements>|RunOn|RefLink.
    Alias parameters / Quest Alias read like xEdit ("002 Player") - Ctx.alias_names."""
    els, ref = C.elements(ctx, ctda, cis1, cis2)
    d, params = {}, []
    for name, val in els:
        low = name.lower()
        val = cc(val)
        if low in ("type", "comparison value - float", "comparison value - global", "function", "run on"):
            d[low] = val
            continue
        if val == "":
            continue
        params.append(val)
    comp = d.get("comparison value - float", d.get("comparison value - global", ""))
    esm = ctx.esm
    refcell = "%08X:%s:%s" % (ref, esm.edid_of(ref), esm.sig_of(ref)) if ref and esm.exists(ref) else ""
    return "|".join([d["type"], comp, d["function"], "|".join(params), d["run on"], cc(refcell)])


def _q2_conds(ctx, conds):
    return " ; ".join(c for c in (_q2_cond(ctx, *x) for x in conds[:_Q2_MAX_CONDS]) if c)


def _q2_linkcell(esm, fid):
    if not fid:
        return ""
    if esm.exists(fid):
        return "%08X:%s:%s" % (fid, esm.edid_of(fid), esm.sig_of(fid))
    w = esm.world_record(fid)          # placed references (ALFR) live in the world data
    if w is not None:
        return "%08X:%s:%s" % (fid, w[1].edid or "", w[0])
    return ""


_Q2_SORTED = {"ALFC"}   # xEdit sorts this repeated field by FormID (ALPC stays in file order)


def _q2_links(esm, subs, fields, arrays=()):
    # runs of a sorted repeated field come out in FormID order
    subs = list(subs)
    i = 0
    while i < len(subs):
        j = i
        while j < len(subs) and subs[j][0] == subs[i][0]:
            j += 1
        if subs[i][0] in _Q2_SORTED and j - i > 1:
            subs[i:j] = sorted(subs[i:j], key=lambda kv: struct.unpack_from("<I", kv[1], 0)[0])
        i = j
    out = []
    for k, v in subs:
        if k in fields and len(v) >= 4:
            c = _q2_linkcell(esm, struct.unpack_from("<I", v, 0)[0])
            if c:
                out.append("%s=%s" % (k, c))
        elif k in arrays:
            stride = 8 if k == "ALLA" else 4
            fids = [struct.unpack_from("<I", v, o)[0] for o in range(0, len(v) - 3, stride)]
            if k == "KWDA":
                fids.sort()            # xEdit's Keywords array is sorted by FormID
            for f in fids:
                c = _q2_linkcell(esm, f)
                if c:
                    out.append("Keyword=" + c)
    return " ; ".join(cc(x) for x in out[:_Q2_MAX_LINKS])


def _q2_vmad(raw):
    """(script names in xEdit's sorted order, fragment script name, [(stage, index, fragment name)])."""
    if not raw:
        return [], "", []
    try:
        rd = _VmadReader(raw)
        scripts = rd.scripts()
        names = sorted((n for n, _p in scripts), key=_xedit_text_key)
    except Exception:                                     # noqa: BLE001
        return [], "", []
    try:
        frag_file, frags = "", []
        if rd.p < len(raw):
            rd.u8()
            n = rd.u16()
            frag_file = rd.s()
            if frag_file:
                rd.u8()
                fmt = struct.unpack_from("<h", raw, 2)[0]
                for _ in range(rd.u16()):
                    rd.s(); t = rd.u8(); rd.u8(); rd.value(t, fmt)
            for _ in range(n):
                st = rd.u32(); ix = rd.u32(); rd.u8(); rd.s()
                frags.append((st, ix, rd.s()))
        frags.sort(key=lambda f: (f[0], f[1]))
        return names, frag_file, frags
    except Exception:                                     # noqa: BLE001
        return names, "", []          # not a quest's fragment layout (INFO, SCEN)


def _q2_kind(full):
    low = full.lower()
    for pre, kind in _Q2_PREFIXES:
        if low.startswith(pre.lower()):
            return kind
    return ""


def _q2_alias_rows(esm, r, tree):
    """[(index, alias dict, id, name, kind)] for the real aliases, like Process()."""
    out, pending = [], ""
    for i, al in enumerate(tree["aliases"]):
        aid = str(struct.unpack_from("<I", al["id"], 0)[0]) if len(al["id"]) >= 4 else ""
        if al["marker"]:
            pending = aid
            continue
        kind = "Ref" if al["kind"] == "ALST" else "Loc"
        if pending:
            if pending == aid:
                kind = "RefCol"
            pending = ""
        name = cc(_str(dict(al["subs"]).get("ALID")))
        out.append((i, al, aid, name, kind))
    return out


def exp_qust2(esm):
    quests, stages, objs, aliases, items = [], [], [], [], []
    for r in R.xedit_order(esm, "QUST"):
        full = cc(esm.lstring(r.first("FULL")))
        tree = _qust_tree(r)
        qfid, ed = "%08X" % r.fid, cc(r.edid)
        ctx = C.Ctx(esm, r)
        arows = _q2_alias_rows(esm, r, tree)
        for i, al, aid, name, kind in arows:
            links = _q2_links(esm, al["subs"], _Q2_ALIAS_LINKS, ("KWDA", "ALLA"))
            if "ALCO=" not in links:
                continue
            fn = dict(al["subs"]).get("FNAM")
            items.append("\t".join([qfid, ed, full, str(i), name,
                                    _q2_flags(int.from_bytes(fn, "little") if fn else 0, _Q2_ALIAS_FLAGS),
                                    links]))
        kind = _q2_kind(full)
        if not kind:
            continue
        # xEdit's alias combo finds named aliases, not collections ("045")
        named = {int(aid): name for _i, _a, aid, name, k in arows if aid and name and k != "RefCol"}
        ctx = C.Ctx(esm, r, named)
        head = dict(tree["head"])
        names, frag_file, frags = _q2_vmad(head.get("VMAD"))
        qline = qfid + "\t" + ed + "\t" + full
        for i, al, aid, name, akind in arows:
            fn = dict(al["subs"]).get("FNAM")
            aliases.append("\t".join([qline, str(i), aid, name, "Alias", akind,
                                      _q2_flags(int.from_bytes(fn, "little") if fn else 0, _Q2_ALIAS_FLAGS),
                                      _q2_links(esm, al["subs"], _Q2_ALIAS_LINKS, ("KWDA", "ALLA")),
                                      _q2_conds(ctx, al["conds"]), ""]))
        alias_names = {int(aid): name for _i, _a, aid, name, _k in arows if aid and name}
        for st in sorted(tree["stages"], key=lambda s: struct.unpack_from("<H", s["INDX"], 0)[0]):
            sidx = struct.unpack_from("<H", st["INDX"], 0)[0]
            sfl = _q2_flags(st["INDX"][2] if len(st["INDX"]) > 2 else 0, _Q2_STAGE_FLAGS)
            fr = " ; ".join(f[2] or "Stage %d Item %d" % (f[0], f[1]) for f in frags if f[0] == sidx)
            if not st["logs"]:
                stages.append("\t".join([qline, str(sidx), sfl, "", "", "", "", fr, "", ""]))
                continue
            for j, lg in enumerate(st["logs"]):
                ls = dict(lg["subs"])
                q = ls.get("QSDT")
                nq = ls.get("NAM0")
                stages.append("\t".join([qline, str(sidx), sfl, str(j),
                                         _q2_flags(q[0] if q else 0, _Q2_LOG_FLAGS),
                                         cc(esm.lstring(ls.get("CNAM"))),
                                         _q2_linkcell(esm, struct.unpack_from("<I", nq, 0)[0]) if nq else "",
                                         fr, _q2_conds(ctx, lg["conds"]),
                                         _q2_links(esm, lg["subs"], _Q2_LOG_LINKS)]))
        for ob in tree["objs"]:
            os_ = dict(ob["subs"])
            oidx = str(struct.unpack_from("<H", ob["QOBJ"], 0)[0])
            fn = os_.get("FNAM")
            ofl = _q2_flags(struct.unpack_from("<I", fn, 0)[0] if fn else 0, _Q2_OBJ_FLAGS)
            otext = cc(esm.lstring(os_.get("NNAM")))
            if not ob["targets"]:
                objs.append("\t".join([qline, oidx, ofl, otext, "", "", "", "", "", ""]))
                continue
            for k, tg in enumerate(ob["targets"]):
                a = struct.unpack_from("<i", tg["QSTA"], 0)[0]
                an = alias_names.get(a, "")
                aid = ctx.alias_text(a)
                objs.append("\t".join([qline, oidx, ofl, otext, str(k), aid, an, "",
                                       _q2_conds(ctx, tg["conds"]), ""]))
        loc = head.get("LNAM")
        gm = [c for c in _refcells(esm, r.fid) if c.endswith(":GMRW")][:_Q2_MAX_GMRW]
        dat = head.get("DATA") or b""
        qt = dat[16] if len(dat) > 16 else None
        flags_v = int.from_bytes(dat[:8], "little") if len(dat) >= 20 else (int.from_bytes(dat[:4], "little") if dat else 0)
        pri = dat[8] if len(dat) >= 20 else (dat[4] if len(dat) > 4 else None)
        quests.append("\t".join([qline, kind, cc(esm.lstring(head.get("DESC"))),
                                 (_Q2_QUEST_TYPES[qt] if qt is not None and qt < len(_Q2_QUEST_TYPES) else ("" if qt is None else str(qt))),
                                 _q2_flags(flags_v, _Q2_QUEST_FLAGS), "" if pri is None else str(pri),
                                 cc(esm.link_edit_value(struct.unpack_from("<I", loc, 0)[0])) if loc else "",
                                 str(len(tree["stages"])), str(len(tree["objs"])), str(len(arows)),
                                 " ; ".join(cc(n) for n in names if n), cc(frag_file), " ; ".join(gm),
                                 "SeventySix.esm"]))
    return {
        "_Quests": ["FormID\tEDID\tFULL\tKind\tDESC\tQuestType\tFlags\tPriority\tLocation\tStageCount\t"
                    "ObjectiveCount\tAliasCount\tScripts\tFragmentFile\tGMRWRefs\tSourceFile"] + quests,
        "_Stages": ["QuestFormID\tQuestEDID\tQuestName\tStageIndex\tStageFlags\tLogEntryIndex\tLogFlags\t"
                    "LogText\tNextQuest\tFragments\tConditions\tLinks"] + stages,
        "_Objectives": ["QuestFormID\tQuestEDID\tQuestName\tObjectiveIndex\tObjectiveFlags\tObjectiveText\t"
                        "TargetIndex\tTargetAliasID\tTargetAliasName\tTargetFlags\tTargetConditions\t"
                        "TargetLinks"] + objs,
        "_Aliases": ["QuestFormID\tQuestEDID\tQuestName\tAliasIndex\tAliasID\tAliasName\tAliasLabel\t"
                     "AliasKind\tAliasFlags\tFillLinks\tConditions\tScripts"] + aliases,
        "_ItemAliases": ["QuestFormID\tQuestEDID\tQuestName\tAliasIndex\tAliasName\tAliasFlags\tLinks"] + items,
    }


# ------------------------------------------------------------- LVLI ------
# ExportLVLIToCSV.pas: _LVLI_List, _LVLI_Entries, _LVLI_Refs, _LVLI_Math.
# Two record layouts exist in the game file:
#   old - LVLD (chance none byte) / LVLM, and a 12-byte LVLO holding level,
#         reference and count. xEdit's paths for LVOV/LVIV/LVLV find nothing
#         there, so those columns are blank, exactly like the .pas.
#   new - a 4-byte LVLO (reference) followed by its own LVOV / LVOG / LVOC /
#         LVOT / LVIV / LVIG / LVIT / LVLV / LVLG / LVLT / COED / CTDA.
# The MinLvl GLOBs in the LVOG column are real: Bethesda stores them in LVOG
# (see the MinLvl trap in rng76.py), not an export bug.
_LVLO_FIELDS = ("LVOV", "LVOG", "LVOC", "LVOT", "LVIV", "LVIG", "LVIT", "LVLV",
                "LVLG", "LVLT", "COED", "CTDA", "CIS1", "CIS2", "LVUD")


def _delphi_float(v):
    """Delphi FloatToStr: general format, 15 significant digits."""
    if v == 0:
        return "0"
    t = "%.15g" % v
    if "e" in t:
        m, e = t.split("e")
        n = int(e)
        t = "%sE%s%d" % (m, "-" if n < 0 else "", abs(n))
    return t


def _lvli_link(esm, raw):
    """GetLinkFromPath: FormID:EDID:SIG, or xEdit's edit value when it links to nothing."""
    if raw is None or len(raw) < 4:
        return ""
    fid = struct.unpack("<I", raw[:4])[0]
    if fid and esm.exists(fid):
        return "%08X:%s:%s" % (fid, esm.edid_of(fid), esm.sig_of(fid))
    # this .pas reads GetEditValue on the bare field: "<Error: ...>", no spaces
    return cc(esm.link_edit_value(fid)).replace("< Error: Could not be resolved >",
                                                 "<Error: Could not be resolved>")


def _lvli_float(raw):
    return C.fmt_float(struct.unpack("<f", raw[:4])[0]) if raw is not None and len(raw) >= 4 else ""


def _lvli_cond(ctx, ctda, cis1, cis2):
    """The LVLI .pas BuildCondFromCTDA: RunOn.Function(params) <Type> <Comparison>.
    Params are every other non-empty element except Reference (the slot after
    Run On, which xEdit names Reference) and the comparison value."""
    els, _ref = C.elements(ctx, ctda, cis1, cis2)
    d, params, after_run_on = {}, [], False
    for name, val in els:
        low = name.lower()
        if after_run_on:
            after_run_on = False
            continue                      # the Reference slot
        if low == "run on":
            d[low] = val
            after_run_on = True
            continue
        if low in ("type", "function") or low.startswith("comparison value"):
            d[low if not low.startswith("comparison value") else "cmp"] = val
            continue
        if val == "":
            continue
        params.append(val)
    run_on = d.get("run on", "")
    out = (run_on + "." if run_on else "") + d.get("function", "") + "(" + ", ".join(params) + ")"
    # OpFromType gets xEdit's 8-character Type bits, which match none of its
    # words, so it hands them back unchanged.
    return cc(out + " " + d.get("type", "") + " " + d.get("cmp", ""))


def _lvli_rand_chance(conds):
    """The .pas CondChanceFromRandomPercent, character for character."""
    p = 1.0
    for s in conds:
        s2 = s.lower()
        if "getrandompercent" not in s2:
            continue
        num = ""
        for ch in reversed(s2):
            if "0" <= ch <= "9":
                num = ch + num
            elif num:
                break
        if num:
            n = float(num)
            if ">=" in s2 or ")>" in s2 or "> " in s2:
                p *= (100.0 - n) / 100.0
            elif "<=" in s2 or "<" in s2:
                p *= n / 100.0
    return min(1.0, max(0.0, p))


def _lvli_glob_value(esm, cell):
    """ResolveGlobValue: the GLOB's FLTV edit value, '' when not a GLOB."""
    if not cell or cell.rsplit(":", 1)[-1] != "GLOB":
        return ""
    try:
        g = esm.get(int(cell.split(":")[0], 16))
    except ValueError:
        return ""
    f = g.first("FLTV") if g else None
    return C.fmt_float(struct.unpack("<f", f[:4])[0]) if f and len(f) >= 4 else ""


def _to_float(s):
    try:
        return float(s.strip())
    except (ValueError, AttributeError):
        return None


def exp_lvli(esm):
    lists, ents, refs, maths = [], [], [], []
    for r in R.xedit_order(esm, "LVLI"):
        fid = "%08X" % r.fid
        ed = cc(r.edid)
        ctx = C.Ctx(esm, r)
        head, entries, cur = {}, [], None
        list_conds = []
        for k, v in r.subs:
            if k == "LVLO":
                cur = {"LVLO": v, "CTDA": []}
                entries.append(cur)
            elif cur is not None and k in _LVLO_FIELDS:
                if k == "CTDA":
                    cur["CTDA"].append([v, None, None])
                elif k in ("CIS1", "CIS2"):
                    if cur["CTDA"]:
                        cur["CTDA"][-1][1 if k == "CIS1" else 2] = v.rstrip(b"\0").decode("utf-8", "replace")
                else:
                    cur.setdefault(k, v)
            elif cur is None and k in ("CTDA", "CIS1", "CIS2"):
                if k == "CTDA":
                    list_conds.append([v, None, None])
                elif k in ("CIS1", "CIS2") and list_conds:
                    list_conds[-1][1 if k == "CIS1" else 2] = v.rstrip(b"\0").decode("utf-8", "replace")
            else:
                # list-level fields, including the ones stored after the
                # entries (LVSG epic chance, DIQO, ENLT, MODL ...)
                head.setdefault(k, v)
        flags = head.get("LVLF")
        fv = int.from_bytes(flags, "little") if flags else None
        if fv is None:
            fstr = ""
        elif fv == 0:
            fstr = "0" * 64
        else:
            fstr = _xedit_bits(fv)
        llct = head.get("LLCT")
        lconds = [_lvli_cond(ctx, *c) for c in list_conds]
        lvcv = _lvli_float(head.get("LVCV"))
        lvlg = _lvli_link(esm, head.get("LVLG"))
        lvct = _lvli_link(esm, head.get("LVCT"))
        lvsg = head.get("LVSG")
        diqo = head.get("DIQO")
        lists.append(([fid, ed, cc(esm.lstring(r.first("FULL"))), fstr,
                       str(llct[0]) if llct else "", str(len(entries)),
                       lvcv, lvlg, lvct,
                       _lvli_float(head.get("LVMV")), _lvli_link(esm, head.get("LVMG")),
                       _lvli_link(esm, head.get("LVMT")), "",
                       cc(esm.link_edit_value(struct.unpack("<I", lvsg[:4])[0])) if lvsg and len(lvsg) >= 4 else "",
                       _lvli_link(esm, diqo), "", str(len(lconds))], lconds))
        rc = _refcells(esm, r.fid)
        refs.append(([fid, ed, str(len(rc))], rc))
        if not entries:
            continue
        lcn = _lvli_glob_value(esm, lvlg) if lvlg else ""
        if lcn == "" and lvcv != "":
            lcn = lvcv.strip()
        lcnv = _to_float(lcn)
        lcnv = min(100.0, max(0.0, lcnv if lcnv is not None else 0.0))
        lpres = 1.0 - lcnv / 100.0
        for i, e in enumerate(entries):
            lvlo = e["LVLO"]
            refraw = lvlo[4:8] if len(lvlo) >= 12 else lvlo[:4]
            ref = _lvli_link(esm, refraw)
            conds = [_lvli_cond(ctx, *c) for c in e["CTDA"]]
            cnv = _lvli_float(e.get("LVOV"))
            cng = _lvli_link(esm, e.get("LVOG"))
            cnc = _lvli_link(esm, e.get("LVOC"))
            ents.append(([fid, ed, str(i), ref, "", cnv, cng, cnc,
                          _lvli_float(e.get("LVIV")), _lvli_link(esm, e.get("LVIG")), _lvli_link(esm, e.get("LVIT")),
                          _lvli_float(e.get("LVLV")), _lvli_link(esm, e.get("LVLG")), _lvli_link(esm, e.get("LVLT")),
                          str(len(conds))], conds))
            ecn = _lvli_glob_value(esm, cng) if cng else ""
            if ecn == "" and cnv != "":
                ecn = cnv.strip()
            ecnv = _to_float(ecn)
            ecnv = min(100.0, max(0.0, ecnv if ecnv is not None else 0.0))
            cond_ch = _lvli_rand_chance(conds)
            rsig = ref.rsplit(":", 1)[-1] if ":" in ref else ""
            maths.append("\t".join([fid, ed, fstr, str(len(entries)), str(i), ref, rsig,
                                    ref.split(":")[0] if rsig == "LVLI" else "",
                                    lvcv, lvlg, lvct, lcn, cnv, cng, cnc, ecn,
                                    _delphi_float(cond_ch),
                                    _delphi_float(lpres * (1.0 - ecnv / 100.0)),
                                    _delphi_float(lpres * (1.0 - ecnv / 100.0) * cond_ch),
                                    _lvli_link(esm, e.get("LVOT"))]))
    math_head = ("LVLI_FormID\tLVLI_EDID\tLVLF_Flags\tEntryCount\tEntryIndex\tLVLO_Reference\tRefSig\t"
                 "SubLVLI_FormID\tListChanceNoneValue\tListChanceNoneGlobal\tListChanceNoneCurve\t"
                 "ListChanceNoneResolved\tEntryChanceNoneValue\tEntryChanceNoneGlobal\tEntryChanceNoneCurve\t"
                 "EntryChanceNoneResolved\tEntryCondChance_RandomPercent\tEntryPresenceChance\t"
                 "EntryAprioriChance_NoSublist\tEntryChanceNoneCurveTable")
    return {
        "_LVLI_List": _pad_conds(lists, ["LVLI_FormID", "LVLI_EDID", "LVLI_FULL", "LVLF_Flags", "LLCT_Count",
                                         "EntryCount", "LVCV_ChanceNoneValue", "LVLG_ChanceNoneGlobal",
                                         "LVCT_ChanceNoneCurve", "LVMV_MaxValue", "LVMG_MaxGlobal", "LVMT_MaxCurve",
                                         "LLKC_FilterKeywordChances", "LVSG_EpicLootChance", "DIQO_Quest",
                                         "LIMC_MarkerColor", "ListCondCount"], "ListCond"),
        "_LVLI_Entries": _pad_conds(ents, ["LVLI_FormID", "LVLI_EDID", "EntryIndex", "LVLO_Reference",
                                           "COED_ExtraData", "LVOV_ChanceNoneValue", "LVOG_ChanceNoneGlobal",
                                           "LVOC_ChanceNoneCurve", "LVIV_Quantity", "LVIG_QuantityGlobal",
                                           "LVIT_QuantityCurve", "LVLV_MinimumLevel", "LVLG_MinimumLevelGlobal",
                                           "LVLT_MinimumLevelCurve", "CondCount"]),
        "_LVLI_Refs": _pad_refs(refs, ["LVLI_FormID", "LVLI_EDID", "ReferencedByCount"]),
        "_LVLI_Math": [math_head] + maths,
    }


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


def _quest_children(esm):
    """{quest FormID: [(sig, fid, subs)]} in xEdit's ChildGroup order (the
    quest's own child group: DIAL topics and SCEN scenes as they sit in the
    file) and {DIAL FormID: [(INFO fid, subs)]} from each topic's child group."""
    import zlib as _zlib
    kids, infos = {}, {}
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
                if sig == b"INFO":
                    if gtype == 7:
                        infos.setdefault(gparent, []).append((fid, subs))
                elif gtype == 10:
                    kids.setdefault(gparent, []).append((sig.decode(), fid, subs))

        f.seek(0)
        h = f.read(24)
        f.seek(24 + struct.unpack("<I", h[4:8])[0])
        while f.tell() < size:
            g = f.read(24)
            gs = struct.unpack("<I", g[4:8])[0]
            st = f.tell() - 24
            if g[8:12] == b"QUST":
                walk(st + gs, 0, 0)
            f.seek(st + gs)
    return kids, infos


# ------------------------------------------------------------- ALLYDLG -----
# ExportCampAllyDialogToTSV.pas: _Quests, _Topics, _Lines, _Scenes for CAMP
# ally quests (the _Lore / _LoreNotes files are the LORE export above).
_AD_INFO_FLAGS = dict(enumerate(
    ["Start Scene on End", "Random", "Say Once", "Requires Player Activation", "Info Refusal",
     "Random End", "End Running Scene", "ForceGreet Hello", "Player Address", "Force Subtitle",
     "Can Move While Greeting", "No LIP File", "Requires post-processing", "Audio Output Override",
     "Has Capture", "Unknown 15", "Unknown 16", "Unknown 17", "Unknown 18", "Unknown 19", "Unknown 20"]))
_AD_SCENE_FLAGS = dict(enumerate(
    ["Begin on Quest Start", "Stop on Quest End", "Show All Text", "Repeat Conditions While True",
     "Interruptible", "Unknown 5", "Prevent Player Exit Dialogue", "Unknown 7", "Unknown 8", "Unknown 9",
     "Unknown 10", "Disable Dialogue Camera", "No Follower Idle Chatter", "Unknown 13", "Unknown 14"]))
_AD_CATEGORIES = ["Player", "Command", "Scene", "Combat", "Detection", "Miscellaneous", "Unknown 6", "Unknown 7"]
# wbDialogueSubtypeEnum. A name used by more than one code reads "Name@<code as int>"
# after its first use - xEdit's way of keeping enum values apart ("Custom@1313162067" = SCEN).
_AD_SUBTYPES = [("CUST", "Custom"), ("PFGT", "Force Greet"), ("RUMO", "Rumors"), ("FVDL", "Custom"),
    ("INTI", "Call"), ("FLAT", "Follow"), ("BRIB", "Move"), ("ASKG", "Attack"), ("GIFF", "Inspect"),
    ("ASKF", "Retrieve"), ("STAY", "Stay"), ("RELE", "Release"), ("SHRE", "Show Relationships"),
    ("FRJT", "Reject"), ("HEAL", "Heal"), ("ASSI", "Assign"), ("ENTE", "Enter"), ("SCEN", "Custom"),
    ("SHOW", "Show"), ("AGRE", "Agree"), ("REFU", "Refuse"), ("FEXT", "Exit Favor State"),
    ("MREF", "Moral Refusal"), ("TRAD", "Trade"), ("PRJT", "Pathing Refusal"), ("ATCK", "Attack"),
    ("POAT", "Power Attack"), ("BASH", "Bash"), ("HIT_", "Hit"), ("FLEE", "Flee"), ("BLED", "Bleed Out"),
    ("AVTH", "Avoid Threat"), ("DETH", "Death"), ("BLOC", "Block"), ("TAUT", "Taunt"),
    ("THGR", "Throw Grenade"), ("ALKL", "Ally Killed"), ("ORFB", "Order Fallback"), ("ORAV", "Order Move Up"),
    ("ORFL", "Order Flank"), ("ORTC", "Order Take Cover"), ("FLBK", "Retreat"), ("RQST", "Cover Me"),
    ("BGST", "Suppressive Fire"), ("CRIL", "Crippled Limb"), ("PATT", "Paired Attack"), ("STEA", "Steal"),
    ("YIEL", "Yield"), ("ACYI", "Accept Yield"), ("PICC", "Pickpocket Combat"), ("ASSA", "Assault"),
    ("MURD", "Murder"), ("ASNC", "Assault NC"), ("MUNC", "Murder NC"), ("PICN", "Pickpocket NC"),
    ("STFN", "Steal From NC"), ("TRAN", "Trespass Against NC"), ("TRES", "Trespass"), ("WTCR", "UNUSED01"),
    ("VPSS", "Voice Power Start Short"), ("VPSL", "Voice Power Start Long"), ("VPES", "Voice Power End Short"),
    ("VPEL", "Voice Power End Long"), ("ALIL", "Alert Idle"), ("LOIL", "Lost Idle"), ("NOTA", "Normal To Alert"),
    ("NOTC", "Normal To Combat"), ("NOTL", "Normal To Lost"), ("ALTN", "Alert To Normal"),
    ("ALTC", "Alert To Combat"), ("COTN", "Combat To Normal"), ("COLO", "Combat To Lost"),
    ("LOTN", "Lost To Normal"), ("LOTC", "Lost To Combat"), ("DFDA", "Detect Friend Die"),
    ("SERU", "Service Refusal"), ("REPA", "Repair"), ("TRAV", "Travel"), ("TRAI", "Training"),
    ("BAEX", "Barter Exit"), ("REEX", "Repair Exit"), ("RECH", "Recharge"), ("RCEX", "Recharge Exit"),
    ("TREX", "Training Exit"), ("OBCO", "Observe Combat"), ("NOTI", "Notice Corpse"), ("TITG", "Time To Go"),
    ("GBYE", "Goodbye"), ("HELO", "Hello"), ("SWMW", "Swing Melee Weapon"), ("FIWE", "Shoot Bow"),
    ("ZKEY", "Z Key Object"), ("JUMP", "Jump"), ("KNOO", "Knock Over Object"), ("DEOB", "Destroy Object"),
    ("STOF", "Standon Furniture"), ("LOOB", "Locked Object"), ("PICT", "Pickpocket Topic"),
    ("PURS", "Pursue Idle Topic"), ("IDAT", "Shared Info"), ("SCCH", "Scene Choice"),
    ("PCPS", "Player Cast Projectile Spell"), ("PCSS", "Player Cast Self Spell"), ("PCSH", "Player Shout"),
    ("IDLE", "Idle"), ("BREA", "Enter Sprint Breath"), ("ENBZ", "Enter Bow Zoom Breath"),
    ("EXBZ", "Exit Bow Zoom Breath"), ("ACAC", "Actor Collidewith Actor"), ("PIRN", "Playerin Iron Sights"),
    ("OUTB", "Outof Breath"), ("GRNT", "Combat Grunt"), ("LWBS", "Leave Water Breath"),
    ("IMPT", "Impatient Positive"), ("IMNG", "Impatient Negative"), ("IMNU", "Impatient Neutral"),
    ("IMQU", "Impatient Question"), ("WFPI", "Waiting For Player Input"), ("GREE", "Greeting"),
    ("PADR", "Player Activate Door"), ("PATR", "Player Activate Terminals"), ("PAFU", "Player Activate Furniture"),
    ("PAAC", "Player Activate Activators"), ("PACO", "Player Activate Container"),
    ("PAFI", "Player Aquire Featured Item")]
_AD_SUBTYPE_NAME = {}
_seen_names = set()
for _code, _nm in _AD_SUBTYPES:
    _v = struct.unpack("<I", _code.encode("ascii"))[0]
    _AD_SUBTYPE_NAME[_v] = _nm if _nm not in _seen_names else "%s@%d" % (_nm, _v)
    _seen_names.add(_nm)
_AD_INFO_LINKS = ("TPIC", "PNAM", "DNAM", "GNAM", "TSCE", "ANAM", "SNAM", "TCLT")


def _ad_segment_after(s, marker):
    p = s.lower().find(marker.lower())
    if p < 0:
        return ""
    return s[p + len(marker):].split("_", 1)[0]


def _ad_ally_name(edid):
    """AllyNameFromEdid."""
    for m in ("_Quest_Camp_Lite_", "_Quest_Intro_Lite_", "_Quest_Camp_Full_", "_Quest_Camp_Ally_",
              "_Quest_Ally_", "COMP_Conversation_", "COMP_Dialogue_"):
        r = _ad_segment_after(edid, m)
        if r:
            return r
    if "liteally" in edid.lower():
        return edid.rsplit("_", 1)[-1]
    r = _ad_segment_after(edid, "COMP_")
    if r and not any(w in r.lower() for w in ("quest", "conversation", "dialogue")):
        return r
    return edid.rsplit("_", 1)[-1]


def _ad_rule(edid, scripts, alias_names):
    """AllyMatchRule (EXTRA_INCLUDE / EXTRA_EXCLUDE are empty in the .pas)."""
    e, sc = edid.lower(), scripts.lower()
    if any(t in e for t in ("do_not_export", "template_", "comp_rq_", "comp_re_", "playerconnect")):
        return ""
    if any(t in e for t in ("camp_lite", "camplite", "liteally", "quest_camp_ally")):
        return "EDID-CampLite"
    if any(t in e for t in ("comp_quest_ally", "comp_quest_camp", "comp_ally")):
        return "EDID-CompAlly"
    if "liteallyscript" in sc:
        return "Script-LiteAlly"
    if any(t in sc for t in ("shutdownonallycleanup", "comp_rq_dialoguevariable", "allyquestscript")):
        return "Script-CompAlly"
    if ("comp" in e or "camp" in e) and any(
            n.lower() == "ally" or n.lower().startswith("ally_") or n.lower().startswith("liteally")
            for n in alias_names):
        return "Alias-Ally"
    return ""


def _ad_conds(ctx, subs, stop=None):
    conds = []
    for k, v in subs:
        if stop and k in stop:
            break
        if k == "CTDA":
            conds.append([v, None, None])
        elif k in ("CIS1", "CIS2") and conds:
            conds[-1][1 if k == "CIS1" else 2] = v.rstrip(b"\0").decode("utf-8", "replace")
    return _q2_conds(ctx, conds)


def exp_allydlg(esm):
    kids, infos = _quest_children(esm)
    quests, topics, lines, scenes = [], [], [], []
    seen = set()
    for r in R.xedit_order(esm, "QUST"):
        ed = cc(r.edid)
        head = dict(_qust_tree(r)["head"])
        names, _ff, _fr = _q2_vmad(head.get("VMAD"))
        scripts = " ; ".join(cc(n) for n in names if n)
        tree = _qust_tree(r)
        arows = _q2_alias_rows(esm, r, tree)
        rule = _ad_rule(ed, scripts, [n for _i, _a, _id, n, _k in arows])
        if not rule:
            continue
        ally = _ad_ally_name(ed)
        qfid = "%08X" % r.fid
        named = {int(aid): name for _i, _a, aid, name, k in arows if aid and name and k != "RefCol"}
        ctx = C.Ctx(esm, r, named)
        tcount = scount = icount = lcount = 0
        for sig, fid, subs in kids.get(r.fid, []):
            sd = dict(subs)
            if sig == "DIAL":
                tfid, ted = "%08X" % fid, cc(_str(sd.get("EDID")))
                prompt = cc(esm.lstring(sd.get("FULL")))
                sn = sd.get("SNAM")
                sub = ""
                dat = sd.get("DATA") or b""
                if sn and len(sn) >= 4:
                    v = struct.unpack("<I", sn[:4])[0]
                    sub = _AD_SUBTYPE_NAME.get(v, str(v))
                cat = (_AD_CATEGORIES[dat[1]] if len(dat) > 1 and dat[1] < len(_AD_CATEGORIES)
                       else (str(dat[1]) if len(dat) > 1 else ""))
                pri = sd.get("PNAM")
                br = sd.get("BNAM")
                ic = lc = 0
                for io, (ifid, isubs) in enumerate(infos.get(fid, [])):
                    key = "%08X" % ifid
                    if key in seen:
                        continue
                    seen.add(key)
                    ic += 1
                    isd = dict(isubs)
                    ien = isd.get("ENAM") or b""
                    iflags = ""
                    if ien:
                        fv = int.from_bytes(ien[:4] if len(ien) >= 8 else ien[:2], "little")
                        parts = [_AD_INFO_FLAGS[b] for b in range(32) if fv >> b & 1 and b in _AD_INFO_FLAGS]
                        parts.append("Reset Hours")
                        if len(ien) >= 8:
                            parts.append("Unknown")
                        iflags = " ; ".join(parts)
                    iconds = _ad_conds(ctx, isubs)
                    rn = isd.get("RNAM")
                    iprompt = cc(esm.lstring(rn)) if rn else ""
                    an = isd.get("ANAM")
                    speaker = _q2_linkcell(esm, struct.unpack_from("<I", an, 0)[0]) if an and len(an) == 4 else ""
                    tc = isd.get("TCLT")
                    linkto = _q2_linkcell(esm, struct.unpack_from("<I", tc, 0)[0]) if tc and len(tc) >= 4 else ""
                    pn = isd.get("PNAM")
                    prev = _q2_linkcell(esm, struct.unpack_from("<I", pn, 0)[0]) if pn and len(pn) >= 4 else ""
                    links = [x for x in ("Topic=" + _q2_linkcell(esm, fid) if _q2_linkcell(esm, fid) else "",
                                         "FormID=%08X:%s:INFO" % (ifid, cc(_str(isd.get("EDID"))))) if x]
                    for k, v in isubs:
                        if k in _AD_INFO_LINKS and len(v) == 4:
                            c = _q2_linkcell(esm, struct.unpack_from("<I", v, 0)[0])
                            if c:
                                links.append("%s=%s" % (k, c))
                    links = " ; ".join(cc(x) for x in links[:_Q2_MAX_LINKS])
                    inames, _f, _g = _q2_vmad(isd.get("VMAD"))
                    iscripts = " ; ".join(cc(n) for n in inames if n)
                    resps, cur = [], None
                    for k, v in isubs:
                        if k == "TRDA":
                            cur = {}
                            resps.append(cur)
                        elif cur is not None and k in ("NAM1", "NAM2", "NAM3"):
                            cur.setdefault(k, v)
                    base = [qfid, ed, ally, str(tcount), tfid, ted, prompt, str(io), key, cc(_str(isd.get("EDID")))]
                    tail = [speaker, iflags, iconds, linkto, prev, links, iscripts]
                    if not resps:
                        # No Responses: the .pas falls back to the first child whose
                        # name says "response" - the ENAM "Response flags" struct -
                        # and writes one empty line per member (Flags, Reset Hours,
                        # Unknown). Without ENAM either, one row.
                        n_empty = (3 if len(ien) >= 8 else 2) if ien else 1
                        for ri in range(n_empty):
                            lines.append("\t".join(base + [str(ri) if ien else "", "", iprompt, "", ""] + tail))
                        lc += n_empty
                        continue
                    for ri, rs in enumerate(resps[:24]):
                        lines.append("\t".join(base + [str(ri), cc(esm.lstring(rs.get("NAM1"))), iprompt,
                                                      cc(_str(rs.get("NAM2"))), cc(_str(rs.get("NAM3")))] + tail))
                        lc += 1
                topics.append("\t".join([qfid, ed, ally, str(tcount), tfid, ted, prompt, sub, cat,
                                         _lvli_float(pri) if pri else "", "",
                                         _q2_linkcell(esm, struct.unpack_from("<I", br, 0)[0]) if br else "",
                                         str(ic), str(lc)]))
                tcount += 1
                icount += ic
                lcount += lc
            elif sig == "SCEN":
                fl = next((v for k, v in subs if k == "FNAM"), None)
                flv = int.from_bytes(fl[:4], "little") if fl else 0
                phases = sum(1 for k, _v in subs if k == "HNAM") // 2
                # actors: an ALID followed by its LNAM flags (action ALIDs aren't)
                actors = [struct.unpack_from("<i", v, 0)[0] for n_, (k, v) in enumerate(subs)
                          if k == "ALID" and len(v) >= 4 and n_ + 1 < len(subs) and subs[n_ + 1][0] == "LNAM"]
                snames, _f, _g = _q2_vmad(sd.get("VMAD"))
                scenes.append("\t".join([qfid, ed, ally, "%08X" % fid, cc(_str(sd.get("EDID"))),
                                         _q2_flags(flv, _AD_SCENE_FLAGS), str(phases), str(len(actors)),
                                         " ; ".join(ctx.alias_text(a) for a in actors),
                                         _ad_conds(ctx, subs, stop=("HNAM",)),
                                         " ; ".join(cc(n) for n in snames if n)]))
                scount += 1
        dat = head.get("DATA") or b""
        flags_v = int.from_bytes(dat[:8], "little") if len(dat) >= 20 else (int.from_bytes(dat[:4], "little") if dat else 0)
        pri = dat[8] if len(dat) >= 20 else (dat[4] if len(dat) > 4 else None)
        quests.append("\t".join([qfid, ed, ally, cc(esm.lstring(head.get("FULL"))), rule,
                                 _q2_flags(flags_v, _Q2_QUEST_FLAGS), "" if pri is None else str(pri),
                                 str(tcount), str(icount), str(lcount), str(scount), scripts, "SeventySix.esm"]))
    return {
        "_Quests": ["QuestFormID\tQuestEDID\tAllyGuess\tQuestName\tMatchRule\tQuestFlags\tPriority\t"
                    "TopicCount\tInfoCount\tLineCount\tSceneCount\tScripts\tSourceFile"] + quests,
        "_Topics": ["QuestFormID\tQuestEDID\tAllyGuess\tTopicOrder\tTopicFormID\tTopicEDID\tTopicPrompt\t"
                    "TopicSubtype\tTopicCategory\tTopicPriority\tTopicFlags\tBranch\tInfoCount\tLineCount"] + topics,
        "_Lines": ["QuestFormID\tQuestEDID\tAllyGuess\tTopicOrder\tTopicFormID\tTopicEDID\tTopicPrompt\t"
                   "InfoOrder\tInfoFormID\tInfoEDID\tResponseIndex\tResponseText\tPlayerPrompt\tScriptNotes\t"
                   "Edits\tSpeaker\tInfoFlags\tConditions\tLinkTo\tPrevInfo\tLinks\tScripts"] + lines,
        "_Scenes": ["QuestFormID\tQuestEDID\tAllyGuess\tSceneFormID\tSceneEDID\tSceneFlags\tPhaseCount\t"
                    "ActorCount\tActorAliases\tConditions\tScripts"] + scenes,
    }


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


NEEDS_REFS = {"ARMO2", "AVTR", "BOOK", "BOOK2", "COBJ", "CONT", "ENCH", "ENTM", "FISH", "FLST", "FURN", "GMRW", "KEYM", "LGDI", "LCTN", "MGEF", "NOTE", "PCRD", "ALCH", "QUST2", "LVLI", "GLOB", "AVIF", "KYWD", "CURV", "CMPO", "RESO", "BPTD", "ECAT", "CNDF", "LSCR", "MISC", "NPC", "NPC2", "MISC2", "ACTI", "ACTI2"}
# exports that need Referenced By for more than their own records
REF_TARGETS = {"ARMO2": armo2_ref_targets, "BOOK2": book2_ref_targets, "NPC2": npc2_ref_targets, "MISC2": misc2_ref_targets, "ACTI2": acti2_ref_targets}

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
    "LVLI": (["LVLI", "GLOB"], "LVLI_Export", exp_lvli),
    "QUST2": (["QUST"], "QUST2_Export", exp_qust2),
    "ALLYDLG": (["QUST"], "ALLYDLG_Export", exp_allydlg),
    "TERMPL": (["TERM"], "TERM_Export", exp_term_placements),
    "ALCH": (["ALCH"], "ALCH_Export", exp_alch),
    "ARMO": (["ARMO"], "ARMO_Export", exp_armo),
    "ARMO2": (["ARMO", "OMOD"], "ARMO2_Underarmour", exp_armo2),
    "AVTR": (["AVTR"], "AVTR_Export", exp_avtr),
    "BOOK": (["BOOK"], "BOOK_Export", exp_book),
    "BOOK2": (["BOOK"], "BOOK2_Export", exp_book2),
    "COBJ": (["COBJ"], "COBJ_Export", exp_cobj),
    "CONT": (["CONT"], "CONT_Export", exp_cont),
    "ENCH": (["ENCH"], "ENCH_Export", exp_ench),
    "ENTM": (["ENTM"], "ENTM_Export", exp_entm),
    "FISH": (["FISH"], "FISH_Export", exp_fish),
    "FLST": (["FLST"], "FLST_Export", exp_flst),
    "FURN": (["FURN"], "FURN_Export", exp_furn),
    "GMRW": (["GMRW"], "GMRW_Export", exp_gmrw),
    "KEYM": (["KEYM"], "KEYM_Export", exp_keym),
    "LGDI": (["LGDI", "OMOD"], "LGDI_Export", exp_lgdi),
    "LCTN": (["LCTN"], "LCTN_Export", exp_lctn),
    "MGEF": (["MGEF"], "MGEF_Export", exp_mgef),
    "NOTE": (["NOTE"], "NOTE_Export", exp_note),
    "PCRD": (["PCRD"], "PCRD_Export", exp_pcrd),
    "REFR": (["ACTI"], "REFR_Placements", exp_refr_placements),
    "LORE": (["NOTE", "BOOK"], "ALLYDLG_Export", exp_lore),
}

# exports whose files have different prefixes: {export: {result key: file prefix}}
PREFIX_FILES = {"COBJ": {"COBJ": "COBJ_Export", "COBJ2": "COBJ2_Export"}}

# PTS file names carry the date only (no time) for these, like their .pas
DATE_ONLY_PTS = {"REFRLVLI", "REFR"}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("which", nargs="*", help="exports to run (default: all): " + " ".join(EXPORTS))
    ap.add_argument("--pts", action="store_true", help="PTS: read the Playtest install, write tsv\\pts\\")
    ap.add_argument("--data", help="game Data folder (default: the live or PTS install)")
    ap.add_argument("--out", help="output folder (default: tsv\\ or tsv\\pts\\)")
    ap.add_argument("--no-ledger", action="store_true", help="don't add lines to tsv\\_exports.tsv")
    ap.add_argument("--bases", default="", help="REFR: extra base FormIDs, comma separated (e.g. 008E069E,008F1672)")
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
    esm.out_dir = out_dir
    esm.extra_bases = [int(b, 16) for b in a.bases.replace(" ", "").split(",") if b]
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
        stamp = R.out_name(prefix, channel)[len(prefix):-4]   # _October_2026 / _PTS_2026-10-25_1615
        if channel == "PTS" and w in DATE_ONLY_PTS:
            stamp = stamp[:-5]   # drop the _HHMM
        for suffix, lines in files.items():
            if len(lines) <= 1:
                print("  %-7s no records found - nothing written" % (w + suffix))
                continue
            # a key in PREFIX_FILES names a whole prefix (COBJ writes COBJ_Export_* and COBJ2_Export_*)
            name = (PREFIX_FILES[w][suffix] + stamp) if w in PREFIX_FILES else (prefix + stamp + suffix)
            path = os.path.join(out_dir, name + ".tsv")
            R.write_tsv(path, lines)
            if not a.no_ledger:
                R.append_ledger(channel, path)
            print("  %-7s %5d rows -> %s" % (w, len(lines) - 1, path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
