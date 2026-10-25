r"""
esm_conditions.py - shows a record's conditions (CTDA) the way xEdit does.

Each condition becomes the list of (element name, edit value) pairs that xEdit's
GetEditValue gives for the CTDA struct, in xEdit's element order:

    Type, Unused, Comparison Value, Function, Unused, Parameter #1, Parameter #2,
    Run On, Reference, Parameter #3

Parameter display follows xEdit's wbConditionParameters (see xedit_functions.py).
"""
from __future__ import annotations

import struct

from xedit_functions import ENUMS, FUNCTIONS

RUN_ON = ["Subject", "Target", "Reference", "Combat Target", "Linked Reference",
          "Quest Alias", "Package Data", "Event Data", "Command Target",
          "Event Camera Ref", "My Killer", "Active Players", "Potential Players",
          "Player Teammates", "Target List", "Instance Owner"]

EVENT_DATA = {-1: "None", 0x314F: "Created Object", 0x3146: "Form", 0x3147: "Global",
              0x3149: "Identifier", 0x314B: "Keyword 1", 0x324B: "Keyword 2",
              0x334B: "Keyword 3", 0x314C: "Location 1", 0x324C: "Location 2",
              0x334C: "Location 3", 0x3150: "Player 1", 0x3250: "Player 2",
              0x3251: "Quest", 0x3152: "Reference 1", 0x3252: "Reference 2",
              0x3352: "Reference 3", 0x3156: "Value 1", 0x3256: "Value 2",
              0x7FFFFFFF: "All"}

# xEdit TConditionParameterType numbers (0 = ptNone) for the special cases
PT_REFERENCE, PT_ACTOR, PT_PACKAGE, PT_QUEST = 57, 23, 52, 55
PT_ALIAS, PT_PACKDATA = 5, 8

UNKNOWN = []  # (function, union index, raw) seen but not decoded exactly - for checking


def hexbytes(b):
    return " ".join("%02X" % x for x in b)


def type_str(t):
    r = list("00000000")
    op = t & 224
    if op == 0:
        r[0] = "1"
    elif op == 64:
        r[1] = "1"
    elif op == 96:
        r[0] = r[1] = "1"
    elif op == 128:
        r[2] = "1"
    elif op == 160:
        r[0] = r[2] = "1"
    for bit, pos in ((1, 3), (2, 4), (4, 5), (8, 6), (16, 7)):
        if t & bit:
            r[pos] = "1"
    return "".join(r)


def fmt_float(v):
    if v != v:
        return "NaN"
    return "%.6f" % v


# xEdit wbConditionEventToStr: Parameter #1 of GetEventData is a function
# (low 16 bits) and an event member (high 16 bits), shown "GetValue:Value2".
EVENT_FUNCTIONS = ["GetIsID", "IsInList", "GetValue", "HasKeyword", "GetItemValue"]
EVENT_MEMBERS = {0x0000: "None", 0x314F: "CreatedObject", 0x3146: "Form", 0x3147: "Global",
                 0x3149: "Identifier", 0x314B: "Keyword1", 0x324B: "Keyword2", 0x334B: "Keyword3",
                 0x314C: "Location1", 0x324C: "Location2", 0x334C: "Location3", 0x3150: "Player1",
                 0x3250: "Player2", 0x3251: "Quest", 0x3152: "Reference1", 0x3252: "Reference2",
                 0x3352: "Reference3", 0x3156: "Value1", 0x3256: "Value2", 0x7FFFFFFF: "All"}


def _enum_or_num(names, i):
    if isinstance(names, dict):
        return names.get(i, str(i))
    return names[i] if 0 <= i < len(names) else str(i)


class Ctx:
    """What a condition needs from the record that owns it.

    alias_names: {alias id: name} of the owning quest, when the caller has it
    (QUST2). Alias parameters and Quest Alias then read like xEdit's alias
    combo: "002 Player", or "045" when the name isn't found. Left None, they
    stay plain numbers (the exports already checked against xEdit that way)."""

    def __init__(self, esm, rec, alias_names=None):
        self.esm, self.rec = esm, rec
        self.param1_fid = 0
        self.alias_names = alias_names

    def alias_text(self, n):
        name = self.alias_names.get(n, "") if self.alias_names is not None else ""
        if n == -1:
            return "None"          # xEdit's alias combo for "no alias"
        return ("%03d %s" % (n, name)).strip() if n >= 0 else str(n)

    def owner_quest(self):
        """Quest used when a Quest parameter is NULL (xEdit's wbConditionQuestOverlay)."""
        r = self.rec
        if r.sig == "QUST":
            return r.fid
        for sigs, sub in ((("ACTI", "TACT", "FURN"), "QSTI"), (("SCEN",), "PNAM"), (("PACK", "TERM"), "QNAM"),
                          (("GMRW",), "ANAM")):
            if r.sig in sigs:
                v = r.first(sub)
                return struct.unpack("<I", v[:4])[0] if v else 0
        return 0


def _param(ctx, union, raw, cis):
    esm = ctx.esm
    u = struct.unpack("<I", raw)[0]
    s = struct.unpack("<i", raw)[0]
    if union in (0, 1):
        return hexbytes(raw)
    if union == 2:
        return fmt_float(struct.unpack("<f", raw)[0])
    if union == 3:
        return str(s)
    if union in (4, 7):
        return cis or ""
    if union == 5:
        return esm.link_edit_value(u)
    if union == 6:
        if ctx.alias_names is not None:
            return ctx.alias_text(s)
        UNKNOWN.append(("alias", u))
        return str(s)
    if union == 8:  # ptEvent
        return _enum_or_num(EVENT_FUNCTIONS, u & 0xFFFF) + ":" + _enum_or_num(EVENT_MEMBERS, u >> 16)
    if union == 9:
        return str(u)
    if union == 10:  # quest stage of the quest that owns this record
        return quest_stage(esm, ctx.owner_quest(), u)
    if union == 11:  # quest stage of the quest in Parameter #1
        return quest_stage(esm, ctx.param1_fid, u)
    if union in ENUMS:
        names = ENUMS[union]
        if isinstance(names, dict):
            return names.get(s, str(s))
        return names[s] if 0 <= s < len(names) and names[s] else str(s)
    if 12 <= union <= 22:
        UNKNOWN.append(("enum%d" % union, u))
        return str(u)
    if union in (PT_ACTOR + 1, PT_REFERENCE + 1) and u == 0:
        return "TARGET - Target Reference [00000000]"
    if union == PT_QUEST + 1 and u == 0:
        q = ctx.owner_quest()
        if q:
            return esm.link_edit_value(q)
    return esm.link_edit_value(u)


def elements(ctx, ctda, cis1=None, cis2=None):
    """[(name, edit value)] for one CTDA, like xEdit's ElementByIndex/GetEditValue."""
    esm = ctx.esm
    t = ctda[0]
    func = struct.unpack_from("<H", ctda, 8)[0]
    desc = FUNCTIONS.get(func)
    out = [("Type", type_str(t)), ("Unused", hexbytes(ctda[1:4]))]
    if t & 4:
        out.append(("Comparison Value - Global", esm.link_edit_value(struct.unpack_from("<I", ctda, 4)[0])))
    else:
        out.append(("Comparison Value - Float", fmt_float(struct.unpack_from("<f", ctda, 4)[0])))
    out.append(("Function", desc[0] if desc else str(func)))
    out.append(("Unused", hexbytes(ctda[10:12])))
    run_on = struct.unpack_from("<I", ctda, 20)[0]
    ctx.param1_fid = struct.unpack_from("<I", ctda, 12)[0]
    for n, (off, cis) in enumerate(((12, cis1), (16, cis2))):
        if desc is None:
            union = 0
        else:
            pt = desc[1 + n]
            if pt in (PT_REFERENCE, PT_ACTOR, PT_PACKAGE):
                if t & 2:  # "use aliases"
                    if not (n == 0 and run_on == 5 and desc[0] == "GetIsCurrentPackage"):
                        pt = PT_ALIAS
                elif t & 8:  # "use packdata"
                    pt = PT_PACKDATA
            union = pt + 1
        # element name = the union member xEdit picked ("Unknown"/"None" for no parameter)
        pname = {0: "Unknown", 1: "None"}.get(union, "Parameter #%d" % (n + 1))
        out.append((pname, _param(ctx, union, ctda[off:off + 4], cis)))
    out.append(("Run On", RUN_ON[run_on] if run_on < len(RUN_ON) else str(run_on)))
    ref = struct.unpack_from("<I", ctda, 24)[0]
    if run_on == 2:
        out.append(("Reference", esm.link_edit_value(ref)))
    else:
        out.append(("Unused", str(ref)))
    if len(ctda) >= 32:
        p3 = struct.unpack_from("<i", ctda, 28)[0]
        if run_on == 5:
            if ctx.alias_names is not None:
                out.append(("Quest Alias", ctx.alias_text(p3)))
            else:
                UNKNOWN.append(("alias3", p3))
                out.append(("Quest Alias", str(p3)))
        elif run_on == 7:
            out.append(("Event Data", EVENT_DATA.get(p3, str(p3))))
        else:
            out.append(("Parameter #3", str(p3)))
    return out, (ref if run_on == 2 else 0)


def conditions(rec, stop_at=("NEXT",)):
    """[(ctda bytes, cis1, cis2)] for the record's main Conditions array (stops at NEXT)."""
    res = []
    for k, v in rec.subs:
        if k in stop_at:
            break
        if k == "CTDA":
            res.append([v, None, None])
        elif k == "CIS1" and res:
            res[-1][1] = v.rstrip(b"\0").decode("utf-8", "replace")
        elif k == "CIS2" and res:
            res[-1][2] = v.rstrip(b"\0").decode("utf-8", "replace")
    return res


def pas_cond(ctx, ctda, cis1=None, cis2=None, clean=lambda s: s):
    """The BuildCondFromCTDA string the CHAL/CMPT/PLYT .pas scripts write:
    Type|CompareValue|Function|<every other non-empty element, |-joined>|RunOn|RefLinkCell"""
    els, ref = elements(ctx, ctda, cis1, cis2)
    d = dict()
    params = []
    for name, val in els:
        val = clean(val)
        low = name.lower()
        if low in ("type", "comparison value - float", "comparison value - global", "function", "run on"):
            d[low] = val
            continue
        if val == "":
            continue
        params.append(val)
    comp = d.get("comparison value - float", d.get("comparison value - global", ""))
    esm = ctx.esm
    refcell = ""
    if ref and esm.exists(ref):
        refcell = "%08X:%s:%s" % (ref, esm.edid_of(ref), esm.sig_of(ref))
    return "|".join([d["type"], comp, d["function"], "|".join(params), d["run on"], clean(refcell)])


def op_from_type(type_bits):
    """The SPEL .pas OpFromType on xEdit's 8-character Type string."""
    b = type_bits
    if b[0] == "1" and b[1] == "1":
        return ">="
    if b[0] == "1" and b[2] == "1":
        return "<="
    if b[0] == "1":
        return "="
    if b[1] == "1":
        return ">"
    if b[2] == "1":
        return "<"
    return "!="


_STAGES = {}


def quest_stages(esm, qfid):
    """{stage index: first log entry text} for a quest (xEdit Stages\Log Entries\CNAM)."""
    if qfid in _STAGES:
        return _STAGES[qfid]
    res = None
    q = esm.get(qfid) if qfid else None
    if q is not None and q.sig == "QUST":
        res = {}
        # xEdit reads the FIRST log entry's text only: a stage whose first
        # entry has no CNAM shows just its number, even when a later entry
        # has text (GQ_Horde stage 200).
        cur, logs = None, 0
        for k, v in q.subs:
            if k == "INDX":
                cur, logs = struct.unpack("<H", v[:2])[0], 0
                res.setdefault(cur, "")
            elif k in ("QOBJ", "ANAM", "ALST", "ALLS", "ALCS"):
                cur = None
            elif k == "QSDT" and cur is not None:
                logs += 1
            elif k == "CNAM" and cur is not None and logs <= 1 and not res[cur]:
                res[cur] = esm.lstring(v).strip()
    _STAGES[qfid] = res
    return res


def quest_stage(esm, qfid, stage):
    """xEdit's quest-stage display: 3-digit stage + first log entry, or the bare number."""
    st = quest_stages(esm, qfid)
    if st is None or stage not in st:
        return str(stage)
    t = "%03d" % stage
    return t + (" " + st[stage] if st[stage] else "")
