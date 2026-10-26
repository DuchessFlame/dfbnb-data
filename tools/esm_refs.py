r"""
esm_refs.py - xEdit's "Referenced By" lists, worked out from the game files.

xEdit knows from its record definitions which fields hold a FormID. This module
gets the same answer three ways:
  * CTDA conditions - decoded exactly (esm_conditions / xedit_functions).
  * VMAD scripts    - parsed exactly (vmad.py, from xEdit's wbVMAD definitions).
  * everything else - xedit_ref_fields.json: for each record/subrecord, which byte
    positions hold FormIDs and what record types they point at. Learned from
    xEdit's own reference cache (FO76Edit Cache\*.refcache, Sep 2026 build), so
    it matches xEdit field for field. Rules are kept per subrecord size where a
    subrecord has more than one layout (e.g. LVLO is 4 or 12 bytes).
A one-pass scan (~45 s) of SeventySix.esm, world data included, finds every record
pointing at the targets. Lists come back sorted by FormID, like ReferencedByIndex.
"""
from __future__ import annotations

import bisect
import json
import os
import struct
import zlib

import numpy as np

import vmad
from xedit_functions import FUNCTIONS

_HERE = os.path.dirname(os.path.abspath(__file__))
_RULES = None


def _rules():
    global _RULES
    if _RULES is None:
        raw = json.load(open(os.path.join(_HERE, "xedit_ref_fields.json"), encoding="utf-8"))
        r = {}
        for k, v in raw.items():
            sig, sub = k.split("|")
            bys = {int(z): {int(o): frozenset(sg) for o, sg in offs.items()}
                   for z, offs in v.get("bysize", {}).items()}
            if bys:
                r[(sig, sub, "size")] = bys
            if "fixed" in v:
                r[(sig, sub)] = ("fixed", {int(o): frozenset(s) for o, s in v["fixed"].items()})
            elif "stride" in v:
                r[(sig, sub)] = ("stride", v["stride"], {int(o): frozenset(s) for o, s in v["phases"].items()})
            elif "any" in v:
                r[(sig, sub)] = ("any", frozenset(v["any"]))
        _RULES = r
    return _RULES


def _ctda_offsets(d):
    t = d[0]
    out = set()
    if t & 4:
        out.add(4)
    fn = FUNCTIONS.get(struct.unpack_from("<H", d, 8)[0])
    if fn:
        for n, o in ((1, 12), (2, 16)):
            pt = fn[n]
            if pt in (57, 23, 52) and t & 10:  # reference/actor/package param turned into alias/packdata
                continue
            if pt >= 22 or pt == 4:  # FormID parameter types (and Actor Value)
                out.add(o)
    if struct.unpack_from("<I", d, 20)[0] == 2:  # Run On = Reference
        out.add(24)
    return out


def _subs(data):
    out, p, big = [], 0, None
    n = len(data)
    while p + 6 <= n:
        ss = data[p:p + 4]
        sz = struct.unpack_from("<H", data, p + 4)[0]
        p += 6
        if ss == b"XXXX":
            big = struct.unpack_from("<I", data, p)[0]
            p += sz
            continue
        if big is not None:
            sz, big = big, None
        out.append((ss.decode("latin-1"), p, p + sz))
        p += sz
    return out


SAME_FIELDS_AS = {"INGR": "ALCH"}
PLACED_LIKE_REFR = {"PGRE", "PMIS", "PARW", "PBEA", "PFLA", "PCON", "PBAR", "PHZD"}


def _omod_data_offsets(d):
    """Byte offsets of the FormIDs in an OMOD DATA subrecord (FO76 form version 107+)."""
    out = set()
    try:
        inc, prop = struct.unpack_from("<II", d, 0)
        out.add(16)
        n = struct.unpack_from("<I", d, 20)[0]
        out.update(24 + 4 * i for i in range(n))
        p = 24 + 4 * n
        p += 4 + 8 * struct.unpack_from("<I", d, p)[0]
        for _ in range(inc):
            out.add(p)
            p += 7
        for _ in range(prop):
            if d[p] in (4, 6):
                out.add(p + 12)
            out.add(p + 20)
            p += 24
    except (struct.error, IndexError):
        pass
    return out


def _is_ref(rules, sig, sub, rel, field, tsig):
    if sub == "CTDA":
        return rel in _ctda_offsets(field)
    if sub == "VMAD":
        try:
            return rel in vmad.formid_offsets(sig, field)
        except Exception:
            pass
    # Fields xEdit defines as plain FormIDs, so it lists whatever record the
    # number happens to be - the learned rules only saw some record types there:
    #   CELL XCRI (precombined meshes): 16-byte header (mesh count at 0,
    #     reference count at 8), 8 bytes per mesh, then 8 bytes per reference
    #     with the FormID first. Any number in a reference slot that equals a
    #     leveled list / curve / keyword FormID shows up in xEdit's Referenced
    #     By (LostHomeExt03 -> three recipe lists). Slots inside the mesh block
    #     are hashes, not FormIDs.
    #   PACK PTDA (package target): type 0 Specific Reference / 1 Object ID
    #     holds a FormID of any type (XPD_AC01_SalSit -> LL_Drink_Alcohol_Wine).
    if sig == "CELL" and sub == "XCRI" and len(field) >= 16:
        start = 16 + 8 * struct.unpack_from("<I", field, 0)[0]
        # only when the slot's second u32 is 0: a pair like 003CB4B7 00000009 (thirteen of
        # them in EasternRegionalPen01) is no link in xEdit, 00006AFA 00000000 is (26 Oct 2026)
        if rel >= start and (rel - start) % 8 == 0:
            return rel + 8 <= len(field) and struct.unpack_from("<I", field, rel + 4)[0] == 0
        # anywhere else: the learned rule, unchanged
    if sig == "PACK" and sub == "PTDA" and rel == 4 and len(field) >= 8 \
            and struct.unpack_from("<I", field, 0)[0] in (0, 1):
        return True
    #     Types 2 Object Type, 4 Ref Alias, 5 Interrupt Data and 6 Self hold a number,
    #     not a FormID: XPD_Pitt01_Hex_Obj_A01_Sit's alias 357 (0x165) is no link to
    #     HelpManualXBox, and xEdit doesn't list it (25 Oct 2026).
    if sig == "PACK" and sub == "PTDA" and rel == 4 and len(field) >= 8 \
            and struct.unpack_from("<I", field, 0)[0] in (2, 4, 5, 6):
        return False
    #   LOUT LMML (loadout map marker list): an FLST the learned rules never saw
    #     there (ATX_Loadout_50Boost -> LevelBoostMapMarkers, 25 Oct 2026).
    if sig == "LOUT" and sub == "LMML" and rel == 0:
        return True
    #   EXPL DATA offset 16 (Placed Object): whatever the explosion leaves behind,
    #     any record type (Storm_SE09_ChickenExplosion -> an ALCH, 25 Oct 2026).
    if sig == "EXPL" and sub == "DATA" and rel == 16 and len(field) >= 20:
        return True
    #   QUST QOTM (objective timer): a GLOB; the learned rules never saw the field.
    #     241 quest -> global links in GLOB's Referenced By (25 Oct 2026).
    if sig == "QUST" and sub == "QOTM" and rel == 0:
        return True
    #   DMGT DSES / DSEB (status-effect recovery / build-up actor value) and LVLI
    #     LVIG (an AVIF as well as a GLOB): AVIF links xEdit lists (25 Oct 2026).
    if rel == 0 and ((sig == "DMGT" and sub in ("DSES", "DSEB")) or (sig == "LVLI" and sub == "LVIG")):
        return True
    #   COBJ CNAM (created object) and FVPA components (12 bytes each, FormID first):
    #     any base object. The learned rules never saw a BOOK there; PTS build 211's
    #     HIDE_co_Lead_* recipes make one lead (BOOK) from another (25 Oct 2026).
    if sig == "COBJ" and ((sub == "CNAM" and rel == 0) or (sub == "FVPA" and rel % 12 == 0)):
        return True
    #   DFOB DATA (Default Object): xEdit lists a FISH there (FishingFallbackFish_DO ->
    #     BrookSilverside), which the learned rules never saw. Not every type: xEdit's
    #     October ENTM export leaves out UTILITY_ENTM_ItemDisableEntitlement_DO, so the
    #     learned type list stands and only FISH is added (25 Oct 2026).
    if sig == "DFOB" and sub == "DATA" and rel == 0 and tsig in ("FISH", "WTHR"):
        return True
    #   CELL XISR (interior sky / weather override): a WTHR, then 4 more bytes. The
    #     learned rules had no XISR (41 cells -> weathers in xEdit's WTHR _Refs; the WTHR
    #     DFOB BabylonNukeZoneWeather_DO likewise; 26 Oct 2026).
    if sig == "CELL" and sub == "XISR" and rel == 0:
        return True
    #   DOBJ DNAM (Default Object Manager): 8-byte pairs, a 4-letter use code then a
    #     FormID of any type (DOBJ 00000800 -> HelpManualPC FLST, test_FurnitureTester
    #     NPC_); the learned rules had no DOBJ at all (25 Oct 2026).
    if sig == "DOBJ" and sub == "DNAM" and rel % 8 == 4:
        return True
    #   LVLI LVLO (12-byte entry: level, item, count) and GMRW QSRD (rewarded item,
    #     count): the item is any type. The learned rules missed KEYM in both
    #     (RSVP02_Container_Safe_Reward, Burn_SQ04_CacheKey1Reward; 25 Oct 2026).
    if (sig == "LVLI" and sub == "LVLO" and len(field) == 12 and rel == 4) or \
            (sig == "GMRW" and sub == "QSRD" and rel == 0):
        return True
    #   MGEF DATA offset 12 (Associated Item): any type, by archetype; the learned
    #     rules saw DMGT / KYWD / LIGH / SPEL / STHD only. W05_TurboFert_Magiceffect
    #     -> an FLST (25 Oct 2026).
    if sig == "MGEF" and sub == "DATA" and rel == 12:
        return True
    #   BOOK DNAM: Flags (u8), then "Teaches" at offset 1 - an Actor Value (flag 0x01),
    #     Spell (0x04) or Perk (0x10). The learned rules never saw that unaligned slot:
    #     PerkMagMassSurgicalJournal01..09 -> the perk 0008E3A5, which xEdit lists (26 Oct 2026).
    if sig == "BOOK" and sub == "DNAM" and rel == 1 and len(field) >= 5 and field[0] & 0x15:
        return True
    #   OMOD DATA, read by layout (26 Oct 2026). The learned rule allowed a FormID at any
    #     byte, so a 4-byte window straddling two fields counted: an include of an
    #     OMOD 0079xxxx put Bot_RoseNanny_FrontArmor (00007976) in 32 mods' Referenced
    #     By, which xEdit doesn't list. FormIDs sit only at: Attach Point (16), the
    #     Attach Parent Slots, each include's Mod (7-byte includes), and per 24-byte
    #     property Value 1 when the value type is FormID,Int / FormID,Float (4 / 6)
    #     and the Curve Table (+20). Items are byte pairs, not FormIDs.
    if sig == "OMOD" and sub == "DATA" and len(field) >= 28:
        return rel in _omod_data_offsets(field)
    #   OBTS (object template combination, any record): a property whose value type
    #     is FormID,Int (4) or FormID,Float (6) holds a form in Value 1. Properties
    #     follow the includes: Include Count u32, Property Count u32, 7 bytes, Default,
    #     u8 keyword count + FormIDs, 2 bytes, 7-byte includes, then 24-byte properties
    #     (type, function, property, Value 1, Value 2, curve). MTR04_EmployeeSecurityBot
    #     -> an ENCH (25 Oct 2026).
    if sub == "OBTS" and len(field) >= 18:
        inc, prop = struct.unpack_from("<II", field, 0)
        p = 16 + field[15] * 4 + 2 + inc * 7
        if p <= rel < p + prop * 24 and (rel - p) % 24 == 12 and field[p + (rel - p) // 24 * 24] in (4, 6):
            return True
    # Placed projectiles / hazards / grenades ... (PGRE, PHZD, PMIS ...) are REFRs with
    # another signature and the same fields; the learned rules mostly saw REFR, so a
    # field they have no rule for uses the REFR rule (PHZD / PGRE XEZN -> the
    # encounter-zone LCTN, LocForestPoseidonEnergyLocation, 25 Oct 2026).
    if sig in PLACED_LIKE_REFR and (sig, sub) not in rules and (sig, sub, "size") not in rules:
        sig = "REFR"
    #   INGR (ingredient) has the same effect fields as ALCH; the learned rules have no
    #   INGR at all (CopperPipe EFID -> RestoreHealthFood MGEF, 25 Oct 2026).
    if sig in SAME_FIELDS_AS and (sig, sub) not in rules and (sig, sub, "size") not in rules:
        sig = SAME_FIELDS_AS[sig]
    bys = rules.get((sig, sub, "size"))
    if bys is not None and len(field) in bys:  # rule for this exact subrecord size
        return tsig in bys[len(field)].get(rel, ())
    r = rules.get((sig, sub))
    if r is None:
        return False
    if r[0] == "fixed":
        return tsig in r[1].get(rel, ())
    if r[0] == "stride":
        return tsig in r[2].get(rel % r[1], ())
    return tsig in r[1]


def referenced_by(esm, targets, chunk=48 << 20, referrers=None):
    """{target fid: sorted [referrer fid]}, and {referrer fid: (sig, edid)}.
    referrers: optional set of record types (e.g. {"REFR"}) - only those are scanned."""
    rules = _rules()
    # A placed reference (REFR / ACHR ... in the world data) has no entry in the
    # main index; the learned rules call that whole class "W".
    tsig = {t: (esm.sig_of(t) or ("W" if esm.world_record(t) is not None else "")) for t in targets}
    tarr = np.array(sorted(targets), dtype="<u4")
    found = {}
    info = {}
    buf = bytearray()
    meta = []

    def flush():
        nonlocal buf, meta
        if not meta:
            return
        b = bytes(buf)
        starts = [m[0] for m in meta]
        cand = {}
        for k in range(4):
            n = (len(b) - k) // 4
            if n <= 0:
                continue
            v = np.frombuffer(b, dtype="<u4", count=n, offset=k)
            idx = np.searchsorted(tarr, v)
            idx[idx >= len(tarr)] = 0
            for p in np.nonzero(tarr[idx] == v)[0]:
                off = k + 4 * int(p)
                ri = bisect.bisect_right(starts, off) - 1
                cand.setdefault(ri, []).append((off - meta[ri][0], int(v[p])))
        for ri, hits in cand.items():
            s, e, fid, sig = meta[ri]
            data = b[s:e]
            sp = _subs(data)
            sst = [x[1] for x in sp]
            for rel, t in hits:
                i = bisect.bisect_right(sst, rel) - 1
                if i < 0:
                    continue
                sub, a, z = sp[i]
                if rel + 4 > z:
                    continue
                hit = _is_ref(rules, sig, sub, rel - a, data[a:z], tsig[t])
                # PERK EPFD with entry-point function type (EPFT) 9 holds a form of any
                # type (PlayerPerk -> an ALCH, 25 Oct 2026); the learned rule only saw
                # SPEL / LVLI there. EPFT is the nearest earlier EPFT subrecord.
                if not hit and sig == "PERK" and sub == "EPFD" and rel == a and z - a == 4:
                    ep = next((data[x] for ss, x, _y in reversed(sp[:i]) if ss == "EPFT"), None)
                    hit = ep == 9
                if hit:
                    found.setdefault(t, set()).add(fid)
                    if fid not in info:
                        ed = next((data[x:y] for ss, x, y in sp if ss == "EDID"), b"")
                        info[fid] = (sig, ed.rstrip(b"\0").decode("utf-8", "replace"))
        buf = bytearray()
        meta = []

    with open(esm.esm_path, "rb") as f:
        size = os.path.getsize(esm.esm_path)
        f.seek(24 + struct.unpack("<I", f.read(24)[4:8])[0])
        while f.tell() < size:
            hdr = f.read(24)
            ds = struct.unpack_from("<I", hdr, 4)[0]
            if hdr[:4] == b"GRUP":
                continue
            flags, fid = struct.unpack_from("<II", hdr, 8)
            if referrers is not None and hdr[:4].decode() not in referrers:
                f.seek(ds, 1)
                continue
            data = f.read(ds)
            if flags & 0x40000:
                data = zlib.decompress(data[4:])
            s = len(buf)
            buf += data
            meta.append((s, len(buf), fid, hdr[:4].decode()))
            if len(buf) > chunk:
                flush()
    flush()
    return {t: sorted(v) for t, v in found.items()}, info
