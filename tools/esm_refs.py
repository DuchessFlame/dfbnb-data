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


def _is_ref(rules, sig, sub, rel, field, tsig):
    if sub == "CTDA":
        return rel in _ctda_offsets(field)
    if sub == "VMAD":
        try:
            return rel in vmad.formid_offsets(sig, field)
        except Exception:
            pass
    # Two fields xEdit defines as plain FormIDs, so it lists whatever record the
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
        if rel >= start and (rel - start) % 8 == 0:
            return True
        # anywhere else: the learned rule, unchanged
    if sig == "PACK" and sub == "PTDA" and rel == 4 and len(field) >= 8 \
            and struct.unpack_from("<I", field, 0)[0] in (0, 1):
        return True
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
    tsig = {t: esm.sig_of(t) for t in targets}
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
                if _is_ref(rules, sig, sub, rel - a, data[a:z], tsig[t]):
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
