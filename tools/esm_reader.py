#!/usr/bin/env python3
r"""
esm_reader.py - shared reader for the Python versions of the xEdit exports.

Reads SeventySix.esm and the English strings straight from the game folder,
so the exports run without xEdit. One pass over the ESM loads every record
type the chosen exports need, plus a light FormID -> (Signature, EDID) index
used to print links the way xEdit does.
"""
from __future__ import annotations

import datetime
import math
import os
import struct
import zlib

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIVE_DATA = r"D:\steamlibrary\steamapps\common\Fallout76\Data"
PTS_DATA = r"D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data"
LEDGER = os.path.join(REPO, "tsv", "_exports.tsv")

# top-level groups never needed for links (world data; huge and slow)
_INDEX_SKIP = {b"CELL", b"WRLD", b"DIAL", b"NAVI"}


# ------------------------------------------------------------ strings ------
def load_strings(ba2_path):
    """{string_id: text} from the English .strings/.dlstrings/.ilstrings."""
    out = {}
    with open(ba2_path, "rb") as f:
        _magic, _ver, _typ, n, nto = struct.unpack("<4sI4sIQ", f.read(24))
        recs = [struct.unpack("<I4sIIQIII", f.read(36)) for _ in range(n)]
        f.seek(nto)
        names = []
        for _ in range(n):
            ln = struct.unpack("<H", f.read(2))[0]
            names.append(f.read(ln).decode("utf-8", "replace"))
        for nm, r in zip(names, recs):
            low = nm.lower().replace("\\", "/")
            if not low.startswith("strings/seventysix_en."):
                continue
            f.seek(r[4])
            data = f.read(r[5] or r[6])
            if r[5]:
                data = zlib.decompress(data)
            cnt = struct.unpack("<I", data[:4])[0]
            base = 8 + cnt * 8
            prefixed = not low.endswith(".strings")
            for i in range(cnt):
                sid, off = struct.unpack("<II", data[8 + i * 8:16 + i * 8])
                p = base + off
                if prefixed:
                    ln = struct.unpack("<I", data[p:p + 4])[0]
                    s = data[p + 4:p + 4 + ln]
                else:
                    s = data[p:data.index(b"\0", p)]
                out[sid] = s.rstrip(b"\0").decode("utf-8", "replace")
    return out


# ---------------------------------------------------------------- esm ------
def _parse_subs(data):
    subs, p, big = [], 0, None
    n = len(data)
    while p < n:
        ss = data[p:p + 4]
        sz = struct.unpack_from("<H", data, p + 4)[0]
        p += 6
        if ss == b"XXXX":
            big = struct.unpack_from("<I", data, p)[0]
            p += sz
            continue
        if big is not None:
            sz, big = big, None
        subs.append((ss.decode("latin-1"), data[p:p + sz]))
        p += sz
    return subs


def main_full(subs):
    """The record's own FULL - skipping the FULLs inside OBTE ... STOP object templates."""
    in_obt = False
    for k, v in subs:
        if k == "OBTE":
            in_obt = True
        elif k == "STOP":
            in_obt = False
        elif k == "FULL" and not in_obt:
            return v
    return None


class Record:
    __slots__ = ("sig", "fid", "subs", "flags", "version")

    def __init__(self, sig, fid, subs, flags=0, version=0):
        self.sig, self.fid, self.subs, self.flags, self.version = sig, fid, subs, flags, version

    def first(self, sub):
        for k, v in self.subs:
            if k == sub:
                return v
        return None

    def all(self, sub):
        return [v for k, v in self.subs if k == sub]

    @property
    def full(self):
        return main_full(self.subs)

    @property
    def edid(self):
        v = self.first("EDID")
        return v.rstrip(b"\0").decode("utf-8", "replace") if v else ""


class Esm:
    """records[sig] = [Record in file order]; index[fid] = (sig, edid)."""

    def __init__(self, data_dir, sigs, want_index=True):
        self.data_dir = data_dir
        self.esm_path = os.path.join(data_dir, "SeventySix.esm")
        self.sigs = {s.encode() for s in sigs}
        self.records = {s: [] for s in sigs}
        self.index = {}
        self._full = {}
        self._world = None
        self._world_cache = {}
        self._get_cache = {}
        self._load(want_index)
        self._full_inline = {}
        self._load_hardcoded()
        self.strings = load_strings(os.path.join(data_dir, "SeventySix - Localization.ba2"))

    def _load_hardcoded(self):
        """xEdit's built-in records (FormIDs below 0x800 the engine hard-codes, e.g.
        AvailableCondition1 [AVIF:00000373]). They are not in SeventySix.esm; xEdit
        ships them as Core/Hardcoded/Fallout76.esp, copied here beside this script."""
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "xedit_hardcoded_fo76.esp")
        if not os.path.isfile(path):
            return
        data = open(path, "rb").read()
        p = 24 + struct.unpack_from("<I", data, 4)[0]  # skip the TES4 header record

        def walk(p, end):
            while p < end:
                sig = data[p:p + 4]
                ds = struct.unpack_from("<I", data, p + 4)[0]
                if sig == b"GRUP":
                    walk(p + 24, p + ds)
                    p += ds
                    continue
                flags, fid = struct.unpack_from("<II", data, p + 8)
                body = data[p + 24:p + 24 + ds]
                if flags & 0x40000:
                    body = zlib.decompress(body[4:])
                subs = _parse_subs(body)
                if fid not in self.index:
                    ed = next((v for k, v in subs if k == "EDID"), b"")
                    self.index[fid] = (sig.decode(), ed.rstrip(b"\0").decode("utf-8", "replace"), None)
                    full = main_full(subs)
                    if full:
                        self._full_inline[fid] = full.rstrip(b"\0").decode("utf-8", "replace")
                p += 24 + ds

        walk(p, len(data))
        # xEdit also adds the player reference itself (wbImplementation: PLYR "PlayerRef")
        self.index.setdefault(0x14, ("PLYR", "PlayerRef", None))

    def _load(self, want_index):
        sigs, index, records = self.sigs, self.index, self.records
        unpack = struct.unpack
        with open(self.esm_path, "rb") as f:
            size = os.path.getsize(self.esm_path)

            def walk(end, top_label):
                while f.tell() < end:
                    hdr = f.read(24)
                    sig = hdr[:4]
                    ds = unpack("<I", hdr[4:8])[0]
                    if sig == b"GRUP":
                        label = hdr[8:12]
                        gtype = unpack("<i", hdr[12:16])[0]
                        if gtype == 0:
                            if label not in sigs and (not want_index or label in _INDEX_SKIP):
                                f.seek(ds - 24, 1)
                                continue
                            walk(f.tell() + ds - 24, label)
                        elif top_label in sigs:
                            walk(f.tell() + ds - 24, top_label)
                        else:
                            f.seek(ds - 24, 1)
                        continue
                    flags, fid = unpack("<II", hdr[8:16])
                    rec_off = f.tell() - 24
                    data = f.read(ds)
                    keep = sig in sigs
                    if not keep and not want_index:
                        continue
                    if flags & 0x40000:
                        data = zlib.decompress(data[4:])
                    if keep:
                        subs = _parse_subs(data)
                        records[sig.decode()].append(
                            Record(sig.decode(), fid, subs, flags, unpack("<H", hdr[20:22])[0]))
                        ed = next((v for k, v in subs if k == "EDID"), b"")
                        fullv = main_full(subs)
                    else:
                        ed = b""
                        fullv = None
                        if data[:4] == b"EDID":
                            ln = unpack("<H", data[4:6])[0]
                            ed = data[6:6 + ln]
                        # the record's own FULL (not one inside an object-template block)
                        p, in_obt = 0, False
                        while p + 6 <= len(data) and p < 65536:
                            ss = data[p:p + 4]
                            ln = unpack("<H", data[p + 4:p + 6])[0]
                            if ss == b"OBTE":
                                in_obt = True
                            elif ss == b"STOP":
                                in_obt = False
                            elif ss == b"FULL" and not in_obt:
                                fullv = data[p + 6:p + 6 + ln]
                                break
                            elif ss == b"XXXX":
                                break
                            p += 6 + ln
                    index[fid] = (sig.decode(), ed.rstrip(b"\0").decode("utf-8", "replace"), rec_off)
                    if fullv is not None:
                        self._full[fid] = fullv

            walk(size, None)

    # -------------------------------------------------- value helpers ----
    def lstring(self, raw):
        if raw is None:
            return ""
        if len(raw) == 4:
            sid = struct.unpack("<I", raw)[0]
            if sid == 0:
                return ""
            return self.strings.get(sid, "")
        return raw.rstrip(b"\0").decode("utf-8", "replace")

    def full_of(self, fid):
        if fid in self._full_inline:
            return self._full_inline[fid]
        return self.lstring(self._full.get(fid))

    def sig_of(self, fid):
        return self.index.get(fid, ("", ""))[0]

    def edid_of(self, fid):
        return self.index.get(fid, ("", ""))[1]

    def get(self, fid):
        """Any top-level record by FormID, read on demand (cached)."""
        rec = self._get_cache.get(fid)
        if rec is None:
            hit = self.index.get(fid)
            if not hit or hit[2] is None:
                return None
            rec = self._read_at(hit[2])
            self._get_cache[fid] = rec
        return rec

    def exists(self, fid):
        return fid in self.index

    def link_edit_value(self, fid):
        """xEdit's edit value for a FormID field:  EDID "FULL" [SIG:FORMID]
        (placed references:  EDID [REFR:..] (Places <base> in <cell>) )"""
        if fid == 0:
            return "NULL - Null Reference [00000000]"
        if fid not in self.index:
            w = self.world_record(fid)
            if w is None:
                return "[%08X] < Error: Could not be resolved >" % fid
            return self._world_edit_value(fid, w)
        sig, ed = self.index[fid][:2]
        return _named(ed, self.full_of(fid), sig, fid)

    # ------------------------------------------- world (CELL/WRLD) data ----
    # Placed references, cells and worldspaces live in the huge CELL/WRLD groups,
    # which the main pass skips. They are looked up only when an export links to
    # one: a one-off header walk (~20 s) maps every FormID there to its file
    # offset, cell and worldspace; only the records actually needed get decoded.
    def world_record(self, fid):
        if self._world is None:
            self._world = self._walk_world()
        hit = self._world.get(fid)
        if hit is None:
            return None
        sig, off, cell, wrld = hit
        rec = self._world_cache.get(fid)
        if rec is None:
            rec = self._read_at(off)
            self._world_cache[fid] = rec
        return sig, rec, cell, wrld

    def _walk_world(self):
        res = {}
        unpack = struct.unpack_from
        with open(self.esm_path, "rb") as f:
            size = os.path.getsize(self.esm_path)

            def walk(end, world, cell):
                while f.tell() < end:
                    hdr = f.read(24)
                    ds = unpack("<I", hdr, 4)[0]
                    if hdr[:4] == b"GRUP":
                        gt = unpack("<i", hdr, 12)[0]
                        if gt == 0:
                            if hdr[8:12] in (b"CELL", b"WRLD"):
                                walk(f.tell() + ds - 24, None, None)
                            else:
                                f.seek(ds - 24, 1)
                            continue
                        lab = unpack("<I", hdr, 8)[0]
                        walk(f.tell() + ds - 24, lab if gt == 1 else world, lab if gt == 6 else cell)
                        continue
                    res[unpack("<I", hdr, 12)[0]] = (hdr[:4].decode(), f.tell() - 24, cell, world)
                    f.seek(ds, 1)

            walk(size, None, None)
        return res

    def _read_at(self, off):
        with open(self.esm_path, "rb") as f:
            f.seek(off)
            hdr = f.read(24)
            ds = struct.unpack("<I", hdr[4:8])[0]
            flags, fid = struct.unpack("<II", hdr[8:16])
            data = f.read(ds)
        if flags & 0x40000:
            data = zlib.decompress(data[4:])
        return Record(hdr[:4].decode(), fid, _parse_subs(data), flags, struct.unpack("<H", hdr[20:22])[0])

    def _cell_edit_value(self, cfid):
        w = self.world_record(cfid)
        if w is None:
            return "[%08X] < Error: Could not be resolved >" % cfid
        sig, rec, _cell, wrld = w
        base = _named(rec.edid, self.lstring(rec.full), sig, cfid)
        if wrld:
            xclc = rec.first("XCLC")
            at = ""
            if xclc and len(xclc) >= 8 and not rec.flags & 0x400:
                at = " at %d,%d" % struct.unpack("<ii", xclc[:8])
            return "%s (in %s)%s" % (base, self.link_edit_value(wrld), at)
        return base

    def _world_edit_value(self, fid, w):
        sig, rec, cell, wrld = w
        if sig in ("WRLD",):
            return _named(rec.edid, self.lstring(rec.full), sig, fid)
        if sig == "CELL":
            return self._cell_edit_value(fid)
        head = ("%s [%s:%08X]" % (rec.edid, sig, fid)) if rec.edid else "[%s:%08X]" % (sig, fid)
        name = rec.first("NAME")
        base_fid = struct.unpack("<I", name[:4])[0] if name else 0
        info = "Places " + (self.link_edit_value(base_fid) if name else "")
        if cell:
            info += " in " + self._cell_edit_value(cell)
        cw = self.world_record(cell) if cell else None
        if cw is not None and cw[1].flags & 0x400:  # persistent cell: add the grid the ref stands in
            d = rec.first("DATA")
            if d and len(d) >= 8:
                x, y = struct.unpack("<ff", d[:8])
                info += " at %d,%d" % (math.floor(x / 4096), math.floor(y / 4096))
        # FO76 precombined meshes (xEdit TwbMainRecord.GetPrecombinedMesh)
        if cw is not None and sig in PRECOMBINABLE and base_fid >= 0x800:
            xcrp = cw[1].first("XCRP")
            if xcrp and len(xcrp) >= 4:
                refs = struct.unpack_from("<%dI" % ((len(xcrp) - 4) // 4), xcrp, 4)
                if fid in refs:
                    info += " in Precombined\\%08X\\%08Xnif" % (cell, cell)
        return "%s (%s)" % (head, info)


def xedit_order(esm, sig):
    """Records in the order xEdit's Process() sees them (newest-in-file first)."""
    return list(reversed(esm.records[sig]))


PRECOMBINABLE = {"REFR", "PGRE", "PMIS", "PARW", "PBEA", "PFLA", "PCON", "PBAR", "PHZD"}


def _named(edid, full, sig, fid):
    """xEdit's  EDID "FULL" [SIG:FORMID]  - quotes inside FULL are doubled (AnsiQuotedStr)."""
    s = edid
    if full:
        s += ' "%s"' % full.replace('"', '""')
    return ("%s [%s:%08X]" % (s, sig, fid)).lstrip(" ")


# ---------------------------------------------------------------- out ------
def clean_cell(s):
    s = (s or "").replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return s.strip(" ")


def out_name(prefix, channel, now=None):
    now = now or datetime.datetime.now()
    if channel == "PTS":
        return "%s_PTS_%s.tsv" % (prefix, now.strftime("%Y-%m-%d_%H%M"))
    return "%s_%s_%d.tsv" % (prefix, now.strftime("%B"), now.year)


def write_tsv(path, lines):
    """UTF-8 (no BOM), one row per line, LF endings (the repo's .gitattributes stores
    every .tsv as LF, so this is exactly what git keeps of an xEdit export)."""
    text = "".join(l + "\n" for l in lines)
    with open(path, "wb") as f:
        f.write(text.encode("utf-8"))


def append_ledger(channel, path, ledger=LEDGER):
    try:
        fn = os.path.basename(path)
        rec = fn.split("_", 1)[0]
        line = "%s\t%s\t%s\t%s" % (channel, rec, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), fn)
        if not os.path.exists(ledger):
            with open(ledger, "ab") as f:
                f.write(b"channel\trecord_type\texported_at\tfile\n")
        else:
            with open(ledger, "rb") as g:
                g.seek(max(0, os.path.getsize(ledger) - 1))
                if g.read(1) not in (b"\n", b""):
                    line = "\n" + line
        with open(ledger, "ab") as f:
            f.write((line + "\n").encode("utf-8"))
    except Exception:
        print("[Export] ledger write failed - the export itself is unaffected.")
