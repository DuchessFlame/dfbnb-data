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


class Record:
    __slots__ = ("sig", "fid", "subs")

    def __init__(self, sig, fid, subs):
        self.sig, self.fid, self.subs = sig, fid, subs

    def first(self, sub):
        for k, v in self.subs:
            if k == sub:
                return v
        return None

    def all(self, sub):
        return [v for k, v in self.subs if k == sub]

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
        self._load(want_index)
        self.strings = load_strings(os.path.join(data_dir, "SeventySix - Localization.ba2"))

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
                    data = f.read(ds)
                    keep = sig in sigs
                    if not keep and not want_index:
                        continue
                    if flags & 0x40000:
                        data = zlib.decompress(data[4:])
                    if keep:
                        subs = _parse_subs(data)
                        records[sig.decode()].append(Record(sig.decode(), fid, subs))
                        ed = next((v for k, v in subs if k == "EDID"), b"")
                        fullv = next((v for k, v in subs if k == "FULL"), None)
                    else:
                        ed = b""
                        fullv = None
                        if data[:4] == b"EDID":
                            ln = unpack("<H", data[4:6])[0]
                            ed = data[6:6 + ln]
                        # FULL is usually right after EDID (and OBND/VMAD); scan a little
                        p = 0
                        while p + 6 <= len(data) and p < 65536:
                            ss = data[p:p + 4]
                            ln = unpack("<H", data[p + 4:p + 6])[0]
                            if ss == b"FULL":
                                fullv = data[p + 6:p + 6 + ln]
                                break
                            if ss == b"XXXX":
                                break
                            p += 6 + ln
                    index[fid] = (sig.decode(), ed.rstrip(b"\0").decode("utf-8", "replace"))
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
        return self.lstring(self._full.get(fid))

    def sig_of(self, fid):
        return self.index.get(fid, ("", ""))[0]

    def edid_of(self, fid):
        return self.index.get(fid, ("", ""))[1]

    def exists(self, fid):
        return fid in self.index

    def link_edit_value(self, fid):
        """xEdit's edit value for a FormID field:  EDID "FULL" [SIG:FORMID]"""
        if fid == 0:
            return "NULL - Null Reference [00000000]"
        if fid not in self.index:
            return "[%08X] < Error: Could not be resolved >" % fid
        sig, ed = self.index[fid]
        full = self.full_of(fid)
        s = ed
        if full:
            s += ' "%s"' % full
        return "%s [%s:%08X]" % (s, sig, fid)


def xedit_order(esm, sig):
    """Records in the order xEdit's Process() sees them (newest-in-file first)."""
    return list(reversed(esm.records[sig]))


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
