"""FormID offsets inside a VMAD subrecord (xEdit wbVMAD* definitions for Fallout 76)."""
import struct

FRAG_PERK_STYLE = {"PERK", "TERM"}


class _P:
    def __init__(self, d):
        self.d, self.p, self.out = d, 0, []

    def u8(self):
        v = self.d[self.p]; self.p += 1; return v

    def u16(self):
        v = struct.unpack_from("<H", self.d, self.p)[0]; self.p += 2; return v

    def u32(self):
        v = struct.unpack_from("<I", self.d, self.p)[0]; self.p += 4; return v

    def cnt(self):
        v = self.u32()
        if v > len(self.d):
            raise ValueError("bad count")
        return v

    def lstr(self):
        n = self.u16(); self.p += n
        if self.p > len(self.d):
            raise ValueError("past end")

    def obj(self, fmt):
        if self.p + 8 > len(self.d):
            raise ValueError("past end")
        self.out.append(self.p + (4 if fmt == 2 else 0)); self.p += 8

    def value(self, t, fmt):
        if t == 1:
            self.obj(fmt)
        elif t == 2:
            self.lstr()
        elif t in (3, 4):
            self.p += 4
        elif t == 5:
            self.p += 1
        elif t == 7:
            self.struct(fmt)
        elif t == 11:
            for _ in range(self.cnt()):
                self.obj(fmt)
        elif t == 12:
            for _ in range(self.cnt()):
                self.lstr()
        elif t in (13, 14):
            n = self.cnt(); self.p += 4 * n
            if self.p > len(self.d): raise ValueError("past end")
        elif t == 15:
            n = self.cnt(); self.p += n
            if self.p > len(self.d): raise ValueError("past end")
        elif t == 16:
            self.p += 4
        elif t == 17:
            for _ in range(self.cnt()):
                self.struct(fmt)
        elif t in (0, 6):
            pass
        else:
            raise ValueError("prop type %d" % t)

    def struct(self, fmt):
        for _ in range(self.cnt()):
            self.lstr(); t = self.u8(); self.u8(); self.value(t, fmt)

    def props(self, fmt):
        for _ in range(self.u16()):
            self.lstr(); t = self.u8(); self.u8(); self.value(t, fmt)

    def script(self, fmt):
        self.lstr(); self.u8(); self.props(fmt)

    def scripts(self, fmt):
        for _ in range(self.u16()):
            self.script(fmt)


def formid_offsets(sig, data):
    """Offsets of every FormID xEdit reads in this VMAD. Raises on data it can't parse."""
    p = _P(data)
    p.u16()
    fmt = struct.unpack_from("<h", data, 2)[0]; p.p = 4
    p.scripts(fmt)
    if p.p >= len(data):
        return p.out
    if sig in ("INFO", "PACK", "SCEN"):
        p.u8(); flags = p.u8(); p.script(fmt)
        nbits = 3 if sig == "PACK" else 2
        for i in range(nbits):
            if flags >> i & 1:
                p.u8(); p.lstr(); p.lstr()
        if sig == "SCEN":
            for _ in range(p.u16()):
                p.u8(); p.u32(); p.u8(); p.lstr(); p.lstr()
    elif sig in FRAG_PERK_STYLE:
        p.u8(); p.script(fmt)
        if p.p + 2 > len(data):
            return p.out
        for _ in range(p.u16()):
            p.u32(); p.u8(); p.lstr(); p.lstr()
    elif sig == "QUST":
        p.u8(); n = p.u16()
        ln = p.u16(); p.p += ln
        if ln:
            p.u8(); p.props(fmt)
        for _ in range(n):
            p.u32(); p.u32(); p.u8(); p.lstr(); p.lstr()
        for _ in range(p.u16()):
            p.obj(fmt); p.u16(); afmt = struct.unpack_from("<h", data, p.p)[0]; p.p += 2
            p.scripts(afmt)
    return p.out
