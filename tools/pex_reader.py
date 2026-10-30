#!/usr/bin/env python3
r"""
pex_reader.py - read Fallout 76's Papyrus scripts straight from the game files
(29 Oct 2026).

Fallout 76 ships only the CLIENT half of its scripts, compiled (.pex, no .psc
source), in "SeventySix - MiscClient.ba2" (about 7,200 under scripts/client/).
The server half (legendary rolls, Brutalist's, Chemist's ...) is not on the PC,
so many client scripts are stubs: properties and empty functions only.

This tool pulls them out of the archive and turns each one into readable text:
the script it extends, its structs, variables (with their default values),
properties, and every function with its instructions (the "assembly", e.g.
`callstatic utility RandomInt dieRoll 1 100` / `cmp_le ...`).

Note: a property's real value is set on the record that uses the script
(VMAD in SeventySix.esm), not in the .pex. The .pex only has the default.

Format notes (FO76 PEX version 3.15, game id 3): the same layout as Fallout 4,
except every variable and struct member has ONE extra byte after its value.

Usage (on the PC, from dfbnb-data):
  python tools\pex_reader.py list  [pattern]                 names in the archive
  python tools\pex_reader.py show  <pattern>                 print matching scripts
  python tools\pex_reader.py dump  [pattern] --out <folder>  one .txt per script
  python tools\pex_reader.py grep  <text>                    which scripts mention it
  python tools\pex_reader.py diff  [pattern]                 live vs PTS (ignores build paths)
  python tools\pex_reader.py export [--pts]                  every script into ONE text file:
                                                             papyrus\papyrus_live.txt / papyrus_pts.txt
                                                             (not in git; run after a patch / PTS update)
Options:
  --pts            read the Playtest install instead of live
  --data <folder>  game Data folder (default: the live / PTS install)
  pattern          a regular expression on the path, e.g. legendary  or  ^scripts/client/player\.pex$

Examples:
  python tools\pex_reader.py show legendaryvoltaic
  python tools\pex_reader.py grep LGND_
  python tools\pex_reader.py dump legendary --out "%TEMP%\pex"
  python tools\pex_reader.py diff legendary
"""
from __future__ import annotations

import argparse
import io
import os
import re
import struct
import sys
import zlib

LIVE_DATA = r"D:\steamlibrary\steamapps\common\Fallout76\Data"
PTS_DATA = r"D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data"
ARCHIVE = "SeventySix - MiscClient.ba2"


def default_data(pts):
    p = PTS_DATA if pts else LIVE_DATA
    if os.path.isdir(p):
        return p
    # the PC's Linux shell sees the game folders under ~/mnt/
    alt = os.path.expanduser("~/mnt/Fallout 76 Playtest/Data" if pts else "~/mnt/Fallout76/Data")
    return alt if os.path.isdir(alt) else p


# ------------------------------------------------------------- BA2 (GNRL) ---
class Ba2:
    """A general (GNRL) BA2 archive: names + zlib'd files."""

    def __init__(self, path):
        self.path = path
        self.fh = open(path, "rb")
        magic, _ver, typ, n, name_off = struct.unpack("<4sI4sIQ", self.fh.read(24))
        if magic != b"BTDX" or typ != b"GNRL":
            raise SystemExit(f"{path}: not a general BA2 archive")
        recs = [struct.unpack("<I4sIIQIII", self.fh.read(36)) for _ in range(n)]
        self.fh.seek(name_off)
        names = []
        for _ in range(n):
            ln = struct.unpack("<H", self.fh.read(2))[0]
            names.append(self.fh.read(ln).decode("utf-8", "replace").replace("\\", "/").lower())
        self.files = dict(zip(names, recs))

    def read(self, name):
        _h, _e, _d, _f, off, packed, size, _a = self.files[name]
        self.fh.seek(off)
        return zlib.decompress(self.fh.read(packed)) if packed else self.fh.read(size)

    def pex(self, pattern=""):
        rx = re.compile(pattern, re.I) if pattern else None
        return sorted(n for n in self.files if n.endswith(".pex") and (rx is None or rx.search(n)))


# ------------------------------------------------------------- PEX ----------
OPS = {0: ("nop", 0, 0), 1: ("iadd", 3, 0), 2: ("fadd", 3, 0), 3: ("isub", 3, 0), 4: ("fsub", 3, 0),
       5: ("imul", 3, 0), 6: ("fmul", 3, 0), 7: ("idiv", 3, 0), 8: ("fdiv", 3, 0), 9: ("imod", 3, 0),
       10: ("not", 2, 0), 11: ("ineg", 2, 0), 12: ("fneg", 2, 0), 13: ("assign", 2, 0), 14: ("cast", 2, 0),
       15: ("cmp_eq", 3, 0), 16: ("cmp_lt", 3, 0), 17: ("cmp_le", 3, 0), 18: ("cmp_gt", 3, 0), 19: ("cmp_ge", 3, 0),
       20: ("jmp", 1, 0), 21: ("jmpt", 2, 0), 22: ("jmpf", 2, 0), 23: ("callmethod", 3, 1),
       24: ("callparent", 2, 1), 25: ("callstatic", 3, 1), 26: ("return", 1, 0), 27: ("strcat", 3, 0),
       28: ("propget", 3, 0), 29: ("propset", 3, 0), 30: ("array_create", 2, 0), 31: ("array_length", 2, 0),
       32: ("array_getelement", 3, 0), 33: ("array_setelement", 3, 0), 34: ("array_findelement", 4, 0),
       35: ("array_rfindelement", 4, 0), 36: ("is", 3, 0), 37: ("struct_create", 1, 0), 38: ("struct_get", 3, 0),
       39: ("struct_set", 3, 0), 40: ("array_findstruct", 5, 0), 41: ("array_rfindstruct", 5, 0),
       42: ("array_add", 3, 0), 43: ("array_insert", 3, 0), 44: ("array_removelast", 1, 0),
       45: ("array_remove", 3, 0), 46: ("array_clear", 1, 0), 47: ("array_getallmatchingstructs", 6, 0),
       48: ("lock_guards", 0, 1), 49: ("unlock_guards", 0, 1), 50: ("try_lock_guards", 1, 1)}


class _R:
    def __init__(self, b):
        self.b, self.p = b, 0

    def u(self, f):
        v = struct.unpack_from("<" + f, self.b, self.p)
        self.p += struct.calcsize("<" + f)
        return v[0]

    def ws(self):
        n = self.u("H")
        v = self.b[self.p:self.p + n].decode("utf-8", "replace")
        self.p += n
        return v


def disassemble(data, with_source=True):
    """one .pex (bytes) -> readable text"""
    r = _R(data)
    out = io.StringIO()
    if r.u("I") != 0xFA57C0DE:
        return "(not a .pex file)\n"
    major, minor, game = r.u("B"), r.u("B"), r.u("H")
    r.u("Q")
    src = r.ws()
    r.ws()
    r.ws()
    S = [r.ws() for _ in range(r.u("H"))]

    def st():
        return S[r.u("H")]

    def var():
        t = r.u("B")
        if t == 0:
            return "None"
        if t == 1:
            return S[r.u("H")]
        if t == 2:
            return repr(S[r.u("H")])
        if t == 3:
            return str(r.u("i"))
        if t == 4:
            return repr(round(r.u("f"), 6))
        if t == 5:
            return str(bool(r.u("B")))
        raise ValueError(f"unknown value type {t}")

    if r.u("B"):                                   # debug info: skipped
        r.u("Q")
        for _ in range(r.u("H")):
            r.u("H"), r.u("H"), r.u("H"), r.u("B")
            n = r.u("H")
            r.p += 2 * n
        for _ in range(r.u("H")):
            r.u("H"), r.u("H"), r.u("H"), r.u("I")
            n = r.u("H")
            r.p += 2 * n
        for _ in range(r.u("H")):
            r.u("H"), r.u("H")
            n = r.u("H")
            r.p += 2 * n
    for _ in range(r.u("H")):                      # user flags
        r.u("H"), r.u("B")

    def func(name):
        ret = st()
        st()
        r.u("I")
        fl = r.u("B")
        ps = [(st(), st()) for _ in range(r.u("H"))]
        ls = [(st(), st()) for _ in range(r.u("H"))]
        out.write(f"  FUNCTION {name}({', '.join(t + ' ' + n for n, t in ps)}) -> {ret}"
                  + (" [native]" if fl & 2 else "") + (" [global]" if fl & 1 else "") + "\n")
        if ls:
            out.write("    locals: " + ", ".join(t + " " + n for n, t in ls) + "\n")
        for i in range(r.u("H")):
            nm, na, va = OPS[r.u("B")]
            a = [var() for _ in range(na)]
            if va:
                a += [var() for _ in range(int(var()))]
            out.write(f"    {i:3d} {nm} " + " ".join(a) + "\n")

    for _ in range(r.u("H")):
        name = st()
        r.u("I")
        parent = st()
        st()
        r.u("B")
        r.u("I")
        st()
        out.write(f"SCRIPT {name} extends {parent}" + (f"   ({os.path.basename(src)}, pex {major}.{minor})" if with_source else "") + "\n")
        for _ in range(r.u("H")):
            out.write(f" STRUCT {st()}\n")
            for _ in range(r.u("H")):
                mn, mt = st(), st()
                r.u("I")
                d = var()
                r.u("B")
                st()
                out.write(f"   {mt} {mn} = {d}\n")
        for _ in range(r.u("H")):
            n, t = st(), st()
            r.u("I")
            d = var()
            r.u("B")                               # FO76: one extra byte per variable
            out.write(f" VAR {t} {n} = {d}\n")
        for _ in range(r.u("H")):
            n, t = st(), st()
            st()
            r.u("I")
            f = r.u("B")
            out.write(f" PROPERTY {t} {n}\n")
            if f & 4:
                st()
            else:
                if f & 1:
                    func(n + ".get")
                if f & 2:
                    func(n + ".set")
        for _ in range(r.u("H")):
            out.write(f' STATE "{st()}"\n')
            for _ in range(r.u("H")):
                func(st())
    return out.getvalue()


def _sorted_text(text):
    """for diffs: drop the build path line and sort declarations (the compiler
    reorders variables / properties between builds)"""
    lines = text.splitlines()
    head = [l for l in lines if l.startswith(("SCRIPT", " VAR", " PROPERTY", " STRUCT", "   "))
            and not l.startswith("    ")]
    body = [l for l in lines if l not in head]
    head = [re.sub(r"\s+\(.*\)$", "", l) for l in head]
    return sorted(head) + body


# ------------------------------------------------------------- CLI ----------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["list", "show", "dump", "grep", "diff", "export"])
    ap.add_argument("pattern", nargs="?", default="")
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--data")
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    data = a.data or default_data(a.pts)
    arc = os.path.join(data, ARCHIVE)
    if not os.path.isfile(arc):
        sys.exit(f"{arc} not found - use --data")
    ba = Ba2(arc)

    if a.cmd == "list":
        names = ba.pex(a.pattern)
        print("\n".join(names))
        print(f"({len(names)} scripts)", file=sys.stderr)
    elif a.cmd == "show":
        if not a.pattern:
            sys.exit("show needs a pattern")
        for n in ba.pex(a.pattern):
            print(f"===== {n}")
            print(disassemble(ba.read(n)))
    elif a.cmd == "dump":
        if not a.out:
            sys.exit("dump needs --out <folder>")
        os.makedirs(a.out, exist_ok=True)
        names, bad = ba.pex(a.pattern), 0
        for n in names:
            try:
                txt = disassemble(ba.read(n))
            except Exception as e:
                txt, bad = f"(could not read: {e})\n", bad + 1
            dest = os.path.join(a.out, *n[len("scripts/"):-4].split("/")) + ".txt"
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w", encoding="utf-8") as f:
                f.write(txt)
        print(f"wrote {len(names)} scripts to {a.out}" + (f" ({bad} could not be read)" if bad else ""))
    elif a.cmd == "export":
        # one searchable text file per channel, outside git (papyrus/ is in .gitignore)
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out_dir = a.out or os.path.join(here, "papyrus")
        os.makedirs(out_dir, exist_ok=True)
        dest = os.path.join(out_dir, "papyrus_pts.txt" if a.pts else "papyrus_live.txt")
        names, bad = ba.pex(a.pattern), 0
        with open(dest, "w", encoding="utf-8") as f:
            f.write(f"# Fallout 76 client Papyrus scripts ({'PTS' if a.pts else 'live'}) - {arc}\n"
                    f"# written by tools/pex_reader.py export; {len(names)} scripts; search for '===== scripts/...'\n\n")
            for n in names:
                try:
                    txt = disassemble(ba.read(n))
                except Exception as e:
                    txt, bad = f"(could not read: {e})\n", bad + 1
                f.write(f"===== {n}\n{txt}\n")
        print(f"wrote {len(names)} scripts to {dest} ({os.path.getsize(dest) // 1024} KB)"
              + (f", {bad} could not be read" if bad else ""))
    elif a.cmd == "grep":
        if not a.pattern:
            sys.exit("grep needs some text")
        rx = re.compile(re.escape(a.pattern), re.I)
        hits = 0
        for n in ba.pex():
            raw = ba.read(n)
            if not rx.search(raw.decode("latin-1")):
                continue
            hits += 1
            lines = [l.strip() for l in disassemble(raw).splitlines() if rx.search(l)]
            print(f"{n}: " + ("  |  ".join(lines[:4]) if lines else "(in the string table)"))
        print(f"({hits} scripts)", file=sys.stderr)
    elif a.cmd == "diff":
        other = Ba2(os.path.join(a.data or default_data(not a.pts), ARCHIVE))
        live, pts = (other, ba) if a.pts else (ba, other)
        L, P = set(live.pex(a.pattern)), set(pts.pex(a.pattern))
        for n in sorted(P - L):
            print(f"NEW on PTS   {n}")
        for n in sorted(L - P):
            print(f"GONE on PTS  {n}")
        changed = 0
        for n in sorted(L & P):
            x, y = _sorted_text(disassemble(live.read(n))), _sorted_text(disassemble(pts.read(n)))
            if x != y:
                changed += 1
                print(f"CHANGED      {n}")
                import difflib
                for d in list(difflib.unified_diff(x, y, "live", "pts", lineterm="", n=0))[2:40]:
                    print("    " + d)
        print(f"({len(P - L)} new, {len(L - P)} gone, {changed} changed)", file=sys.stderr)


if __name__ == "__main__":
    main()
