#!/usr/bin/env python3
"""Export the INNR (instance naming rules) records straight from the game files.

Why: the name a unique weapon/armour shows in game is NOT its custom mod's FULL
("Vault 63 Gatling Laser") and not its EditorID. The mod adds a keyword
(CustomItemName_V63GatlingLaser) and the weapon's naming rules (INNR
dn_CommonGun / dn_CommonMelee / dn_CommonArmor) turn that keyword into the
text the player sees ("V63-HELGA"). This script reads those rules from
SeventySix.esm + the English strings in "SeventySix - Localization.ba2" and
writes tsv/INNR_Export_<Month>_<Year>.tsv for build_unique_weapons_armour_json.py.

Run on the PC with the game installed (takes about a second):
    python src/export_innr_from_esm.py "D:/steamlibrary/steamapps/common/Fallout76/Data"
    python src/export_innr_from_esm.py ".../Fallout 76 Playtest/Data" --out tsv/pts

Also writes INNR_Export_<Month>_<Year>_Users.tsv: which naming rules each WEAP /
ARMO uses (its INRD), because a melee weapon and a gun can spell the same
unique differently (Cauterizer / The Cauterizer).

Columns: INNR_FormID INNR_EDID RulesetIndex RuleIndex Text KeywordCount Keywords
(Keywords = "EDID [KYWD:FormID] | ...", same style as the xEdit exports).
Read-only: never writes to the game folder.
"""
import argparse
import os
import struct
import sys
import zlib
from datetime import date

WANT = {b"KYWD", b"INNR", b"WEAP", b"ARMO"}


def _subs(data):
    i, big = 0, None
    while i + 6 <= len(data):
        t = data[i:i + 4]
        sz = struct.unpack_from("<H", data, i + 4)[0]
        i += 6
        if t == b"XXXX":
            big = struct.unpack_from("<I", data, i)[0]
            i += sz
            continue
        if big is not None:
            sz, big = big, None
        yield t, data[i:i + sz]
        i += sz


def scan_esm(path):
    kywd, innr, users = {}, [], []
    with open(path, "rb") as f:
        f.seek(0, 2)
        end = f.tell()
        f.seek(0)
        h = f.read(24)
        f.seek(24 + struct.unpack_from("<I", h, 4)[0])        # skip TES4 header
        while f.tell() < end:
            h = f.read(24)
            if len(h) < 24:
                break
            gsz = struct.unpack_from("<I", h, 4)[0]
            start = f.tell() - 24
            if h[8:12] not in WANT:
                f.seek(start + gsz)
                continue
            gend = start + gsz
            while f.tell() < gend:
                h = f.read(24)
                sig = h[:4]
                if sig == b"GRUP":
                    continue
                dsz, flags, fid = struct.unpack_from("<III", h, 4)
                data = f.read(dsz)
                if flags & 0x40000:
                    data = zlib.decompress(data[4:])
                if sig == b"KYWD":
                    for t, d in _subs(data):
                        if t == b"EDID":
                            kywd["%08X" % fid] = d.rstrip(b"\0").decode("latin1")
                            break
                elif sig in (b"WEAP", b"ARMO"):
                    ed, inrd = "", ""
                    for t, d in _subs(data):
                        if t == b"EDID":
                            ed = d.rstrip(b"\0").decode("latin1")
                        elif t == b"INRD" and len(d) == 4:
                            inrd = "%08X" % struct.unpack("<I", d)[0]
                    if inrd:
                        users.append((sig.decode(), "%08X" % fid, ed, inrd))
                elif sig == b"INNR":
                    rec = {"fid": "%08X" % fid, "edid": "", "rules": []}
                    rs, ri = -1, 0
                    cur = None
                    for t, d in _subs(data):
                        if t == b"EDID":
                            rec["edid"] = d.rstrip(b"\0").decode("latin1")
                        elif t == b"VNAM":
                            rs += 1
                            ri = 0
                        elif t == b"WNAM":
                            cur = {"rs": rs, "ri": ri, "kw": [],
                                   "sid": struct.unpack("<I", d[:4])[0] if len(d) == 4 else None,
                                   "raw": None if len(d) == 4 else d.rstrip(b"\0").decode("utf-8", "replace")}
                            ri += 1
                            rec["rules"].append(cur)
                        elif t == b"KWDA" and cur is not None:
                            cur["kw"] = ["%08X" % x for x in struct.unpack("<%dI" % (len(d) // 4), d)]
                    innr.append(rec)
    return kywd, innr, users


def load_strings(ba2_path, want="seventysix_en.strings"):
    with open(ba2_path, "rb") as f:
        _mag, _ver, _typ, n, nto = struct.unpack("<4sI4sIQ", f.read(24))
        ents = [struct.unpack("<I4sIIQIII", f.read(36)) for _ in range(n)]
        f.seek(nto)
        names = []
        for _ in range(n):
            ln = struct.unpack("<H", f.read(2))[0]
            names.append(f.read(ln).decode("latin1"))
        for nm, e in zip(names, ents):
            if nm.lower().replace("\\", "/").endswith(want):
                off, packed, unp = e[4], e[5], e[6]
                f.seek(off)
                b = f.read(packed or unp)
                b = zlib.decompress(b) if packed else b
                break
        else:
            sys.exit(f"{want} not found in {ba2_path}")
    cnt = struct.unpack_from("<I", b, 0)[0]
    base = 8 + cnt * 8
    out = {}
    for i in range(cnt):
        sid, off = struct.unpack_from("<II", b, 8 + i * 8)
        s = base + off
        out[sid] = b[s:b.index(b"\0", s)].decode("utf-8", "replace")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir", help="the game's Data folder")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "tsv"))
    ap.add_argument("--stamp", default=date.today().strftime("%B_%Y"))
    a = ap.parse_args()
    kywd, innr, users = scan_esm(os.path.join(a.data_dir, "SeventySix.esm"))
    strings = load_strings(os.path.join(a.data_dir, "SeventySix - Localization.ba2"))
    path = os.path.join(a.out, f"INNR_Export_{a.stamp}.tsv")
    n = 0
    with open(path, "w", encoding="utf-8", newline="\n") as w:
        w.write("INNR_FormID\tINNR_EDID\tRulesetIndex\tRuleIndex\tText\tKeywordCount\tKeywords\n")
        for r in innr:
            for ru in r["rules"]:
                text = ru["raw"] if ru["sid"] is None else (strings.get(ru["sid"], "") if ru["sid"] else "")
                text = text.replace("\t", " ").replace("\n", " ")
                kws = " | ".join(f'{kywd.get(k, "")} [KYWD:{k}]' for k in ru["kw"])
                w.write(f'{r["fid"]}\t{r["edid"]}\t{ru["rs"]}\t{ru["ri"]}\t{text}\t{len(ru["kw"])}\t{kws}\n')
                n += 1
    upath = os.path.join(a.out, f"INNR_Export_{a.stamp}_Users.tsv")
    iname = {r["fid"]: r["edid"] for r in innr}
    with open(upath, "w", encoding="utf-8", newline="\n") as w:
        w.write("Signature\tFormID\tEDID\tINRD_FormID\tINRD_EDID\n")
        for sig, fid, ed, inrd in users:
            w.write(f"{sig}\t{fid}\t{ed}\t{inrd}\t{iname.get(inrd, '')}\n")
    print(f"wrote {len(users)} WEAP/ARMO naming-rule links -> {upath}")
    print(f"wrote {n} rules from {len(innr)} INNR records -> {path}")


if __name__ == "__main__":
    main()
