#!/usr/bin/env python3
r"""
export_term_from_esm.py — Personal Terminal TERM export, straight from SeventySix.esm.

Writes exactly what  xedit scripts\!!!Wordpress - ExportTERMToTSV.pas  writes,
for when xEdit isn't open:

    tsv/TERM_Export_<Month>_<Year>.tsv           one row per header / body / menu item
    tsv/TERM_Export_<Month>_<Year>_Quests.tsv    quests the terminal's conditions use

    --pts   writes tsv/pts/TERM_Export_PTS_<yyyy-mm-dd_hhnn>[ _Quests].tsv instead
            (point --data at the PTS install's Data folder)

Run it on the PC that has the game installed:
    python tools/export_term_from_esm.py --data "D:\steamlibrary\steamapps\common\Fallout76\Data"

Only TERM records whose EDID starts with --prefix (default X01X_PlayerTerminal)
are exported, plus anything those menus link to.

Oct 2026: also the token-exchange terminals (EDID has "Redeem", or "Exchange"+"Terminal")
for the Currency pages. NOT YET HERE: the .pas also writes ..._Placements.tsv (where those
terminals stand). Run the .pas for that file.

TSV SCHEMA (shared with the .pas — keep both in step)
-----------------------------------------------------
Main:   TERM_FormID  TERM_EDID  Row  Idx  Text  ItemType  Target_FormID  Display  Conditions
        Row = HEADER | BODY | ITEM.  ItemType = ANAM number (4 submenu, 8 display text).
        Conditions = " && "-joined  Function|Op|Value|ParamFormID|ParamEDID|ParamSig
Quests: QUST_FormID  QUST_EDID  FULL  Objectives  FirstLog  Referenced
        Objectives = " || "-joined.  Referenced = 1 when a TERM condition names it.
Text is escaped: newline -> \n, tab -> \t, backslash -> \\ ; every value quoted.
"""
from __future__ import annotations

import argparse
import datetime
import os
import struct
import sys
import zlib

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPS = ["==", "!=", ">", ">=", "<", "<="]


# ------------------------------------------------------------ strings ------
def load_strings(ba2_path):
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
def load_records(esm_path, sigs):
    """{formid: (sig, [(subsig, bytes)])} for top-level groups in `sigs`."""
    res = {}
    with open(esm_path, "rb") as f:
        size = os.path.getsize(esm_path)

        def walk(end):
            while f.tell() < end:
                hdr = f.read(24)
                sig = hdr[:4]
                ds = struct.unpack("<I", hdr[4:8])[0]
                if sig == b"GRUP":
                    label = hdr[8:12]
                    gtype = struct.unpack("<i", hdr[12:16])[0]
                    if gtype == 0 and label not in sigs:
                        f.seek(ds - 24, 1)
                        continue
                    walk(f.tell() + ds - 24)
                    continue
                flags, fid = struct.unpack("<II", hdr[8:16])
                data = f.read(ds)
                if sig not in sigs:
                    continue
                if flags & 0x40000:
                    data = zlib.decompress(data[4:])
                subs, p, big = [], 0, None
                while p < len(data):
                    ss = data[p:p + 4]
                    sz = struct.unpack("<H", data[p + 4:p + 6])[0]
                    p += 6
                    if ss == b"XXXX":
                        big = struct.unpack("<I", data[p:p + sz])[0]
                        p += sz
                        continue
                    if big is not None:
                        sz, big = big, None
                    subs.append((ss.decode("latin-1"), data[p:p + sz]))
                    p += sz
                res[fid] = (sig.decode(), subs)

        walk(size)
    return res


def edid_of(subs):
    for k, v in subs:
        if k == "EDID":
            return v.rstrip(b"\0").decode("utf-8", "replace")
    return ""


# -------------------------------------------------------------- write ------
def esc(s):
    s = (s or "").replace("\\", "\\\\").replace("\r\n", "\n").replace("\r", "")
    s = s.replace("\n", "\\n").replace("\t", "\\t")
    return '"' + s.replace('"', '""') + '"'


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="Fallout76 Data folder")
    ap.add_argument("--prefix", default="X01X_PlayerTerminal")
    ap.add_argument("--pts", action="store_true", help="write a PTS-channel export into tsv/pts/")
    ap.add_argument("--outdir", help="override output folder")
    a = ap.parse_args(argv)

    esm = os.path.join(a.data, "SeventySix.esm")
    ba2 = os.path.join(a.data, "SeventySix - Localization.ba2")
    S = load_strings(ba2)
    R = load_records(esm, {b"TERM", b"QUST", b"GLOB", b"KYWD", b"AVIF"})
    edids = {fid: edid_of(subs) for fid, (_s, subs) in R.items()}

    def lstr(b):
        if len(b) == 4:
            return S.get(struct.unpack("<I", b)[0], "")
        return b.rstrip(b"\0").decode("utf-8", "replace")

    def cond(b):
        flags = b[0]
        val = struct.unpack("<f", b[4:8])[0]
        func = struct.unpack("<H", b[8:10])[0]
        p1 = struct.unpack("<I", b[12:16])[0]
        if p1 in R:
            pfid, ped, psig = f"{p1:08X}", edids.get(p1, ""), R[p1][0]
        else:
            pfid, ped, psig = (f"{p1:08X}" if p1 else ""), "", ""
        v = f"{val:g}"
        return f"Func{func}|{OPS[(flags >> 5) % 6]}|{v}|{pfid}|{ped}|{psig}"

    # TERM records to export: prefix family + anything they link to.
    # + the token-exchange terminals (Currency pages) - keep in step with
    #   IsExchangeTerm in the .pas
    def is_exchange(ed):
        return "Redeem" in ed or ("Exchange" in ed and "Terminal" in ed)
    terms = {fid for fid, (s, subs) in R.items() if s == "TERM" and
             (edids[fid].startswith(a.prefix) or is_exchange(edids[fid]))}
    rows, referenced = [], set()
    queue = sorted(terms)
    seen = set()
    while queue:
        fid = queue.pop(0)
        if fid in seen or fid not in R or R[fid][0] != "TERM":
            continue
        seen.add(fid)
        subs = R[fid][1]
        ed = edids[fid]
        cur = None          # current row dict that CTDA / ANAM / TNAM / UNAM attach to
        n_body = n_item = 0
        out = []
        for k, v in subs:
            if k == "NAM0":
                out.append({"Row": "HEADER", "Idx": 0, "Text": lstr(v), "conds": []})
                cur = None
            elif k == "BTXT":
                cur = {"Row": "BODY", "Idx": n_body, "Text": lstr(v), "conds": []}
                n_body += 1
                out.append(cur)
            elif k == "ITXT":
                cur = {"Row": "ITEM", "Idx": n_item, "Text": lstr(v), "conds": [], "type": "", "target": "", "display": ""}
                n_item += 1
                out.append(cur)
            elif cur is not None and cur["Row"] == "ITEM" and k == "ANAM" and v:
                cur["type"] = str(v[0])
            elif cur is not None and cur["Row"] == "ITEM" and k == "TNAM" and len(v) >= 4:
                t = struct.unpack("<I", v[:4])[0]
                if t:
                    cur["target"] = f"{t:08X}"
                    queue.append(t)
            elif cur is not None and cur["Row"] == "ITEM" and k == "UNAM":
                cur["display"] = lstr(v)
            elif cur is not None and k == "CTDA" and len(v) >= 16:
                c = cond(v)
                cur["conds"].append(c)
                p = c.split("|")
                if p[5] == "QUST":
                    referenced.add(int(p[3], 16))
        for r in out:
            rows.append([f"{fid:08X}", ed, r["Row"], str(r["Idx"]), r["Text"], r.get("type", ""),
                         r.get("target", ""), r.get("display", ""), " && ".join(r["conds"])])

    # Quests: every quest a condition names, plus its "family" (same EDID stem),
    # so a misc-pointer quest can be tied to the real quest it leads to.
    def stem(e):
        parts = e.split("_")
        if len(parts) > 1 and "misc" in parts[-1].lower():
            parts = parts[:-1]
        return "_".join(parts)

    stems = {stem(edids[q]) for q in referenced if q in edids}
    quests = []
    for fid, (s, subs) in sorted(R.items()):
        if s != "QUST":
            continue
        ed = edids[fid]
        if fid not in referenced and not any(ed == st or ed.startswith(st + "_") for st in stems):
            continue
        full = next((lstr(v) for k, v in subs if k == "FULL"), "")
        objs = [lstr(v) for k, v in subs if k == "NNAM"]
        logs = [lstr(v) for k, v in subs if k == "CNAM"]
        quests.append([f"{fid:08X}", ed, full, " || ".join(o for o in objs if o),
                       next((l for l in logs if l), ""), "1" if fid in referenced else "0"])

    now = datetime.datetime.now()
    if a.pts:
        outdir = a.outdir or os.path.join(REPO, "tsv", "pts")
        base = f"TERM_Export_PTS_{now:%Y-%m-%d_%H%M}"
    else:
        outdir = a.outdir or os.path.join(REPO, "tsv")
        base = f"TERM_Export_{now:%B_%Y}"
    os.makedirs(outdir, exist_ok=True)
    main_path = os.path.join(outdir, base + ".tsv")
    q_path = os.path.join(outdir, base + "_Quests.tsv")
    with open(main_path, "w", encoding="utf-8", newline="\r\n") as f:
        f.write("\t".join(["TERM_FormID", "TERM_EDID", "Row", "Idx", "Text", "ItemType",
                           "Target_FormID", "Display", "Conditions"]) + "\n")
        for r in rows:
            f.write("\t".join(esc(x) for x in r) + "\n")
    with open(q_path, "w", encoding="utf-8", newline="\r\n") as f:
        f.write("\t".join(["QUST_FormID", "QUST_EDID", "FULL", "Objectives", "FirstLog", "Referenced"]) + "\n")
        for r in quests:
            f.write("\t".join(esc(x) for x in r) + "\n")

    # Same one-line export ledger the .pas scripts keep (tsv/_exports.tsv).
    # Skipped for --outdir test runs so they never look like a real export.
    try:
        if a.outdir:
            raise OSError
        ledger = os.path.join(REPO, "tsv", "_exports.tsv")
        new = not os.path.exists(ledger)
        with open(ledger, "a", encoding="utf-8", newline="\r\n") as f:
            if new:
                f.write("channel\trecord_type\texported_at\tfile\n")
            f.write(f"{'PTS' if a.pts else 'LIVE'}\tTERM\t{now:%Y-%m-%d %H:%M}\t{os.path.basename(main_path)}\n")
    except OSError:
        pass

    print(f"[TERM export] {len(seen)} terminals, {len(rows)} rows -> {main_path}")
    print(f"[TERM export] {len(quests)} quests -> {q_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
