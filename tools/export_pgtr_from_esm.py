#!/usr/bin/env python3
r"""
export_pgtr_from_esm.py - PGTR (progression track) export, straight from SeventySix.esm.

The World Pets level-up tracks (WorldPets_ProgressionTrack_<Species>_NEW) are PGTR
records. xEdit has no export script for them, so this reads the ESM directly
(13 Nov 2026, for the World Pets - Leveling Rewards page).

    tsv/PGTR_Export_<Month>_<Year>.tsv          (LIVE)
    tsv/pts/PGTR_Export_PTS_<yyyy-mm-dd_hhnn>.tsv  (--pts)

Run on the PC that has the game installed:
    python tools/export_pgtr_from_esm.py          (LIVE)
    python tools/export_pgtr_from_esm.py --pts    (PTS)
    (--data "<Data folder>" only if the game is somewhere else)

ONE ROW PER REWARD SLOT. A track entry (PGTI) is one level. Its first block is the
main reward; a block opened by an empty NAM3 is the ALTERNATIVE reward the game
gives when the main one is already owned (e.g. "+500 Caps ... reward already
claimed"). Entries with no VPRR (Pet Carrier, Prowess, buff rank upgrades) still
get a row: their text + picture keyword say what they are.

Columns:
  Track_FormID  Track_EDID  Entry  Level  Slot (main|alt)
  Name  Desc  Category       NAM1 / NAM3 / NAM2 text (Name is often blank in the ESM)
  Img_KYWD  Icon_KYWD        NAM4 / NAM5 keywords (WorldPets_Img_* = the in-game picture,
                             textures/interface/petprogression/wpimg*.dds)
  Reward_FormID  Reward_EDID  the VPRR reward record (GMRW)
"""
from __future__ import annotations

import argparse
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import esm_reader as er  # noqa: E402


def rows_for(esm, rec):
    S = esm.strings
    out, cur, blk, entry = [], None, None, 0

    def new_block(slot):
        return {"Slot": slot, "Name": "", "Desc": "", "Category": "", "Img": "", "Icon": "", "Reward": 0}

    def flush():
        if cur is None:
            return
        for b in cur["blocks"]:
            out.append((cur["entry"], cur["level"], b))

    for k, v in rec.subs:
        if k == "PGTI":
            flush()
            entry += 1
            cur = {"entry": entry, "level": None, "blocks": [new_block("main")]}
            blk = cur["blocks"][0]
            continue
        if cur is None:
            continue
        if k == "NAM0" and cur["level"] is None and len(v) == 4:
            cur["level"] = struct.unpack("<I", v)[0]
            continue
        if k == "NAM3" and len(v) == 0:
            blk = new_block("alt")
            cur["blocks"].append(blk)
            continue
        if len(v) != 4:
            continue
        u = struct.unpack("<I", v)[0]
        if k == "NAM1" and not blk["Name"]:
            blk["Name"] = S.get(u, "")
        elif k == "NAM2" and not blk["Category"]:
            blk["Category"] = S.get(u, "")
        elif k == "NAM3" and not blk["Desc"]:
            blk["Desc"] = S.get(u, "")
        elif k == "NAM4" and not blk["Img"]:
            blk["Img"] = esm.edid_of(u) if u else ""
        elif k == "NAM5" and not blk["Icon"]:
            blk["Icon"] = esm.edid_of(u) if u else ""
        elif k == "VPRR" and not blk["Reward"]:
            blk["Reward"] = u
    flush()
    # an alt block that only carries the "NAM3" marker (no reward) is noise
    return [r for r in out if r[2]["Slot"] == "main" or r[2]["Reward"]]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--data", default=None)
    a = ap.parse_args(argv)
    channel = "PTS" if a.pts else "LIVE"
    data = a.data or (er.PTS_DATA if a.pts else er.LIVE_DATA)
    if not os.path.isdir(data):  # device shell: the game folders are mounted under ~/mnt
        alt = os.path.expanduser("~/mnt/" + ("Fallout 76 Playtest" if a.pts else "Fallout76") + "/Data")
        data = alt if os.path.isdir(alt) else data
    esm = er.Esm(data, ["PGTR"], want_index=True)
    head = ["Track_FormID", "Track_EDID", "Entry", "Level", "Slot", "Name", "Desc", "Category",
            "Img_KYWD", "Icon_KYWD", "Reward_FormID", "Reward_EDID"]
    lines = ["\t".join(head)]
    for rec in esm.records["PGTR"]:
        for entry, level, b in rows_for(esm, rec):
            lines.append("\t".join(er.clean_cell(str(x)) for x in [
                "%08X" % rec.fid, rec.edid, entry, "" if level is None else level, b["Slot"],
                b["Name"], b["Desc"], b["Category"], b["Img"], b["Icon"],
                "%08X" % b["Reward"] if b["Reward"] else "",
                esm.edid_of(b["Reward"]) if b["Reward"] else ""]))
    out_dir = os.path.join(er.REPO, "tsv", "pts") if a.pts else os.path.join(er.REPO, "tsv")
    path = os.path.join(out_dir, er.out_name("PGTR_Export", channel))
    er.write_tsv(path, lines)
    er.append_ledger(channel, path)
    print("[PGTR] %d rows from %d tracks -> %s" % (len(lines) - 1, len(esm.records["PGTR"]), path))


if __name__ == "__main__":
    main()
