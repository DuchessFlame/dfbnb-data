#!/usr/bin/env python3
r"""
esm_buff_guides_extract.py - the game-file pieces the Buffs guide pages need that
the xEdit exports don't carry. Reads SeventySix.esm + the English strings
straight from the game folder (esm_reader.py), the same way export_from_esm.py does.

Writes (tsv\ for live, tsv\pts\ with --pts):

  BUFFCOND_Export_<Month>_<Year>.tsv
      Every condition on a consumable's effect (ALCH effect CTDA) and on a magic
      effect itself (MGEF CTDA), one row per record/effect:
          Scope (ALCH|MGEF)  FormID  EDID  EffectIndex  MGEF_FormID  MGEF_EDID  CondCount  Cond1..
      Conditions are written exactly like the CNDF / CHAL exports
      (Type|Value|Function|params...|RunOn|RefCell), so the same reader parses them.
      e.g. Live & Love 1's team buff: 10000000|1.000000|IsMemberOfAPlayerTeam|...
           Guns and Bullets 6 at night: GetGlobalValue(GameHour) >= 21 OR < 6

  MESGTXT_Export_<Month>_<Year>.tsv
      Every message's title and text (MESG FULL / DESC), e.g. the pickup message
      a bobblehead shows: Bobblehead_Strength_Msg "It's essential to give your
      arguments impact." When used, gain +2 Strength for 1 hour.
          MESG_FormID  MESG_EDID  FULL  DESC

  CONTITEMS_Export_<Month>_<Year>.tsv
      What every container holds (CONT CNTO), e.g. a caps stash tin's leveled list:
          CONT_FormID  CONT_EDID  Item_FormID  Item_EDID  Item_Sig  Count

Why: the buff guides show WHEN a buff works (team / night / sneaking / swimming /
a drink active / a weapon equipped), the game's own description of each
bobblehead, and what the Caps bobblehead does to caps stashes. None of that is in
the ALCH / MGEF / LVLI exports. GitHub Actions has no game files, so run this on
the PC after a patch and commit the three TSVs (like CURVIN / STATREC / MGEFKW).

Usage (on the PC with the game installed):
    python tools\esm_buff_guides_extract.py            LIVE -> tsv\
    python tools\esm_buff_guides_extract.py --pts      PTS  -> tsv\pts\
Options: --data "<Data folder>"  --out "<folder>"  --no-ledger
"""
from __future__ import annotations

import argparse
import os
import struct
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import esm_reader as R          # noqa: E402
import esm_conditions as C      # noqa: E402
from export_from_esm import _cond_cols, _effects, _Sub, cc   # noqa: E402


def _u32(v):
    return struct.unpack("<I", v[:4])[0] if v and len(v) >= 4 else 0


def _pad(rows, head, prefix="Cond"):
    mx = max((len(c) for _b, c in rows), default=0)
    out = ["\t".join(head + ["%s%d" % (prefix, i) for i in range(1, mx + 1)])]
    for base, conds in rows:
        out.append("\t".join(base + conds + [""] * (mx - len(conds))))
    return out


def exp_buffcond(esm):
    rows = []
    for r in R.xedit_order(esm, "MGEF"):
        conds = _cond_cols(esm, r)
        if conds:
            rows.append((["MGEF", "%08X" % r.fid, cc(r.edid), "", "%08X" % r.fid, cc(r.edid), str(len(conds))], conds))
    for r in R.xedit_order(esm, "ALCH"):
        for i, subs in enumerate(_effects(r)):
            sub = _Sub(r, subs)
            ctx = C.Ctx(esm, r)
            conds = [cc(C.pas_cond(ctx, *c, clean=cc)) for c in C.conditions(sub, stop_at=())]
            if not conds:
                continue
            mfid = _u32(sub.first("EFID"))
            rows.append((["ALCH", "%08X" % r.fid, cc(r.edid), str(i), "%08X" % mfid,
                          cc(esm.edid_of(mfid)), str(len(conds))], conds))
    return _pad(rows, ["Scope", "FormID", "EDID", "EffectIndex", "MGEF_FormID", "MGEF_EDID", "CondCount"])


def exp_mesgtxt(esm):
    out = ["MESG_FormID\tMESG_EDID\tFULL\tDESC"]
    for r in R.xedit_order(esm, "MESG"):
        out.append("\t".join(["%08X" % r.fid, cc(r.edid), cc(esm.lstring(r.first("FULL"))),
                              cc(esm.lstring(r.first("DESC")))]))
    return out


def exp_contitems(esm):
    out = ["CONT_FormID\tCONT_EDID\tItem_FormID\tItem_EDID\tItem_Sig\tCount"]
    for r in R.xedit_order(esm, "CONT"):
        for k, v in r.subs:
            if k != "CNTO" or len(v) < 8:
                continue
            fid, cnt = struct.unpack("<Ii", v[:8])
            out.append("\t".join(["%08X" % r.fid, cc(r.edid), "%08X" % fid, cc(esm.edid_of(fid)),
                                  esm.sig_of(fid), str(cnt)]))
    return out


EXPORTS = [("BUFFCOND_Export", exp_buffcond), ("MESGTXT_Export", exp_mesgtxt), ("CONTITEMS_Export", exp_contitems)]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pts", action="store_true", help="PTS: read the Playtest install, write tsv\\pts\\")
    ap.add_argument("--data", help="game Data folder (default: the live or PTS install)")
    ap.add_argument("--out", help="output folder (default: tsv\\ or tsv\\pts\\)")
    ap.add_argument("--no-ledger", action="store_true", help="don't add lines to tsv\\_exports.tsv")
    a = ap.parse_args(argv)
    channel = "PTS" if a.pts else "LIVE"
    data = a.data or (R.PTS_DATA if a.pts else R.LIVE_DATA)
    out_dir = a.out or os.path.join(R.REPO, "tsv", "pts" if a.pts else "")
    if not os.path.isfile(os.path.join(data, "SeventySix.esm")):
        sys.exit("SeventySix.esm not found in %s - use --data" % data)
    t0 = time.time()
    print("[%s] reading %s ..." % (channel, data))
    esm = R.Esm(data, ["MGEF", "ALCH", "MESG", "CONT"])
    print("  loaded in %.0fs" % (time.time() - t0))
    for prefix, fn in EXPORTS:
        lines = fn(esm)
        path = os.path.join(out_dir, R.out_name(prefix, channel))
        R.write_tsv(path, lines)
        if not a.no_ledger:
            try:
                R.append_ledger(channel, path)
            except Exception as e:          # the ledger is a convenience, never fatal
                print("  (ledger not updated: %s)" % e)
        print("  wrote %s (%d rows)" % (path, len(lines) - 1))


if __name__ == "__main__":
    main()
