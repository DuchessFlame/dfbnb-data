#!/usr/bin/env python3
"""
esm_speed_extract.py - every effect in SeventySix.esm that changes how fast the
player moves, for the Movement Speed Calculator (Kat 14 Nov 2026).

The xEdit MGEF export has no Actor Value column, so this reads the ESM directly:
every magic effect whose actor value is SpeedMult ("Move Speed") is found, then
every ENCH / SPEL / ALCH effect that uses one is written out with its magnitude,
duration, the actor value its magnitude comes from (MAGA) and its conditions.

Writes tsv/SPEEDX_Export_<Month>_<Year>.tsv (or tsv/pts/SPEEDX_Export_PTS_<date>.tsv
with --pts). build_movement_speed_calculator_json.py reads the newest one.

Columns: Signature FormID EDID FULL Index MGEF_EDID Archetype NoStackKeyword Magnitude Duration MAGA_EDID Conditions
Conditions are the pas_cond strings (as the SPEL / CNDF exports write them), " ;; "-joined.
Archetype is the magic effect's archetype number: 0 = Value Modifier (adds up),
34 = Peak Value Modifier. NoStackKeyword is the effect's Assoc. Item 2 (DATA offset 92):
a Peak Value Modifier only refuses to stack with effects carrying that keyword. With no
keyword it stacks like a Value Modifier (Creation Kit wiki; Bethesda's patch 56 notes give
Propelling "+5% up to +25% on full stack", which is a Peak Value Modifier with no keyword).

Usage:
  python tools/esm_speed_extract.py                       (live game folder)
  python tools/esm_speed_extract.py --pts                 (Playtest game folder -> tsv/pts/)
  python tools/esm_speed_extract.py --data "<...\\Data>"
"""
import argparse, datetime, os, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import esm_reader as R  # noqa: E402
import esm_conditions as C  # noqa: E402

HDR = ["Signature", "FormID", "EDID", "FULL", "Index", "MGEF_EDID", "Archetype", "NoStackKeyword", "Magnitude", "Duration", "MAGA_EDID", "Conditions"]
AV_SPEED = 0x000002DA  # SpeedMult "Move Speed"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--data", default=None)
    a = ap.parse_args()
    data = a.data or (R.PTS_DATA if a.pts else R.LIVE_DATA)
    if not os.path.isdir(data):  # Claude's Linux shell on Sally sees the game under ~/mnt
        alt = os.path.expanduser("~/mnt/" + ("Fallout 76 Playtest" if a.pts else "Fallout76") + "/Data")
        if os.path.isdir(alt):
            data = alt
    esm = R.Esm(data, ["MGEF", "ENCH", "SPEL", "ALCH"], want_index=True)

    def name(fid):
        v = esm.index.get(fid)
        return v[1] if v else ""

    speed = {}
    for m in esm.records["MGEF"]:
        d = m.first("DATA") or b""
        if len(d) >= 76 and struct.unpack_from("<I", d, 72)[0] == AV_SPEED:
            kw = struct.unpack_from("<I", d, 92)[0] if len(d) >= 96 else 0
            speed[m.fid] = (struct.unpack_from("<I", d, 68)[0], kw)  # archetype, Assoc. Item 2

    rows = []
    for sig in ("ENCH", "SPEL", "ALCH"):
        for r in esm.records[sig]:
            subs, idx = r.subs, -1
            full = None
            for i, (k, v) in enumerate(subs):
                if k != "EFID":
                    continue
                idx += 1
                mg = struct.unpack("<I", v)[0]
                if mg not in speed:
                    continue
                if full is None:
                    full = esm.lstring(r.first("FULL"))
                efit = maga = None
                conds = []
                for kk, vv in subs[i + 1:]:
                    if kk == "EFID":
                        break
                    if kk == "EFIT" and efit is None:
                        efit = vv
                    elif kk == "MAGA" and maga is None:
                        maga = vv
                    elif kk == "CTDA":
                        try:
                            conds.append(C.pas_cond(C.Ctx(esm, r), vv))
                        except Exception as e:  # keep going, but say so
                            conds.append("?%s" % e)
                mag = struct.unpack_from("<f", efit, 4)[0] if efit and len(efit) >= 8 else 0.0
                dur = struct.unpack_from("<I", efit, 12)[0] if efit and len(efit) >= 16 else 0
                mfid = struct.unpack("<I", maga)[0] if maga else 0
                rows.append([sig, "%08X" % r.fid, r.edid, full or "", str(idx), name(mg), str(speed[mg][0]), name(speed[mg][1]) if speed[mg][1] not in (0, 0xFFFFFFFF) else "", "%.6f" % mag, str(dur),
                             name(mfid) if mfid else "", " ;; ".join(conds)])

    now = datetime.datetime.now()
    if a.pts:
        out = os.path.join(REPO, "tsv", "pts", "SPEEDX_Export_PTS_%s.tsv" % now.strftime("%Y-%m-%d_%H%M"))
    else:
        out = os.path.join(REPO, "tsv", "SPEEDX_Export_%s_%d.tsv" % (now.strftime("%B"), now.year))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write("\t".join(HDR) + "\n")
        for r in rows:
            f.write("\t".join(str(x).replace("\t", " ").replace("\n", " ") for x in r) + "\n")
    print("wrote %s: %d effects from %d move-speed magic effects" % (os.path.relpath(out, REPO), len(rows), len(speed)))


if __name__ == "__main__":
    main()
