#!/usr/bin/env python3
"""
esm_ap_extract.py - what the AP Refresh Calculator needs that the regular
xEdit exports don't carry, straight out of SeventySix.esm (Kat 6 Nov 2026).

Writes tsv/APX_Export_<Month>_<Year>.tsv (or tsv/pts/APX_Export_PTS_<date>.tsv
with --pts). build_ap_calculator_json.py reads the newest one.

Rows (columns match AWARE_ / STATREC_ exports):
    ENCH / SPEL  EFFECT  every effect whose magic effect raises ActionPointsRate
                 (flat AP refresh) or ActionPointsRateMult (% AP refresh):
                 Value = magnitude, Ref = the magic effect, Curve = MAGA actor value
                 when the magnitude comes from an actor value (Kinetic Servos ->
                 PA_KineticServos_AV, set +1 per leg by the OMOD).
                 Field = FLAT or MULT.
    CURV  CRVE   the curve-table JSON path of every curve those need
                 (CT_PA_Mod_CoreAssembly ...). The September/October xEdit CURV
                 exports left 617 of these paths blank.

Usage:
  python tools/esm_ap_extract.py                       (live game folder)
  python tools/esm_ap_extract.py --pts                 (Playtest game folder -> tsv/pts/)
  python tools/esm_ap_extract.py --data "<...\\Data>"
"""
import argparse, datetime, os, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import esm_reader as R  # noqa: E402

HDR = ["Signature", "FormID", "EDID", "Field", "Index", "Ref_FormID", "Ref_EDID", "Value", "Curve_FormID", "Curve_EDID"]
CURVES = ("CT_PA_Mod_CoreAssembly",)
AV_FLAT, AV_MULT = 0x000002D8, 0x00000359  # ActionPointsRate, ActionPointsRateMult


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
    esm = R.Esm(data, ["ENCH", "SPEL", "MGEF", "CURV"], want_index=True)

    def name(fid):
        v = esm.index.get(fid)
        return v[1] if v else ""

    mgef_av = {}
    for m in esm.records["MGEF"]:
        d = m.first("DATA") or b""
        if len(d) >= 76:
            mgef_av[m.fid] = struct.unpack_from("<I", d, 72)[0]

    rows = []
    for sig in ("ENCH", "SPEL"):
        for r in esm.records[sig]:
            subs, idx = r.subs, 0
            for i, (k, v) in enumerate(subs):
                if k != "EFID":
                    continue
                mg = struct.unpack("<I", v)[0]
                av = mgef_av.get(mg)
                if av not in (AV_FLAT, AV_MULT):
                    idx += 1
                    continue
                efit = maga = None
                for kk, vv in subs[i + 1:]:
                    if kk == "EFID":
                        break
                    if kk == "EFIT" and efit is None:
                        efit = vv
                    if kk == "MAGA" and maga is None:
                        maga = vv
                mag = struct.unpack_from("<f", efit, 4)[0] if efit and len(efit) >= 8 else 0.0
                mfid = struct.unpack("<I", maga)[0] if maga else 0
                rows.append([sig, "%08X" % r.fid, r.edid, "FLAT" if av == AV_FLAT else "MULT", str(idx),
                             "%08X" % mg, name(mg), "%.6f" % mag, ("%08X" % mfid) if mfid else "", name(mfid) if mfid else ""])
                idx += 1
    for c in esm.records["CURV"]:
        if c.edid in CURVES:
            p = (c.first("CRVE") or b"").rstrip(b"\0").decode("utf-8", "replace")
            rows.append(["CURV", "%08X" % c.fid, c.edid, "CRVE", "0", "", "", p, "", ""])

    now = datetime.datetime.now()
    if a.pts:
        out = os.path.join(REPO, "tsv", "pts", "APX_Export_PTS_%s.tsv" % now.strftime("%Y-%m-%d_%H%M"))
    else:
        out = os.path.join(REPO, "tsv", "APX_Export_%s_%d.tsv" % (now.strftime("%B"), now.year))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write("\t".join(HDR) + "\n")
        for r in rows:
            f.write("\t".join(x.replace("\t", " ") for x in r) + "\n")
    print("wrote %s: %d rows (%d flat, %d mult, %d curves)" % (os.path.relpath(out, REPO), len(rows),
          sum(r[3] == "FLAT" for r in rows), sum(r[3] == "MULT" for r in rows), sum(r[0] == "CURV" for r in rows)))


if __name__ == "__main__":
    main()
