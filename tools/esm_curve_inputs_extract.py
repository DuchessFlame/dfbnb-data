#!/usr/bin/env python3
"""
esm_curve_inputs_extract.py - what every curve table is keyed on, straight out of SeventySix.esm.

A curve (CURV) is a lookup chart: "when X is this much, give this much". The CURV
record only holds the points; WHAT X is lives on the record that uses the curve:

    ENCH / SPEL effect   CVT0 = curve, MAGA = actor value fed in as X
    PERK entry point     EPF2 = curve, EPF4 = actor value fed in as X  (xEdit: "Actor value 2")

Writes tsv/CURVIN_Export_<Month>_<Year>.tsv, one row per (record, effect/entry, curve):

    Source_Sig Source_FormID Source_EDID Index Effect_FormID Effect_EDID
    Curve_FormID Curve_EDID Field Input_FormID Input_EDID

Input_EDID is blank when the actor value is built into the engine and has no record
in the ESM (00000392 = the player's health as a fraction of max - used by
Unyielding, Bolstering, Nerd Rage, Last Bastion and the "inverse / proportional
health" legendaries, whose curves all run 0 - 1).

Why this exists: the xEdit ENCH export only gained its MAGA columns on 20 Oct 2026
("!!!Wordpress - ExportENCHToTSV.pas"). build_specials_stats_json.py reads both;
either source works, the same as STATREC.

Armour / backpack mod curves (OMOD properties) have no input field at all - the game
evaluates them at the item's level. build_specials_stats_json.py labels those itself.

Stdlib only; reuses the reader in esm_buff_rules_extract.py.

Usage:
  python tools/esm_curve_inputs_extract.py "D:\\steamlibrary\\steamapps\\common\\Fallout76\\Data\\SeventySix.esm"
  python tools/esm_curve_inputs_extract.py <esm> --outdir tsv --stamp October_2026
  python tools/esm_curve_inputs_extract.py <esm> --outdir tsv/pts --pts
"""
import argparse, datetime, os, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import esm_buff_rules_extract as E  # noqa: E402

WANT = ("ENCH", "SPEL", "PERK", "MGEF", "CURV", "AVIF")
HDR = ["Source_Sig", "Source_FormID", "Source_EDID", "Index", "Effect_FormID", "Effect_EDID",
       "Curve_FormID", "Curve_EDID", "Field", "Input_FormID", "Input_EDID"]


def u32(d):
    return struct.unpack("<I", d[:4])[0] if len(d) >= 4 else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("esm")
    ap.add_argument("--outdir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    ap.add_argument("--stamp", default=datetime.date.today().strftime("%B_%Y"))
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args()

    recs = E.read_esm(a.esm, want=WANT)
    names = {}
    for by in recs.values():
        for fid, subs in by.items():
            for t, d in subs:
                if t == "EDID":
                    names[fid] = d.rstrip(b"\0").decode("latin1")
                    break
    curves = recs.get("CURV") or {}

    def h(fid):
        return "%08X" % fid if fid else ""

    rows = []
    # ENCH / SPEL: each effect starts at EFID; CVT0 and MAGA follow it
    for sig in ("ENCH", "SPEL"):
        for fid, subs in sorted((recs.get(sig) or {}).items()):
            idx, eff, cv, maga = -1, 0, 0, None

            def flush():
                if idx >= 0 and cv and maga is not None:
                    rows.append([sig, h(fid), names.get(fid, ""), str(idx), h(eff), names.get(eff, ""),
                                 h(cv), names.get(cv, ""), "MAGA", h(maga), names.get(maga, "")])
            for t, d in subs:
                if t == "EFID":
                    flush()
                    idx, eff, cv, maga = idx + 1, u32(d), 0, None
                elif t == "CVT0" and idx >= 0:
                    cv = u32(d)
                elif t == "MAGA" and idx >= 0:
                    maga = u32(d)
            flush()
    # PERK: each entry starts at PRKE; EPF2 (curve) and EPF4 (actor value) follow
    for fid, subs in sorted((recs.get("PERK") or {}).items()):
        idx, cv = -1, 0
        for t, d in subs:
            if t == "PRKE":
                idx, cv = idx + 1, 0
            elif t == "EPF2" and len(d) == 4 and u32(d) in curves:
                cv = u32(d)
            elif t == "EPF4" and cv and len(d) == 4:
                av = u32(d)
                rows.append(["PERK", h(fid), names.get(fid, ""), str(idx), "", "",
                             h(cv), names.get(cv, ""), "EPF4", h(av), names.get(av, "")])
                cv = 0

    name = ("CURVIN_Export_PTS_%s.tsv" % datetime.datetime.now().strftime("%Y-%m-%d_%H%M")) if a.pts \
        else "CURVIN_Export_%s.tsv" % a.stamp
    os.makedirs(a.outdir, exist_ok=True)
    out = os.path.join(a.outdir, name)
    with open(out, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(HDR) + "\n")
        for r in rows:
            f.write("\t".join(str(c).replace("\t", " ") for c in r) + "\n")
    print(f"[curvin] wrote {out} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
