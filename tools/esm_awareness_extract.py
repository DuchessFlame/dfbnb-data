#!/usr/bin/env python3
"""
esm_awareness_extract.py - what the V.A.T.S. Awareness Bar needs that the
regular xEdit exports don't carry, straight out of SeventySix.esm.

Writes tsv/AWARE_Export_<Month>_<Year>.tsv (or tsv/pts/AWARE_Export_PTS_<date>.tsv
with --pts). build_awareness_bar_json.py reads the newest one.

Why: the bar's resistance number is (Matt, Bethesda, 5 Nov 2026)
    the enemy's resistances summed from its NPC record AND its RACE record
    (curve tables read at the player's level, held between the NPC's min/max
    level) plus resistances from spells, then the player's
    "Mod Target Damage Resistance" perks, truncated once at the end.
The NPC PRPS export already has the NPC side. This adds:
    RACE  PRPS  every race's actor value properties (resistances + curves)
    RACE  SPLO  abilities the race gives
    NPC_  PRKR  perks on each NPC (ImmuneToPoison / ImmuneToRadiation ... = the
                crossed-out circle on the bar)
    NPC_  SPLO  abilities on each NPC
    RACE / NPC_  KWDA  ActorType* keywords (robot, bug, animal ...) - perks like
                Exterminator only lower the resistance of some enemy types
    NPC_  PFRN  the power armour an enemy spawns in (a furniture record). The bar
                counts the power armour frame (ArmorPoweredFrame: 60 DR / ER / Rad)
                plus whatever the pieces carry - Bloodhound (Gleaming Depths) shows
                +60 physical and energy over its own record (SpaceLime's video, 7 Nov).
    FURN  CNTO  what that power armour furniture holds (pieces / leveled lists)
    LVLI  LVLO  leveled item lists reached from it: entry + level
    ARMO  DAMA  resistances on those pieces and on the frame: damage type,
                amount, curve
    LVLN  LVLO  leveled character lists: each entry and the level it starts at.
                The enemies that spawn (LvlScorchedMelee ...) carry the resistances but
                no name; the name on the bar ("Blood Eagle Bruiser") comes from the
                list in their template (TPLT), entry by level.
Columns match STATREC_Export so the readers stay simple:
    Signature FormID EDID Field Index Ref_FormID Ref_EDID Value Curve_FormID Curve_EDID

Stdlib only; reuses the reader in esm_buff_rules_extract.py.

Usage:
  python tools/esm_awareness_extract.py "D:\\steamlibrary\\steamapps\\common\\Fallout76\\Data\\SeventySix.esm"
  python tools/esm_awareness_extract.py <esm> --pts      (Playtest ESM -> tsv/pts/)
"""
import argparse, datetime, os, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import esm_buff_rules_extract as E  # noqa: E402

WANT = ("RACE", "NPC_", "PERK", "SPEL", "AVIF", "CURV", "KYWD", "LVLN", "FURN", "LVLI", "ARMO", "DMGT")
HDR = ["Signature", "FormID", "EDID", "Field", "Index", "Ref_FormID", "Ref_EDID", "Value",
       "Curve_FormID", "Curve_EDID"]


def edid_of(subs):
    for t, d in subs:
        if t == "EDID":
            return d.rstrip(b"\0").decode("latin1")
    return ""


def fmt(v):
    return "%.6f" % v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("esm")
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--stamp", default=datetime.date.today().strftime("%B_%Y"))
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args()
    outdir = a.outdir or os.path.join(os.path.dirname(HERE), "tsv", "pts" if a.pts else "")

    recs = E.read_esm(a.esm, want=WANT)
    names = {}
    for sig, by in recs.items():
        for fid, subs in by.items():
            names[fid] = edid_of(subs)

    def ref(fid):
        return ("%08X" % fid, names.get(fid, "")) if fid else ("", "")

    rows = []
    for fid, subs in sorted((recs.get("LVLN") or {}).items()):
        # FO76 LVLN entries: LVLO (4-byte ref) then LVOV / LVIV / LVLV (floats).
        # Value column = LVLV, Curve columns unused.
        base = ["LVLN", "%08X" % fid, edid_of(subs)]
        n, cur = 0, None
        for t, d in subs + [("END", b"")]:
            if t in ("LVLO", "END") and cur is not None:
                rows.append(base + ["LVLO", str(n), *ref(cur[0]), fmt(cur[1]), "", ""])
                n += 1
                cur = None
            if t == "LVLO" and len(d) >= 4:
                cur = [struct.unpack("<I", d[:4])[0], 0.0]
            elif t == "LVLV" and cur is not None and len(d) >= 4:
                cur[1] = struct.unpack("<f", d[:4])[0]
    for sig in ("RACE", "NPC_"):
        for fid, subs in sorted((recs.get(sig) or {}).items()):
            ed = edid_of(subs)
            base = [sig, "%08X" % fid, ed]
            nsp = npk = 0
            for t, d in subs:
                if t == "PRPS" and sig == "RACE":
                    for i in range(len(d) // 12):
                        av, val, cv = struct.unpack("<IfI", d[i * 12:i * 12 + 12])
                        rows.append(base + ["PRPS", str(i), *ref(av), fmt(val), *ref(cv)])
                elif t == "SPLO" and len(d) >= 4:
                    rows.append(base + ["SPLO", str(nsp), *ref(struct.unpack("<I", d[:4])[0]), "", "", ""])
                    nsp += 1
                elif t == "KWDA":
                    for i in range(len(d) // 4):
                        kw = struct.unpack("<I", d[i * 4:i * 4 + 4])[0]
                        if names.get(kw, "").startswith("ActorType"):
                            rows.append(base + ["KWDA", str(i), *ref(kw), "", "", ""])
                elif t == "PRKR" and len(d) >= 4:
                    rank = d[4] if len(d) > 4 else 0
                    rows.append(base + ["PRKR", str(npk), *ref(struct.unpack("<I", d[:4])[0]), str(rank), "", ""])
                    npk += 1

    # ---- power armour: NPC PFRN -> FURN CNTO -> (LVLI LVLO)* -> ARMO DAMA, + the frame
    import json
    rules = json.load(open(os.path.join(os.path.dirname(HERE), "data", "awareness_bar", "rules.json"), encoding="utf-8"))
    frame_ed = rules.get("pa_frame", "ArmorPoweredFrame")
    furn, lvli, armo = recs.get("FURN") or {}, recs.get("LVLI") or {}, recs.get("ARMO") or {}
    want_furn, want_lvli, want_armo = set(), set(), {f for f, s in armo.items() if edid_of(s) == frame_ed}
    for fid, subs in sorted((recs.get("NPC_") or {}).items()):
        for t, d in subs:
            if t == "PFRN" and len(d) >= 4:
                pf = struct.unpack("<I", d[:4])[0]
                rows.append(["NPC_", "%08X" % fid, edid_of(subs), "PFRN", "0", *ref(pf), "", "", ""])
                want_furn.add(pf)

    def walk(fid):
        if fid in armo:
            want_armo.add(fid)
        elif fid in lvli and fid not in want_lvli:
            want_lvli.add(fid)
            for t, d in lvli[fid]:
                if t == "LVLO" and len(d) >= 4:
                    walk(struct.unpack("<I", d[:4])[0])
    for pf in sorted(want_furn):
        subs = furn.get(pf) or []
        n = 0
        for t, d in subs:
            if t == "CNTO" and len(d) >= 8:
                it, cnt = struct.unpack("<Ii", d[:8])
                rows.append(["FURN", "%08X" % pf, edid_of(subs), "CNTO", str(n), *ref(it), str(cnt), "", ""])
                n += 1
                walk(it)
    for fid in sorted(want_lvli):
        subs = lvli[fid]
        base = ["LVLI", "%08X" % fid, edid_of(subs)]
        for t, d in subs:
            if t == "LVLF" and d:   # flags: 4 = use all entries (a full set), else one is picked
                rows.append(base + ["LVLF", "0", "", "", str(d[0]), "", ""])
        n, cur = 0, None
        for t, d in subs + [("END", b"")]:
            if t in ("LVLO", "END") and cur is not None:
                rows.append(base + ["LVLO", str(n), *ref(cur[0]), fmt(cur[1]), "", ""])
                n += 1
                cur = None
            if t == "LVLO" and len(d) >= 4:
                cur = [struct.unpack("<I", d[:4])[0], 0.0]
            elif t == "LVLV" and cur is not None and len(d) >= 4:
                cur[1] = struct.unpack("<f", d[:4])[0]
    for fid in sorted(want_armo):
        subs = armo[fid]
        n = 0
        for t, d in subs:
            if t == "DAMA":
                for dt, amt, cv in struct.iter_unpack("<III", d[:len(d) // 12 * 12]):
                    rows.append(["ARMO", "%08X" % fid, edid_of(subs), "DAMA", str(n), *ref(dt), str(amt), *ref(cv)])
                    n += 1

    name = ("AWARE_Export_PTS_%s.tsv" % datetime.datetime.now().strftime("%Y-%m-%d_%H%M")) if a.pts \
        else "AWARE_Export_%s.tsv" % a.stamp
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, name)
    with open(out, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(HDR) + "\n")
        for r in rows:
            f.write("\t".join(str(c).replace("\t", " ") for c in r) + "\n")
    print(f"[aware] wrote {out} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
