#!/usr/bin/env python3
"""
esm_stat_records_extract.py - the base-stat records, straight out of SeventySix.esm.

Writes tsv/STATREC_Export_<Month>_<Year>.tsv, the same file the xEdit script
"!!!Wordpress - ExportStatRecordsToTSV.pas" writes. Either source works;
build_specials_stats_json.py reads whichever is newest.

What is in it (one row per value):
    RACE  HumanRace / GhoulRace / GHL_PlayerGhoul* / *PowerArmor* races
          PRPS  actor value properties (HumanRace: Health +40, ActionPointsRate 6 ...)
          SPLO  spells the race gives
    NPC_  Player (00000007): PRPS (Health 205, SPECIALs 1 ...), SPLO, PRKR
    CLAS  every class: PRPS
    AVIF  SPECIAL + stat actor values: NAM0 default, NAM5 minimum, NAM6 maximum

Columns:
    Signature FormID EDID Field Index Ref_FormID Ref_EDID Value Curve_FormID Curve_EDID

Stdlib only; reuses the reader in esm_buff_rules_extract.py.

Usage:
  python tools/esm_stat_records_extract.py "D:\\steamlibrary\\steamapps\\common\\Fallout76\\Data\\SeventySix.esm"
  python tools/esm_stat_records_extract.py <esm> --outdir tsv --stamp October_2026
  python tools/esm_stat_records_extract.py <esm> --outdir tsv/pts --pts
"""
import argparse, datetime, os, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import esm_buff_rules_extract as E  # noqa: E402

WANT = ("RACE", "NPC_", "CLAS", "AVIF", "SPEL", "PERK", "CURV")
AVIF_KEEP = {
    "Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck", "Experience",
    "Health", "HealthBonus", "HealRate", "STAT_HealMult", "STAT_HealMultStimpak", "ActionPoints",
    "ActionPointsMax", "ActionPointsRate", "ActionPointsRateMult", "CarryWeight", "SpeedMult",
    "SprintSpeedMult", "DamageResist", "EnergyResist", "FireResist", "FrostResist", "PoisonResist",
    "ElectricResist", "RadResistExposure", "RadResistIngestion", "STAT_ResistRadIngestion",
    "SURV_DiseaseResistance", "Rads", "RadsRate", "STAT_XPMult", "STAT_CritDmgAll", "STAT_VATSAccuracy",
    "STAT_GunAccuracy", "STAT_DmgMelee", "STAT_DmgMeleeUnarmed", "STAT_Sneak", "STAT_SneakAttackBonus",
    "STAT_LimbDamageResistance", "AimStability", "EpicInitialBaseHealthAV",
}
HDR = ["Signature", "FormID", "EDID", "Field", "Index", "Ref_FormID", "Ref_EDID", "Value",
       "Curve_FormID", "Curve_EDID"]


def edid_of(subs):
    for t, d in subs:
        if t == "EDID":
            return d.rstrip(b"\0").decode("latin1")
    return ""


def want_record(sig, fid, edid):
    e = edid.lower()
    if sig == "RACE":
        return e in ("humanrace", "ghoulrace") or e.startswith("ghl_player") or "powerarmor" in e
    if sig == "NPC_":
        return (fid & 0xFFFFFF) == 7
    if sig == "CLAS":
        return True
    if sig == "AVIF":
        return edid in AVIF_KEEP
    return False


def fmt(v):
    return "%.6f" % v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("esm")
    ap.add_argument("--outdir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    ap.add_argument("--stamp", default=datetime.date.today().strftime("%B_%Y"))
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args()

    recs = E.read_esm(a.esm, want=WANT)
    names = {}
    for sig, by in recs.items():
        for fid, subs in by.items():
            names[fid] = edid_of(subs)

    def ref(fid):
        return ("%08X" % fid, names.get(fid, "")) if fid else ("", "")

    rows = []
    for sig in ("RACE", "NPC_", "CLAS", "AVIF"):
        for fid, subs in sorted((recs.get(sig) or {}).items()):
            ed = edid_of(subs)
            if not want_record(sig, fid, ed):
                continue
            base = [sig, "%08X" % fid, ed]
            if sig == "AVIF":
                for t, d in subs:
                    if t in ("NAM0", "NAM5", "NAM6") and len(d) >= 4:
                        rows.append(base + [t, "0", "", "", fmt(struct.unpack("<f", d[:4])[0]), "", ""])
                continue
            nsp = npk = 0
            for t, d in subs:
                if t == "PRPS":
                    for i in range(len(d) // 12):
                        av, val, cv = struct.unpack("<IfI", d[i * 12:i * 12 + 12])
                        rows.append(base + ["PRPS", str(i), *ref(av), fmt(val), *ref(cv)])
                elif t == "SPLO" and len(d) >= 4:
                    rows.append(base + ["SPLO", str(nsp), *ref(struct.unpack("<I", d[:4])[0]), "", "", ""])
                    nsp += 1
                elif t == "PRKR" and len(d) >= 4:
                    rank = d[4] if len(d) > 4 else 0
                    rows.append(base + ["PRKR", str(npk), *ref(struct.unpack("<I", d[:4])[0]), str(rank), "", ""])
                    npk += 1

    name = ("STATREC_Export_PTS_%s.tsv" % datetime.datetime.now().strftime("%Y-%m-%d_%H%M")) if a.pts \
        else "STATREC_Export_%s.tsv" % a.stamp
    os.makedirs(a.outdir, exist_ok=True)
    out = os.path.join(a.outdir, name)
    with open(out, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(HDR) + "\n")
        for r in rows:
            f.write("\t".join(str(c).replace("\t", " ") for c in r) + "\n")
    print(f"[statrec] wrote {out} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
