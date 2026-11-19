#!/usr/bin/env python3
r"""
esm_hide_normalize_perks.py - which Hideout scaling perk each HIDE_ actor carries
(19 Nov 2026).

Hideout enemy levels are set per difficulty by the HIDE_crNormalizePerk_* perks
(Mod NPC Normalized Min/Max Level from a HIDE_Renorm* curve, read at the run's
HIDE_SelectedDifficulty actor value 0-3, plus a level multiplier). Each actor's
perk sits on its own NPC_ record (PRKR), which no xEdit export carries, so this
reads it once from SeventySix.esm with esm_refs (the same scan xEdit's
Referenced By uses).

Writes data/hideouts/normalize_perks_<channel>.tsv:
    NPC_FormID  NPC_EDID  PERK_FormID  PERK_EDID
read by src/build_boss_stats_json.py (Hideouts page). The perks' curves and
multipliers come from the PERKEP export, not from here.

Usage (on the PC, from dfbnb-data):
  python tools\esm_hide_normalize_perks.py --pts     Playtest install -> *_pts.tsv
  python tools\esm_hide_normalize_perks.py           live install     -> *_live.tsv
  --data <folder>  game Data folder (overrides the default install)
Re-run after a patch / PTS update that touches Hideouts.
"""
import argparse
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import esm_reader as R  # noqa: E402
import esm_refs  # noqa: E402

DEFAULT_DATA = {
    "pts": r"D:\steamlibrary\steamapps\common\Fallout 76 Playtest\Data",
    "live": r"D:\steamlibrary\steamapps\common\Fallout76\Data",
}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pts", action="store_true", help="read the Playtest install")
    ap.add_argument("--data", default=None, help="game Data folder")
    a = ap.parse_args(argv)
    channel = "pts" if a.pts else "live"
    data_dir = a.data or DEFAULT_DATA[channel]
    esm = R.Esm(data_dir, ["PERK"])
    perks = {r.fid: r for r in esm.records["PERK"]}
    targets = {fid for fid in perks if (esm.index.get(fid) or ("", ""))[1].startswith("HIDE_crNormalizePerk_")}
    if not targets:
        print(f"[hide-normalize] no HIDE_crNormalizePerk_ perks on {channel} - nothing written.")
        return 0
    refs, info = esm_refs.referenced_by(esm, targets, referrers={"NPC_"})
    rows = []
    for pfid, npcs in refs.items():
        pedid = esm.index[pfid][1]
        for nfid in npcs:
            sig, nedid = info.get(nfid) or ("", "")
            if sig == "NPC_" and nedid:
                rows.append((f"{nfid:08X}", nedid, f"{pfid:08X}", pedid))
    rows.sort(key=lambda r: (r[3], r[1]))
    out = os.path.join(REPO, "data", "hideouts", f"normalize_perks_{channel}.tsv")
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(["NPC_FormID", "NPC_EDID", "PERK_FormID", "PERK_EDID"])
        w.writerows(rows)
    print(f"[hide-normalize] {len(rows)} actors -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
