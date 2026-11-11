#!/usr/bin/env python3
r"""
build_ammo_converter_json.py
/df/calculators/ammo-converter-calculator/  ->  dist/calculators/ammo_converter.json
                                     (--pts  ->  dist/pts/calculators/ammo_converter.json)
Kat 11 Nov 2026.

Every number comes from the game files:
  tsv/AMMOCONV_Export_*.tsv   which ammo the converter takes, its menu group/order and the
                              ammo's in-game name (tools/export_ammo_converter_from_esm.py)
  tsv/GLOB_Export_*.tsv       AmmoConverter_<Ammo>_Ammo_Count / _Point_SellGain / _Point_BuyCost
                              (re-read here so a rate change in a patch always lands)
  papyrus/papyrus_*.txt       Economy:AmmoConverterTerminalScript - AmmoPointPlayerMax (100000)
                              and the Med / High transaction counts (10x / 100x buttons)

How the converter works (terminal text + script):
  Sell 1x  = hand over <Count> rounds, get <SellGain> points
  Buy  1x  = pay <BuyCost> points, get <Count> rounds
  10x / 100x buttons = 10 / 100 of those at once. Points are stored in the machine,
  up to AmmoPointPlayerMax; a sale that would go over the max fails.
The trade runs on Bethesda's servers - Fallout76.exe has no converter settings or code.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import tsv_source  # noqa: E402

WARN = []


def warn(m):
    WARN.append(m)
    print("[ammo_converter] WARN", m, file=sys.stderr)


def rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def num(v):
    try:
        return float(str(v).strip().strip('"'))
    except (TypeError, ValueError):
        return None


def script_ints(channel):
    """AmmoPointPlayerMax + transaction counts from the papyrus dump."""
    p = os.path.join(ROOT, "papyrus", "papyrus_pts.txt" if channel == "pts" else "papyrus_live.txt")
    out = {"max": None, "med": None, "high": None}
    try:
        txt = open(p, encoding="utf-8", errors="replace").read()
    except OSError:
        warn(f"{p} missing")
        return out
    i = txt.find("SCRIPT Economy:AmmoConverterTerminalScript")
    if i < 0:
        warn("AmmoConverterTerminalScript not in the papyrus dump")
        return out
    block = txt[i:txt.find("\n=====", i)]
    for k, name in (("max", "AmmoPointPlayerMax"), ("med", "MedTransactionCount"), ("high", "HighTransactionCount")):
        m = re.search(r"VAR \w+ " + name + r" = ([0-9.]+)", block)
        if m:
            out[k] = int(float(m.group(1)))
        else:
            warn(f"{name} not found in the script")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args(argv)
    ch = "pts" if a.pts else "live"
    conv_path = tsv_source.newest("AMMOCONV_Export_*.tsv", channel=ch)
    glob_path = tsv_source.newest("GLOB_Export_*.tsv", channel=ch)
    gl = {r["EDID"].strip(): num(r.get("FLTV")) for r in rows(glob_path)}
    sc = script_ints(ch)

    groups, ammo = [], []
    for r in rows(conv_path):
        g = r["Group"].strip()
        if g not in groups:
            groups.append(g)
        vals = {}
        for col, gcol in (("Count", "Count_GLOB"), ("SellGain", "SellGain_GLOB"), ("BuyCost", "BuyCost_GLOB")):
            v = gl.get(r[gcol].strip())
            if v is None:
                v = num(r[col])
                warn(f"{r[gcol]} not in {os.path.basename(glob_path)} - using the AMMOCONV value {v}")
            vals[col] = int(round(v or 0))
        if not all(vals.values()):
            warn(f"{r['Key']} has a zero rate - left out")
            continue
        name = (r.get("AMMO_FULL") or "").strip() or re.sub(r"\s+Ammo$", "", r["MenuName"].strip())
        ammo.append({
            "id": r["Key"].strip(), "name": name, "group": groups.index(g),
            "count": vals["Count"], "sell": vals["SellGain"], "buy": vals["BuyCost"],
        })
    if not ammo:
        raise SystemExit("no ammo rows - not writing an empty calculator over a good one")

    out = {
        "_meta": {
            "page": "/df/calculators/ammo-converter-calculator/",
            "channel": ch,
            "built": datetime.date.today().isoformat(),
            "sources": [os.path.basename(conv_path), os.path.basename(glob_path),
                        "papyrus Economy:AmmoConverterTerminalScript"],
            "warnings": WARN,
        },
        "maxPoints": sc["max"] or 100000,
        "bulk": [1, sc["med"] or 10, sc["high"] or 100],
        "groups": groups,
        "ammo": ammo,
    }
    od = os.path.join(ROOT, "dist", "pts" if a.pts else "", "calculators")
    os.makedirs(od, exist_ok=True)
    p = os.path.join(od, "ammo_converter.json")
    with open(p, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
        f.write("\n")
    print(f"[ammo_converter] {len(ammo)} ammo types, max {out['maxPoints']} points -> {p}")


if __name__ == "__main__":
    main()
