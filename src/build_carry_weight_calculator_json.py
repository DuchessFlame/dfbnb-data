"""Carry Weight Calculator data (Kat 6 Nov 2026).

/bnb/calculators/carry-weight-calculator/  ->  dist/calculators/carry_weight_calculator.json
(df-bnb-carry-weight-calculator.js reads it; loaded by the router in df-bnb-bnb-calculators.js).

Every number comes from the game files, nothing typed in:
  base + per STR     dist/specials_stats.json stats.carry.formulas (GMST fAVDCarryWeightBase /
                     fAVDCarryWeightMult)
  Strong Back        CURV StrongBackBonus (input = Strength)        via specials_stats curves
  food / drink / mag / mutation / legendary / armour + PA mods
                     dist/specials_stats.json stats.carry.boost (food-table, items, legendary, mods)
  backpacks          OMOD mod_BackPack_Small_CarryCapacity / mod_BackPack_Base_CarryCapacity
                     (Actor Value CarryWeight on a curve), mod_BackPack_Effect_* (CarryWeight
                     curve per mod), CURV points for each of those curves (input = item level)
  Excavator          ENCH EnchPowerArmor_Generic_Excavator_Torso (AbFortifyCarryWeight magnitude)

Usage:  python src/build_carry_weight_calculator_json.py [--pts]
"""
from __future__ import annotations
import csv, datetime, json, os, re, sys

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import tsv_source  # noqa: E402

CHANNEL = tsv_source.channel_of()  # --pts or DFBNB_CHANNEL=pts
DIST = os.path.join(ROOT, "dist", "pts") if CHANNEL == "pts" else os.path.join(ROOT, "dist")
OUT = os.path.join(DIST, "calculators", "carry_weight_calculator.json")


def read_tsv(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def curve_points(rows, edid):
    pts = []
    for r in rows:
        vals = list(r.values())
        if len(vals) >= 4 and vals[1] == edid:
            try:
                pts.append([float(vals[2]), float(vals[3])])
            except ValueError:
                pass
    pts.sort()
    return [[int(x) if x.is_integer() else x, int(y) if y.is_integer() else y] for x, y in pts]


def main():
    warnings = []
    ss_path = os.path.join(DIST, "specials_stats.json")
    if not os.path.exists(ss_path):
        ss_path = os.path.join(ROOT, "dist", "specials_stats.json")
    ss = json.load(open(ss_path, encoding="utf-8"))
    carry = ss["stats"]["carry"]
    f0 = carry["formulas"][0]
    base = f0["inputs"]["base"]
    mult = f0["inputs"]["mult"]

    boosts = {b["id"]: b for b in carry["boost"]}
    curves = {c["edid"]: c for c in carry.get("curves", [])}

    # ---- CURV points + OMOD properties for backpacks -------------------------
    curv_path = tsv_source.newest("CURV_Export_*_POINTS.tsv", channel=CHANNEL)
    curv_rows = read_tsv(curv_path)
    omod_path = tsv_source.newest("OMOD_Export_*_Properties.tsv", channel=CHANNEL)
    omod_rows = read_tsv(omod_path)

    def carry_curve_for(omod_edid):
        for r in omod_rows:
            if r.get("OMOD_EDID") == omod_edid and "CarryWeight" in (r.get("Value1") or ""):
                m = re.match(r"([A-Za-z0-9_]+)\s*\[CURV:([0-9A-F]{8})\]", r.get("CurveTable") or "")
                if m:
                    return m.group(1), m.group(2), float(r.get("Value2") or 1)
        return None, None, None

    backpacks = []
    for label, edid in (("Small backpack", "mod_BackPack_Small_CarryCapacity"),
                        ("Standard backpack", "mod_BackPack_Base_CarryCapacity")):
        cv, fid, scale = carry_curve_for(edid)
        if not cv:
            warnings.append(f"no carry curve on {edid}")
            continue
        backpacks.append({"id": edid, "label": label, "curve": cv, "curveFormId": fid,
                          "scale": scale, "points": curve_points(curv_rows, cv),
                          "src": {"type": "OMOD", "edid": edid}})

    backpack_mods = [{"id": "", "label": "No mod / other mod", "curve": None, "points": []}]
    mod_labels = {
        "mod_BackPack_Effect_HighCapacity": "High Capacity",
        "mod_BackPack_Effect_ArmorPlated": "Armor Plated",
        "mod_BackPack_Effect_LeadLined": "Lead Lined",
        "mod_BackPack_Effect_Insulated": "Insulated",
        "mod_BackPack_Effect_Refrigerated": "Refrigerated",
    }
    for edid, label in mod_labels.items():
        cv, fid, scale = carry_curve_for(edid)
        if not cv:
            warnings.append(f"no carry curve on {edid}")
            continue
        backpack_mods.append({"id": edid, "label": label, "curve": cv, "curveFormId": fid,
                              "scale": scale, "points": curve_points(curv_rows, cv),
                              "src": {"type": "OMOD", "edid": edid}})

    # ---- Excavator torso (ENCH) ---------------------------------------------
    excavator = None
    ench_path = tsv_source.newest("ENCH_Export_*.tsv", channel=CHANNEL, required=False)
    if ench_path:
        with open(ench_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                c = line.rstrip("\n").split("\t")
                if len(c) > 13 and c[1] == "EnchPowerArmor_Generic_Excavator_Torso":
                    for i in range(10, len(c) - 2):
                        if c[i] == "AbFortifyCarryWeight":
                            excavator = {"value": float(c[i + 1]), "src": {"type": "ENCH", "edid": c[1], "formId": c[0]}}
                            break
    if not excavator:
        warnings.append("Excavator torso carry weight not found in ENCH export")

    # ---- Pieces from specials_stats -----------------------------------------
    def mod_rows(name, piece_hint):
        out = []
        for r in boosts.get("mods", {}).get("rows", []):
            if r["name"] != name:
                continue
            fits = " ".join(r.get("fits", []))
            if piece_hint == "torso" and "Torso" in fits and "Limb" not in fits:
                out.append(r["magnitude"])
            if piece_hint == "limb" and "Limb" in fits:
                out.append(r["magnitude"])
            if piece_hint == "any":
                out.append(r["magnitude"])
        return max(out) if out else None

    linings = []
    for name in ("Pocketed", "Deep Pocketed"):
        t, l = mod_rows(name, "torso"), mod_rows(name, "limb")
        if t is None or l is None:
            warnings.append(f"{name}: torso/limb values missing")
            continue
        linings.append({"id": name.lower().replace(" ", "-"), "label": name, "torso": t, "limb": l})

    shocks = mod_rows("Calibrated Shocks", "any")
    hauler = None
    for r in boosts.get("legendary", {}).get("rows", []):
        m = re.search(r"(\d+)", r.get("effect", ""))
        if r["name"].startswith("Hauler") and m:
            hauler = {"label": r["name"], "star": r.get("star"), "value": int(m.group(1)), "armour": r.get("armour"), "pa": r.get("pa")}

    foods = []
    for r in boosts.get("food", {}).get("rows", []):
        if any(f["id"] == r["id"] and f["diet"] == r["diet"] for f in foods):
            continue
        foods.append({k: r.get(k) for k in ("diet", "dish", "id", "durationSec", "base", "mutation", "sin", "stacks", "recommended", "category", "seasonal")})

    def items(bid):
        out = []
        for r in boosts.get(bid, {}).get("rows", []):
            for e in r.get("effects", []):
                if "Carry Weight" in e.get("text", "") and e.get("magnitude"):
                    sin = re.search(r"\+(\d+) Carry Weight in a team with Strange In Numbers", e["text"])
                    out.append({"name": r["name"], "id": r.get("id"), "value": e["magnitude"],
                                "sin": int(sin.group(1)) if sin else None, "duration": e.get("duration"),
                                "other": r.get("other", [])})
        return out

    strong_back = curves.get("StrongBackBonus")
    if not strong_back:
        warnings.append("StrongBackBonus curve missing from specials_stats")

    out = {
        "version": 1,
        "generated": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        "channel": CHANNEL,
        "sources": {"specials_stats": os.path.relpath(ss_path, ROOT).replace("\\", "/"),
                    "curv": os.path.basename(curv_path), "omod": os.path.basename(omod_path),
                    "ench": os.path.basename(ench_path) if ench_path else None},
        "base": {"value": base["value"], "src": base["src"]},
        "perStrength": {"value": mult["value"], "src": mult["src"]},
        "strongBack": {"points": strong_back["points"] if strong_back else [], "src": {"type": "CURV", "edid": "StrongBackBonus"}},
        "backpacks": backpacks,
        "backpackMods": backpack_mods,
        "linings": linings,
        "calibratedShocks": {"value": shocks, "src": {"type": "OMOD", "name": "Calibrated Shocks"}} if shocks else None,
        "excavator": excavator,
        "hauler": hauler,
        "foods": foods,
        "alcohol": items("alcohol"),
        "magazines": items("magazines"),
        "mutations": items("mutations"),
        "notes": carry.get("details", []),
        "warnings": warnings,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"wrote {os.path.relpath(OUT, ROOT)}: {len(backpacks)} backpacks, {len(backpack_mods) - 1} backpack mods, "
          f"{len(linings)} linings, {len(foods)} foods, excavator={excavator and excavator['value']}, "
          f"hauler={hauler and hauler['value']}, warnings={warnings}")


if __name__ == "__main__":
    main()
