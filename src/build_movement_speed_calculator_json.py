"""Movement Speed Calculator data (Kat 14 Nov 2026).

/bnb/calculators/movement-speed-calculator/  ->  dist/calculators/movement_speed_calculator.json
(df-bnb-movement-speed-calculator.js reads it; loaded by the router in df-bnb-bnb-calculators.js).

Your movement speed is the Move Speed actor value (SpeedMult, base 100 = normal speed).
Everything that changes it is a magic effect on that actor value, so the list comes straight from
tsv/SPEEDX_Export_* (tools/esm_speed_extract.py reads SeventySix.esm; the xEdit MGEF export has no
actor value column). How effects add up comes from each magic effect's archetype:
  0  Value Modifier       every one adds (Speed Demon, Dead Man Sprinting, Overdrive Servos,
                          Emergency Protocols)
  34 Peak Value Modifier  only refuses to stack with effects carrying its no-stack keyword
                          (Assoc. Item 2, SPEEDX NoStackKeyword). None of the speed effects has
                          one, so they all add up (Creation Kit wiki; patch 56 notes: Propelling
                          "+5% up to +25% on full stack"). Kat 14 Nov 2026.
When each one works comes from its conditions (IsSprinting 1 = sprinting only, 0 = not sprinting,
WornHasKeyword ArmorTypePower / WeaponTypeRanged, IsMemberOfAPlayerTeam, GetHealthPercentage,
GetValuePercent Health, GetIsPlayerGhoul + GetValue Rads, Mutation_Check_UseSuperVersion = Strange
in Numbers). Ranks: the HasPerk conditions on each effect (lowest magnitude = rank 1).

Other sources:
  dist/perk_cards.json                   card name, SPECIAL, rank count
  GMST export                            fMoveSpeedMultMax = the most Move Speed can be (150)
  OMOD properties export                 _PARENT_mod_PowerArmor_GENERIC_SprintBoost adds SprintSpeedMult
                                         per leg; EnchPowerArmor_OverdriveServos takes its magnitude from
                                         that (MAGA) while sprinting

Usage:  python src/build_movement_speed_calculator_json.py [--pts]
"""
from __future__ import annotations
import csv, datetime, glob, json, os, re, sys

csv.field_size_limit(10 ** 9)
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
PTS = "--pts" in sys.argv
TSV = os.path.join(REPO, "tsv")
warnings: list[str] = []


def newest(pattern: str, pts_ok: bool = True) -> str | None:
    if PTS and pts_ok:
        c = sorted(glob.glob(os.path.join(TSV, "pts", pattern.replace("_Export_", "_Export_PTS_"))))
        if c:
            return c[-1]
        warnings.append("no PTS %s - used live" % pattern)
    c = glob.glob(os.path.join(TSV, pattern))
    return max(c, key=os.path.getmtime) if c else None


def read_tsv(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def tidy(x: float):
    return int(x) if abs(x - round(x)) < 1e-9 else round(x, 4)


SPX = newest("SPEEDX_Export_*.tsv")
if not SPX:
    sys.exit("no tsv/SPEEDX_Export_*.tsv - run tools/esm_speed_extract.py first")
rows = read_tsv(SPX)


def eff(edid: str):
    out = [r for r in rows if r["EDID"] == edid]
    if not out:
        warnings.append("SPEEDX: %s not found" % edid)
    return out


def conds(r):
    """[(function, compare value, param text, type bits)]"""
    res = []
    for c in (r.get("Conditions") or "").split(" ;; "):
        p = c.split("|")
        if len(p) >= 3:
            res.append((p[2], float(p[1] or 0), c, p[0]))
    return res


def cval(r, fn, param=None):
    for f, v, txt, _t in conds(r):
        if f == fn and (param is None or param in txt):
            return v
    return None


def when(r):
    s = cval(r, "IsSprinting")
    return "both" if s is None else ("sprint" if s == 1 else "run")


def grp(r):
    # Peak Value Modifier with a no-stack keyword: only the biggest with that keyword counts.
    # No keyword (every speed effect, Nov 2026) = stacks like a Value Modifier.
    kw = (r.get("NoStackKeyword") or "").strip()
    return kw if r.get("Archetype") == "34" and kw else None


def ranked(edid):
    rs = sorted(eff(edid), key=lambda r: float(r["Magnitude"]))
    return [tidy(float(r["Magnitude"])) for r in rs], rs


# perk cards (name, SPECIAL, ranks)
pc_path = os.path.join(REPO, "dist", "perk_cards.pts.json" if PTS and os.path.exists(os.path.join(REPO, "dist", "perk_cards.pts.json")) else "perk_cards.json")
cards = {c["id"]: c for c in json.load(open(pc_path, encoding="utf-8"))["cards"]}


def card(cid, spel, need):
    c = cards.get(cid)
    vals, rs = ranked(spel)
    if not c or not vals:
        warnings.append("perk %s / %s missing" % (cid, spel))
        return None
    if c.get("rankCount") and c["rankCount"] != len(vals):
        warnings.append("%s: %d ranks on the card, %d speed effects" % (cid, c["rankCount"], len(vals)))
    out = {"name": c["name"], "special": c.get("special", ""), "ranks": vals, "when": when(rs[0]),
           "group": grp(rs[0]), "src": {"type": "SPEL", "edid": spel}}
    out.update(need(rs))
    return out


perks = {}
perks["gunRunner"] = card("GunRunnerCard", "AbPerkGunRunner", lambda rs: {"needs": "ranged"})
perks["squadManeuvers"] = card("SquadManeuversCard", "AbPerkSquadManeuvers", lambda rs: {"needs": "team"})
perks["portablePower"] = card("PortablePowerCard", "AbPerkPortablePower", lambda rs: {"needs": "powerArmour"})
perks["deadManSprinting"] = card("DeadManSprintingCard", "AbPerkDeadManSprinting",
                                 lambda rs: {"needs": "lowHp", "hpBelow": [tidy(cval(r, "GetHealthPercentage") * 100) for r in rs]})
perks["jaguarSpeed"] = card("GHL_JaguarSpeedCard", "GHL_AbPerkJaguarSpeed",
                            lambda rs: {"needs": "glow", "ghoulOnly": True, "radsAtLeast": tidy(cval(rs[0], "GetValue", "Rads") or 0)})
perks = {k: v for k, v in perks.items() if v}

# Speed Demon: normal / Strange in Numbers ("super") version
sd = eff("Mutation_SpeedDemon")
speed_demon = None
if sd:
    norm = [r for r in sd if "UseNormalVersion" in r["Conditions"]]
    sup = [r for r in sd if "UseSuperVersion" in r["Conditions"]]
    if norm and sup:
        speed_demon = {"name": sd[0]["FULL"] or "Speed Demon", "value": tidy(float(norm[0]["Magnitude"])),
                       "sin": tidy(float(sup[0]["Magnitude"])), "when": when(norm[0]), "group": grp(norm[0]),
                       "src": {"type": "SPEL", "edid": "Mutation_SpeedDemon"}}
    else:
        warnings.append("Speed Demon: normal / Strange in Numbers versions not found")

# Freight Train backpack mod
ft = eff("AbPerkFreightTrain")
freight = {"name": ft[0]["FULL"] or "Freight Train", "value": tidy(float(ft[0]["Magnitude"])), "when": when(ft[0]),
           "group": grp(ft[0]), "src": {"type": "SPEL", "edid": "AbPerkFreightTrain"}} if ft else None

# Power armour
ep = eff("EnchPowerArmor_EmergencyProtocols")
emergency = None
if ep:
    hp = cval(ep[0], "GetValuePercent", "Health")
    emergency = {"name": ep[0]["FULL"] or "Emergency Protocols", "value": tidy(float(ep[0]["Magnitude"])),
                 "hpBelow": tidy((hp or 0) * 100), "orEqual": ep[0]["Conditions"].split("|")[0][:3] == "101",
                 "when": when(ep[0]), "group": grp(ep[0]), "src": {"type": "ENCH", "edid": "EnchPowerArmor_EmergencyProtocols"}}

od = eff("EnchPowerArmor_OverdriveServos")
overdrive = None
omod_path = newest("OMOD_Export_*_Properties.tsv", pts_ok=False)
sprint_av = None
if omod_path:
    for r in read_tsv(omod_path):
        if r.get("OMOD_EDID") == "_PARENT_mod_PowerArmor_GENERIC_SprintBoost" and "SprintSpeedMult" in (r.get("Value1") or ""):
            sprint_av = float(r.get("Value2") or 0)
if od and od[0]["MAGA_EDID"] == "SprintSpeedMult" and sprint_av:
    overdrive = {"name": od[0]["FULL"] or "Overdrive Servos", "perLeg": tidy(sprint_av), "when": when(od[0]),
                 "group": grp(od[0]), "src": {"type": "OMOD+ENCH", "edid": "_PARENT_mod_PowerArmor_GENERIC_SprintBoost / EnchPowerArmor_OverdriveServos"}}
else:
    warnings.append("Overdrive Servos not found (SPEEDX / OMOD)")

pr = eff("ench_LegendaryPowerArmor_Propelling")
propelling = {"name": "Propelling", "star": 4, "perPiece": tidy(float(pr[0]["Magnitude"])), "when": when(pr[0]),
              "group": grp(pr[0]), "src": {"type": "ENCH", "edid": "ench_LegendaryPowerArmor_Propelling"}} if pr else None

# Speed cap: GMST fMoveSpeedMultMax (Move Speed can't go above this; 150 in Oct 2026)
cap = None
gm = newest("GMST_Export_*.tsv")
if gm:
    for r in read_tsv(gm):
        vals = list(r.values())
        if len(vals) >= 4 and vals[1] == "fMoveSpeedMultMax":
            try:
                cap = tidy(float(vals[3]))
            except ValueError:
                pass
if cap is None:
    warnings.append("GMST fMoveSpeedMultMax not found - no speed cap")

# Over-encumbered (Kat 14 Nov 2026). It doesn't slow your running: no sprinting, running drains AP,
# at 0 AP you walk. AP drain a second, from Fallout76.exe (function at 0x140b703d0, read by the PC):
#   (weight x WeightMult + WeightBase) x (Endurance x EndMult + EndBase) x DrainMult
# Values: dist/exe_settings.json (src/exe_settings.py) - the ESM value replaces the exe default.
# AP needed to run again: fMinAPRecoverToEnableEncumberedRunning (ESM 40; server-side).
encumbered = None
exe_path = os.path.join(REPO, "dist", "exe_settings.json")
if os.path.exists(exe_path):
    es = json.load(open(exe_path, encoding="utf-8")).get("settings") or {}
    es = es if isinstance(es, dict) else {x["name"]: x for x in es}

    def setting(name):
        x = es.get(name)
        if not x:
            return None
        v = x.get("esm_value")
        try:
            return tidy(float(v)) if v not in (None, "", "None") else tidy(float(x.get("exe_default")))
        except (TypeError, ValueError):
            return None
    p = "fEncumberedRunningActionPoints"
    vals = {k: setting(p + k) for k in ("DrainMult", "WeightMult", "WeightBase", "EndMult", "EndBase")}
    if None in vals.values():
        warnings.append("exe_settings: encumbered running settings missing %s" % vals)
    else:
        encumbered = {"drain": vals, "apToRunAgain": setting("fMinAPRecoverToEnableEncumberedRunning"),
                      "src": {"type": "EXE+GMST", "edid": p + "*"}}
else:
    warnings.append("no dist/exe_settings.json - run src/exe_settings.py")

# Food
food = []
for r in rows:
    if r["Signature"] == "ALCH" and float(r["Magnitude"]) > 0 and "Damage" not in r["MGEF_EDID"]:
        food.append({"name": r["FULL"], "value": tidy(float(r["Magnitude"])), "minutes": tidy(int(r["Duration"]) / 60),
                     "when": when(r), "group": grp(r), "src": {"type": "ALCH", "edid": r["EDID"]}})
food.sort(key=lambda x: (-x["value"], x["name"]))

out = {
    "generated": datetime.date.today().isoformat(),
    "source": os.path.basename(SPX),
    "base": 100,
    "cap": cap,
    "perks": perks,
    "speedDemon": speed_demon,
    "freightTrain": freight,
    "powerArmour": {"overdriveServos": overdrive, "emergencyProtocols": emergency, "propelling": propelling},
    "food": food,
    "encumbered": encumbered,
    "warnings": warnings,
}
dest = os.path.join(REPO, "dist", "pts" if PTS else "", "calculators", "movement_speed_calculator.json")
os.makedirs(os.path.dirname(dest), exist_ok=True)
with open(dest, "w", encoding="utf-8", newline="\n") as f:
    json.dump(out, f, ensure_ascii=False, indent=1)
print("wrote %s: perks %s, speed demon %s, freight %s, PA %s, food %d, warnings=%s" % (
    os.path.relpath(dest, REPO), list(perks), bool(speed_demon), bool(freight),
    [k for k, v in out["powerArmour"].items() if v], len(food), warnings))
