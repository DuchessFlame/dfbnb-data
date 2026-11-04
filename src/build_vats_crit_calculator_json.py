"""V.A.T.S. Crit & Luck Calculator data (Kat 4 Nov 2026).

/bnb/calculators/vats-crit-luck-calculator/  ->  dist/calculators/vats_crit_calculator.json
(df-bnb-vats-crit-calculator.js reads it).

Every number comes from the game exports in tsv/ (newest of each) - nothing is typed
in by hand. The choices the files can't make (the crit meter maths runs on Bethesda's
servers) are NOT made here: the page shows both meter amounts the files hold and leaves
the rest as dev questions (Kat 4 Nov: game files only, no assumptions).

  crit meter  CURV CT_LuckVATSCriticalCharge (meter per hit by Luck), FourLeafCloverBonus
              (share of a hit a miss gives), GMST fVATSCriticalChargeBase / Mult,
              exe fVATSCriticalChargeLimit (100, meter cap), WEAP DNAM CRDT_CritChargeBonus
              (weapon's own bonus), PERKEP CriticalSavvy01-03 (meter used per crit),
              PERKEP Legendary_Armor_LimitBreakPerk (x per piece), OMOD
              mod_Legendary_Weapon3_CritSpeed "Lucky Hit" (CriticalChargeBonus ADD).
  Luck        SPEL AbLgnPerkLuck (Legendary Luck), OMOD mod_Legendary_Armor2_StatLuck /
              PowerArmor2 / Weapon3 (legendary Luck), CURV CT_Legendary_Armor_
              LowHealthIncreasesStats (Unyielding), SPEL P62_Legendary_Armor_Crusaders
              SPECIALSpell, Mutation_HerdMentality, abUnderArmor_*_Buffs, GHL_AbPerk
              UnitedOrdeal, ALCH Perk Happy-Go-Lucky effects, Magazine_LiveAndLove05Spell,
              GLOB BobbleHead_SPECIALGlobal, NWOT_FortuneSpell (Lady G), food / drink /
              chem Luck items from dist/calculators/food_buff_calculator.json (diet x).
"""
from __future__ import annotations
import csv, datetime, glob, json, os, re, sys

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV = os.path.join(ROOT, "tsv")
OUT = os.path.join(ROOT, "dist", "calculators", "vats_crit_calculator.json")
RULES = os.path.join(ROOT, "data", "vats_crit_calculator", "rules.json")
FOOD = os.path.join(ROOT, "dist", "calculators", "food_buff_calculator.json")
EXE = os.path.join(ROOT, "dist", "exe_settings.json")
WARN = []


def warn(m):
    WARN.append(m); print("[vats_crit] WARN", m, file=sys.stderr)


def newest(pattern):
    try:
        sys.path.insert(0, os.path.join(ROOT, "src")); import tsv_source; key = tsv_source.export_key
    except Exception:  # noqa: BLE001
        key = os.path.getmtime
    fs = [f for f in glob.glob(os.path.join(TSV, pattern))]
    if not fs:
        raise SystemExit(f"missing export {pattern}")
    return max(fs, key=key)


def rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def fnum(v, d=None):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


def main():
    rules = json.load(open(RULES, encoding="utf-8"))
    src = {k: newest(p) for k, p in {
        "curv": "CURV_Export_*_POINTS.tsv", "gmst": "GMST_Export_*.tsv", "perkep": "PERKEP_Export_*.tsv",
        "spel": "SPEL_Export_*_EFFECTS.tsv", "omod": "OMOD_Export_*_Properties.tsv", "omodh": "OMOD_Export_*[0-9].tsv",
        "glob": "GLOB_Export_*.tsv", "dnam": "WEAP_Export_*_DNAM.tsv", "alch": "ALCH_Export_*_Effects.tsv"}.items()}

    curves = {}
    for r in rows(src["curv"]):
        x, y = fnum(r.get("X")), fnum(r.get("Y"))
        if x is not None and y is not None:
            curves.setdefault(r["EDID"].strip(), []).append([x, y])
    for k in curves: curves[k].sort()
    gm = {r["EDID"].strip(): fnum(r.get("Value")) for r in rows(src["gmst"])}
    gl = {r["EDID"].strip(): fnum(r.get("FLTV")) for r in rows(src["glob"])}
    ep = {}
    for r in rows(src["perkep"]):
        ep.setdefault(r["PERK_EDID"].strip(), []).append(r)
    spel = {}
    for r in rows(src["spel"]):
        spel.setdefault(r["SPEL_EDID"].strip(), []).append(r)
    omod = {}
    for r in rows(src["omod"]):
        omod.setdefault(r["OMOD_EDID"].strip(), []).append(r)
    omod_name = {r["OMOD_EDID"].strip(): (r.get("FULL") or "").strip() for r in rows(src["omodh"])}

    def need(c):
        if c not in curves: warn(f"curve {c} missing")
        return curves.get(c, [])

    def ep_val(perk, entry):
        for r in ep.get(perk, []):
            if r.get("EntryPoint") == entry: return fnum(r.get("Value"))
        warn(f"{perk} {entry} missing"); return None

    def spel_luck(edid):
        """[(magnitude, condition text)] of the Luck effects of a spell."""
        out = []
        for r in spel.get(edid, []):
            e = r.get("EFID_MGEF_EDID") or ""
            if (r.get("MAGA_AVIF_EDID") or "") == "Luck" or "Luck" in e or e == "Magazine_LiveAndLove05Effect":
                out.append((fnum(r.get("EFIT_Magnitude"), 0), r.get("Conditions") or ""))
        if not out: warn(f"spell {edid}: no Luck effect")
        return out

    def omod_prop(edid, prop, contains=None):
        for r in omod.get(edid, []):
            if r.get("PropertyName") == prop and (not contains or contains in (r.get("Value1") or "")):
                return fnum(r.get("Value2")) if contains else fnum(r.get("Value1"))
        warn(f"OMOD {edid} {prop} missing"); return None

    # ---- crit meter ------------------------------------------------------------
    exe_limit = 100.0
    try:
        ex = json.load(open(EXE, encoding="utf-8"))
        for s in (ex.get("settings") if isinstance(ex, dict) else ex) or []:
            if isinstance(s, dict) and s.get("name") == "fVATSCriticalChargeLimit": exe_limit = fnum(s.get("exe_default"), 100.0)
    except Exception:  # noqa: BLE001
        pass
    savvy = {0: 100.0}
    for i, p in enumerate(["CriticalSavvy01", "CriticalSavvy02", "CriticalSavvy03"], 1):
        savvy[i] = ep_val(p, "Mod VATS Critical Cost")
    lb = {0: 1.0}
    for r in ep.get("Legendary_Armor_LimitBreakPerk", []):
        m = re.search(r"HasLegendary_Armor_LimitBreak \[KYWD:[0-9A-F]+\]\) \d+ ([0-9.]+)", r.get("Conditions") or "")
        if m: lb[int(float(m.group(1)))] = fnum(r.get("Value"))
    wb = {}
    for r in rows(src["dnam"]):
        e = r.get("WEAP_EDID") or ""
        if e.lower().startswith(("cr", "zzz", "del_", "npc", "hto_", "sdow_", "debug")): continue
        v = fnum(r.get("CRDT_CritChargeBonus"))
        if v is not None: wb[v] = wb.get(v, 0) + 1
    weapon_bonus = max(wb, key=wb.get) if wb else 1.0
    if len([k for k in wb if k]) > 1: warn(f"player weapons with different crit charge bonus: {wb}")
    meter = {
        "max": exe_limit,
        "base": gm.get("fVATSCriticalChargeBase"), "gmstMult": gm.get("fVATSCriticalChargeMult"),
        "weaponBonus": weapon_bonus,
        "luckyHit": omod_prop("mod_Legendary_Weapon3_CritSpeed", "CriticalChargeBonus"),
        "luckyHitName": omod_name.get("mod_Legendary_Weapon3_CritSpeed", "Lucky Hit"),
        "luckCurve": need("CT_LuckVATSCriticalCharge"),
        "fourLeaf": need("FourLeafCloverBonus"),
        "savvy": savvy, "limitBreak": lb,
        "limitBreakName": omod_name.get("mod_Legendary_Armor4_LimitBreak", "Limit-Breaking"),
            }

    # ---- Luck sources ------------------------------------------------------------
    lgn = spel_luck("AbLgnPerkLuck")                     # rank 1 always, +1 r2, +1 r3, +2 r4
    lgn_ranks, tot = [], 0
    for mag, cond in lgn:
        tot += mag; lgn_ranks.append(tot)
    herd = {"team": None, "teamSin": None, "solo": None}
    for r in spel.get("Mutation_HerdMentality", []):
        e, mag, c = r.get("EFID_MGEF_EDID") or "", fnum(r.get("EFIT_Magnitude"), 0), r.get("Conditions") or ""
        if e == "Mutation_FortifyLuck_Hidden": herd["teamSin" if "SuperVersion" in c else "team"] = mag
        if e == "Mutation_ReduceLuck_Hidden": herd["solo"] = -mag
    under = {}
    for k, lab in (("abUnderArmor_VaultSuit_Buffs", "Vault Suit"), ("abUnderArmor_Casual_Buffs", "Casual"), ("abUnderArmor_Raider_Buffs", "Raider")):
        lv = []
        for mag, c in spel_luck(k):
            m = re.search(r"dn_VaultSuitLining(\d)", c); lv.append(int(m.group(1)) if m else 0)
        under[lab] = {"v": 1, "minLining": min(lv) if lv else 0}
    ordeal = [m for m, c in spel_luck("GHL_AbPerkUnitedOrdeal")]
    crus = None
    for mag, c in spel_luck("P62_Legendary_Armor_CrusadersSPECIALSpell"):
        m = re.search(r">= ([0-9.]+)", c); crus = {"v": mag, "pieces": int(float(m.group(1))) if m else 4}
    hgl = {}
    for r in rows(src["alch"]):
        e = r.get("MGEF_EDID") or ""
        if e in ("PerkHappyGoLuckyFortifyLuck", "PerkHappyGoLucky02FortifyLuck"):
            k = 2 if "02" in e else 1
            d = fnum(r.get("EFIT_Duration"), 0)
            if k not in hgl or d > hgl[k]["d"]: hgl[k] = {"v": fnum(r.get("EFIT_Magnitude")), "d": d}
    ll5 = spel_luck("Magazine_LiveAndLove05Spell")
    fortune = spel_luck("NWOT_FortuneSpell_PositiveMajor")
    luck = {
        "legendaryPerk": lgn_ranks,
        "armourLegendary": omod_prop("mod_Legendary_Armor2_StatLuck", "Actor Values", "Luck"),
        "paLegendary": omod_prop("mod_Legendary_PowerArmor2_StatLuck", "Actor Values", "Luck"),
        "weaponLegendary": omod_prop("mod_Legendary_Weapon3_StatLuck", "ActorValues", "Luck"),
        "unyielding": need("CT_Legendary_Armor_LowHealthIncreasesStats"),
        "crusaders": crus, "herd": herd, "underarmour": under, "unitedOrdeal": ordeal,
        "happyGoLucky": hgl, "liveLove5": ll5[0][0] if ll5 else None,
        "bobblehead": gl.get("BobbleHead_SPECIALGlobal"),
        "ladyG": gl.get("NWOT_Fortune_Effect_Mag_Major") if fortune else None,
    }

    # ---- food / drinks / chems with Luck (from the food buff calculator data) -----
    items = []
    try:
        fd = json.load(open(FOOD, encoding="utf-8"))
        never = set(fd.get("rules", {}).get("never_recommend") or [])
        for it in fd["items"]:
            if it["t"] not in ("food", "alcohol", "chem"): continue
            lk = [f for f in it["fx"] if f.get("s") == "luck" and not f.get("neg")]
            base = [f for f in lk if not f.get("w")]
            if not base: continue
            f = base[0]
            team = [g for g in lk if g.get("w") == "team"]
            menu = it.get("menu") if isinstance(it.get("menu"), dict) else {}
            items.append({"n": it["n"], "t": it["t"], "v": f.get("m"), "d": f.get("d"),
                          "dm": f.get("dm"), "pm": f.get("pm"), "team": team[0].get("m") if team else None,
                          "bnb": menu.get("px") is not None, "dg": it.get("dgAll") or [],
                          "never": it["n"] in never})
        party = fd["mechanics"].get("partyGirl")
    except Exception as e:  # noqa: BLE001
        warn(f"food buff data not read ({e})"); party = None
    items.sort(key=lambda x: (x["t"], -(x["v"] or 0), x["n"]))

    out = {"version": 1, "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
           "page": {"path": "/bnb/calculators/vats-crit-luck-calculator/", "title": "VATS Crit & Luck Calculator"},
           "meter": meter, "luck": luck, "items": items, "partyBoy": party, "defaults": rules.get("defaults", {}),
           "sources": {k: os.path.basename(v) for k, v in src.items()}, "warnings": WARN}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[vats_crit] wrote {OUT}: {len(items)} Luck items, warnings {len(WARN)}")


if __name__ == "__main__":
    main()
