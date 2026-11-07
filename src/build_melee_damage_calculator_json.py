"""Melee Damage Calculator data (Kat 7 Nov 2026).

/bnb/calculators/melee-damage-calculator/  ->  dist/calculators/melee_damage_calculator.json
                                               (--pts -> dist/pts/calculators/...)

Every number comes from the game files or data built from them (nothing typed in):
  weapons     dist/builds/builds_data.json (player melee / unarmed list) + WEAP Base / DNAM:
              damage curve CVT0 by weapon level (the curve wins over DNAM_BaseDamage - same rule
              as the weapon guides and boss stats), EILV levels, DAMA extra damage types,
              CRDT crit mult, WSAM sneak mult, keywords.
  mods        OMOD (+ included PARENT templates, recursive) on the weapon's ma_ keyword:
              AttackDamage MUL+ADD (physical x (1 + v1)), DamageBonusMult ADD (adds to the
              damage bonus), DamageTypeValues (MUL+ADD = % of base damage as that type,
              ADD = flat), Enchantments -> ENCH Damage effects (amount, or its level curve,
              over the effect's duration). Same reading as build_weapon_guides_json.py.
  bonus pile  PERKEP STAT_DamagePerk / STAT_DamageVsPerk "Mod Weapon DMG Bonus Mult"
              (Add Actor Value Mult) -> which actor value reaches which weapon / enemy.
  Strength    GMST fAVDMeleeDamageMult (melee weapons, per point) / fDamageStrengthMult
              (unarmed); additive with the other bonuses (exe bCombatUseAdditiveStrengthAttackBonus).
              Sources: AbLgnPerkStrength, OMOD *_StatStrength, CURV
              CT_Legendary_Armor_LowHealthIncreasesStats (Unyielding), Mutation_HerdMentality,
              underarmour styles, GHL_AbPerkUnitedOrdeal, Crusaders, BobbleHead_SPECIALGlobal.
  cards       AbPerkAdrenaline (AdrenalineBonus by kill streak), AbPerkNerdRage (NerdRageDamageBonus
              by HP %), AbPerkIronFist (IronFistBonus by DR), Ninja01-02 (Mod Sneak Attack Mult).
  mutations   Mutation_TwistedMuscles / Talons (normal + Strange In Numbers), Mutation_AdrenalReaction
              (AdrenalNormalBonus / AdrenalSuperBonus by kill streak).
  sneak       PlayerPerk "Mod Sneak Attack Mult" Add Actor Value Mult (STAT_SneakAttackBonus).
  legendary   dist/legendary_star_pages/{1..5}-star.json outputs (records: actor value, entry point,
              magnitude, curve + input, conditions).
  crit        dist/calculators/vats_crit_calculator.json "crit" (same maths as its Crit Damage tab).
  food etc.   dist/calculators/food_buff_calculator.json items (diet / Party Girl multipliers).

Left OFF the page until the devs answer (all server-side, no PC code reads them): power attack
damage (STAT_DmgPowerAttack - Heavy Hitter card, Heavy Hitter's 2*), damage vs crippled
(STAT_DmgVsCrippled - Slugger), vs bleeding (STAT_DmgVsBleeding - Wound Salter, Severing 4*),
per crippled limb (STAT_DmgPerCrippled - Bully's 4*), execute damage (LGND_ExecuteDmg).
"""
from __future__ import annotations
import csv, datetime, glob, json, os, re, sys

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV = os.path.join(ROOT, "tsv")
PTS = "--pts" in sys.argv
DIST = os.path.join(ROOT, "dist", "pts") if PTS else os.path.join(ROOT, "dist")
OUT = os.path.join(DIST, "calculators", "melee_damage_calculator.json")
WARN = []
SERVER_SIDE_AVS = {"STAT_DmgPowerAttack", "STAT_DmgVsCrippled", "STAT_DmgVsBleeding", "STAT_DmgPerCrippled",
                   "LGND_ExecuteDmg", "STAT_DmgVsBurning", "STAT_DmgVsPoisoned"}


def warn(m):
    WARN.append(m); print("[melee_dmg] WARN", m, file=sys.stderr)


def newest(pattern):
    try:
        sys.path.insert(0, os.path.join(ROOT, "src")); import tsv_source; key = tsv_source.export_key
    except Exception:  # noqa: BLE001
        key = os.path.getmtime
    d = os.path.join(TSV, "pts") if PTS else TSV
    fs = glob.glob(os.path.join(d, pattern))
    if not fs and PTS:
        fs = glob.glob(os.path.join(TSV, pattern))
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


def jload(*parts):
    p = os.path.join(DIST, *parts)
    if not os.path.exists(p):
        p = os.path.join(ROOT, "dist", *parts)
    return json.load(open(p, encoding="utf-8"))


def r2(v):
    return None if v is None else round(v, 4)


def main():
    src = {k: newest(p) for k, p in {
        "curv": "CURV_Export_*_POINTS.tsv", "gmst": "GMST_Export_*.tsv", "perkep": "PERKEP_Export_*.tsv",
        "spel": "SPEL_Export_*_EFFECTS.tsv", "omod": "OMOD_Export_*_Properties.tsv", "omodh": "OMOD_Export_*[0-9].tsv",
        "glob": "GLOB_Export_*.tsv", "dnam": "WEAP_Export_*_DNAM.tsv", "base": "WEAP_Export_*_Base.tsv",
        "ench": "ENCH_Export_*[0-9].tsv", "mgef": "MGEF_Export_*[0-9].tsv"}.items()}

    curves = {}
    for r in rows(src["curv"]):
        x, y = fnum(r.get("X")), fnum(r.get("Y"))
        if x is not None and y is not None:
            curves.setdefault(r["EDID"].strip(), []).append([x, y])
    for k in curves: curves[k].sort()

    def need(c):
        c = re.sub(r"\s*\[CURV:[0-9A-Fa-f]+\]", "", c or "").strip()
        if c and c not in curves: warn(f"curve {c} missing")
        return curves.get(c, [])

    gm = {r["EDID"].strip(): fnum(r.get("Value")) for r in rows(src["gmst"])}
    gl = {r["EDID"].strip(): fnum(r.get("FLTV")) for r in rows(src["glob"])}
    ep, spel, omod = {}, {}, {}
    for r in rows(src["perkep"]): ep.setdefault(r["PERK_EDID"].strip(), []).append(r)
    for r in rows(src["spel"]): spel.setdefault(r["SPEL_EDID"].strip(), []).append(r)
    for r in rows(src["omod"]): omod.setdefault(r["OMOD_EDID"].strip(), []).append(r)
    head = {r["OMOD_EDID"].strip(): r for r in rows(src["omodh"])}
    mgef_av = {r["EDID"].strip(): re.sub(r"\s*\".*|\s*\[AVIF.*", "", r.get("ActorValue") or "").strip()
               for r in rows(src["mgef"])}
    mgef_resist = {r["EDID"].strip(): re.sub(r"\s*\".*|\s*\[AVIF.*", "", r.get("ResistValue") or "").strip()
                   for r in rows(src["mgef"])}
    mgef_arch = {r["EDID"].strip(): (r.get("Archetype") or "").strip() for r in rows(src["mgef"])}
    ench = {r["ENCH_EDID"].strip(): r for r in rows(src["ench"])}

    def spel_fx(edid, mgef=None):
        out = []
        for r in spel.get(edid, []):
            if mgef and (r.get("EFID_MGEF_EDID") or "") != mgef: continue
            out.append({"mgef": r.get("EFID_MGEF_EDID") or "", "v": fnum(r.get("EFIT_Magnitude"), 0),
                        "curve": (r.get("CVTO_CURV_EDID") or "").strip(), "in": (r.get("MAGA_AVIF_EDID") or "").strip(),
                        "cond": r.get("Conditions") or ""})
        if not out: warn(f"spell {edid} {mgef or ''}: nothing")
        return out

    # ---- which damage bonus reaches which weapon / enemy ----------------------------
    reach = {}
    for perk in ("STAT_DamagePerk", "STAT_DamageVsPerk"):
        for r in ep.get(perk, []):
            if r.get("EntryPoint") != "Mod Weapon DMG Bonus Mult": continue
            av = re.search(r"Actor Value=(STAT_\w+)", r.get("Raw") or "")
            if not av: continue
            cond = r.get("Conditions") or ""
            anyk = re.findall(r"HasKeyword\((WeaponType\w+) \[KYWD:[0-9A-F]+\]\) \d+ 1\.0", cond)
            nonek = re.findall(r"HasKeyword\((WeaponType\w+) \[KYWD:[0-9A-F]+\]\) \d+ 0\.0", cond)
            enemy = perk == "STAT_DamageVsPerk"
            other = bool(re.search(r"GetInIronSights|IsTrueForConditionForm", cond)) and not enemy
            reach[av.group(1)] = {"any": sorted(set(anyk)), "none": sorted(set(nonek)), "per": fnum(r.get("Value")),
                                  "enemy": enemy, "skip": other}
    if "STAT_DmgAll" not in reach: warn("STAT_DmgAll not in STAT_DamagePerk")
    avif_words = {"STAT_DmgVsHumans": "Humans", "STAT_DmgVsZetans": "Aliens", "STAT_DmgVsYaoGuai": "Yao guai",
                  "STAT_DmgVsMirelurks": "Mirelurks", "STAT_DmgVsSuperMutants": "Super mutants", "STAT_DmgVsRobots": "Robots",
                  "STAT_DmgVsGhouls": "Ghouls", "STAT_DmgVsAnimals": "Animals", "STAT_DmgVsScorched": "Scorched",
                  "STAT_DmgVsGlowing": "Glowing enemies", "STAT_DmgVsBugs": "Insects", "STAT_DmgVsLiberators": "Liberators",
                  "STAT_DmgVsGulpers": "Gulpers", "STAT_DmgVsMolerats": "Mole rats", "STAT_DmgVsMoleMinersAndRats": "Mole miners",
                  "STAT_DmgVsCryptids": "Cryptids"}
    enemies = sorted({v for k, v in avif_words.items() if k in reach})
    # Exterminator's also says Mirelurks etc. - each AV is its own enemy row, an enemy picks every AV that names it
    enemy_av = {}
    for k, v in avif_words.items():
        if k in reach: enemy_av.setdefault(v, []).append(k)
    enemy_av.setdefault("Mole rats", []).append("STAT_DmgVsMoleMinersAndRats")

    # ---- weapons --------------------------------------------------------------------
    bw = jload("builds", "builds_data.json")["weapons"]
    dn = {r["WEAP_EDID"]: r for r in rows(src["dnam"])}
    base = {r["WEAP_EDID"]: r for r in rows(src["base"])}

    def mod_rows(e, seen=None):
        seen = seen if seen is not None else set()
        if e in seen: return []
        seen.add(e)
        out = list(omod.get(e, []))
        for inc in re.findall(r"(\S+) \"\"[^\"]*\"\" \[OMOD:", (head.get(e) or {}).get("Includes_Flat") or ""):
            out += mod_rows(inc, seen)
        return out

    def dtype_name(v1):
        m = re.match(r"dt(\w+)", (v1 or "").strip())
        return {"Energy": "energy", "Fire": "fire", "Cryo": "cryo", "Poison": "poison",
                "RadiationExposure": "radiation"}.get(m.group(1), m.group(1).lower()) if m else ""

    RESIST_WORD = {"FireResist": "fire", "EnergyResist": "energy", "PoisonResist": "poison", "FrostResist": "cryo",
                   "DamageResist": "physical", "RadResistExposure": "radiation"}

    def mod_effect(e):
        """-> {mul, pile, split:[{t,pct}], flat:[{t,v}], dot:[{t,v|curve,d}]}"""
        mul, pile, split, flat, dot = 1.0, 0.0, [], [], []
        for r in mod_rows(e):
            pn, fn = (r.get("PropertyName") or "").strip(), (r.get("FunctionType") or "").strip()
            v1, v2 = fnum(r.get("Value1"), 0), fnum(r.get("Value2"), 0)
            if pn == "AttackDamage" and fn == "MUL+ADD": mul *= (1 + v1)
            elif pn == "DamageBonusMult" and fn == "ADD": pile += v1
            elif pn == "DamageTypeValues":
                t = dtype_name(r.get("Value1"))
                if fn == "MUL+ADD" and v2: split.append({"t": t, "pct": r2(v2)})
                elif fn == "ADD" and v2: flat.append({"t": t, "v": r2(v2)})
            elif pn == "Enchantments" and fn == "ADD":
                en = re.match(r"(\S+)", (r.get("Value1") or "").strip())
                er = ench.get(en.group(1)) if en else None
                if not er: continue
                for i in range(1, 9):
                    mg = (er.get(f"Effect_{i}_MGEF_EID") or "").strip()
                    if not mg: break
                    if mgef_arch.get(mg) != "Damage": continue
                    t = RESIST_WORD.get(mgef_resist.get(mg), "")
                    if not t: continue
                    d = fnum(er.get(f"Effect_{i}_Duration"), 0) or 0
                    cv = (er.get(f"Effect_{i}_CURV_EID") or "").strip()
                    x = {"t": t, "d": d}
                    if cv: x["curve"] = need(cv)
                    else: x["v"] = r2(fnum(er.get(f"Effect_{i}_Magnitude"), 0))
                    if x.get("curve") or x.get("v"): (dot if d > 0 else flat).append(x)
        return {"mul": r2(mul), "pile": r2(pile), "split": split, "flat": flat, "dot": dot}

    mods_by_kw = {}
    for e, h in head.items():
        if "egendary" in e or e.startswith(("_PARENT", "zzz", "DEL_", "CUT_", "PARENT", "test", "Burn_")): continue
        ap = (h.get("AttachPoint_Name") or "").strip()
        if not ap or any(t in ap for t in ("ppearance", "Paint", "egendary", "Unique", "Customization", "Skin")): continue
        name = (h.get("FULL") or "").strip()
        if not name: continue
        for kw in re.findall(r"(ma_\w+)", h.get("MNAM_TargetKWDs") or ""):
            if "ppearance" in kw or "egendary" in kw: continue
            mods_by_kw.setdefault(kw, []).append((e, name, ap))

    weapons = []
    for w in bw:
        cls = [c for c in (w.get("classes") or []) if c in ("melee", "unarmed")]
        if not cls: continue
        e = w.get("edid"); d = dn.get(e); b = base.get(e)
        if not d or not b: warn(f"weapon {e} not in WEAP export"); continue
        kw = b.get("Keywords") or ""
        tags = sorted(set(re.findall(r"(WeaponType\w+)", kw)))
        cv = need(d.get("CVT0_DamageCurve")) if (d.get("CVT0_DamageCurve") or "").strip() and "NULL" not in d.get("CVT0_DamageCurve") else []
        flat = fnum(d.get("DNAM_BaseDamage"), 0)
        lv = sorted({int(x) for x in re.findall(r"\d+", b.get("EILV_Levels") or "")} | ({int(fnum(b.get("EILV_Level"), 0))} if b.get("EILV_Level") else set()))
        lv = [x for x in lv if x > 0] or [1]
        extra = []
        for ent in (d.get("DAMA_DamageTypes") or "").split("|"):
            bits = ent.split(":")
            if len(bits) >= 2 and bits[0]:
                x = {"t": dtype_name(bits[0])}
                if len(bits) > 2 and bits[2]: x["curve"] = need(bits[2])
                else: x["v"] = fnum(bits[1], 0)
                if x.get("curve") or x.get("v"): extra.append(x)
        mods, seen = [], set()
        for m in sorted(set(re.findall(r"(ma_\w+)", kw))):
            for me, mn, ap in mods_by_kw.get(m, []):
                if (mn, ap) in seen: continue
                seen.add((mn, ap))
                mods.append({"n": mn, "slot": ap, **mod_effect(me)})
        mods.sort(key=lambda x: (x["slot"], x["n"]))
        wcls = "unarmed" if "WeaponTypeUnarmed" in tags else "melee"     # the weapon's own keyword decides
        weapons.append({"n": w.get("name") or b.get("WEAP_FULL"), "e": e, "cls": wcls, "tags": tags,
                        "curve": cv, "base": flat, "levels": lv, "extra": extra,
                        "crit": fnum(d.get("CRDT_CritDamageMult")), "sneak": fnum(d.get("WSAM_SneakAttackMult")),
                        "ap": fnum(d.get("DNAM_ActionPointCost")), "mods": mods})
    weapons.sort(key=lambda x: (x["cls"], x["n"]))
    if not weapons: warn("no melee weapons")

    # ---- Strength ---------------------------------------------------------------------
    def omod_av(edid, prop, contains):
        for r in omod.get(edid, []):
            if r.get("PropertyName") == prop and contains in (r.get("Value1") or ""): return fnum(r.get("Value2"))
        warn(f"OMOD {edid} {prop} {contains} missing"); return None

    lgn, tot = [], 0
    for x in spel_fx("AbLgnPerkStrength"):
        tot += x["v"]; lgn.append(tot)
    herd = {"team": None, "teamSin": None, "solo": None}
    for x in spel_fx("Mutation_HerdMentality"):
        if x["mgef"] == "Mutation_FortifyStrength_Hidden": herd["teamSin" if "SuperVersion" in x["cond"] else "team"] = x["v"]
        if x["mgef"] == "Mutation_ReduceStrength_Hidden": herd["solo"] = -x["v"]
    under = {}
    for e, props in sorted(omod.items()):
        if not e.startswith("mod_armor_UnderArmor_style_"): continue
        for r in props:
            if r.get("PropertyName") == "Actor Values" and "Strength" in (r.get("Value1") or ""):
                under[(head.get(e) or {}).get("FULL", "").strip() or e] = fnum(r.get("Value2"))
    ordeal = [x["v"] for x in spel_fx("GHL_AbPerkUnitedOrdeal") if x["mgef"] == "AbPerkFortifyStrength"]
    crus = None
    for x in spel_fx("P62_Legendary_Armor_CrusadersSPECIALSpell"):
        if x["mgef"] == "AbPerkFortifyStrength":
            m = re.search(r">= ([0-9.]+)", x["cond"]); crus = {"v": x["v"], "pieces": int(float(m.group(1))) if m else 5}
    strength = {
        "meleePer": r2((gm.get("fAVDMeleeDamageMult") or 0) * 100), "unarmedPer": r2((gm.get("fDamageStrengthMult") or 0) * 100),
        "legendaryPerk": lgn,
        "armourLegendary": omod_av("mod_Legendary_Armor2_StatStrength", "Actor Values", "Strength"),
        "paLegendary": omod_av("mod_Legendary_PowerArmor2_StatStrength", "Actor Values", "Strength"),
        "unyielding": need("CT_Legendary_Armor_LowHealthIncreasesStats"),
        "herd": herd, "underarmour": under, "unitedOrdeal": ordeal, "crusaders": crus,
        "bobblehead": gl.get("BobbleHead_SPECIALGlobal"),
    }

    # ---- perk cards + mutations ----------------------------------------------------------
    def curve_fx(edid, mgef):
        x = spel_fx(edid, mgef)
        return need(x[0]["curve"]) if x else []
    cards = {
        "adrenaline": curve_fx("AbPerkAdrenaline", "abPerkFortifyDmgAll"),
        "nerdRage": curve_fx("AbPerkNerdRage", "abPerkFortifyDmgAll"),
        "ironFist": curve_fx("AbPerkIronFist", "AbPerkFortifyDmgMeleeUnarmed"),
        "ninja": [fnum(r.get("Value")) for p in ("Ninja01", "Ninja02") for r in ep.get(p, [])
                  if r.get("EntryPoint") == "Mod Sneak Attack Mult"][:2],
    }
    if len(cards["ninja"]) != 2: warn("Ninja ranks not found")
    sneak_per = None
    for r in ep.get("PlayerPerk", []):
        if r.get("EntryPoint") == "Mod Sneak Attack Mult" and "STAT_SneakAttackBonus" in (r.get("Raw") or ""): sneak_per = fnum(r.get("Value"))
    if sneak_per is None: warn("PlayerPerk sneak attack bonus not found")

    def mut(edid, mgef):
        o = {}
        for x in spel_fx(edid, mgef):
            o["sin" if "SuperVersion" in x["cond"] else "normal"] = x["v"]
        return o
    adr = {}
    for x in spel_fx("Mutation_AdrenalReaction", "abPerkFortifyDmgAll"):
        adr["sin" if "Super" in x["curve"] else "normal"] = need(x["curve"])
    mutations = {
        "twisted": {**mut("Mutation_TwistedMuscles", "Mutation_TwistedMusclesEffect"), "av": mgef_av.get("Mutation_TwistedMusclesEffect")},
        "talons": {**mut("Mutation_Talons", "Mutation_TalonsEffect"), "av": mgef_av.get("Mutation_TalonsEffect")},
        "adrenal": adr,
    }

    # ---- legendary effects ---------------------------------------------------------------
    def out_kind(o):
        rec = o.get("records") or {}
        av = re.sub(r"\s*\[AVIF.*", "", rec.get("actorValue") or "").strip()
        epn = (rec.get("entryPoint") or "").strip()
        if av in SERVER_SIDE_AVS: return "later", av
        if av == "Strength": return "str", av
        if av == "STAT_SneakAttackBonus": return "sneak", av
        if av in reach and not reach[av].get("skip"): return "pile", av
        if epn == "Mod Weapon DMG Bonus Mult" and (rec.get("function") or "").startswith("Add Value"): return "pileEP", "STAT_DmgAll"
        if epn == "Mod Damage on Consecutive Hits": return "stack", "STAT_DmgAll"
        if "critical damage" in (o.get("text") or "").lower() and not av: return "crit", ""
        return None, av

    leg = {}
    for s in range(1, 6):
        lst = []
        for it in jload("legendary_star_pages", f"{s}-star.json")["items"]:
            for blk in it.get("outputs", []):
                ap = blk.get("appliesTo") or []
                if not any(a in ("Weapons", "Melee weapons") for a in ap): continue
                outs, later = [], []
                for o in blk.get("outputs", []):
                    k, av = out_kind(o)
                    rec = o.get("records") or {}
                    if k == "later": later.append(av); continue
                    if not k: continue
                    x = {"k": k, "av": av, "when": o.get("when") or [], "text": o.get("gameText") or o.get("text") or ""}
                    cv = o.get("curve") or {}
                    if k == "str" and not cv.get("points") and (rec.get("curveInput") or "") == "Kill Streak":
                        # Barbarian: no curve, the magnitude is the Kill Streak value itself (+1 per kill)
                        x["curve"] = [[0, 0], [100, 100]]; x["in"] = "Kill Streak"; x["inPct"] = False
                    elif cv.get("points"):
                        x["curve"] = [[p["x"], p["y"]] for p in cv["points"]]; x["in"] = cv.get("input") or ""
                        x["inPct"] = bool(cv.get("inputIsPercent"))
                    elif k == "pileEP": x["v"] = r2((fnum(rec.get("value"), 0) or 0) * 100)
                    elif k == "stack":
                        x["per"] = r2((fnum(rec.get("value"), 0) or 0) * 100)
                    elif k == "crit":
                        m = re.search(r"([0-9.]+)", (rec.get("value") or o.get("text") or "")); x["v"] = fnum(m.group(1)) if m else None
                        if x["v"] and x["v"] > 5: x["v"] = x["v"] / 100
                    else:
                        v = rec.get("magnitude")
                        if v in (None, 0, 0.0):
                            m = re.search(r"ADD ([0-9.]+)", rec.get("entryPoint", "") + " " + (rec.get("function") or "") + " " + str(rec.get("value") or ""))
                            v = fnum(rec.get("value")) if rec.get("value") not in (None, "") else (fnum(m.group(1)) if m else None)
                        if v is None:
                            m = re.search(r"([+-]?[0-9.]+)", o.get("text") or ""); v = fnum(m.group(1)) if m else None
                        x["v"] = r2(v)
                    outs.append(x)
                # max consecutive hits for Furious / Pounder's
                for o in blk.get("outputs", []):
                    if ((o.get("records") or {}).get("entryPoint") or "") == "Mod Max Consecutive Hits Allowed":
                        for x in outs:
                            if x["k"] == "stack": x["max"] = 1 + (fnum((o.get("records") or {}).get("value"), 0) or 0)
                if outs or later:
                    lst.append({"n": it["name"], "headline": it.get("headline") or "", "melee": "Melee weapons" in ap,
                                "outs": outs, "later": sorted(set(later))})
        lst.sort(key=lambda x: x["n"])
        leg[str(s)] = lst

    # ---- food, drinks, chems, magazines, bobbleheads -----------------------------------
    fd = jload("calculators", "food_buff_calculator.json")
    never = set((fd.get("rules") or {}).get("never_recommend") or [])
    slot_av = {"dmg_all": "STAT_DmgAll", "dmg_melee": "STAT_DmgMelee", "dmg_melee_weapons": "STAT_DmgMeleeWeapons",
               "dmg_unarmed": "STAT_DmgMeleeUnarmed", "str": "Strength", "sneak_attack": "STAT_SneakAttackBonus"}
    mgef_text = {r["EDID"].strip(): ((r.get("FULL") or "") + " " + (r.get("DNAM_MagicItemDescription") or "")).lower()
                 for r in rows(src["mgef"])}
    wnames = sorted({(w.get("name") or "") for w in bw if w.get("name")}, key=len, reverse=True)
    items = []
    for it in fd["items"]:
        fx = []
        for f in it["fx"]:
            if f.get("neg") or f.get("w"): continue
            s = f.get("s")
            if s not in slot_av: continue
            av = mgef_av.get(f.get("e") or "", "") or slot_av[s]
            if s != "str" and av not in reach and av != "STAT_SneakAttackBonus":
                continue          # e.g. a buff for one named weapon only (Gulper Smacker)
            if s != "str" and av in reach and reach[av].get("enemy"): continue
            only = [n for n in wnames if n.lower() in mgef_text.get(f.get("e") or "", "")][:1]
            x = {"s": s, "av": av if s != "str" else "Strength", "v": f.get("m"), "dm": f.get("dm"), "pm": f.get("pm")}
            if only and s != "str": x["only"] = only[0]      # e.g. Mire Magic Moonshine: Gulper Smacker only
            fx.append(x)
        if not fx: continue
        menu = it.get("menu") if isinstance(it.get("menu"), dict) else {}
        items.append({"n": it["n"], "t": it["t"], "fx": fx, "bnb": menu.get("px") is not None, "never": it["n"] in never})
    items.sort(key=lambda x: (x["t"], x["n"]))

    crit = jload("calculators", "vats_crit_calculator.json").get("crit") or {}
    crit = {k: crit.get(k) for k in ("reach", "buffs", "betterCrits", "vital", "vitalName", "unarmedBobble")}
    cw = {w["e"]: w for w in jload("calculators", "vats_crit_calculator.json")["crit"].get("weapons", [])}
    for w in weapons:
        c = cw.get(w["e"])
        w["critMods"] = [m for m in (c or {}).get("mods", [])]

    out = {"version": 1, "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
           "channel": "pts" if PTS else "live",
           "page": {"path": "/bnb/calculators/melee-damage-calculator/", "title": "Melee Damage Calculator"},
           "weapons": weapons, "reach": reach, "enemies": enemy_av, "strength": strength, "cards": cards,
           "sneakPer": sneak_per, "mutations": mutations, "legendary": leg, "items": items,
           "partyBoy": (fd.get("mechanics") or {}).get("partyGirl"), "crit": crit,
           "laterAvs": sorted(SERVER_SIDE_AVS),
           "sources": {k: os.path.basename(v) for k, v in src.items()}, "warnings": WARN}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[melee_dmg] wrote {OUT}: {len(weapons)} weapons, {sum(len(v) for v in leg.values())} legendaries, "
          f"{len(items)} items, warnings {len(WARN)}")


if __name__ == "__main__":
    main()
