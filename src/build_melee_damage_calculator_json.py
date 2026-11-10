"""Melee Damage + Ranged Damage Calculator data (Kat 7 Nov 2026, ranged 8 Nov).

/bnb/calculators/melee-damage-calculator/   ->  dist/calculators/melee_damage_calculator.json
/bnb/calculators/ranged-damage-calculator/  ->  --ranged: dist/calculators/ranged_damage_calculator.json
                                                (--pts -> dist/pts/calculators/...)

Every number comes from the game files or data built from them (nothing typed in):
  weapons     dist/builds/builds_data.json (player list by class) + WEAP Base / DNAM:
              damage curve CVT0 by weapon level (the curve wins over DNAM_BaseDamage), EILV levels,
              DAMA extra damage types (energy, fire...), CRDT crit mult, keywords.
              builds_data sometimes points at an enemy's copy (boss Railway Rifle / Tesla Rifle):
              the player's own WEAP is used; weapons with only an enemy copy (Splinter, Chaos Engine,
              Tempest, Slasher Knife) and explosion-only weapons go to "laterWeapons".
              The PTS WEAP export has no DAMA column yet -> live damage types are borrowed.
  mods        OMOD (+ included PARENT templates) on the weapon's ma_ keyword:
              AttackDamage MUL+ADD (physical x (1 + v1)), DamageBonusMult ADD (adds to the bonus),
              DamageTypeValues (MUL+ADD = % of base as that type, ADD = flat or its level curve),
              Enchantments -> ENCH Damage effects (amount or curve, over the effect's duration).
  bonus pile  PERKEP STAT_DamagePerk / STAT_DamageVsPerk "Mod Weapon DMG Bonus Mult" rows ->
              which actor value reaches which weapon / enemy (STAT_DmgScoped = aiming only).
  Strength    (melee) GMST fAVDMeleeDamageMult / fDamageStrengthMult, additive.
  cards       AbPerkAdrenaline, AbPerkNerdRage, AbPerkIronFist (melee), AbPerkHeavyGunner (Bullet Storm,
              HeavyGunnerDamageBonus 1/2/3 by AmmoSpenderStacks; +10 stacks with HeavyGunnerMaster01),
              ghoul: GHL_AbPerkRadiationPower (Glow, not power attacks), GHL_AbScienceMonsterSpell
              (10 s after being hit, Rads), GHL_AbPerkBloodyVats (Glowing Criticals, in V.A.T.S.),
              GHL_AbPerkMadScientist (energy damage, ranged).
  mutations   Twisted Muscles / Talons (melee), Adrenal Reaction, ghoul Feral meter (GHL_SURV_Feral_Ability,
              melee damage at the top tier).
  legendary   dist/legendary_star_pages/{1..5}-star.json. Furious / Pounder's: 0.01 x the actor value
              their enchantment names (AVIF NAM0 default, tsv/STATREC_Export_*.tsv): 5% / 10% a hit.
  crit        dist/calculators/vats_crit_calculator.json "crit" + each weapon's crit mods.
  food etc.   dist/calculators/food_buff_calculator.json items whose effect reaches these weapons.

Left OFF the pages (server-side, no PC code reads them): power attacks, vs crippled / bleeding /
burning / poisoned, per crippled limb, execute, torso, far / close, per AP, Science! energy mult,
Explosive payload; sneak attacks (Kat 8 Nov: hidden until the devs answer). Those cards are listed
in "laterCards".
"""
from __future__ import annotations
import csv, datetime, glob, json, os, re, sys

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV = os.path.join(ROOT, "tsv")
PTS = "--pts" in sys.argv
# Kat 8 Nov 2026: the same builder makes the Ranged Damage Calculator data (--ranged).
KIND = "ranged" if "--ranged" in sys.argv else "melee"
KIND_CLASSES = {"melee": ("melee", "unarmed"), "ranged": ("rifle", "pistol", "shotgun", "heavy", "bow")}   # explosives later (Kat)
DIST = os.path.join(ROOT, "dist", "pts") if PTS else os.path.join(ROOT, "dist")
OUT = os.path.join(DIST, "calculators", f"{KIND}_damage_calculator.json")
WARN = []
# Effects whose conditions can never pass (checked in SeventySix.esm, 7 Nov 2026): the Mire Magic Moonshine
# melee damage effect needs HasActiveMagicEffect(E08A_GulperSmacker_GulpershineBuff_ME), and the only record
# that gives that effect is the retired enchantment zzzE08A_Weapon_Gulpershine_Damage, which nothing uses.
NEVER_ACTIVE_EFFECTS = {"E08A_Gulpershine_GulperSmackerBuff_ME"}
# Read only through default objects (DFOB) or damage-type records; no PC code uses them (exe checked 7-8 Nov),
# so how they stack is server-side -> left off until the devs answer (Ask the Devs #30, #31, #32).
SERVER_SIDE_AVS = {"STAT_DmgPowerAttack", "STAT_DmgVsCrippled", "STAT_DmgVsBleeding", "STAT_DmgPerCrippled",
                   "LGND_ExecuteDmg", "STAT_DmgVsBurning", "STAT_DmgVsPoisoned",
                   "STAT_DmgVsTorso", "STAT_DmgVsFar", "STAT_DmgVsClose", "STAT_DmgAP", "STAT_DmgMultEnergy",
                   "LGND_ExplosivePayload",
                   "STAT_SneakAttackBonus"}   # Kat 8 Nov: sneak off both pages until the devs answer
# perk cards whose damage is one of those (shown by name only, "not on the calculator yet")
# cards that only add a server-side bonus (target hit / crippled / far / close / weak spot / sneak)
LATER_CARDS = {"melee": ["Slugger", "Wound Salter", "Heavy Hitter", "Ninja", "Faulty Spots"],
               "ranged": ["Center Masochist", "Down Ranger", "Guerrilla", "Gunslinger", "Number Cruncher", "Easy Target",
                          "Tormentor", "Science!", "Covert Operative", "Faulty Spots"]}


def warn(m):
    WARN.append(m); print(f"[{KIND}_dmg] WARN", m, file=sys.stderr)


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
        "ench": "ENCH_Export_*[0-9].tsv", "mgef": "MGEF_Export_*[0-9].tsv",
        "statrec": "STATREC_Export_*.tsv"}.items()}

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
    # actor value defaults (AVIF NAM0 / NAM6) - tools/esm_stat_records_extract.py
    av_def, av_max = {}, {}
    for r in rows(src["statrec"]):
        if r.get("Signature") == "AVIF" and r.get("Field") == "NAM0": av_def[r["EDID"].strip()] = fnum(r.get("Value"))
        if r.get("Signature") == "AVIF" and r.get("Field") == "NAM6": av_max[r["EDID"].strip()] = fnum(r.get("Value"))

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
            aim = bool(re.search(r"GetInIronSights", cond)) and not enemy
            other = bool(re.search(r"IsTrueForConditionForm", cond)) and not enemy and av.group(1) != "STAT_DmgHeavyGuns"
            e = {"any": [] if aim else sorted(set(anyk)), "none": sorted(set(nonek)), "per": fnum(r.get("Value")),
                 "enemy": enemy, "skip": other}
            if aim: e["aim"] = True          # STAT_DmgScoped: only while aiming down the sights (never V.A.T.S.)
            if av.group(1) == "STAT_DmgHeavyGuns": e["any"] = sorted(set(anyk) | {"WeaponTypeHeavyGun"})
            reach[av.group(1)] = e
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
    later_weapons, seen_e = [], set()
    bw = jload("builds", "builds_data.json")["weapons"]
    dn = {r["WEAP_EDID"]: r for r in rows(src["dnam"])}
    if PTS and dn and "DAMA_DamageTypes" not in next(iter(dn.values())):
        # the PTS WEAP export has no DAMA column yet - borrow the live damage types (energy, fire...)
        sys.path.insert(0, os.path.join(ROOT, "src")); import tsv_source
        live = {r["WEAP_EDID"]: r.get("DAMA_DamageTypes") or "" for r in rows(tsv_source.newest("WEAP_Export_*_DNAM.tsv", channel="live"))}
        for k, r in dn.items(): r["DAMA_DamageTypes"] = live.get(k, "")
        warn("PTS WEAP export has no DAMA_DamageTypes - live damage types used")
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
                ct = (r.get("CurveTable") or "").strip()
                cv = need(ct) if ct and not ct.startswith("NULL") else []
                if fn == "MUL+ADD" and v2: split.append({"t": t, "pct": r2(v2)})
                elif fn == "ADD" and cv: flat.append({"t": t, "curve": cv})       # e.g. Severe receivers: fire by level
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
        def uniq(l):
            seen, out = set(), []
            for x in l:
                k = json.dumps(x, sort_keys=True)
                if k not in seen: seen.add(k); out.append(x)
            return out
        return {"mul": r2(mul), "pile": r2(pile), "split": uniq(split), "flat": uniq(flat), "dot": uniq(dot)}

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
        cls = [c for c in (w.get("classes") or []) if c in KIND_CLASSES[KIND]]
        if w.get("name") == "Tesla": w = dict(w, name="Tesla Rifle")   # DLC01LightningGun, listed twice in builds_data
        if not cls: continue
        e = w.get("edid"); d = dn.get(e); b = base.get(e)
        if not d or not b: warn(f"weapon {e} not in WEAP export"); continue
        if "CT_Creatures" in (d.get("CVT0_DamageCurve") or "") or re.match(r"(P62_cr|SDOW_|HTO_|cr[A-Z])", e):
            # builds_data points at an enemy's copy (boss Railway Rifle etc.) - find the player's own WEAP
            tail = re.sub(r"^.*?(_Boss_|_cr[A-Z][a-z]+(?=[A-Z]))", "", e)
            tail = re.sub(r"^(SDOW_|HTO_|P62_)?cr", "", tail)
            alt = next((x for x in (tail, "DLC01" + tail) if x in dn and "CT_Creatures" not in (dn[x].get("CVT0_DamageCurve") or "")), None)
            if not alt:
                later_weapons.append(w.get("name")); warn(f"weapon {w.get('name')}: only an enemy copy ({e}) in the WEAP export")
                continue
            e = alt; d = dn[e]; b = base[e]
        if e in seen_e: continue
        seen_e.add(e)
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
        if "WeaponTypeExplosive" in tags and not cv and not extra or (not cv and not extra and flat <= 0):
            later_weapons.append(w.get("name")); continue      # explosion / spell damage - Kat 8 Nov: explosives later
        wcls = ("unarmed" if "WeaponTypeUnarmed" in tags else "melee") if KIND == "melee" else cls[0]
        weapons.append({"n": w.get("name") or b.get("WEAP_FULL"), "e": e, "cls": wcls, "tags": tags,
                        "curve": cv, "base": flat, "levels": lv, "extra": extra,
                        "crit": fnum(d.get("CRDT_CritDamageMult")), "charge": fnum(d.get("CRDT_CritChargeBonus"), 0),
                        "sneak": fnum(d.get("WSAM_SneakAttackMult")),
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

    def ranks_of(edid, mgef):
        """per-rank magnitudes (rows are in rank order: rank 1 first)"""
        return [x["v"] for x in spel_fx(edid, mgef)]

    def pk_name(e, d):
        try:
            for c in pcards:
                if any(rk.get("perkEdid") == e for rk in c.get("ranks") or []): return c["name"], c.get("special")
        except Exception:  # noqa: BLE001
            pass
        return d, None
    pc = jload("perk_cards.json"); pcards = pc.get("cards") if isinstance(pc, dict) else pc
    if isinstance(pcards, dict): pcards = list(pcards.values())

    def card(cid, perk, d, **kw):
        n, sp = pk_name(perk, d)
        return dict({"id": cid, "n": n, "special": sp}, **kw)
    # every card here adds into the damage bonus (abPerkFortifyDmgAll -> STAT_DmgAll, etc.)
    common = [
        card("adren", "Adrenaline01", "Adrenaline", av="STAT_DmgAll", curve=curve_fx("AbPerkAdrenaline", "abPerkFortifyDmgAll"), input="kills"),
        card("nerd", "NerdRage01", "Nerd Rage!", av="STAT_DmgAll", curve=curve_fx("AbPerkNerdRage", "abPerkFortifyDmgAll"), input="hp"),
        card("radpow", "GHL_RadiationPower01", "Radiation Power", av=mgef_av.get("GHL_AbPerkRadioactiveFortifyDmgAllEffect") or "STAT_DmgAll",
             ranks=ranks_of("GHL_AbPerkRadiationPower", "GHL_AbPerkRadioactiveFortifyDmgAllEffect"), ghoul=True, glow=True),
        card("scimon", "GHL_ScienceMonster01", "Science Monster", av=mgef_av.get("GHL_abPerkScienceMonsterFortifyDmgAll") or "STAT_DmgAll",
             ranks=ranks_of("GHL_AbScienceMonsterSpell", "GHL_abPerkScienceMonsterFortifyDmgAll"), ghoul=True, hit=True),
        # Glowing Criticals: GHL_AbPerkBloodyVats, ghoul + GetIsInVATS
        card("glowcrit", "GHL_GlowingCriticals01", "Glowing Criticals", av=mgef_av.get("abPerkFortifyDmgAll") or "STAT_DmgAll",
             ranks=ranks_of("GHL_AbPerkBloodyVats", "abPerkFortifyDmgAll"), ghoul=True, vats=True),
    ]
    # Radiation Power's spell turns off while power attacking (IsPowerAttacking = 0)
    common[2]["notPower"] = any("IsPowerAttacking" in x["cond"] for x in spel_fx("GHL_AbPerkRadiationPower", None))
    if KIND == "melee":
        cards = common + [card("iron", "IronFist01", "Iron Fist", av="STAT_DmgMeleeUnarmed",
                                curve=curve_fx("AbPerkIronFist", "AbPerkFortifyDmgMeleeUnarmed"), input="dr")]
    else:
        hg = [x for x in spel_fx("AbPerkHeavyGunner", "abPerkFortifyDmgAll")]
        stk = spel_fx("AbPerkHeavyGunner", "abAmmoSpenderFortifyStacks")
        base_st = next((x["v"] for x in stk if not x["cond"]), 10)          # 10 stacks
        big_st = next((x["v"] for x in stk if "HeavyGunnerMaster01" in x["cond"]), 0)   # +10 with Bringing the Big Guns
        cards = common + [
            card("bstorm", "HeavyGunner01", "Bullet Storm", av="STAT_DmgAll", rankCurves=[need(x["curve"]) for x in hg],
                 input="stacks", maxStacks=base_st, bigGunsStacks=big_st,
                 bigGunsName=pk_name("HeavyGunnerMaster01", "Bringing the Big Guns")[0], cls=["heavy"]),
            card("madsci", "GHL_MadScientist01", "Mad Scientist", av=mgef_av.get("AbPerkFortifyDmgEnergy") or "STAT_DmgEnergy",
                 ranks=ranks_of("GHL_AbPerkMadScientist", "AbPerkFortifyDmgEnergy"), ghoul=True)]
    # where a spell lookup failed, look the radiation power spell up by its effect
    for c in cards:
        if c["id"] == "radpow" and not c.get("ranks"):
            for k, v in spel.items():
                vals = [fnum(r.get("EFIT_Magnitude"), 0) for r in v if (r.get("EFID_MGEF_EDID") or "") == "GHL_AbPerkRadioactiveFortifyDmgAllEffect"]
                if vals: c["ranks"] = vals; break
            if not c.get("ranks"): warn("Radiation Power ranks not found")
    later_cards = LATER_CARDS[KIND]
    sneak_per = None   # sneak hidden (Kat 8 Nov) - kept in the JSON for later
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
    mutations = {"adrenal": adr}
    # ghoul Feral meter: GHL_SURV_Feral_Ability gives melee damage at the top tier
    feral = [x for x in spel_fx("GHL_SURV_Feral_Ability", None) if mgef_av.get(x["mgef"]) in ("STAT_DmgMelee", "STAT_DmgAll")]
    if KIND == "melee" and feral:
        mutations["feral"] = {"v": feral[0]["v"], "av": mgef_av.get(feral[0]["mgef"]),
                              "tier": (re.search(r"FeralThreshold_\d_(\w+) \[GLOB", feral[0]["cond"]) or [None, ""])[1]}
    if KIND == "melee":
        mutations["twisted"] = {**mut("Mutation_TwistedMuscles", "Mutation_TwistedMusclesEffect"), "av": mgef_av.get("Mutation_TwistedMusclesEffect")}
        mutations["talons"] = {**mut("Mutation_Talons", "Mutation_TalonsEffect"), "av": mgef_av.get("Mutation_TalonsEffect")}

    # ---- legendary effects ---------------------------------------------------------------
    def out_kind(o):
        rec = o.get("records") or {}
        av = re.sub(r"\s*\[AVIF.*", "", rec.get("actorValue") or "").strip()
        epn = (rec.get("entryPoint") or "").strip()
        if av in SERVER_SIDE_AVS: return "later", av
        if av == "Strength": return "str", av
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
                mine = "Melee weapons" if KIND == "melee" else "Ranged weapons"
                if not any(a in ("Weapons", mine) for a in ap): continue
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
                        # 0.01 x the actor value the legendary's enchantment names (its default: Furious 5, Pounder's 10)
                        avn = next((((o2.get("records") or {}).get("curveInput") or "") for o2 in blk.get("outputs", [])
                                    if ((o2.get("records") or {}).get("curveInput") or "") in av_def), "")
                        x["per"] = r2((fnum(rec.get("value"), 0) or 0) * (av_def.get(avn) or 1) * 100)
                        x["cap"] = av_max.get(avn)
                        if not avn: warn(f"{it['name']}: consecutive-hit actor value default not found (STATREC)")
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
                            if x["k"] == "stack": x["max"] = fnum((o.get("records") or {}).get("value"), 0) or 0
                if outs or later:
                    lst.append({"n": it["name"], "headline": it.get("headline") or "", "only": mine in ap,
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
    def reaches(av, tags):
        r = reach.get(av) or {}
        if r.get("any") and not (set(r["any"]) & tags): return False
        return not (set(r.get("none") or []) & tags)
    items = []
    for it in fd["items"]:
        fx = []
        for f in it["fx"]:
            if f.get("neg") or f.get("w"): continue
            s = f.get("s")
            if (f.get("e") or "") in NEVER_ACTIVE_EFFECTS: continue
            av = mgef_av.get(f.get("e") or "", "") or slot_av.get(s, "")
            if s == "str" or av == "Strength":
                if KIND != "melee": continue
                s, av = "str", "Strength"
            elif av not in reach or reach[av].get("enemy") or reach[av].get("skip") or av in SERVER_SIDE_AVS:
                continue          # not a damage bonus for these weapons (or worked out on the servers)
            if s != "str" and not any(reaches(av, set(wp["tags"])) for wp in weapons): continue
            only = [n for n in wnames if n.lower() in mgef_text.get(f.get("e") or "", "")][:1]
            x = {"s": s, "av": av if s != "str" else "Strength", "v": f.get("m"), "dm": f.get("dm"), "pm": f.get("pm")}
            if only and s != "str": x["only"] = only[0]      # e.g. Mire Magic Moonshine: Gulper Smacker only
            fx.append(x)
        if not fx: continue
        menu = it.get("menu") if isinstance(it.get("menu"), dict) else {}
        items.append({"n": it["n"], "t": it["t"], "fx": fx, "bnb": menu.get("px") is not None, "never": it["n"] in never})
    items.sort(key=lambda x: (x["t"], x["n"]))

    crit = jload("calculators", "vats_crit_calculator.json").get("crit") or {}
    crit = {k: crit.get(k) for k in ("reach", "buffs", "betterCrits", "glowingCrits", "vital", "vitalName", "unarmedBobble")}
    cw = {w["e"]: w for w in jload("calculators", "vats_crit_calculator.json")["crit"].get("weapons", [])}
    for w in weapons:
        c = cw.get(w["e"])
        w["critMods"] = [m for m in (c or {}).get("mods", [])]

    crit["eagleEyes"] = (jload("calculators", "vats_crit_calculator.json").get("crit") or {}).get("eagleEyes")
    out = {"version": 1, "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
           "channel": "pts" if PTS else "live", "kind": KIND, "laterCards": later_cards, "laterWeapons": sorted(set(later_weapons)),
           "page": {"path": f"/bnb/calculators/{KIND}-damage-calculator/", "title": f"{KIND.title()} Damage Calculator"},
           "weapons": weapons, "reach": reach, "enemies": enemy_av, "strength": strength, "cards": cards,
           "sneakPer": sneak_per, "mutations": mutations, "legendary": leg, "items": items,
           "partyBoy": (fd.get("mechanics") or {}).get("partyGirl"), "crit": crit,
           "laterAvs": sorted(SERVER_SIDE_AVS),
           "sources": {k: os.path.basename(v) for k, v in src.items()}, "warnings": WARN}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[{KIND}_dmg] wrote {OUT}: {len(weapons)} weapons, {sum(len(v) for v in leg.values())} legendaries, "
          f"{len(items)} items, warnings {len(WARN)}")


if __name__ == "__main__":
    main()
