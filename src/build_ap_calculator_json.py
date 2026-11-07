"""AP Calculator data — AP Refresh | Max AP tabs (Kat 6 Nov 2026).

/bnb/calculators/ap-refresh-calculator/  ->  dist/calculators/ap_calculator.json
(df-bnb-ap-calculator.js reads it; loaded by the router in df-bnb-bnb-calculators.js).

AP refresh (% of your AP bar per second) = ActionPointsRate × ActionPointsRateMult ÷ 100.
Every boost is sorted into one of the two by its magic effect's actor value (MGEF export):
  ActionPointsRate      (flat, base 6 from HumanRace PRPS)  food, magazines, alcohol, Injector,
                        Powered legendary (OMOD ADD ActionPointsRate)
  ActionPointsRateMult  (%, base 100 from AVIF NAM0)        Action Boy / Girl, Action Ghoul,
                        Lone Wanderer, Nerd Rage!, Packin' Light, Well Tuned, thirst bar
Kat's AP sheets (Thaang / mario_will_die) split it the same way; the numbers here come from the files.

Sources (newest of each): dist/specials_stats.json (ap, ap_regen, agi, hunger.feral), SPEL effects
(AbPerkActionBoyGirl, GHL_AbPerkActionGhoul, AbPerkPackinLight, SURV_WellTunedSpell, PerkInjector01Spell),
MGEF (actor values), ALCHFX + CURV (Hard Lemonade Brewing_HardLemonade_APRegen / _FortifyAP),
OMOD properties (mod_Legendary_Armor_APRegen / PowerArmor2).
Power armour (tsv/APX_Export_* from tools/esm_ap_extract.py, read straight from SeventySix.esm):
  Kinetic Servos   OMOD _PARENT_mod_PowerArmor_GENERIC_APRegen adds PA_KineticServos_AV +1 per leg;
                   ENCH EnchPowerArmor_KineticServos takes its magnitude from that (MAGA) -> flat, while moving
  Core Assembly    OMOD _PARENT_mod_PowerArmor_GENERIC_BatRegen ActionPointsRate on CT_PA_Mod_CoreAssembly
                   (curve JSON from data/curvetables/json; the xEdit CURV export leaves its path blank) -> flat
  Enclave Vulcan   ENCH RD01_PowerArmor_Torso_EnclaveVulcanEnch -> % (ActionPointsRateMult)
AP Refills tab (Kat 7 Nov 2026) - things that put AP straight back, on top of the % refresh:
  Radioactive-Powered  APX PERSEC rows on ENCH ench_LegendaryPowerArmor_RadioactivePowered: +2 AP / -2 rads a
                       second per piece, while rads > 0 and the AP bar isn't full
  Rushing              ENCH BOUNTY_ench_LegendaryArmor_Rushing: BOUNTY_Rushing_RestoreActionPointsEffect, magnitude
                       GLOB BOUNTY_KillstreakArmorBonus01..10 (one per Kill Streak 1..10), per piece
  Rejuvenator's        SPEL SURV_Thirst_Ability abRestoreActionPoints rows: curve CT_Legendary_Armor_RejuvenatorT1..T5
                       with input LGND_RejuvenatorTier (+1 per piece, OMOD), by thirst tier / Rejuvenated; humans only
  food / drinks        dist/calculators/food_buff_calculator.json effects on slot ap_refill (total AP, seconds)
  in one go            ALCHFX RestoreActionPoints* with no duration (Nuka-Cola Quantum, Halloween Candy)
  Grim Reaper's Sprint PERKEP GrimReapersSprint01 Mod VATS Player AP On Kill Chance, CURV GrimReapersSprintBonus
                       (input Luck) = chance a V.A.T.S. kill fills the whole AP bar

Usage:  python src/build_ap_calculator_json.py [--pts]
"""
from __future__ import annotations
import csv, datetime, json, os, re, sys

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import tsv_source  # noqa: E402

CHANNEL = tsv_source.channel_of()
DIST = os.path.join(ROOT, "dist", "pts") if CHANNEL == "pts" else os.path.join(ROOT, "dist")
OUT = os.path.join(DIST, "calculators", "ap_calculator.json")
warnings: list[str] = []


def tidy(v):
    return int(v) if isinstance(v, float) and v.is_integer() else v


def read(pattern):
    path = tsv_source.newest(pattern, channel=CHANNEL, required=CHANNEL == "live")
    if path is None:  # PTS has no export of this type yet: use live, say so
        path = tsv_source.newest(pattern, channel="live")
        warnings.append(f"no PTS {pattern} - used live {os.path.basename(path)}")
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t")), os.path.basename(path)


def boost(stat, bid):
    for b in stat["boost"]:
        if b["id"] == bid:
            return b["rows"]
    warnings.append(f"{stat['key']}: no '{bid}' boost")
    return []


def main():
    ss_path = os.path.join(DIST, "specials_stats.json")
    if not os.path.exists(ss_path):
        ss_path = os.path.join(ROOT, "dist", "specials_stats.json")
    ss = json.load(open(ss_path, encoding="utf-8"))
    ap, regen, agi = ss["stats"]["ap"], ss["stats"]["ap_regen"], ss["stats"]["agi"]

    mgef_rows, mgef_file = read("MGEF_Export_*.tsv")
    av_of = {r["EDID"]: (r.get("ActorValue") or "") for r in mgef_rows}
    spel_rows, spel_file = read("SPEL_Export_*_EFFECTS.tsv")
    alch_rows, alch_file = read("ALCHFX_Export_*.tsv")
    curv_rows, curv_file = read("CURV_Export_*_POINTS.tsv")
    omod_rows, omod_file = read("OMOD_Export_*_Properties.tsv")

    def spel(edid):
        return [r for r in spel_rows if r.get("SPEL_EDID") == edid]

    def curve(edid):
        pts = sorted([float(r["X"]), float(r["Y"])] for r in curv_rows if r.get("EDID") == edid)
        return [[tidy(x), tidy(y)] for x, y in pts]

    def kind_of(mgef):
        av = av_of.get(mgef, "")
        if "ActionPointsRateMult" in av:
            return "mult"
        if "ActionPointsRate" in av:
            return "flat"
        return None

    def ranked(edid, rank_re):
        out = []
        for r in spel(edid):
            m = re.findall(rank_re, r["Conditions"])
            vals = [int(x) for x in m]
            rank = max(vals) if vals else 1
            out.append({"rank": rank, "value": tidy(float(r["EFIT_Magnitude"])), "kind": kind_of(r["EFID_MGEF_EDID"])})
        # one value per rank (highest rank named in the conditions = 1)
        best = {}
        for o in out:
            best.setdefault(o["rank"], o)
        return [best[k] for k in sorted(best)]

    action_boy = ranked("AbPerkActionBoyGirl", r"HasPerk\(Action(?:Boy|Girl)0(\d)[^)]*\) = 1")
    action_ghoul = ranked("GHL_AbPerkActionGhoul", r"HasPerk\(GHL_ActionGhoul0(\d)[^)]*\) = 1")
    # Action Boy rank 1 row has no "= 1" rank condition -> rank 1; make sure ranks are 1..n in order
    for lst in (action_boy, action_ghoul):
        lst.sort(key=lambda x: x["value"])
        for i, x in enumerate(lst):
            x["rank"] = i + 1

    def single(edid, mgef=None):
        for r in spel(edid):
            if mgef is None or r["EFID_MGEF_EDID"] == mgef:
                return {"value": tidy(float(r["EFIT_Magnitude"])), "kind": kind_of(r["EFID_MGEF_EDID"]), "src": {"type": "SPEL", "edid": edid}}
        warnings.append(f"SPEL {edid} not found")
        return None

    packin = single("AbPerkPackinLight")
    well_tuned = single("SURV_WellTunedSpell", "SURV_WellTunedEffect")
    injector = single("PerkInjector01Spell")

    curves = {c["edid"]: c for c in regen.get("curves", [])}
    lone = curves.get("LoneWandererAPRegenBonus")
    nerd = curves.get("NerdRageAPBonus")

    powered = None
    for r in omod_rows:
        if r.get("OMOD_EDID") == "mod_Legendary_Armor_APRegen" and "ActionPointsRate" in (r.get("Value1") or ""):
            powered = {"value": tidy(float(r["Value2"])), "kind": "flat", "star": 2, "src": {"type": "OMOD", "edid": r["OMOD_EDID"]}}
    if not powered:
        warnings.append("Powered OMOD not found")

    # ---- Power armour (ESM extract) ----------------------------------------
    apx_rows, apx_file = read("APX_Export_*.tsv")
    def apx(edid):
        return [r for r in apx_rows if r.get("EDID") == edid]
    def omod_av(omod, av_word):
        for r in omod_rows:
            if r.get("OMOD_EDID") == omod and av_word in (r.get("Value1") or ""):
                return r
        return None
    servos = None
    ks = apx("EnchPowerArmor_KineticServos")
    ks_av = omod_av("_PARENT_mod_PowerArmor_GENERIC_APRegen", "PA_KineticServos_AV")
    if ks and ks[0]["Curve_EDID"] == "PA_KineticServos_AV" and ks_av:
        servos = {"value": tidy(float(ks_av["Value2"])), "kind": "flat" if ks[0]["Field"] == "FLAT" else "mult", "perLeg": True,
                  "whileMoving": True, "src": {"type": "OMOD+ENCH", "edid": "_PARENT_mod_PowerArmor_GENERIC_APRegen / EnchPowerArmor_KineticServos"}}
    else:
        warnings.append("Kinetic Servos not found (APX / OMOD)")
    core = None
    ca = omod_av("_PARENT_mod_PowerArmor_GENERIC_BatRegen", "ActionPointsRate")
    crve = next((r["Value"] for r in apx("CT_PA_Mod_CoreAssembly")), None)
    if ca and crve:
        jp = os.path.join(ROOT, "data", "curvetables", "json", *crve.replace("\\", "/").lower().split("/"))
        try:
            cj = json.load(open(jp, encoding="utf-8"))
            pts = sorted((float(p["x"]), float(p["y"])) for p in cj["curve"])
            core = {"value": tidy(pts[-1][1]), "points": [[tidy(x), tidy(y)] for x, y in pts], "kind": "flat",
                    "src": {"type": "OMOD+CURV", "edid": "_PARENT_mod_PowerArmor_GENERIC_BatRegen / CT_PA_Mod_CoreAssembly"}}
        except (OSError, KeyError, ValueError) as e:
            warnings.append(f"Core Assembly curve {crve}: {e}")
    else:
        warnings.append("Core Assembly not found (OMOD / APX CRVE)")
    vulcan = None
    vu = apx("RD01_PowerArmor_Torso_EnclaveVulcanEnch")
    if vu:
        vulcan = {"value": tidy(float(vu[0]["Value"])), "kind": "flat" if vu[0]["Field"] == "FLAT" else "mult",
                  "src": {"type": "ENCH", "edid": "RD01_PowerArmor_Torso_EnclaveVulcanEnch"}}
    else:
        warnings.append("Enclave Vulcan torso not found (APX)")

    # ---- AP Refills tab -----------------------------------------------------
    refills = {}
    rp = {r["Field"]: r for r in apx("ench_LegendaryPowerArmor_RadioactivePowered")}
    if "PERSEC_AP" in rp:
        refills["radioactivePowered"] = {"apPerSec": tidy(float(rp["PERSEC_AP"]["Value"])),
                                         "radsPerSec": tidy(float(rp["PERSEC_RADS"]["Value"])) if "PERSEC_RADS" in rp else None,
                                         "star": 4, "pa": True, "src": {"type": "ENCH", "edid": "ench_LegendaryPowerArmor_RadioactivePowered"}}
    else:
        warnings.append("Radioactive-Powered not found (APX PERSEC)")
    glob_rows, glob_file = read("GLOB_Export_*.tsv")
    gv = {r["EDID"]: float(r["FLTV"]) for r in glob_rows if r.get("FLTV") not in (None, "")}
    rush = [tidy(round(gv[k], 4)) for k in ("BOUNTY_KillstreakArmorBonus%02d" % i for i in range(1, 11)) if k in gv]
    if len(rush) == 10:
        refills["rushing"] = {"perStreak": rush, "star": 2, "src": {"type": "ENCH+GLOB", "edid": "BOUNTY_ench_LegendaryArmor_Rushing / BOUNTY_KillstreakArmorBonus01-10"}}
    else:
        warnings.append(f"Rushing kill streak GLOBs: found {len(rush)} of 10")
    rej = []
    for r in spel("SURV_Thirst_Ability"):
        if r["EFID_MGEF_EDID"] != "abRestoreActionPoints" or r.get("MAGA_AVIF_EDID") != "LGND_RejuvenatorTier":
            continue
        c = r["Conditions"]
        tier = re.search(r"SURV_Thirst \[AVIF:[0-9A-F]+\], 00 00 00 00\) >= SURV_NewThirstThreshold_\d_(\w+) ", c)
        r1 = re.search(r"HasPerk\(Rejuvenated01 [^\[]*\[PERK:[0-9A-F]+\][^)]*\) = (\d)", c)
        r2 = re.search(r"HasPerk\(Rejuvenated02 [^\[]*\[PERK:[0-9A-F]+\][^)]*\) = (\d)", c)
        if r1 and r2 and r1.group(1) == "1" and r2.group(1) == "1":
            rv = "r2_low_rads"
        elif r1 and r1.group(1) == "1":
            rv = "r1_or_r2_high_rads"
        elif r1 and r1.group(1) == "0":
            rv = "none"
        else:
            rv = "any"
        names = {"FullyHydrated": "Fully Hydrated", "WellHydrated": "Well Hydrated", "Hydrated": "Hydrated", "PartiallyHydrated": "Partially Hydrated"}
        pts = curve(r["CVTO_CURV_EDID"])
        if tier and pts:
            rej.append({"tier": names.get(tier.group(1), tier.group(1)), "rejuv": rv, "curve": r["CVTO_CURV_EDID"], "points": pts})
    if rej:
        refills["rejuvenators"] = {"tiers": rej, "star": 4, "pa": True, "humanOnly": True,
                                   "src": {"type": "SPEL", "edid": "SURV_Thirst_Ability (abRestoreActionPoints, MAGA LGND_RejuvenatorTier)"}}
    else:
        warnings.append("Rejuvenator's thirst rows not found")
    fb_path = os.path.join(DIST, "calculators", "food_buff_calculator.json")
    if not os.path.exists(fb_path):
        fb_path = os.path.join(ROOT, "dist", "calculators", "food_buff_calculator.json")
    foods_r, seen = [], set()
    for it in json.load(open(fb_path, encoding="utf-8")).get("items", []):
        for f in it.get("fx", []):
            if f.get("s") == "ap_refill" and f.get("m") and f.get("d") and it["n"] not in seen:
                seen.add(it["n"])
                foods_r.append({"name": it["n"], "type": it.get("t"), "total": tidy(f["m"]), "seconds": f["d"],
                                "perSec": tidy(round(f["m"] / f["d"], 3)), "edid": it.get("edid")})
    foods_r.sort(key=lambda x: (-x["total"], x["name"]))
    refills["foods"] = foods_r
    alch_names = {}
    for r in read("ALCH_Export_*.tsv")[0]:
        if r.get("ALCH_EDID"):
            alch_names[r["ALCH_EDID"]] = r.get("FULL") or ""
    instant = []
    for r in alch_rows:
        if r.get("MGEF_EDID", "").startswith("RestoreActionPoints") and float(r.get("Duration") or 0) == 0 and float(r.get("Magnitude") or 0) > 0:
            nm = alch_names.get(r["ALCH_EDID"]) or r["ALCH_EDID"]
            if not r["ALCH_EDID"].startswith(("zzz", "AAA", "DEL_", "Test")) and nm not in [x["name"] for x in instant]:
                instant.append({"name": nm, "ap": tidy(float(r["Magnitude"])), "edid": r["ALCH_EDID"]})
    refills["instant"] = sorted(instant, key=lambda x: -x["ap"])
    grs = curve("GrimReapersSprintBonus")
    if grs:
        refills["grimReaper"] = {"points": grs, "input": "Luck", "src": {"type": "PERK+CURV", "edid": "GrimReapersSprint01 / GrimReapersSprintBonus"}}
    else:
        warnings.append("GrimReapersSprintBonus curve missing")

    # Hard Lemonade: curve-valued effects
    hard = {}
    for r in alch_rows:
        if r.get("ALCH_EDID") == "Brew_LemonadeHard" and r.get("Curve_EDID"):
            pts = curve(r["Curve_EDID"])
            if pts:
                hard[kind_of(r["MGEF_EDID"]) or av_of.get(r["MGEF_EDID"], "")] = {"value": pts[-1][1], "durationSec": tidy(float(r["Duration"] or 0)), "mgef": r["MGEF_EDID"]}

    # ALCH item -> regen kind (flat / mult) and magic effect
    def item_effects(edid, want):
        for r in alch_rows:
            if r.get("ALCH_EDID") == edid and kind_of(r["MGEF_EDID"]) == want:
                return r["MGEF_EDID"]
        return None

    regen_foods = []
    for f in boost(regen, "food"):
        regen_foods.append({k: f.get(k) for k in ("diet", "dish", "id", "edid", "durationSec", "base", "mutation", "sin")}
                           | {"kind": "flat" if item_effects(f["edid"], "flat") else ("mult" if item_effects(f["edid"], "mult") else "flat")})
    def items(rows, word, want):
        out = []
        for r in rows:
            for e in r.get("effects", []):
                if word in e.get("text", "") and e.get("magnitude"):
                    k = kind_of(e.get("mgef") or "") or want
                    out.append({"name": r["name"], "id": r.get("id"), "value": tidy(e["magnitude"]), "mgef": e.get("mgef"),
                                "kind": k, "team": "When in a Team" in e["text"], "duration": e.get("duration")})
        return out
    regen_mags = items(boost(regen, "magazines"), "AP Regen", "flat")
    nukashine = items(boost(regen, "alcohol"), "APRegen", "flat")

    # thirst bar AP regen rows (human)
    thirst = []
    for r in boost(regen, "hunger-thirst"):
        texts = [" OR ".join(c["text"] for c in g) for g in r["conditions"]]
        tier = next((re.match(r"Thirst at or past (.+)$", t).group(1) for t in texts if t.startswith("Thirst at or past")), None)
        if not tier:
            continue
        has1 = "Has Rejuvenated rank 1" in texts
        has2 = "Has Rejuvenated rank 2" in texts
        rejuv = "r2_low_rads" if (has1 and has2) else ("r1_or_r2_high_rads" if any("Rads > 100" in t for t in texts) else ("none" if "Does not have Rejuvenated rank 1" in texts else "any"))
        thirst.append({"tier": tier, "rejuv": rejuv, "value": tidy(r["magnitude"]), "kind": "mult"})

    # Max AP
    fap = next(f for f in ap["formulas"] if f["id"] == "ap")
    ap_curves = {c["edid"]: c for c in ap.get("curves", [])}
    tq = ap_curves.get("ThirstQuencherBonus")
    active = next(({"name": r["name"], "star": r["star"], "value": tidy(float(re.search(r"(\d+)", r["effect"]).group(1)))}
                   for r in boost(ap, "legendary") if r["name"] == "Active"), None)
    builds = {}
    for r in boost(ap, "mods"):
        fits = " ".join(r.get("fits", []))
        part = "torso" if "Torso" in fits and "Limb" not in fits else "limb"
        builds.setdefault(r["name"], {})[part] = tidy(r["magnitude"])
    ap_foods = [{k: f.get(k) for k in ("diet", "dish", "id", "durationSec", "base", "mutation", "sin")} for f in boost(ap, "food")]
    ap_alcohol = items(boost(ap, "alcohol"), "Action Points", "flat")
    if hard.get("ActionPoints"):
        ap_alcohol.append({"name": "Hard Lemonade", "id": None, "value": hard["ActionPoints"]["value"], "mgef": hard["ActionPoints"]["mgef"], "kind": "max", "team": False, "duration": f"{int(hard['ActionPoints']['durationSec'] // 60)} min"})
    ap_mags = items(boost(ap, "magazines"), "Action Points", "max")

    feral_ap = []
    for f in ss.get("hunger", {}).get("feral", []):
        if f["effect"] in ("Reduce Action Points", "Fortify Action Points"):
            tiers = [c for g in f["conditions"] for c in g if c.get("op") == ">="]
            if tiers:
                feral_ap.append({"tier": tiers[0]["tier"], "value": tidy((-1 if f["effect"].startswith("Reduce") else 1) * f["magnitude"])})
    feral_tiers = []
    for f in ss.get("hunger", {}).get("feral", []):
        for g in f["conditions"]:
            for c in g:
                if c.get("op") == ">=" and c["tier"] not in [x["tier"] for x in feral_tiers]:
                    feral_tiers.append({"tier": c["tier"], "value": c.get("value") or 0})
    feral_tiers.sort(key=lambda x: x["value"])

    fregen = next(f for f in regen["formulas"] if f["id"] == "regen")
    out = {
        "version": 1,
        "generated": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        "channel": CHANNEL,
        "sources": {"specials_stats": os.path.relpath(ss_path, ROOT).replace("\\", "/"), "mgef": mgef_file, "spel": spel_file,
                    "alchfx": alch_file, "curv": curv_file, "omod": omod_file, "apx": apx_file, "glob": glob_file,
                    "foodBuff": os.path.relpath(fb_path, ROOT).replace("\\", "/")},
        "refresh": {
            "base": fregen["inputs"]["base"], "multBase": fregen["inputs"]["multBase"],
            "notes": regen.get("details", []),
            "actionBoy": action_boy, "actionGhoul": action_ghoul,
            "loneWanderer": {"points": lone["points"] if lone else [], "input": "Charisma", "kind": "mult", "src": {"type": "CURV", "edid": "LoneWandererAPRegenBonus"}},
            "nerdRage": {"points": nerd["points"] if nerd else [], "input": "Health (fraction of max HP)", "kind": "mult", "src": {"type": "CURV", "edid": "NerdRageAPBonus"}},
            "packinLight": packin, "wellTuned": well_tuned, "injector": injector, "powered": powered,
            "kineticServos": servos, "coreAssembly": core, "vulcanTorso": vulcan,
            "hardLemonade": hard.get("flat"),
            "nukashine": nukashine[0] if nukashine else None,
            "magazines": regen_mags,
            "foods": regen_foods,
            "thirst": thirst,
        },
        "maxAp": {
            "base": fap["inputs"]["base"], "perAgility": fap["inputs"]["mult"],
            "notes": ap.get("details", []),
            "thirstQuencher": {"points": tq["points"] if tq else [], "input": "Endurance", "src": {"type": "CURV", "edid": "ThirstQuencherBonus"}},
            "active": active,
            "builds": [{"name": k, **v} for k, v in builds.items()],
            "foods": ap_foods,
            "alcohol": ap_alcohol,
            "magazines": ap_mags,
            "feral": feral_ap,
        },
        "refills": refills,
        "feralTiers": [x["tier"] for x in feral_tiers],
        "warnings": warnings,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    r, m = out["refresh"], out["maxAp"]
    print(f"wrote {os.path.relpath(OUT, ROOT)}: actionBoy {[x['value'] for x in action_boy]}, actionGhoul {[x['value'] for x in action_ghoul]}, "
          f"packin {packin and packin['value']}, wellTuned {well_tuned and well_tuned['value']}, injector {injector and injector['value']}, "
          f"powered {powered and powered['value']}, servos {servos and servos['value']}, core {core and core['value']}, vulcan {vulcan and vulcan['value']}, hardLemonade {hard}, regen foods {len(r['foods'])} kinds {sorted(set(x['kind'] for x in r['foods']))}, "
          f"mags {[(x['name'], x['value'], x['kind']) for x in r['magazines']]}, thirst {len(thirst)}; maxAp active {active}, builds {m['builds']}, "
          f"foods {len(ap_foods)}, alcohol {[(x['name'], x['value']) for x in ap_alcohol]}, mags {[(x['name'], x['value']) for x in ap_mags]}, feral {feral_ap}, warnings={warnings}")


if __name__ == "__main__":
    main()
