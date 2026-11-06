"""HP Calculator data, with the Endurance tab (Kat 6 Nov 2026).

/bnb/calculators/hp-calculator/  ->  dist/calculators/hp_calculator.json
(df-bnb-hp-calculator.js reads it; loaded by the router in df-bnb-bnb-calculators.js).
The old Endurance Calculator page is merged in as a tab (Kat 6 Nov).

Every number comes from the game files, nothing typed in:
  Max HP      dist/specials_stats.json stats.max_hp (NPC_/RACE PRPS Health, GMST
              fAVDHealthStartEndMult / fHealthEnduranceOffset), CURV LifeGiverBonus,
              boosts (food-table, chems, alcohol, magazines, legendary, hunger bar)
  Nocturnal Fortitude / Solar Powered   SPEL AbPerkNocturnalFortitude / AbPerkSolarPowered
              (the perk card text says +50 for both Nocturnal ranks; the spell gives 50 / 100)
  Endurance   dist/specials_stats.json stats.end (formulas sprint / disease, boosts: food,
              chems, alcohol, nuka-cola, mutations, bobbleheads, legendary perk, legendary
              effects, underarmour styles, C.A.M.P. buff stations, thirst bar, public teams)

Usage:  python src/build_hp_calculator_json.py [--pts]
"""
from __future__ import annotations
import csv, datetime, json, os, re, sys

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import tsv_source  # noqa: E402

CHANNEL = tsv_source.channel_of()
DIST = os.path.join(ROOT, "dist", "pts") if CHANNEL == "pts" else os.path.join(ROOT, "dist")
OUT = os.path.join(DIST, "calculators", "hp_calculator.json")
warnings: list[str] = []


def boost(stat, bid):
    for b in stat["boost"]:
        if b["id"] == bid:
            return b["rows"]
    warnings.append(f"{stat['key']}: no '{bid}' boost")
    return []


def num(s):
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", s or "")
    return float(m.group(1)) if m else None


def tidy(v):
    return int(v) if isinstance(v, float) and v.is_integer() else v


def item_groups(rows, word):
    """Items whose effect text names `word`, grouped by magic effect (same effect = one group)."""
    groups = {}
    for r in rows:
        for e in r.get("effects", []):
            if word not in e.get("text", "") or not e.get("magnitude"):
                continue
            sin = re.search(r"\+(\d+(?:\.\d+)?) [^()]*in a team with Strange In Numbers", e["text"])
            team = "When in a Team" in e["text"]
            groups.setdefault(e.get("mgef") or r["name"], []).append({
                "name": r["name"], "id": r.get("id"), "value": tidy(e["magnitude"]),
                "sin": tidy(float(sin.group(1))) if sin else None, "team": team,
                "duration": e.get("duration")})
    out = []
    for mgef, items in groups.items():
        items.sort(key=lambda x: (-x["value"], x["name"]))
        out.append({"mgef": mgef, "items": items})
    out.sort(key=lambda g: -g["items"][0]["value"])
    return out


def foods(rows):
    out = []
    for r in rows:
        out.append({k: tidy(r.get(k)) if isinstance(r.get(k), float) else r.get(k)
                    for k in ("diet", "dish", "id", "durationSec", "base", "mutation", "sin", "recommended")})
    return out


def bar_tiers(rows, bar):
    """Hunger / thirst bar rows -> [{tier, rejuv, rads, value}] from the condition text."""
    out = []
    for r in rows:
        texts = [" OR ".join(c["text"] for c in g) for g in r["conditions"]]
        tier = None
        for t in texts:
            m = re.match(rf"{bar} at or past (.+)$", t)
            if m:
                tier = m.group(1).strip()
        if not tier:
            continue
        has1 = any(t == "Has Rejuvenated rank 1" for t in texts)
        has2 = any(t == "Has Rejuvenated rank 2" for t in texts)
        not2_or_rads = any("Does not have Rejuvenated rank 2 OR Rads > 100" in t for t in texts)
        rads_ok = any(t == "Rads <= 100" for t in texts)
        if has1 and has2:
            rejuv = "r2_low_rads"
        elif has1 and not2_or_rads:
            rejuv = "r1_or_r2_high_rads"
        elif any(t == "Does not have Rejuvenated rank 1" for t in texts):
            rejuv = "none"
        else:
            rejuv = "any"
        out.append({"tier": tier, "rejuv": rejuv, "value": tidy(r["magnitude"]), "radsCheck": rads_ok})
    return out


def spell_values(edid):
    path = tsv_source.newest("SPEL_Export_*_EFFECTS.tsv", channel=CHANNEL)
    rows = []
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r.get("SPEL_EDID") == edid:
                rows.append(r)
    return rows, os.path.basename(path)


def main():
    ss_path = os.path.join(DIST, "specials_stats.json")
    if not os.path.exists(ss_path):
        ss_path = os.path.join(ROOT, "dist", "specials_stats.json")
    ss = json.load(open(ss_path, encoding="utf-8"))
    hp, end = ss["stats"]["max_hp"], ss["stats"]["end"]

    fhp = next(f for f in hp["formulas"] if f["id"] == "hp")
    curves = {c["edid"]: c for c in hp.get("curves", [])}
    lifegiver = curves.get("LifeGiverBonus")
    if not lifegiver:
        warnings.append("LifeGiverBonus curve missing")

    noct_rows, spel_file = spell_values("AbPerkNocturnalFortitude")
    nocturnal = []
    for r in noct_rows:
        rank = 2 if "NocturnalFortitude02" in r["Conditions"] and "= 1.0" in r["Conditions"].split("NocturnalFortitude02")[1][:80] else 1
        nocturnal.append({"rank": rank, "value": tidy(float(r["EFIT_Magnitude"]))})
    nocturnal.sort(key=lambda x: x["rank"])
    solar_rows, _ = spell_values("AbPerkSolarPowered")
    solar = next((tidy(float(r["EFIT_Magnitude"])) for r in solar_rows if r["EFID_MGEF_EDID"] == "AbPerkFortifyEndurance"), None)
    herd_rows, _ = spell_values("Mutation_HerdMentality")
    herd_solo = next((tidy(float(r["EFIT_Magnitude"])) for r in herd_rows
                      if r["EFID_MGEF_EDID"] == "Mutation_ReduceEndurance_Hidden"), None)
    if herd_solo is None:
        warnings.append("Herd Mentality solo Endurance loss not found")
    # United Ordeal (ghoul): SPEL GHL_AbPerkUnitedOrdeal, END magnitude per rank (HasPerk GHL_UnitedOrdeal0N = 1).
    uo_rows, _ = spell_values("GHL_AbPerkUnitedOrdeal")
    united = []
    for r in uo_rows:
        if r["EFID_MGEF_EDID"] != "AbPerkFortifyEndurance":
            continue
        ranks = re.findall(r"HasPerk\(GHL_UnitedOrdeal0(\d)[^)]*\) = 1", r["Conditions"])
        if ranks:
            united.append({"rank": int(ranks[0]), "value": tidy(float(r["EFIT_Magnitude"]))})
    united.sort(key=lambda x: x["rank"])
    if not united:
        warnings.append("United Ordeal END not found")

    # Ghoul Feral meter (dist/specials_stats.json hunger.feral): Health / Endurance per tier.
    def feral(av):
        out = []
        for f in ss.get("hunger", {}).get("feral", []):
            sign = 1 if f["effect"].startswith("Fortify") else (-1 if f["effect"].startswith("Reduce") else 0)
            if not sign or not f["effect"].endswith(av):
                continue
            tiers = [c for g in f["conditions"] for c in g if c.get("op") == ">="]
            if tiers:
                out.append({"tier": tiers[0]["tier"], "value": tidy(sign * f["magnitude"])})
        return out
    feral_hp, feral_end = feral("Health"), feral("Endurance")
    feral_tiers = []
    for f in ss.get("hunger", {}).get("feral", []):
        for g in f["conditions"]:
            for c in g:
                if c.get("op") == ">=" and c["tier"] not in [x["tier"] for x in feral_tiers]:
                    feral_tiers.append({"tier": c["tier"], "value": c.get("value")})
    feral_tiers.sort(key=lambda x: x["value"] or 0)
    if not nocturnal:
        warnings.append("Nocturnal Fortitude spell not found")
    if solar is None:
        warnings.append("Solar Powered END not found")

    healthy = next(({"name": r["name"], "star": r["star"], "value": tidy(num(r["effect"]))}
                    for r in boost(hp, "legendary") if r["name"] == "Healthy"), None)

    end_leg = [{"name": r["name"], "star": r["star"], "value": tidy(num(r["effect"])),
                "armour": r["armour"], "weapon": r["weapon"]} for r in boost(end, "legendary")]
    leg_perk = next(({"name": r["name"], "ranks": len(r["ranks"]),
                      "perRank": tidy(num(r["ranks"][0]["desc"]))}
                     for r in boost(end, "legendary-perks") if r["name"] == "Legendary Endurance"), None)
    styles = sorted([{"name": r["name"], "value": tidy(r["magnitude"]), "piece": r.get("piece")}
                     for r in boost(end, "mods")], key=lambda x: -x["value"])
    camp = [{"name": r["name"], "value": tidy(num(r["effect"])), "effect": r["effect"]} for r in boost(end, "camp")]
    teams = sorted([{"bond": tidy(num(r["conditions"][0][0]["text"].split("=")[-1])), "value": tidy(r["magnitude"])}
                    for r in boost(end, "teams")], key=lambda x: x["bond"])

    f_sprint = next((f for f in end["formulas"] if f["id"] == "sprint"), None)
    f_disease = next((f for f in end["formulas"] if f["id"] == "disease"), None)

    out = {
        "version": 1,
        "generated": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        "channel": CHANNEL,
        "sources": {"specials_stats": os.path.relpath(ss_path, ROOT).replace("\\", "/"), "spel": spel_file},
        "maxHp": {
            "player": fhp["inputs"]["player"], "race": fhp["inputs"]["race"],
            "perEndurance": fhp["inputs"]["mult"], "offset": fhp["inputs"].get("offset"),
            "notes": hp.get("details", []),
            "lifeGiver": {"points": lifegiver["points"] if lifegiver else [], "src": {"type": "CURV", "edid": "LifeGiverBonus"}},
            "nocturnal": nocturnal,
            "healthy": healthy,
            "foods": foods(boost(hp, "food")),
            "chems": item_groups(boost(hp, "chems"), "Max HP"),
            "alcohol": item_groups(boost(hp, "alcohol"), "Max HP"),
            "magazines": item_groups(boost(hp, "magazines"), "Max HP"),
            "hunger": bar_tiers(boost(hp, "hunger-thirst"), "Hunger"),
            "feral": feral_hp,
        },
        "endurance": {
            "min": ss["player"]["special"]["min"], "max": ss["player"]["special"]["max"],
            "notes": end.get("details", []),
            "sprint": f_sprint, "disease": f_disease,
            "legendaryPerk": leg_perk,
            "solarPowered": solar,
            "foods": foods(boost(end, "food")),
            "chems": item_groups(boost(end, "chems"), "Endurance"),
            "drinks": item_groups(boost(end, "alcohol") + boost(end, "nuka-cola"), "Endurance"),
            "mutations": item_groups(boost(end, "mutations"), "Endurance"),
            "herdSolo": herd_solo,  # SPEL Mutation_HerdMentality: lost when not in a team (no Rad-X / serum)
            "bobbleheads": item_groups(boost(end, "bobbleheads"), "Endurance"),
            "legendary": end_leg,
            "styles": styles,
            "camp": camp,
            "thirst": bar_tiers(boost(end, "hunger-thirst"), "Thirst"),
            "teams": teams,
            "feral": feral_end,
            "unitedOrdeal": united,
        },
        "feralTiers": [x["tier"] for x in feral_tiers],  # Feral meter tiers, best first (ghouls)
        "warnings": warnings,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    e = out["endurance"]
    print(f"wrote {os.path.relpath(OUT, ROOT)}: hp foods {len(out['maxHp']['foods'])}, chem groups {len(out['maxHp']['chems'])}, "
          f"hunger {len(out['maxHp']['hunger'])}, nocturnal {nocturnal}, healthy {healthy and healthy['value']}; "
          f"end foods {len(e['foods'])}, chem groups {len(e['chems'])}, drinks {len(e['drinks'])}, thirst {len(e['thirst'])}, "
          f"teams {len(e['teams'])}, solar {solar}, warnings={warnings}")


if __name__ == "__main__":
    main()
