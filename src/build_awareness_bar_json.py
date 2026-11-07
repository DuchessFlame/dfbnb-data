#!/usr/bin/env python3
"""
build_awareness_bar_json.py - data for the Awareness Bar calculator on
/bnb/specials-stats/special-stat-guides/awareness-bar-meaning/
-> dist/calculators/awareness_bar.json  (dist/pts/calculators/... with --pts)
   read by dfbnb-child/assets/df-bnb-awareness-bar.js

How the bar works (Matt, Bethesda, 5 Nov 2026 - Mia To-Finish #493; proven 6 Nov
2026 against 70 of 70 numbers on Damoshu's screenshots: Scorched, Mr. Handy,
Watoga Assaultron, Super Mutant Warlord, Blood Eagle Bruiser, each with no perk,
Tank Killer 1 and Tank Killer 2):

    level   = the level on the enemy's bar = your level held between the enemy's
              min and max level (AJNG / AJXG globals on the NPC record)
    resist  = NPC curve(level) + RACE curve(level) + resistance from its spells
    shown   = truncate( resist x every "Mod Target Damage Resistance" entry you have )
    immune  = the enemy has a perk that sets that damage type to 0
              (ImmuneToPoison, ImmuneToRadiation ...) -> crossed-out circle

Every number comes from the game exports (newest of each, tsv_source):
    NPC_Export_*.tsv / *_PRPS.tsv   names, race, level globals, resist curves
    AWARE_Export_*.tsv              race resists, NPC perks, spells, ActorType keywords
                                    (tools/esm_awareness_extract.py, run after a patch)
    GLOB_Export_*.tsv               the min / max level globals
    CURV_Export_*_POINTS.tsv        the resistance curves
    SPEL_Export_*_EFFECTS.tsv + MGEF_Export_*.tsv   resistance from abilities
    PERKEP_Export_*.tsv             immunity perks, perk-card resistance cuts
    OMOD_Export_*_Properties.tsv    Armour Penetration on weapon mods
    WEAP_Export_*_Base.tsv + dist/builds/builds_data.json   player weapons + keywords
data/awareness_bar/rules.json says WHAT to show (labels, which cards / mods); no numbers.

Usage:
    python src/build_awareness_bar_json.py          (live: tsv/ -> dist/)
    python src/build_awareness_bar_json.py --pts    (tsv/pts/ -> dist/pts/)
"""
from __future__ import annotations
import csv, datetime, json, os, re, sys
from collections import defaultdict

csv.field_size_limit(10 ** 9)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import tsv_source  # noqa: E402

CHANNEL = tsv_source.channel_of()
RULES = os.path.join(ROOT, "data", "awareness_bar", "rules.json")
OUT = os.path.join(ROOT, "dist", "pts" if CHANNEL == "pts" else "", "calculators", "awareness_bar.json")
WARN = []


def warn(m):
    WARN.append(m)
    print("[awareness_bar] WARN", m, file=sys.stderr)


def rows(pattern, exclude=None, required=True):
    p = tsv_source.newest(pattern, channel=CHANNEL, exclude=exclude, required=required)
    if not p:
        warn(f"no export for {pattern}")
        return [], ""
    with open(p, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t")), os.path.basename(p)


def fnum(v, d=None):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


REF_RE = re.compile(r"([A-Za-z0-9_\-]+)(?:\s+\"[^\"]*\")*\s*\[([A-Z_]{4}):([0-9A-Fa-f]{8})\]")
COND_RE = re.compile(r"\[tab (\d+)\]\s*(?:\w+\.)?(\w+)\((.*?)\)\s+([01]{8})\s+(-?[\d.]+)")


def parse_conds(text):
    """'[tab 1] HasKeyword(WeaponTypeRifle [KYWD:..]) 10010000 1.000000 | ...'
    -> [{t, f, r, op, v, or}]  op: 0 !=, 1 ==, 2 >, 3 >=, 4 <, 5 <= (plan_conditions order,
    settled in game); bit 4 = OR with the next line."""
    out = []
    for part in (text or "").split(" | "):
        m = COND_RE.search(part)
        if not m:
            continue
        tab, fn, params, bits, val = m.groups()
        ref = REF_RE.search(params)
        out.append({"t": int(tab), "f": fn, "r": ref.group(1) if ref else "",
                    "op": int(bits[0]) + 2 * int(bits[1]) + 4 * int(bits[2]),
                    "v": fnum(val, 0), "or": bits[3] == "1"})
    return out


def main():
    R = json.load(open(RULES, encoding="utf-8"))
    used = {}

    def load(key, pattern, **kw):
        rs, name = rows(pattern, **kw)
        used[key] = name
        return rs

    npc = load("npc", "NPC_Export_*.tsv", exclude=("_PRPS", "_Refs"))
    prps = load("prps", "NPC_Export_*_PRPS.tsv")
    aware = load("aware", "AWARE_Export_*.tsv")
    glob_rows = load("glob", "GLOB_Export_*.tsv")
    curv = load("curv", "CURV_Export_*_POINTS.tsv")
    spel = load("spel", "SPEL_Export_*_EFFECTS.tsv")
    mgef = load("mgef", "MGEF_Export_*.tsv")
    perkep = load("perkep", "PERKEP_Export_*.tsv")
    omodp = load("omod", "OMOD_Export_*_Properties.tsv")
    weap = load("weap", "WEAP_Export_*_Base.tsv")

    types = R["types"]
    av2slot = {av: t["key"] for t in types for av in t["av"]}
    dtslot = R["damage_type_slots"]
    skip_e = re.compile(R["skip_edid"])
    skip_n = re.compile(R["skip_name"])

    glob = {r["EDID"]: fnum(r.get("FLTV")) for r in glob_rows}
    curves = defaultdict(list)
    for r in curv:
        x, y = fnum(r.get("X")), fnum(r.get("Y"))
        if x is not None and y is not None:
            curves[r["EDID"]].append([x, y])
    for k in curves:
        curves[k].sort()

    # ---- spells: resistance from abilities (Value / Peak Value Modifier on a resist AV)
    mg = {}
    for r in mgef:
        av = re.match(r"\s*(\w+)", r.get("ActorValue") or "")
        mg[r.get("EDID")] = (av.group(1) if av else "", (r.get("Archetype") or "").strip())
    spell_res = defaultdict(lambda: defaultdict(float))
    for r in spel:
        av, arch = mg.get(r.get("EFID_MGEF_EDID"), ("", ""))
        if av in av2slot and arch in ("Value Modifier", "Peak Value Modifier"):
            spell_res[r["SPEL_EDID"]][av2slot[av]] += fnum(r.get("EFIT_Magnitude"), 0) or 0

    # ---- perks: immunities + perk-card resistance cuts
    ep_by_perk = defaultdict(list)
    for r in perkep:
        ep_by_perk[r["PERK_EDID"]].append(r)
    imm_perk = {}
    for pk, eps in ep_by_perk.items():
        slots = set()
        for r in eps:
            if r.get("Function") != "Set Value" or fnum(r.get("Value"), 1) != 0:
                continue
            if "Incoming" not in (r.get("EntryPoint") or ""):
                continue
            for c in parse_conds(r.get("Conditions")):
                if c["r"] in dtslot and c["op"] == 1 and c["v"] == 1:
                    slots.add(dtslot[c["r"]])
        if slots:
            imm_perk[pk] = sorted(slots)

    # ---- AWARE: race resists / spells / keywords, NPC perks / spells / keywords
    race_res = defaultdict(dict)
    race_spl = defaultdict(list)
    kw = defaultdict(set)
    npc_prk = defaultdict(list)
    npc_spl = defaultdict(list)
    lvln = defaultdict(list)
    for r in aware:
        sig, ed, fld, ref = r["Signature"], r["EDID"], r["Field"], r["Ref_EDID"]
        if sig == "LVLN" and fld == "LVLO":
            lvln[ed].append(ref)
            continue
        if sig == "RACE" and fld == "PRPS" and ref in av2slot:
            race_res[ed][av2slot[ref]] = {"c": r["Curve_EDID"]} if r["Curve_EDID"] else {"v": fnum(r["Value"], 0)}
        elif fld == "SPLO":
            (race_spl if sig == "RACE" else npc_spl)[ed].append(ref)
        elif fld == "KWDA":
            kw[(sig, ed)].add(ref)
        elif sig == "NPC_" and fld == "PRKR":
            npc_prk[ed].append(ref)
    if not aware:
        warn("no AWARE export - race resistances, immunities and enemy types missing "
             "(run tools/esm_awareness_extract.py)")

    npc_res = defaultdict(dict)
    skull = {}
    for r in prps:
        ed, av = r["NPC_EDID"], r["ActorValue_EDID"]
        if av in av2slot:
            npc_res[ed][av2slot[av]] = {"c": r["CurveTable_EDID"]} if r["CurveTable_EDID"] else {"v": fnum(r["Value"], 0)}
        elif av == "EncounterSkullIndex":
            skull[ed] = int(fnum(r["Value"], 0) or 0)

    # ---- names: the spawning records (LvlScorchedMelee ...) carry the resistances but
    # no name. The name on the bar comes from their template (TPLT): an NPC with a
    # name, or a leveled list (LVLN) of templates, one per rank ("Super Mutant Brute",
    # "Super Mutant Warlord" ...). Walk it and keep every name the record can show.
    full = {r["EDID"]: (r.get("FULL") or "").strip() for r in npc}
    tplt = {r["EDID"]: (r.get("TPLT_EDID") or "") for r in npc}
    name_cache = {}

    def names_of(ed, depth=0, seen=None):
        if ed in name_cache:
            return name_cache[ed]
        seen = seen or set()
        if not ed or ed in seen or depth > 12:
            return set()
        seen.add(ed)
        if full.get(ed):
            out = {full[ed]}
        elif ed in lvln:
            out = set()
            for ch in lvln[ed]:
                out |= names_of(ch, depth + 1, seen)
        else:
            out = names_of(tplt.get(ed, ""), depth + 1, seen)
        if depth == 0:
            name_cache[ed] = out
        return out

    # ---- enemies
    groups = defaultdict(dict)
    used_curves = set()
    for r in npc:
        ed = r["EDID"]
        if skip_e.search(ed):
            continue
        shown = sorted(n for n in names_of(ed) if not skip_n.search(n))
        if not shown:
            continue
        race = r.get("RNAM_EDID") or ""
        mine, theirs = npc_res.get(ed, {}), race_res.get(race, {})
        imm = sorted({s for pk in npc_prk.get(ed, []) for s in imm_perk.get(pk, [])})
        # No resistances anywhere: drop it (pets, allies, collectrons ...) - unless it has an
        # encounter skull (raid Mole Miners): those are enemies and their bar shows the
        # skull + immune circle only.
        if not mine and not theirs and not skull.get(ed):
            continue
        res = {}
        for t in types:
            k = t["key"]
            parts = [p for p in (mine.get(k), theirs.get(k)) if p]
            sp = sum(spell_res[s].get(k, 0) for s in npc_spl.get(ed, []) + race_spl.get(race, []))
            if sp:
                parts.append({"v": sp})
            parts = [p for p in parts if p.get("c") or p.get("v")]
            for p in parts:
                if p.get("c"):
                    if p["c"] not in curves:
                        warn(f"{ed}: curve {p['c']} not in CURV points")
                    used_curves.add(p["c"])
            if parts:
                res[k] = parts
        lo = glob.get(r.get("AJNG_EDID") or "")
        hi = glob.get(r.get("AJXG_EDID") or "")
        if lo is None or hi is None:
            a, b = fnum(r.get("ACBS_CalcMinLvl"), 0), fnum(r.get("ACBS_CalcMaxLvl"), 0)
            lvl = fnum(r.get("ACBS_Level"), 1) or 1
            lo, hi = (a, b) if a and b else (lvl, lvl)
        lo, hi = int(max(1, lo)), int(max(lo, hi))
        kws = sorted(kw.get(("NPC_", ed), set()) | kw.get(("RACE", race), set()))
        prof = json.dumps([res, imm, kws], sort_keys=True)
        sk = skull.get(ed, 0)
        words = []
        for tok, word in R["variant_words"].items():
            if tok.lower() in ed.lower() and word not in words:
                words.append(word)
        for name in shown:
            g = groups[name]
            key = (prof, sk)
            if key in g:
                v = g[key]
                for w in words:
                    v["words"][w] = v["words"].get(w, 0) + 1
                v["eds"].append(ed)
                v["lo"], v["hi"] = min(v["lo"], lo), max(v["hi"], hi)
                continue
            g[key] = {"prof": prof, "res": res, "sk": sk, "lo": lo, "hi": hi,
                      "words": {w: 1 for w in words}, "eds": [ed]}

    def curve_at(pts, x):
        if not pts:
            return 0
        if x <= pts[0][0]:
            return pts[0][1]
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            if x0 <= x <= x1:
                return y0 + (y1 - y0) * (x - x0) / (x1 - x0) if x1 != x0 else y1
        return pts[-1][1]

    def total50(res):
        return sum(curve_at(curves.get(p["c"], []), 50) if p.get("c") else p.get("v", 0)
                   for parts in res.values() for p in parts)

    # Shared resistance profiles (many names / versions use the same set) - stored once.
    profiles, prof_ix = [], {}
    enemies = []
    for name in sorted(groups, key=str.lower):
        # the version most spawning records use comes first ("Standard")
        vs = sorted(groups[name].values(), key=lambda v: (-len(v["eds"]), v["eds"][0]))
        std = total50(vs[0]["res"])
        out_v, seen = [], {}
        for i, v in enumerate(vs):
            if v["prof"] not in prof_ix:
                prof_ix[v["prof"]] = len(profiles)
                res_, imm_, kws_ = json.loads(v["prof"])
                profiles.append({"r": res_, "i": imm_, "k": kws_})
            if i == 0:
                lab = "Standard"
            # a place / mode word labels a version only when most of its records carry it
            elif [w for w, n in v["words"].items() if n * 2 >= len(v["eds"])]:
                lab = ", ".join(sorted(w for w, n in v["words"].items() if n * 2 >= len(v["eds"])))
            else:
                t = total50(v["res"])
                lab = "Tougher version" if t > std * 1.02 else ("Weaker version" if t < std * 0.98 else "Other version")
            seen[lab] = seen.get(lab, 0) + 1
            if seen[lab] > 1:
                lab += " %d" % seen[lab]
            out_v.append({"l": lab, "lo": v["lo"], "hi": v["hi"], "sk": v["sk"], "p": prof_ix[v["prof"]],
                          "e": v["eds"][:3]})
        if len(out_v) == 1:
            out_v[0]["l"] = ""
        enemies.append({"n": name, "v": out_v})

    # ---- perk cards
    cards = []
    for c in R["cards"]:
        rx = re.compile(c["edid"])
        ranks = {}
        for pk, eps in ep_by_perk.items():
            m = rx.match(pk)
            if not m:
                continue
            ents = []
            for r in eps:
                if r.get("EntryPoint") != "Mod Target Damage Resistance":
                    continue
                fn = {"Multiply Value": "mul", "Add Value": "add"}.get(r.get("Function"))
                if not fn:
                    warn(f"{pk}: function {r.get('Function')} not handled")
                    continue
                ents.append({"f": fn, "v": fnum(r.get("Value"), 1), "c": parse_conds(r.get("Conditions"))})
            if ents:
                ranks[int(m.group(1))] = {"p": pk, "e": ents}
        if not ranks:
            warn(f"card {c['label']}: no perks matched")
            continue
        cards.append({"id": c["id"], "l": c["label"], "s": c.get("special", ""), "pa": bool(c.get("pa")),
                      "ranks": [ranks[k] for k in sorted(ranks)]})

    # ---- armour penetration
    per_point = None
    for r in ep_by_perk.get(R["pen_perk"], []):
        if r.get("EntryPoint") == "Mod Target Damage Resistance" and "Actor Value Mult" in (r.get("Function") or ""):
            per_point = fnum(r.get("Value"))
    if per_point is None:
        warn("armour penetration perk not found")
    pen_val = {}
    for r in omodp:
        if r.get("PropertyName") == "ActorValues" and (r.get("Value1") or "").startswith("ArmorPenetration") \
                and r.get("FunctionType") == "ADD":
            pen_val[r["OMOD_EDID"]] = fnum(r.get("Value2"), 0)
    pen = []
    for m in R["pen_mods"]:
        v = pen_val.get(m["omod"])
        if v is None:
            warn(f"pen mod {m['omod']} not found")
            continue
        pen.append({"id": m["id"], "l": m["label"], "kind": m["kind"], "v": v})

    # ---- player weapons (Kat's builds list) + their WeaponType keywords
    wk = {}
    kre = re.compile(R["weapon_keywords"])
    for r in weap:
        wk[r["WEAP_EDID"]] = sorted({k for k in re.findall(r"(\w+)\s*(?:\"[^\"]*\")?\s*\[KYWD", r.get("Keywords") or "") if kre.match(k)})
    weapons = []
    try:
        bw = json.load(open(os.path.join(ROOT, "dist", "builds", "builds_data.json"), encoding="utf-8"))["weapons"]
    except Exception as e:  # noqa: BLE001
        warn(f"builds_data weapons not read ({e})")
        bw = []
    for w in bw:
        k = wk.get(w.get("edid"))
        if k is None:
            warn(f"weapon {w.get('name')} ({w.get('edid')}) not in WEAP export")
            continue
        weapons.append({"n": w.get("name"), "k": k})
    weapons.sort(key=lambda x: x["n"].lower())

    out = {
        "version": 1,
        "channel": CHANNEL,
        "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "observed": tsv_source.observed(),
        "sources": used,
        "types": [{"k": t["key"], "l": t["label"], "icon": t["icon"]} for t in types],
        "curves": {k: curves[k] for k in sorted(used_curves) if k in curves},
        "profiles": profiles,
        "enemies": enemies,
        "cards": cards,
        "pen": {"perPoint": per_point, "mods": pen},
        "weapons": weapons,
        "warnings": WARN[:50],
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    nv = sum(len(e["v"]) for e in enemies)
    print(f"[awareness_bar] {CHANNEL}: {len(enemies)} enemies ({nv} versions, {len(profiles)} profiles), {len(out['curves'])} curves, "
          f"{len(cards)} cards, {len(pen)} pen mods, {len(weapons)} weapons, {len(WARN)} warnings -> {OUT}")


if __name__ == "__main__":
    main()
