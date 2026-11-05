"""Damage model for the Recommended Food Buff Calculator (v2, 2 Nov 2026).

Reads the game files for the numbers the calculator's damage scoring needs and
returns mechanics["damage"]. The choices (which cards a typical build runs, how
often a situational card is active) live in rules.json -> damage_model; every
number comes from the exports here:

  * weapons   WEAP_Export_*_Base (WeaponType* keywords) + _DNAM (CRDT crit mult)
  * cards     SPEL_Export_*_EFFECTS (ability magnitude / MAGG global / CVTO curve)
              GLOB_Export_* (Perk_WeaponBonus1-3 ...), CURV_Export_*_POINTS
  * crits     CURV CT_LuckVATSCriticalCharge (crit meter per V.A.T.S. hit by Luck),
              PERKEP CriticalSavvy03 (Set Value = meter used per crit),
              PERKEP BetterCriticals03 (Add Value to crit damage mult)

How the pieces combine (STAT_DamagePerk / STAT_DamageVsPerk add into one Damage
Bonus total, STAT_CritDamagePerk adds into the crit mult, Mod Weapon Attack Damage
multiplies) is written up in the project doc "Food Buff Calculator - next thread
prompt.md". The crit meter and crit damage maths themselves run on Bethesda's
servers (no PC code reads fVATSCritical* / fCriticalDamageMult).
Crit meter (Kat 5 Nov 2026, matches the community crit table - same rule as the VATS Crit &
Luck Calculator): meter per hit = Luck curve + GMST fVATSCriticalChargeBase + the weapon's
CRDT_CritChargeBonus; the crit shot adds nothing, so a crit comes every
ceil(cost / meter per hit) + 1 shots and the share of crits is 1 / that.
"""
from __future__ import annotations
import csv, glob, os, re

csv.field_size_limit(10 ** 9)


def _newest(data_dir, pattern):
    try:
        import tsv_source
        key = tsv_source.export_key
    except Exception:            # noqa: BLE001
        key = os.path.getmtime
    files = [f for f in glob.glob(os.path.join(data_dir, pattern)) if os.sep + "pts" + os.sep not in f]
    return max(files, key=key) if files else None


def _rows(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _f(v, d=None):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


def _curve_at(pts, x):
    pts = sorted(pts)
    if not pts:
        return None
    if x <= pts[0][0]:
        return pts[0][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= x <= x1:
            return y0 + (y1 - y0) * (x - x0) / ((x1 - x0) or 1)
    return pts[-1][1]


def damage_mechanics(rules, data_dir, warn=print):
    dm = rules.get("damage_model")
    if not dm:
        return None
    p = {k: _newest(data_dir, pat) for k, pat in {
        "base": "WEAP_Export_*_Base.tsv", "dnam": "WEAP_Export_*_DNAM.tsv",
        "spel": "SPEL_Export_*_EFFECTS.tsv", "glob": "GLOB_Export_*.tsv",
        "curv": "CURV_Export_*_POINTS.tsv", "perkep": "PERKEP_Export_*.tsv",
        "gmst": "GMST_Export_*.tsv"}.items()}
    miss = [k for k, v in p.items() if not v]
    if miss:
        warn(f"damage model: missing exports {miss} - damage buffs fall back to rules weights")
        return None

    curves = {}
    for r in _rows(p["curv"]):
        x, y = _f(r.get("X")), _f(r.get("Y"))
        if x is not None and y is not None:
            curves.setdefault((r.get("EDID") or "").strip(), []).append((x, y))
    globs = {(r.get("EDID") or "").strip(): _f(r.get("FLTV")) for r in _rows(p["glob"])}
    spel = {}
    for r in _rows(p["spel"]):
        spel.setdefault((r.get("SPEL_EDID") or "").strip(), []).append(r)
    ep = {}
    for r in _rows(p["perkep"]):
        ep.setdefault((r.get("PERK_EDID") or "").strip(), []).append(r)

    # ---- weapons: tags + crit mult -------------------------------------------------
    want = set()
    for b in (dm.get("builds") or {}).values():
        want.update((b.get("weapons") or {}).values())
    want.update((dm.get("weapon_option") or {}).values())
    dn = [r for r in _rows(p["dnam"]) if r.get("WEAP_EDID") in want]
    crit = {r["WEAP_EDID"]: _f(r.get("CRDT_CritDamageMult")) for r in dn}
    charge = {r["WEAP_EDID"]: _f(r.get("CRDT_CritChargeBonus")) for r in dn}
    weapons = {}
    for r in _rows(p["base"]):
        e = r.get("WEAP_EDID")
        if e not in want:
            continue
        kw = r.get("Keywords") or ""
        tags = sorted(set(re.findall(r"WeaponType(\w+)(?: \"[^\"]*\")? \[KYWD", kw)))
        weapons[e] = {"n": r.get("WEAP_FULL") or e, "crit": crit.get(e) or 2.0,
                      "charge": charge.get(e) or 0.0, "tags": tags}
    for e in sorted(want - set(weapons)):
        warn(f"damage model: weapon {e} not in the WEAP export")

    # ---- perk card values ----------------------------------------------------------
    def card_value(c):
        """Highest rank of the card's ability spell (magnitude, MAGG global or curve)."""
        rows = spel.get(c["spell"]) or []
        if not rows:
            warn(f"damage model: ability {c['spell']} ({c.get('name')}) not in SPEL")
            return 0.0
        if c.get("curve"):
            pts = curves.get(c["curve"])
            if not pts:
                warn(f"damage model: curve {c['curve']} not found")
                return 0.0
            return round(_curve_at(pts, float(c.get("x", 0))), 3)
        best = 0.0
        for r in rows:
            if c.get("effect") and (r.get("EFID_MGEF_EDID") or "").strip() != c["effect"]:
                continue
            v = _f(r.get("EFIT_Magnitude"), 0.0)
            g = (r.get("MAGG_Global") or "").split()
            if g:
                gv = globs.get(g[-1]) if len(g) > 1 else None
                v = gv if gv is not None else v
            best = max(best, v or 0.0)
        return round(best * float(c.get("per", 1)), 3)

    def ep_value(perk, entry):
        for r in ep.get(perk) or []:
            if r.get("EntryPoint") == entry:
                return _f(r.get("Value"))
        warn(f"damage model: {perk} {entry} not in PERKEP")
        return None

    def cards(lst):
        out = []
        for c in lst or []:
            if c.get("by_answer"):                       # e.g. Iron Fist: value depends on the armour answer
                q, xs = c["by_answer"]["q"], c["by_answer"]["x"]
                vb = {a: card_value(dict(c, x=x)) for a, x in xs.items()}
                out.append({"n": c["name"], "q": q, "vBy": vb, "share": c.get("share", 1)})
            else:
                out.append({"n": c["name"], "v": card_value(c), "share": c.get("share", 1)})
        return out

    builds = {}
    for k, b in (dm.get("builds") or {}).items():
        builds[k] = {"weapons": b.get("weapons") or {}, "cards": cards(b.get("cards")),
                     "scoped": bool(b.get("scoped")), "explosive": cards(b.get("explosive_cards"))}
    # ---- Strength -> melee / unarmed damage (Kat 4 Nov) -----------------------------
    gm = {(r.get("EDID") or "").strip(): _f(r.get("Value")) for r in _rows(p["gmst"])}
    strength = None
    sd = dm.get("strength")
    if sd:
        pp = {}
        for b, edid in (sd.get("builds") or {}).items():
            v = gm.get(edid)
            if v is None:
                warn(f"damage model: Strength setting {edid} not in the GMST export")
                continue
            pp[b] = round(v * 100, 4)          # 0.05 -> 5 (% per point)
        strength = {"slot": sd.get("slot", "str"), "base": sd.get("base", 15), "perPoint": pp,
                    "src": {b: e for b, e in (sd.get("builds") or {}).items()}}
    luck = curves.get(dm.get("luck_curve", "CT_LuckVATSCriticalCharge"))
    if not luck:
        warn("damage model: Luck crit meter curve missing")
    return {
        "scale": dm.get("scale", 10),
        "luck": dm.get("luck", 15),
        "luckCurve": sorted(luck or []),
        "critBase": gm.get("fVATSCriticalChargeBase") or 0,
        "critCost": ep_value(dm.get("crit_cost_perk", "CriticalSavvy03"), "Mod VATS Critical Cost") or 100,
        "betterCrit": ep_value(dm.get("crit_bonus_perk", "BetterCriticals03"), "Mod My Critical Hit Damage Mult") or 0,
        "allBuilds": cards(dm.get("all_builds")),
        "bloodied": cards(dm.get("bloodied")),
        "builds": builds,
        "weaponOption": dm.get("weapon_option") or {},
        "weapons": weapons,
        "slots": dm.get("slots") or {},
        "enemyShare": dm.get("enemy_share", 1),
        "strength": strength,
        "src": {k: os.path.basename(v) for k, v in p.items()},
    }
